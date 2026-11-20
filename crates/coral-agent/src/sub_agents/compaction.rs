//! Compaction: once a thread nears the model's context window, the model
//! summarizes its older messages, and later calls send the brief, the latest
//! summary and the newest messages. The stored thread keeps every message.

use tracing::Instrument as _;

use crate::model::{Block, Message, ModelError, Reply, Role, Usage};
use crate::prompt::ToolText;

use super::{RunningTurn, SubAgents, ThreadEntry, TurnFailure};

impl SubAgents {
    /// The model's reply to the thread, compacted first when the request
    /// would leave less than [`RESERVE_TOKENS`] of the configured window free.
    /// When the server still answers that the request is too long, the thread
    /// is compacted and, if that added a summary, sent once more. Otherwise
    /// the refusal stands.
    pub(super) async fn reply_within_window(
        &self,
        turn: &RunningTurn,
        system: &str,
        tools: &[ToolText],
        thread: &mut Vec<ThreadEntry>,
    ) -> Result<Reply, TurnFailure> {
        let window = self.model.context_window();
        if let Some(window) = window
            && is_near(system, tools, thread, window)
        {
            self.compact(turn, system, tools, thread, window).await?;
        }
        let reply = self.model.reply(system, &for_model(thread), tools).await;
        let (Err(ModelError::ContextTooLong(_)), Some(window)) = (&reply, window) else {
            return Ok(reply?);
        };
        // The server counts tokens and Coral estimates them. With nothing
        // left to summarize, the refusal stands.
        if !self.compact(turn, system, tools, thread, window).await? {
            return Ok(reply?);
        }
        Ok(self.model.reply(system, &for_model(thread), tools).await?)
    }

    /// Compacts the thread in a span that records its tokens before and
    /// after, and returns whether it added a summary.
    async fn compact(
        &self,
        turn: &RunningTurn,
        system: &str,
        tools: &[ToolText],
        thread: &mut Vec<ThreadEntry>,
        window: u32,
    ) -> Result<bool, TurnFailure> {
        let before = context_tokens(system, tools, thread);
        let (length, compaction) = (thread.len(), turn.trace.compaction(before));
        let summarized = self.summarize_older(turn, system, tools, thread, window);
        let summarized = summarized.instrument(compaction.span()).await;
        let compacted = thread.len() > length;
        compaction.ended(compacted, context_tokens(system, tools, thread));
        summarized.map(|()| compacted)
    }

    /// Adds the model's summary of the messages before the newest ones to the
    /// thread. With nothing to summarize, or a failed summary, the thread
    /// stays as it was.
    async fn summarize_older(
        &self,
        turn: &RunningTurn,
        system: &str,
        tools: &[ToolText],
        thread: &mut Vec<ThreadEntry>,
        window: u32,
    ) -> Result<(), TurnFailure> {
        let previous = thread
            .iter()
            .rev()
            .find_map(|entry| summary_in(&entry.message));
        let start = previous.map_or(1, |(_, kept_from)| kept_from);
        let keep_budget = keep_recent_budget(system, tools, thread, window);
        let Some(kept_from) = cut(thread, start, keep_budget) else {
            return Ok(());
        };
        let older = thread.get(start..kept_from).unwrap_or_default();
        let previous = previous.map(|(text, _)| text);
        let request = [summary_request(previous, older, window)];
        let summarized = self
            .model
            .summarize(SUMMARIZER, &request, SUMMARY_MAX_TOKENS);
        let reply = match summarized.await {
            Ok(reply) if !reply.message.text().trim().is_empty() => reply,
            Ok(_) | Err(_) => {
                tracing::warn!("a Sub-agent thread was not compacted");
                return Ok(());
            }
        };
        let text = reply.message.text();
        let summary = Message {
            role: Role::User,
            content: vec![Block::Summary { text, kept_from }],
        };
        self.add(turn, thread, summary, Some(&reply.call)).await
    }
}

/// The room a request leaves for the reply and the round after it.
const RESERVE_TOKENS: usize = 16_384;
/// About how many tokens of the newest messages stay whole.
const KEEP_RECENT_TOKENS: usize = 20_000;
/// The most a summary may take, reasoning included: 0.8 of the reserve.
const SUMMARY_MAX_TOKENS: u32 = 13_107;
/// Bytes per token in an estimate: fewer than prose takes, so that JSON and
/// code are not undercounted.
const BYTES_PER_TOKEN: usize = 3;
/// The most of one tool call or result that the summarizer reads.
const BLOCK_MAX_BYTES: usize = 2_000;

const SUMMARIZER: &str = "You summarize a data agent's work so that it can continue from your summary. Do not continue the work. Write only the summary.";

const INSTRUCTIONS: &str = "\
Summarize the work above for the agent that did it. If it starts with an earlier summary, keep what still holds in it. Use these sections:
## Goal
What the lead asked for, with every later instruction.
## Facts
Each fact found so far, with its source: a run_id, record ID or link.
## Done
Each check and what it returned, empty results and errors included.
## Next
What to do next, with any run_id still running.
Copy IDs, call paths, run_ids and error messages exactly.";

const SUMMARY_OPENING: &str =
    "Your thread grew too long to keep whole. This summary replaces its older messages:\n\n";

/// The thread as the model reads it. After a summary: the brief, the latest
/// summary, and the messages that it left whole.
fn for_model(thread: &[ThreadEntry]) -> Vec<Message> {
    let Some((text, kept_from)) = thread
        .iter()
        .rev()
        .find_map(|entry| summary_in(&entry.message))
    else {
        return thread.iter().map(|entry| entry.message.clone()).collect();
    };
    let summary = Message::user(format!("{SUMMARY_OPENING}{text}"));
    let kept = thread.iter().skip(kept_from);
    let kept = kept
        .filter(|entry| summary_in(&entry.message).is_none())
        .map(|entry| entry.message.clone());
    let brief = thread.first().map(|entry| entry.message.clone());
    brief.into_iter().chain([summary]).chain(kept).collect()
}

/// The summary that `message` holds, and where the messages it left whole start.
fn summary_in(message: &Message) -> Option<(&str, usize)> {
    match message.content.as_slice() {
        [Block::Summary { text, kept_from }] => Some((text, *kept_from)),
        _ => None,
    }
}

/// Whether sending `thread` would leave less than [`RESERVE_TOKENS`] of
/// `window` free.
fn is_near(system: &str, tools: &[ToolText], thread: &[ThreadEntry], window: u32) -> bool {
    context_tokens(system, tools, thread) + RESERVE_TOKENS > window as usize
}

/// The most recent post-summary assistant reply that carries non-zero reported
/// token usage, and its index in `thread`.
fn usage_anchor(thread: &[ThreadEntry]) -> Option<(usize, usize)> {
    let after_summary = thread
        .iter()
        .rposition(|entry| summary_in(&entry.message).is_some())
        .map_or(0, |index| index + 1);
    thread
        .get(after_summary..)
        .unwrap_or_default()
        .iter()
        .enumerate()
        .rev()
        .find_map(|(offset, entry)| {
            (entry.message.role == Role::Assistant)
                .then(|| entry.usage.and_then(Usage::context_tokens))
                .flatten()
                .map(|tokens| (after_summary + offset, tokens))
        })
}

/// The next request's size in tokens, estimated from bytes alone.
fn estimated_tokens(system: &str, tools: &[ToolText], thread: &[ThreadEntry]) -> usize {
    let tools = tools
        .iter()
        .map(|tool| tool.name.len() + tool.description.len() + tool.parameters.len());
    let fixed = (system.len() + tools.sum::<usize>()).div_ceil(BYTES_PER_TOKEN);
    fixed + for_model(thread).iter().map(tokens).sum::<usize>()
}

/// The next request's size in tokens for `system`, `tools`, and `thread`.
/// Anchors on the last post-summary assistant reply's reported usage plus a
/// byte estimate of trailing messages; falls back to estimating the full model
/// view when no post-summary usage is available.
fn context_tokens(system: &str, tools: &[ToolText], thread: &[ThreadEntry]) -> usize {
    if let Some((index, reported)) = usage_anchor(thread) {
        let trailing: usize = thread
            .get(index + 1..)
            .unwrap_or_default()
            .iter()
            .map(|entry| tokens(&entry.message))
            .sum();
        return reported.saturating_add(trailing);
    }
    estimated_tokens(system, tools, thread)
}

/// The `bytes / 3` budget that [`cut`] keeps whole: capped at half the usable
/// window (`window - RESERVE_TOKENS`) so small windows retain room to grow,
/// and scaled down by `estimated / reported` when the model's tokenizer runs
/// denser than `bytes / 3`.
fn keep_recent_budget(
    system: &str,
    tools: &[ToolText],
    thread: &[ThreadEntry],
    window: u32,
) -> usize {
    let target = KEEP_RECENT_TOKENS.min((window as usize).saturating_sub(RESERVE_TOKENS) / 2);
    let Some((index, reported)) = usage_anchor(thread) else {
        return target;
    };
    let estimated = estimated_tokens(system, tools, thread.get(..=index).unwrap_or(thread));
    if reported > estimated && estimated > 0 {
        target.saturating_mul(estimated) / reported
    } else {
        target
    }
}

/// A message's size in tokens, estimated from its bytes.
fn tokens(message: &Message) -> usize {
    let bytes = message.content.iter().map(|block| match block {
        Block::Text { text } | Block::Summary { text, .. } => text.len(),
        Block::Reasoning { text, opaque, .. } => {
            let opaque_bytes = opaque
                .as_ref()
                .map_or(0, |value| value.to_string().len() / 2);
            text.len().max(opaque_bytes)
        }
        Block::ToolUse(call) => call.name.len() + call.arguments.len(),
        Block::ToolResult { content, .. } => content.len(),
    });
    bytes.sum::<usize>().div_ceil(BYTES_PER_TOKEN)
}

/// Where the messages left whole start: walking back from the newest while
/// fewer than `keep_budget` estimated tokens are kept, the earliest message
/// that answers no call. The newest results stay with the reply that called
/// for them, however large they are: the model has not read them yet. `None`
/// when nothing before the cut is left to summarize.
fn cut(thread: &[ThreadEntry], start: usize, keep_budget: usize) -> Option<usize> {
    let mut kept = 0;
    let mut cut = thread.len();
    for (index, entry) in thread.iter().enumerate().skip(start).rev() {
        if kept >= keep_budget && cut < thread.len() {
            break;
        }
        if summary_in(&entry.message).is_some() {
            continue;
        }
        kept += tokens(&entry.message);
        let answers_a_call = entry
            .message
            .content
            .iter()
            .any(|block| matches!(block, Block::ToolResult { .. }));
        if !answers_a_call {
            cut = index;
        }
    }
    (cut > start).then_some(cut)
}

/// What the summarizer reads: the previous summary, then `older` as a
/// transcript whose oldest lines are left out to fit `window`.
fn summary_request(previous: Option<&str>, older: &[ThreadEntry], window: u32) -> Message {
    let previous = previous.map(|text| format!("[Earlier summary]\n{text}\n\n"));
    let previous = previous.unwrap_or_default();
    let fixed = SUMMARIZER.len() + INSTRUCTIONS.len() + previous.len();
    let window = (window as usize).saturating_sub(SUMMARY_MAX_TOKENS as usize);
    let mut room = (window * BYTES_PER_TOKEN).saturating_sub(fixed);
    let mut lines = Vec::new();
    for line in older
        .iter()
        .flat_map(|entry| transcript(&entry.message))
        .rev()
    {
        let Some(left) = room.checked_sub(line.len() + 2) else {
            break;
        };
        room = left;
        lines.push(line);
    }
    lines.reverse();
    let work = lines.join("\n\n");
    Message::user(format!(
        "<work>\n{previous}{work}\n</work>\n\n{INSTRUCTIONS}"
    ))
}

/// One message as transcript lines, each call and result cut short and its
/// reasoning left out.
fn transcript(message: &Message) -> Vec<String> {
    let lines = message.content.iter().filter_map(|block| match block {
        Block::Text { text } if message.role == Role::User => Some(format!("[Lead]: {text}")),
        Block::Text { text } => Some(format!("[Agent]: {text}")),
        Block::ToolUse(call) => Some(format!(
            "[Agent calls {}]: {}",
            call.name,
            clip(&call.arguments)
        )),
        Block::ToolResult { content, .. } => Some(format!("[Tool result]: {}", clip(content))),
        Block::Reasoning { .. } | Block::Summary { .. } => None,
    });
    lines.collect()
}

/// `text` cut to its first [`BLOCK_MAX_BYTES`], saying how much was left out.
fn clip(text: &str) -> String {
    let end = text.floor_char_boundary(BLOCK_MAX_BYTES);
    match text.get(..end) {
        Some(kept) if end < text.len() => format!("{kept} [{} bytes left out]", text.len() - end),
        _ => text.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ToolCall;

    // Catches: a cut that sends the model a tool result without its call, or
    // a call without its result.
    #[test]
    fn the_model_reads_each_kept_call_with_its_result() {
        let mut thread = vec![ThreadEntry::plain(Message::user("brief".to_owned()))];
        for round in 0..6 {
            let calls: Vec<ToolCall> = (0..2)
                .map(|n| ToolCall {
                    id: format!("call_{round}_{n}"),
                    name: "exec".to_owned(),
                    arguments: "{}".to_owned(),
                })
                .collect();
            let uses = calls.iter().cloned().map(Block::ToolUse).collect();
            let results = calls
                .iter()
                .map(|call| call.result("x".repeat(12_000), false, None));
            thread.push(ThreadEntry::plain(Message {
                role: Role::Assistant,
                content: uses,
            }));
            thread.push(ThreadEntry::plain(Message::tool_results(results.collect())));
        }
        let kept_from = cut(&thread, 1, KEEP_RECENT_TOKENS).unwrap();
        let text = "Two rounds are done.".to_owned();
        thread.push(ThreadEntry::plain(Message {
            role: Role::User,
            content: vec![Block::Summary { text, kept_from }],
        }));

        let sent = for_model(&thread);

        let opening = Message::user(format!("{SUMMARY_OPENING}Two rounds are done."));
        let first = thread.first().unwrap().message.clone();
        assert_eq!(sent.get(..2), Some([first, opening].as_slice()));
        assert!(sent.len() > 3 && sent.len() < thread.len(), "{sent:?}");
        let mut open = Vec::new();
        for block in sent.iter().flat_map(|message| &message.content) {
            match block {
                Block::ToolUse(call) => open.push(call.id.as_str()),
                Block::ToolResult { tool_use_id, .. } => {
                    assert!(open.contains(&tool_use_id.as_str()), "{tool_use_id}");
                    open.retain(|id| id != tool_use_id);
                }
                Block::Text { .. } | Block::Reasoning { .. } | Block::Summary { .. } => {}
            }
        }
        assert_eq!(open, Vec::<&str>::new());
    }

    // Catches: a cut that summarizes away results the model has not read,
    // because they alone fill what is kept.
    #[test]
    fn the_newest_results_stay_with_their_call_however_large() {
        let round = |results: usize| {
            let call = |n| ToolCall {
                id: format!("call_{results}_{n}"),
                name: "exec".to_owned(),
                arguments: "{}".to_owned(),
            };
            let calls: Vec<ToolCall> = (0..results).map(call).collect();
            let results = calls
                .iter()
                .map(|call| call.result("x".repeat(20_480), false, None));
            let results = Message::tool_results(results.collect());
            let content = calls.into_iter().map(Block::ToolUse).collect();
            let role = Role::Assistant;
            [
                ThreadEntry::plain(Message { role, content }),
                ThreadEntry::plain(results),
            ]
        };
        let brief = ThreadEntry::plain(Message::user("brief".to_owned()));
        let thread = [vec![brief], round(1).to_vec(), round(3).to_vec()].concat();

        assert_eq!(
            cut(&thread, 1, KEEP_RECENT_TOKENS),
            Some(3),
            "the reply that made the three calls"
        );
    }

    // Catches: a tokenizer denser than 3 bytes/token (such as Claude on JSON
    // tool results) keeping too many real tokens at a 48K window so the
    // compacted prompt lands back over the compaction threshold.
    #[test]
    fn keep_recent_budget_caps_small_windows_and_scales_for_dense_tokenizers() {
        let mut thread = vec![ThreadEntry::plain(Message::user("brief".to_owned()))];
        for round in 0..6 {
            let call = ToolCall {
                id: format!("call_{round}"),
                name: "exec".to_owned(),
                arguments: "{}".to_owned(),
            };
            let result = call.result("x".repeat(12_000), false, None);
            let usage = (round == 5).then_some(Usage {
                // ~24K estimated tokens at bytes/3, ~32.4K real tokens at 2.3 bytes/token.
                input_tokens: 31_400,
                cache_read_input_tokens: 0,
                output_tokens: 1_000,
            });
            thread.push(ThreadEntry {
                message: Message {
                    role: Role::Assistant,
                    content: vec![Block::ToolUse(call)],
                },
                usage,
            });
            thread.push(ThreadEntry::plain(Message::tool_results(vec![result])));
        }

        let budget_48k = keep_recent_budget("system", &[], &thread, 48_000);
        let budget_200k = keep_recent_budget("system", &[], &thread, 200_000);
        // 48K target is 15,808, scaled down by ~20K / 32.4K (~9,700 est tokens).
        assert!(budget_48k < 12_000, "{budget_48k}");
        assert!(budget_200k < KEEP_RECENT_TOKENS, "{budget_200k}");
        assert!(budget_48k < budget_200k, "{budget_48k} vs {budget_200k}");

        let kept_from = cut(&thread, 1, budget_48k).unwrap();
        let kept_est: usize = thread
            .get(kept_from..)
            .unwrap_or_default()
            .iter()
            .map(|entry| tokens(&entry.message))
            .sum();
        // At 2.3 bytes/token (~1.3x bytes/3), the kept tail stays well under the 31,616 trigger.
        let kept_real = kept_est * 30 / 23;
        assert!(
            kept_real < 20_000,
            "kept_est={kept_est}, kept_real={kept_real}"
        );
    }

    // Catches: ignoring provider-reported usage, double-counting pre-anchor
    // messages, anchoring on pre-summary or summarizer usage after compaction,
    // or failing to fall back when usage is absent or zero.
    #[test]
    fn context_tokens_anchors_on_last_post_summary_assistant_usage() {
        let assistant = |text: &str, usage: Option<Usage>| ThreadEntry {
            message: Message {
                role: Role::Assistant,
                content: vec![Block::Text {
                    text: text.to_owned(),
                }],
            },
            usage,
        };
        let usage = |input, cache_read, output| {
            Some(Usage {
                input_tokens: input,
                cache_read_input_tokens: cache_read,
                output_tokens: output,
            })
        };

        // Anchors on reported usage + trailing byte estimate.
        let mut thread = vec![
            ThreadEntry::plain(Message::user("x".repeat(30_000))),
            assistant("ok", usage(10_000, 20_000, 1_500)),
            ThreadEntry::plain(Message::user("y".repeat(300))),
        ];
        assert_eq!(context_tokens("system", &[], &thread), 31_600);

        // Zero reported usage falls back to the byte estimate.
        let zero_thread = vec![
            ThreadEntry::plain(Message::user("x".repeat(300))),
            assistant("ok", usage(0, 0, 0)),
        ];
        let zero_expected: usize = 1 + zero_thread
            .iter()
            .map(|entry| tokens(&entry.message))
            .sum::<usize>();
        assert_eq!(context_tokens("sys", &[], &zero_thread), zero_expected);

        // After compaction, pre-summary assistant usage and the summarizer's
        // own usage are ignored; falls back to estimating the compacted view.
        thread.push(ThreadEntry {
            message: Message {
                role: Role::User,
                content: vec![Block::Summary {
                    text: "summary".to_owned(),
                    kept_from: 1,
                }],
            },
            usage: usage(40_000, 0, 500),
        });
        let post_compaction_estimate = "system".len().div_ceil(BYTES_PER_TOKEN)
            + for_model(&thread).iter().map(tokens).sum::<usize>();
        assert_eq!(
            context_tokens("system", &[], &thread),
            post_compaction_estimate
        );

        // Once a post-summary assistant reply arrives, its usage becomes the anchor.
        thread.push(assistant("next", usage(2_000, 5_000, 400)));
        thread.push(ThreadEntry::plain(Message::user("z".repeat(600))));
        assert_eq!(context_tokens("system", &[], &thread), 7_600);
    }

    // Catches: reasoning shown to the summarizer as the agent's words, and
    // reasoning (plain text or opaque-only) left out of the size that decides
    // when to compact.
    #[test]
    fn reasoning_counts_toward_the_size_and_is_not_summarized() {
        let reasoning = Block::Reasoning {
            text: "x".repeat(295),
            model: "sailfish-1".to_owned(),
            protocol: Some(crate::model::Protocol::ChatCompletions),
            opaque: None,
        };
        let text = Block::Text {
            text: "Done.".to_owned(),
        };
        let message = Message {
            role: Role::Assistant,
            content: vec![reasoning, text],
        };

        assert_eq!(tokens(&message), 100);
        assert_eq!(transcript(&message), ["[Agent]: Done."]);

        let opaque_only = Block::Reasoning {
            text: String::new(),
            model: "gpt-5.4".to_owned(),
            protocol: Some(crate::model::Protocol::OpenAiResponses),
            opaque: Some(serde_json::json!({
                "type": "reasoning",
                "encrypted_content": "a".repeat(600),
                "summary": [],
            })),
        };
        let opaque_message = Message {
            role: Role::Assistant,
            content: vec![
                opaque_only,
                Block::Text {
                    text: "Done.".to_owned(),
                },
            ],
        };
        assert!(
            tokens(&opaque_message) >= 100,
            "opaque reasoning must count"
        );
        assert_eq!(transcript(&opaque_message), ["[Agent]: Done."]);

        let summary_with_opaque = Block::Reasoning {
            text: "Short summary.".to_owned(),
            model: "gpt-5.4".to_owned(),
            protocol: Some(crate::model::Protocol::OpenAiResponses),
            opaque: Some(serde_json::json!({
                "type": "reasoning",
                "encrypted_content": "a".repeat(600),
                "summary": [{"type": "summary_text", "text": "Short summary."}],
            })),
        };
        let mixed_message = Message {
            role: Role::Assistant,
            content: vec![
                summary_with_opaque,
                Block::Text {
                    text: "Done.".to_owned(),
                },
            ],
        };
        assert!(
            tokens(&mixed_message) >= 100,
            "opaque payload must still count alongside a short summary"
        );
        assert_eq!(transcript(&mixed_message), ["[Agent]: Done."]);
    }
}
