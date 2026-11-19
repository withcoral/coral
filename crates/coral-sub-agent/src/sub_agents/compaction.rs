//! Compaction: once a thread nears the model's context window, the model
//! summarizes its older messages, and later calls send the brief, the latest
//! summary and the newest messages. The stored thread keeps every message.

use tracing::Instrument as _;

use crate::model::{Block, Message, ModelError, Reply, Role};
use crate::prompt::ToolText;

use super::{RunningTurn, SubAgents, TurnFailure};

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
        thread: &mut Vec<Message>,
    ) -> Result<Reply, TurnFailure> {
        let window = self.model.context_window();
        if let Some(window) = window
            && is_near(system, tools, &for_model(thread), window)
        {
            self.compact(turn, thread, window).await?;
        }
        let reply = self.model.reply(system, &for_model(thread), tools).await;
        let (Err(ModelError::ContextTooLong(_)), Some(window)) = (&reply, window) else {
            return Ok(reply?);
        };
        // The server counts tokens and Coral estimates them. With nothing
        // left to summarize, the refusal stands.
        if !self.compact(turn, thread, window).await? {
            return Ok(reply?);
        }
        Ok(self.model.reply(system, &for_model(thread), tools).await?)
    }

    /// Compacts the thread in a span that records its tokens before and
    /// after, and returns whether it added a summary.
    async fn compact(
        &self,
        turn: &RunningTurn,
        thread: &mut Vec<Message>,
        window: u32,
    ) -> Result<bool, TurnFailure> {
        let (length, compaction) = (thread.len(), turn.trace.compaction(size(thread)));
        let summarized = self.summarize_older(turn, thread, window);
        let summarized = summarized.instrument(compaction.span()).await;
        let compacted = thread.len() > length;
        compaction.ended(compacted, size(thread));
        summarized.map(|()| compacted)
    }

    /// Adds the model's summary of the messages before the newest ones to the
    /// thread. With nothing to summarize, or a failed summary, the thread
    /// stays as it was.
    async fn summarize_older(
        &self,
        turn: &RunningTurn,
        thread: &mut Vec<Message>,
        window: u32,
    ) -> Result<(), TurnFailure> {
        let previous = thread.iter().rev().find_map(summary_in);
        let start = previous.map_or(1, |(_, kept_from)| kept_from);
        let Some(kept_from) = cut(thread, start) else {
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
fn for_model(thread: &[Message]) -> Vec<Message> {
    let Some((text, kept_from)) = thread.iter().rev().find_map(summary_in) else {
        return thread.to_vec();
    };
    let summary = Message::user(format!("{SUMMARY_OPENING}{text}"));
    let kept = thread.iter().skip(kept_from);
    let kept = kept
        .filter(|message| summary_in(message).is_none())
        .cloned();
    let brief = thread.first().cloned();
    brief.into_iter().chain([summary]).chain(kept).collect()
}

/// The summary that `message` holds, and where the messages it left whole start.
fn summary_in(message: &Message) -> Option<(&str, usize)> {
    match message.content.as_slice() {
        [Block::Summary { text, kept_from }] => Some((text, *kept_from)),
        _ => None,
    }
}

/// Whether sending `messages` would leave less than [`RESERVE_TOKENS`] of
/// `window` free.
fn is_near(system: &str, tools: &[ToolText], messages: &[Message], window: u32) -> bool {
    let tools = tools
        .iter()
        .map(|tool| tool.name.len() + tool.description.len() + tool.parameters.len());
    let fixed = (system.len() + tools.sum::<usize>()).div_ceil(BYTES_PER_TOKEN);
    let tokens = fixed + messages.iter().map(tokens).sum::<usize>();
    tokens + RESERVE_TOKENS > window as usize
}

/// The thread's size in tokens as the model reads it, estimated.
fn size(thread: &[Message]) -> usize {
    for_model(thread).iter().map(tokens).sum()
}

/// A message's size in tokens, estimated from its bytes.
fn tokens(message: &Message) -> usize {
    let bytes = message.content.iter().map(|block| match block {
        Block::Text { text } | Block::Reasoning { text, .. } | Block::Summary { text, .. } => {
            text.len()
        }
        Block::ToolUse(call) => call.name.len() + call.arguments.len(),
        Block::ToolResult { content, .. } => content.len(),
    });
    bytes.sum::<usize>().div_ceil(BYTES_PER_TOKEN)
}

/// Where the messages left whole start: walking back from the newest while
/// fewer than [`KEEP_RECENT_TOKENS`] are kept, the earliest message that
/// answers no call. The newest results stay with the reply that called for
/// them, however large they are: the model has not read them yet. `None`
/// when nothing before the cut is left to summarize.
fn cut(thread: &[Message], start: usize) -> Option<usize> {
    let mut kept = 0;
    let mut cut = thread.len();
    for (index, message) in thread.iter().enumerate().skip(start).rev() {
        if kept >= KEEP_RECENT_TOKENS && cut < thread.len() {
            break;
        }
        if summary_in(message).is_some() {
            continue;
        }
        kept += tokens(message);
        let answers_a_call = message
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
fn summary_request(previous: Option<&str>, older: &[Message], window: u32) -> Message {
    let previous = previous.map(|text| format!("[Earlier summary]\n{text}\n\n"));
    let previous = previous.unwrap_or_default();
    let fixed = SUMMARIZER.len() + INSTRUCTIONS.len() + previous.len();
    let window = (window as usize).saturating_sub(SUMMARY_MAX_TOKENS as usize);
    let mut room = (window * BYTES_PER_TOKEN).saturating_sub(fixed);
    let mut lines = Vec::new();
    for line in older.iter().flat_map(transcript).rev() {
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
        let mut thread = vec![Message::user("brief".to_owned())];
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
                .map(|call| call.result("x".repeat(12_000), None));
            thread.push(Message {
                role: Role::Assistant,
                content: uses,
            });
            thread.push(Message::tool_results(results.collect()));
        }
        let kept_from = cut(&thread, 1).unwrap();
        let text = "Two rounds are done.".to_owned();
        thread.push(Message {
            role: Role::User,
            content: vec![Block::Summary { text, kept_from }],
        });

        let sent = for_model(&thread);

        let opening = Message::user(format!("{SUMMARY_OPENING}Two rounds are done."));
        let first = thread.first().cloned().unwrap();
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
                .map(|call| call.result("x".repeat(20_480), None));
            let results = Message::tool_results(results.collect());
            let content = calls.into_iter().map(Block::ToolUse).collect();
            let role = Role::Assistant;
            [Message { role, content }, results]
        };
        let brief = Message::user("brief".to_owned());
        let thread = [vec![brief], round(1).to_vec(), round(3).to_vec()].concat();

        assert_eq!(
            cut(&thread, 1),
            Some(3),
            "the reply that made the three calls"
        );
    }

    // Catches: reasoning shown to the summarizer as the agent's words, and
    // reasoning left out of the size that decides when to compact.
    #[test]
    fn reasoning_counts_toward_the_size_and_is_not_summarized() {
        let reasoning = Block::Reasoning {
            text: "x".repeat(295),
            model: "sailfish-1".to_owned(),
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
    }
}
