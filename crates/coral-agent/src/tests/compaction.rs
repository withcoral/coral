use wiremock::Request;

use super::*;

const SUMMARY: &str = "## Goal\nCount the open invoices.";
const ANSWER: &str = "12 are open.";

/// A Chat Completions reply carrying `message` and explicit token usage.
fn completion_with_usage(
    message: &Value,
    prompt_tokens: i64,
    cached_tokens: i64,
    completion_tokens: i64,
) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "model": MODEL,
        "choices": [{"message": message}],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "prompt_tokens_details": {"cached_tokens": cached_tokens},
        },
    }))
}

fn tool_calls_message(calls: &[ToolCall]) -> Value {
    let calls: Vec<_> = calls
        .iter()
        .map(|call| {
            json!({
                "id": call.id,
                "type": "function",
                "function": {"name": call.name, "arguments": call.arguments},
            })
        })
        .collect();
    json!({"role": "assistant", "content": null, "tool_calls": calls})
}

/// The stub writes a summary, with reasoning, when asked for one. Otherwise
/// it makes `calls` failed searches of about 11,000 tokens each, then answers.
pub(super) async fn stub(lead: &Lead, calls: usize) {
    stub_steps(lead, calls, true).await;
}

async fn stub_steps(lead: &Lead, calls: usize, include_usage: bool) {
    let summary = json!({"role": "assistant", "content": SUMMARY, "reasoning": "It is long."});
    let summary_response = if include_usage {
        completion(&summary)
    } else {
        ResponseTemplate::new(200).set_body_json(json!({
            "model": MODEL,
            "choices": [{"message": summary}],
        }))
    };
    Mock::given(body_string_contains("max_completion_tokens"))
        .respond_with(summary_response)
        .with_priority(1)
        .mount(&lead.model)
        .await;
    for call in 1..=calls {
        let arguments = json!({"program": "x".repeat(33_000)});
        let message =
            tool_calls_message(&[tool_call(&format!("call_{call}"), "search", &arguments)]);
        let response = if include_usage {
            let prompt_tokens = 1_000 + i64::try_from(call - 1).unwrap() * 11_000;
            completion_with_usage(&message, prompt_tokens, 0, 11_000)
        } else {
            ResponseTemplate::new(200).set_body_json(json!({
                "model": MODEL,
                "choices": [{"message": message}],
            }))
        };
        Mock::given(any())
            .respond_with(response)
            .up_to_n_times(1)
            .mount(&lead.model)
            .await;
    }
    lead.model_answers(reply(ANSWER)).await;
}

/// What each request to the stub asked for.
async fn requests(lead: &Lead) -> Vec<&'static str> {
    let requests = lead.model.received_requests().await.unwrap();
    let kind = |request: &Request| {
        let body = String::from_utf8_lossy(&request.body);
        if body.contains("max_completion_tokens") {
            "summary"
        } else {
            "step"
        }
    };
    requests.iter().map(kind).collect()
}

// Catches: a thread sent whole once it leaves less than the reserve free, a
// summary that is lost from the stored thread or replaces a message in it,
// and a summary refused for its reasoning.
#[tokio::test]
async fn a_thread_near_the_window_is_summarized_and_the_turn_answers() {
    let lead = Lead::new().await;
    let tools = lead.tools_with_window(Some(48_000));
    stub(&lead, 3).await;

    let (sub_agent_id, waited) = lead.turn(&*tools, &lead.task().await).await;

    assert_eq!(waited.get("answer"), Some(&json!(ANSWER)), "{waited}");
    let steps = ["step", "step", "step", "summary", "step"];
    assert_eq!(requests(&lead).await, steps);
    let received = lead.model.received_requests().await.unwrap();
    let [.., summarize, last] = received.as_slice() else {
        panic!("five requests");
    };
    let summarize: Value = summarize.body_json().unwrap();
    assert_eq!(summarize.get("max_completion_tokens"), Some(&json!(13_107)));
    assert_eq!(summarize.get("tools"), None);
    let last = String::from_utf8_lossy(&last.body);
    assert!(last.contains("Count the open invoices.") && !last.contains("call_1"));
    let thread = lead.thread(&sub_agent_id).await;
    let calls = thread.iter().flat_map(Message::tool_calls);
    let calls: Vec<_> = calls.map(|call| call.id.as_str()).collect();
    assert_eq!(calls, ["call_1", "call_2", "call_3"]);
    let text = SUMMARY.to_owned();
    let summary = [Block::Summary { text, kept_from: 3 }];
    let stored = thread.get(7).map(|message| message.content.as_slice());
    assert_eq!(stored, Some(summary.as_slice()));
    assert_eq!(
        thread.len(),
        9,
        "brief, 3 calls, 3 results, summary, answer"
    );
}

// Catches: a model with no context window set that is never compacted, or is compacted as if its
// window were small.
#[tokio::test]
async fn a_model_with_no_window_compacts_as_if_it_had_128_000_tokens() {
    let lead = Lead::new().await;
    // The twelfth call leaves less than the reserve free of 128,000 tokens.
    stub(&lead, 11).await;

    let (_, waited) = lead.turn(&*lead.tools(), &lead.task().await).await;

    assert_eq!(waited.get("answer"), Some(&json!(ANSWER)), "{waited}");
    let mut steps = vec!["step"; 11];
    steps.extend(["summary", "step"]);
    assert_eq!(requests(&lead).await, steps);
}

// Catches: a server's "too long" that fails the Turn where a summary of its
// older messages lets it go on, and a retry that sends the thread whole again.
#[tokio::test]
async fn a_refusal_as_too_long_compacts_the_thread_and_asks_again() {
    let lead = Lead::new().await;
    let tools = lead.tools_with_window(Some(200_000));
    let too_long = json!({"error": {"message": "m", "code": "context_length_exceeded"}});
    // The request that holds the third round is refused once.
    Mock::given(body_string_contains("call_3"))
        .respond_with(ResponseTemplate::new(400).set_body_json(too_long))
        .up_to_n_times(1)
        .mount(&lead.model)
        .await;
    stub(&lead, 3).await;

    let (_, waited) = lead.turn(&*tools, &lead.task().await).await;

    assert_eq!(waited.get("answer"), Some(&json!(ANSWER)), "{waited}");
    let steps = ["step", "step", "step", "step", "summary", "step"];
    assert_eq!(requests(&lead).await, steps);
    let received = lead.model.received_requests().await.unwrap();
    let retried = received
        .last()
        .map(|request| String::from_utf8_lossy(&request.body));
    let retried = retried.unwrap();
    assert!(retried.contains("Count the open invoices.") && !retried.contains("call_1"));
}

// Catches: reported usage (including cached input tokens) ignored when deciding
// to compact, or lost when a second Turn resumes from the database.
#[tokio::test]
async fn reported_usage_triggers_compaction_across_turns() {
    let lead = Lead::new().await;
    let tools = lead.tools_with_window(Some(48_000));
    let summary = json!({"role": "assistant", "content": SUMMARY});
    Mock::given(body_string_contains("max_completion_tokens"))
        .respond_with(completion(&summary))
        .with_priority(1)
        .mount(&lead.model)
        .await;
    // Two rounds totaling ~22,000 estimated tokens (below 48,000 - 16,384),
    // but Turn 1's final reply reports 32,000 actual tokens (cached + uncached + output).
    let older = tool_calls_message(&[tool_call("call_1", "search", &json!({"program": "old"}))]);
    let recent = tool_calls_message(&[tool_call(
        "call_2",
        "search",
        &json!({"program": "x".repeat(63_000)}),
    )]);
    Mock::given(any())
        .respond_with(completion_with_usage(&older, 1_000, 0, 500))
        .up_to_n_times(1)
        .mount(&lead.model)
        .await;
    Mock::given(any())
        .respond_with(completion_with_usage(&recent, 2_000, 0, 21_000))
        .up_to_n_times(1)
        .mount(&lead.model)
        .await;
    let first_answer = json!({"role": "assistant", "content": "first"});
    Mock::given(any())
        .respond_with(completion_with_usage(&first_answer, 30_000, 10_000, 2_000))
        .up_to_n_times(1)
        .mount(&lead.model)
        .await;
    lead.model_answers(reply(ANSWER)).await;

    let task = lead.task().await;
    let (sub_agent_id, waited) = lead.turn(&*tools, &task).await;
    assert_eq!(waited.get("answer"), Some(&json!("first")), "{waited}");
    assert_eq!(requests(&lead).await, ["step", "step", "step"]);

    let caller = lead.caller();
    let follow_up = json!({"sub_agent_id": sub_agent_id, "message": "Check again."});
    let sent = result(call(&*tools, &caller, "agent", follow_up).await);
    assert_eq!(sent.get("state"), Some(&json!("running")), "{sent}");
    let wait = json!({"sub_agent_id": sub_agent_id, "wait_ms": 30_000});
    let waited = result(call(&*tools, &caller, "agent", wait).await);
    assert_eq!(waited.get("answer"), Some(&json!(ANSWER)), "{waited}");
    assert_eq!(
        requests(&lead).await,
        ["step", "step", "step", "summary", "step"]
    );
    let received = lead.model.received_requests().await.unwrap();
    let last = String::from_utf8_lossy(&received.last().unwrap().body);
    assert!(last.contains("Count the open invoices.") && !last.contains("call_1"));
}

// Catches: a server that omits `usage` preventing pre-emptive compaction.
#[tokio::test]
async fn missing_usage_falls_back_to_byte_estimate() {
    let lead = Lead::new().await;
    let tools = lead.tools_with_window(Some(48_000));
    stub_steps(&lead, 3, false).await;

    let (_, waited) = lead.turn(&*tools, &lead.task().await).await;

    assert_eq!(waited.get("answer"), Some(&json!(ANSWER)), "{waited}");
    assert_eq!(
        requests(&lead).await,
        ["step", "step", "step", "summary", "step"]
    );
}

/// Stub that reports token usage at ~2.3 bytes per token (matching Claude on
/// JSON tool results) so end-to-end compaction tests exercise dense tokenizers.
struct DenseTokenizerStub {
    step: std::sync::atomic::AtomicUsize,
    calls: usize,
}

impl Respond for DenseTokenizerStub {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let prompt_tokens = i64::try_from(request.body.len() * 10 / 23).unwrap_or(i64::MAX);
        let body = String::from_utf8_lossy(&request.body);
        if body.contains("max_completion_tokens") {
            let summary = json!({"role": "assistant", "content": SUMMARY});
            return completion_with_usage(&summary, prompt_tokens, 0, 2_000);
        }
        let step = self.step.fetch_add(1, Ordering::Relaxed) + 1;
        let message = if step <= self.calls {
            let arguments = json!({"program": "x".repeat(12_000)});
            tool_calls_message(&[tool_call(&format!("call_{step}"), "search", &arguments)])
        } else {
            json!({"role": "assistant", "content": ANSWER})
        };
        let completion_tokens =
            i64::try_from(message.to_string().len() * 10 / 23).unwrap_or(i64::MAX);
        completion_with_usage(&message, prompt_tokens, 0, completion_tokens)
    }
}

// Catches: a dense tokenizer (~2.3 bytes/token) at a 48K window looping into
// compaction on every call because `cut` keeps too many real tokens.
#[tokio::test]
async fn dense_tokenizer_at_48k_does_not_compact_on_every_call() {
    let lead = Lead::new().await;
    let tools = lead.tools_with_window(Some(48_000));
    lead.model_answers(DenseTokenizerStub {
        step: std::sync::atomic::AtomicUsize::new(0),
        calls: 8,
    })
    .await;

    let (_, waited) = lead.turn(&*tools, &lead.task().await).await;

    assert_eq!(waited.get("answer"), Some(&json!(ANSWER)), "{waited}");
    let kinds = requests(&lead).await;
    let summaries = kinds.iter().filter(|&&kind| kind == "summary").count();
    assert!(summaries >= 1, "expected compaction to fire: {kinds:?}");
    assert!(
        !kinds
            .windows(3)
            .any(|w| w == ["summary", "step", "summary"]),
        "compacted on consecutive calls: {kinds:?}"
    );
}
