use wiremock::Request;

use super::*;

const SUMMARY: &str = "## Goal\nCount the open invoices.";
const ANSWER: &str = "12 are open.";

/// The stub writes a summary, with reasoning, when asked for one. Otherwise
/// it makes `calls` failed searches of about 11,000 tokens each, then answers.
pub(super) async fn stub(lead: &Lead, calls: usize) {
    let summary = json!({"role": "assistant", "content": SUMMARY, "reasoning": "It is long."});
    Mock::given(body_string_contains("max_completion_tokens"))
        .respond_with(completion(&summary))
        .with_priority(1)
        .mount(&lead.model)
        .await;
    for call in 1..=calls {
        let arguments = json!({"program": "x".repeat(33_000)});
        let call = tool_call(&format!("call_{call}"), "search", &arguments);
        Mock::given(any())
            .respond_with(tool_calls(&[call]))
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
    let section = format!(
        "base_url = '{}'\nmodel = '{MODEL}'\ncontext_window = 48000",
        lead.base_url()
    );
    let factory = SubAgentFactory;
    let tools = factory.build(&toml::from_str(&section).unwrap(), lead.dependencies());
    stub(&lead, 3).await;

    let (sub_agent_id, waited) = lead.turn(&*tools.unwrap(), &lead.task().await).await;

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
