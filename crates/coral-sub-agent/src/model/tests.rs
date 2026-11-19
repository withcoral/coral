use std::time::Duration;

use coral_credentials::Secret;
use coral_outbound::{DestinationPolicy, NetworkAccess};
use reqwest::StatusCode;
use serde_json::json;
use url::Url;
use wiremock::matchers::{any, body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{Message, ModelClient, ModelError, Role};

const MODEL: &str = "test-model";
const KEY: &str = "model-key";

fn client(server: &MockServer, api_key: Option<&str>) -> ModelClient {
    let http = coral_outbound::Client::new(
        DestinationPolicy::new(NetworkAccess::HttpAndHttps, NetworkAccess::Deny),
        Duration::from_secs(5),
    )
    .unwrap();
    let base_url = Url::parse(&format!("{}/v1", server.uri())).unwrap();
    let api_key = api_key.map(|key| Secret::new(key.to_owned()).unwrap());
    ModelClient {
        retry_waits: [Duration::ZERO; 3],
        ..ModelClient::new(http, &base_url, MODEL.to_owned(), api_key)
    }
}

fn conversation() -> Vec<Message> {
    [
        (Role::System, "Be brief."),
        (Role::User, "Hello?"),
        (Role::Assistant, "Hi."),
        (Role::User, "Again?"),
    ]
    .map(|(role, text)| Message {
        role,
        text: text.to_owned(),
    })
    .into()
}

fn answer(text: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "choices": [{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
    }))
}

async fn received(server: &MockServer) -> Vec<wiremock::Request> {
    server.received_requests().await.unwrap()
}

// Catches: a request that is not Chat Completions, or a reply read from the wrong field.
#[tokio::test]
async fn reply_is_the_text_of_the_first_choice() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_json(json!({
            "model": MODEL,
            "messages": [
                {"role": "system", "content": "Be brief."},
                {"role": "user", "content": "Hello?"},
                {"role": "assistant", "content": "Hi."},
                {"role": "user", "content": "Again?"},
            ],
        })))
        .respond_with(answer("Once more."))
        .mount(&server)
        .await;

    let reply = client(&server, None).reply(&conversation()).await;

    assert_eq!(reply, Ok("Once more.".to_owned()));
}

// Catches: a key not sent as a bearer token, or an Authorization header sent without one.
#[tokio::test]
async fn bearer_header_is_sent_only_with_a_key() {
    for (api_key, expected) in [(Some(KEY), Some(format!("Bearer {KEY}"))), (None, None)] {
        let server = MockServer::start().await;
        Mock::given(any())
            .respond_with(answer("Hi."))
            .mount(&server)
            .await;

        client(&server, api_key)
            .reply(&conversation())
            .await
            .unwrap();

        let sent: Vec<_> = received(&server)
            .await
            .iter()
            .map(|request| {
                request
                    .headers
                    .get("authorization")
                    .map(|value| value.to_str().unwrap().to_owned())
            })
            .collect();
        assert_eq!(sent, [expected]);
    }
}

// Catches: a busy or failing server not retried, or retried more than three times.
#[tokio::test]
async fn busy_or_failing_server_is_retried_three_times() {
    for (status, failures, requests, expected) in [
        (429, 1, 2, Ok("Hi.".to_owned())),
        (500, 3, 4, Ok("Hi.".to_owned())),
        (503, 4, 4, Err(ModelError::Unavailable)),
    ] {
        let server = MockServer::start().await;
        Mock::given(any())
            .respond_with(ResponseTemplate::new(status))
            .up_to_n_times(failures)
            .mount(&server)
            .await;
        Mock::given(any())
            .respond_with(answer("Hi."))
            .mount(&server)
            .await;

        let reply = client(&server, None).reply(&conversation()).await;

        assert_eq!(reply, expected, "status {status}");
        assert_eq!(received(&server).await.len(), requests);
    }
}

// Catches: a refusal or an unreadable answer retried, or an error's text holding the answer or the key.
#[tokio::test]
async fn refusal_and_unreadable_answer_end_at_once_without_the_answer_or_key() {
    const BODY: &str = "the server's own detail";
    for (answer, expected) in [
        (
            ResponseTemplate::new(400).set_body_string(BODY),
            ModelError::Refused(StatusCode::BAD_REQUEST),
        ),
        (
            ResponseTemplate::new(200).set_body_string(BODY),
            ModelError::Misconfigured,
        ),
    ] {
        let server = MockServer::start().await;
        Mock::given(any()).respond_with(answer).mount(&server).await;

        let error = client(&server, Some(KEY))
            .reply(&conversation())
            .await
            .unwrap_err();

        assert_eq!(error, expected);
        assert_eq!(received(&server).await.len(), 1);
        let text = error.to_string();
        assert!(!text.contains(BODY) && !text.contains(KEY), "{text}");
    }
}
