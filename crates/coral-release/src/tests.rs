use std::time::Duration;

use coral_outbound::{DestinationPolicy, NetworkAccess};
use semver::Version;
use serde_json::json;
use url::Url;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{CheckError, ReleaseCheck};

const FEED_PATH: &str = "/repos/withcoral/coral-releases/releases/latest";

fn check(server: &MockServer, current: &str, loopback: NetworkAccess) -> ReleaseCheck {
    let client = coral_outbound::Client::new(
        DestinationPolicy::new(loopback, NetworkAccess::Deny),
        Duration::from_secs(5),
    )
    .expect("client");
    let feed = Url::parse(&format!("{}{FEED_PATH}", server.uri())).expect("feed URL");
    ReleaseCheck::new(
        client,
        feed,
        Version::parse(current).expect("current version"),
    )
}

async fn answer(server: &MockServer, response: ResponseTemplate) {
    server.reset().await;
    Mock::given(method("GET"))
        .and(path(FEED_PATH))
        .respond_with(response)
        .mount(server)
        .await;
}

fn tag(tag: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({ "tag_name": tag, "assets": [] }))
}

#[tokio::test]
async fn names_a_stable_release_only_when_it_is_newer() {
    let server = MockServer::start().await;
    // (running, feed tag, newer release shown)
    let cases = [
        ("1.3.0", "v1.4.0", Some("1.4.0")),
        ("1.3.0", "v2.0.0", Some("2.0.0")),
        ("1.4.0-beta.1", "v1.4.0", Some("1.4.0")),
        ("1.3.0", "v1.3.0", None),
        ("1.4.0", "v1.3.0", None),
        ("1.3.0", "v1.10.0", Some("1.10.0")),
    ];
    for (current, feed, expected) in cases {
        answer(&server, tag(feed)).await;
        let check = check(&server, current, NetworkAccess::HttpAndHttps);
        check.refresh().await.expect("refresh");
        let newer = check.newer_release();
        assert_eq!(
            newer.as_ref().map(|release| release.version().to_string()),
            expected.map(str::to_owned),
            "{current} reading {feed}"
        );
        if let Some(release) = newer {
            assert_eq!(
                release.notes_url(),
                format!(
                    "https://github.com/withcoral/coral/releases/tag/v{}",
                    release.version()
                )
            );
        }
    }
}

#[tokio::test]
async fn a_feed_answer_without_a_stable_version_keeps_the_last_answer() {
    let server = MockServer::start().await;
    let check = check(&server, "1.3.0", NetworkAccess::HttpAndHttps);
    answer(&server, tag("v1.4.0")).await;
    check.refresh().await.expect("first refresh");

    let failures = [
        (tag("v1.5.0-beta.1"), "prerelease tag"),
        (tag("1.5.0"), "tag without v"),
        (tag("latest"), "not a version"),
        (
            ResponseTemplate::new(200).set_body_string("<html>"),
            "not JSON",
        ),
        (
            ResponseTemplate::new(200).set_body_json(
                json!({ "tag_name": "v1.5.0", "body": "x".repeat(super::BODY_LIMIT) }),
            ),
            "too large",
        ),
        (ResponseTemplate::new(403), "rate limited"),
        (ResponseTemplate::new(500), "server error"),
    ];
    for (response, case) in failures {
        answer(&server, response).await;
        let error = check.refresh().await.expect_err(case);
        assert!(
            matches!(
                (case, &error),
                (
                    "prerelease tag" | "tag without v" | "not a version",
                    CheckError::NotStable
                ) | ("not JSON" | "too large", CheckError::Unreadable)
                    | ("rate limited" | "server error", CheckError::Http(_))
            ),
            "{case}: {error:?}"
        );
        assert_eq!(
            check
                .newer_release()
                .map(|release| release.version().to_string()),
            Some("1.4.0".to_owned()),
            "{case} changed the answer"
        );
    }
}

#[tokio::test]
async fn the_destination_policy_decides_whether_the_feed_is_read() {
    let server = MockServer::start().await;
    answer(&server, tag("v9.0.0")).await;
    let check = check(&server, "1.3.0", NetworkAccess::Deny);

    let error = check.refresh().await.expect_err("denied");

    assert!(
        matches!(error, CheckError::DestinationRejected),
        "{error:?}"
    );
    assert!(check.newer_release().is_none());
    assert!(
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .is_empty()
    );
}
