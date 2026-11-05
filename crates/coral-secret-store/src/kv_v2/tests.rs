use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use coral_outbound::{DestinationPolicy, NetworkAccess};
use serde_json::json;
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{AppRole, BODY_LIMIT, KvV2Settings, KvV2Store, MountPath, StoreAddress};
use crate::{Secret, SecretReference, SecretStore as _};

const LOGIN: &str = "/v1/auth/approle/login";
const TOKEN: &str = "store-token";

fn loopback_client() -> coral_outbound::Client {
    coral_outbound::Client::new(
        DestinationPolicy::new(NetworkAccess::HttpAndHttps, NetworkAccess::Deny),
        Duration::from_secs(5),
    )
    .unwrap()
}

fn store(server: &MockServer) -> KvV2Store {
    KvV2Store::new(loopback_client(), settings(&server.uri()))
}

fn settings(address: &str) -> KvV2Settings {
    KvV2Settings {
        address: StoreAddress::parse(address).unwrap(),
        mount: MountPath::parse("secret").unwrap(),
        auth: AppRole {
            mount: MountPath::parse("approle").unwrap(),
            role_id: "role".into(),
            secret_id: Secret::new("secret-id".into()).unwrap(),
        },
    }
}

fn reference(path: &str, key: Option<&str>) -> SecretReference {
    SecretReference::new("store".into(), path.into(), key.map(str::to_owned)).unwrap()
}

fn login_answer(lease_duration: u64, renewable: bool) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "auth": { "client_token": TOKEN, "lease_duration": lease_duration, "renewable": renewable }
    }))
}

async fn answer_login(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path(LOGIN))
        .respond_with(login_answer(3600, true))
        .mount(server)
        .await;
}

fn entry(data: &serde_json::Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "data": { "data": data, "metadata": { "version": 3 } }
    }))
}

#[tokio::test]
async fn logs_in_once_then_reads_the_latest_version_and_selects_the_key() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(LOGIN))
        .and(body_json(
            json!({ "role_id": "role", "secret_id": "secret-id" }),
        ))
        .respond_with(login_answer(3600, true))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/secret/data/team/jira"))
        .and(header("x-vault-token", TOKEN))
        .respond_with(entry(
            &json!({ "email": "a@example.test", "token": "value" }),
        ))
        .expect(2)
        .mount(&server)
        .await;
    let store = store(&server);
    let reference = reference("team/jira", Some("token"));

    assert_eq!(store.read(&reference).await.unwrap().expose(), "value");
    store.check(&reference).await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert!(
        requests.iter().all(|request| request.url.query().is_none()),
        "no version pin, so the store answers with the latest version"
    );
}

/// Each answer that yields no value is its own error, so a Workspace Owner
/// knows what to fix.
#[tokio::test]
async fn maps_each_failed_read_onto_a_distinct_port_error() {
    use crate::Error::{
        EntryNotFound, KeyNotFound, MalformedResponse, PermissionDenied, Unreachable,
    };
    let deleted = json!({ "data": { "data": null, "metadata": { "deletion_time": "2026-01-01T00:00:00Z" } } });
    for (answer, key, expected) in [
        (ResponseTemplate::new(403), Some("token"), PermissionDenied),
        (ResponseTemplate::new(404), Some("token"), EntryNotFound),
        (
            ResponseTemplate::new(404).set_body_json(&deleted),
            Some("token"),
            EntryNotFound,
        ),
        (
            ResponseTemplate::new(200).set_body_json(&deleted),
            Some("token"),
            EntryNotFound,
        ),
        (ResponseTemplate::new(503), Some("token"), Unreachable),
        (ResponseTemplate::new(429), Some("token"), Unreachable),
        (ResponseTemplate::new(307), Some("token"), Unreachable),
        (
            entry(&json!({ "other": "value" })),
            Some("token"),
            KeyNotFound,
        ),
        (entry(&json!({ "token": "value" })), None, KeyNotFound),
        (
            entry(&json!({ "token": 7 })),
            Some("token"),
            MalformedResponse,
        ),
        (
            entry(&json!({ "token": "" })),
            Some("token"),
            MalformedResponse,
        ),
        (
            entry(&json!({ "token": "a\nb" })),
            Some("token"),
            MalformedResponse,
        ),
        (
            ResponseTemplate::new(200).set_body_string("not json"),
            Some("token"),
            MalformedResponse,
        ),
        (
            ResponseTemplate::new(200).set_body_json(json!({ "data": {} })),
            Some("token"),
            MalformedResponse,
        ),
        (
            ResponseTemplate::new(200).set_body_json(json!({ "data": [] })),
            Some("token"),
            MalformedResponse,
        ),
        (ResponseTemplate::new(400), Some("token"), MalformedResponse),
        (
            ResponseTemplate::new(200).set_body_bytes(vec![b' '; BODY_LIMIT + 1]),
            Some("token"),
            MalformedResponse,
        ),
    ] {
        let server = MockServer::start().await;
        answer_login(&server).await;
        Mock::given(method("GET"))
            .respond_with(answer)
            .mount(&server)
            .await;
        let read = store(&server).read(&reference("team/jira", key)).await;
        assert_eq!(read.unwrap_err(), expected, "{key:?}");
    }
}

/// A store that refuses Coral's identity differs from one that is down.
#[tokio::test]
async fn maps_each_failed_login_onto_a_distinct_port_error() {
    use crate::Error::{MalformedResponse, PermissionDenied, Unreachable};
    for (answer, expected) in [
        (ResponseTemplate::new(400), PermissionDenied),
        (ResponseTemplate::new(403), PermissionDenied),
        (ResponseTemplate::new(503), Unreachable),
        (ResponseTemplate::new(429), Unreachable),
        (
            ResponseTemplate::new(200).set_body_string("not json"),
            MalformedResponse,
        ),
        (
            ResponseTemplate::new(200).set_body_json(json!({ "auth": { "client_token": "" } })),
            MalformedResponse,
        ),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(LOGIN))
            .respond_with(answer)
            .mount(&server)
            .await;
        let read = store(&server)
            .read(&reference("team/jira", Some("token")))
            .await;
        assert_eq!(read.unwrap_err(), expected);
    }
}

#[tokio::test]
async fn a_store_nobody_answers_for_is_unreachable() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let store = KvV2Store::new(loopback_client(), settings(&address));
    assert_eq!(
        store
            .read(&reference("team/jira", Some("token")))
            .await
            .unwrap_err(),
        crate::Error::Unreachable
    );
}

/// All store traffic obeys the operator's destination policy.
#[tokio::test]
async fn a_destination_the_policy_rejects_is_unreachable_and_sends_nothing() {
    let server = MockServer::start().await;
    let client = coral_outbound::Client::new(
        DestinationPolicy::new(NetworkAccess::Deny, NetworkAccess::Deny),
        Duration::from_secs(5),
    )
    .unwrap();
    let store = KvV2Store::new(client, settings(&server.uri()));
    assert_eq!(
        store
            .read(&reference("team/jira", Some("token")))
            .await
            .unwrap_err(),
        crate::Error::Unreachable
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

/// A reference is a Workspace Owner's text. It cannot climb out of the KV mount
/// to reach the store's other APIs, and it sends nothing when it tries.
#[tokio::test]
async fn a_path_cannot_leave_the_mount() {
    for path in [
        "../../sys/seal",
        "team/../../x",
        "./team",
        "team//jira",
        "/team",
        "team/",
    ] {
        let server = MockServer::start().await;
        let read = store(&server).read(&reference(path, Some("token"))).await;
        assert_eq!(read.unwrap_err(), crate::Error::EntryNotFound, "{path}");
        assert!(
            server.received_requests().await.unwrap().is_empty(),
            "{path}"
        );
    }
}

/// Text that looks like URL syntax stays one literal path segment.
#[tokio::test]
async fn a_path_is_percent_encoded_segment_by_segment() {
    for (path, sent) in [
        ("%2e%2e/sys", "/v1/secret/data/%252e%252e/sys"),
        ("a%2Fb", "/v1/secret/data/a%252Fb"),
        ("a b?c#d", "/v1/secret/data/a%20b%3Fc%23d"),
        ("a\\..\\b", "/v1/secret/data/a%5C..%5Cb"),
    ] {
        let server = MockServer::start().await;
        answer_login(&server).await;
        Mock::given(method("GET"))
            .respond_with(entry(&json!({ "token": "value" })))
            .mount(&server)
            .await;
        store(&server)
            .read(&reference(path, Some("token")))
            .await
            .unwrap();
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.last().unwrap().url.path(), sent);
    }
}

#[test]
fn settings_refuse_what_coral_should_not_send_a_token_to() {
    for address in [
        "http://vault.internal:8200",
        "ftp://localhost",
        "https://user:pass@vault.example.test",
        "https://vault.example.test/prefix",
        "https://vault.example.test?x=1",
        "not a url",
    ] {
        assert!(StoreAddress::parse(address).is_err(), "{address}");
    }
    for address in ["https://vault.example.test:8200/", "http://127.0.0.1:8200"] {
        assert!(StoreAddress::parse(address).is_ok(), "{address}");
    }
    for mount in ["", "/", "a/", "/a", "a//b", "..", "a/../b"] {
        assert!(MountPath::parse(mount).is_err(), "{mount}");
    }
    for mount in ["secret", "team/kv"] {
        assert!(MountPath::parse(mount).is_ok(), "{mount}");
    }
}

/// Moves on demand, so a test can pass a lease without waiting for it.
#[derive(Clone)]
struct TestClock(Arc<Mutex<Instant>>);

impl TestClock {
    fn advance(&self, by: Duration) {
        *self.0.lock().unwrap() += by;
    }
}

fn store_at(server: &MockServer) -> (KvV2Store, TestClock) {
    let clock = TestClock(Arc::new(Mutex::new(Instant::now())));
    let reader = clock.clone();
    let store = KvV2Store::with_clock(
        loopback_client(),
        settings(&server.uri()),
        Arc::new(move || *reader.0.lock().unwrap()),
    );
    (store, clock)
}

fn count(requests: &[wiremock::Request], path: &str) -> usize {
    requests.iter().filter(|r| r.url.path() == path).count()
}

const RENEW: &str = "/v1/auth/token/renew-self";

/// (lease, renewable, time passed, renew answers, logins, renewals)
#[tokio::test]
async fn keeps_its_token_fresh_as_the_lease_passes() {
    let renewed = login_answer(3600, true);
    let refused = ResponseTemplate::new(403);
    let down = ResponseTemplate::new(503);
    for (name, lease, renewable, passed, renew, logins, renewals) in [
        ("early in the lease", 3600, true, 1799, &renewed, 1, 0),
        ("past half of the lease", 3600, true, 1800, &renewed, 1, 1),
        ("a refused renewal", 3600, true, 1800, &refused, 2, 1),
        (
            "a renewal the store cannot answer",
            3600,
            true,
            1800,
            &down,
            1,
            1,
        ),
        (
            "near the end of the lease",
            3600,
            true,
            3240,
            &renewed,
            2,
            0,
        ),
        (
            "not renewable, past half",
            3600,
            false,
            1800,
            &renewed,
            1,
            0,
        ),
        (
            "not renewable, near the end",
            3600,
            false,
            3240,
            &renewed,
            2,
            0,
        ),
        ("a lease that never ends", 0, true, 1 << 40, &renewed, 1, 0),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(LOGIN))
            .respond_with(login_answer(lease, renewable))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(RENEW))
            .and(header("x-vault-token", TOKEN))
            .respond_with(renew.clone())
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(entry(&json!({ "token": "value" })))
            .mount(&server)
            .await;
        let (store, clock) = store_at(&server);
        let reference = reference("team/jira", Some("token"));

        store.read(&reference).await.unwrap();
        clock.advance(Duration::from_secs(passed));
        store.read(&reference).await.expect(name);

        let requests = server.received_requests().await.unwrap();
        assert_eq!(count(&requests, LOGIN), logins, "{name}: logins");
        assert_eq!(count(&requests, RENEW), renewals, "{name}: renewals");
    }
}

/// A renewal restarts the lease, so the next read needs neither step.
#[tokio::test]
async fn a_renewal_restarts_the_lease() {
    let server = MockServer::start().await;
    answer_login(&server).await;
    Mock::given(method("POST"))
        .and(path(RENEW))
        .respond_with(login_answer(3600, true))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(entry(&json!({ "token": "value" })))
        .mount(&server)
        .await;
    let (store, clock) = store_at(&server);
    let reference = reference("team/jira", Some("token"));
    store.read(&reference).await.unwrap();
    clock.advance(Duration::from_mins(30));
    store.read(&reference).await.unwrap();
    clock.advance(Duration::from_secs(1000));
    store.read(&reference).await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(count(&requests, LOGIN), 1);
    assert_eq!(count(&requests, RENEW), 1);
}

const LOOKUP: &str = "/v1/auth/token/lookup-self";

/// A policy denial and a revoked token both answer 403. Only a revoked token is
/// worth a login, and a login can use up a limited secret ID, so Coral asks the
/// store which it is. A token straight from login needs no question.
#[tokio::test]
async fn a_denied_read_signs_in_again_only_when_the_token_is_gone() {
    const ENTRY: &str = "/v1/secret/data/team/jira";
    use crate::Error::{PermissionDenied, Unreachable};
    // (lookup status, second read's error, logins, entry reads, lookups)
    for (lookup, expected, logins, reads, lookups) in [
        (200, PermissionDenied, 1, 2, 1),
        (403, PermissionDenied, 2, 3, 1),
        (503, Unreachable, 1, 2, 1),
    ] {
        let server = MockServer::start().await;
        answer_login(&server).await;
        Mock::given(method("GET"))
            .and(path(LOOKUP))
            .and(header("x-vault-token", TOKEN))
            .respond_with(ResponseTemplate::new(lookup))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(ENTRY))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        let store = store(&server);
        let reference = reference("team/jira", Some("token"));

        let first = store.read(&reference).await.unwrap_err();
        assert_eq!(first, PermissionDenied);
        let requests = server.received_requests().await.unwrap();
        assert_eq!(count(&requests, LOOKUP), 0, "a new token needs no question");

        let second = store.read(&reference).await.unwrap_err();
        assert_eq!(second, expected, "lookup {lookup}");
        let requests = server.received_requests().await.unwrap();
        assert_eq!(
            (
                count(&requests, LOGIN),
                count(&requests, ENTRY),
                count(&requests, LOOKUP)
            ),
            (logins, reads, lookups),
            "lookup {lookup}"
        );
    }
}

#[tokio::test]
async fn a_revoked_token_recovers_on_the_same_read() {
    let server = MockServer::start().await;
    answer_login(&server).await;
    Mock::given(method("GET"))
        .and(path(LOOKUP))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(entry(&json!({ "token": "value" })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(403))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(entry(&json!({ "token": "after" })))
        .mount(&server)
        .await;
    let store = store(&server);
    let reference = reference("team/jira", Some("token"));
    store.read(&reference).await.unwrap();
    assert_eq!(store.read(&reference).await.unwrap().expose(), "after");
}

/// Reads wait their turn for a login. When the store hangs, each must not then
/// wait out its own timeout, so they share a failure seconds old.
#[tokio::test]
async fn reads_share_a_recent_login_failure_then_try_again() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(LOGIN))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let (store, clock) = store_at(&server);
    let reference = reference("team/jira", Some("token"));
    let logins = async || count(&server.received_requests().await.unwrap(), LOGIN);

    for _ in 0..2 {
        let read = store.read(&reference).await.unwrap_err();
        assert_eq!(read, crate::Error::Unreachable);
    }
    assert_eq!(logins().await, 1);

    clock.advance(super::LOGIN_RETRY_AFTER);
    assert_eq!(
        store.read(&reference).await.unwrap_err(),
        crate::Error::Unreachable
    );
    assert_eq!(logins().await, 2);
}

/// A token revoked after a renewal is still found and replaced.
#[tokio::test]
async fn a_renewed_token_that_is_revoked_is_replaced() {
    let server = MockServer::start().await;
    answer_login(&server).await;
    Mock::given(method("POST"))
        .and(path(RENEW))
        .respond_with(login_answer(3600, true))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(LOOKUP))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(entry(&json!({ "token": "value" })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(403))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(entry(&json!({ "token": "value" })))
        .mount(&server)
        .await;
    let (store, clock) = store_at(&server);
    let reference = reference("team/jira", Some("token"));

    store.read(&reference).await.unwrap();
    // Past half of the lease: renews, the store denies, the token is gone.
    clock.advance(Duration::from_mins(30));
    assert_eq!(store.read(&reference).await.unwrap().expose(), "value");

    let requests = server.received_requests().await.unwrap();
    assert_eq!(count(&requests, LOGIN), 2);
}
