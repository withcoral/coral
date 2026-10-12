//! Proxy variables never reroute guarded requests. The request runs in a child
//! process, so its proxy environment cannot leak into other tests.
#![cfg(feature = "transport")]
#![allow(
    unused_crate_dependencies,
    reason = "integration tests inherit library dependencies"
)]

use coral_outbound::{Client, DestinationPolicy, NetworkAccess};
use std::time::Duration;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

const TARGET: &str = "CORAL_OUTBOUND_PROXY_TEST_TARGET";
const PROXIES: [&str; 6] = [
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
];

#[tokio::test]
#[expect(
    clippy::disallowed_methods,
    reason = "the child reads the target its parent set, as process bootstrap would"
)]
async fn proxy_environment_variables_are_ignored() {
    // The child side: send one request with the inherited environment.
    if let Ok(target) = std::env::var(TARGET) {
        let client = Client::new(
            DestinationPolicy::default().with_loopback(NetworkAccess::HttpAndHttps),
            Duration::from_secs(5),
        )
        .expect("client");
        let status = client
            .get(target)
            .expect("loopback is permitted")
            .send()
            .await
            .expect("response")
            .status();
        assert_eq!(status, 204);
        return;
    }

    let target = MockServer::start().await;
    let proxy = MockServer::start().await;
    for server in [&target, &proxy] {
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(204))
            .mount(server)
            .await;
    }
    let mut child = std::process::Command::new(std::env::current_exe().expect("test binary"));
    child
        .args([
            "--exact",
            "proxy_environment_variables_are_ignored",
            "--nocapture",
        ])
        .env(TARGET, format!("{}/data", target.uri()))
        .env_remove("NO_PROXY")
        .env_remove("no_proxy");
    for name in PROXIES {
        child.env(name, proxy.uri());
    }
    let output = tokio::task::spawn_blocking(move || child.output())
        .await
        .expect("child task")
        .expect("child process");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(target.received_requests().await.expect("requests").len(), 1);
    assert!(
        proxy
            .received_requests()
            .await
            .expect("requests")
            .is_empty(),
        "a guarded request went through the proxy"
    );
}
