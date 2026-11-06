use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use tonic::Code;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

use super::*;
use crate::testing::{Answer, ScriptedCoral};

const TOKEN: &str = "test-run-executor-token";

/// Starts a Run Executor that holds eight Runs, until the sender drops.
fn start(coral: &ScriptedCoral, health: Option<TcpListener>) -> oneshot::Sender<()> {
    let config = RunExecutorConfig {
        coral_address: coral.address().parse().unwrap(),
        token: Token::new(TOKEN.to_owned()).unwrap(),
        max_runs_held: 8,
        run_limits: RunLimits::default(),
    };
    let (retire, retired) = oneshot::channel::<()>();
    tokio::spawn(run(config, health, async {
        drop(retired.await);
    }));
    retire
}

#[tokio::test]
async fn hello_carries_the_token_and_a_move_reopens_on_a_fresh_connection() {
    let mut coral = ScriptedCoral::start([
        Answer::Move,
        Answer::Move,
        Answer::Move,
        Answer::Welcome(PROTOCOL_LEVEL),
    ])
    .await;
    let _retire = start(&coral, None);

    let first = coral.next_call().await;
    let hello = first.hello.clone().unwrap();
    uuid::Uuid::parse_str(&hello.run_executor_id).unwrap();
    let limits = Limits {
        max_run_duration_seconds: 900,
        fixed: None,
    };
    assert_eq!(
        (hello.protocol_level, hello.free, hello.limits),
        (1, 8, Some(limits))
    );
    let mut previous = None;
    // Three moves, then the fourth stream Coral holds.
    let mut calls = vec![first];
    for _ in 0..3 {
        calls.push(coral.next_call().await);
    }
    for call in calls {
        assert_eq!(
            call.authorization.as_deref(),
            Some("Bearer test-run-executor-token")
        );
        assert_eq!(
            call.hello,
            Some(hello.clone()),
            "each reopen reports the room again"
        );
        assert_ne!(call.peer, previous, "each move dials a fresh connection");
        previous = call.peer;
    }
}

#[tokio::test]
async fn every_other_end_redials_with_the_backoff() {
    for answer in [
        Answer::Refuse(Code::Unavailable),
        Answer::Refuse(Code::Unauthenticated),
        Answer::Refuse(Code::FailedPrecondition),
        Answer::Welcome(PROTOCOL_LEVEL + 2),
        Answer::Close,
    ] {
        let mut coral = ScriptedCoral::start([answer; 4]).await;
        let _retire = start(&coral, None);
        let mut previous = coral.next_call().await.at;
        // Each wait is at least half its ceiling of 100, 200 and 400 ms.
        for floor in [50, 100, 200].map(Duration::from_millis) {
            let at = coral.next_call().await.at;
            assert!(at.duration_since(previous) >= floor, "{answer:?}");
            previous = at;
        }
    }
}

#[test]
fn the_backoff_doubles_to_five_seconds_skips_a_move_and_resets() {
    let mut backoff = Backoff::default();
    let ceilings = [100, 200, 400, 800, 1600, 3200, 5000, 5000].map(Duration::from_millis);
    for ceiling in ceilings {
        // A move reopens at once and leaves the next wait where it is.
        assert_eq!(backoff.after(&End::Moved), None);
        let delay = backoff.after(&End::Failed).unwrap();
        assert!(ceiling / 2 <= delay && delay <= ceiling, "{delay:?}");
    }
    backoff.reset();
    assert!(backoff.after(&End::Failed).unwrap() <= FIRST_BACKOFF);
}

#[test]
fn an_address_dials_only_http_with_a_host() {
    for (address, dials) in [
        ("http://coral:50052", true),
        ("http://127.0.0.1:50052", true),
        ("https://coral:50052", false),
        ("coral:50052", false),
        ("http:///path", false),
        ("not a url", false),
    ] {
        let parsed = address.parse::<CoralAddress>();
        assert_eq!(parsed.is_ok(), dials, "{address}");
        if let Err(error) = parsed {
            assert!(matches!(error, AddressError), "{address}");
        }
    }
}

#[test]
fn a_token_is_visible_ascii_up_to_16_kib() {
    for (token, accepted) in [
        ("x".repeat(16 * 1024), true),
        (String::new(), false),
        ("two words".to_owned(), false),
        ("line\nbreak".to_owned(), false),
        ("caf\u{e9}".to_owned(), false),
        ("x".repeat(16 * 1024 + 1), false),
    ] {
        let created = Token::new(token.clone());
        assert_eq!(created.is_ok(), accepted, "{token:.16}");
        if let Err(error) = created {
            assert!(matches!(error, TokenError), "{token:.16}");
        }
    }
}

#[tokio::test]
async fn readiness_serves_only_while_a_pull_stream_is_linked() {
    let mut coral = ScriptedCoral::start([
        Answer::Refuse(Code::Unavailable),
        Answer::Welcome(PROTOCOL_LEVEL),
    ])
    .await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let health = format!("http://{}", listener.local_addr().unwrap());
    let _retire = start(&coral, Some(listener));
    let mut health = HealthClient::new(Endpoint::from_shared(health).unwrap().connect_lazy());

    coral.next_call().await;
    wait_for(&mut health, ServingStatus::NotServing).await;
    coral.next_call().await;
    wait_for(&mut health, ServingStatus::Serving).await;
    coral.release();
    wait_for(&mut health, ServingStatus::NotServing).await;
}

async fn wait_for(health: &mut HealthClient<tonic::transport::Channel>, status: ServingStatus) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let request = HealthCheckRequest {
            service: READINESS_SERVICE.to_owned(),
        };
        if let Ok(response) = health.check(request).await
            && response.into_inner().status == status as i32
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "readiness never reached {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
