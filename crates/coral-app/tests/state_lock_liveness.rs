//! Pins that a contended state lock cannot starve the runtime's liveness.
//!
//! The state lock is a blocking `flock(2)` acquired on runtime workers: every
//! query takes it shared (`load_query_sources`), and source installs or config
//! writes hold it exclusive across filesystem I/O — seconds at a time on
//! network storage. Before `FileLock::acquire` routed contended waits through
//! `block_in_place`, as many parked shared waiters as the runtime had workers
//! wedged the process whole: health endpoints, readiness, even `accept(2)`
//! stopped, and a liveness probe then killed a server that was only waiting.
//! Observed in production on a two-CPU host, where two concurrent queries
//! behind one slow exclusive holder were enough.
//!
//! The model here: an OS thread holds the lock file exclusively while two
//! queries run on a two-worker runtime; an external prober (its own thread,
//! runtime, and connection — the kubelet's view) must keep getting health
//! answers within the 1s budget a kubelet probe defaults to.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set."
)]

use std::fs::OpenOptions;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tonic::Request;

use coral_api::v1::{CreateWorkspaceRequest, ExecuteSqlRequest};
use coral_app::{ServerBuilder, shutdown_tracing};
use coral_client::{AppClient, workspace};

const KUBELET_BUDGET: Duration = Duration::from_secs(1);
const HOLD: Duration = Duration::from_secs(2);

/// Waits for an OS-thread signal without parking a runtime worker.
async fn recv_signal(rx: mpsc::Receiver<()>, what: &str) {
    tokio::task::spawn_blocking(move || rx.recv_timeout(Duration::from_secs(30)))
        .await
        .expect("join recv task")
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

async fn probe_once(client: &AppClient) -> Duration {
    let start = Instant::now();
    client.check_engine_ready().await.expect("health rpc");
    start.elapsed()
}

/// The kubelet's view: an OS thread with its own runtime and connection,
/// probing the health service every 100ms and recording each round trip.
fn spawn_kubelet_prober(
    endpoint: String,
    stop: Arc<AtomicBool>,
    latencies: Arc<Mutex<Vec<Duration>>>,
    warmed: mpsc::Sender<()>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("prober runtime");
        rt.block_on(async move {
            let client = AppClient::connect(&endpoint).await.expect("prober connect");
            probe_once(&client).await; // warm the readiness cache
            warmed.send(()).expect("signal prober warmed");
            while !stop.load(Ordering::Relaxed) {
                let elapsed = probe_once(&client).await;
                latencies.lock().expect("latencies mutex").push(elapsed);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
    })
}

/// The slow exclusive holder: models a source install writing through the
/// state lock on slow storage. Same flock, held from an OS thread. Sends on
/// `held` once the lock is actually acquired, so the test can sequence on the
/// lock itself rather than on wall-clock sleeps a loaded CI runner can miss.
fn hold_lock_exclusively(
    lock_path: std::path::PathBuf,
    held: mpsc::Sender<()>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .expect("open lock file");
        file.lock().expect("exclusive flock");
        held.send(()).expect("signal lock held");
        std::thread::sleep(HOLD);
        drop(file);
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_state_lock_must_not_starve_liveness() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    std::fs::create_dir_all(&config_dir).expect("create config dir");
    std::fs::write(config_dir.join("config.toml"), "version = 1\n").expect("write config");

    let server = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .start()
        .await
        .expect("start server");

    let app = AppClient::connect(server.endpoint_uri())
        .await
        .expect("connect client");
    let ws = workspace("lock-repro");
    app.workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(ws.clone()),
        }))
        .await
        .expect("create workspace");

    let stop = Arc::new(AtomicBool::new(false));
    let latencies: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::new()));
    let (warmed_tx, warmed_rx) = mpsc::channel();
    let poller = spawn_kubelet_prober(
        server.endpoint_uri().to_string(),
        Arc::clone(&stop),
        Arc::clone(&latencies),
        warmed_tx,
    );
    recv_signal(warmed_rx, "prober warm-up").await;

    let (held_tx, held_rx) = mpsc::channel();
    let holder = hold_lock_exclusively(config_dir.join(".lock"), held_tx);
    recv_signal(held_rx, "exclusive lock acquisition").await;

    // Two trivial queries. Each takes state_lock_shared on a runtime worker.
    let q = |sql: &str| {
        let mut client = app.query_client();
        let ws = ws.clone();
        let sql = sql.to_string();
        async move {
            client
                .execute_sql(Request::new(ExecuteSqlRequest {
                    workspace: Some(ws),
                    sql,
                    guide_read_context: None,
                    task_attribution: None,
                }))
                .await
        }
    };
    let started = Instant::now();
    let (r1, r2) = tokio::join!(q("SELECT 1"), q("SELECT 2"));
    let query_elapsed = started.elapsed();

    holder.join().expect("join holder");
    stop.store(true, Ordering::Relaxed);
    poller.join().expect("join poller");

    // Read the shared vec before the awaits below: a MutexGuard must not be
    // held across an await point (clippy::await_holding_lock).
    let (polls, max, over_budget) = {
        let latencies = latencies.lock().expect("latencies mutex");
        (
            latencies.len(),
            latencies.iter().max().copied().unwrap_or_default(),
            latencies.iter().filter(|l| **l > KUBELET_BUDGET).count(),
        )
    };
    r1.expect("query behind the contended lock should still succeed");
    r2.expect("query behind the contended lock should still succeed");

    shutdown_tracing();
    server.shutdown().await.expect("shutdown");

    assert!(
        query_elapsed >= Duration::from_millis(500),
        "queries finished in {query_elapsed:?} without waiting for the lock; the hold never contended"
    );
    assert!(
        polls > 3,
        "prober produced only {polls} samples; harness broken"
    );
    assert_eq!(
        over_budget, 0,
        "{over_budget} of {polls} health probes exceeded the kubelet budget (max {max:?}) while the \
         state lock was contended — lock waiters starved the runtime"
    );
}
