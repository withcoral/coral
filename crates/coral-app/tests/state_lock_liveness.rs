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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tonic::Request;

use coral_api::v1::{CreateWorkspaceRequest, ExecuteSqlRequest};
use coral_app::{ServerBuilder, shutdown_tracing};
use coral_client::{AppClient, workspace};

const KUBELET_BUDGET: Duration = Duration::from_secs(1);
const HOLD: Duration = Duration::from_secs(2);

async fn probe_once(client: &AppClient) -> Duration {
    let start = Instant::now();
    client.check_engine_ready().await.expect("health rpc");
    start.elapsed()
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

    // External prober on its own runtime + connection (the kubelet).
    let endpoint = server.endpoint_uri().to_string();
    let stop = Arc::new(AtomicBool::new(false));
    let latencies: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::new()));
    let poller = {
        let stop = Arc::clone(&stop);
        let latencies = Arc::clone(&latencies);
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("prober runtime");
            rt.block_on(async move {
                let client = AppClient::connect(&endpoint).await.expect("prober connect");
                probe_once(&client).await; // warm the readiness cache
                while !stop.load(Ordering::Relaxed) {
                    let elapsed = probe_once(&client).await;
                    latencies.lock().unwrap().push(elapsed);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });
        })
    };
    std::thread::sleep(Duration::from_millis(500));

    // The slow exclusive holder: models a source install writing to EFS while
    // holding state_lock_exclusive. Same flock, held from an OS thread.
    let lock_path = config_dir.join(".lock");
    let holder = std::thread::spawn(move || {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .expect("open lock file");
        file.lock().expect("exclusive flock");
        std::thread::sleep(HOLD);
        drop(file);
    });
    std::thread::sleep(Duration::from_millis(200));

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

    let latencies = latencies.lock().unwrap();
    let max = latencies.iter().max().copied().unwrap_or_default();
    let over_budget = latencies.iter().filter(|l| **l > KUBELET_BUDGET).count();
    eprintln!(
        "queries: {:?}/{:?} in {query_elapsed:?}; probes={} max={max:?} over_1s={over_budget}",
        r1.as_ref().map(|_| "ok").map_err(|e| e.code()),
        r2.as_ref().map(|_| "ok").map_err(|e| e.code()),
        latencies.len(),
    );

    shutdown_tracing();
    server.shutdown().await.expect("shutdown");

    assert_eq!(
        over_budget, 0,
        "{over_budget} health probes exceeded the kubelet budget (max {max:?}) while the state lock \
         was contended — lock waiters starved the runtime"
    );
}
