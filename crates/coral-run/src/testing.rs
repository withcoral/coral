//! Serves a Run Module's Runs as production does, for tests: a link that
//! serves Run streams, a take loop, and a Run Executor in the test process
//! that dials the link (ADR 0027: the in-process V8 engine stays only as a
//! test engine). The Coral process itself starts no V8 isolate.

use crate::RunModule;

/// How many Runs the test's Run Executor holds at once: room for the load
/// harness's concurrent cells.
const ROOM: u32 = 128;

/// The tasks that serve one Run Module's Runs. Dropping it aborts them;
/// [`Self::stop`] also waits until they have ended.
pub struct Serving {
    tasks: Vec<tokio::task::JoinHandle<()>>,
    _retire: tokio::sync::oneshot::Sender<()>,
}

impl Serving {
    /// Stops the tasks and waits until each has ended, so that no take loop
    /// of this Serving takes a Run afterwards.
    pub async fn stop(mut self) {
        for task in std::mem::take(&mut self.tasks) {
            task.abort();
            drop(task.await);
        }
    }
}

impl Drop for Serving {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// Serves `runs`'s Runs on a Run Executor in the test process, and returns
/// the module that serves them. Configure `runs` fully first: the take loop
/// and the link hold this clone. Serve one Run Module per database at a
/// time: two take loops take each other's Runs.
///
/// # Panics
///
/// When the loopback link cannot bind.
pub async fn serve_on_run_executor(runs: RunModule) -> (RunModule, Serving) {
    serve_on_run_executor_within(runs, coral_run_executor::RunLimits::default()).await
}

/// [`serve_on_run_executor`], with a Run Executor whose limits are `limits`.
///
/// # Panics
///
/// When the loopback link cannot bind.
pub async fn serve_on_run_executor_within(
    runs: RunModule,
    limits: coral_run_executor::RunLimits,
) -> (RunModule, Serving) {
    let link = coral_run_executor::LinkServer::new(
        coral_run_executor::Credentials::new([coral_run_executor::Token::new(
            coral_run_executor::testing::RUN_EXECUTOR_TOKEN.to_owned(),
        )
        .expect("a token")]),
        runs.database.clone(),
        runs.instance.as_str().to_owned(),
    )
    .with_run_streams(runs.run_stream_server());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the link");
    let address = format!("http://{}", listener.local_addr().expect("its address"));
    let streams = link.streams();
    let serving =
        tokio::spawn(async move { drop(link.serve(listener, std::future::pending()).await) });
    let taker = runs.clone();
    let taking = tokio::spawn(async move {
        let _never = taker.take_runs(streams, None).await;
    });
    let (retire, executor) =
        coral_run_executor::testing::spawn_run_executor_at(&address, ROOM, limits, None);
    let executor = tokio::spawn(async move { drop(executor.await) });
    (
        runs,
        Serving {
            tasks: vec![serving, taking, executor],
            _retire: retire,
        },
    )
}
