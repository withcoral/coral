//! The fixture's view of [`crate::testing`]: one served Run Module per
//! database at a time.
use crate::tests::*;

pub(crate) use crate::testing::Serving;

/// Serves `runs`'s Runs on a Run Executor in the test process.
pub(crate) async fn serve_runs(runs: RunModule) -> (RunModule, Serving) {
    crate::testing::serve_on_run_executor(runs).await
}

impl TestModules {
    /// Serves `runs`'s Runs for as long as this fixture lives, in place of
    /// any Run Module it served before, and returns it: two take loops on
    /// one database would take each other's Runs.
    pub(crate) async fn served(&self, runs: RunModule) -> RunModule {
        self.serving.lock().expect("the serving tasks").clear();
        let (runs, serving) = serve_runs(runs).await;
        self.serving
            .lock()
            .expect("the serving tasks")
            .push(serving);
        runs
    }

    /// Serves this fixture's own Run Module, configured as it is now.
    pub(crate) async fn serve(&mut self) {
        self.runs = self.served(self.runs.clone()).await;
    }

    /// Makes `runs` this fixture's Run Module, served in place of the one
    /// served before.
    pub(crate) async fn replace_runs(&mut self, runs: RunModule) {
        self.runs = self.served(runs).await;
    }
}
