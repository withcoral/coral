use datafusion::common::{DataFusionError, Result};
use serde_json::Value;
use tokio::task::JoinSet;
use tracing::Instrument as _;

use crate::runtime::dependent_join::bindings::Tuple;
use crate::runtime::dependent_join::fetcher::BindingFetcher;
use crate::runtime::dependent_join::state::DependentJoinRuntimeState;

pub(crate) async fn run_binding_phase(
    mut state: DependentJoinRuntimeState,
    tuples: Vec<Tuple>,
    fetcher: &BindingFetcher,
) -> Result<DependentJoinRuntimeState> {
    let mut tuples = tuples.into_iter();
    let mut tasks = JoinSet::new();
    let max_concurrency = fetcher.max_concurrency();

    while tasks.len() < max_concurrency {
        let Some(tuple) = tuples.next() else {
            break;
        };
        spawn_fetch(&mut tasks, fetcher.clone(), tuple);
    }

    while let Some(result) = tasks.join_next().await {
        let (tuple, rows) = result.map_err(|error| join_error(&error))??;
        state.buffer_fetch_result(tuple, rows);

        if let Some(tuple) = tuples.next() {
            spawn_fetch(&mut tasks, fetcher.clone(), tuple);
        }
    }

    Ok(state)
}

fn spawn_fetch(
    tasks: &mut JoinSet<Result<(Tuple, Vec<Value>)>>,
    fetcher: BindingFetcher,
    tuple: Tuple,
) {
    tasks.spawn(async move { fetcher.fetch_one(tuple).await }.instrument(tracing::Span::current()));
}

fn join_error(error: &tokio::task::JoinError) -> DataFusionError {
    DataFusionError::Execution(format!("dependent join fetch task failed: {error}"))
}
