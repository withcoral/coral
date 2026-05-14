use arrow::array::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use tokio::sync::mpsc;

use crate::runtime::dependent_join::bindings::BindingProjector;
use crate::runtime::dependent_join::fetcher::BindingFetcher;
use crate::runtime::dependent_join::state::{DependentJoinRuntimeState, ResolverCaps};

pub(crate) async fn run_binding_phase(
    resolver_batches: Vec<RecordBatch>,
    projector: &BindingProjector,
    fetcher: &BindingFetcher,
    caps: &ResolverCaps,
) -> Result<DependentJoinRuntimeState> {
    let (tuples_tx, tuples_rx) = mpsc::channel(caps.max_bindings.max(1));
    let mut results_rx = fetcher.dispatch(tuples_rx);
    let mut state = DependentJoinRuntimeState::default();

    for batch in resolver_batches {
        for tuple in state.ingest_resolver_batch(&batch, projector, caps)? {
            tuples_tx.send(tuple).await.map_err(|_error| {
                DataFusionError::Execution("dependent join tuple channel closed".to_string())
            })?;
        }
    }

    drop(tuples_tx);

    while let Some(result) = results_rx.recv().await {
        let (tuple, rows) = result?;
        state.buffer_fetch_result(tuple, rows);
    }

    Ok(state)
}
