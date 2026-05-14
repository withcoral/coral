use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use coral_spec::backends::http::HttpTableSpec;
use datafusion::common::{DataFusionError, Result};
use serde_json::Value;
use tokio::sync::{Semaphore, mpsc};

use crate::backends::http::HttpSourceClient;
use crate::backends::http::target::HttpFetchTarget;
use crate::runtime::dependent_join::bindings::Tuple;
use crate::runtime::dependent_join::error::DependentJoinError;

#[derive(Clone)]
pub(crate) struct BindingFetcher {
    client: HttpSourceClient,
    source_schema: Arc<str>,
    table: Arc<HttpTableSpec>,
    binding_filters: Arc<[String]>,
    literal_filters: Arc<BTreeMap<String, String>>,
    semaphore: Arc<Semaphore>,
    result_channel_capacity: usize,
    max_rows_per_binding: usize,
    page_hint: Option<usize>,
}

pub(crate) struct BindingFetcherConfig {
    pub(crate) client: HttpSourceClient,
    pub(crate) source_schema: String,
    pub(crate) table: Arc<HttpTableSpec>,
    pub(crate) binding_filters: Arc<[String]>,
    pub(crate) literal_filters: Arc<BTreeMap<String, String>>,
    pub(crate) max_concurrency: usize,
    pub(crate) max_rows_per_binding: usize,
    pub(crate) page_hint: Option<usize>,
}

impl BindingFetcher {
    pub(crate) fn new(config: BindingFetcherConfig) -> Self {
        let max_concurrency = config.max_concurrency.max(1);

        Self {
            client: config.client,
            source_schema: Arc::from(config.source_schema),
            table: config.table,
            binding_filters: config.binding_filters,
            literal_filters: config.literal_filters,
            semaphore: Arc::new(Semaphore::new(max_concurrency)),
            result_channel_capacity: max_concurrency,
            max_rows_per_binding: config.max_rows_per_binding,
            page_hint: config.page_hint,
        }
    }

    pub(crate) fn dispatch(
        &self,
        tuples_rx: mpsc::Receiver<Tuple>,
    ) -> mpsc::Receiver<Result<(Tuple, Vec<Value>)>> {
        let (results_tx, results_rx) = mpsc::channel(self.result_channel_capacity);
        let fetcher = self.clone();
        let cancellation = Arc::new(AtomicBool::new(false));

        tokio::spawn(async move {
            fetcher
                .run_dispatch_loop(tuples_rx, results_tx, cancellation)
                .await;
        });

        results_rx
    }

    async fn run_dispatch_loop(
        self,
        mut tuples_rx: mpsc::Receiver<Tuple>,
        results_tx: mpsc::Sender<Result<(Tuple, Vec<Value>)>>,
        cancellation: Arc<AtomicBool>,
    ) {
        while let Some(tuple) = tuples_rx.recv().await {
            if cancellation.load(Ordering::Acquire) {
                break;
            }

            let Ok(permit) = self.semaphore.clone().acquire_owned().await else {
                break;
            };

            if cancellation.load(Ordering::Acquire) {
                break;
            }

            let worker = self.clone();
            let tx = results_tx.clone();
            let token = Arc::clone(&cancellation);

            tokio::spawn(async move {
                let _permit = permit;
                let result = worker.fetch_one(tuple).await;
                if result.is_err() {
                    token.store(true, Ordering::Release);
                }
                if tx.send(result).await.is_err() {
                    // Receiver dropped: query execution no longer needs this result.
                }
            });
        }
    }

    async fn fetch_one(&self, tuple: Tuple) -> Result<(Tuple, Vec<Value>)> {
        let filters = build_filters(
            self.literal_filters.as_ref(),
            self.binding_filters.as_ref(),
            &tuple,
        )?;
        let filter_values = filters
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let target = http_target_for_filters(&self.table, &filter_values);
        let rows = self
            .client
            .fetch(&target, &filter_values, &HashMap::new(), self.page_hint)
            .await?;

        if rows.len() > self.max_rows_per_binding {
            return Err(DependentJoinError::RowsPerBinding {
                source_schema: self.source_schema.to_string(),
                table: self.table.name().to_string(),
                observed: rows.len(),
                cap: self.max_rows_per_binding,
            }
            .into_datafusion());
        }

        Ok((tuple, rows))
    }
}

fn build_filters(
    literal_filters: &BTreeMap<String, String>,
    binding_filters: &[String],
    tuple: &Tuple,
) -> Result<BTreeMap<String, String>> {
    if binding_filters.len() != tuple.values().len() {
        return Err(DataFusionError::Internal(format!(
            "dependent join binding arity mismatch: {} filters for {} values",
            binding_filters.len(),
            tuple.values().len()
        )));
    }

    let mut filters = literal_filters.clone();

    for (filter_name, value) in binding_filters.iter().zip(tuple.values()) {
        if filters.contains_key(filter_name) {
            return Err(DataFusionError::Internal(format!(
                "dependent join over-constrained filter '{filter_name}'"
            )));
        }

        filters.insert(filter_name.clone(), value.to_wire_string());
    }

    Ok(filters)
}

fn http_target_for_filters(
    table: &HttpTableSpec,
    filter_values: &HashMap<String, String>,
) -> HttpFetchTarget {
    let filter_keys: HashSet<String> = filter_values.keys().cloned().collect();
    let active_request = table.resolve_request(&filter_keys).clone();
    HttpFetchTarget::from_resolved_table_request(table, active_request)
}
