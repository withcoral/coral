use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use coral_spec::backends::http::HttpTableSpec;
use datafusion::common::{DataFusionError, Result};
use serde_json::Value;

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
    max_concurrency: usize,
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
            max_concurrency,
            max_rows_per_binding: config.max_rows_per_binding,
            page_hint: config.page_hint,
        }
    }

    pub(crate) fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }

    pub(crate) async fn fetch_one(&self, tuple: Tuple) -> Result<(Tuple, Vec<Value>)> {
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
        let row_limit = self.max_rows_per_binding.checked_add(1);
        let rows = self
            .client
            .fetch_complete(
                &target,
                &filter_values,
                &HashMap::new(),
                row_limit,
                self.page_hint,
            )
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
