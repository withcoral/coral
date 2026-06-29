use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use coral_spec::backends::http::HttpTableSpec;
use datafusion::common::Result;
use serde_json::Value;

use crate::backends::http::HttpSourceClient;
use crate::backends::http::target::HttpFetchTarget;
use crate::runtime::dependent_join::bindings::{Tuple, filter_values_for_tuple};
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
        let filter_values = filter_values_for_tuple(
            self.literal_filters.as_ref(),
            self.binding_filters.as_ref(),
            &tuple,
        )?;
        let target = http_target_for_filters(&self.table, &filter_values);
        let row_limit = dependent_row_limit(self.max_rows_per_binding, self.page_hint);
        let rows = self
            .client
            .fetch_complete(
                &target,
                &filter_values,
                &HashMap::new(),
                row_limit,
                row_limit,
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

fn dependent_row_limit(max_rows_per_binding: usize, page_hint: Option<usize>) -> Option<usize> {
    match (max_rows_per_binding.checked_add(1), page_hint) {
        (Some(cap_probe_limit), Some(page_hint)) => Some(cap_probe_limit.min(page_hint)),
        (Some(cap_probe_limit), None) => Some(cap_probe_limit),
        (None, Some(page_hint)) => Some(page_hint),
        (None, None) => None,
    }
}

fn http_target_for_filters(
    table: &HttpTableSpec,
    filter_values: &HashMap<String, String>,
) -> HttpFetchTarget {
    let filter_keys: HashSet<String> = filter_values.keys().cloned().collect();
    let active_request = table.resolve_request(&filter_keys).clone();
    HttpFetchTarget::from_resolved_table_request(table, active_request)
}
