//! Benchmark needle planting: unions synthetic rows with live provider data.
//!
//! This implements a "needle in a haystack" evaluation pattern: a benchmark
//! harness writes a YAML file of synthetic rows, and when `CORAL_NEEDLES_FILE`
//! is set the engine converts matching entries to Arrow batches at source
//! registration time, wrapping each affected table provider with
//! [`provider::NeedleTableProvider`].

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};

use crate::backends::BackendRegistration;

pub(crate) mod error;
pub(crate) mod loader;
pub(crate) mod provider;
pub(crate) mod tracker;

use error::NeedleError;
use loader::{LoadedNeedles, NeedleGroups};
use provider::{NeedleTableProvider, build_needle_batches};
pub(crate) use tracker::NeedleTracker;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NeedleSpec {
    pub(crate) schema: String,
    pub(crate) table: String,
    pub(crate) column_values: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Default)]
pub(crate) struct NeedleState {
    groups: NeedleGroups,
    tracker_specs: Vec<NeedleSpec>,
    log_path: Option<PathBuf>,
}

impl NeedleState {
    pub(crate) fn from_path(path: Option<&Path>) -> Result<Self> {
        match path {
            Some(path) => {
                let LoadedNeedles { groups, specs } =
                    loader::load_needles(path).map_err(DataFusionError::from)?;
                Ok(Self {
                    groups,
                    tracker_specs: specs,
                    log_path: Some(log_path_for_needles(path)),
                })
            }
            None => Ok(Self::default()),
        }
    }

    pub(crate) fn decorate(
        &mut self,
        schema_name: &str,
        mut registration: BackendRegistration,
    ) -> Result<BackendRegistration> {
        registration.tables = self.wrap_tables(schema_name, registration.tables)?;
        Ok(registration)
    }

    pub(crate) fn source_registration_error(
        &self,
        schema_name: &str,
        detail: &impl std::fmt::Display,
    ) -> Option<DataFusionError> {
        let tables = tracked_table_names(&self.tracker_specs, schema_name);
        if tables.is_empty() {
            return None;
        }

        Some(
            NeedleError::SourceRegistrationFailed {
                schema: schema_name.to_string(),
                tables: tables.join(", "),
                detail: detail.to_string(),
            }
            .into(),
        )
    }

    pub(crate) fn finish(&self) -> Result<()> {
        self.groups.ensure_all_consumed().map_err(Into::into)
    }

    pub(crate) fn into_tracker(self) -> Option<NeedleTracker> {
        let log_path = self.log_path?;
        if self.tracker_specs.is_empty() {
            return None;
        }
        Some(NeedleTracker::new(log_path, self.tracker_specs))
    }

    fn wrap_tables(
        &mut self,
        schema_name: &str,
        mut tables: std::collections::HashMap<String, Arc<dyn TableProvider>>,
    ) -> Result<std::collections::HashMap<String, Arc<dyn TableProvider>>> {
        if self.groups.is_empty() {
            return Ok(tables);
        }

        for (name, provider) in &mut tables {
            let Some(rows) = self.groups.take(schema_name, name) else {
                continue;
            };
            let batches = build_needle_batches(&rows, &provider.schema()).map_err(|error| {
                DataFusionError::Plan(format!(
                    "failed to build needle batches for {schema_name}.{name}: {error}"
                ))
            })?;
            if !batches.is_empty() {
                *provider = Arc::new(NeedleTableProvider::new(Arc::clone(provider), batches));
            }
        }

        Ok(tables)
    }
}

fn tracked_table_names(specs: &[NeedleSpec], schema_name: &str) -> Vec<String> {
    let mut tables = specs
        .iter()
        .filter(|spec| spec.schema == schema_name)
        .map(|spec| spec.table.clone())
        .collect::<Vec<_>>();
    tables.sort();
    tables.dedup();
    tables
}

fn log_path_for_needles(path: &Path) -> PathBuf {
    let mut log_path = path.as_os_str().to_os_string();
    log_path.push(".log");
    PathBuf::from(log_path)
}
