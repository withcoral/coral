//! Benchmark needle planting: unions synthetic rows with live provider data.
//!
//! This implements a "needle in a haystack" evaluation pattern: a benchmark
//! harness writes a YAML file of synthetic rows, and when `CORAL_NEEDLES_FILE`
//! is set the engine converts matching entries to Arrow batches at source
//! registration time, wrapping each affected table provider with
//! [`provider::NeedleTableProvider`].

use std::path::Path;
use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};

use crate::backends::BackendRegistration;

pub(crate) mod error;
pub(crate) mod loader;
pub(crate) mod provider;

use error::NeedleError;
use loader::NeedleGroups;
use provider::{NeedleTableProvider, build_needle_batches};

pub(crate) struct NeedleState {
    groups: NeedleGroups,
}

impl NeedleState {
    pub(crate) fn from_path(path: Option<&Path>) -> Result<Self> {
        let groups = match path {
            Some(path) => loader::load_needle_groups(path).map_err(DataFusionError::from)?,
            None => NeedleGroups::default(),
        };
        Ok(Self { groups })
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
        let tables = self.groups.table_names_for_schema(schema_name);
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

    pub(crate) fn finish(self) -> Result<()> {
        self.groups.ensure_all_consumed().map_err(Into::into)
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
