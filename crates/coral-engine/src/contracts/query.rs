//! Typed query inputs and results.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use coral_spec::ValidatedSourceManifest;

use crate::CoreError;

use super::ColumnInfo;

/// One managed source selected into the current query runtime.
#[derive(Debug, Clone)]
pub struct QuerySource {
    workspace_name: String,
    source_spec: ValidatedSourceManifest,
    variables: BTreeMap<String, String>,
}

impl QuerySource {
    #[must_use]
    /// Builds one app-to-query source selection from installed metadata and a
    /// validated declarative source spec.
    pub fn new(
        workspace_name: impl Into<String>,
        source_spec: ValidatedSourceManifest,
        variables: BTreeMap<String, String>,
    ) -> Self {
        Self {
            workspace_name: workspace_name.into(),
            source_spec,
            variables,
        }
    }

    #[must_use]
    /// Returns the owning workspace name.
    pub fn workspace_name(&self) -> &str {
        &self.workspace_name
    }

    #[must_use]
    /// Returns the canonical source name. This is also the visible SQL schema name.
    pub fn source_name(&self) -> &str {
        self.source_spec.schema_name()
    }

    #[must_use]
    /// Returns the installed manifest version for this source.
    pub fn version(&self) -> &str {
        self.source_spec.source_version()
    }

    #[must_use]
    /// Returns the validated declarative source spec for this source.
    pub fn source_spec(&self) -> &ValidatedSourceManifest {
        &self.source_spec
    }

    #[must_use]
    /// Returns configured non-secret source variables.
    pub fn variables(&self) -> &BTreeMap<String, String> {
        &self.variables
    }
}

/// App-owned non-secret runtime inputs needed while compiling sources.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct QueryRuntimeContext {
    /// Current user's home directory for local path resolution.
    pub home_dir: Option<PathBuf>,
    /// Optional path to a YAML needles file for benchmark needle planting.
    ///
    /// When set, the engine reads needle entries from this file and unions
    /// matching synthetic rows into table query results at registration time.
    /// This implements a "needle in a haystack" evaluation pattern.
    pub needles_file: Option<PathBuf>,
}

impl QueryRuntimeContext {
    #[must_use]
    /// Builds app-owned runtime context with the provided home directory.
    pub fn new(home_dir: Option<PathBuf>) -> Self {
        Self {
            home_dir,
            needles_file: None,
        }
    }

    #[must_use]
    /// Returns a copy of this context with an optional needles file attached.
    pub fn with_needles_file(mut self, needles_file: Option<PathBuf>) -> Self {
        self.needles_file = needles_file;
        self
    }
}

/// Resolves app-owned runtime inputs at query time.
pub trait QueryRuntimeProvider: Send + Sync {
    /// Resolves named source-owned secrets for one selected source.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if the source's credentials cannot be loaded from
    /// the owning application environment.
    fn resolve_source_secrets(
        &self,
        source: &QuerySource,
        secret_names: &BTreeSet<String>,
    ) -> Result<BTreeMap<String, String>, CoreError>;

    /// Returns non-secret runtime inputs owned by the application layer.
    fn runtime_context(&self) -> QueryRuntimeContext;
}

/// The fully materialized result of executing one `SQL` statement.
#[derive(Debug, Clone)]
pub struct QueryExecution {
    schema: Vec<ColumnInfo>,
    arrow_schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
    row_count: usize,
}

impl QueryExecution {
    #[must_use]
    /// Builds a validated fully materialized query result.
    pub fn new(arrow_schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Self {
        let schema = arrow_schema
            .fields()
            .iter()
            .map(|field| ColumnInfo {
                name: field.name().clone(),
                data_type: field.data_type().to_string(),
                nullable: field.is_nullable(),
            })
            .collect();
        let row_count = batches.iter().map(RecordBatch::num_rows).sum();
        Self {
            schema,
            arrow_schema,
            batches,
            row_count,
        }
    }

    #[must_use]
    /// Returns the logical result-set schema.
    pub fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    #[must_use]
    /// Returns the Arrow schema preserved even for empty result sets.
    pub fn arrow_schema(&self) -> &Arc<Schema> {
        &self.arrow_schema
    }

    #[must_use]
    /// Returns the materialized Arrow record batches.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    #[must_use]
    /// Returns the total number of rows across all batches.
    pub fn row_count(&self) -> usize {
        self.row_count
    }
}
