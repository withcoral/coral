//! Concrete `DataFusion` runtime assembly for the data plane.

use std::sync::Arc;

use datafusion::common::SchemaError;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SQLOptions, SessionConfig, SessionContext};

use crate::backends::compile_query_source;
use crate::backends::http::ProviderQueryError;
use crate::needles::error::NeedleError;
use crate::needles::{NeedleState, NeedleTracker};
use crate::runtime::catalog;
use crate::runtime::registry::{SourceRegistrationFailure, register_sources};
use crate::{CoreError, QueryExecution, QueryRuntimeProvider, QuerySource, TableInfo};

pub(crate) struct QueryRuntimeAdapter {
    ctx: Arc<SessionContext>,
    tables: Vec<TableInfo>,
    needle_tracker: Option<NeedleTracker>,
}

pub(crate) async fn build_runtime(
    sources: &[QuerySource],
    runtime: &dyn QueryRuntimeProvider,
) -> Result<QueryRuntimeAdapter, CoreError> {
    let session_config = SessionConfig::new().with_information_schema(true);
    let runtime_env = Arc::new(
        RuntimeEnvBuilder::new()
            .with_object_list_cache_limit(0)
            .build()
            .map_err(datafusion_to_core)?,
    );
    let ctx = Arc::new(SessionContext::new_with_config_rt(
        session_config,
        runtime_env,
    ));

    let runtime_context = runtime.runtime_context();
    let mut needles = NeedleState::from_path(runtime_context.needles_file.as_deref())
        .map_err(datafusion_to_core)?;
    let mut compiled_sources = Vec::new();
    let mut failures = Vec::new();
    for source in sources {
        match compile_query_source(source, &runtime_context) {
            Ok(compiled) => compiled_sources.push(compiled),
            Err(error) => failures.push(SourceRegistrationFailure {
                schema_name: source.source_name().to_string(),
                detail: error.to_string(),
            }),
        }
    }
    let registration = register_sources(&ctx, compiled_sources, &mut needles)
        .await
        .map_err(datafusion_to_core)?;
    catalog::register(&ctx, &registration.active_sources).map_err(datafusion_to_core)?;
    let tables = catalog::collect_tables(&registration.active_sources);
    let needle_tracker = needles.into_tracker();
    for failure in &failures {
        tracing::warn!(
            source = %failure.schema_name,
            detail = %failure.detail,
            "skipping source during runtime build"
        );
    }

    Ok(QueryRuntimeAdapter {
        ctx,
        tables,
        needle_tracker,
    })
}

impl QueryRuntimeAdapter {
    pub(crate) fn list_tables(&self, source_filter: Option<&str>) -> Vec<TableInfo> {
        self.tables
            .iter()
            .filter(|table| source_filter.is_none_or(|value| table.schema_name == value))
            .cloned()
            .collect()
    }

    pub(crate) async fn execute_sql(&self, sql: &str) -> Result<QueryExecution, CoreError> {
        let df = self
            .ctx
            .sql_with_options(sql, read_only_sql_options())
            .await
            .map_err(datafusion_to_core)?;
        let arrow_schema = Arc::new(df.schema().as_arrow().clone());
        let batches = df.collect().await.map_err(datafusion_to_core)?;
        if let Some(tracker) = &self.needle_tracker {
            tracker
                .check_and_log(sql, &batches)
                .map_err(|error| needle_error_to_core(&error))?;
        }
        Ok(QueryExecution::new(arrow_schema, batches))
    }
}

fn read_only_sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

fn datafusion_to_core(error: DataFusionError) -> CoreError {
    match error {
        DataFusionError::SQL(detail, _) => CoreError::InvalidInput(detail.to_string()),
        DataFusionError::Plan(detail) => CoreError::InvalidInput(detail),
        DataFusionError::SchemaError(schema_error, _) => match schema_error.as_ref() {
            SchemaError::FieldNotFound { field, .. } => CoreError::NotFound(field.to_string()),
            _ => CoreError::InvalidInput(schema_error.to_string()),
        },
        DataFusionError::NotImplemented(detail) => CoreError::Unimplemented(detail),
        DataFusionError::External(inner) => {
            if let Some(provider_error) = inner.downcast_ref::<ProviderQueryError>() {
                return provider_error_to_core(provider_error);
            }
            if let Some(needle_error) = inner.downcast_ref::<NeedleError>() {
                return needle_error_to_core(needle_error);
            }
            CoreError::internal(inner.to_string())
        }
        DataFusionError::ObjectStore(error) => CoreError::Unavailable(error.to_string()),
        DataFusionError::ResourcesExhausted(detail) => CoreError::Unavailable(detail),
        other => CoreError::internal(other.to_string()),
    }
}

fn provider_error_to_core(error: &ProviderQueryError) -> CoreError {
    match error {
        ProviderQueryError::MissingRequiredFilter {
            schema,
            table,
            field,
        } => CoreError::FailedPrecondition(format!(
            "{schema}.{table} requires WHERE {field} = <constant>"
        )),
        ProviderQueryError::ApiRequest {
            status,
            detail,
            method,
            url,
            ..
        } => match status {
            Some(429 | 500..=599) => CoreError::Unavailable(format!(
                "{}{}{}",
                detail,
                method
                    .as_ref()
                    .map(|value| format!(" [{value}]"))
                    .unwrap_or_default(),
                url.as_ref()
                    .map(|value| format!(" {value}"))
                    .unwrap_or_default()
            )),
            _ => CoreError::FailedPrecondition(detail.clone()),
        },
    }
}

fn needle_error_to_core(error: &NeedleError) -> CoreError {
    match error {
        NeedleError::Io { .. } | NeedleError::SourceRegistrationFailed { .. } => {
            CoreError::FailedPrecondition(error.to_string())
        }
        NeedleError::Yaml(_)
        | NeedleError::CastFailed { .. }
        | NeedleError::JsonConversion(_)
        | NeedleError::Arrow(_)
        | NeedleError::UnusedEntries { .. } => CoreError::InvalidInput(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use serde_json::json;
    use tempfile::tempdir;

    use super::build_runtime;
    use crate::{CoreError, QueryRuntimeContext, QueryRuntimeProvider, QuerySource};
    use coral_spec::{ValidatedSourceManifest, parse_source_manifest_value};

    struct TestRuntimeProvider {
        ctx: QueryRuntimeContext,
    }

    impl QueryRuntimeProvider for TestRuntimeProvider {
        fn resolve_source_secrets(
            &self,
            _source: &QuerySource,
            _secret_names: &BTreeSet<String>,
        ) -> Result<BTreeMap<String, String>, CoreError> {
            Ok(BTreeMap::new())
        }

        fn runtime_context(&self) -> QueryRuntimeContext {
            self.ctx.clone()
        }
    }

    fn jsonl_manifest(location: &str) -> ValidatedSourceManifest {
        parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "test_jsonl",
            "version": "0.1.0",
            "backend": "jsonl",
            "tables": [{
                "name": "events",
                "description": "test events",
                "source": {
                    "location": location,
                    "glob": "**/*.jsonl",
                    "partitions": [],
                },
                "columns": [
                    {"name": "id", "type": "Utf8", "nullable": false},
                    {"name": "text", "type": "Utf8"},
                    {"name": "score", "type": "Int64", "nullable": false},
                ],
            }]
        }))
        .expect("jsonl manifest should parse")
    }

    #[tokio::test]
    async fn execute_sql_logs_matching_needles_to_ndjson() {
        let fixture_dir = tempdir().expect("tempdir should be created");
        std::fs::write(
            fixture_dir.path().join("events.jsonl"),
            r#"{"id":"live-1","text":"baseline row","score":10}
{"id":"live-2","text":"high priority live row","score":75}
"#,
        )
        .expect("write jsonl fixture");

        let needles_path = fixture_dir.path().join("needles.yaml");
        std::fs::write(
            &needles_path,
            r#"
- schema: test_jsonl
  table: events
  data:
    id: "needle-1"
    text: "matching needle row"
    score: 99
"#,
        )
        .expect("write needles fixture");

        let source = QuerySource::new(
            "default",
            jsonl_manifest(&format!("file://{}/", fixture_dir.path().display())),
            BTreeMap::new(),
        );
        let runtime = TestRuntimeProvider {
            ctx: QueryRuntimeContext::default().with_needles_file(Some(needles_path.clone())),
        };

        let adapter = build_runtime(&[source], &runtime)
            .await
            .expect("runtime should build");
        adapter
            .execute_sql("SELECT id, text FROM test_jsonl.events WHERE score > 50 ORDER BY id")
            .await
            .expect("query should succeed");

        let log = std::fs::read_to_string(format!("{}.log", needles_path.display()))
            .expect("log should be readable");
        let lines = log.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 1);

        let entry: serde_json::Value =
            serde_json::from_str(lines[0]).expect("log entry should parse");
        assert_eq!(entry["schema"], "test_jsonl");
        assert_eq!(entry["table"], "events");
        assert_eq!(entry["needle"]["id"], "needle-1");
        assert_eq!(
            entry["sql"],
            "SELECT id, text FROM test_jsonl.events WHERE score > 50 ORDER BY id"
        );
    }
}
