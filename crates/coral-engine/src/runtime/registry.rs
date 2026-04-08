//! Registers compiled backend sources into a shared `DataFusion` session.

use std::path::Path;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;

use crate::backends::{BackendRegistration, CompiledBackendSource, RegisteredSource};
use crate::needles::error::NeedleError;
use crate::needles::loader::{self, NeedleGroups};
use crate::needles::provider::{NeedleTableProvider, build_needle_batches};
use crate::runtime::schema_provider::StaticSchemaProvider;

const RESERVED_SCHEMA_NAMES: &[&str] = &["coral", "coral_admin"];

/// Captures one source manifest that failed to initialize during registration.
#[derive(Debug, Clone)]
pub(crate) struct SourceRegistrationFailure {
    /// Schema name whose registration failed.
    pub schema_name: String,
    /// Human-readable failure detail.
    pub detail: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SourceRegistrationResult {
    pub(crate) active_sources: Vec<RegisteredSource>,
    pub(crate) failures: Vec<SourceRegistrationFailure>,
}

fn check_reserved_schema(schema: &str) -> Result<()> {
    if RESERVED_SCHEMA_NAMES.contains(&schema) {
        return Err(DataFusionError::Execution(format!(
            "source schema '{schema}' is reserved and cannot be used by manifests"
        )));
    }
    Ok(())
}

/// Register all configured source manifests into the active `SessionContext`.
///
/// # Errors
///
/// Returns a `DataFusionError` if the catalog is missing or if the source list
/// itself cannot be processed. Individual source registration failures are
/// logged and skipped so the remaining sources can still be registered.
pub(crate) async fn register_sources(
    ctx: &SessionContext,
    sources: Vec<Box<dyn CompiledBackendSource>>,
    needles_file: Option<&Path>,
) -> Result<SourceRegistrationResult> {
    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| DataFusionError::Plan("catalog 'datafusion' not found".to_string()))?;

    let mut needle_groups = load_needle_groups(needles_file)?;

    let mut result = SourceRegistrationResult::default();
    let mut seen_schemas = std::collections::HashSet::new();

    for source in sources {
        let schema_name = source.schema_name().to_string();
        let source_name = source.source_name().to_string();
        let targeted_tables = needle_groups.table_names_for_schema(source.schema_name());

        match register_source(ctx, &mut seen_schemas, source.as_ref()).await {
            Ok(registration) => {
                let tables = wrap_tables_with_needles(
                    registration.tables,
                    source.schema_name(),
                    &mut needle_groups,
                )?;
                match catalog.register_schema(
                    source.schema_name(),
                    Arc::new(StaticSchemaProvider::new(tables)),
                ) {
                    Ok(_) => result.active_sources.push(registration.source),
                    Err(error) => {
                        if !targeted_tables.is_empty() {
                            return Err(NeedleError::SourceRegistrationFailed {
                                schema: schema_name,
                                tables: targeted_tables.join(", "),
                                detail: error.to_string(),
                            }
                            .into());
                        }
                        tracing::warn!(source = %source_name, error = %error, "skipping source");
                        result.failures.push(SourceRegistrationFailure {
                            schema_name,
                            detail: error.to_string(),
                        });
                    }
                }
            }
            Err(error) => {
                if !targeted_tables.is_empty() {
                    return Err(NeedleError::SourceRegistrationFailed {
                        schema: schema_name,
                        tables: targeted_tables.join(", "),
                        detail: error.to_string(),
                    }
                    .into());
                }
                tracing::warn!(source = %source_name, error = %error, "skipping source");
                result.failures.push(SourceRegistrationFailure {
                    schema_name,
                    detail: error.to_string(),
                });
            }
        }
    }

    needle_groups.ensure_all_consumed()?;

    Ok(result)
}

fn load_needle_groups(path: Option<&Path>) -> Result<NeedleGroups> {
    match path {
        Some(path) => loader::load_needle_groups(path).map_err(Into::into),
        None => Ok(NeedleGroups::default()),
    }
}

#[cfg(test)]
pub(crate) fn register_sources_blocking(
    ctx: &SessionContext,
    sources: Vec<Box<dyn CompiledBackendSource>>,
) -> Result<SourceRegistrationResult> {
    futures::executor::block_on(register_sources(ctx, sources, None))
}

async fn register_source(
    ctx: &SessionContext,
    seen_schemas: &mut std::collections::HashSet<String>,
    source: &dyn CompiledBackendSource,
) -> Result<BackendRegistration> {
    check_reserved_schema(source.schema_name())?;

    if !seen_schemas.insert(source.schema_name().to_string()) {
        return Err(DataFusionError::Execution(format!(
            "duplicate source schema '{}'",
            source.schema_name()
        )));
    }

    source.register(ctx).await
}

/// Wraps each table provider with [`NeedleTableProvider`] if there are matching
/// needle entries. Tables without needles are returned unchanged.
///
/// Returns an error if any matching needle group fails to convert to Arrow
/// batches — a silent skip would cause benchmark results to be silently wrong.
fn wrap_tables_with_needles(
    mut tables: std::collections::HashMap<String, Arc<dyn datafusion::datasource::TableProvider>>,
    schema_name: &str,
    needle_groups: &mut NeedleGroups,
) -> Result<std::collections::HashMap<String, Arc<dyn datafusion::datasource::TableProvider>>> {
    if needle_groups.is_empty() {
        return Ok(tables);
    }

    for (name, provider) in &mut tables {
        let Some(rows) = needle_groups.take(schema_name, name) else {
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, StringArray};
    use datafusion::prelude::SessionContext;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::fs;
    use tempfile::tempdir;

    use super::{check_reserved_schema, register_sources};
    use crate::QueryRuntimeContext;
    use crate::backends::{CompiledBackendSource, compile_source_manifest};
    use coral_spec::{ValidatedSourceManifest, parse_source_manifest_value};

    fn compile_sources(
        manifests: Vec<ValidatedSourceManifest>,
    ) -> Vec<Box<dyn CompiledBackendSource>> {
        manifests
            .into_iter()
            .map(|manifest| {
                compile_source_manifest(
                    &manifest,
                    BTreeMap::new(),
                    BTreeMap::new(),
                    &QueryRuntimeContext::default(),
                )
                .expect("manifest should compile")
            })
            .collect()
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

    async fn query_ids(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx
            .sql(sql)
            .await
            .expect("query should plan")
            .collect()
            .await
            .expect("query should execute");
        let mut ids = Vec::new();
        for batch in &batches {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("id column should be Utf8");
            for i in 0..col.len() {
                ids.push(col.value(i).to_string());
            }
        }
        ids
    }

    #[test]
    fn reserved_schema_coral_is_rejected() {
        let result = check_reserved_schema("coral");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("coral"),
            "error message should mention the schema name"
        );
    }

    #[test]
    fn non_reserved_schema_is_accepted() {
        assert!(check_reserved_schema("github").is_ok());
        assert!(check_reserved_schema("pagerduty").is_ok());
        assert!(check_reserved_schema("slack").is_ok());
    }

    #[tokio::test]
    async fn register_sources_unions_needles_and_respects_where_filters() {
        let fixture_dir = tempdir().expect("tempdir should be created");
        fs::write(
            fixture_dir.path().join("events.jsonl"),
            r#"{"id":"live-1","text":"baseline row","score":10}
{"id":"live-2","text":"high priority live row","score":75}
"#,
        )
        .expect("write jsonl fixture");

        let needles_path = fixture_dir.path().join("needles.yaml");
        fs::write(
            &needles_path,
            r#"
- schema: test_jsonl
  table: events
  role: goal
  data:
    id: "needle-1"
    text: "matching needle row"
    score: 99
- schema: test_jsonl
  table: events
  role: distractor
  data:
    id: "needle-2"
    text: "filtered needle row"
    score: 1
"#,
        )
        .expect("write needles fixture");

        let ctx = SessionContext::new();
        let location = format!("file://{}/", fixture_dir.path().display());
        let manifest = jsonl_manifest(&location);

        register_sources(&ctx, compile_sources(vec![manifest]), Some(&needles_path))
            .await
            .expect("jsonl source with needles should register");

        let all_ids = query_ids(&ctx, "SELECT id FROM test_jsonl.events ORDER BY id").await;
        assert_eq!(all_ids, vec!["live-1", "live-2", "needle-1", "needle-2"]);

        let filtered_ids = query_ids(
            &ctx,
            "SELECT id FROM test_jsonl.events WHERE score > 50 ORDER BY id",
        )
        .await;
        assert_eq!(filtered_ids, vec!["live-2", "needle-1"]);
    }

    #[tokio::test]
    async fn register_sources_fails_when_needles_yaml_is_invalid() {
        let fixture_dir = tempdir().expect("tempdir should be created");
        let needles_path = fixture_dir.path().join("needles.yaml");
        fs::write(&needles_path, "not: valid: yaml: [").expect("write malformed needles fixture");

        let ctx = SessionContext::new();
        let error = register_sources(&ctx, Vec::new(), Some(&needles_path))
            .await
            .expect_err("invalid needles yaml should fail runtime build");
        assert!(
            error.to_string().contains("failed to parse needles YAML"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn register_sources_fails_when_needle_row_violates_non_nullable_schema() {
        let fixture_dir = tempdir().expect("tempdir should be created");
        fs::write(
            fixture_dir.path().join("events.jsonl"),
            r#"{"id":"live-1","text":"baseline row","score":10}
"#,
        )
        .expect("write jsonl fixture");

        let needles_path = fixture_dir.path().join("needles.yaml");
        fs::write(
            &needles_path,
            r#"
- schema: test_jsonl
  table: events
  role: goal
  data:
    id: "needle-1"
    text: "missing required score"
"#,
        )
        .expect("write needles fixture");

        let ctx = SessionContext::new();
        let location = format!("file://{}/", fixture_dir.path().display());
        let manifest = jsonl_manifest(&location);

        let error = register_sources(&ctx, compile_sources(vec![manifest]), Some(&needles_path))
            .await
            .expect_err("invalid needle row should fail runtime build");
        assert!(
            error
                .to_string()
                .contains("failed to build needle batches for test_jsonl.events"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn register_sources_fails_when_needles_target_unregistered_table() {
        let fixture_dir = tempdir().expect("tempdir should be created");
        fs::write(
            fixture_dir.path().join("events.jsonl"),
            r#"{"id":"live-1","text":"baseline row","score":10}
"#,
        )
        .expect("write jsonl fixture");

        let needles_path = fixture_dir.path().join("needles.yaml");
        fs::write(
            &needles_path,
            r#"
- schema: test_jsonl
  table: missing_table
  role: goal
  data:
    id: "needle-1"
    text: "orphan needle row"
    score: 99
"#,
        )
        .expect("write needles fixture");

        let ctx = SessionContext::new();
        let location = format!("file://{}/", fixture_dir.path().display());
        let manifest = jsonl_manifest(&location);

        let error = register_sources(&ctx, compile_sources(vec![manifest]), Some(&needles_path))
            .await
            .expect_err("unused needle entries should fail runtime build");
        assert!(
            error.to_string().contains("test_jsonl.missing_table"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn register_sources_fails_with_source_error_when_targeted_source_cannot_register() {
        let fixture_dir = tempdir().expect("tempdir should be created");
        let needles_path = fixture_dir.path().join("needles.yaml");
        fs::write(
            &needles_path,
            r#"
- schema: test_jsonl
  table: events
  role: goal
  data:
    id: "needle-1"
    text: "blocked by source registration failure"
    score: 99
"#,
        )
        .expect("write needles fixture");

        let ctx = SessionContext::new();
        let manifest = jsonl_manifest("file:///path/that/does/not/exist/");

        let error = register_sources(&ctx, compile_sources(vec![manifest]), Some(&needles_path))
            .await
            .expect_err("source failure for targeted needles should be fatal");
        assert!(
            error.to_string().contains(
                "source 'test_jsonl' failed to register while needles target table(s) events"
            ),
            "unexpected error: {error}"
        );
    }
}
