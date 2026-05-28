//! Passive observed-value indexing for successful SQL results.

use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use coral_engine::{QueryResultObserver, QueryResultObserverError, QuerySource};
use coral_spec::ColumnSpec;
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};
use sqlparser::ast::{
    Expr, Ident, ObjectName, Select, SelectItem, Statement, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::search::index::{
    ObservedValueRecord, ObservedValueSensitivityTier, ObservedValueSuggestedOperator,
    ObservedValueSurfaceKind, SearchIndexError, SearchIndexStore,
};
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

/// Query-result observer that writes direct-provenance cell values into search `SQLite`.
pub(crate) struct ObservedValueIndexer {
    layout: AppStateLayout,
    workspace_name: WorkspaceName,
    surfaces: Vec<ObservedSurface>,
}

impl ObservedValueIndexer {
    pub(crate) fn new(
        layout: AppStateLayout,
        workspace_name: WorkspaceName,
        selected_sources: &[QuerySource],
    ) -> Self {
        Self {
            layout,
            workspace_name,
            surfaces: observed_surfaces(selected_sources),
        }
    }

    fn observe_result_inner(
        &self,
        sql: &str,
        schema: &Schema,
        batches: &[RecordBatch],
    ) -> Result<(), ObservedValueIndexError> {
        let provenance = resolve_projection_provenance(sql, schema, &self.surfaces);
        if provenance.iter().all(Option::is_none) {
            tracing::debug!(
                "skipping observed-value indexing because result provenance is unknown"
            );
            return Ok(());
        }

        let records = observed_records_from_batches(schema, batches, &provenance)?;
        if records.is_empty() {
            return Ok(());
        }

        let store = SearchIndexStore::open_workspace(&self.layout, &self.workspace_name)?;
        store.upsert_observed_values(&self.workspace_name, records)?;
        Ok(())
    }
}

impl QueryResultObserver for ObservedValueIndexer {
    fn name(&self) -> &'static str {
        "observed_value_indexer"
    }

    fn observe_result(
        &self,
        sql: &str,
        schema: &Schema,
        batches: &[RecordBatch],
    ) -> Result<(), QueryResultObserverError> {
        if let Err(error) = self.observe_result_inner(sql, schema, batches) {
            tracing::warn!(
                error = %error,
                "observed-value indexing failed; returning SQL result without indexed values"
            );
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
enum ObservedValueIndexError {
    #[error(transparent)]
    SearchIndex(#[from] SearchIndexError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
struct ObservedSurface {
    source_name: String,
    surface_kind: ObservedValueSurfaceKind,
    surface_name: String,
    column_names: BTreeSet<String>,
}

impl ObservedSurface {
    fn allows_column(&self, column_name: &str) -> bool {
        self.column_names.is_empty()
            || self
                .column_names
                .contains(&normalize_identifier(column_name))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FieldProvenance {
    source_name: String,
    surface_kind: ObservedValueSurfaceKind,
    surface_name: String,
    column_name: String,
}

fn observed_surfaces(selected_sources: &[QuerySource]) -> Vec<ObservedSurface> {
    let mut surfaces = Vec::new();
    for source in selected_sources {
        let source_name = source.source_name().to_string();
        let spec = source.source_spec();
        if let Some(http) = spec.as_http() {
            for table in &http.tables {
                surfaces.push(observed_surface(
                    &source_name,
                    ObservedValueSurfaceKind::Table,
                    table.name(),
                    table.columns(),
                ));
            }
            for function in &http.functions {
                surfaces.push(observed_surface(
                    &source_name,
                    ObservedValueSurfaceKind::TableFunction,
                    &function.name,
                    &function.columns,
                ));
            }
        }
        if let Some(file) = spec.as_file() {
            for table in &file.tables {
                surfaces.push(observed_surface(
                    &source_name,
                    ObservedValueSurfaceKind::Table,
                    table.name(),
                    table.columns(),
                ));
            }
        }
        if let Some(mcp) = spec.as_mcp() {
            for table in &mcp.tables {
                surfaces.push(observed_surface(
                    &source_name,
                    ObservedValueSurfaceKind::Table,
                    table.name(),
                    table.columns(),
                ));
            }
            for function in &mcp.functions {
                surfaces.push(observed_surface(
                    &source_name,
                    ObservedValueSurfaceKind::TableFunction,
                    function.name(),
                    function.columns(),
                ));
            }
        }
    }
    surfaces
}

fn observed_surface(
    source_name: &str,
    surface_kind: ObservedValueSurfaceKind,
    surface_name: &str,
    columns: &[ColumnSpec],
) -> ObservedSurface {
    ObservedSurface {
        source_name: source_name.to_string(),
        surface_kind,
        surface_name: surface_name.to_string(),
        column_names: columns
            .iter()
            .map(|column| normalize_identifier(&column.name))
            .collect(),
    }
}

fn resolve_projection_provenance(
    sql: &str,
    schema: &Schema,
    surfaces: &[ObservedSurface],
) -> Vec<Option<FieldProvenance>> {
    let empty = vec![None; schema.fields().len()];
    let Ok(statements) = Parser::parse_sql(&GenericDialect {}, sql) else {
        return empty;
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return empty;
    };
    let Some(select) = query.body.as_select() else {
        return empty;
    };
    let Some(surface) = select_from_surface(select, surfaces) else {
        return empty;
    };

    if let [item] = select.projection.as_slice()
        && projection_is_wildcard(item)
    {
        return schema
            .fields()
            .iter()
            .map(|field| {
                direct_field_provenance(
                    surface,
                    field.name(),
                    field.name(),
                    std::slice::from_ref(&surface.surface_name),
                )
            })
            .collect();
    }

    if select.projection.len() != schema.fields().len() {
        return empty;
    }

    let qualifiers = surface_qualifiers(surface);
    select
        .projection
        .iter()
        .zip(schema.fields().iter())
        .map(|(item, field)| {
            projection_column_name(item, &qualifiers).and_then(|column_name| {
                direct_field_provenance(surface, &column_name, field.name(), &qualifiers)
            })
        })
        .collect()
}

fn select_from_surface<'a>(
    select: &Select,
    surfaces: &'a [ObservedSurface],
) -> Option<&'a ObservedSurface> {
    let [from] = select.from.as_slice() else {
        return None;
    };
    if !from.joins.is_empty() {
        return None;
    }
    table_with_joins_surface(from, surfaces)
}

fn table_with_joins_surface<'a>(
    from: &TableWithJoins,
    surfaces: &'a [ObservedSurface],
) -> Option<&'a ObservedSurface> {
    match &from.relation {
        TableFactor::Table { name, args, .. } => {
            let expected_kind = if args.is_some() {
                Some(ObservedValueSurfaceKind::TableFunction)
            } else {
                Some(ObservedValueSurfaceKind::Table)
            };
            resolve_surface_name(name, expected_kind, surfaces)
        }
        TableFactor::Function { name, .. } => resolve_surface_name(
            name,
            Some(ObservedValueSurfaceKind::TableFunction),
            surfaces,
        ),
        _ => None,
    }
}

fn resolve_surface_name<'a>(
    name: &ObjectName,
    expected_kind: Option<ObservedValueSurfaceKind>,
    surfaces: &'a [ObservedSurface],
) -> Option<&'a ObservedSurface> {
    let parts = object_name_parts(name)?;
    let matches = surfaces
        .iter()
        .filter(|surface| expected_kind.is_none_or(|kind| surface.surface_kind == kind))
        .filter(|surface| surface_matches_parts(surface, &parts))
        .collect::<Vec<_>>();
    let [surface] = matches.as_slice() else {
        return None;
    };
    Some(*surface)
}

fn surface_matches_parts(surface: &ObservedSurface, parts: &[String]) -> bool {
    if let [surface_name] = parts {
        return same_identifier(surface_name, &surface.surface_name);
    }
    if parts.len() < 2 {
        return false;
    }
    for split in 1..parts.len() {
        let Some(source_parts) = parts.get(..split) else {
            continue;
        };
        let Some(surface_parts) = parts.get(split..) else {
            continue;
        };
        let source = source_parts.join(".");
        let surface_name = surface_parts.join(".");
        if same_identifier(&source, &surface.source_name)
            && same_identifier(&surface_name, &surface.surface_name)
        {
            return true;
        }
    }
    false
}

fn projection_is_wildcard(item: &SelectItem) -> bool {
    matches!(
        item,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
    )
}

fn projection_column_name(item: &SelectItem, qualifiers: &[String]) -> Option<String> {
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            expr_column_name(expr, qualifiers)
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => None,
    }
}

fn expr_column_name(expr: &Expr, qualifiers: &[String]) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => {
            let (column, qualifier_parts) = idents.split_last()?;
            qualifier_matches(qualifier_parts, qualifiers).then(|| column.value.clone())
        }
        _ => None,
    }
}

fn direct_field_provenance(
    surface: &ObservedSurface,
    column_name: &str,
    output_field_name: &str,
    _qualifiers: &[String],
) -> Option<FieldProvenance> {
    if !surface.allows_column(column_name) || is_sensitive_column(column_name) {
        return None;
    }
    if output_field_name.trim().is_empty() {
        return None;
    }
    Some(FieldProvenance {
        source_name: surface.source_name.clone(),
        surface_kind: surface.surface_kind,
        surface_name: surface.surface_name.clone(),
        column_name: column_name.to_string(),
    })
}

fn surface_qualifiers(surface: &ObservedSurface) -> Vec<String> {
    let qualified = format!("{}.{}", surface.source_name, surface.surface_name);
    vec![
        normalize_identifier(&surface.source_name),
        normalize_identifier(&surface.surface_name),
        normalize_identifier(&qualified),
    ]
}

fn qualifier_matches(idents: &[Ident], qualifiers: &[String]) -> bool {
    let parts = idents
        .iter()
        .map(|ident| ident.value.clone())
        .collect::<Vec<_>>();
    qualifier_matches_parts(&parts, qualifiers)
}

fn qualifier_matches_parts(parts: &[String], qualifiers: &[String]) -> bool {
    let qualifier = normalize_identifier(&parts.join("."));
    qualifiers.iter().any(|known| known == &qualifier)
}

fn object_name_parts(name: &ObjectName) -> Option<Vec<String>> {
    name.0
        .iter()
        .map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect()
}

fn observed_records_from_batches(
    schema: &Schema,
    batches: &[RecordBatch],
    provenance: &[Option<FieldProvenance>],
) -> Result<Vec<ObservedValueRecord>, ObservedValueIndexError> {
    let mut records = BTreeMap::<ObservedValueRecordKey, ObservedValueRecord>::new();
    for batch in batches {
        for row in record_batch_rows(batch)? {
            for (field_index, field_provenance) in provenance.iter().enumerate() {
                let Some(field_provenance) = field_provenance else {
                    continue;
                };
                let Some(field) = schema.fields().get(field_index) else {
                    continue;
                };
                let Some(value) = row.get(field.name()) else {
                    continue;
                };
                let Some(display_value) = observed_display_value(value) else {
                    continue;
                };
                let normalized_value_key = normalized_value_key(&display_value);
                let key = ObservedValueRecordKey {
                    source_name: field_provenance.source_name.clone(),
                    surface_kind: field_provenance.surface_kind,
                    surface_name: field_provenance.surface_name.clone(),
                    column_name: field_provenance.column_name.clone(),
                    normalized_value_key: normalized_value_key.clone(),
                };
                records
                    .entry(key)
                    .and_modify(|record| {
                        record.observed_count = record.observed_count.saturating_add(1);
                    })
                    .or_insert_with(|| ObservedValueRecord {
                        source_name: field_provenance.source_name.clone(),
                        surface_kind: field_provenance.surface_kind,
                        surface_name: field_provenance.surface_name.clone(),
                        column_name: field_provenance.column_name.clone(),
                        normalized_value_key,
                        display_value: display_value.clone(),
                        searchable_text: observed_searchable_text(field_provenance, &display_value),
                        sensitivity_tier: ObservedValueSensitivityTier::LowRisk,
                        suggested_operator: ObservedValueSuggestedOperator::Exact,
                        observed_count: 1,
                    });
            }
        }
    }
    Ok(records.into_values().collect())
}

fn record_batch_rows(
    batch: &RecordBatch,
) -> Result<Vec<Map<String, Value>>, ObservedValueIndexError> {
    let mut bytes = Vec::new();
    {
        let mut writer = arrow::json::ArrayWriter::new(&mut bytes);
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn observed_display_value(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) => {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        }
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).ok(),
    }
}

fn observed_searchable_text(provenance: &FieldProvenance, display_value: &str) -> String {
    [
        provenance.source_name.as_str(),
        provenance.surface_name.as_str(),
        provenance.column_name.as_str(),
        display_value,
    ]
    .into_iter()
    .filter(|part| !part.trim().is_empty())
    .collect::<Vec<_>>()
    .join(" ")
}

fn normalized_value_key(display_value: &str) -> String {
    let normalized = display_value.trim().to_ascii_lowercase();
    let digest = Sha256::digest(normalized.as_bytes());
    format!("{digest:x}")
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ObservedValueRecordKey {
    source_name: String,
    surface_kind: ObservedValueSurfaceKind,
    surface_name: String,
    column_name: String,
    normalized_value_key: String,
}

fn is_sensitive_column(column_name: &str) -> bool {
    let normalized = normalize_identifier(column_name);
    [
        "api_key",
        "apikey",
        "auth",
        "authorization",
        "cookie",
        "password",
        "private_key",
        "refresh_token",
        "secret",
        "session",
        "token",
    ]
    .into_iter()
    .any(|needle| normalized.contains(needle))
}

fn same_identifier(left: &str, right: &str) -> bool {
    normalize_identifier(left) == normalize_identifier(right)
}

fn normalize_identifier(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field};
    use coral_engine::QuerySource;
    use coral_spec::parse_source_manifest_value;
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn indexes_direct_table_values_including_long_text_and_json_strings() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = query_source("fixture");
        let indexer = ObservedValueIndexer::new(layout.clone(), workspace.clone(), &[source]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("service", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
            Field::new("payload", DataType::Utf8, true),
            Field::new("api_token", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["payments-api", "billing-worker"])),
                Arc::new(StringArray::from(vec![
                    "short incident note",
                    "very long incident body with retry budget exhausted and deploy rollback context",
                ])),
                Arc::new(StringArray::from(vec![
                    r#"{"error":"timeout","region":"us-east-1"}"#,
                    r#"{"error":"deploy_failed","sha":"abc123"}"#,
                ])),
                Arc::new(StringArray::from(vec!["secret-token", "another-secret"])),
            ],
        )
        .expect("batch");

        indexer
            .observe_result(
                "SELECT service, body, payload, api_token FROM fixture.messages",
                &schema,
                &[batch],
            )
            .expect("observer does not fail SQL");

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        let hits = store
            .search_observed_values(&workspace, &["deploy_failed".to_string()], 10)
            .expect("search observed");
        assert!(hits.iter().any(|hit| hit.column_name == "payload"));

        let hits = store
            .search_observed_values(&workspace, &["rollback".to_string()], 10)
            .expect("search observed");
        assert!(hits.iter().any(|hit| hit.column_name == "body"));

        let hits = store
            .search_observed_values(&workspace, &["secret-token".to_string()], 10)
            .expect("search sensitive");
        assert!(hits.is_empty());
    }

    #[test]
    fn skips_queries_without_direct_single_surface_provenance() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = query_source("fixture");
        let indexer = ObservedValueIndexer::new(layout.clone(), workspace.clone(), &[source]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["payments-api"]))],
        )
        .expect("batch");

        indexer
            .observe_result(
                "SELECT upper(service) AS service FROM fixture.messages",
                &schema,
                &[batch],
            )
            .expect("observer does not fail SQL");

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        let hits = store
            .search_observed_values(&workspace, &["payments-api".to_string()], 10)
            .expect("search observed");
        assert!(hits.is_empty());
    }

    #[test]
    fn duplicate_values_are_aggregated_before_storage() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = query_source("fixture");
        let indexer = ObservedValueIndexer::new(layout.clone(), workspace.clone(), &[source]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                "payments-api",
                "payments-api",
                "payments-api",
            ]))],
        )
        .expect("batch");

        indexer
            .observe_result("SELECT service FROM fixture.messages", &schema, &[batch])
            .expect("observer does not fail SQL");

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        let hits = store
            .search_observed_values(&workspace, &["payments-api".to_string()], 10)
            .expect("search observed");
        assert_eq!(hits.len(), 1);
        let observed_count: i64 = store
            .connect()
            .expect("connect")
            .query_row(
                "
                SELECT observed_count
                FROM observed_values
                WHERE workspace = 'default' AND display_value = 'payments-api'
                ",
                [],
                |row| row.get(0),
            )
            .expect("observed count");
        assert_eq!(observed_count, 3);
    }

    fn query_source(name: &str) -> QuerySource {
        let manifest = parse_source_manifest_value(json!({
            "name": name,
            "version": "0.1.0",
            "dsl_version": 3,
            "backend": "file",
            "tables": [{
                "name": "messages",
                "description": "Messages fixture",
                "format": "jsonl",
                "source": {
                    "location": "file:///tmp",
                    "glob": "**/*.jsonl"
                },
                "columns": [
                    {"name": "service", "type": "Utf8"},
                    {"name": "body", "type": "Utf8"},
                    {"name": "payload", "type": "Utf8"},
                    {"name": "api_token", "type": "Utf8"}
                ]
            }]
        }))
        .expect("manifest");
        QuerySource::new(manifest, BTreeMap::new(), BTreeMap::new())
    }
}
