//! Passive observed-value indexing for successful SQL results.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::time::Duration as StdDuration;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use coral_engine::{
    QueryResultObserver, QueryResultObserverError, QuerySource, RuntimeSourceComponent,
};
use coral_spec::ColumnSpec;
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};
use sqlparser::ast::{
    Expr, Ident, ObjectName, Select, SelectItem, Statement, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use uuid::Uuid;

use crate::search::index::{
    ObservedValueRecord, ObservedValueSuggestedOperator, ObservedValueSurfaceKind,
    SearchIndexError, SearchIndexStore,
};
use crate::sources::SourceName;
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

const CATALOG_FINGERPRINT_FILE_NAME: &str = "catalog.sha256";
const DEFAULT_OBSERVED_QUEUE_FOREGROUND_DRAIN_MS: u64 = 1_000;
const DEFAULT_OBSERVED_MAX_STORAGE_MB: u64 = 256;
const DEFAULT_OBSERVED_COLLECTION_MAX_CANDIDATES: usize = 10_000;
const DEFAULT_OBSERVED_COLLECTION_MAX_CANDIDATE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_OBSERVED_COLLECTION_MAX_JSON_DEPTH: usize = 8;
const BYTES_PER_MIB: u64 = 1024 * 1024;
const SOURCE_GENERATION_DIR_NAME: &str = "source-generations";

#[derive(Debug, Default, serde::Deserialize)]
struct ObservedSearchConfigFile {
    #[serde(default)]
    search: ObservedSearchConfig,
}

#[derive(Debug, Default, serde::Deserialize)]
struct ObservedSearchConfig {
    #[serde(rename = "observed_queue_foreground_drain_ms")]
    queue_foreground_drain_ms: Option<u64>,
    #[serde(rename = "observed_max_storage_mb")]
    storage_mb: Option<u64>,
    #[serde(rename = "observed_collection_max_candidates")]
    collection_candidates: Option<usize>,
    #[serde(rename = "observed_collection_max_candidate_bytes")]
    collection_candidate_bytes: Option<usize>,
    #[serde(rename = "observed_collection_max_json_depth")]
    collection_json_depth: Option<usize>,
}

/// Query-result observer that writes direct-provenance cell values into search index storage.
pub(crate) struct ObservedValueIndexer {
    layout: AppStateLayout,
    workspace_name: WorkspaceName,
    surfaces: Vec<ObservedSurface>,
    source_generations: BTreeMap<String, Option<String>>,
    collection_budget: ObservedCollectionBudget,
    storage_budget_bytes: u64,
}

impl ObservedValueIndexer {
    pub(crate) fn new(
        layout: AppStateLayout,
        workspace_name: WorkspaceName,
        selected_sources: &[QuerySource],
    ) -> Self {
        let surfaces = observed_surfaces(selected_sources);
        let source_generations = observed_source_generations(&layout, &workspace_name, &surfaces);
        let config = observed_search_config_or_default(&layout);
        Self {
            layout,
            workspace_name,
            surfaces,
            source_generations,
            collection_budget: observed_collection_budget_from_config(&config.search),
            storage_budget_bytes: observed_storage_budget_bytes_from_config(&config.search),
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
        if self.source_generations_changed(&provenance)? {
            tracing::debug!(
                workspace = %self.workspace_name,
                "skipping observed-value indexing because a source changed while the query was running"
            );
            return Ok(());
        }

        let collection =
            observed_records_from_batches(schema, batches, &provenance, self.collection_budget)?;
        if collection.budget_exhausted {
            tracing::debug!(
                workspace = %self.workspace_name,
                accepted_candidates = collection.accepted_candidates,
                accepted_candidate_bytes = collection.accepted_candidate_bytes,
                skipped_oversize_candidates = collection.skipped_oversize_candidates,
                "observed-value collection budget exhausted; enqueueing bounded chunks"
            );
        }
        if collection.is_empty() {
            return Ok(());
        }

        if !SearchIndexStore::workspace_index_is_usable(&self.layout, &self.workspace_name) {
            clear_catalog_fingerprint(&self.layout, &self.workspace_name)?;
        }
        let store = SearchIndexStore::open_workspace(&self.layout, &self.workspace_name)?;
        let enforcement = store.enforce_observed_storage_budget(self.storage_budget_bytes)?;
        if enforcement.budget_exceeded {
            tracing::warn!(
                workspace = %self.workspace_name,
                storage_bytes = enforcement.storage_bytes,
                max_storage_bytes = self.storage_budget_bytes,
                "pausing observed-value enqueue because storage budget is exhausted"
            );
            return Ok(());
        }
        for records in collection.record_chunks {
            let enforcement = store.enforce_observed_storage_budget(self.storage_budget_bytes)?;
            if enforcement.budget_exceeded {
                tracing::warn!(
                    workspace = %self.workspace_name,
                    storage_bytes = enforcement.storage_bytes,
                    max_storage_bytes = self.storage_budget_bytes,
                    "pausing observed-value enqueue because storage budget is exhausted"
                );
                break;
            }
            store.enqueue_observed_values(&self.workspace_name, records)?;
        }
        store.enforce_observed_storage_budget(self.storage_budget_bytes)?;
        Ok(())
    }

    fn source_generations_changed(
        &self,
        provenance: &[Option<FieldProvenance>],
    ) -> Result<bool, ObservedValueIndexError> {
        let source_names = provenance
            .iter()
            .filter_map(|provenance| provenance.as_ref())
            .map(|provenance| provenance.source_name.as_str())
            .collect::<BTreeSet<_>>();
        for source_name in source_names {
            let generation =
                read_observed_source_generation(&self.layout, &self.workspace_name, source_name)?;
            let expected = self
                .source_generations
                .get(source_name)
                .cloned()
                .unwrap_or(None);
            if generation != expected {
                return Ok(true);
            }
        }
        Ok(false)
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
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Toml(#[from] toml::de::Error),
}

fn clear_catalog_fingerprint(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
) -> Result<(), ObservedValueIndexError> {
    let path = layout
        .search_dir(workspace_name)
        .join(CATALOG_FINGERPRINT_FILE_NAME);
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn mark_observed_source_generation(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
) -> Result<(), std::io::Error> {
    let path = observed_source_generation_file(layout, workspace_name, source_name.as_str());
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, format!("{}\n", Uuid::new_v4()))
}

fn observed_source_generations(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    surfaces: &[ObservedSurface],
) -> BTreeMap<String, Option<String>> {
    surfaces
        .iter()
        .map(|surface| {
            let generation =
                match read_observed_source_generation(layout, workspace_name, &surface.source_name)
                {
                    Ok(generation) => generation,
                    Err(error) => {
                        tracing::warn!(
                            workspace = %workspace_name,
                            source = %surface.source_name,
                            error = %error,
                            "failed to read observed-value source generation"
                        );
                        None
                    }
                };
            (surface.source_name.clone(), generation)
        })
        .collect()
}

fn read_observed_source_generation(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &str,
) -> Result<Option<String>, std::io::Error> {
    let path = observed_source_generation_file(layout, workspace_name, source_name);
    match fs::read_to_string(path) {
        Ok(generation) => Ok(Some(generation.trim().to_string())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn observed_source_generation_file(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &str,
) -> std::path::PathBuf {
    layout
        .search_dir(workspace_name)
        .join(SOURCE_GENERATION_DIR_NAME)
        .join(source_name)
}

pub(crate) fn observed_queue_foreground_drain_budget(layout: &AppStateLayout) -> StdDuration {
    let config = observed_search_config_or_default(layout);
    StdDuration::from_millis(
        config
            .search
            .queue_foreground_drain_ms
            .unwrap_or(DEFAULT_OBSERVED_QUEUE_FOREGROUND_DRAIN_MS),
    )
}

pub(crate) fn observed_storage_budget_bytes(layout: &AppStateLayout) -> u64 {
    let config = observed_search_config_or_default(layout);
    observed_storage_budget_bytes_from_config(&config.search)
}

fn observed_search_config_or_default(layout: &AppStateLayout) -> ObservedSearchConfigFile {
    match load_observed_search_config(layout) {
        Ok(config) => config,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "failed to load observed-value search config; using defaults"
            );
            ObservedSearchConfigFile::default()
        }
    }
}

fn observed_storage_budget_bytes_from_config(config: &ObservedSearchConfig) -> u64 {
    config
        .storage_mb
        .unwrap_or(DEFAULT_OBSERVED_MAX_STORAGE_MB)
        .saturating_mul(BYTES_PER_MIB)
}

fn observed_collection_budget_from_config(
    config: &ObservedSearchConfig,
) -> ObservedCollectionBudget {
    ObservedCollectionBudget {
        candidates: config
            .collection_candidates
            .unwrap_or(DEFAULT_OBSERVED_COLLECTION_MAX_CANDIDATES),
        candidate_bytes: config
            .collection_candidate_bytes
            .unwrap_or(DEFAULT_OBSERVED_COLLECTION_MAX_CANDIDATE_BYTES),
        json_depth: config
            .collection_json_depth
            .unwrap_or(DEFAULT_OBSERVED_COLLECTION_MAX_JSON_DEPTH),
    }
}

fn load_observed_search_config(
    layout: &AppStateLayout,
) -> Result<ObservedSearchConfigFile, ObservedValueIndexError> {
    if !layout.config_file().exists() {
        return Ok(ObservedSearchConfigFile::default());
    }

    let raw = std::fs::read_to_string(layout.config_file())?;
    Ok(toml::from_str(&raw)?)
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

struct SelectedObservedSurface<'a> {
    surface: &'a ObservedSurface,
    aliases: Vec<String>,
}

impl SelectedObservedSurface<'_> {
    fn qualifiers(&self) -> Vec<String> {
        let mut qualifiers = surface_qualifiers(self.surface);
        qualifiers.extend(self.aliases.iter().map(|alias| normalize_identifier(alias)));
        qualifiers.sort();
        qualifiers.dedup();
        qualifiers
    }
}

fn observed_surfaces(selected_sources: &[QuerySource]) -> Vec<ObservedSurface> {
    let mut surfaces = Vec::new();
    for source in selected_sources {
        let source_name = source.source_name().to_string();
        for component in source.components() {
            match component {
                RuntimeSourceComponent::Http(http) => {
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
                RuntimeSourceComponent::File(file) => {
                    for table in &file.tables {
                        surfaces.push(observed_surface(
                            &source_name,
                            ObservedValueSurfaceKind::Table,
                            table.name(),
                            table.columns(),
                        ));
                    }
                }
                RuntimeSourceComponent::Mcp(mcp) => {
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
    if query.with.is_some() {
        return empty;
    }
    let Some(select) = query.body.as_select() else {
        return empty;
    };
    let Some(selected_surface) = select_from_surface(select, surfaces) else {
        return empty;
    };
    let surface = selected_surface.surface;

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

    let qualifiers = selected_surface.qualifiers();
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
) -> Option<SelectedObservedSurface<'a>> {
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
) -> Option<SelectedObservedSurface<'a>> {
    match &from.relation {
        TableFactor::Table {
            name, args, alias, ..
        } => {
            let expected_kind = if args.is_some() {
                Some(ObservedValueSurfaceKind::TableFunction)
            } else {
                Some(ObservedValueSurfaceKind::Table)
            };
            resolve_surface_name(name, expected_kind, surfaces).map(|surface| {
                SelectedObservedSurface {
                    surface,
                    aliases: table_alias_names(alias.as_ref()),
                }
            })
        }
        TableFactor::Function { name, alias, .. } => resolve_surface_name(
            name,
            Some(ObservedValueSurfaceKind::TableFunction),
            surfaces,
        )
        .map(|surface| SelectedObservedSurface {
            surface,
            aliases: table_alias_names(alias.as_ref()),
        }),
        _ => None,
    }
}

fn table_alias_names(alias: Option<&sqlparser::ast::TableAlias>) -> Vec<String> {
    alias
        .map(|alias| vec![alias.name.value.clone()])
        .unwrap_or_default()
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
    budget: ObservedCollectionBudget,
) -> Result<ObservedRecordCollection, ObservedValueIndexError> {
    let mut accumulator = ObservedRecordAccumulator::new(budget);
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
                let candidates =
                    observed_candidate_values(field_provenance, value, budget.json_depth);
                if candidates.depth_exhausted {
                    accumulator.mark_budget_exhausted();
                }
                for candidate in &candidates.values {
                    accumulator.push(field_provenance, candidate);
                }
            }
        }
    }
    Ok(accumulator.finish())
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedCandidateValue {
    field_path: String,
    display_value: String,
    searchable_text: String,
    normalized_value_key: String,
}

#[derive(Debug, Clone)]
struct ObservedCandidateCollection {
    values: Vec<ObservedCandidateValue>,
    depth_exhausted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ObservedCollectionBudget {
    candidates: usize,
    candidate_bytes: usize,
    json_depth: usize,
}

impl Default for ObservedCollectionBudget {
    fn default() -> Self {
        Self {
            candidates: DEFAULT_OBSERVED_COLLECTION_MAX_CANDIDATES,
            candidate_bytes: DEFAULT_OBSERVED_COLLECTION_MAX_CANDIDATE_BYTES,
            json_depth: DEFAULT_OBSERVED_COLLECTION_MAX_JSON_DEPTH,
        }
    }
}

#[derive(Debug, Clone)]
struct ObservedRecordCollection {
    record_chunks: Vec<Vec<ObservedValueRecord>>,
    budget_exhausted: bool,
    accepted_candidates: usize,
    accepted_candidate_bytes: usize,
    skipped_oversize_candidates: usize,
}

impl ObservedRecordCollection {
    fn is_empty(&self) -> bool {
        self.record_chunks.iter().all(Vec::is_empty)
    }
}

#[derive(Debug)]
struct ObservedRecordAccumulator {
    budget: ObservedCollectionBudget,
    records: BTreeMap<ObservedValueRecordKey, ObservedValueRecord>,
    record_chunks: Vec<Vec<ObservedValueRecord>>,
    window_candidates: usize,
    window_candidate_bytes: usize,
    accepted_candidates: usize,
    accepted_candidate_bytes: usize,
    skipped_oversize_candidates: usize,
    budget_exhausted: bool,
}

impl ObservedRecordAccumulator {
    fn new(budget: ObservedCollectionBudget) -> Self {
        Self {
            budget,
            records: BTreeMap::new(),
            record_chunks: Vec::new(),
            window_candidates: 0,
            window_candidate_bytes: 0,
            accepted_candidates: 0,
            accepted_candidate_bytes: 0,
            skipped_oversize_candidates: 0,
            budget_exhausted: false,
        }
    }

    fn mark_budget_exhausted(&mut self) {
        self.budget_exhausted = true;
    }

    fn push(&mut self, provenance: &FieldProvenance, candidate: &ObservedCandidateValue) {
        let candidate_bytes = candidate_bytes(candidate);
        if self.budget.candidates == 0
            || self.budget.candidate_bytes == 0
            || candidate_bytes > self.budget.candidate_bytes
        {
            self.budget_exhausted = true;
            self.skipped_oversize_candidates = self.skipped_oversize_candidates.saturating_add(1);
            return;
        }

        if self.window_candidates >= self.budget.candidates
            || self.window_candidate_bytes.saturating_add(candidate_bytes)
                > self.budget.candidate_bytes
        {
            self.budget_exhausted = true;
            self.flush_current_chunk();
        }

        self.window_candidates = self.window_candidates.saturating_add(1);
        self.window_candidate_bytes = self.window_candidate_bytes.saturating_add(candidate_bytes);
        self.accepted_candidates = self.accepted_candidates.saturating_add(1);
        self.accepted_candidate_bytes = self
            .accepted_candidate_bytes
            .saturating_add(candidate_bytes);
        self.insert(provenance, candidate);
    }

    fn insert(&mut self, provenance: &FieldProvenance, candidate: &ObservedCandidateValue) {
        let key = ObservedValueRecordKey {
            source_name: provenance.source_name.clone(),
            surface_kind: provenance.surface_kind,
            surface_name: provenance.surface_name.clone(),
            column_name: candidate.field_path.clone(),
            normalized_value_key: candidate.normalized_value_key.clone(),
        };
        self.records
            .entry(key)
            .and_modify(|record| {
                record.observed_count = record.observed_count.saturating_add(1);
            })
            .or_insert_with(|| ObservedValueRecord {
                source_name: provenance.source_name.clone(),
                surface_kind: provenance.surface_kind,
                surface_name: provenance.surface_name.clone(),
                column_name: candidate.field_path.clone(),
                normalized_value_key: candidate.normalized_value_key.clone(),
                display_value: candidate.display_value.clone(),
                searchable_text: candidate.searchable_text.clone(),
                suggested_operator: ObservedValueSuggestedOperator::Exact,
                observed_count: 1,
            });
    }

    fn flush_current_chunk(&mut self) {
        if !self.records.is_empty() {
            self.record_chunks
                .push(std::mem::take(&mut self.records).into_values().collect());
        }
        self.window_candidates = 0;
        self.window_candidate_bytes = 0;
    }

    fn finish(mut self) -> ObservedRecordCollection {
        self.flush_current_chunk();
        ObservedRecordCollection {
            record_chunks: self.record_chunks,
            budget_exhausted: self.budget_exhausted,
            accepted_candidates: self.accepted_candidates,
            accepted_candidate_bytes: self.accepted_candidate_bytes,
            skipped_oversize_candidates: self.skipped_oversize_candidates,
        }
    }
}

fn candidate_bytes(candidate: &ObservedCandidateValue) -> usize {
    candidate
        .field_path
        .len()
        .saturating_add(candidate.display_value.len())
        .saturating_add(candidate.searchable_text.len())
}

fn observed_candidate_values(
    provenance: &FieldProvenance,
    value: &Value,
    max_json_depth: usize,
) -> ObservedCandidateCollection {
    let mut candidates = BTreeMap::<(String, String), ObservedCandidateValue>::new();
    let mut depth_exhausted = false;
    collect_observed_candidates(
        provenance,
        &provenance.column_name,
        value,
        0,
        max_json_depth,
        &mut depth_exhausted,
        &mut candidates,
    );
    ObservedCandidateCollection {
        values: candidates.into_values().collect(),
        depth_exhausted,
    }
}

fn collect_observed_candidates(
    provenance: &FieldProvenance,
    field_path: &str,
    value: &Value,
    depth: usize,
    max_json_depth: usize,
    depth_exhausted: &mut bool,
    candidates: &mut BTreeMap<(String, String), ObservedCandidateValue>,
) {
    if depth > max_json_depth {
        *depth_exhausted = true;
        return;
    }

    match value {
        Value::Null => {}
        Value::String(value) => {
            collect_string_candidates(
                provenance,
                field_path,
                value,
                depth,
                max_json_depth,
                depth_exhausted,
                candidates,
            );
        }
        Value::Bool(value) => push_observed_candidate(
            provenance,
            field_path,
            if *value { "true" } else { "false" },
            candidates,
        ),
        Value::Number(value) => {
            push_observed_candidate(provenance, field_path, &value.to_string(), candidates);
        }
        Value::Array(items) => {
            if !contains_sensitive_observed_path(field_path, value)
                && !json_depth_exceeds(value, depth, max_json_depth)
                && let Ok(display_value) = serde_json::to_string(value)
            {
                push_observed_candidate(provenance, field_path, &display_value, candidates);
            }
            for item in items {
                collect_observed_candidates(
                    provenance,
                    field_path,
                    item,
                    depth.saturating_add(1),
                    max_json_depth,
                    depth_exhausted,
                    candidates,
                );
            }
        }
        Value::Object(object) => {
            if !contains_sensitive_observed_path(field_path, value)
                && !json_depth_exceeds(value, depth, max_json_depth)
                && let Ok(display_value) = serde_json::to_string(value)
            {
                push_observed_candidate(provenance, field_path, &display_value, candidates);
            }
            for (key, value) in object {
                let child_path = observed_field_path(field_path, key);
                collect_observed_candidates(
                    provenance,
                    &child_path,
                    value,
                    depth.saturating_add(1),
                    max_json_depth,
                    depth_exhausted,
                    candidates,
                );
            }
        }
    }
}

fn collect_string_candidates(
    provenance: &FieldProvenance,
    field_path: &str,
    value: &str,
    depth: usize,
    max_json_depth: usize,
    depth_exhausted: &mut bool,
    candidates: &mut BTreeMap<(String, String), ObservedCandidateValue>,
) {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return;
    }

    let parsed_json = looks_like_json_container(trimmed)
        .then(|| serde_json::from_str::<Value>(trimmed).ok())
        .flatten();
    let key_value_pairs = key_value_pairs(trimmed);
    let raw_contains_sensitive_child = parsed_json
        .as_ref()
        .is_some_and(|value| contains_sensitive_observed_path(field_path, value))
        || contains_sensitive_raw_value(trimmed)
        || key_value_pairs
            .iter()
            .any(|pair| is_sensitive_column(&observed_field_path(field_path, &pair.key)));
    let raw_exceeds_json_depth = parsed_json
        .as_ref()
        .is_some_and(|value| json_depth_exceeds(value, depth, max_json_depth));

    if !raw_contains_sensitive_child && !raw_exceeds_json_depth {
        push_observed_candidate(provenance, field_path, trimmed, candidates);
    }

    if let Some(parsed) = parsed_json {
        collect_observed_candidates(
            provenance,
            field_path,
            &parsed,
            depth,
            max_json_depth,
            depth_exhausted,
            candidates,
        );
    }

    for pair in key_value_pairs {
        if depth.saturating_add(1) > max_json_depth {
            *depth_exhausted = true;
            break;
        }
        let child_path = observed_field_path(field_path, &pair.key);
        push_observed_candidate(provenance, &child_path, &pair.value, candidates);
    }
}

fn push_observed_candidate(
    provenance: &FieldProvenance,
    field_path: &str,
    display_value: &str,
    candidates: &mut BTreeMap<(String, String), ObservedCandidateValue>,
) {
    let display_value = display_value.trim();
    if field_path.is_empty()
        || display_value.is_empty()
        || !display_value.chars().any(char::is_alphanumeric)
        || is_sensitive_column(field_path)
    {
        return;
    }

    let normalized_value_key = normalized_value_key(display_value);
    let key = (field_path.to_string(), normalized_value_key.clone());
    if candidates.contains_key(&key) {
        return;
    }

    let searchable_text = observed_searchable_text(provenance, field_path, display_value);
    candidates.insert(
        key,
        ObservedCandidateValue {
            field_path: field_path.to_string(),
            display_value: display_value.to_string(),
            searchable_text,
            normalized_value_key,
        },
    );
}

fn observed_searchable_text(
    provenance: &FieldProvenance,
    field_path: &str,
    display_value: &str,
) -> String {
    [
        provenance.source_name.as_str(),
        provenance.surface_name.as_str(),
        provenance.column_name.as_str(),
        field_path,
        display_value,
    ]
    .into_iter()
    .filter(|part| !part.trim().is_empty())
    .collect::<Vec<_>>()
    .join(" ")
}

fn observed_field_path(parent: &str, child: &str) -> String {
    if parent.is_empty() {
        child.to_string()
    } else {
        format!("{parent}.{child}")
    }
}

fn contains_sensitive_observed_path(field_path: &str, value: &Value) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, value)| {
            let child_path = observed_field_path(field_path, key);
            is_sensitive_column(&child_path) || contains_sensitive_observed_path(&child_path, value)
        }),
        Value::Array(items) => items
            .iter()
            .any(|item| contains_sensitive_observed_path(field_path, item)),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
}

fn json_depth_exceeds(value: &Value, depth: usize, max_depth: usize) -> bool {
    if depth > max_depth {
        return true;
    }
    match value {
        Value::Array(items) => items
            .iter()
            .any(|item| json_depth_exceeds(item, depth.saturating_add(1), max_depth)),
        Value::Object(object) => object
            .values()
            .any(|child| json_depth_exceeds(child, depth.saturating_add(1), max_depth)),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
}

fn contains_sensitive_raw_value(value: &str) -> bool {
    contains_sensitive_assignment_key(value) || starts_with_credential_scheme(value)
}

fn contains_sensitive_assignment_key(value: &str) -> bool {
    value
        .char_indices()
        .filter(|(_index, character)| matches!(character, ':' | '='))
        .filter_map(|(separator_index, _separator)| {
            sensitive_key_before_separator(value, separator_index)
        })
        .any(is_sensitive_column)
}

fn sensitive_key_before_separator(value: &str, separator_index: usize) -> Option<&str> {
    value
        .get(..separator_index)?
        .rsplit(|character: char| !is_key_char(character))
        .find(|part| !part.is_empty() && is_key_candidate(part))
}

fn starts_with_credential_scheme(value: &str) -> bool {
    let lower = value.trim_start().to_ascii_lowercase();
    lower.starts_with("bearer ") || lower.starts_with("basic ")
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyValuePair {
    key: String,
    value: String,
}

fn key_value_pairs(value: &str) -> Vec<KeyValuePair> {
    let Some(pairs) = parse_key_value_pairs(value) else {
        return Vec::new();
    };
    if pairs.len() >= 2 || looks_like_single_key_value_bag(value, &pairs) {
        pairs
    } else {
        Vec::new()
    }
}

fn parse_key_value_pairs(value: &str) -> Option<Vec<KeyValuePair>> {
    let mut pairs = Vec::new();
    let mut remaining = value.trim();
    while !remaining.is_empty() {
        remaining = trim_pair_separator_prefix(remaining);
        if remaining.is_empty() {
            break;
        }

        let key_len = remaining
            .char_indices()
            .take_while(|(_index, character)| is_key_char(*character))
            .map(|(index, character)| index + character.len_utf8())
            .last()
            .unwrap_or(0);
        if key_len == 0 {
            return None;
        }
        let key = remaining.get(..key_len)?;
        if !is_key_candidate(key) {
            return None;
        }

        remaining = remaining.get(key_len..)?.trim_start();
        let separator = remaining.chars().next()?;
        if !matches!(separator, ':' | '=') {
            return None;
        }
        remaining = remaining.get(separator.len_utf8()..)?.trim_start();
        let (pair_value, next) = parse_key_value_pair_value(remaining)?;
        if pair_value.trim().is_empty() || !pair_value.chars().any(char::is_alphanumeric) {
            return None;
        }
        pairs.push(KeyValuePair {
            key: key.to_string(),
            value: pair_value.trim().to_string(),
        });
        remaining = next;
    }
    (!pairs.is_empty()).then_some(pairs)
}

fn trim_pair_separator_prefix(value: &str) -> &str {
    value.trim_start_matches(|character: char| {
        character.is_whitespace() || matches!(character, ',' | ';')
    })
}

fn parse_key_value_pair_value(value: &str) -> Option<(&str, &str)> {
    if let Some(quote) = value
        .chars()
        .next()
        .filter(|character| matches!(character, '"' | '\''))
    {
        let after_quote = value.get(quote.len_utf8()..)?;
        let end_index = after_quote.find(quote)?;
        let pair_value = after_quote.get(..end_index)?;
        let next = after_quote.get(end_index + quote.len_utf8()..)?;
        return Some((pair_value, next));
    }

    let value_len = value
        .char_indices()
        .take_while(|(_index, character)| {
            !character.is_whitespace() && !matches!(character, ',' | ';' | '\n' | '\r' | '\t')
        })
        .map(|(index, character)| index + character.len_utf8())
        .last()
        .unwrap_or(0);
    (value_len > 0).then(|| {
        (
            value
                .get(..value_len)
                .expect("value_len is a char boundary"),
            value
                .get(value_len..)
                .expect("value_len is a char boundary"),
        )
    })
}

fn looks_like_single_key_value_bag(value: &str, pairs: &[KeyValuePair]) -> bool {
    pairs.len() == 1 && !value.chars().any(char::is_whitespace) && !looks_like_url(value)
}

fn looks_like_json_container(value: &str) -> bool {
    (value.starts_with('{') && value.ends_with('}'))
        || (value.starts_with('[') && value.ends_with(']'))
}

fn is_key_char(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
}

fn is_key_candidate(key: &str) -> bool {
    key.chars()
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        && key.chars().any(char::is_alphabetic)
}

fn looks_like_url(value: &str) -> bool {
    value.contains("://") || value.starts_with("www.")
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
    let compact = normalized.replace('_', "");
    [
        "api_key",
        "apikey",
        "access_key",
        "accesskey",
        "auth",
        "authorization",
        "card_number",
        "card_num",
        "client_secret",
        "cookie",
        "credit_card",
        "cvc",
        "cvv",
        "debit_card",
        "credential",
        "credentials",
        "drivers_license",
        "id_token",
        "password",
        "passport_number",
        "private_key",
        "refresh_token",
        "secret",
        "session",
        "set_cookie",
        "social_security",
        "social_security_number",
        "ssn",
        "tax_id",
        "tax_identification_number",
        "taxpayer_id",
        "tin_number",
        "token",
        "x_api_key",
    ]
    .into_iter()
    .any(|needle| normalized.contains(needle) || compact.contains(&needle.replace('_', "")))
}

fn same_identifier(left: &str, right: &str) -> bool {
    normalize_identifier(left) == normalize_identifier(right)
}

fn normalize_identifier(value: &str) -> String {
    let mut normalized = String::new();
    let mut previous_was_separator = true;
    let mut previous_was_lower_or_digit = false;

    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            if ch.is_ascii_uppercase()
                && previous_was_lower_or_digit
                && !previous_was_separator
                && !normalized.ends_with('_')
            {
                normalized.push('_');
            }
            normalized.push(ch.to_ascii_lowercase());
            previous_was_separator = false;
            previous_was_lower_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
        } else if !normalized.is_empty() && !normalized.ends_with('_') {
            normalized.push('_');
            previous_was_separator = true;
            previous_was_lower_or_digit = false;
        } else {
            previous_was_separator = true;
            previous_was_lower_or_digit = false;
        }
    }

    normalized.trim_matches('_').to_string()
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
            Field::new("tags", DataType::Utf8, true),
            Field::new("api_token", DataType::Utf8, true),
            Field::new("privateKey", DataType::Utf8, true),
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
                    r#"{"error":"timeout","region":"us-east-1","api_token":"nested-secret","privateKey":"nested-private-key","private-key":"nested-hyphen-private-key"}"#,
                    r#"{"error":"deploy_failed","sha":"abc123"}"#,
                ])),
                Arc::new(StringArray::from(vec![
                    "env:prod,kube_deployment:titaness-worker,service:titaness-worker",
                    "env=prod service=billing-worker status=error",
                ])),
                Arc::new(StringArray::from(vec!["secret-token", "another-secret"])),
                Arc::new(StringArray::from(vec![
                    "direct-private-key",
                    "another-direct-private-key",
                ])),
            ],
        )
        .expect("batch");

        indexer
            .observe_result(
                "SELECT service, body, payload, tags, api_token, privateKey FROM fixture.messages",
                &schema,
                &[batch],
            )
            .expect("observer does not fail SQL");

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        drain_observed_queue(&store);
        let hits = store
            .search_observed_values(&workspace, &["deploy_failed".to_string()], 10)
            .expect("search observed");
        assert!(hits.iter().any(|hit| hit.column_name == "payload.error"));

        let hits = store
            .search_observed_values(&workspace, &["abc123".to_string()], 10)
            .expect("search observed");
        assert!(hits.iter().any(|hit| hit.column_name == "payload.sha"));
        assert!(hits.iter().any(|hit| hit.column_name == "payload"));

        let hits = store
            .search_observed_values(&workspace, &["titaness-worker".to_string()], 10)
            .expect("search observed");
        assert!(hits.iter().any(|hit| hit.column_name == "tags.service"));
        assert!(
            hits.iter()
                .any(|hit| hit.column_name == "tags.kube_deployment")
        );
        assert!(hits.iter().any(|hit| hit.column_name == "tags"));

        let hits = store
            .search_observed_values(&workspace, &["rollback".to_string()], 10)
            .expect("search observed");
        assert!(hits.iter().any(|hit| hit.column_name == "body"));

        let hits = store
            .search_observed_values(&workspace, &["secret-token".to_string()], 10)
            .expect("search sensitive");
        assert!(hits.is_empty());

        let hits = store
            .search_observed_values(&workspace, &["nested-secret".to_string()], 10)
            .expect("search nested sensitive");
        assert!(hits.is_empty());

        let hits = store
            .search_observed_values(&workspace, &["direct-private-key".to_string()], 10)
            .expect("search camel-case sensitive");
        assert!(hits.is_empty());

        let hits = store
            .search_observed_values(&workspace, &["nested-private-key".to_string()], 10)
            .expect("search nested camel-case sensitive");
        assert!(hits.is_empty());

        let hits = store
            .search_observed_values(&workspace, &["nested-hyphen-private-key".to_string()], 10)
            .expect("search nested hyphenated sensitive");
        assert!(hits.is_empty());
    }

    #[test]
    fn skips_obvious_pii_and_payment_columns() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = query_source("fixture");
        let indexer = ObservedValueIndexer::new(layout.clone(), workspace.clone(), &[source]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("ssn", DataType::Utf8, true),
            Field::new("social_security_number", DataType::Utf8, true),
            Field::new("credit_card", DataType::Utf8, true),
            Field::new("card_number", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["123-45-6789"])),
                Arc::new(StringArray::from(vec!["987-65-4321"])),
                Arc::new(StringArray::from(vec!["4111111111111111"])),
                Arc::new(StringArray::from(vec!["5555555555554444"])),
            ],
        )
        .expect("batch");

        indexer
            .observe_result(
                "SELECT ssn, social_security_number, credit_card, card_number FROM fixture.messages",
                &schema,
                &[batch],
            )
            .expect("observer does not fail SQL");

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        drain_observed_queue(&store);
        for sensitive_value in [
            "123-45-6789",
            "987-65-4321",
            "4111111111111111",
            "5555555555554444",
        ] {
            assert!(
                store
                    .search_observed_values(&workspace, &[sensitive_value.to_string()], 10)
                    .expect("search sensitive value")
                    .is_empty()
            );
        }
    }

    #[test]
    fn skips_raw_credential_strings_in_non_sensitive_columns() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = query_source("fixture");
        let indexer = ObservedValueIndexer::new(layout.clone(), workspace.clone(), &[source]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("http_response", DataType::Utf8, true),
            Field::new("params", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    "Authorization: Bearer header-secret-token",
                    "status=ok latency_ms=12",
                ])),
                Arc::new(StringArray::from(vec![
                    "access_key=raw-access-key region=us-east-1",
                    "region=us-east-1 status=ok",
                ])),
            ],
        )
        .expect("batch");

        indexer
            .observe_result(
                "SELECT http_response, params FROM fixture.messages",
                &schema,
                &[batch],
            )
            .expect("observer does not fail SQL");

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        drain_observed_queue(&store);
        let hits = store
            .search_observed_values(&workspace, &["header-secret-token".to_string()], 10)
            .expect("search raw header secret");
        assert!(hits.is_empty());

        let hits = store
            .search_observed_values(&workspace, &["raw-access-key".to_string()], 10)
            .expect("search raw access key");
        assert!(hits.is_empty());
    }

    #[test]
    fn indexes_aliased_direct_projection_values() {
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
                "SELECT msg.service FROM fixture.messages AS msg",
                &schema,
                &[batch],
            )
            .expect("observer does not fail SQL");

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        drain_observed_queue(&store);
        let hits = store
            .search_observed_values(&workspace, &["payments-api".to_string()], 10)
            .expect("search observed");
        assert!(hits.iter().any(|hit| hit.column_name == "service"));
    }

    #[test]
    fn skips_observation_when_source_generation_changes_before_enqueue() {
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

        mark_observed_source_generation(
            &layout,
            &workspace,
            &SourceName::parse("fixture").expect("source"),
        )
        .expect("mark source generation");
        indexer
            .observe_result("SELECT service FROM fixture.messages", &schema, &[batch])
            .expect("observer does not fail SQL");

        assert!(
            SearchIndexStore::open_existing_workspace(&layout, &workspace)
                .expect("open existing search index")
                .is_none()
        );
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
        drain_observed_queue(&store);
        let hits = store
            .search_observed_values(&workspace, &["payments-api".to_string()], 10)
            .expect("search observed");
        assert!(hits.is_empty());
    }

    #[test]
    fn skips_cte_queries_to_avoid_derived_value_misattribution() {
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
                "WITH messages AS (SELECT 'payments-api' AS service) SELECT service FROM messages",
                &schema,
                &[batch],
            )
            .expect("observer does not fail SQL");

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        drain_observed_queue(&store);
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
        drain_observed_queue(&store);
        let hits = store
            .search_observed_values(&workspace, &["payments-api".to_string()], 10)
            .expect("search observed");
        assert_eq!(hits.len(), 1);
        assert_eq!(
            store
                .observed_count_for_test("payments-api")
                .expect("observed state")
                .expect("observed count"),
            3
        );
    }

    #[test]
    fn clears_catalog_fingerprint_when_building_search_index() {
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
        let fingerprint_path = layout
            .search_dir(&workspace)
            .join(CATALOG_FINGERPRINT_FILE_NAME);
        fs::create_dir_all(fingerprint_path.parent().expect("fingerprint parent"))
            .expect("search dir");
        fs::write(&fingerprint_path, "stale-fingerprint\n").expect("fingerprint");

        indexer
            .observe_result("SELECT service FROM fixture.messages", &schema, &[batch])
            .expect("observer does not fail SQL");

        assert!(!fingerprint_path.exists());
        let store = SearchIndexStore::open_existing_workspace(&layout, &workspace)
            .expect("open existing search index")
            .expect("search index exists");
        drain_observed_queue(&store);
        let hits = store
            .search_observed_values(&workspace, &["payments-api".to_string()], 10)
            .expect("search observed");
        assert!(hits.iter().any(|hit| hit.column_name == "service"));
    }

    #[test]
    fn observed_queue_foreground_drain_budget_loads_search_config() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        std::fs::write(
            layout.config_file(),
            r"
version = 1

[search]
observed_queue_foreground_drain_ms = 250
observed_max_storage_mb = 12
observed_collection_max_candidates = 3
observed_collection_max_candidate_bytes = 42
observed_collection_max_json_depth = 2
",
        )
        .expect("write config");

        assert_eq!(
            observed_queue_foreground_drain_budget(&layout),
            StdDuration::from_millis(250)
        );
        assert_eq!(observed_storage_budget_bytes(&layout), 12 * BYTES_PER_MIB);

        let config = observed_search_config_or_default(&layout);
        assert_eq!(
            observed_collection_budget_from_config(&config.search),
            ObservedCollectionBudget {
                candidates: 3,
                candidate_bytes: 42,
                json_depth: 2
            }
        );
    }

    #[test]
    fn observed_collection_candidate_budget_flushes_overflow_to_queue() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        std::fs::write(
            layout.config_file(),
            r"
version = 1

[search]
observed_collection_max_candidates = 1
",
        )
        .expect("write config");
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
                "budget-first",
                "budget-second",
            ]))],
        )
        .expect("batch");

        indexer
            .observe_result("SELECT service FROM fixture.messages", &schema, &[batch])
            .expect("observer does not fail SQL");

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        drain_observed_queue(&store);
        assert!(
            !store
                .search_observed_values(&workspace, &["budget-first".to_string()], 10)
                .expect("search first")
                .is_empty()
        );
        assert!(
            !store
                .search_observed_values(&workspace, &["budget-second".to_string()], 10)
                .expect("search second")
                .is_empty()
        );
    }

    #[test]
    fn observed_collection_depth_budget_does_not_index_deep_json_via_container() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        std::fs::write(
            layout.config_file(),
            r"
version = 1

[search]
observed_collection_max_json_depth = 1
",
        )
        .expect("write config");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = query_source("fixture");
        let indexer = ObservedValueIndexer::new(layout.clone(), workspace.clone(), &[source]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                r#"{"top":"budget-top","outer":{"inner":"budget-deep"}}"#,
            ]))],
        )
        .expect("batch");

        indexer
            .observe_result("SELECT payload FROM fixture.messages", &schema, &[batch])
            .expect("observer does not fail SQL");

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        drain_observed_queue(&store);
        assert!(
            !store
                .search_observed_values(&workspace, &["budget-top".to_string()], 10)
                .expect("search top value")
                .is_empty()
        );
        assert!(
            store
                .search_observed_values(&workspace, &["budget-deep".to_string()], 10)
                .expect("search deep value")
                .is_empty()
        );
    }

    fn drain_observed_queue(store: &SearchIndexStore) {
        for _attempt in 0..16 {
            let drain = store
                .drain_observed_value_queue_for(StdDuration::from_secs(1))
                .expect("drain observed queue");
            if drain.pending_jobs == 0 {
                return;
            }
        }
        panic!("observed queue still has pending jobs after test drain attempts");
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
                    {"name": "tags", "type": "Utf8"},
                    {"name": "http_response", "type": "Utf8"},
                    {"name": "params", "type": "Utf8"},
                    {"name": "api_token", "type": "Utf8"},
                    {"name": "privateKey", "type": "Utf8"},
                    {"name": "ssn", "type": "Utf8"},
                    {"name": "social_security_number", "type": "Utf8"},
                    {"name": "credit_card", "type": "Utf8"},
                    {"name": "card_number", "type": "Utf8"}
                ]
            }]
        }))
        .expect("manifest");
        QuerySource::new(manifest, BTreeMap::new(), BTreeMap::new())
    }
}
