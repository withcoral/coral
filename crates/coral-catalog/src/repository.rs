use coral_db::Transaction;
use sea_query::{Expr, ExprTrait as _, Iden, Order, Query};
use sqlx::FromRow;

mod retirement;
#[cfg(test)]
mod tests;

pub(super) use retirement::{delete_retired, due_retirements, retire_unused};

use crate::{
    BuiltinCatalogIdentity, CatalogDiagnostic, Error, ExecutionBinding, HttpMethod,
    MaterializedOperation, OperationAvailability, OperationKey, PreparedProviderCatalog,
    ProviderGenerationId, ProviderSetupInput, ProviderSetupInputDigest, ProviderSetupInputKind,
    SchemaGraph, SchemaId,
};

#[derive(Iden)]
enum ProviderGenerations {
    Table,
    Id,
    InputKind,
    PinnedInput,
    InputDigest,
    Configuration,
    CredentialContract,
    SchemaGraphJson,
    TypescriptView,
    MaterializerVersion,
    CreatedAtUnixNanos,
}

#[derive(Iden)]
enum MaterializedOperations {
    Table,
    ProviderGenerationId,
    OperationKey,
    RelativePath,
    Protocol,
    Method,
    Path,
    ServerUrl,
    Summary,
    Title,
    Description,
    Deprecated,
    TagsJson,
    Guidance,
    ReadOnlyHint,
    DestructiveHint,
    IdempotentHint,
    OpenWorldHint,
    DefinitionJson,
    InputSchemaJson,
    OutputSchemaJson,
    TypescriptDeclaration,
    Availability,
    DiagnosticCode,
    DiagnosticMessage,
    InputSchemaId,
    DiagnosticsJson,
    TypeReferencesJson,
}

const BUILTIN_INPUT_KIND: &str = "builtin";

/// The exact input one Generation pins: a Setup's input, or the identity of
/// the Built-in Provider's Coral-defined catalog.
#[derive(Clone, Copy)]
pub(super) enum GenerationInput<'a> {
    Setup(&'a ProviderSetupInput),
    Builtin(&'a BuiltinCatalogIdentity),
}

impl<'a> GenerationInput<'a> {
    const fn kind(self) -> &'static str {
        match self {
            Self::Setup(input) => input.kind().as_str(),
            Self::Builtin(_) => BUILTIN_INPUT_KIND,
        }
    }

    fn pinned(self) -> &'a str {
        match self {
            Self::Setup(input) => input.as_str(),
            Self::Builtin(identity) => identity.as_str(),
        }
    }

    fn configuration(self) -> Option<&'a crate::ProviderConfiguration> {
        match self {
            Self::Setup(input) => input.configuration(),
            Self::Builtin(_) => None,
        }
    }
}

/// A stored Generation's decoded pinned input.
enum StoredGenerationInput {
    Setup(ProviderSetupInput),
    Builtin(BuiltinCatalogIdentity),
}

impl StoredGenerationInput {
    fn from_storage(kind: &str, pinned: &str) -> Result<Self, Error> {
        if kind == BUILTIN_INPUT_KIND {
            return BuiltinCatalogIdentity::parse(pinned)
                .map(Self::Builtin)
                .ok_or_else(|| {
                    Error::CorruptData(
                        "built-in Generation pins a malformed catalog identity".to_string(),
                    )
                });
        }
        ProviderSetupInput::from_storage(ProviderSetupInputKind::from_storage(kind)?, pinned)
            .map(Self::Setup)
    }

    fn digest(&self) -> ProviderSetupInputDigest {
        match self {
            Self::Setup(input) => ProviderSetupInputDigest::of_setup_input(input),
            Self::Builtin(identity) => {
                ProviderSetupInputDigest::of_bytes(identity.as_str().as_bytes())
            }
        }
    }
}

#[derive(Iden)]
enum ProviderGenerationTypes {
    Table,
    ProviderGenerationId,
    Name,
    Declaration,
    ReferencesJson,
}

/// Shared types written per insert, within every database's bind limit.
const SHARED_TYPE_ROWS_PER_INSERT: usize = 256;
/// Shared type names looked up per select, within every database's bind limit.
const SHARED_TYPE_NAMES_PER_SELECT: usize = 256;

#[derive(Debug, FromRow)]
struct GenerationRow {
    id: String,
    input_kind: String,
    pinned_input: String,
    input_digest: String,
    credential_contract: String,
    schema_graph_json: String,
    typescript_view: String,
}

impl GenerationRow {
    fn decode(
        self,
    ) -> Result<(ProviderGenerationId, ProviderSetupInputDigest, SchemaGraph), Error> {
        let id = ProviderGenerationId::parse(&self.id).map_err(|_error| {
            Error::CorruptData("Provider Generation has invalid identity".to_string())
        })?;
        let pinned = StoredGenerationInput::from_storage(&self.input_kind, &self.pinned_input)
            .map_err(|_error| {
                Error::CorruptData("Provider Generation has invalid pinned input".to_string())
            })?;
        let input_digest = ProviderSetupInputDigest::parse(self.input_digest)?;
        crate::ProviderCredentialContract::from_storage(&self.credential_contract)?;
        if self.schema_graph_json.is_empty() || self.typescript_view.is_empty() {
            return Err(Error::CorruptData(
                "Provider Generation has incomplete credential or authoring material".to_string(),
            ));
        }
        if pinned.digest() != input_digest {
            return Err(Error::CorruptData(
                "Provider Generation pinned input does not match its digest".to_string(),
            ));
        }
        let schema_graph = SchemaGraph::from_storage_json(&self.schema_graph_json)?;
        Ok((id, input_digest, schema_graph))
    }
}

/// Declared effect hints; NULL is an undeclared hint.
#[derive(Debug, FromRow)]
struct HintColumns {
    #[sqlx(rename = "read_only_hint")]
    read_only: Option<bool>,
    #[sqlx(rename = "destructive_hint")]
    destructive: Option<bool>,
    #[sqlx(rename = "idempotent_hint")]
    idempotent: Option<bool>,
    #[sqlx(rename = "open_world_hint")]
    open_world: Option<bool>,
}

impl HintColumns {
    const fn decode(self) -> crate::OperationHints {
        crate::OperationHints::new(
            self.read_only,
            self.destructive,
            self.idempotent,
            self.open_world,
        )
    }
}

/// Reads an Operation's method column; NULL for a protocol other than HTTP.
fn decode_http_method(method: Option<&str>) -> Result<Option<HttpMethod>, Error> {
    method
        .map(|method| {
            HttpMethod::parse(method).ok_or_else(|| {
                Error::CorruptData("Materialized Operation has invalid HTTP method".to_string())
            })
        })
        .transpose()
}

#[derive(Debug, FromRow)]
struct SearchTargetRow {
    operation_key: String,
    relative_path: String,
    method: Option<String>,
    #[sqlx(flatten)]
    hints: HintColumns,
}

impl SearchTargetRow {
    fn decode(self) -> Result<crate::CatalogSearchTarget, Error> {
        let key = OperationKey::parse(&self.operation_key).map_err(|_error| {
            Error::CorruptData("search target has an invalid Operation Key".to_string())
        })?;
        if !crate::paths::valid_relative_path(&self.relative_path) {
            return Err(Error::CorruptData(
                "search target has an invalid Operation-relative path".to_string(),
            ));
        }
        Ok(crate::CatalogSearchTarget::new(
            key,
            self.relative_path,
            self.hints.decode(),
            decode_http_method(self.method.as_deref())?,
        ))
    }
}

#[derive(Debug, FromRow)]
struct OperationSummaryRow {
    operation_key: String,
    relative_path: String,
    summary: String,
    description: String,
    tags_json: String,
    #[sqlx(flatten)]
    hints: HintColumns,
    availability: String,
    diagnostics_json: Option<String>,
}

impl OperationSummaryRow {
    fn decode(self) -> Result<crate::OperationSummary, Error> {
        let key = OperationKey::parse(&self.operation_key)
            .map_err(|_error| Error::CorruptData("Operation summary has invalid key".into()))?;
        if !crate::paths::valid_relative_path(&self.relative_path) || self.summary.is_empty() {
            return Err(Error::CorruptData(
                "Operation summary has invalid path or summary".into(),
            ));
        }
        if !crate::types::valid_presentation_text(&self.description) {
            return Err(Error::CorruptData(
                "Operation summary has invalid description".into(),
            ));
        }
        let tags: Vec<String> = serde_json::from_str(&self.tags_json)
            .map_err(|_error| Error::CorruptData("Operation summary tags are invalid".into()))?;
        if tags.iter().any(|tag| tag.trim().is_empty()) {
            return Err(Error::CorruptData(
                "Operation summary has an empty tag".into(),
            ));
        }
        let availability = OperationAvailability::from_storage_str(&self.availability)?;
        let diagnostics =
            CatalogDiagnostic::list_from_storage(self.diagnostics_json.as_deref().ok_or_else(
                || Error::CorruptData("Operation summary has no diagnostics".into()),
            )?)?;
        let unavailable = diagnostics
            .iter()
            .any(|d| d.severity() == crate::CatalogDiagnosticSeverity::Error);
        if unavailable != (availability == OperationAvailability::Unavailable) {
            return Err(Error::CorruptData(
                "Operation summary has inconsistent availability".into(),
            ));
        }
        Ok(crate::OperationSummary {
            key,
            relative_path: self.relative_path,
            summary: self.summary,
            description: self.description,
            tags,
            hints: self.hints.decode(),
            availability,
            diagnostics,
        })
    }
}

#[derive(Debug, FromRow)]
struct OperationRow {
    operation_key: String,
    relative_path: String,
    protocol: String,
    method: Option<String>,
    path: Option<String>,
    server_url: Option<String>,
    summary: String,
    title: Option<String>,
    description: String,
    deprecated: bool,
    tags_json: String,
    guidance: Option<String>,
    #[sqlx(flatten)]
    hints: HintColumns,
    definition_json: String,
    input_schema_json: String,
    output_schema_json: String,
    typescript_declaration: String,
    availability: String,
    diagnostic_code: Option<String>,
    diagnostic_message: Option<String>,
    input_schema_id: Option<String>,
    diagnostics_json: Option<String>,
    type_references_json: String,
}

impl OperationRow {
    #[expect(
        clippy::too_many_lines,
        reason = "one persisted Operation row validates every protocol-specific invariant"
    )]
    fn decode(self) -> Result<MaterializedOperation, Error> {
        let key = OperationKey::parse(&self.operation_key).map_err(|_error| {
            Error::CorruptData("Materialized Operation has invalid key".to_string())
        })?;
        if self.relative_path.is_empty()
            || self.summary.is_empty()
            || self.definition_json.is_empty()
            || self.input_schema_json.is_empty()
            || self.output_schema_json.is_empty()
        {
            return Err(Error::CorruptData(
                "Materialized Operation has incomplete binding or summary".to_string(),
            ));
        }
        if self.typescript_declaration.is_empty() {
            return Err(Error::CorruptData(
                "Materialized Operation has no TypeScript declaration".to_string(),
            ));
        }
        let type_references: Vec<String> = serde_json::from_str(&self.type_references_json)
            .map_err(|_error| {
                Error::CorruptData(
                    "Materialized Operation type references are not valid JSON".to_string(),
                )
            })?;
        let presentation = crate::OperationPresentation::new(
            self.title,
            self.description,
            self.deprecated,
            serde_json::from_str(&self.tags_json).map_err(|_error| {
                Error::CorruptData("Materialized Operation tags are not valid JSON".to_string())
            })?,
            self.guidance,
        );
        if !presentation.is_valid() {
            return Err(Error::CorruptData(
                "Materialized Operation has invalid presentation metadata".to_string(),
            ));
        }
        let binding = ExecutionBinding::from_storage_json(&self.protocol, &self.definition_json)?;
        let hints = self.hints.decode();
        let availability = OperationAvailability::from_storage_str(&self.availability)?;
        let stored_diagnostic = match (self.diagnostic_code, self.diagnostic_message) {
            (Some(code), Some(message)) => Some(CatalogDiagnostic::from_storage(code, message)?),
            (None, None) => None,
            _ => {
                return Err(Error::CorruptData(
                    "Materialized Operation has partial diagnostic evidence".to_string(),
                ));
            }
        };
        let input_schema = SchemaId::from_storage(self.input_schema_id.ok_or_else(|| {
            Error::CorruptData("Materialized Operation has no input schema identity".to_string())
        })?)?;
        let diagnostics_json = self.diagnostics_json.ok_or_else(|| {
            Error::CorruptData("Materialized Operation has no diagnostic collection".to_string())
        })?;
        let diagnostics = CatalogDiagnostic::list_from_storage(&diagnostics_json)?;
        let diagnostic = diagnostics
            .iter()
            .find(|diagnostic| diagnostic.severity() == crate::CatalogDiagnosticSeverity::Error);
        if diagnostic != stored_diagnostic.as_ref()
            || diagnostic.is_some() != (availability == OperationAvailability::Unavailable)
        {
            return Err(Error::CorruptData(
                "Materialized Operation has inconsistent availability evidence".to_string(),
            ));
        }
        let binding_json = match &binding {
            ExecutionBinding::Http(binding) => {
                let method = self
                    .method
                    .as_deref()
                    .and_then(HttpMethod::parse)
                    .ok_or_else(|| {
                        Error::CorruptData(
                            "Materialized Operation has invalid HTTP method".to_string(),
                        )
                    })?;
                let path = self.path.as_deref().ok_or_else(|| {
                    Error::CorruptData("HTTP Operation has no path index".to_string())
                })?;
                if binding.method() != method
                    || binding.path_template() != path
                    || binding.server_url() != self.server_url.as_deref()
                    || (availability == OperationAvailability::Available
                        && self.server_url.as_deref().is_none_or(str::is_empty))
                    || hints != crate::OperationHints::default()
                {
                    return Err(Error::CorruptData(
                        "Materialized Operation binding disagrees with its index".to_string(),
                    ));
                }
                self.definition_json
            }
            ExecutionBinding::Mcp(binding) => {
                if self.method.is_some() || self.path.is_some() || self.server_url.is_some() {
                    return Err(Error::CorruptData(
                        "MCP Operation has an HTTP index".to_string(),
                    ));
                }
                if binding.input_schema() != &input_schema {
                    return Err(Error::CorruptData(
                        "MCP Operation binding disagrees with its schema index".to_string(),
                    ));
                }
                self.definition_json
            }
            ExecutionBinding::Builtin(binding) => {
                if self.method.is_some() || self.path.is_some() || self.server_url.is_some() {
                    return Err(Error::CorruptData(
                        "built-in Operation has an HTTP index".to_string(),
                    ));
                }
                if binding.input_schema() != &input_schema || binding.operation_key() != &key {
                    return Err(Error::CorruptData(
                        "built-in Operation binding disagrees with its index".to_string(),
                    ));
                }
                self.definition_json
            }
        };
        Ok(MaterializedOperation::new(
            key,
            self.relative_path,
            self.summary,
            presentation,
            hints,
            binding,
            binding_json,
            input_schema,
            self.input_schema_json,
            self.output_schema_json,
            self.typescript_declaration,
            type_references,
            diagnostics,
            diagnostics_json,
        ))
    }
}

pub(super) async fn publish_generation(
    transaction: &mut Transaction<'_>,
    input: GenerationInput<'_>,
    input_digest: &ProviderSetupInputDigest,
    prepared: PreparedProviderCatalog,
    materializer_version: &str,
    created_at_unix_nanos: i64,
) -> Result<ProviderGenerationId, Error> {
    let generation_id = ProviderGenerationId::generate();
    let contract = prepared.credential_contract().to_storage()?;
    let parts = prepared.into_parts();
    insert_generation(
        transaction,
        &generation_id,
        input,
        input_digest,
        &contract,
        &parts.schema_graph_json,
        &parts.typescript_view,
        materializer_version,
        created_at_unix_nanos,
    )
    .await?;
    for operation in &parts.operations {
        insert_operation(transaction, &generation_id, operation).await?;
    }
    insert_shared_types(transaction, &generation_id, &parts.shared_types).await?;
    Ok(generation_id)
}

async fn insert_shared_types(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
    shared_types: &crate::schema::SharedTypes,
) -> Result<(), Error> {
    let rows = shared_types.iter().collect::<Vec<_>>();
    for chunk in rows.chunks(SHARED_TYPE_ROWS_PER_INSERT) {
        let mut statement = Query::insert()
            .into_table(ProviderGenerationTypes::Table)
            .columns([
                ProviderGenerationTypes::ProviderGenerationId,
                ProviderGenerationTypes::Name,
                ProviderGenerationTypes::Declaration,
                ProviderGenerationTypes::ReferencesJson,
            ])
            .to_owned();
        for (name, shared) in chunk {
            let references = serde_json::to_string(&shared.references).map_err(|_error| {
                Error::CorruptData("shared type references cannot be encoded".to_string())
            })?;
            statement.values_panic([
                Expr::val(generation_id.to_string()),
                Expr::val(*name),
                Expr::val(shared.declaration.as_str()),
                Expr::val(references),
            ]);
        }
        transaction.execute(statement).await?;
    }
    Ok(())
}

/// Loads the shared types of one Generation with the given names. A name with
/// no shared type is left out.
pub(super) async fn load_shared_types(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
    names: &[String],
) -> Result<Vec<(String, crate::schema::SharedType)>, Error> {
    #[derive(FromRow)]
    struct TypeRow {
        name: String,
        declaration: String,
        references_json: String,
    }
    let mut shared_types = Vec::with_capacity(names.len());
    for chunk in names.chunks(SHARED_TYPE_NAMES_PER_SELECT) {
        let rows: Vec<TypeRow> = transaction
            .fetch_all(
                Query::select()
                    .columns([
                        ProviderGenerationTypes::Name,
                        ProviderGenerationTypes::Declaration,
                        ProviderGenerationTypes::ReferencesJson,
                    ])
                    .from(ProviderGenerationTypes::Table)
                    .and_where(
                        Expr::col(ProviderGenerationTypes::ProviderGenerationId)
                            .eq(generation_id.to_string()),
                    )
                    .and_where(
                        Expr::col(ProviderGenerationTypes::Name)
                            .is_in(chunk.iter().map(String::as_str)),
                    )
                    .to_owned(),
            )
            .await?;
        for row in rows {
            let references = serde_json::from_str(&row.references_json).map_err(|_error| {
                Error::CorruptData("shared type references are not valid JSON".to_string())
            })?;
            shared_types.push((
                row.name,
                crate::schema::SharedType {
                    declaration: row.declaration,
                    references,
                },
            ));
        }
    }
    Ok(shared_types)
}

#[expect(
    clippy::too_many_arguments,
    reason = "one immutable Generation row receives its complete package evidence"
)]
async fn insert_generation(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
    input: GenerationInput<'_>,
    input_digest: &ProviderSetupInputDigest,
    credential_contract: &str,
    schema_graph_json: &str,
    typescript_view: &str,
    materializer_version: &str,
    created_at_unix_nanos: i64,
) -> Result<(), Error> {
    let statement = Query::insert()
        .into_table(ProviderGenerations::Table)
        .columns([
            ProviderGenerations::Id,
            ProviderGenerations::InputKind,
            ProviderGenerations::PinnedInput,
            ProviderGenerations::InputDigest,
            ProviderGenerations::Configuration,
            ProviderGenerations::CredentialContract,
            ProviderGenerations::SchemaGraphJson,
            ProviderGenerations::TypescriptView,
            ProviderGenerations::MaterializerVersion,
            ProviderGenerations::CreatedAtUnixNanos,
        ])
        .values_panic([
            Expr::val(generation_id.to_string()),
            Expr::val(input.kind()),
            Expr::val(input.pinned()),
            Expr::val(input_digest.as_str()),
            Expr::val(
                input
                    .configuration()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(|_invalid| Error::CorruptData("invalid configuration".into()))?,
            ),
            Expr::val(credential_contract),
            Expr::val(schema_graph_json),
            Expr::val(typescript_view),
            Expr::val(materializer_version),
            Expr::val(created_at_unix_nanos),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

async fn insert_operation(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
    operation: &MaterializedOperation,
) -> Result<(), Error> {
    let (method, path, server_url) = operation.http_index();
    let statement = Query::insert()
        .into_table(MaterializedOperations::Table)
        .columns([
            MaterializedOperations::ProviderGenerationId,
            MaterializedOperations::OperationKey,
            MaterializedOperations::RelativePath,
            MaterializedOperations::Protocol,
            MaterializedOperations::Method,
            MaterializedOperations::Path,
            MaterializedOperations::ServerUrl,
            MaterializedOperations::Summary,
            MaterializedOperations::Title,
            MaterializedOperations::Description,
            MaterializedOperations::Deprecated,
            MaterializedOperations::TagsJson,
            MaterializedOperations::Guidance,
            MaterializedOperations::ReadOnlyHint,
            MaterializedOperations::DestructiveHint,
            MaterializedOperations::IdempotentHint,
            MaterializedOperations::OpenWorldHint,
            MaterializedOperations::DefinitionJson,
            MaterializedOperations::InputSchemaJson,
            MaterializedOperations::OutputSchemaJson,
            MaterializedOperations::TypescriptDeclaration,
            MaterializedOperations::Availability,
            MaterializedOperations::DiagnosticCode,
            MaterializedOperations::DiagnosticMessage,
            MaterializedOperations::InputSchemaId,
            MaterializedOperations::DiagnosticsJson,
            MaterializedOperations::TypeReferencesJson,
        ])
        .values_panic([
            Expr::val(generation_id.to_string()),
            Expr::val(operation.key().as_str()),
            Expr::val(operation.relative_path()),
            Expr::val(operation.binding_kind()),
            Expr::val(method),
            Expr::val(path),
            Expr::val(server_url),
            Expr::val(operation.summary()),
            Expr::val(operation.presentation().title()),
            Expr::val(operation.presentation().description()),
            Expr::val(operation.presentation().deprecated()),
            Expr::val(
                serde_json::to_string(operation.presentation().tags()).map_err(|_error| {
                    Error::CorruptData("Operation tags cannot be encoded".to_string())
                })?,
            ),
            Expr::val(operation.presentation().guidance()),
            Expr::val(operation.hints().read_only()),
            Expr::val(operation.hints().destructive()),
            Expr::val(operation.hints().idempotent()),
            Expr::val(operation.hints().open_world()),
            Expr::val(operation.binding_json()),
            Expr::val(operation.input_schema_json()),
            Expr::val(operation.output_schema_json()),
            Expr::val(operation.typescript_declaration()),
            Expr::val(operation.availability().as_storage_str()),
            Expr::val(
                operation
                    .diagnostic()
                    .map(|diagnostic| diagnostic.code().to_string()),
            ),
            Expr::val(
                operation
                    .diagnostic()
                    .map(|diagnostic| diagnostic.message().to_string()),
            ),
            Expr::val(operation.input_schema().as_str()),
            Expr::val(operation.diagnostics_json()),
            Expr::val(
                serde_json::to_string(operation.type_references()).map_err(|_error| {
                    Error::CorruptData("Operation type references cannot be encoded".to_string())
                })?,
            ),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

/// Reads the Setup input kind a Generation pins; `None` for a built-in Generation.
pub(super) async fn load_setup_input_kind(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<Option<ProviderSetupInputKind>, Error> {
    let row: Option<(String,)> = transaction
        .fetch_optional(
            Query::select()
                .column(ProviderGenerations::InputKind)
                .from(ProviderGenerations::Table)
                .and_where(Expr::col(ProviderGenerations::Id).eq(generation_id.to_string()))
                .to_owned(),
        )
        .await?;
    let (kind,) =
        row.ok_or_else(|| Error::CorruptData("Provider has no active Generation".to_string()))?;
    if kind == BUILTIN_INPUT_KIND {
        return Ok(None);
    }
    ProviderSetupInputKind::from_storage(&kind).map(Some)
}

/// Reads the exact Setup input a Generation pins; `None` for a built-in Generation.
pub(super) async fn load_setup_input(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<Option<ProviderSetupInput>, Error> {
    let row = load_generation(transaction, generation_id).await?;
    if row.input_kind == BUILTIN_INPUT_KIND {
        row.decode()?;
        return Ok(None);
    }
    let input = ProviderSetupInput::from_storage(
        ProviderSetupInputKind::from_storage(&row.input_kind)?,
        &row.pinned_input,
    )?;
    row.decode()?;
    Ok(Some(input))
}

/// Reads the catalog identity a built-in Generation pins, checked against
/// the Generation's input digest.
pub(super) async fn load_builtin_identity(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<BuiltinCatalogIdentity, Error> {
    let row: Option<(String, String, String)> = transaction
        .fetch_optional(
            Query::select()
                .columns([
                    ProviderGenerations::InputKind,
                    ProviderGenerations::PinnedInput,
                    ProviderGenerations::InputDigest,
                ])
                .from(ProviderGenerations::Table)
                .and_where(Expr::col(ProviderGenerations::Id).eq(generation_id.to_string()))
                .to_owned(),
        )
        .await?;
    let (kind, pinned, digest) =
        row.ok_or_else(|| Error::CorruptData("Provider has no active Generation".to_string()))?;
    let pinned = StoredGenerationInput::from_storage(&kind, &pinned)?;
    if pinned.digest() != ProviderSetupInputDigest::parse(digest)? {
        return Err(Error::CorruptData(
            "Provider Generation pinned input does not match its digest".to_string(),
        ));
    }
    match pinned {
        StoredGenerationInput::Builtin(identity) => Ok(identity),
        StoredGenerationInput::Setup(_) => Err(Error::CorruptData(
            "the Built-in Provider's Generation pins a Setup input".to_string(),
        )),
    }
}

async fn load_generation(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<GenerationRow, Error> {
    transaction
        .fetch_optional(
            Query::select()
                .columns([
                    ProviderGenerations::Id,
                    ProviderGenerations::InputKind,
                    ProviderGenerations::PinnedInput,
                    ProviderGenerations::InputDigest,
                    ProviderGenerations::CredentialContract,
                    ProviderGenerations::SchemaGraphJson,
                    ProviderGenerations::TypescriptView,
                ])
                .from(ProviderGenerations::Table)
                .and_where(Expr::col(ProviderGenerations::Id).eq(generation_id.to_string()))
                .to_owned(),
        )
        .await?
        .ok_or_else(|| Error::CorruptData("Provider has no active Generation".to_string()))
}

pub(super) async fn load_configuration(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<Option<crate::ProviderConfiguration>, Error> {
    #[derive(FromRow)]
    struct ConfigurationRow {
        configuration: Option<String>,
    }
    let row: Option<ConfigurationRow> = transaction
        .fetch_optional(
            Query::select()
                .column(ProviderGenerations::Configuration)
                .from(ProviderGenerations::Table)
                .and_where(Expr::col(ProviderGenerations::Id).eq(generation_id.to_string()))
                .to_owned(),
        )
        .await?;
    row.ok_or_else(|| Error::CorruptData("missing provider generation".into()))?
        .configuration
        .map(|value| {
            serde_json::from_str(&value)
                .map_err(|_invalid| Error::CorruptData("invalid provider configuration".into()))
        })
        .transpose()
}

pub(super) async fn generation_fingerprint(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<crate::ProviderGenerationFingerprint, Error> {
    let row = load_generation(transaction, generation_id).await?;
    let schema_graph_json = row.schema_graph_json.clone();
    let typescript_view = row.typescript_view.clone();
    let (stored_id, _input_digest, schema_graph) = row.decode()?;
    let operations = load_catalog_operations(transaction, &stored_id).await?;
    verify_operation_schema_references(&schema_graph, &operations)?;
    let configuration_json = load_configuration(transaction, generation_id)
        .await?
        .map(|configuration| serde_json::to_string(&configuration))
        .transpose()
        .map_err(|_invalid| Error::CorruptData("invalid provider configuration".into()))?;
    Ok(crate::ProviderGenerationFingerprint::from_material(
        &schema_graph_json,
        &typescript_view,
        &operations,
        configuration_json.as_deref(),
    ))
}

pub(super) async fn load_credential_contract(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<crate::ProviderCredentialContract, Error> {
    let row: Option<(String,)> = transaction
        .fetch_optional(
            Query::select()
                .column(ProviderGenerations::CredentialContract)
                .from(ProviderGenerations::Table)
                .and_where(Expr::col(ProviderGenerations::Id).eq(generation_id.to_string()))
                .to_owned(),
        )
        .await?;
    let (value,) = row.ok_or_else(|| Error::CorruptData("missing credential contract".into()))?;
    crate::ProviderCredentialContract::from_storage(&value)
}

pub(super) async fn load_schema_graph(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<SchemaGraph, Error> {
    let row: Option<(String,)> = transaction
        .fetch_optional(
            Query::select()
                .column(ProviderGenerations::SchemaGraphJson)
                .from(ProviderGenerations::Table)
                .and_where(Expr::col(ProviderGenerations::Id).eq(generation_id.to_string()))
                .to_owned(),
        )
        .await?;
    let (schema_graph_json,) = row.ok_or_else(|| {
        Error::CorruptData("Provider has no retained Generation schema graph".to_string())
    })?;
    SchemaGraph::from_storage_json(&schema_graph_json)
}

pub(super) async fn verify_generation(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<(), Error> {
    let row = load_generation(transaction, generation_id).await?;
    let (stored_id, _input_digest, schema_graph) = row.decode()?;
    let operations = load_catalog_operations(transaction, &stored_id).await?;
    verify_operation_schema_references(&schema_graph, &operations)?;
    if operations.is_empty() {
        return Err(Error::CorruptData(
            "Materialized Catalog contains no Operations".to_string(),
        ));
    }
    Ok(())
}

pub(super) async fn available_search_targets(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<Vec<crate::CatalogSearchTarget>, Error> {
    require_generation(transaction, generation_id).await?;
    let rows: Vec<SearchTargetRow> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    MaterializedOperations::OperationKey,
                    MaterializedOperations::RelativePath,
                    MaterializedOperations::Method,
                    MaterializedOperations::ReadOnlyHint,
                    MaterializedOperations::DestructiveHint,
                    MaterializedOperations::IdempotentHint,
                    MaterializedOperations::OpenWorldHint,
                ])
                .from(MaterializedOperations::Table)
                .and_where(
                    Expr::col(MaterializedOperations::ProviderGenerationId)
                        .eq(generation_id.to_string()),
                )
                .and_where(
                    Expr::col(MaterializedOperations::Availability)
                        .eq(OperationAvailability::Available.as_storage_str()),
                )
                .order_by(MaterializedOperations::OperationKey, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(SearchTargetRow::decode).collect()
}

pub(super) async fn load_operation_summaries(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
    after: Option<&OperationKey>,
    page_size: u32,
) -> Result<Vec<crate::OperationSummary>, Error> {
    require_generation(transaction, generation_id).await?;
    // Match OperationKey's ordinal ordering regardless of the Postgres locale.
    // SQLite's TEXT columns use BINARY by default. This also keeps the exclusive
    // cursor predicate consistent with the transport's continuation ordering.
    let key = match transaction.system() {
        coral_db::DatabaseSystem::Sqlite => Expr::col(MaterializedOperations::OperationKey),
        coral_db::DatabaseSystem::Postgres => Expr::cust("\"operation_key\" COLLATE \"C\""),
    };
    let mut query = Query::select();
    query
        .columns([
            MaterializedOperations::OperationKey,
            MaterializedOperations::RelativePath,
            MaterializedOperations::Summary,
            MaterializedOperations::Description,
            MaterializedOperations::TagsJson,
            MaterializedOperations::ReadOnlyHint,
            MaterializedOperations::DestructiveHint,
            MaterializedOperations::IdempotentHint,
            MaterializedOperations::OpenWorldHint,
            MaterializedOperations::Availability,
            MaterializedOperations::DiagnosticsJson,
        ])
        .from(MaterializedOperations::Table)
        .and_where(
            Expr::col(MaterializedOperations::ProviderGenerationId).eq(generation_id.to_string()),
        )
        .order_by_expr(key.clone(), Order::Asc)
        .limit(u64::from(page_size) + 1);
    if let Some(after) = after {
        query.and_where(key.gt(after.as_str()));
    }
    let rows: Vec<OperationSummaryRow> = transaction.fetch_all(query).await?;
    rows.into_iter().map(OperationSummaryRow::decode).collect()
}

pub(super) async fn load_catalog_operations(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<Vec<MaterializedOperation>, Error> {
    require_generation(transaction, generation_id).await?;
    let rows: Vec<OperationRow> = transaction
        .fetch_all(
            Query::select()
                .columns(operation_columns())
                .from(MaterializedOperations::Table)
                .and_where(
                    Expr::col(MaterializedOperations::ProviderGenerationId)
                        .eq(generation_id.to_string()),
                )
                .order_by(MaterializedOperations::OperationKey, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(OperationRow::decode).collect()
}

pub(super) async fn operation_exists(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
    operation_key: &OperationKey,
) -> Result<bool, Error> {
    let row: Option<(i32,)> = transaction
        .fetch_optional(
            Query::select()
                .expr(Expr::val(1_i32))
                .from(MaterializedOperations::Table)
                .and_where(
                    Expr::col(MaterializedOperations::ProviderGenerationId)
                        .eq(generation_id.to_string()),
                )
                .and_where(
                    Expr::col(MaterializedOperations::OperationKey).eq(operation_key.as_str()),
                )
                .to_owned(),
        )
        .await?;
    Ok(row.is_some())
}

pub(super) async fn load_operation(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
    operation_key: &OperationKey,
) -> Result<Option<MaterializedOperation>, Error> {
    load_one_operation(
        transaction,
        generation_id,
        Expr::col(MaterializedOperations::OperationKey).eq(operation_key.as_str()),
    )
    .await
}

pub(super) async fn load_operation_hints_and_methods(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
    keys: &[OperationKey],
) -> Result<
    std::collections::BTreeMap<OperationKey, (crate::OperationHints, Option<HttpMethod>)>,
    Error,
> {
    #[derive(FromRow)]
    struct Row {
        operation_key: String,
        method: Option<String>,
        #[sqlx(flatten)]
        hints: HintColumns,
    }
    require_generation(transaction, generation_id).await?;
    let rows: Vec<Row> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    MaterializedOperations::OperationKey,
                    MaterializedOperations::Method,
                    MaterializedOperations::ReadOnlyHint,
                    MaterializedOperations::DestructiveHint,
                    MaterializedOperations::IdempotentHint,
                    MaterializedOperations::OpenWorldHint,
                ])
                .from(MaterializedOperations::Table)
                .and_where(
                    Expr::col(MaterializedOperations::ProviderGenerationId)
                        .eq(generation_id.to_string()),
                )
                .and_where(
                    Expr::col(MaterializedOperations::OperationKey)
                        .is_in(keys.iter().map(OperationKey::as_str)),
                )
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|row| {
            let key = OperationKey::parse(&row.operation_key).map_err(|_error| {
                Error::CorruptData("Operation hints have an invalid key".into())
            })?;
            let method = decode_http_method(row.method.as_deref())?;
            Ok((key, (row.hints.decode(), method)))
        })
        .collect()
}

pub(super) async fn load_operation_by_relative_path(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
    relative_path: &str,
) -> Result<Option<MaterializedOperation>, Error> {
    load_one_operation(
        transaction,
        generation_id,
        Expr::col(MaterializedOperations::RelativePath).eq(relative_path),
    )
    .await
}

async fn require_generation(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<(), Error> {
    let generation: Option<(String,)> = transaction
        .fetch_optional(
            Query::select()
                .column(ProviderGenerations::Id)
                .from(ProviderGenerations::Table)
                .and_where(Expr::col(ProviderGenerations::Id).eq(generation_id.to_string()))
                .to_owned(),
        )
        .await?;
    generation
        .map(|_| ())
        .ok_or_else(|| Error::CorruptData("Provider has no retained Generation".to_string()))
}

async fn load_one_operation(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
    predicate: sea_query::SimpleExpr,
) -> Result<Option<MaterializedOperation>, Error> {
    require_generation(transaction, generation_id).await?;
    let row: Option<OperationRow> = transaction
        .fetch_optional(
            Query::select()
                .columns(operation_columns())
                .from(MaterializedOperations::Table)
                .and_where(
                    Expr::col(MaterializedOperations::ProviderGenerationId)
                        .eq(generation_id.to_string()),
                )
                .and_where(predicate)
                .to_owned(),
        )
        .await?;
    row.map(OperationRow::decode).transpose()
}

fn verify_operation_schema_references(
    graph: &SchemaGraph,
    operations: &[MaterializedOperation],
) -> Result<(), Error> {
    for operation in operations {
        if !graph.contains(operation.input_schema())
            || operation
                .schema_ids()
                .any(|schema_id| !graph.contains(schema_id))
        {
            return Err(Error::CorruptData(
                "Materialized Operation references a missing authoritative schema".to_string(),
            ));
        }
    }
    Ok(())
}

fn operation_columns() -> [MaterializedOperations; 26] {
    [
        MaterializedOperations::OperationKey,
        MaterializedOperations::RelativePath,
        MaterializedOperations::Protocol,
        MaterializedOperations::Method,
        MaterializedOperations::Path,
        MaterializedOperations::ServerUrl,
        MaterializedOperations::Summary,
        MaterializedOperations::Title,
        MaterializedOperations::Description,
        MaterializedOperations::Deprecated,
        MaterializedOperations::TagsJson,
        MaterializedOperations::Guidance,
        MaterializedOperations::ReadOnlyHint,
        MaterializedOperations::DestructiveHint,
        MaterializedOperations::IdempotentHint,
        MaterializedOperations::OpenWorldHint,
        MaterializedOperations::DefinitionJson,
        MaterializedOperations::InputSchemaJson,
        MaterializedOperations::OutputSchemaJson,
        MaterializedOperations::TypescriptDeclaration,
        MaterializedOperations::Availability,
        MaterializedOperations::DiagnosticCode,
        MaterializedOperations::DiagnosticMessage,
        MaterializedOperations::InputSchemaId,
        MaterializedOperations::DiagnosticsJson,
        MaterializedOperations::TypeReferencesJson,
    ]
}
