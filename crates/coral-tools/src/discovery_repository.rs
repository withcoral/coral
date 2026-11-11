//! The reads Universal Search makes of Authored Tools. One statement shape
//! serves both kinds: a Private Tool's row is its current definition, and a
//! Shared Tool's is its live record joined to its current revision. Each
//! read names the columns the two share by one name, so one row type decodes
//! either kind.

use coral_catalog::AuthoredToolKind;
use coral_db::Transaction;
use coral_workspace::{Principal, PrincipalId, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, Order, Query, SelectStatement, SimpleExpr};
use sqlx::FromRow;

use crate::Error;
use crate::discovery::{
    AuthoredToolAuthoringMaterial, AuthoredToolEntry, AuthoredToolIndexRevision,
    AuthoredToolSearchMaterial, ExactAuthoredToolAuthoringMaterial,
    ExactAuthoredToolDescriptionMaterial,
};
use crate::repository::{
    PrivateToolRecords, access_state_from_storage, decode_diagnostics, principal_kind_from_storage,
};
use crate::shared_tool_repository::{SharedToolRecords, SharedToolRevisions, is_current_revision};
use crate::types::{
    AuthoredToolDeclaration, AuthoredToolDeclarationField, AuthoredToolDeclarationFieldProjection,
    AuthoredToolDescription, AuthoredToolId, AuthoredToolPath, AuthoredToolSource,
};

/// What every read names alike, whichever kind's tables hold it. A read
/// selects each as a column of this name.
#[derive(Iden, Clone, Copy)]
enum Definition {
    Id,
    WorkspaceId,
    /// The owning Principal of a Private Tool; `NULL` for a Shared Tool.
    PrincipalId,
    PrincipalKind,
    Path,
    Description,
    Declaration,
    Source,
    DeclarationFieldsFormat,
    DeclarationFieldsStatus,
    DeclarationFieldsJson,
    DeclarationFieldsFailure,
    DiagnosticsJson,
    DiagnosticsTruncated,
    /// A Private Tool's definition revision, or a Shared Tool's current
    /// revision.
    Revision,
    DefaultAccessState,
}

const DECLARATION_FIELDS: [Definition; 4] = [
    Definition::DeclarationFieldsFormat,
    Definition::DeclarationFieldsStatus,
    Definition::DeclarationFieldsJson,
    Definition::DeclarationFieldsFailure,
];

/// Where a kind's tables hold `column`.
fn stored(kind: AuthoredToolKind, column: Definition) -> SimpleExpr {
    match kind {
        AuthoredToolKind::Private => {
            let stored = match column {
                Definition::Id => PrivateToolRecords::Id,
                Definition::WorkspaceId => PrivateToolRecords::WorkspaceId,
                Definition::PrincipalId => PrivateToolRecords::PrincipalId,
                Definition::PrincipalKind => PrivateToolRecords::PrincipalKind,
                Definition::Path => PrivateToolRecords::Path,
                Definition::Description => PrivateToolRecords::Description,
                Definition::Declaration => PrivateToolRecords::Declaration,
                Definition::Source => PrivateToolRecords::Source,
                Definition::DeclarationFieldsFormat => PrivateToolRecords::DeclarationFieldsFormat,
                Definition::DeclarationFieldsStatus => PrivateToolRecords::DeclarationFieldsStatus,
                Definition::DeclarationFieldsJson => PrivateToolRecords::DeclarationFieldsJson,
                Definition::DeclarationFieldsFailure => {
                    PrivateToolRecords::DeclarationFieldsFailure
                }
                Definition::DiagnosticsJson => PrivateToolRecords::DiagnosticsJson,
                Definition::DiagnosticsTruncated => PrivateToolRecords::DiagnosticsTruncated,
                Definition::Revision => PrivateToolRecords::DefinitionRevision,
                Definition::DefaultAccessState => PrivateToolRecords::DefaultAccessState,
            };
            Expr::col((PrivateToolRecords::Table, stored))
        }
        AuthoredToolKind::Shared => {
            let record = |stored| Expr::col((SharedToolRecords::Table, stored));
            let revision = |stored| Expr::col((SharedToolRevisions::Table, stored));
            match column {
                Definition::Id => record(SharedToolRecords::Id),
                Definition::WorkspaceId => record(SharedToolRecords::WorkspaceId),
                // The Workspace owns a Shared Tool: its author is
                // attribution only, and no read here selects it.
                Definition::PrincipalId | Definition::PrincipalKind => {
                    Expr::cust("CAST(NULL AS TEXT)")
                }
                Definition::Path => record(SharedToolRecords::Path),
                Definition::Revision => record(SharedToolRecords::CurrentRevision),
                Definition::Description => revision(SharedToolRevisions::Description),
                Definition::Declaration => revision(SharedToolRevisions::Declaration),
                Definition::Source => revision(SharedToolRevisions::Source),
                Definition::DeclarationFieldsFormat => {
                    revision(SharedToolRevisions::DeclarationFieldsFormat)
                }
                Definition::DeclarationFieldsStatus => {
                    revision(SharedToolRevisions::DeclarationFieldsStatus)
                }
                Definition::DeclarationFieldsJson => {
                    revision(SharedToolRevisions::DeclarationFieldsJson)
                }
                Definition::DeclarationFieldsFailure => {
                    revision(SharedToolRevisions::DeclarationFieldsFailure)
                }
                Definition::DiagnosticsJson => revision(SharedToolRevisions::DiagnosticsJson),
                Definition::DiagnosticsTruncated => {
                    revision(SharedToolRevisions::DiagnosticsTruncated)
                }
                Definition::DefaultAccessState => revision(SharedToolRevisions::DefaultAccessState),
            }
        }
    }
}

/// Selects `columns` of every current Authored Tool of `kind`: every
/// Private Tool, or every live Shared Tool at its current revision.
fn current(kind: AuthoredToolKind, columns: &[Definition]) -> SelectStatement {
    let mut statement = Query::select();
    for column in columns {
        statement.expr_as(stored(kind, *column), *column);
    }
    match kind {
        AuthoredToolKind::Private => {
            statement.from(PrivateToolRecords::Table);
        }
        AuthoredToolKind::Shared => {
            statement
                .from(SharedToolRecords::Table)
                .inner_join(SharedToolRevisions::Table, is_current_revision())
                .and_where(
                    Expr::col((
                        SharedToolRecords::Table,
                        SharedToolRecords::RemovedAtUnixNanos,
                    ))
                    .is_null(),
                );
        }
    }
    statement
}

/// Selects `columns` of the Authored Tools of `kind` that `principal` can
/// discover in the Workspace: its own Private Tools, or every live Shared
/// Tool.
fn discovered(
    kind: AuthoredToolKind,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    columns: &[Definition],
) -> SelectStatement {
    let mut statement = current(kind, columns);
    statement.and_where(stored(kind, Definition::WorkspaceId).eq(workspace_id.to_string()));
    if kind == AuthoredToolKind::Private {
        statement
            .and_where(stored(kind, Definition::PrincipalId).eq(principal.id().as_str()))
            .and_where(
                stored(kind, Definition::PrincipalKind).eq(principal.kind().as_storage_str()),
            );
    }
    statement
}

fn corrupt(kind: AuthoredToolKind, what: &str) -> Error {
    Error::CorruptData(format!("{} {what}", kind.name()))
}

fn decode_id(kind: AuthoredToolKind, value: &str) -> Result<AuthoredToolId, Error> {
    AuthoredToolId::parse(kind, value).map_err(|_error| corrupt(kind, "has an invalid identity"))
}

fn decode_workspace(kind: AuthoredToolKind, value: &str) -> Result<WorkspaceId, Error> {
    WorkspaceId::parse(value).map_err(|_error| corrupt(kind, "has an invalid Workspace identity"))
}

fn decode_path(kind: AuthoredToolKind, value: &str) -> Result<AuthoredToolPath, Error> {
    AuthoredToolPath::parse(kind, value).map_err(|_error| corrupt(kind, "has an invalid path"))
}

fn decode_description(
    kind: AuthoredToolKind,
    value: &str,
) -> Result<AuthoredToolDescription, Error> {
    AuthoredToolDescription::parse(value)
        .map_err(|_error| corrupt(kind, "has an invalid description"))
}

fn decode_revision(kind: AuthoredToolKind, stored: i64) -> Result<u64, Error> {
    u64::try_from(stored)
        .ok()
        .filter(|revision| *revision > 0)
        .ok_or_else(|| corrupt(kind, "has an invalid revision"))
}

/// The stored declaration fields. An unreadable projection stays isolated
/// to its one Tool, which remains searchable by path and description.
#[derive(Debug, FromRow)]
#[expect(
    clippy::struct_field_names,
    reason = "each field is named after the column it reads"
)]
struct DeclarationFieldsRow {
    declaration_fields_format: String,
    declaration_fields_status: String,
    declaration_fields_json: Option<String>,
    declaration_fields_failure: Option<String>,
}

impl DeclarationFieldsRow {
    fn decode(self) -> Option<Vec<AuthoredToolDeclarationField>> {
        AuthoredToolDeclarationFieldProjection::from_storage(
            &self.declaration_fields_format,
            &self.declaration_fields_status,
            self.declaration_fields_json.as_deref(),
            self.declaration_fields_failure.as_deref(),
        )
        .ok()
        .and_then(|projection| projection.fields().map(<[_]>::to_vec))
    }
}

#[derive(Debug, FromRow)]
struct SearchRow {
    id: String,
    path: String,
    description: String,
    default_access_state: String,
}

pub(crate) async fn list_search_material(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    kind: AuthoredToolKind,
) -> Result<Vec<AuthoredToolSearchMaterial>, Error> {
    let rows: Vec<SearchRow> = transaction
        .fetch_all(
            discovered(
                kind,
                workspace_id,
                principal,
                &[
                    Definition::Id,
                    Definition::Path,
                    Definition::Description,
                    Definition::DefaultAccessState,
                ],
            )
            .order_by_expr(stored(kind, Definition::Path), Order::Asc)
            .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok(AuthoredToolSearchMaterial {
                id: decode_id(kind, &row.id)?,
                path: decode_path(kind, &row.path)?,
                description: decode_description(kind, &row.description)?,
                default_access_state: access_state_from_storage(&row.default_access_state)?,
            })
        })
        .collect()
}

pub(crate) async fn declaration_fields(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &AuthoredToolPath,
) -> Result<Vec<AuthoredToolDeclarationField>, Error> {
    let kind = path.kind();
    let row: Option<DeclarationFieldsRow> = transaction
        .fetch_optional(
            discovered(kind, workspace_id, principal, &DECLARATION_FIELDS)
                .and_where(stored(kind, Definition::Path).eq(path.relative_path()))
                .to_owned(),
        )
        .await?;
    Ok(row
        .and_then(DeclarationFieldsRow::decode)
        .unwrap_or_default())
}

#[derive(Debug, FromRow)]
struct AuthoringRow {
    path: String,
    description: String,
    declaration: String,
}

impl AuthoringRow {
    fn decode(self, kind: AuthoredToolKind) -> Result<AuthoredToolAuthoringMaterial, Error> {
        Ok(AuthoredToolAuthoringMaterial {
            path: decode_path(kind, &self.path)?,
            description: decode_description(kind, &self.description)?,
            declaration: AuthoredToolDeclaration::from_persisted(&self.declaration)?,
        })
    }
}

pub(crate) async fn hydrate_search_material(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    target: &AuthoredToolSearchMaterial,
) -> Result<Option<AuthoredToolAuthoringMaterial>, Error> {
    let kind = target.id().kind();
    let row: Option<AuthoringRow> = transaction
        .fetch_optional(
            discovered(
                kind,
                workspace_id,
                principal,
                &[
                    Definition::Path,
                    Definition::Description,
                    Definition::Declaration,
                ],
            )
            .and_where(stored(kind, Definition::Id).eq(target.id().to_string()))
            .and_where(stored(kind, Definition::Path).eq(target.path().relative_path()))
            .to_owned(),
        )
        .await?;
    let material = row.map(|row| row.decode(kind)).transpose()?;
    if material
        .as_ref()
        .is_some_and(|material| material.description() != target.description())
    {
        return Err(corrupt(kind, "changed inside one search snapshot"));
    }
    Ok(material)
}

#[derive(Debug, FromRow)]
struct ExactDescriptionRow {
    id: String,
    #[sqlx(flatten)]
    authoring: AuthoringRow,
    source: String,
    diagnostics_json: String,
    diagnostics_truncated: bool,
    revision: i64,
    default_access_state: String,
}

pub(crate) async fn exact_description(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &AuthoredToolPath,
) -> Result<Option<ExactAuthoredToolDescriptionMaterial>, Error> {
    let kind = path.kind();
    let row: Option<ExactDescriptionRow> = transaction
        .fetch_optional(
            discovered(
                kind,
                workspace_id,
                principal,
                &[
                    Definition::Id,
                    Definition::Path,
                    Definition::Description,
                    Definition::Declaration,
                    Definition::Source,
                    Definition::DiagnosticsJson,
                    Definition::DiagnosticsTruncated,
                    Definition::Revision,
                    Definition::DefaultAccessState,
                ],
            )
            .and_where(stored(kind, Definition::Path).eq(path.relative_path()))
            .to_owned(),
        )
        .await?;
    row.map(|row| {
        Ok(ExactAuthoredToolDescriptionMaterial {
            id: decode_id(kind, &row.id)?,
            authoring: row.authoring.decode(kind)?,
            detail: ExactAuthoredToolAuthoringMaterial {
                source: AuthoredToolSource::parse(&row.source)
                    .map_err(|_error| corrupt(kind, "has an invalid source"))?,
                diagnostics: decode_diagnostics(&row.diagnostics_json, row.diagnostics_truncated)?,
                diagnostics_truncated: row.diagnostics_truncated,
            },
            revision: decode_revision(kind, row.revision)?,
            default_access_state: access_state_from_storage(&row.default_access_state)?,
        })
    })
    .transpose()
}

#[derive(Debug, FromRow)]
struct EntryRow {
    id: String,
    workspace_id: String,
    principal_id: Option<String>,
    principal_kind: Option<String>,
    path: String,
    description: String,
    #[sqlx(flatten)]
    declaration_fields: DeclarationFieldsRow,
    revision: i64,
}

impl EntryRow {
    fn decode(self, kind: AuthoredToolKind) -> Result<AuthoredToolEntry, Error> {
        let principal = match (kind, self.principal_id, self.principal_kind) {
            (AuthoredToolKind::Private, Some(id), Some(principal_kind)) => {
                Some(Principal::from_parts(
                    PrincipalId::parse(&id)
                        .map_err(|_error| corrupt(kind, "has an invalid Principal identity"))?,
                    principal_kind_from_storage(&principal_kind)?,
                ))
            }
            (AuthoredToolKind::Shared, None, None) => None,
            _ => return Err(corrupt(kind, "has the owner of another kind")),
        };
        Ok(AuthoredToolEntry {
            id: decode_id(kind, &self.id)?,
            workspace_id: decode_workspace(kind, &self.workspace_id)?,
            principal,
            path: decode_path(kind, &self.path)?,
            description: decode_description(kind, &self.description)?,
            declaration_fields: self.declaration_fields.decode(),
            revision: decode_revision(kind, self.revision)?,
        })
    }
}

/// Selects the columns of an [`EntryRow`] for every current Authored Tool
/// of `kind`.
fn entries_statement(kind: AuthoredToolKind) -> SelectStatement {
    let mut columns = vec![
        Definition::Id,
        Definition::WorkspaceId,
        Definition::PrincipalId,
        Definition::PrincipalKind,
        Definition::Path,
        Definition::Description,
        Definition::Revision,
    ];
    columns.extend(DECLARATION_FIELDS);
    current(kind, &columns)
}

pub(crate) async fn entries(
    transaction: &mut Transaction<'_>,
    kind: AuthoredToolKind,
) -> Result<Vec<AuthoredToolEntry>, Error> {
    let rows: Vec<EntryRow> = transaction
        .fetch_all(
            entries_statement(kind)
                .order_by_expr(stored(kind, Definition::WorkspaceId), Order::Asc)
                .order_by_expr(stored(kind, Definition::Path), Order::Asc)
                .order_by_expr(stored(kind, Definition::Id), Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(|row| row.decode(kind)).collect()
}

pub(crate) async fn entry(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &AuthoredToolId,
) -> Result<Option<AuthoredToolEntry>, Error> {
    let kind = id.kind();
    let row: Option<EntryRow> = transaction
        .fetch_optional(
            entries_statement(kind)
                .and_where(stored(kind, Definition::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(stored(kind, Definition::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(|row| row.decode(kind)).transpose()
}

pub(crate) async fn index_revisions(
    transaction: &mut Transaction<'_>,
    kind: AuthoredToolKind,
) -> Result<Vec<AuthoredToolIndexRevision>, Error> {
    let rows: Vec<(String, String, i64)> = transaction
        .fetch_all(current(
            kind,
            &[
                Definition::Id,
                Definition::WorkspaceId,
                Definition::Revision,
            ],
        ))
        .await?;
    rows.into_iter()
        .map(|(id, workspace, revision)| {
            Ok(AuthoredToolIndexRevision {
                id: decode_id(kind, &id)?,
                workspace_id: decode_workspace(kind, &workspace)?,
                revision: decode_revision(kind, revision)?,
            })
        })
        .collect()
}
