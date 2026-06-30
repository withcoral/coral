use sea_query::Iden;

#[derive(Iden)]
pub(in crate::state::db) enum Workspaces {
    Table,
    Id,
    CreatedAtUnixNanos,
}

#[expect(
    dead_code,
    reason = "source catalog schema lands before the source repository in the stacked PR sequence"
)]
#[derive(Iden)]
pub(in crate::state::db) enum Sources {
    Table,
    WorkspaceId,
    Name,
    Version,
    OriginKind,
    CredentialStorage,
    CreatedAtUnixNanos,
    UpdatedAtUnixNanos,
}

#[expect(
    dead_code,
    reason = "source catalog schema lands before the source repository in the stacked PR sequence"
)]
#[derive(Iden)]
pub(in crate::state::db) enum SourceVariables {
    Table,
    WorkspaceId,
    SourceName,
    Key,
    Value,
}

#[expect(
    dead_code,
    reason = "source catalog schema lands before the source repository in the stacked PR sequence"
)]
#[derive(Iden)]
pub(in crate::state::db) enum SourceSecretKeys {
    Table,
    WorkspaceId,
    SourceName,
    Position,
    Key,
}
