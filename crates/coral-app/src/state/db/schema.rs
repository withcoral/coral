use sea_query::Iden;

#[derive(Iden)]
pub(in crate::state::db) enum Workspaces {
    Table,
    Id,
    CreatedAtUnixNanos,
}

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

#[derive(Iden)]
pub(in crate::state::db) enum SourceVariables {
    Table,
    WorkspaceId,
    SourceName,
    Key,
    Value,
}

#[derive(Iden)]
pub(in crate::state::db) enum SourceSecretKeys {
    Table,
    WorkspaceId,
    SourceName,
    Position,
    Key,
}
