use sea_query::Iden;

#[derive(Iden)]
pub(in crate::state::db) enum Workspaces {
    Table,
    Id,
    CreatedAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum AppStateMigrations {
    Table,
    Id,
    CompletedAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum Tasks {
    Table,
    Id,
    WorkspaceId,
    CreatedByPrincipalId,
    Intent,
    Outcome,
    CreatedAtUnixNanos,
    CompletedAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum TaskQueries {
    Table,
    Id,
    TaskId,
    Intent,
    Sql,
    Status,
    StartedAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum TaskQueryRelations {
    Table,
    QueryId,
    RelationKind,
    CatalogName,
    SchemaName,
    RelationName,
}

#[derive(Iden)]
pub(in crate::state::db) enum IdentitySpecs {
    Table,
    Id,
    WorkspaceId,
    Name,
    Version,
    Description,
    Issuer,
    IdentityType,
    ManifestYaml,
    CreatedAtUnixNanos,
    UpdatedAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum IdentitySpecDocuments {
    Table,
    IdentitySpecId,
    DocumentVersion,
    Ciphertext,
    Nonce,
    WrappedDek,
    WrappedDekNonce,
    KeyId,
    Algorithm,
    BindingVersion,
    CreatedAtUnixNanos,
    UpdatedAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum GuiOnboardingCompletions {
    Table,
    PrincipalId,
}
