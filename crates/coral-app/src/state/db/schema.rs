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
pub(in crate::state::db) enum Users {
    Table,
    UserId,
    Issuer,
    Subject,
    DisplayName,
    CreatedAtUnixNanos,
    LastLoginAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum WorkspaceMembers {
    Table,
    WorkspaceId,
    UserId,
    Role,
    CreatedAtUnixNanos,
}

#[derive(Iden)]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "next stack layer wires capture")
)]
pub(in crate::state::db) enum TraceSearchResponses {
    Table,
    WorkspaceId,
    TraceId,
    SearchSpanId,
    RecordedAtUnixNanos,
    ResponseProto,
    OversizedBytes,
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
    CompletedAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum Sources {
    Table,
    WorkspaceId,
    Name,
    Version,
    OriginKind,
    CredentialStorage,
    CredentialRevision,
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
    Key,
}

#[derive(Iden)]
pub(in crate::state::db) enum SourceTombstones {
    Table,
    WorkspaceId,
    SourceName,
    DeletedAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum SourceManifests {
    Table,
    WorkspaceId,
    SourceName,
    ManifestYaml,
    ManifestHash,
    CreatedAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum Materializations {
    Table,
    WorkspaceId,
    SourceName,
    MaterializationVersion,
    FingerprintYaml,
    ProjectionsYaml,
    DiagnosticsYaml,
    SourceDocumentRaw,
    SourceDocumentYaml,
    SemanticIrYaml,
    OperationMetadataYaml,
    CreatedAtUnixNanos,
}
