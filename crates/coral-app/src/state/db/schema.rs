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
    CreatedAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum MaterializationSurfaces {
    Table,
    WorkspaceId,
    SourceName,
    SurfaceId,
    SourceDocumentRaw,
    SourceDocumentYaml,
    SemanticIrYaml,
}

#[derive(Iden)]
pub(in crate::state::db) enum FeedbackReports {
    Table,
    Id,
    WorkspaceId,
    CreatedAtUnixNanos,
    TryingToDo,
    Tried,
    Stuck,
    PublishStatus,
    PublishError,
    PublishedAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum AppStateMarkers {
    Table,
    Key,
    CreatedAtUnixNanos,
}
