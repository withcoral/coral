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
    WorkspaceId,
    Id,
    Intent,
    Status,
    StartedAtUnixNanos,
    EndedAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum TrajectoryRawSteps {
    Table,
    WorkspaceId,
    Id,
    TaskId,
    StartedAtUnixNanos,
    CompletedAtUnixNanos,
    Operation,
    Input,
    Status,
    RowCount,
    OutputSummaryJson,
    ErrorKind,
    ErrorType,
    ErrorMessage,
}

#[derive(Iden)]
pub(in crate::state::db) enum TrajectoryDistillations {
    Table,
    WorkspaceId,
    Id,
    TaskId,
    Strategy,
    NormalizedIntent,
    PathKey,
    InputStepCount,
    OutputStepCount,
    CreatedAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum TrajectoryDistilledSteps {
    Table,
    WorkspaceId,
    Id,
    DistillationId,
    SourceRawStepId,
    Ordinal,
    SqlTemplate,
    RelationsJson,
    ResultRowCount,
    ResultColumnCount,
    ExactKey,
    CreatedAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum TrajectoryConsolidatedPaths {
    Table,
    WorkspaceId,
    NormalizedIntent,
    PathKey,
    RepresentativeDistillationId,
    SupportCount,
    StepCount,
    UpdatedAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum TrajectoryExactIndex {
    Table,
    WorkspaceId,
    NormalizedIntent,
    PathKey,
    SupportCount,
    UpdatedAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum TrajectoryIndexBuilds {
    Table,
    WorkspaceId,
    Id,
    NormalizedIntent,
    CandidatePathCount,
    SelectedDistillationId,
    SelectedPathKey,
    SelectedSupportCount,
    CreatedAtUnixNanos,
}
