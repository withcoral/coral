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
