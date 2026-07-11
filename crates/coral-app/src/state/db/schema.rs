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
pub(in crate::state::db) enum IdentitySpecs {
    Table,
    ScopeKind,
    ScopeId,
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

#[expect(
    dead_code,
    reason = "identity schema lands before repository behavior in the B1 stack"
)]
#[derive(Iden)]
pub(in crate::state::db) enum IdentitySpecDocuments {
    Table,
    ScopeKind,
    ScopeId,
    Name,
    DocumentVersion,
    Ciphertext,
    Nonce,
    WrappedDek,
    WrappedDekNonce,
    KeyId,
    Algorithm,
    AadVersion,
    CreatedAtUnixNanos,
    UpdatedAtUnixNanos,
}
