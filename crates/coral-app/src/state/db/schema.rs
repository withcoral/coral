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

#[derive(Iden)]
pub(in crate::state::db) enum Identities {
    Table,
    OwnerKind,
    OwnerKey,
    WorkspaceId,
    Name,
    IdentitySpecScopeKind,
    IdentitySpecScopeId,
    IdentitySpecName,
    IdentitySpecFingerprint,
    Issuer,
    IdentityType,
    SafeMetadataJson,
    OauthRefreshClaimId,
    OauthRefreshClaimDeadlineUnixNanos,
    CreatedAtUnixNanos,
    UpdatedAtUnixNanos,
}

#[derive(Iden)]
pub(in crate::state::db) enum IdentityDocuments {
    Table,
    OwnerKind,
    OwnerKey,
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
