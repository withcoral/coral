use sea_query::{Expr, ExprTrait, OnConflict, Order, Query};

use crate::identity::parse_path_segment;
use crate::state::db::schema::{IdentitySpecDocuments, IdentitySpecs};
use crate::state::db::{DbError, DbSession, DbWriteSession};
use crate::workspaces::WorkspaceName;

const GLOBAL_SCOPE_KIND: &str = "global";
const GLOBAL_SCOPE_ID: &str = "__global__";
const WORKSPACE_SCOPE_KIND: &str = "workspace";
const SUPPORTED_AAD_VERSION: i64 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IdentitySpecScope {
    /// A globally installed identity spec definition.
    Global,
    /// An identity spec definition scoped to one workspace.
    Workspace(WorkspaceName),
}

impl IdentitySpecScope {
    /// Build the global identity-spec scope.
    pub(crate) fn global() -> Self {
        Self::Global
    }

    /// Build a workspace identity-spec scope.
    pub(crate) fn workspace(workspace_name: WorkspaceName) -> Self {
        Self::Workspace(workspace_name)
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Global => GLOBAL_SCOPE_KIND,
            Self::Workspace(_workspace_name) => WORKSPACE_SCOPE_KIND,
        }
    }

    fn scope_id(&self) -> &str {
        match self {
            Self::Global => GLOBAL_SCOPE_ID,
            Self::Workspace(workspace_name) => workspace_name.as_str(),
        }
    }

    fn workspace_id(&self) -> Option<&str> {
        match self {
            Self::Global => None,
            Self::Workspace(workspace_name) => Some(workspace_name.as_str()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentitySpecKey {
    /// Scope that owns this identity spec definition.
    pub(crate) scope: IdentitySpecScope,
    /// Identity spec name unique within the scope.
    pub(crate) name: String,
}

impl IdentitySpecKey {
    /// Build an identity-spec key from a scope and validated name.
    pub(crate) fn new(scope: IdentitySpecScope, name: &str) -> Result<Self, DbError> {
        Ok(Self {
            scope,
            name: parse_identity_spec_name(name)?,
        })
    }

    /// Build a global identity-spec key.
    pub(crate) fn global(name: &str) -> Result<Self, DbError> {
        Self::new(IdentitySpecScope::global(), name)
    }

    /// Build a workspace-scoped identity-spec key.
    pub(crate) fn workspace(workspace_name: WorkspaceName, name: &str) -> Result<Self, DbError> {
        Self::new(IdentitySpecScope::workspace(workspace_name), name)
    }

    fn from_spec_storage_parts(
        scope_kind: &str,
        scope_id: &str,
        workspace_id: Option<&str>,
        name: &str,
    ) -> Result<Self, DbError> {
        let scope = match (scope_kind, workspace_id) {
            (GLOBAL_SCOPE_KIND, None) if scope_id == GLOBAL_SCOPE_ID => IdentitySpecScope::Global,
            (GLOBAL_SCOPE_KIND, _) => {
                return Err(DbError::InvalidData(
                    "global identity spec row has invalid scope columns".to_string(),
                ));
            }
            (WORKSPACE_SCOPE_KIND, Some(workspace_id)) if scope_id == workspace_id => {
                IdentitySpecScope::Workspace(parse_workspace_name(workspace_id)?)
            }
            (WORKSPACE_SCOPE_KIND, _) => {
                return Err(DbError::InvalidData(
                    "workspace identity spec row has invalid scope columns".to_string(),
                ));
            }
            (other, _) => {
                return Err(DbError::InvalidData(format!(
                    "identity spec row has invalid scope kind '{other}'"
                )));
            }
        };
        Self::new(scope, name)
    }

    fn from_document_storage_parts(
        scope_kind: &str,
        scope_id: &str,
        name: &str,
    ) -> Result<Self, DbError> {
        let scope = match scope_kind {
            GLOBAL_SCOPE_KIND if scope_id == GLOBAL_SCOPE_ID => IdentitySpecScope::Global,
            GLOBAL_SCOPE_KIND => {
                return Err(DbError::InvalidData(
                    "global identity spec document row has invalid scope columns".to_string(),
                ));
            }
            WORKSPACE_SCOPE_KIND => IdentitySpecScope::Workspace(parse_workspace_name(scope_id)?),
            other => {
                return Err(DbError::InvalidData(format!(
                    "identity spec document row has invalid scope kind '{other}'"
                )));
            }
        };
        Self::new(scope, name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentitySpecRecord {
    /// Scope and name that identify this identity spec.
    pub(crate) key: IdentitySpecKey,
    /// Authored identity spec version string.
    pub(crate) version: String,
    /// Human-readable identity spec description.
    pub(crate) description: String,
    /// Issuer identifier declared by the identity spec.
    pub(crate) issuer: String,
    /// Identity mechanism declared by the identity spec.
    pub(crate) identity_type: String,
    /// Authored identity spec manifest YAML.
    pub(crate) manifest_yaml: String,
    /// Creation timestamp in Unix nanoseconds.
    pub(crate) created_at_unix_nanos: i64,
    /// Last update timestamp in Unix nanoseconds.
    pub(crate) updated_at_unix_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentitySpecWrite {
    /// Authored identity spec version string.
    pub(crate) version: String,
    /// Human-readable identity spec description.
    pub(crate) description: String,
    /// Issuer identifier declared by the identity spec.
    pub(crate) issuer: String,
    /// Identity mechanism declared by the identity spec.
    pub(crate) identity_type: String,
    /// Authored identity spec manifest YAML.
    pub(crate) manifest_yaml: String,
}

impl IdentitySpecWrite {
    fn validate(&self) -> Result<(), DbError> {
        if self.version.trim().is_empty()
            || self.issuer.trim().is_empty()
            || self.identity_type.trim().is_empty()
            || self.manifest_yaml.trim().is_empty()
        {
            return Err(DbError::InvalidData(
                "identity spec write has an empty required field".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, sqlx::FromRow)]
struct IdentitySpecRow {
    scope_kind: String,
    scope_id: String,
    workspace_id: Option<String>,
    name: String,
    version: String,
    description: String,
    issuer: String,
    identity_type: String,
    manifest_yaml: String,
    created_at_unix_nanos: i64,
    updated_at_unix_nanos: i64,
}

impl IdentitySpecRow {
    fn validate(self) -> Result<IdentitySpecRecord, DbError> {
        if self.created_at_unix_nanos < 0 || self.updated_at_unix_nanos < 0 {
            return Err(DbError::InvalidData(
                "identity spec row has a negative timestamp".to_string(),
            ));
        }
        let write = IdentitySpecWrite {
            version: self.version,
            description: self.description,
            issuer: self.issuer,
            identity_type: self.identity_type,
            manifest_yaml: self.manifest_yaml,
        };
        write.validate()?;
        Ok(IdentitySpecRecord {
            key: IdentitySpecKey::from_spec_storage_parts(
                &self.scope_kind,
                &self.scope_id,
                self.workspace_id.as_deref(),
                &self.name,
            )?,
            version: write.version,
            description: write.description,
            issuer: write.issuer,
            identity_type: write.identity_type,
            manifest_yaml: write.manifest_yaml,
            created_at_unix_nanos: self.created_at_unix_nanos,
            updated_at_unix_nanos: self.updated_at_unix_nanos,
        })
    }
}

/// Repository for durable DSL v4 identity spec definitions.
pub(crate) struct IdentitySpecsRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> IdentitySpecsRepo<'a, S>
where
    S: DbSession,
{
    /// Create an identity-spec repository over an existing DB session.
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    /// Load one identity spec by exact scope and name.
    pub(crate) async fn load_optional(
        &mut self,
        key: &IdentitySpecKey,
    ) -> Result<Option<IdentitySpecRecord>, DbError> {
        let row: Option<IdentitySpecRow> = self
            .session
            .fetch_optional(
                identity_spec_select()
                    .and_where(spec_key_where(key))
                    .to_owned(),
            )
            .await?;
        row.map(IdentitySpecRow::validate).transpose()
    }

    /// List globally installed identity specs.
    pub(crate) async fn list_global(&mut self) -> Result<Vec<IdentitySpecRecord>, DbError> {
        self.list_scope(&IdentitySpecScope::Global).await
    }

    /// List identity specs scoped to one workspace.
    pub(crate) async fn list_workspace(
        &mut self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<IdentitySpecRecord>, DbError> {
        self.list_scope(&IdentitySpecScope::Workspace(workspace_name.clone()))
            .await
    }

    /// List global identity specs followed by one workspace's scoped specs.
    pub(crate) async fn list_global_and_workspace(
        &mut self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<IdentitySpecRecord>, DbError> {
        let mut records = self.list_global().await?;
        records.extend(self.list_workspace(workspace_name).await?);
        Ok(records)
    }

    async fn list_scope(
        &mut self,
        scope: &IdentitySpecScope,
    ) -> Result<Vec<IdentitySpecRecord>, DbError> {
        let rows: Vec<IdentitySpecRow> = self
            .session
            .fetch_all(
                identity_spec_select()
                    .and_where(spec_scope_where(scope))
                    .order_by(IdentitySpecs::Name, Order::Asc)
                    .to_owned(),
            )
            .await?;
        rows.into_iter().map(IdentitySpecRow::validate).collect()
    }
}

impl<S> IdentitySpecsRepo<'_, S>
where
    S: DbWriteSession,
{
    /// Insert or replace an identity spec while preserving creation time.
    pub(crate) async fn upsert(
        &mut self,
        key: &IdentitySpecKey,
        spec: &IdentitySpecWrite,
        now_unix_nanos: i64,
    ) -> Result<IdentitySpecRecord, DbError> {
        validate_timestamp(now_unix_nanos)?;
        spec.validate()?;
        let statement = Query::insert()
            .into_table(IdentitySpecs::Table)
            .columns([
                IdentitySpecs::ScopeKind,
                IdentitySpecs::ScopeId,
                IdentitySpecs::WorkspaceId,
                IdentitySpecs::Name,
                IdentitySpecs::Version,
                IdentitySpecs::Description,
                IdentitySpecs::Issuer,
                IdentitySpecs::IdentityType,
                IdentitySpecs::ManifestYaml,
                IdentitySpecs::CreatedAtUnixNanos,
                IdentitySpecs::UpdatedAtUnixNanos,
            ])
            .values_panic([
                Expr::val(key.scope.kind()),
                Expr::val(key.scope.scope_id().to_string()),
                Expr::val(key.scope.workspace_id().map(ToString::to_string)),
                Expr::val(key.name.clone()),
                Expr::val(spec.version.clone()),
                Expr::val(spec.description.clone()),
                Expr::val(spec.issuer.clone()),
                Expr::val(spec.identity_type.clone()),
                Expr::val(spec.manifest_yaml.clone()),
                Expr::val(now_unix_nanos),
                Expr::val(now_unix_nanos),
            ])
            .on_conflict(
                OnConflict::columns([
                    IdentitySpecs::ScopeKind,
                    IdentitySpecs::ScopeId,
                    IdentitySpecs::Name,
                ])
                .update_columns([
                    IdentitySpecs::Version,
                    IdentitySpecs::Description,
                    IdentitySpecs::Issuer,
                    IdentitySpecs::IdentityType,
                    IdentitySpecs::ManifestYaml,
                    IdentitySpecs::UpdatedAtUnixNanos,
                ])
                .to_owned(),
            )
            .to_owned();
        self.session.execute(statement).await?;
        self.load_optional(key)
            .await?
            .ok_or_else(|| DbError::InvalidData("identity spec upsert did not persist".to_string()))
    }

    /// Delete an identity spec and cascade any encrypted setup-input document.
    pub(crate) async fn delete(
        &mut self,
        key: &IdentitySpecKey,
    ) -> Result<Option<IdentitySpecRecord>, DbError> {
        let removed = self.load_optional(key).await?;
        let statement = Query::delete()
            .from_table(IdentitySpecs::Table)
            .and_where(spec_key_where(key))
            .to_owned();
        self.session.execute_delete(statement).await?;
        Ok(removed)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct IdentitySpecDocumentRecord {
    /// Identity spec that owns this encrypted setup-input document.
    pub(crate) key: IdentitySpecKey,
    /// Monotonic version of the stored encrypted document.
    pub(crate) document_version: i64,
    /// Encrypted opaque setup-input bytes.
    pub(crate) ciphertext: Vec<u8>,
    /// Nonce used by the envelope-encrypted document.
    pub(crate) nonce: Vec<u8>,
    /// Wrapped data-encryption key bytes.
    pub(crate) wrapped_dek: Vec<u8>,
    /// Nonce used for the wrapped data-encryption key.
    pub(crate) wrapped_dek_nonce: Vec<u8>,
    /// Key-encryption-key identifier.
    pub(crate) key_id: String,
    /// Envelope encryption algorithm identifier.
    pub(crate) algorithm: String,
    /// AAD version written by the future identity document crypto layer.
    pub(crate) aad_version: i64,
    /// Creation timestamp in Unix nanoseconds.
    pub(crate) created_at_unix_nanos: i64,
    /// Last update timestamp in Unix nanoseconds.
    pub(crate) updated_at_unix_nanos: i64,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct IdentitySpecDocumentWrite {
    /// Encrypted opaque setup-input bytes.
    pub(crate) ciphertext: Vec<u8>,
    /// Nonce used by the envelope-encrypted document.
    pub(crate) nonce: Vec<u8>,
    /// Wrapped data-encryption key bytes.
    pub(crate) wrapped_dek: Vec<u8>,
    /// Nonce used for the wrapped data-encryption key.
    pub(crate) wrapped_dek_nonce: Vec<u8>,
    /// Key-encryption-key identifier.
    pub(crate) key_id: String,
    /// Envelope encryption algorithm identifier.
    pub(crate) algorithm: String,
    /// AAD version written by the future identity document crypto layer.
    pub(crate) aad_version: i64,
}

impl std::fmt::Debug for IdentitySpecDocumentRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentitySpecDocumentRecord")
            .field("key", &self.key)
            .field("document_version", &self.document_version)
            .field("ciphertext_len", &self.ciphertext.len())
            .field("nonce_len", &self.nonce.len())
            .field("wrapped_dek_len", &self.wrapped_dek.len())
            .field("wrapped_dek_nonce_len", &self.wrapped_dek_nonce.len())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for IdentitySpecDocumentWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentitySpecDocumentWrite")
            .field("ciphertext_len", &self.ciphertext.len())
            .field("nonce_len", &self.nonce.len())
            .field("wrapped_dek_len", &self.wrapped_dek.len())
            .field("wrapped_dek_nonce_len", &self.wrapped_dek_nonce.len())
            .finish_non_exhaustive()
    }
}

impl IdentitySpecDocumentWrite {
    fn validate(&self) -> Result<(), DbError> {
        if self.ciphertext.is_empty()
            || self.nonce.is_empty()
            || self.wrapped_dek.is_empty()
            || self.wrapped_dek_nonce.is_empty()
        {
            return Err(DbError::InvalidData(
                "identity spec document has an empty encrypted byte field".to_string(),
            ));
        }
        if self.key_id.trim().is_empty() || self.algorithm.trim().is_empty() {
            return Err(DbError::InvalidData(
                "identity spec document has invalid envelope metadata".to_string(),
            ));
        }
        if self.aad_version != SUPPORTED_AAD_VERSION {
            return Err(DbError::InvalidData(format!(
                "identity spec document has unsupported aad_version {}",
                self.aad_version
            )));
        }
        Ok(())
    }
}

#[derive(Debug, sqlx::FromRow)]
struct IdentitySpecDocumentRow {
    scope_kind: String,
    scope_id: String,
    name: String,
    document_version: i64,
    ciphertext: Vec<u8>,
    nonce: Vec<u8>,
    wrapped_dek: Vec<u8>,
    wrapped_dek_nonce: Vec<u8>,
    key_id: String,
    algorithm: String,
    aad_version: i64,
    created_at_unix_nanos: i64,
    updated_at_unix_nanos: i64,
}

impl IdentitySpecDocumentRow {
    fn validate(self) -> Result<IdentitySpecDocumentRecord, DbError> {
        if self.document_version < 0
            || self.created_at_unix_nanos < 0
            || self.updated_at_unix_nanos < 0
        {
            return Err(DbError::InvalidData(
                "identity spec document has negative version or timestamp".to_string(),
            ));
        }
        let document = IdentitySpecDocumentWrite {
            ciphertext: self.ciphertext,
            nonce: self.nonce,
            wrapped_dek: self.wrapped_dek,
            wrapped_dek_nonce: self.wrapped_dek_nonce,
            key_id: self.key_id,
            algorithm: self.algorithm,
            aad_version: self.aad_version,
        };
        document.validate()?;
        Ok(IdentitySpecDocumentRecord {
            key: IdentitySpecKey::from_document_storage_parts(
                &self.scope_kind,
                &self.scope_id,
                &self.name,
            )?,
            document_version: self.document_version,
            ciphertext: document.ciphertext,
            nonce: document.nonce,
            wrapped_dek: document.wrapped_dek,
            wrapped_dek_nonce: document.wrapped_dek_nonce,
            key_id: document.key_id,
            algorithm: document.algorithm,
            aad_version: document.aad_version,
            created_at_unix_nanos: self.created_at_unix_nanos,
            updated_at_unix_nanos: self.updated_at_unix_nanos,
        })
    }
}

/// Repository for encrypted setup-input documents owned by identity specs.
pub(crate) struct IdentitySpecDocumentsRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> IdentitySpecDocumentsRepo<'a, S>
where
    S: DbSession,
{
    /// Create an identity-spec document repository over an existing DB session.
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    /// Load one encrypted setup-input document by identity spec key.
    pub(crate) async fn load_optional(
        &mut self,
        key: &IdentitySpecKey,
    ) -> Result<Option<IdentitySpecDocumentRecord>, DbError> {
        let row: Option<IdentitySpecDocumentRow> = self
            .session
            .fetch_optional(
                identity_spec_document_select()
                    .and_where(document_key_where(key))
                    .to_owned(),
            )
            .await?;
        row.map(IdentitySpecDocumentRow::validate).transpose()
    }

    /// List encrypted setup-input documents for global identity specs.
    pub(crate) async fn list_global(&mut self) -> Result<Vec<IdentitySpecDocumentRecord>, DbError> {
        self.list_scope(&IdentitySpecScope::Global).await
    }

    /// List encrypted setup-input documents for one workspace's identity specs.
    pub(crate) async fn list_workspace(
        &mut self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<IdentitySpecDocumentRecord>, DbError> {
        self.list_scope(&IdentitySpecScope::Workspace(workspace_name.clone()))
            .await
    }

    /// List global setup-input documents followed by one workspace's documents.
    pub(crate) async fn list_global_and_workspace(
        &mut self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<IdentitySpecDocumentRecord>, DbError> {
        let mut records = self.list_global().await?;
        records.extend(self.list_workspace(workspace_name).await?);
        Ok(records)
    }

    async fn list_scope(
        &mut self,
        scope: &IdentitySpecScope,
    ) -> Result<Vec<IdentitySpecDocumentRecord>, DbError> {
        let rows: Vec<IdentitySpecDocumentRow> = self
            .session
            .fetch_all(
                identity_spec_document_select()
                    .and_where(document_scope_where(scope))
                    .order_by(IdentitySpecDocuments::Name, Order::Asc)
                    .to_owned(),
            )
            .await?;
        rows.into_iter()
            .map(IdentitySpecDocumentRow::validate)
            .collect()
    }
}

impl<S> IdentitySpecDocumentsRepo<'_, S>
where
    S: DbWriteSession,
{
    /// Insert or replace an encrypted setup-input document and bump its version.
    pub(crate) async fn upsert(
        &mut self,
        key: &IdentitySpecKey,
        document: &IdentitySpecDocumentWrite,
        now_unix_nanos: i64,
    ) -> Result<IdentitySpecDocumentRecord, DbError> {
        validate_timestamp(now_unix_nanos)?;
        document.validate()?;
        let Some(current) = self.load_optional(key).await? else {
            self.insert(key, document, now_unix_nanos).await?;
            return self.load_optional(key).await?.ok_or_else(|| {
                DbError::InvalidData("identity spec document upsert did not persist".to_string())
            });
        };
        let document_version = current.document_version.checked_add(1).ok_or_else(|| {
            DbError::InvalidData(format!(
                "identity spec document version overflow for {}:{}",
                key.scope.scope_id(),
                key.name
            ))
        })?;
        let statement = Query::update()
            .table(IdentitySpecDocuments::Table)
            .value(
                IdentitySpecDocuments::DocumentVersion,
                Expr::val(document_version),
            )
            .value(
                IdentitySpecDocuments::Ciphertext,
                Expr::val(document.ciphertext.clone()),
            )
            .value(
                IdentitySpecDocuments::Nonce,
                Expr::val(document.nonce.clone()),
            )
            .value(
                IdentitySpecDocuments::WrappedDek,
                Expr::val(document.wrapped_dek.clone()),
            )
            .value(
                IdentitySpecDocuments::WrappedDekNonce,
                Expr::val(document.wrapped_dek_nonce.clone()),
            )
            .value(
                IdentitySpecDocuments::KeyId,
                Expr::val(document.key_id.clone()),
            )
            .value(
                IdentitySpecDocuments::Algorithm,
                Expr::val(document.algorithm.clone()),
            )
            .value(
                IdentitySpecDocuments::AadVersion,
                Expr::val(document.aad_version),
            )
            .value(
                IdentitySpecDocuments::UpdatedAtUnixNanos,
                Expr::val(now_unix_nanos),
            )
            .and_where(document_key_where(key))
            .and_where(
                Expr::col(IdentitySpecDocuments::DocumentVersion).eq(current.document_version),
            )
            .to_owned();
        if self.session.execute_update(statement).await? != 1 {
            return Err(DbError::InvalidData(
                "identity spec document changed while being updated".to_string(),
            ));
        }
        self.load_optional(key).await?.ok_or_else(|| {
            DbError::InvalidData("identity spec document disappeared after update".to_string())
        })
    }

    /// Delete one encrypted setup-input document without deleting the spec row.
    pub(crate) async fn delete(
        &mut self,
        key: &IdentitySpecKey,
    ) -> Result<Option<IdentitySpecDocumentRecord>, DbError> {
        let removed = self.load_optional(key).await?;
        let statement = Query::delete()
            .from_table(IdentitySpecDocuments::Table)
            .and_where(document_key_where(key))
            .to_owned();
        self.session.execute_delete(statement).await?;
        Ok(removed)
    }

    async fn insert(
        &mut self,
        key: &IdentitySpecKey,
        document: &IdentitySpecDocumentWrite,
        now_unix_nanos: i64,
    ) -> Result<(), DbError> {
        let statement = Query::insert()
            .into_table(IdentitySpecDocuments::Table)
            .columns([
                IdentitySpecDocuments::ScopeKind,
                IdentitySpecDocuments::ScopeId,
                IdentitySpecDocuments::Name,
                IdentitySpecDocuments::DocumentVersion,
                IdentitySpecDocuments::Ciphertext,
                IdentitySpecDocuments::Nonce,
                IdentitySpecDocuments::WrappedDek,
                IdentitySpecDocuments::WrappedDekNonce,
                IdentitySpecDocuments::KeyId,
                IdentitySpecDocuments::Algorithm,
                IdentitySpecDocuments::AadVersion,
                IdentitySpecDocuments::CreatedAtUnixNanos,
                IdentitySpecDocuments::UpdatedAtUnixNanos,
            ])
            .values_panic([
                Expr::val(key.scope.kind()),
                Expr::val(key.scope.scope_id().to_string()),
                Expr::val(key.name.clone()),
                Expr::val(1),
                Expr::val(document.ciphertext.clone()),
                Expr::val(document.nonce.clone()),
                Expr::val(document.wrapped_dek.clone()),
                Expr::val(document.wrapped_dek_nonce.clone()),
                Expr::val(document.key_id.clone()),
                Expr::val(document.algorithm.clone()),
                Expr::val(document.aad_version),
                Expr::val(now_unix_nanos),
                Expr::val(now_unix_nanos),
            ])
            .to_owned();
        self.session.execute(statement).await
    }
}

fn identity_spec_select() -> sea_query::SelectStatement {
    Query::select()
        .columns([
            IdentitySpecs::ScopeKind,
            IdentitySpecs::ScopeId,
            IdentitySpecs::WorkspaceId,
            IdentitySpecs::Name,
            IdentitySpecs::Version,
            IdentitySpecs::Description,
            IdentitySpecs::Issuer,
            IdentitySpecs::IdentityType,
            IdentitySpecs::ManifestYaml,
            IdentitySpecs::CreatedAtUnixNanos,
            IdentitySpecs::UpdatedAtUnixNanos,
        ])
        .from(IdentitySpecs::Table)
        .to_owned()
}

fn identity_spec_document_select() -> sea_query::SelectStatement {
    Query::select()
        .columns([
            IdentitySpecDocuments::ScopeKind,
            IdentitySpecDocuments::ScopeId,
            IdentitySpecDocuments::Name,
            IdentitySpecDocuments::DocumentVersion,
            IdentitySpecDocuments::Ciphertext,
            IdentitySpecDocuments::Nonce,
            IdentitySpecDocuments::WrappedDek,
            IdentitySpecDocuments::WrappedDekNonce,
            IdentitySpecDocuments::KeyId,
            IdentitySpecDocuments::Algorithm,
            IdentitySpecDocuments::AadVersion,
            IdentitySpecDocuments::CreatedAtUnixNanos,
            IdentitySpecDocuments::UpdatedAtUnixNanos,
        ])
        .from(IdentitySpecDocuments::Table)
        .to_owned()
}

fn spec_key_where(key: &IdentitySpecKey) -> sea_query::SimpleExpr {
    spec_scope_where(&key.scope).and(Expr::col(IdentitySpecs::Name).eq(key.name.as_str()))
}

fn spec_scope_where(scope: &IdentitySpecScope) -> sea_query::SimpleExpr {
    Expr::col(IdentitySpecs::ScopeKind)
        .eq(scope.kind())
        .and(Expr::col(IdentitySpecs::ScopeId).eq(scope.scope_id()))
}

fn document_key_where(key: &IdentitySpecKey) -> sea_query::SimpleExpr {
    document_scope_where(&key.scope)
        .and(Expr::col(IdentitySpecDocuments::Name).eq(key.name.as_str()))
}

fn document_scope_where(scope: &IdentitySpecScope) -> sea_query::SimpleExpr {
    Expr::col(IdentitySpecDocuments::ScopeKind)
        .eq(scope.kind())
        .and(Expr::col(IdentitySpecDocuments::ScopeId).eq(scope.scope_id()))
}

fn parse_identity_spec_name(name: &str) -> Result<String, DbError> {
    parse_path_segment("identity spec", name).map_err(|error| {
        DbError::InvalidData(format!("invalid identity spec name '{name}': {error}"))
    })
}

fn parse_workspace_name(workspace_id: &str) -> Result<WorkspaceName, DbError> {
    WorkspaceName::parse(workspace_id).map_err(|error| {
        DbError::InvalidData(format!("invalid workspace id '{workspace_id}': {error}"))
    })
}

fn validate_timestamp(now_unix_nanos: i64) -> Result<(), DbError> {
    if now_unix_nanos < 0 {
        return Err(DbError::InvalidData(
            "identity spec timestamp is negative".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use sea_query::{Expr, ExprTrait, Query};
    use tempfile::tempdir;

    use super::{
        GLOBAL_SCOPE_ID, IdentitySpecDocumentRecord, IdentitySpecDocumentWrite, IdentitySpecKey,
        IdentitySpecRecord, IdentitySpecWrite,
    };
    use crate::bootstrap;
    use crate::state::db::schema::{IdentitySpecDocuments, IdentitySpecs};
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DbError, DbWriteSession, ResolvedDatabaseConfig};
    use crate::workspaces::WorkspaceName;

    #[tokio::test]
    async fn identity_spec_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");

        assert_identity_spec_repository_round_trip(&db).await;
    }

    #[tokio::test]
    async fn identity_spec_repository_rejects_corrupt_rows_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");

        assert_identity_spec_repository_rejects_corrupt_rows(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn identity_spec_repository_round_trips_against_postgres() {
        let Some(url) = bootstrap::env_var("CORAL_TEST_POSTGRES_URL") else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_identity_spec_repository_round_trip(&db).await;
    }

    #[expect(clippy::too_many_lines, reason = "repository contract fixture")]
    async fn assert_identity_spec_repository_round_trip(db: &CoralDb) {
        let workspace = unique_workspace("identity");
        let alternate_workspace = unique_workspace("identityalt");
        let global_key = IdentitySpecKey::global("github").expect("global key");
        let workspace_key =
            IdentitySpecKey::workspace(workspace.clone(), "github").expect("workspace key");
        let alternate_key =
            IdentitySpecKey::workspace(alternate_workspace.clone(), "github").expect("alt key");

        let first_global = spec_write("1.0.0", "GitHub OAuth", "github", "oauth");
        let replacement_global = spec_write("1.1.0", "GitHub OAuth v2", "github", "oauth");
        let workspace_spec =
            spec_write("2.0.0", "Workspace override", "github-enterprise", "oauth");

        let mut tx = db.begin().await.expect("begin tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("ensure workspace");
        tx.identity_specs()
            .upsert(&global_key, &first_global, 10)
            .await
            .expect("upsert global spec");
        tx.identity_specs()
            .upsert(&workspace_key, &workspace_spec, 11)
            .await
            .expect("upsert workspace spec");
        tx.identity_spec_documents()
            .upsert(&global_key, &document_write("key-1", b"global-secret"), 12)
            .await
            .expect("upsert global document");
        tx.identity_spec_documents()
            .upsert(
                &workspace_key,
                &document_write("key-2", b"workspace-secret"),
                13,
            )
            .await
            .expect("upsert workspace document");
        tx.commit().await.expect("commit first identity specs");

        assert_eq!(
            load_spec(db, &global_key).await,
            Some(spec_record(global_key.clone(), &first_global, 10, 10))
        );
        assert_eq!(
            list_spec_names(db, &workspace).await,
            vec!["github".to_string(), "github".to_string()]
        );

        let mut tx = db.begin().await.expect("begin replacement tx");
        tx.identity_specs()
            .upsert(&global_key, &replacement_global, 20)
            .await
            .expect("replace global spec");
        let replaced_doc = tx
            .identity_spec_documents()
            .upsert(
                &global_key,
                &document_write("key-3", b"global-replacement"),
                21,
            )
            .await
            .expect("replace global document");
        tx.commit().await.expect("commit replacement");

        assert_eq!(
            load_spec(db, &global_key).await,
            Some(spec_record(global_key.clone(), &replacement_global, 10, 20))
        );
        assert_document(
            &replaced_doc,
            &global_key,
            2,
            "key-3",
            b"global-replacement",
            12,
            21,
        );
        let debug = format!("{replaced_doc:?} {:?}", document_write("debug", b"secret"));
        assert!(debug.contains("ciphertext_len") && !debug.contains("global-replacement"));
        assert!(!debug.contains("secret"));

        let mut tx = db.begin().await.expect("begin direct document delete tx");
        let removed = tx
            .identity_spec_documents()
            .delete(&workspace_key)
            .await
            .expect("delete workspace document");
        tx.identity_spec_documents()
            .upsert(
                &workspace_key,
                &document_write("key-4", b"workspace-secret-2"),
                22,
            )
            .await
            .expect("reinsert workspace document");
        tx.commit().await.expect("commit direct document delete");
        assert!(removed.is_some());
        assert!(load_spec(db, &workspace_key).await.is_some());

        let mut tx = db.begin().await.expect("begin alternate tx");
        tx.workspaces()
            .ensure(alternate_workspace.as_str(), 23)
            .await
            .expect("ensure alternate workspace");
        tx.identity_specs()
            .upsert(&alternate_key, &workspace_spec, 24)
            .await
            .expect("upsert alternate workspace spec");
        tx.identity_spec_documents()
            .upsert(&alternate_key, &document_write("key-5", b"alternate"), 25)
            .await
            .expect("upsert alternate document");
        tx.commit().await.expect("commit alternate");
        assert_eq!(document_list(db, &alternate_workspace).await.len(), 2);

        let mut tx = db.begin().await.expect("begin overflow tx");
        set_document_version(&mut tx, &alternate_key, i64::MAX).await;
        let error = tx
            .identity_spec_documents()
            .upsert(&alternate_key, &document_write("key-6", b"overflow"), 26)
            .await
            .expect_err("max document_version should reject next upsert");
        assert!(error.to_string().contains("version overflow"));
        tx.rollback().await.expect("rollback overflow tx");

        let mut tx = db.begin().await.expect("begin unsupported aad tx");
        let error = tx
            .identity_spec_documents()
            .upsert(
                &alternate_key,
                &document_write_with_aad_version("key-unsupported", b"unsupported", 2),
                27,
            )
            .await
            .expect_err("unsupported aad_version should reject writes");
        assert_invalid_data_contains(error, "unsupported aad_version 2");
        tx.rollback().await.expect("rollback unsupported aad tx");

        let mut tx = db.begin().await.expect("begin workspace cascade tx");
        tx.workspaces()
            .remove(workspace.as_str())
            .await
            .expect("delete workspace");
        tx.commit().await.expect("commit workspace cascade");
        assert!(load_spec(db, &workspace_key).await.is_none());
        assert!(load_document(db, &workspace_key).await.is_none());
        assert!(load_spec(db, &global_key).await.is_some());
        assert!(load_document(db, &global_key).await.is_some());

        let mut tx = db.begin().await.expect("begin spec delete tx");
        let removed = tx
            .identity_specs()
            .delete(&global_key)
            .await
            .expect("delete global spec");
        tx.commit().await.expect("commit spec delete");
        assert!(removed.is_some());
        assert!(load_document(db, &global_key).await.is_none());

        let missing_workspace = unique_workspace("missing");
        let missing_workspace_key =
            IdentitySpecKey::workspace(missing_workspace, "missing").expect("missing key");
        let mut tx = db.begin().await.expect("begin orphan tx");
        let error = tx
            .identity_specs()
            .upsert(&missing_workspace_key, &workspace_spec, 30)
            .await
            .expect_err("workspace spec rows must require an existing workspace");
        assert!(error.to_string().to_lowercase().contains("foreign key"));
        tx.rollback().await.expect("rollback orphan tx");
    }

    async fn assert_identity_spec_repository_rejects_corrupt_rows(db: &CoralDb) {
        let key = IdentitySpecKey::global("corrupt").expect("key");
        insert_corrupt_spec_row(db).await;
        let mut session = db;
        let error = session
            .identity_specs()
            .list_global()
            .await
            .expect_err("invalid identity spec row should fail");
        assert_invalid_data_contains(error, "negative timestamp");

        let mut tx = db.begin().await.expect("begin valid spec tx");
        tx.identity_specs()
            .upsert(&key, &spec_write("1.0.0", "Corrupt", "issuer", "oauth"), 10)
            .await
            .expect("upsert valid spec");
        insert_corrupt_document_row(&mut tx, &key).await;
        tx.commit().await.expect("commit corrupt document");

        let error = session
            .identity_spec_documents()
            .load_optional(&key)
            .await
            .expect_err("invalid document row should fail");
        assert_invalid_data_contains(error, "unsupported aad_version 2");
    }

    async fn load_spec(db: &CoralDb, key: &IdentitySpecKey) -> Option<IdentitySpecRecord> {
        let mut session = db;
        session
            .identity_specs()
            .load_optional(key)
            .await
            .expect("load identity spec")
    }

    async fn load_document(
        db: &CoralDb,
        key: &IdentitySpecKey,
    ) -> Option<IdentitySpecDocumentRecord> {
        let mut session = db;
        session
            .identity_spec_documents()
            .load_optional(key)
            .await
            .expect("load identity spec document")
    }

    async fn list_spec_names(db: &CoralDb, workspace: &WorkspaceName) -> Vec<String> {
        let mut session = db;
        session
            .identity_specs()
            .list_global_and_workspace(workspace)
            .await
            .expect("list identity specs")
            .into_iter()
            .map(|record| record.key.name)
            .collect()
    }

    async fn document_list(
        db: &CoralDb,
        workspace: &WorkspaceName,
    ) -> Vec<IdentitySpecDocumentRecord> {
        let mut session = db;
        session
            .identity_spec_documents()
            .list_global_and_workspace(workspace)
            .await
            .expect("list identity spec documents")
    }

    async fn set_document_version<S>(session: &mut S, key: &IdentitySpecKey, version: i64)
    where
        S: DbWriteSession,
    {
        session
            .execute_update(
                Query::update()
                    .table(IdentitySpecDocuments::Table)
                    .value(IdentitySpecDocuments::DocumentVersion, Expr::val(version))
                    .and_where(Expr::col(IdentitySpecDocuments::ScopeKind).eq(key.scope.kind()))
                    .and_where(Expr::col(IdentitySpecDocuments::ScopeId).eq(key.scope.scope_id()))
                    .and_where(Expr::col(IdentitySpecDocuments::Name).eq(key.name.as_str()))
                    .to_owned(),
            )
            .await
            .expect("set document version");
    }

    async fn insert_corrupt_spec_row(db: &CoralDb) {
        let mut tx = db.begin().await.expect("begin corrupt spec tx");
        tx.execute(
            Query::insert()
                .into_table(IdentitySpecs::Table)
                .columns([
                    IdentitySpecs::ScopeKind,
                    IdentitySpecs::ScopeId,
                    IdentitySpecs::WorkspaceId,
                    IdentitySpecs::Name,
                    IdentitySpecs::Version,
                    IdentitySpecs::Description,
                    IdentitySpecs::Issuer,
                    IdentitySpecs::IdentityType,
                    IdentitySpecs::ManifestYaml,
                    IdentitySpecs::CreatedAtUnixNanos,
                    IdentitySpecs::UpdatedAtUnixNanos,
                ])
                .values_panic([
                    Expr::val("global"),
                    Expr::val(GLOBAL_SCOPE_ID),
                    Expr::val(Option::<String>::None),
                    Expr::val("corrupt-timestamp"),
                    Expr::val("1.0.0"),
                    Expr::val("corrupt"),
                    Expr::val("issuer"),
                    Expr::val("oauth"),
                    Expr::val("name: corrupt\n"),
                    Expr::val(-1),
                    Expr::val(-1),
                ])
                .to_owned(),
        )
        .await
        .expect("insert corrupt identity spec");
        tx.commit().await.expect("commit corrupt spec");
    }

    async fn insert_corrupt_document_row<S>(session: &mut S, key: &IdentitySpecKey)
    where
        S: DbWriteSession,
    {
        session
            .execute(
                Query::insert()
                    .into_table(IdentitySpecDocuments::Table)
                    .columns([
                        IdentitySpecDocuments::ScopeKind,
                        IdentitySpecDocuments::ScopeId,
                        IdentitySpecDocuments::Name,
                        IdentitySpecDocuments::DocumentVersion,
                        IdentitySpecDocuments::Ciphertext,
                        IdentitySpecDocuments::Nonce,
                        IdentitySpecDocuments::WrappedDek,
                        IdentitySpecDocuments::WrappedDekNonce,
                        IdentitySpecDocuments::KeyId,
                        IdentitySpecDocuments::Algorithm,
                        IdentitySpecDocuments::AadVersion,
                        IdentitySpecDocuments::CreatedAtUnixNanos,
                        IdentitySpecDocuments::UpdatedAtUnixNanos,
                    ])
                    .values_panic([
                        Expr::val(key.scope.kind()),
                        Expr::val(key.scope.scope_id().to_string()),
                        Expr::val(key.name.clone()),
                        Expr::val(1),
                        Expr::val(b"ciphertext".to_vec()),
                        Expr::val(b"nonce".to_vec()),
                        Expr::val(b"wrapped-dek".to_vec()),
                        Expr::val(b"wrapped-dek-nonce".to_vec()),
                        Expr::val("key"),
                        Expr::val("AES-256-GCM"),
                        Expr::val(2),
                        Expr::val(10),
                        Expr::val(10),
                    ])
                    .to_owned(),
            )
            .await
            .expect("insert corrupt identity spec document");
    }

    fn spec_record(
        key: IdentitySpecKey,
        spec: &IdentitySpecWrite,
        created_at_unix_nanos: i64,
        updated_at_unix_nanos: i64,
    ) -> IdentitySpecRecord {
        IdentitySpecRecord {
            key,
            version: spec.version.clone(),
            description: spec.description.clone(),
            issuer: spec.issuer.clone(),
            identity_type: spec.identity_type.clone(),
            manifest_yaml: spec.manifest_yaml.clone(),
            created_at_unix_nanos,
            updated_at_unix_nanos,
        }
    }

    fn assert_document(
        record: &IdentitySpecDocumentRecord,
        key: &IdentitySpecKey,
        version: i64,
        key_id: &str,
        ciphertext: &[u8],
        created_at_unix_nanos: i64,
        updated_at_unix_nanos: i64,
    ) {
        assert_eq!(&record.key, key);
        assert_eq!(record.document_version, version);
        assert_eq!(record.key_id, key_id);
        assert_eq!(record.ciphertext.as_slice(), ciphertext);
        assert_eq!(record.created_at_unix_nanos, created_at_unix_nanos);
        assert_eq!(record.updated_at_unix_nanos, updated_at_unix_nanos);
    }

    fn spec_write(
        version: &str,
        description: &str,
        issuer: &str,
        identity_type: &str,
    ) -> IdentitySpecWrite {
        IdentitySpecWrite {
            version: version.to_string(),
            description: description.to_string(),
            issuer: issuer.to_string(),
            identity_type: identity_type.to_string(),
            manifest_yaml: format!(
                "name: identity\nversion: {version}\nissuer: {issuer}\ntype: {identity_type}\n"
            ),
        }
    }

    fn document_write(key_id: &str, ciphertext: &[u8]) -> IdentitySpecDocumentWrite {
        IdentitySpecDocumentWrite {
            ciphertext: ciphertext.to_vec(),
            nonce: format!("nonce-{key_id}").into_bytes(),
            wrapped_dek: format!("wrapped-{key_id}").into_bytes(),
            wrapped_dek_nonce: format!("wrapped-nonce-{key_id}").into_bytes(),
            key_id: key_id.to_string(),
            algorithm: "AES-256-GCM".to_string(),
            aad_version: 1,
        }
    }

    fn document_write_with_aad_version(
        key_id: &str,
        ciphertext: &[u8],
        aad_version: i64,
    ) -> IdentitySpecDocumentWrite {
        IdentitySpecDocumentWrite {
            aad_version,
            ..document_write(key_id, ciphertext)
        }
    }

    fn unique_workspace(prefix: &str) -> WorkspaceName {
        WorkspaceName::parse(&format!("{prefix}{}", uuid::Uuid::new_v4().simple()))
            .expect("workspace")
    }

    fn assert_invalid_data_contains(error: DbError, expected: &str) {
        let DbError::InvalidData(message) = error else {
            panic!("unexpected error: {error}");
        };
        assert!(
            message.contains(expected),
            "expected {expected:?} in error: {message}"
        );
    }
}
