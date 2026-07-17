//! Read and workspace-fallback behavior for installed identity specs.

use std::sync::Arc;

use coral_spec::{IdentityManifest, IdentitySpecType, parse_identity_manifest_yaml};

use crate::bootstrap::AppError;
use crate::state::db::{
    CoralDb, CoralTx, DbError, DbRepos, IdentitySpecKey, IdentitySpecRecord, IdentitySpecScope,
};
use crate::workspaces::WorkspaceName;

/// One installed identity spec and the exact scope that supplied it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InstalledIdentitySpec {
    pub(crate) key: IdentitySpecKey,
    pub(crate) manifest_yaml: String,
    pub(crate) manifest: IdentityManifest,
}

/// Database-backed identity-spec read and resolution behavior.
#[derive(Clone)]
pub(crate) struct IdentitySpecManager {
    db: Arc<CoralDb>,
}

impl IdentitySpecManager {
    pub(crate) fn new(db: Arc<CoralDb>) -> Self {
        Self { db }
    }

    /// Fetch one spec in exactly the requested scope, without fallback.
    pub(crate) async fn get_exact(
        &self,
        key: &IdentitySpecKey,
    ) -> Result<InstalledIdentitySpec, AppError> {
        self.read_exact(key).await
    }

    /// Fetch one global spec by name.
    pub(crate) async fn get_global(&self, name: &str) -> Result<InstalledIdentitySpec, AppError> {
        self.read_exact(&IdentitySpecKey::global(name)?).await
    }

    /// List specs in exactly one scope, without fallback.
    pub(crate) async fn list_exact(
        &self,
        scope: &IdentitySpecScope,
    ) -> Result<Vec<InstalledIdentitySpec>, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        require_scope_workspace(&mut tx, scope).await?;
        let records = tx.identity_specs().list(scope).await?;
        tx.commit().await?;
        convert_records(records)
    }

    /// List global specs followed by workspace specs, preserving shadowed entries.
    pub(crate) async fn list_workspace_with_global(
        &self,
        workspace: &WorkspaceName,
    ) -> Result<Vec<InstalledIdentitySpec>, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        require_workspace(&mut tx, workspace).await?;
        let mut records = tx
            .identity_specs()
            .list(&IdentitySpecScope::global())
            .await?;
        records.extend(
            tx.identity_specs()
                .list(&IdentitySpecScope::workspace(workspace.clone()))
                .await?,
        );
        tx.commit().await?;
        convert_records(records)
    }

    /// Resolve one workspace spec, preferring workspace scope over global scope.
    pub(crate) async fn resolve_for_workspace(
        &self,
        workspace: &WorkspaceName,
        name: &str,
    ) -> Result<InstalledIdentitySpec, AppError> {
        let requested = IdentitySpecKey::workspace(workspace.clone(), name)?;
        let mut tx = self.db.begin_read_snapshot().await?;
        require_workspace(&mut tx, workspace).await?;
        let record = match tx.identity_specs().get(&requested).await? {
            some @ Some(_) => some,
            None => {
                tx.identity_specs()
                    .get(&IdentitySpecKey::global(name)?)
                    .await?
            }
        };
        tx.commit().await?;
        convert_optional(record, &requested)
    }

    async fn read_exact(&self, key: &IdentitySpecKey) -> Result<InstalledIdentitySpec, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        require_scope_workspace(&mut tx, key.scope()).await?;
        let record = tx.identity_specs().get(key).await?;
        tx.commit().await?;
        convert_optional(record, key)
    }
}

async fn require_scope_workspace(
    tx: &mut CoralTx<'_>,
    scope: &IdentitySpecScope,
) -> Result<(), AppError> {
    if let IdentitySpecScope::Workspace(workspace) = scope {
        require_workspace(tx, workspace).await?;
    }
    Ok(())
}

async fn require_workspace(
    tx: &mut CoralTx<'_>,
    workspace: &WorkspaceName,
) -> Result<(), AppError> {
    if tx.workspaces().get(workspace.as_str()).await?.is_none() {
        return Err(AppError::WorkspaceNotFound(workspace.to_string()));
    }
    Ok(())
}

fn convert_optional(
    record: Option<IdentitySpecRecord>,
    requested: &IdentitySpecKey,
) -> Result<InstalledIdentitySpec, AppError> {
    record
        .ok_or_else(|| spec_not_found(requested))
        .and_then(|record| record_to_installed(record).map_err(Into::into))
}

fn convert_records(
    records: Vec<IdentitySpecRecord>,
) -> Result<Vec<InstalledIdentitySpec>, AppError> {
    records
        .into_iter()
        .map(|record| record_to_installed(record).map_err(Into::into))
        .collect()
}

fn record_to_installed(record: IdentitySpecRecord) -> Result<InstalledIdentitySpec, DbError> {
    let manifest = parse_identity_manifest_yaml(&record.manifest_yaml).map_err(|error| {
        corrupt_record(&record.key, &format!("manifest cannot be parsed: {error}"))
    })?;
    require_match(&record.key, "name", record.key.name(), &manifest.name)?;
    require_match(&record.key, "version", &record.version, &manifest.version)?;
    require_match(
        &record.key,
        "description",
        &record.description,
        &manifest.description,
    )?;
    require_match(&record.key, "issuer", &record.issuer, &manifest.issuer)?;
    require_match(
        &record.key,
        "identity_type",
        &record.identity_type,
        identity_type_label(manifest.identity_type),
    )?;
    Ok(InstalledIdentitySpec {
        key: record.key,
        manifest_yaml: record.manifest_yaml,
        manifest,
    })
}

fn require_match(
    key: &IdentitySpecKey,
    field: &str,
    stored: &str,
    parsed: &str,
) -> Result<(), DbError> {
    (stored == parsed)
        .then_some(())
        .ok_or_else(|| corrupt_record(key, &format!("stored {field} does not match manifest")))
}

fn identity_type_label(identity_type: IdentitySpecType) -> &'static str {
    match identity_type {
        IdentitySpecType::OAuth => "oauth",
        IdentitySpecType::FixedToken => "fixed_token",
    }
}

fn corrupt_record(key: &IdentitySpecKey, detail: &str) -> DbError {
    DbError::CorruptData(format!(
        "identity spec '{}:{}' is corrupt: {detail}",
        scope_label(key.scope()),
        key.name()
    ))
}

fn spec_not_found(key: &IdentitySpecKey) -> AppError {
    AppError::IdentitySpecNotFound {
        name: key.name().to_string(),
        scope: scope_label(key.scope()),
    }
}

fn scope_label(scope: &IdentitySpecScope) -> String {
    match scope {
        IdentitySpecScope::Global => "global".to_string(),
        IdentitySpecScope::Workspace(workspace) => format!("workspace:{workspace}"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::{IdentitySpecManager, identity_type_label, record_to_installed, scope_label};
    use crate::bootstrap::AppError;
    use crate::state::db::{
        CoralDb, DbRepos, IdentitySpecId, IdentitySpecKey, IdentitySpecRecord, IdentitySpecScope,
        ResolvedDatabaseConfig,
    };
    use crate::workspaces::WorkspaceName;

    #[tokio::test]
    async fn manager_reads_resolves_and_rejects_corruption() {
        let temp = tempdir().unwrap();
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite {
                path: temp.path().join("coral.sqlite"),
            })
            .await
            .unwrap(),
        );
        db.migrate().await.unwrap();
        let workspace = WorkspaceName::parse("work").unwrap();
        let mut tx = db.begin().await.unwrap();
        tx.workspaces().ensure(workspace.as_str(), 1).await.unwrap();
        for (key, version) in [
            (IdentitySpecKey::global("alpha").unwrap(), "global_alpha"),
            (IdentitySpecKey::global("beta").unwrap(), "global_beta"),
            (
                IdentitySpecKey::workspace(workspace.clone(), "alpha").unwrap(),
                "workspace_alpha",
            ),
            (
                IdentitySpecKey::workspace(workspace.clone(), "gamma").unwrap(),
                "workspace_gamma",
            ),
        ] {
            let manifest_yaml = manifest(key.name(), version);
            let parsed = coral_spec::parse_identity_manifest_yaml(&manifest_yaml).unwrap();
            tx.identity_specs()
                .upsert(&key, &parsed, &manifest_yaml, 2)
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
        let manager = IdentitySpecManager::new(db);

        assert_eq!(
            manager.get_global("alpha").await.unwrap().manifest.version,
            "global_alpha"
        );
        let beta = IdentitySpecKey::workspace(workspace.clone(), "beta").unwrap();
        assert_not_found(manager.get_exact(&beta).await, "workspace:work");
        assert_eq!(
            manager
                .resolve_for_workspace(&workspace, "beta")
                .await
                .unwrap()
                .key
                .scope(),
            &IdentitySpecScope::Global
        );
        assert_eq!(
            manager
                .resolve_for_workspace(&workspace, "alpha")
                .await
                .unwrap()
                .manifest
                .version,
            "workspace_alpha"
        );
        let exact = manager
            .list_exact(&IdentitySpecScope::workspace(workspace.clone()))
            .await
            .unwrap();
        assert_eq!(
            labels(&exact),
            ["workspace:work:alpha", "workspace:work:gamma"]
        );
        let combined = manager
            .list_workspace_with_global(&workspace)
            .await
            .unwrap();
        assert_eq!(
            labels(&combined),
            [
                "global:alpha",
                "global:beta",
                "workspace:work:alpha",
                "workspace:work:gamma"
            ]
        );

        let missing = WorkspaceName::parse("missing").unwrap();
        assert!(
            matches!(manager.resolve_for_workspace(&missing, "beta").await, Err(AppError::WorkspaceNotFound(name)) if name == "missing")
        );
        assert!(
            matches!(manager.list_workspace_with_global(&missing).await, Err(AppError::WorkspaceNotFound(name)) if name == "missing")
        );
        assert_not_found(
            manager.resolve_for_workspace(&workspace, "absent").await,
            "workspace:work",
        );
    }

    #[test]
    fn conversion_rejects_corrupt_manifest_and_metadata_drift() {
        let record = canonical_record();
        let mut invalid_yaml = record.clone();
        invalid_yaml.manifest_yaml = "not: [yaml".to_string();
        assert_corrupt(invalid_yaml);
        for drift in [
            |row: &mut IdentitySpecRecord| row.key = IdentitySpecKey::global("other").unwrap(),
            |row: &mut IdentitySpecRecord| row.version.push_str("_drift"),
            |row: &mut IdentitySpecRecord| row.description.push_str("_drift"),
            |row: &mut IdentitySpecRecord| row.issuer.push_str("_drift"),
            |row: &mut IdentitySpecRecord| row.identity_type.push_str("_drift"),
        ] {
            let mut drifted = record.clone();
            drift(&mut drifted);
            assert_corrupt(drifted);
        }
    }

    fn manifest(name: &str, version: &str) -> String {
        format!(
            "kind: identity\nspec_version: 1\nname: {name}\nversion: {version}\ndescription: description {version}\nissuer: issuer_{version}\ntype: fixed_token\naudience: {{host: example.com}}\n"
        )
    }

    fn labels(specs: &[super::InstalledIdentitySpec]) -> Vec<String> {
        specs
            .iter()
            .map(|spec| format!("{}:{}", scope_label(spec.key.scope()), spec.key.name()))
            .collect()
    }

    fn assert_not_found(result: Result<super::InstalledIdentitySpec, AppError>, scope: &str) {
        assert!(
            matches!(result, Err(AppError::IdentitySpecNotFound { scope: actual, .. }) if actual == scope)
        );
    }

    fn canonical_record() -> IdentitySpecRecord {
        let manifest_yaml = manifest("drift", "canonical");
        let parsed = coral_spec::parse_identity_manifest_yaml(&manifest_yaml).unwrap();
        IdentitySpecRecord {
            id: IdentitySpecId::new(),
            key: IdentitySpecKey::global("drift").unwrap(),
            version: parsed.version,
            description: parsed.description,
            issuer: parsed.issuer,
            identity_type: identity_type_label(parsed.identity_type).to_string(),
            manifest_yaml,
            created_at_unix_nanos: 1,
            updated_at_unix_nanos: 1,
        }
    }

    fn assert_corrupt(record: IdentitySpecRecord) {
        assert!(matches!(
            record_to_installed(record),
            Err(crate::state::db::DbError::CorruptData(_))
        ));
    }
}
