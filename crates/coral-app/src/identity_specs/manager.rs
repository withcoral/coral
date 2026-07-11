//! Read and resolution behavior for installed identity specs.

use std::sync::Arc;

use coral_spec::{IdentityManifest, parse_identity_manifest_yaml};

use crate::bootstrap::AppError;
use crate::state::db::{
    CoralDb, CoralTx, DbRepos, IdentitySpecKey, IdentitySpecRecord, IdentitySpecScope,
};
use crate::workspaces::WorkspaceName;

/// One installed identity spec, including the scope that actually supplied it.
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
        let mut tx = self.db.begin_read_snapshot().await?;
        require_scope_workspace(&mut tx, key.scope()).await?;
        let record = tx.identity_specs().load_optional(key).await?;
        tx.commit().await?;
        let installed = record
            .map(record_to_installed)
            .transpose()?
            .ok_or_else(|| spec_not_found(key))?;
        Ok(installed)
    }

    /// Fetch one global spec by name.
    pub(crate) async fn get_global(&self, name: &str) -> Result<InstalledIdentitySpec, AppError> {
        self.get_exact(&IdentitySpecKey::global(name)?).await
    }

    /// List specs in exactly one scope, without fallback.
    pub(crate) async fn list_exact(
        &self,
        scope: &IdentitySpecScope,
    ) -> Result<Vec<InstalledIdentitySpec>, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        require_scope_workspace(&mut tx, scope).await?;
        let records = match scope {
            IdentitySpecScope::Global => tx.identity_specs().list_global().await?,
            IdentitySpecScope::Workspace(workspace) => {
                tx.identity_specs().list_workspace(workspace).await?
            }
        };
        tx.commit().await?;
        convert_records(records)
    }

    /// List global specs followed by workspace specs, preserving same-name entries.
    pub(crate) async fn list_workspace_with_global(
        &self,
        workspace: &WorkspaceName,
    ) -> Result<Vec<InstalledIdentitySpec>, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        require_workspace(&mut tx, workspace).await?;
        let mut records = tx.identity_specs().list_global().await?;
        records.extend(tx.identity_specs().list_workspace(workspace).await?);
        tx.commit().await?;
        convert_records(records)
    }

    /// Resolve one workspace spec, preferring its workspace definition over global.
    pub(crate) async fn resolve_for_workspace(
        &self,
        workspace: &WorkspaceName,
        name: &str,
    ) -> Result<InstalledIdentitySpec, AppError> {
        let key = IdentitySpecKey::workspace(workspace.clone(), name)?;
        let mut tx = self.db.begin_read_snapshot().await?;
        require_workspace(&mut tx, workspace).await?;
        let record = tx.identity_specs().resolve_optional(&key).await?;
        tx.commit().await?;
        let installed = record
            .map(record_to_installed)
            .transpose()?
            .ok_or_else(|| spec_not_found(&key))?;
        Ok(installed)
    }

    /// List the effective specs for a workspace, shadowed and sorted by name.
    pub(crate) async fn list_resolved_for_workspace(
        &self,
        workspace: &WorkspaceName,
    ) -> Result<Vec<InstalledIdentitySpec>, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        require_workspace(&mut tx, workspace).await?;
        let records = tx
            .identity_specs()
            .list_resolved_for_workspace(workspace)
            .await?;
        tx.commit().await?;
        convert_records(records)
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

fn convert_records(
    records: Vec<IdentitySpecRecord>,
) -> Result<Vec<InstalledIdentitySpec>, AppError> {
    records.into_iter().map(record_to_installed).collect()
}

fn record_to_installed(record: IdentitySpecRecord) -> Result<InstalledIdentitySpec, AppError> {
    let manifest = parse_identity_manifest_yaml(&record.manifest_yaml).map_err(|error| {
        corrupt_record(&record.key, &format!("manifest cannot be parsed: {error}"))
    })?;
    for (field, stored, parsed) in [
        ("name", record.key.name(), manifest.name.as_str()),
        (
            "version",
            record.version.as_str(),
            manifest.version.as_str(),
        ),
        (
            "description",
            record.description.as_str(),
            manifest.description.as_str(),
        ),
        ("issuer", record.issuer.as_str(), manifest.issuer.as_str()),
        (
            "identity_type",
            record.identity_type.as_str(),
            manifest.identity_type.label(),
        ),
    ] {
        if stored != parsed {
            return Err(corrupt_record(
                &record.key,
                &format!("stored {field} does not match manifest"),
            ));
        }
    }
    Ok(InstalledIdentitySpec {
        key: record.key,
        manifest_yaml: record.manifest_yaml,
        manifest,
    })
}

fn corrupt_record(key: &IdentitySpecKey, detail: &str) -> AppError {
    AppError::Database(format!(
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

    use coral_api::{CORAL_ERROR_DOMAIN, CORAL_ERROR_REASON_IDENTITY_SPEC_NOT_FOUND};
    use coral_spec::parse_identity_manifest_yaml;
    use tempfile::{TempDir, tempdir};
    use tonic::Code;
    use tonic_types::{ErrorDetail, StatusExt as _};

    use super::{IdentitySpecManager, InstalledIdentitySpec, record_to_installed, scope_label};
    use crate::bootstrap::{AppError, app_status};
    use crate::state::db::{
        CoralDb, CoralTx, DbRepos, IdentitySpecKey, IdentitySpecRecord, IdentitySpecScope,
        IdentitySpecWrite, ResolvedDatabaseConfig,
    };
    use crate::workspaces::WorkspaceName;

    macro_rules! assert_workspace_missing {
        ($future:expr) => {
            assert!(matches!(
                $future.await,
                Err(AppError::WorkspaceNotFound(name)) if name == "missing"
            ));
        };
    }

    macro_rules! assert_list {
        ($future:expr, $expected:expr) => {{
            let specs = $future.await.expect("identity spec list");
            assert_eq!(labels(&specs), $expected);
        }};
    }

    struct Fixture {
        _temp: TempDir,
        db: Arc<CoralDb>,
        manager: IdentitySpecManager,
        workspace: WorkspaceName,
    }

    type RecordDrift = (&'static str, fn(&mut IdentitySpecRecord));

    #[tokio::test]
    async fn reads_exact_combined_and_effective_scopes() {
        let fixture = fixture().await;
        let workspace = &fixture.workspace;
        let workspace_scope = IdentitySpecScope::workspace(workspace.clone());

        let global_alpha = fixture
            .manager
            .get_global("alpha")
            .await
            .expect("global alpha");
        assert_eq!(global_alpha.manifest.version, "global_alpha");
        assert_eq!(scope_label(global_alpha.key.scope()), "global");

        let exact_workspace_beta = fixture
            .manager
            .get_exact(&IdentitySpecKey::workspace(workspace.clone(), "beta").expect("key"))
            .await
            .expect_err("exact workspace read must not fall back");
        assert!(matches!(
            exact_workspace_beta,
            AppError::IdentitySpecNotFound { name, scope }
                if name == "beta" && scope == format!("workspace:{workspace}")
        ));

        let fallback = fixture
            .manager
            .resolve_for_workspace(workspace, "beta")
            .await
            .expect("global fallback");
        assert_eq!(scope_label(fallback.key.scope()), "global");
        let shadow = fixture
            .manager
            .resolve_for_workspace(workspace, "alpha")
            .await
            .expect("workspace shadow");
        assert_eq!(shadow.manifest.version, "workspace_alpha");
        assert_eq!(scope_label(shadow.key.scope()), "workspace:work");

        assert_list!(
            fixture.manager.list_exact(&IdentitySpecScope::global()),
            ["global:alpha", "global:beta"]
        );
        assert_list!(
            fixture.manager.list_exact(&workspace_scope),
            ["workspace:work:alpha", "workspace:work:gamma"]
        );
        assert_list!(
            fixture.manager.list_workspace_with_global(workspace),
            [
                "global:alpha",
                "global:beta",
                "workspace:work:alpha",
                "workspace:work:gamma",
            ]
        );
        assert_list!(
            fixture.manager.list_resolved_for_workspace(workspace),
            [
                "workspace:work:alpha",
                "global:beta",
                "workspace:work:gamma",
            ]
        );

        assert!(matches!(
            fixture.manager.get_global("gamma").await,
            Err(AppError::IdentitySpecNotFound { scope, .. }) if scope == "global"
        ));
        let missing = fixture
            .manager
            .get_global("missing")
            .await
            .expect_err("missing global spec");
        let status = app_status(missing);
        assert_eq!(status.code(), Code::NotFound);
        let info = status
            .get_error_details_vec()
            .into_iter()
            .find_map(|detail| match detail {
                ErrorDetail::ErrorInfo(info) => Some(info),
                _ => None,
            })
            .expect("typed ErrorInfo");
        assert_eq!(info.reason, CORAL_ERROR_REASON_IDENTITY_SPEC_NOT_FOUND);
        assert_eq!(info.domain, CORAL_ERROR_DOMAIN);
        assert!(info.metadata.is_empty());
        assert!(matches!(
            fixture.manager.get_global("bad-name").await,
            Err(AppError::InvalidInput(_))
        ));
    }

    #[tokio::test]
    async fn workspace_checks_and_corruption_fail_closed() {
        let fixture = fixture().await;
        let missing = WorkspaceName::parse("missing").expect("missing workspace");
        let missing_scope = IdentitySpecScope::workspace(missing.clone());
        let missing_key = IdentitySpecKey::workspace(missing.clone(), "alpha").expect("key");
        assert_workspace_missing!(fixture.manager.get_exact(&missing_key));
        assert_workspace_missing!(fixture.manager.list_exact(&missing_scope));
        assert_workspace_missing!(fixture.manager.list_workspace_with_global(&missing));
        assert_workspace_missing!(fixture.manager.resolve_for_workspace(&missing, "alpha"));
        assert_workspace_missing!(fixture.manager.list_resolved_for_workspace(&missing));

        seed_corrupt_records(&fixture).await;

        assert!(matches!(
            fixture
                .manager
                .resolve_for_workspace(&fixture.workspace, "corrupt")
                .await,
            Err(AppError::Database(_))
        ));
        assert!(matches!(
            fixture
                .manager
                .list_resolved_for_workspace(&fixture.workspace)
                .await,
            Err(AppError::Database(_))
        ));
        assert_metadata_drifts_fail();
    }

    async fn seed_corrupt_records(fixture: &Fixture) {
        let mut tx = fixture.db.begin().await.expect("begin corruption seed");
        seed_valid(
            &mut tx,
            &IdentitySpecKey::global("corrupt").expect("key"),
            "fallback",
        )
        .await;
        seed_write(
            &mut tx,
            &IdentitySpecKey::workspace(fixture.workspace.clone(), "corrupt").expect("corrupt key"),
            IdentitySpecWrite::new(
                "invalid",
                "invalid",
                "issuer_invalid",
                "fixed_token",
                "not: [valid yaml",
            )
            .expect("repository-shaped corrupt spec"),
        )
        .await;
        tx.commit().await.expect("commit corruption seed");
    }

    fn assert_metadata_drifts_fail() {
        let manifest_yaml = manifest("drift", "canonical");
        let manifest = parse_identity_manifest_yaml(&manifest_yaml).expect("valid manifest");
        let record = IdentitySpecRecord {
            key: IdentitySpecKey::global("drift").expect("key"),
            version: manifest.version,
            description: manifest.description,
            issuer: manifest.issuer,
            identity_type: manifest.identity_type.label().to_string(),
            manifest_yaml,
            created_at_unix_nanos: 1,
            updated_at_unix_nanos: 1,
        };
        let drifts: [RecordDrift; 5] = [
            ("name", |row| {
                row.key = IdentitySpecKey::global("other").expect("key");
            }),
            ("version", |row| row.version.push_str("_drift")),
            ("description", |row| row.description.push_str("_drift")),
            ("issuer", |row| row.issuer.push_str("_drift")),
            ("identity_type", |row| row.identity_type.push_str("_drift")),
        ];
        for (field, drift) in drifts {
            let mut drifted = record.clone();
            drift(&mut drifted);
            let error = record_to_installed(drifted).expect_err("metadata drift must fail");
            assert!(
                matches!(&error, AppError::Database(detail) if detail.contains(field)),
                "unexpected {field} drift error: {error}"
            );
        }
    }

    async fn fixture() -> Fixture {
        let temp = tempdir().expect("temp dir");
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite {
                path: temp.path().join("coral.sqlite"),
            })
            .await
            .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate sqlite");
        let workspace = WorkspaceName::parse("work").expect("workspace");
        let mut tx = db.begin().await.expect("begin fixture seed");
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("seed workspace");
        for (key, label) in [
            (
                IdentitySpecKey::global("alpha").expect("key"),
                "global_alpha",
            ),
            (IdentitySpecKey::global("beta").expect("key"), "global_beta"),
            (
                IdentitySpecKey::workspace(workspace.clone(), "alpha").expect("key"),
                "workspace_alpha",
            ),
            (
                IdentitySpecKey::workspace(workspace.clone(), "gamma").expect("key"),
                "workspace_gamma",
            ),
        ] {
            seed_valid(&mut tx, &key, label).await;
        }
        tx.commit().await.expect("commit fixture seed");
        Fixture {
            _temp: temp,
            manager: IdentitySpecManager::new(Arc::clone(&db)),
            db,
            workspace,
        }
    }

    async fn seed_valid(tx: &mut CoralTx<'_>, key: &IdentitySpecKey, label: &str) {
        let yaml = manifest(key.name(), label);
        let parsed = parse_identity_manifest_yaml(&yaml).expect("valid identity manifest");
        let write = IdentitySpecWrite::new(
            &parsed.version,
            &parsed.description,
            &parsed.issuer,
            parsed.identity_type.label(),
            yaml,
        )
        .expect("valid identity write");
        seed_write(tx, key, write).await;
    }

    async fn seed_write(tx: &mut CoralTx<'_>, key: &IdentitySpecKey, write: IdentitySpecWrite) {
        tx.identity_specs()
            .upsert(key, &write, 2)
            .await
            .expect("seed identity spec");
    }

    fn manifest(name: &str, label: &str) -> String {
        format!(
            "kind: identity\nspec_version: 1\nname: {name}\nversion: {label}\ndescription: description {label}\nissuer: issuer_{label}\ntype: fixed_token\n"
        )
    }

    fn labels(specs: &[InstalledIdentitySpec]) -> Vec<String> {
        specs
            .iter()
            .map(|spec| format!("{}:{}", scope_label(spec.key.scope()), spec.key.name()))
            .collect()
    }
}
