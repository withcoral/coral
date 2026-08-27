use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};

use super::session::DbRepos;
use super::{CoralDb, CoralTx, DbError, MaterializationRecord, now_unix_nanos_i64};
use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::sources::catalog::validate_imported_manifest_database_persistence;
use crate::sources::materialization::{
    SourceDiagnosticReporter, load_v4_materialization_from_record, materialization_record_from_dir,
};
use crate::sources::model::{InstalledSource, SourceOrigin};
use crate::state::db::FeedbackReportRecord;
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::{WorkspaceName, WorkspaceRecord};
use coral_spec::parse_source_manifest_yaml;
use uuid::Uuid;

const WORKSPACE_CATALOG_CUTOVER_ID: &str = "workspace_catalog_cutover_v1";
const SOURCE_CATALOG_IMPORT_ID: &str = "source_catalog_import_v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspaceCatalogCutoverReport {
    pub(crate) workspace_count: usize,
    pub(crate) cutover_performed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceCatalogImportReport {
    source_count: usize,
    import_performed: bool,
}

pub(crate) async fn run_state_migrations(
    db: &CoralDb,
    config_store: &ConfigStore,
    layout: &AppStateLayout,
) -> Result<(), AppError> {
    cutover_legacy_workspace_catalog(db, config_store, layout).await?;
    let now_unix_nanos = now_unix_nanos_i64()?;
    import_config_source_catalog(db, config_store, layout, now_unix_nanos).await?;
    import_filesystem_source_manifests(db, layout, now_unix_nanos).await?;
    import_filesystem_v4_materializations(db, layout, now_unix_nanos).await?;
    remove_legacy_task_jsonl(config_store, layout)?;
    Ok(())
}

fn remove_legacy_task_jsonl(
    config_store: &ConfigStore,
    layout: &AppStateLayout,
) -> Result<(), AppError> {
    let _state_lock = config_store.state_lock_exclusive()?;
    layout.remove_legacy_task_event_logs()?;
    Ok(())
}

async fn cutover_legacy_workspace_catalog(
    db: &CoralDb,
    config_store: &ConfigStore,
    layout: &AppStateLayout,
) -> Result<WorkspaceCatalogCutoverReport, AppError> {
    cutover_legacy_workspace_catalog_at(db, config_store, layout, now_unix_nanos_i64()?).await
}

async fn cutover_legacy_workspace_catalog_at(
    db: &CoralDb,
    config_store: &ConfigStore,
    layout: &AppStateLayout,
    now_unix_nanos: i64,
) -> Result<WorkspaceCatalogCutoverReport, AppError> {
    let _state_lock = config_store.state_lock_exclusive()?;
    let mut tx = db.begin().await?;
    if !tx
        .state_migrations()
        .try_claim(WORKSPACE_CATALOG_CUTOVER_ID, now_unix_nanos)
        .await?
    {
        tx.rollback().await?;
        let mut session = db;
        return Ok(WorkspaceCatalogCutoverReport {
            workspace_count: session.workspaces().list().await?.len(),
            cutover_performed: false,
        });
    }

    let config = config_store.load_config_unlocked()?;
    let mut workspaces = config.legacy_workspace_records();
    if workspaces.is_empty() {
        workspaces = implicitly_provisioned_workspaces(layout)?;
    }
    let workspace_count = workspaces.len();

    tx.workspaces().delete_all().await?;
    import_legacy_workspaces(&mut tx, &workspaces, now_unix_nanos).await?;
    verify_workspace_parity(&mut tx, &workspaces).await?;
    tx.commit().await?;

    Ok(WorkspaceCatalogCutoverReport {
        workspace_count,
        cutover_performed: true,
    })
}

/// Lists the workspaces on-disk state proves an install had, for a legacy
/// config that names none itself.
///
/// Workspaces were once implicit: the catalog seeded one, so a `config.toml`
/// with no workspace tables still described an install that had a workspace,
/// with sources, tasks, and search state under its directory. Nothing records
/// that name any more except the directory itself, so the cutover reads it
/// from there. It does not fall back to a fixed `default`: a genuinely fresh
/// install has no workspace directory and must cut over to no workspaces.
///
/// Every entry here is read as a live workspace, and one class of them is
/// not: a deletion stages the workspace directory into its own root, outside
/// this one, but only after the deletion has committed, and staging that fails
/// only warns. A directory such a deletion left behind — or an older Coral
/// staged beside the live workspaces and failed to remove — carries no
/// evidence that it was deleted, so this scan resurrects it. That window is
/// deliberately open rather than closed: staging before the commit would shut
/// it, at the price of a crash between the rename and the commit leaving a
/// live workspace whose directory is already gone, which is worse than a
/// directory that outlives its workspace. Nothing on disk can tell the two
/// apart after the fact.
///
/// Deliberately a fallback and not a union with the config's own names, which
/// leaves one residual open. A config that named `analytics` and nothing else
/// could still have had a live implicit workspace beside it, and that one is
/// orphaned for good because the cutover marker never re-runs. Closing it by
/// unioning would resurrect exactly the orphans described above, and
/// `cuts_over_legacy_workspaces_into_database` pins that a leftover directory
/// must not come back beside a config that names workspaces. The exposed
/// population is narrow: every config Coral itself persisted serializes its
/// workspaces back, so only a hand-edited config reaches this shape.
fn implicitly_provisioned_workspaces(
    layout: &AppStateLayout,
) -> Result<Vec<WorkspaceRecord>, AppError> {
    let entries = match std::fs::read_dir(layout.workspaces_root()) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };

    let mut workspaces = Vec::new();
    for entry in entries {
        let entry = entry?;
        // Metadata, not `file_type`: a workspace directory may be a symlink to
        // another volume, and skipping it here would orphan it for good once
        // the cutover marker commits. Following the link means a dangling one
        // reports `NotFound`, which is this scan's answer for "not a workspace
        // directory" rather than a reason to fail startup; every other io error
        // still surfaces.
        match std::fs::metadata(entry.path()) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => continue,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        }
        let directory_name = entry.file_name();
        let Some(name) = directory_name
            .to_str()
            .and_then(|name| WorkspaceName::parse(name).ok())
        else {
            continue;
        };
        workspaces.push(WorkspaceRecord { name });
    }
    workspaces.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(workspaces)
}

async fn import_config_source_catalog(
    db: &CoralDb,
    config_store: &ConfigStore,
    layout: &AppStateLayout,
    now_unix_nanos: i64,
) -> Result<SourceCatalogImportReport, AppError> {
    let _state_lock = config_store.state_lock_exclusive()?;
    let mut tx = db.begin().await?;
    let migration_id = source_catalog_import_id(layout);
    if !tx
        .state_migrations()
        .try_claim(&migration_id, now_unix_nanos)
        .await?
    {
        tx.rollback().await?;
        return Ok(SourceCatalogImportReport {
            source_count: 0,
            import_performed: false,
        });
    }

    let config = config_store.load_config_unlocked()?;
    let source_entries = config
        .legacy_workspace_records()
        .into_iter()
        .flat_map(|workspace| {
            config
                .workspace_sources(&workspace.name)
                .into_iter()
                .map(move |source| (workspace.name.clone(), source))
        })
        .collect::<Vec<_>>();
    let source_count =
        import_config_sources(&mut tx, layout, &source_entries, now_unix_nanos).await?;
    tx.commit().await?;
    clear_legacy_source_catalog_config(config_store, source_entries.len());

    Ok(SourceCatalogImportReport {
        source_count,
        import_performed: true,
    })
}

fn source_catalog_import_id(layout: &AppStateLayout) -> String {
    let digest = Sha256::digest(layout.config_dir().as_os_str().as_encoded_bytes());
    format!("{SOURCE_CATALOG_IMPORT_ID}:{digest:x}")
}

async fn import_legacy_workspaces<S>(
    session: &mut S,
    workspaces: &[WorkspaceRecord],
    now_unix_nanos: i64,
) -> Result<(), AppError>
where
    S: DbRepos,
{
    for workspace in workspaces {
        session
            .workspaces()
            .ensure(workspace.name.as_str(), now_unix_nanos)
            .await?;
    }
    Ok(())
}

async fn import_config_sources(
    session: &mut CoralTx<'_>,
    layout: &AppStateLayout,
    entries: &[(WorkspaceName, InstalledSource)],
    now_unix_nanos: i64,
) -> Result<usize, AppError> {
    let mut source_count = 0;
    for (workspace_name, source) in entries {
        if session
            .workspaces()
            .get(workspace_name.as_str())
            .await?
            .is_none()
        {
            return Err(AppError::WorkspaceNotFound(workspace_name.to_string()));
        }
        if session
            .sources()
            .get_source(workspace_name, &source.name)
            .await?
            .is_some()
        {
            continue;
        }

        let manifest_yaml = match source.origin {
            SourceOrigin::Bundled => None,
            SourceOrigin::Imported => {
                read_optional_imported_manifest_file(layout, workspace_name, &source.name)?
            }
        };
        if let Some(manifest_yaml) = manifest_yaml.as_deref() {
            validate_imported_manifest_database_persistence(manifest_yaml, &source.variables)?;
        }

        session
            .sources()
            .upsert_source(workspace_name, source, now_unix_nanos)
            .await?;
        if let Some(manifest_yaml) = manifest_yaml {
            session
                .source_manifests()
                .upsert(workspace_name, &source.name, &manifest_yaml, now_unix_nanos)
                .await?;
        }
        let imported = session
            .sources()
            .get_source(workspace_name, &source.name)
            .await?;
        if imported.as_ref() != Some(source) {
            return Err(AppError::Database(format!(
                "source catalog import failed validation for {workspace_name}:{}",
                source.name
            )));
        }
        source_count += 1;
    }
    Ok(source_count)
}

async fn verify_workspace_parity<S>(
    session: &mut S,
    legacy_workspaces: &[WorkspaceRecord],
) -> Result<(), AppError>
where
    S: DbRepos,
{
    let expected = legacy_workspaces
        .iter()
        .map(|workspace| workspace.name.as_str().to_string())
        .collect::<BTreeSet<_>>();
    let actual = session
        .workspaces()
        .list()
        .await?
        .into_iter()
        .map(|workspace| workspace.id)
        .collect::<BTreeSet<_>>();
    if actual == expected {
        return Ok(());
    }
    Err(AppError::Database(format!(
        "workspace catalog cutover parity validation failed: legacy={expected:?} database={actual:?}"
    )))
}

async fn import_filesystem_source_manifests(
    db: &CoralDb,
    layout: &AppStateLayout,
    now_unix_nanos: i64,
) -> Result<(), AppError> {
    let mut session = db;
    let workspaces = session.workspaces().list().await?;
    for workspace in workspaces {
        let workspace_name = WorkspaceName::parse(&workspace.id)?;
        let sources = session
            .sources()
            .list_workspace_sources(&workspace_name)
            .await?;
        for source in sources
            .into_iter()
            .filter(|source| source.origin == SourceOrigin::Imported)
        {
            if session
                .source_manifests()
                .get(&workspace_name, &source.name)
                .await?
                .is_some()
            {
                continue;
            }

            let Some(manifest_yaml) = read_validated_manifest_for_backfill(
                layout,
                &workspace_name,
                &source.name,
                &source.variables,
            ) else {
                continue;
            };
            let mut tx = db.begin().await?;
            tx.source_manifests()
                .upsert(
                    &workspace_name,
                    &source.name,
                    &manifest_yaml,
                    now_unix_nanos,
                )
                .await?;
            tx.commit().await?;
        }
    }
    Ok(())
}

fn read_validated_manifest_for_backfill(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    source_variables: &BTreeMap<String, String>,
) -> Option<String> {
    let manifest_yaml = match read_optional_imported_manifest_file(
        layout,
        workspace_name,
        source_name,
    ) {
        Ok(Some(manifest_yaml)) => manifest_yaml,
        Ok(None) => return None,
        Err(error) => {
            tracing::warn!(
                workspace = %workspace_name,
                source = %source_name,
                detail = %error,
                "skipping imported source manifest database backfill because the legacy manifest could not be read"
            );
            return None;
        }
    };

    if let Err(error) =
        validate_imported_manifest_database_persistence(&manifest_yaml, source_variables)
    {
        tracing::warn!(
            workspace = %workspace_name,
            source = %source_name,
            detail = %error,
            "skipping imported source manifest database backfill because the legacy manifest is invalid"
        );
        return None;
    }

    Some(manifest_yaml)
}

async fn import_filesystem_v4_materializations(
    db: &CoralDb,
    layout: &AppStateLayout,
    now_unix_nanos: i64,
) -> Result<(), AppError> {
    // One-time backfill: artifacts are re-validated on the normal load path, so
    // this migration reports no per-source diagnostics of its own.
    let diagnostic_reporter = SourceDiagnosticReporter::default();
    let mut session = db;
    for workspace in session.workspaces().list().await? {
        let workspace_name = WorkspaceName::parse(&workspace.id)?;
        for source in session
            .sources()
            .list_workspace_sources(&workspace_name)
            .await?
            .into_iter()
            .filter(|source| source.origin == SourceOrigin::Imported)
        {
            let materialized_dir = layout.v4_materialized_dir(&workspace_name, &source.name);
            if !materialized_dir.exists() {
                continue;
            }
            if session
                .materializations()
                .get(&workspace_name, &source.name)
                .await?
                .is_some()
            {
                remove_v4_materialization_dir(&materialized_dir);
                continue;
            }
            let Some(manifest_yaml) = session
                .source_manifests()
                .get(&workspace_name, &source.name)
                .await?
                .map(|record| record.manifest_yaml)
            else {
                continue;
            };
            let Some(manifest) = v4_backfill_or_skip(
                parse_source_manifest_yaml(&manifest_yaml),
                &workspace_name,
                &source.name,
            ) else {
                continue;
            };
            let Some(v4) = manifest.as_v4() else {
                continue;
            };
            let Some(record) = v4_backfill_or_skip(
                materialization_record_from_dir(&source.name, &materialized_dir, now_unix_nanos),
                &workspace_name,
                &source.name,
            ) else {
                continue;
            };
            if v4_backfill_or_skip(
                load_v4_materialization_from_record(
                    layout,
                    &workspace_name,
                    &source.name,
                    &manifest_yaml,
                    v4,
                    &record,
                    &diagnostic_reporter,
                ),
                &workspace_name,
                &source.name,
            )
            .is_none()
            {
                continue;
            }
            upsert_imported_v4_materialization(db, &workspace_name, &source.name, &record).await?;
            remove_v4_materialization_dir(&materialized_dir);
        }
    }
    Ok(())
}

fn v4_backfill_or_skip<T, E: std::fmt::Display>(
    result: Result<T, E>,
    workspace_name: &WorkspaceName,
    source_name: &crate::sources::SourceName,
) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(error) => {
            tracing::warn!(
                workspace = %workspace_name,
                source = %source_name,
                detail = %error,
                "skipping legacy DSL v4 materialization database backfill; re-add the source to regenerate materialized artifacts"
            );
            None
        }
    }
}

async fn upsert_imported_v4_materialization(
    db: &CoralDb,
    workspace_name: &WorkspaceName,
    source_name: &crate::sources::SourceName,
    record: &MaterializationRecord,
) -> Result<(), AppError> {
    let mut tx = db.begin().await?;
    match tx
        .materializations()
        .upsert(workspace_name, source_name, record)
        .await
    {
        Ok(()) => tx.commit().await.map_err(AppError::from),
        Err(error) if is_unique_constraint_error(&error) => {
            tx.rollback().await?;
            let mut session = db;
            if session
                .materializations()
                .get(workspace_name, source_name)
                .await?
                .is_some()
            {
                Ok(())
            } else {
                Err(error.into())
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn is_unique_constraint_error(error: &DbError) -> bool {
    matches!(error, DbError::Sqlx(sqlx::Error::Database(database_error)) if database_error.is_unique_violation())
}

fn remove_v4_materialization_dir(materialized_dir: &std::path::Path) {
    match fs::remove_dir_all(materialized_dir) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(
                path = %materialized_dir.display(),
                detail = %error,
                "DSL v4 materialization imported into database but legacy artifact cleanup failed"
            );
        }
    }
}

#[derive(Debug, Deserialize)]
struct PersistedFeedbackReport {
    id: String,
    workspace: String,
    created_at: String,
    trying_to_do: String,
    tried: String,
    stuck: String,
    task_id: Option<String>,
}

pub(crate) async fn import_filesystem_feedback_reports(
    db: &CoralDb,
    layout: &AppStateLayout,
) -> Result<usize, AppError> {
    let mut imported = 0;
    for workspace_name in filesystem_feedback_workspaces(layout)? {
        let path = layout.feedback_reports_file(&workspace_name);
        if !path.exists() {
            continue;
        }
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(error) => {
                tracing::warn!(
                    path = %path.display(),
                    detail = %error,
                    "skipping legacy feedback JSONL import because the file could not be read"
                );
                continue;
            }
        };
        let mut records = Vec::new();
        let mut has_unimported_rows = false;
        for (index, line) in raw
            .lines()
            .enumerate()
            .filter(|(_index, line)| !line.trim().is_empty())
        {
            if let Some(record) =
                parse_legacy_feedback_report_line(&path, workspace_name.as_str(), index + 1, line)
            {
                records.push(record);
            } else {
                has_unimported_rows = true;
            }
        }
        for record in records {
            if insert_imported_feedback_report(db, &workspace_name, &record).await? {
                imported += 1;
            }
        }
        if has_unimported_rows {
            tracing::warn!(
                path = %path.display(),
                "legacy feedback JSONL retained because at least one row was not imported"
            );
        } else {
            remove_feedback_reports_file(&path);
        }
    }
    Ok(imported)
}

async fn insert_imported_feedback_report(
    db: &CoralDb,
    workspace_name: &WorkspaceName,
    record: &FeedbackReportRecord,
) -> Result<bool, AppError> {
    let mut tx = db.begin().await?;
    tx.workspaces()
        .ensure(workspace_name.as_str(), record.created_at_unix_nanos)
        .await?;
    match tx.feedback_reports().append(workspace_name, record).await {
        Ok(()) => {
            tx.commit().await?;
            Ok(true)
        }
        Err(error) if is_unique_constraint_error(&error) => {
            tx.rollback().await?;
            let mut session = db;
            if session
                .feedback_reports()
                .get(workspace_name, &record.id)
                .await?
                .is_some()
            {
                Ok(false)
            } else {
                Err(error.into())
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn parse_legacy_feedback_report_line(
    path: &std::path::Path,
    file_workspace: &str,
    line_number: usize,
    line: &str,
) -> Option<FeedbackReportRecord> {
    let record = match serde_json::from_str::<PersistedFeedbackReport>(line) {
        Ok(record) => record,
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                line = line_number,
                %error,
                "leaving invalid feedback report record in legacy JSONL"
            );
            return None;
        }
    };
    let workspace = match WorkspaceName::parse(&record.workspace) {
        Ok(workspace) => workspace,
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                line = line_number,
                %error,
                "leaving feedback report with invalid workspace in legacy JSONL"
            );
            return None;
        }
    };
    if workspace.as_str() != file_workspace {
        tracing::warn!(
            path = %path.display(),
            line = line_number,
            report_workspace = %workspace,
            file_workspace,
            "leaving feedback report stored under a different workspace in legacy JSONL"
        );
        return None;
    }
    let created_at = match DateTime::parse_from_rfc3339(&record.created_at) {
        Ok(created_at) => created_at.with_timezone(&Utc),
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                line = line_number,
                %error,
                "leaving feedback report with invalid timestamp in legacy JSONL"
            );
            return None;
        }
    };
    let Some(created_at_unix_nanos) = created_at.timestamp_nanos_opt() else {
        tracing::warn!(
            path = %path.display(),
            line = line_number,
            "leaving feedback report with out-of-range timestamp in legacy JSONL"
        );
        return None;
    };
    Some(FeedbackReportRecord {
        id: record.id,
        created_at_unix_nanos,
        trying_to_do: record.trying_to_do,
        tried: record.tried,
        stuck: record.stuck,
        task_id: record.task_id,
        publish_status: None,
        publish_error: None,
        published_at_unix_nanos: None,
    })
}

fn filesystem_feedback_workspaces(layout: &AppStateLayout) -> Result<Vec<WorkspaceName>, AppError> {
    let root = layout.workspaces_root();
    let mut workspaces = Vec::new();
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(workspaces),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if is_workspace_delete_rollback_dir(&name) {
            continue;
        }
        match WorkspaceName::parse(&name) {
            Ok(workspace) => workspaces.push(workspace),
            Err(error) => tracing::warn!(
                path = %entry.path().display(),
                detail = %error,
                "skipping legacy feedback import for invalid workspace directory"
            ),
        }
    }
    Ok(workspaces)
}

fn is_workspace_delete_rollback_dir(name: &str) -> bool {
    let Some((_workspace_name, rollback_id)) = name.split_once(".delete.rollback.") else {
        return false;
    };
    Uuid::parse_str(rollback_id).is_ok()
}

fn remove_feedback_reports_file(path: &std::path::Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                detail = %error,
                "feedback reports imported into database but legacy JSONL cleanup failed"
            );
        }
    }
}

fn read_imported_manifest_file(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
) -> Result<String, AppError> {
    let manifest_path = layout.manifest_file(workspace_name, source_name);
    fs::read_to_string(&manifest_path).map_err(|error| {
        if error.kind() == ErrorKind::NotFound {
            AppError::SourceNotFound(format!(
                "manifest for imported source '{workspace_name}:{source_name}' at {}",
                manifest_path.display()
            ))
        } else {
            AppError::Io(error)
        }
    })
}

fn read_optional_imported_manifest_file(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
) -> Result<Option<String>, AppError> {
    match read_imported_manifest_file(layout, workspace_name, source_name) {
        Ok(manifest_yaml) => Ok(Some(manifest_yaml)),
        Err(AppError::SourceNotFound(message)) => {
            tracing::warn!(
                detail = %message,
                "imported source manifest file is missing; source metadata will remain without a database manifest row"
            );
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn clear_legacy_source_catalog_config(config_store: &ConfigStore, source_count: usize) {
    if source_count != 0
        && let Err(error) = config_store.clear_source_catalog_unlocked()
    {
        tracing::warn!(
            detail = %error,
            "source catalog imported into database but legacy config cleanup failed"
        );
    }
}
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;

    use tempfile::tempdir;

    use super::{
        SourceCatalogImportReport, WORKSPACE_CATALOG_CUTOVER_ID, WorkspaceCatalogCutoverReport,
        cutover_legacy_workspace_catalog, cutover_legacy_workspace_catalog_at,
        import_config_source_catalog, import_filesystem_feedback_reports, run_state_migrations,
        source_catalog_import_id,
    };
    use crate::credentials::CredentialStorageKind;
    use crate::sources::SourceName;
    use crate::sources::materialization::{
        MaterializationInputs, build_v4_materialization_tmp, replace_or_retire_v4_materialization,
    };
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::db::session::DbRepos;
    use crate::state::db::{
        CoralDb, DatabaseConfig, FeedbackReportRecord, MaterializationRecord,
        MaterializationSurfaceRecord, ResolvedDatabaseConfig,
    };
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::storage::fs::DELETION_BACKUP_INFIX;
    use crate::workspaces::WorkspaceName;
    use coral_spec::parse_source_manifest_yaml;

    const OPENAPI_FIXTURE: &str = r#"{"openapi":"3.0.3","servers":[{"url":"https://api.example.com"}],"paths":{"/issues":{"get":{"operationId":"issues/list","responses":{"200":{"content":{"application/json":{"schema":{"type":"array","items":{"type":"object","properties":{"id":{"type":"integer"},"title":{"type":"string"}}}}}}}}}}}}"#;

    /// The unique suffix a staged deletion carries, fixed so the directories
    /// these tests plant read exactly as `move_for_delete` would have written
    /// them.
    const STAGED_DELETION_SUFFIX: &str = "7f1c5a4e-1d29-4f3a-9f2b-2c6d0f9a1b34";

    #[tokio::test]
    async fn completed_workspace_cutover_does_not_skip_source_catalog_import() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = source(
            "github",
            Some("1.2.3"),
            [("GITHUB_API_BASE", "https://api.github.com")],
            ["GITHUB_TOKEN"],
            Some(CredentialStorageKind::Keychain),
            SourceOrigin::Imported,
        );
        config_store
            .create_legacy_workspace_entry_for_tests(&workspace)
            .expect("create legacy workspace entry");
        config_store
            .upsert_source(&workspace, source.clone())
            .expect("write config source");
        let manifest_yaml = imported_manifest_yaml("github", "1.2.3");
        write_manifest_file(&layout, &workspace, &source.name, &manifest_yaml);
        let db = open_sqlite(&layout).await;

        cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("get source before source import"),
            None
        );

        run_state_migrations(&db, &config_store, &layout)
            .await
            .expect("run state migrations after workspace cutover");

        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("get source"),
            Some(source.clone())
        );
        assert_eq!(
            session
                .source_manifests()
                .get(&workspace, &source.name)
                .await
                .expect("get source manifest")
                .expect("source manifest")
                .manifest_yaml,
            manifest_yaml
        );
        assert!(
            session
                .state_migrations()
                .has_completed(WORKSPACE_CATALOG_CUTOVER_ID)
                .await
                .expect("read workspace cutover marker")
        );
        assert!(
            session
                .state_migrations()
                .has_completed(&source_catalog_import_id(&layout))
                .await
                .expect("read source import marker")
        );
        assert!(matches!(
            config_store.get_source(&workspace, &source.name),
            Err(crate::bootstrap::AppError::SourceNotFound(_))
        ));
        assert!(
            layout.manifest_file(&workspace, &source.name).exists(),
            "legacy manifest file should be preserved for rollback compatibility"
        );
    }

    #[tokio::test]
    async fn invalid_imported_config_manifest_rolls_back_catalog_import() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = unsafe_secret_endpoint_source();
        config_store
            .upsert_source(&workspace, source.clone())
            .expect("write config source");
        let manifest_yaml = unsafe_secret_endpoint_manifest_yaml("github", "1.2.3");
        write_manifest_file(&layout, &workspace, &source.name, &manifest_yaml);
        let db = open_sqlite(&layout).await;
        cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 10)
            .await
            .expect("cut over legacy workspace catalog");

        let error = import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect_err("unsafe legacy manifest should fail active config import");
        let crate::bootstrap::AppError::InvalidInput(message) = error else {
            panic!("expected invalid input error, got {error:?}");
        };
        assert!(message.contains("base_url must use https"));
        let mut session = &db;
        assert!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("get source after rollback")
                .is_none()
        );
        assert!(
            session
                .state_migrations()
                .has_completed(WORKSPACE_CATALOG_CUTOVER_ID)
                .await
                .expect("workspace cutover marker should remain complete")
        );
        assert!(
            !session
                .state_migrations()
                .has_completed(&source_catalog_import_id(&layout))
                .await
                .expect("source import marker should not be inserted")
        );
    }

    #[tokio::test]
    async fn cuts_over_legacy_workspaces_into_database() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let analytics_workspace = WorkspaceName::parse("analytics").expect("workspace");
        config_store
            .create_legacy_workspace_entry_for_tests(&analytics_workspace)
            .expect("create legacy workspace entry");
        // A config that names workspaces is authoritative, so a directory left
        // behind by a deleted workspace must not come back beside them.
        let deleted_workspace = WorkspaceName::parse("removed").expect("workspace");
        std::fs::create_dir_all(layout.workspace_dir(&deleted_workspace))
            .expect("create leftover workspace dir");
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 1,
                cutover_performed: true,
            }
        );
        assert_eq!(
            workspace_ids(&db).await,
            vec!["analytics".to_string()],
            "the cutover carries the legacy names across and invents none"
        );
        let mut session = &db;
        assert!(
            session
                .state_migrations()
                .has_completed(WORKSPACE_CATALOG_CUTOVER_ID)
                .await
                .expect("read cutover marker")
        );
    }

    #[tokio::test]
    async fn source_import_preserves_existing_database_source() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("source-only").expect("workspace");
        let existing = source("github", None, [], [], None, SourceOrigin::Bundled);
        let replacement = source(
            "github",
            Some("1.2.3"),
            [("OWNER", "coral")],
            [],
            Some(CredentialStorageKind::File),
            SourceOrigin::Imported,
        );
        config_store
            .create_legacy_workspace_entry_for_tests(&workspace)
            .expect("create legacy workspace entry");
        config_store
            .upsert_source(&workspace, replacement)
            .expect("write config source");
        let db = open_sqlite(&layout).await;
        cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 10)
            .await
            .expect("cut over legacy workspace catalog");
        {
            let mut tx = db.begin().await.expect("begin seed tx");
            tx.workspaces()
                .ensure(workspace.as_str(), 7)
                .await
                .expect("seed workspace");
            tx.sources()
                .upsert_source(&workspace, &existing, 7)
                .await
                .expect("seed db source");
            tx.commit().await.expect("commit seed tx");
        }

        let report = import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect("import config source catalog");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                source_count: 0,
                import_performed: true,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &existing.name)
                .await
                .expect("get preserved source"),
            Some(existing.clone())
        );
        assert!(matches!(
            config_store.get_source(&workspace, &existing.name),
            Err(crate::bootstrap::AppError::SourceNotFound(_))
        ));
    }

    #[tokio::test]
    async fn source_import_adds_missing_sources_and_preserves_database_workspaces() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let default_workspace = WorkspaceName::parse("default").expect("workspace");
        let other_workspace = WorkspaceName::parse("other").expect("workspace");
        let database_only_workspace = WorkspaceName::parse("database-only").expect("workspace");
        let existing_source = source("github", None, [], [], None, SourceOrigin::Bundled);
        let config_source = source("slack", None, [], [], None, SourceOrigin::Bundled);
        config_store
            .create_legacy_workspace_entry_for_tests(&other_workspace)
            .expect("create config workspace");
        config_store
            .upsert_source(&other_workspace, config_source.clone())
            .expect("write config source");
        let db = open_sqlite(&layout).await;
        cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 5)
            .await
            .expect("cut over legacy workspace catalog");
        {
            let mut tx = db.begin().await.expect("begin seed tx");
            tx.workspaces()
                .ensure(default_workspace.as_str(), 7)
                .await
                .expect("seed workspace");
            tx.workspaces()
                .ensure(database_only_workspace.as_str(), 7)
                .await
                .expect("seed database-only workspace");
            tx.sources()
                .upsert_source(&default_workspace, &existing_source, 7)
                .await
                .expect("seed db source");
            tx.commit().await.expect("commit seed tx");
        }

        let report = import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect("import config source catalog");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                source_count: 1,
                import_performed: true,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&default_workspace, &existing_source.name)
                .await
                .expect("get existing source"),
            Some(existing_source)
        );
        assert_eq!(
            session
                .sources()
                .get_source(&other_workspace, &config_source.name)
                .await
                .expect("get imported source"),
            Some(config_source)
        );
        assert!(
            session
                .workspaces()
                .get(database_only_workspace.as_str())
                .await
                .expect("get database-only workspace")
                .is_some()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "The cleanup contract keeps its committed database and retained filesystem assertions in one fixture."
    )]
    async fn cleanup_failure_does_not_fail_committed_source_import() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let db_path = temp.path().join("db").join("coral.db");
        fs::create_dir_all(db_path.parent().expect("db parent")).expect("create db dir");
        fs::write(
            layout.config_file(),
            format!(
                "[database]\nbackend = \"sqlite\"\npath = \"{}\"\n",
                db_path.display()
            ),
        )
        .expect("write database config");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = source("github", None, [], [], None, SourceOrigin::Bundled);
        config_store
            .upsert_source(&workspace, source.clone())
            .expect("write config source");
        drop(
            config_store
                .state_lock_exclusive()
                .expect("create state lock before read-only config dir"),
        );
        let db = open_sqlite(&layout).await;
        cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 10)
            .await
            .expect("cut over legacy workspace catalog");

        let original_mode = fs::metadata(layout.config_dir())
            .expect("config dir metadata")
            .permissions()
            .mode();
        fs::set_permissions(layout.config_dir(), fs::Permissions::from_mode(0o500))
            .expect("make config dir read-only");
        let report = import_config_source_catalog(&db, &config_store, &layout, 11).await;
        fs::set_permissions(
            layout.config_dir(),
            fs::Permissions::from_mode(original_mode),
        )
        .expect("restore config dir permissions");

        assert_eq!(
            report.expect("cleanup failure should not fail committed source import"),
            SourceCatalogImportReport {
                source_count: 1,
                import_performed: true,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("get imported source"),
            Some(source.clone())
        );
        assert!(
            session
                .state_migrations()
                .has_completed(&source_catalog_import_id(&layout))
                .await
                .expect("read source import marker")
        );
        assert_eq!(
            config_store
                .get_source(&workspace, &source.name)
                .expect("legacy config source should remain after cleanup failure"),
            source.clone()
        );

        let mut stale_config_source = source.clone();
        stale_config_source
            .variables
            .insert("OWNER".to_string(), "coral".to_string());
        config_store
            .upsert_source(&workspace, stale_config_source)
            .expect("update stale config source");
        {
            let mut tx = db.begin().await.expect("begin delete tx");
            tx.sources()
                .remove_source(&workspace, &source.name)
                .await
                .expect("delete db source");
            tx.commit().await.expect("commit delete tx");
        }

        let report = import_config_source_catalog(&db, &config_store, &layout, 99)
            .await
            .expect("completed source import should not reimport stale config");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                source_count: 0,
                import_performed: false,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("source should remain deleted"),
            None
        );
    }

    #[tokio::test]
    async fn shared_database_imports_each_local_source_catalog_once() {
        let temp = tempdir().expect("temp dir");
        let first_layout =
            AppStateLayout::discover(Some(temp.path().join("first"))).expect("first layout");
        let second_layout =
            AppStateLayout::discover(Some(temp.path().join("second"))).expect("second layout");
        first_layout.ensure().expect("ensure first layout");
        second_layout.ensure().expect("ensure second layout");
        let first_store = ConfigStore::new(first_layout.clone());
        let second_store = ConfigStore::new(second_layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let first_source = source("github", None, [], [], None, SourceOrigin::Bundled);
        let second_source = source("slack", None, [], [], None, SourceOrigin::Bundled);
        for (store, source) in [
            (&first_store, first_source.clone()),
            (&second_store, second_source.clone()),
        ] {
            store
                .create_legacy_workspace_entry_for_tests(&workspace)
                .expect("create legacy workspace entry");
            store
                .upsert_source(&workspace, source)
                .expect("write config source");
        }
        let db = open_sqlite(&first_layout).await;
        cutover_legacy_workspace_catalog_at(&db, &first_store, &first_layout, 10)
            .await
            .expect("cut over shared workspace catalog");

        let first_report = import_config_source_catalog(&db, &first_store, &first_layout, 11)
            .await
            .expect("import first local source catalog");
        let second_report = import_config_source_catalog(&db, &second_store, &second_layout, 12)
            .await
            .expect("import second local source catalog");

        assert_eq!(first_report.source_count, 1);
        assert_eq!(second_report.source_count, 1);
        assert_ne!(
            source_catalog_import_id(&first_layout),
            source_catalog_import_id(&second_layout)
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &first_source.name)
                .await
                .expect("get first source"),
            Some(first_source)
        );
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &second_source.name)
                .await
                .expect("get second source"),
            Some(second_source)
        );
        assert!(
            session
                .state_migrations()
                .has_completed(&source_catalog_import_id(&first_layout))
                .await
                .expect("read first import marker")
        );
        assert!(
            session
                .state_migrations()
                .has_completed(&source_catalog_import_id(&second_layout))
                .await
                .expect("read second import marker")
        );
    }

    #[tokio::test]
    async fn source_import_rejects_missing_workspace_without_marking_complete() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("missing").expect("workspace");
        let source = source("github", None, [], [], None, SourceOrigin::Bundled);
        config_store
            .upsert_source(&workspace, source.clone())
            .expect("write config source");
        let db = open_sqlite(&layout).await;
        cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 5)
            .await
            .expect("cut over legacy workspace catalog");
        let mut session = &db;
        session
            .workspaces()
            .delete(workspace.as_str())
            .await
            .expect("delete imported workspace");

        let error = import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect_err("missing workspace should reject source import");

        assert!(matches!(
            error,
            crate::bootstrap::AppError::WorkspaceNotFound(ref actual)
                if actual == workspace.as_str()
        ));
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("get rejected source"),
            None
        );
        assert!(
            !session
                .state_migrations()
                .has_completed(&source_catalog_import_id(&layout))
                .await
                .expect("read source import marker")
        );
    }

    #[tokio::test]
    async fn cutover_resets_stale_shadow_database_rows() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let analytics_workspace = WorkspaceName::parse("analytics").expect("workspace");
        config_store
            .create_legacy_workspace_entry_for_tests(&analytics_workspace)
            .expect("create legacy workspace entry");
        let db = open_sqlite(&layout).await;
        let mut tx = db.begin().await.expect("begin stale seed tx");
        tx.workspaces()
            .ensure("stale", 7)
            .await
            .expect("seed stale workspace");
        tx.commit().await.expect("commit stale seed tx");

        cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        let mut session = &db;
        assert_eq!(
            session
                .workspaces()
                .list()
                .await
                .expect("list workspaces")
                .into_iter()
                .map(|workspace| workspace.id)
                .collect::<Vec<_>>(),
            vec!["analytics".to_string()]
        );
    }

    #[tokio::test]
    async fn completed_cutover_does_not_reimport_legacy_config() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let db = open_sqlite(&layout).await;

        cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("initial cutover");
        std::fs::write(layout.config_file(), "[[workspaces]\n").expect("corrupt config");

        let report = cutover_legacy_workspace_catalog(&db, &config_store, &layout)
            .await
            .expect("marker should skip legacy config reload");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 0,
                cutover_performed: false,
            }
        );
    }

    /// A fresh install names no workspace and holds none on disk, so the
    /// cutover must not seed one on its way into the database.
    #[tokio::test]
    async fn cutover_without_legacy_workspaces_creates_none() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        // An install that has held a workspace and lost it looks the same as
        // one that never had one: an emptied root, and at most a directory a
        // deletion staged aside — into its own root — and failed to remove.
        std::fs::create_dir_all(layout.workspaces_root()).expect("create workspaces root");
        std::fs::create_dir_all(layout.deleted_workspaces_root().join(format!(
            "default{DELETION_BACKUP_INFIX}{STAGED_DELETION_SUFFIX}"
        )))
        .expect("stage a deletion that was never removed");
        let config_store = ConfigStore::new(layout.clone());
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 0,
                cutover_performed: true,
            }
        );
        let mut session = &db;
        assert!(
            session
                .workspaces()
                .list()
                .await
                .expect("list workspaces")
                .is_empty()
        );
    }

    /// Workspaces were once implicit, so an older install can hold one whose
    /// name only its directory records. The cutover happens once and marks
    /// itself done, so a workspace it drops is orphaned for good: its name and
    /// its contents have to come across.
    #[tokio::test]
    async fn cutover_preserves_an_implicitly_provisioned_legacy_workspace() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let implicit_workspace = WorkspaceName::parse("default").expect("workspace");
        let installed_source = layout
            .sources_root(&implicit_workspace)
            .join("github")
            .join("manifest.yaml");
        std::fs::create_dir_all(installed_source.parent().expect("source dir"))
            .expect("create legacy source dir");
        std::fs::write(&installed_source, "name: github").expect("write legacy manifest");
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 1,
                cutover_performed: true,
            }
        );
        assert_eq!(workspace_ids(&db).await, vec!["default".to_string()]);
        assert!(
            installed_source.exists(),
            "the preserved workspace keeps its contents"
        );
    }

    /// A workspace directory can be a symlink — relocated to another volume by
    /// hand, say. The cutover runs once, so reading only the link itself and
    /// not what it points at would orphan that workspace for good.
    #[cfg(unix)]
    #[tokio::test]
    async fn cutover_preserves_a_symlinked_legacy_workspace() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let implicit_workspace = WorkspaceName::parse("default").expect("workspace");
        let relocated = temp.path().join("other-volume").join("default");
        std::fs::create_dir_all(&relocated).expect("create relocated workspace dir");
        std::fs::create_dir_all(layout.workspaces_root()).expect("create workspaces root");
        std::os::unix::fs::symlink(&relocated, layout.workspace_dir(&implicit_workspace))
            .expect("link the relocated workspace into place");
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 1,
                cutover_performed: true,
            }
        );
        assert_eq!(workspace_ids(&db).await, vec!["default".to_string()]);
    }

    /// Following the link is what carries a relocated workspace across, and a
    /// link whose target is gone is the cost of that. It names no workspace, so
    /// the cutover skips it — refusing to start over a broken link would strand
    /// the whole install.
    #[cfg(unix)]
    #[tokio::test]
    async fn cutover_skips_a_dangling_workspace_symlink() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let live_workspace = WorkspaceName::parse("analytics").expect("workspace");
        let dangling_workspace = WorkspaceName::parse("relocated").expect("workspace");
        std::fs::create_dir_all(layout.workspace_dir(&live_workspace))
            .expect("create the live workspace dir");
        std::os::unix::fs::symlink(
            temp.path().join("other-volume").join("relocated"),
            layout.workspace_dir(&dangling_workspace),
        )
        .expect("link a workspace that is no longer there");
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 1,
                cutover_performed: true,
            }
        );
        assert_eq!(workspace_ids(&db).await, vec!["analytics".to_string()]);
    }

    /// A workspace name is free to look exactly like a staged deletion, so a
    /// name can never say which of the two a directory is. Location can: the
    /// live workspace stays in the workspaces root and the staged deletion
    /// sits in its own, and only the former is imported.
    #[tokio::test]
    async fn cutover_separates_a_staged_deletion_from_a_workspace_named_like_one() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let named_like_a_backup =
            format!("analytics{DELETION_BACKUP_INFIX}{STAGED_DELETION_SUFFIX}");
        let implicit_workspace = WorkspaceName::parse(&named_like_a_backup).expect("workspace");
        std::fs::create_dir_all(layout.workspace_dir(&implicit_workspace))
            .expect("create the legacy workspace dir");
        std::fs::create_dir_all(layout.deleted_workspaces_root().join(format!(
            "work{DELETION_BACKUP_INFIX}{STAGED_DELETION_SUFFIX}"
        )))
        .expect("stage a deletion outside the workspaces root");
        let db = open_sqlite(&layout).await;

        cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(workspace_ids(&db).await, vec![named_like_a_backup]);
    }

    async fn workspace_ids(db: &CoralDb) -> Vec<String> {
        let mut session = db;
        session
            .workspaces()
            .list()
            .await
            .expect("list workspaces")
            .into_iter()
            .map(|workspace| workspace.id)
            .collect()
    }

    #[tokio::test]
    async fn shared_database_does_not_scope_task_cleanup_to_the_first_layout() {
        let temp = tempdir().expect("temp dir");
        let first_layout =
            AppStateLayout::discover(Some(temp.path().join("first"))).expect("first layout");
        let second_layout =
            AppStateLayout::discover(Some(temp.path().join("second"))).expect("second layout");
        first_layout.ensure().expect("ensure first layout");
        second_layout.ensure().expect("ensure second layout");
        let first_config_store = ConfigStore::new(first_layout.clone());
        let second_config_store = ConfigStore::new(second_layout.clone());
        let legacy_workspace = WorkspaceName::parse("analytics").expect("workspace");
        let first_legacy_file = first_layout
            .workspace_dir(&legacy_workspace)
            .join("tasks")
            .join("tasks.jsonl");
        let second_legacy_file = second_layout
            .workspace_dir(&legacy_workspace)
            .join("tasks")
            .join("tasks.jsonl");
        for path in [&first_legacy_file, &second_legacy_file] {
            std::fs::create_dir_all(path.parent().expect("legacy task dir"))
                .expect("create legacy task dir");
            std::fs::write(path, "sensitive task intent").expect("write legacy task file");
        }
        let db = open_sqlite(&first_layout).await;

        run_state_migrations(&db, &first_config_store, &first_layout)
            .await
            .expect("run migrations for first layout");
        run_state_migrations(&db, &second_config_store, &second_layout)
            .await
            .expect("run migrations for second layout");

        assert!(!first_legacy_file.exists());
        assert!(!second_legacy_file.exists());
    }

    #[tokio::test]
    async fn completed_source_import_does_not_reload_config() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let db = open_sqlite(&layout).await;

        import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect("initial source import");
        std::fs::write(layout.config_file(), "[[workspaces]\n").expect("corrupt config");

        let report = import_config_source_catalog(&db, &config_store, &layout, 12)
            .await
            .expect("source marker should skip config reload");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                source_count: 0,
                import_performed: false,
            }
        );
    }

    #[tokio::test]
    async fn imported_config_source_without_manifest_file_keeps_source_without_manifest_row() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = source("github", None, [], [], None, SourceOrigin::Imported);
        config_store
            .upsert_source(&workspace, source.clone())
            .expect("write config source");
        let db = open_sqlite(&layout).await;
        cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 10)
            .await
            .expect("cut over legacy workspace catalog");

        let report = import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect("missing imported manifest should not block source catalog import");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                source_count: 1,
                import_performed: true,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("get source"),
            Some(source.clone())
        );
        assert_eq!(
            session
                .source_manifests()
                .get(&workspace, &source.name)
                .await
                .expect("get missing source manifest"),
            None
        );
        assert!(matches!(
            config_store.get_source(&workspace, &source.name),
            Err(crate::bootstrap::AppError::SourceNotFound(_))
        ));

        run_state_migrations(&db, &config_store, &layout)
            .await
            .expect("missing imported manifest should not block later backfill attempts");
        let mut session = &db;
        assert_eq!(
            session
                .source_manifests()
                .get(&workspace, &source.name)
                .await
                .expect("get still-missing source manifest"),
            None
        );
    }

    #[tokio::test]
    async fn backfills_manifest_for_already_imported_database_source() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = source(
            "github",
            Some("1.2.3"),
            [],
            [],
            None,
            SourceOrigin::Imported,
        );
        let manifest_yaml = imported_manifest_yaml("github", "1.2.3");
        write_manifest_file(&layout, &workspace, &source.name, &manifest_yaml);
        let db = open_sqlite(&layout).await;
        let mut tx = db.begin().await.expect("begin tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 7)
            .await
            .expect("ensure workspace");
        tx.sources()
            .upsert_source(&workspace, &source, 7)
            .await
            .expect("write source without manifest row");
        assert!(
            tx.state_migrations()
                .try_claim(WORKSPACE_CATALOG_CUTOVER_ID, 7)
                .await
                .expect("mark workspace cutover complete")
        );
        assert!(
            tx.state_migrations()
                .try_claim(&source_catalog_import_id(&layout), 7)
                .await
                .expect("mark source import complete")
        );
        tx.commit().await.expect("commit source");

        run_state_migrations(&db, &config_store, &layout)
            .await
            .expect("backfill manifest");

        let mut session = &db;
        assert_eq!(
            session
                .source_manifests()
                .get(&workspace, &source.name)
                .await
                .expect("get source manifest")
                .expect("source manifest")
                .manifest_yaml,
            manifest_yaml
        );
        assert!(
            layout.manifest_file(&workspace, &source.name).exists(),
            "legacy manifest file should be preserved after DB backfill"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "The backfill contract keeps its filesystem and database assertions in one fixture."
    )]
    async fn invalid_filesystem_manifest_backfill_skips_source_manifest() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let unsafe_source = unsafe_secret_endpoint_source();
        let manifest_yaml = unsafe_secret_endpoint_manifest_yaml("github", "1.2.3");
        write_manifest_file(&layout, &workspace, &unsafe_source.name, &manifest_yaml);
        let healthy = source("healthy_v4", None, [], [], None, SourceOrigin::Imported);
        let corrupt = source("corrupt_v4", None, [], [], None, SourceOrigin::Imported);
        let healthy_descriptor = temp.path().join("healthy-openapi.json");
        fs::write(&healthy_descriptor, OPENAPI_FIXTURE).expect("write OpenAPI fixture");
        let healthy_manifest = format!(
            "name: {}\ndsl_version: 4\nsurface:\n  type: openapi\n  file: {}\n",
            healthy.name,
            healthy_descriptor.display()
        );
        let parsed_manifest = parse_source_manifest_yaml(&healthy_manifest)
            .expect("parse manifest")
            .as_v4()
            .expect("v4 manifest")
            .clone();
        let build = build_v4_materialization_tmp(
            &layout,
            &workspace,
            &healthy.name,
            &healthy_manifest,
            &parsed_manifest,
            &MaterializationInputs::default(),
            "test",
        );
        replace_or_retire_v4_materialization(
            &layout,
            &workspace,
            &healthy.name,
            Some(&build.expect("build materialization").temp_dir),
        )
        .expect("install legacy materialization");
        let corrupt_manifest = healthy_manifest.replace("healthy_v4", "corrupt_v4");
        fs::create_dir_all(layout.v4_materialized_dir(&workspace, &corrupt.name))
            .expect("create corrupt materialization dir");
        fs::write(
            layout.v4_fingerprint_file(&workspace, &corrupt.name),
            "not: [yaml",
        )
        .expect("corrupt fingerprint");
        let db = open_sqlite(&layout).await;
        let mut tx = db.begin().await.expect("begin tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 7)
            .await
            .expect("ensure workspace");
        tx.sources()
            .upsert_source(&workspace, &unsafe_source, 7)
            .await
            .expect("write source without manifest row");
        for (source, manifest) in [(&healthy, &healthy_manifest), (&corrupt, &corrupt_manifest)] {
            tx.sources()
                .upsert_source(&workspace, source, 7)
                .await
                .expect("upsert source");
            tx.source_manifests()
                .upsert(&workspace, &source.name, manifest, 7)
                .await
                .expect("upsert manifest");
        }
        assert!(
            tx.state_migrations()
                .try_claim(WORKSPACE_CATALOG_CUTOVER_ID, 7)
                .await
                .expect("mark workspace cutover complete")
        );
        assert!(
            tx.state_migrations()
                .try_claim(&source_catalog_import_id(&layout), 7)
                .await
                .expect("mark source import complete")
        );
        tx.commit().await.expect("commit sources");

        run_state_migrations(&db, &config_store, &layout)
            .await
            .expect("invalid backfills should not fail startup cutover");

        let mut session = &db;
        assert!(
            session
                .source_manifests()
                .get(&workspace, &unsafe_source.name)
                .await
                .expect("get skipped source manifest")
                .is_none()
        );
        let mut materializations = session.materializations();
        assert!(
            materializations
                .get(&workspace, &healthy.name)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            materializations
                .get(&workspace, &corrupt.name)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn backfills_v4_materialization_for_already_imported_database_source() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = source(
            "github_v4_import",
            Some("0.1.0"),
            [],
            [],
            None,
            SourceOrigin::Imported,
        );
        let (manifest_yaml, materialized_dir) = install_legacy_v4_materialization(
            &layout,
            &workspace,
            &source.name,
            &temp.path().join("openapi.yaml"),
        );

        let db = open_sqlite(&layout).await;
        let mut tx = db.begin().await.expect("begin tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 7)
            .await
            .expect("ensure workspace");
        tx.sources()
            .upsert_source(&workspace, &source, 7)
            .await
            .expect("upsert source");
        tx.source_manifests()
            .upsert(&workspace, &source.name, &manifest_yaml, 7)
            .await
            .expect("upsert source manifest");
        assert!(
            tx.state_migrations()
                .try_claim(WORKSPACE_CATALOG_CUTOVER_ID, 7)
                .await
                .expect("mark workspace cutover complete")
        );
        assert!(
            tx.state_migrations()
                .try_claim(&source_catalog_import_id(&layout), 7)
                .await
                .expect("mark source import complete")
        );
        tx.commit().await.expect("commit source");

        run_state_migrations(&db, &config_store, &layout)
            .await
            .expect("backfill materialization");

        let mut session = &db;
        let materialization = session
            .materializations()
            .get(&workspace, &source.name)
            .await
            .expect("get materialization")
            .expect("materialization");
        assert_eq!(materialization.surfaces.len(), 1);
        assert!(
            !materialized_dir.exists(),
            "legacy materialized directory should be cleaned after DB backfill"
        );
    }

    #[tokio::test]
    async fn imports_config_v4_source_and_legacy_materialization_in_one_pass() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = source(
            "github_v4_config",
            Some("0.1.0"),
            [],
            [],
            None,
            SourceOrigin::Imported,
        );
        config_store
            .upsert_source(&workspace, source.clone())
            .expect("write config source");
        let (manifest_yaml, materialized_dir) = install_legacy_v4_materialization(
            &layout,
            &workspace,
            &source.name,
            &temp.path().join("openapi.yaml"),
        );
        write_manifest_file(&layout, &workspace, &source.name, &manifest_yaml);
        let db = open_sqlite(&layout).await;

        run_state_migrations(&db, &config_store, &layout)
            .await
            .expect("import source and materialization");

        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("get imported source"),
            Some(source.clone())
        );
        assert_eq!(
            session
                .source_manifests()
                .get(&workspace, &source.name)
                .await
                .expect("get source manifest")
                .expect("source manifest")
                .manifest_yaml,
            manifest_yaml
        );
        assert_eq!(
            session
                .materializations()
                .get(&workspace, &source.name)
                .await
                .expect("get materialization")
                .expect("materialization")
                .surfaces
                .len(),
            1
        );
        assert!(!materialized_dir.exists());
        assert!(
            config_store
                .load_config_unlocked()
                .expect("legacy config should be cleaned")
                .workspace_sources(&workspace)
                .is_empty()
        );
    }

    #[tokio::test]
    async fn concurrent_v4_materialization_backfill_race_is_benign_postgres() {
        let Some(url) = postgres_test_url() else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");
        let workspace = WorkspaceName::parse(&format!("race{}", uuid::Uuid::new_v4().simple()))
            .expect("workspace");
        let source = source("race_v4", None, [], [], None, SourceOrigin::Imported);
        let mut tx = db.begin().await.expect("begin tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 7)
            .await
            .expect("ensure workspace");
        tx.sources()
            .upsert_source(&workspace, &source, 7)
            .await
            .expect("upsert source");
        tx.commit().await.expect("commit source");
        let winner_record = MaterializationRecord {
            materialization_version: "v4".into(),
            fingerprint_yaml: "winner-fingerprint".into(),
            projections_yaml: "projections".into(),
            diagnostics_yaml: "diagnostics".into(),
            created_at_unix_nanos: 11,
            surfaces: vec![MaterializationSurfaceRecord {
                surface_id: "rest".into(),
                source_document_raw: b"{}".to_vec(),
                source_document_yaml: "{}".into(),
                semantic_ir_yaml: "{}".into(),
                operation_metadata_yaml: "{}".into(),
            }],
        };
        let mut loser_record = winner_record.clone();
        loser_record.fingerprint_yaml = "loser-fingerprint".into();
        let mut tx = db.begin().await.expect("begin winner tx");
        tx.materializations()
            .upsert(&workspace, &source.name, &winner_record)
            .await
            .expect("insert uncommitted winner materialization");
        let loser =
            super::upsert_imported_v4_materialization(&db, &workspace, &source.name, &loser_record);
        tokio::pin!(loser);

        if tokio::time::timeout(std::time::Duration::from_millis(250), &mut loser)
            .await
            .is_ok()
        {
            panic!("loser backfill completed before the winner transaction committed");
        }
        tx.commit().await.expect("commit winner materialization");
        loser
            .await
            .expect("loser backfill recovers after unique violation");
        let mut session = &db;
        assert_eq!(
            session
                .materializations()
                .get(&workspace, &source.name)
                .await
                .expect("get materialization")
                .expect("materialization"),
            winner_record
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn feedback_import_skips_bad_legacy_state_and_imports_healthy_rows() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let default = WorkspaceName::parse("default").expect("workspace");
        let healthy = WorkspaceName::parse("healthy").expect("workspace");
        let corrupt = WorkspaceName::parse("corrupt").expect("workspace");
        let retained_file = layout.feedback_reports_file(&default);
        let healthy_file = layout.feedback_reports_file(&healthy);
        write_feedback_reports_file(
            &retained_file,
            &format!("{}not json\n", feedback_jsonl("default", "feedback-1")),
        );
        write_feedback_reports_file(&healthy_file, &feedback_jsonl("healthy", "feedback-1"));
        let corrupt_file = layout.feedback_reports_file(&corrupt);
        fs::create_dir_all(corrupt_file.parent().expect("reports parent"))
            .expect("create reports parent");
        fs::write(&corrupt_file, b"{\xff").expect("write invalid UTF-8 JSONL");
        let invalid_workspace_file = layout
            .workspaces_root()
            .join(r"bad\workspace")
            .join("feedback")
            .join("reports.jsonl");
        write_feedback_reports_file(
            &invalid_workspace_file,
            &feedback_jsonl(r"bad\workspace", "feedback-1"),
        );
        let rollback_file = layout
            .workspaces_root()
            .join(format!("rollback.delete.rollback.{}", uuid::Uuid::new_v4()))
            .join("feedback")
            .join("reports.jsonl");
        write_feedback_reports_file(&rollback_file, &feedback_jsonl("rollback", "feedback-1"));
        let db = open_sqlite(&layout).await;

        let imported = import_filesystem_feedback_reports(&db, &layout)
            .await
            .expect("import feedback reports");

        assert_eq!(imported, 2);
        assert!(retained_file.exists());
        assert!(!healthy_file.exists());
        assert!(corrupt_file.exists());
        assert!(invalid_workspace_file.exists());
        assert!(rollback_file.exists());
        let mut session = &db;
        assert_eq!(
            session
                .feedback_reports()
                .get(&healthy, "feedback-1")
                .await
                .expect("get imported feedback report")
                .expect("imported feedback report")
                .task_id
                .as_deref(),
            Some("550e8400-e29b-41d4-a716-446655440000")
        );
        assert!(
            session
                .workspaces()
                .get("rollback")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn concurrent_feedback_report_import_race_is_benign_postgres() {
        let Some(url) = postgres_test_url() else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");
        let workspace = WorkspaceName::parse(&format!("feedback{}", uuid::Uuid::new_v4().simple()))
            .expect("workspace");
        let winner = feedback_record("feedback-1", "winner");
        let mut tx = db.begin().await.expect("begin workspace tx");
        tx.workspaces()
            .ensure(workspace.as_str(), winner.created_at_unix_nanos)
            .await
            .expect("ensure workspace");
        tx.commit().await.expect("commit workspace");
        let mut tx = db.begin().await.expect("begin winner tx");
        tx.feedback_reports()
            .append(&workspace, &winner)
            .await
            .expect("insert uncommitted winner feedback");
        let mut loser = winner.clone();
        loser.trying_to_do = "loser".into();
        let import = super::insert_imported_feedback_report(&db, &workspace, &loser);
        tokio::pin!(import);

        if tokio::time::timeout(std::time::Duration::from_millis(250), &mut import)
            .await
            .is_ok()
        {
            panic!("loser feedback import completed before winner commit");
        }
        tx.commit().await.expect("commit winner feedback");
        assert!(!import.await.expect("loser import should recover"));
        let mut session = &db;
        assert_eq!(
            session
                .feedback_reports()
                .get(&workspace, &winner.id)
                .await
                .expect("get feedback report"),
            Some(winner)
        );
    }

    async fn open_sqlite(layout: &AppStateLayout) -> CoralDb {
        let config = DatabaseConfig::load(layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        db
    }

    fn feedback_jsonl(workspace: &str, id: &str) -> String {
        format!(
            r#"{{"id":"{id}","workspace":"{workspace}","created_at":"2026-06-30T12:00:00Z","trying_to_do":"trying","tried":"tried","stuck":"stuck","task_id":"550e8400-e29b-41d4-a716-446655440000"}}
"#
        )
    }

    fn write_feedback_reports_file(path: &std::path::Path, contents: &str) {
        fs::create_dir_all(path.parent().expect("reports parent")).expect("create reports parent");
        fs::write(path, contents).expect("write feedback JSONL");
    }

    fn feedback_record(id: &str, trying_to_do: &str) -> FeedbackReportRecord {
        FeedbackReportRecord {
            id: id.to_string(),
            created_at_unix_nanos: 42,
            trying_to_do: trying_to_do.to_string(),
            tried: "tried".to_string(),
            stuck: "stuck".to_string(),
            task_id: None,
            publish_status: None,
            publish_error: None,
            published_at_unix_nanos: None,
        }
    }

    fn source<const V: usize, const S: usize>(
        name: &str,
        version: Option<&str>,
        variables: [(&str, &str); V],
        secrets: [&str; S],
        credential_storage: Option<CredentialStorageKind>,
        origin: SourceOrigin,
    ) -> InstalledSource {
        InstalledSource {
            name: SourceName::parse(name).expect("source name"),
            version: version.map(str::to_string),
            variables: variables
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect::<BTreeMap<_, _>>(),
            secrets: secrets.into_iter().map(str::to_string).collect(),
            credential_storage,
            credential_revision: uuid::Uuid::from_u128(1),
            origin,
        }
    }

    fn write_manifest_file(
        layout: &AppStateLayout,
        workspace: &WorkspaceName,
        source_name: &SourceName,
        manifest_yaml: &str,
    ) {
        let manifest_path = layout.manifest_file(workspace, source_name);
        fs::create_dir_all(manifest_path.parent().expect("manifest parent"))
            .expect("create manifest parent");
        fs::write(manifest_path, manifest_yaml).expect("write manifest file");
    }

    fn imported_manifest_yaml(name: &str, version: &str) -> String {
        format!(
            r"
name: {name}
version: {version}
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
    response: {{}}
    columns:
      - name: id
        type: Utf8
"
        )
    }

    fn unsafe_secret_endpoint_source() -> InstalledSource {
        source(
            "github",
            Some("1.2.3"),
            [("API_BASE", "http://api.example.com")],
            ["API_TOKEN"],
            Some(CredentialStorageKind::Keychain),
            SourceOrigin::Imported,
        )
    }

    fn unsafe_secret_endpoint_manifest_yaml(name: &str, version: &str) -> String {
        imported_manifest_yaml(name, version).replacen(
            "base_url: https://example.com",
            r#"base_url: "{{input.API_BASE}}"
inputs: { API_BASE: { kind: variable }, API_TOKEN: { kind: secret } }
auth: { type: HeaderAuth, headers: [{ name: Authorization, from: template, template: "Bearer {{input.API_TOKEN}}" }] }"#,
            1,
        )
    }

    fn install_legacy_v4_materialization(
        layout: &AppStateLayout,
        workspace: &WorkspaceName,
        source_name: &SourceName,
        descriptor_file: &std::path::Path,
    ) -> (String, std::path::PathBuf) {
        fs::write(descriptor_file, openapi_fixture()).expect("write OpenAPI fixture");
        let manifest_yaml = v4_manifest_yaml(source_name.as_str(), descriptor_file);
        let parsed_manifest = parse_source_manifest_yaml(&manifest_yaml)
            .expect("parse manifest")
            .as_v4()
            .expect("v4 manifest")
            .clone();
        let build = build_v4_materialization_tmp(
            layout,
            workspace,
            source_name,
            &manifest_yaml,
            &parsed_manifest,
            &MaterializationInputs::default(),
            "test",
        )
        .expect("build materialization");
        replace_or_retire_v4_materialization(layout, workspace, source_name, Some(&build.temp_dir))
            .expect("install legacy materialization");
        let materialized_dir = layout.v4_materialized_dir(workspace, source_name);
        assert!(materialized_dir.exists());
        (manifest_yaml, materialized_dir)
    }

    fn v4_manifest_yaml(name: &str, descriptor_file: &std::path::Path) -> String {
        format!(
            r"
name: {name}
dsl_version: 4
surface:
  type: openapi
  file: {}
",
            descriptor_file.display()
        )
    }

    fn openapi_fixture() -> &'static str {
        r"
openapi: 3.0.3
info:
  title: GitHub
servers:
  - url: https://api.example.com
paths:
  /issues:
    get:
      operationId: issues/list
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    id: {type: integer}
                    title: {type: string}
"
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "The Postgres import harness is explicitly gated by this CI/test-only variable."
    )]
    fn postgres_test_url() -> Option<String> {
        std::env::var("CORAL_TEST_POSTGRES_URL")
            .ok()
            .filter(|value| !value.is_empty())
    }
}
