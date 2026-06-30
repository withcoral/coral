use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;

use chrono::{DateTime, Utc};
use coral_api::CORAL_EPISODE_INTENT_MAX_CHARS;
use serde::Deserialize;

use super::session::DbRepos;
use super::{CoralDb, DbError, MaterializationRecord};
use crate::bootstrap::AppError;
use crate::episode::EpisodeId;
use crate::episode::store::{
    EpisodeStoreError, MAX_EPISODE_BYTES_PER_WORKSPACE, episode_record_bytes,
    next_db_episode_created_at, retain_episode_records_within_budget,
};
use crate::sources::catalog::validate_imported_manifest_database_persistence;
use crate::sources::materialization::{
    load_v4_materialization_from_record, materialization_record_from_dir,
};
use crate::sources::model::SourceOrigin;
use crate::state::db::{EpisodeRecord, FeedbackReportRecord};
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceName;
use coral_spec::parse_source_manifest_yaml;
use uuid::Uuid;

const LEGACY_SOURCE_CATALOG_IMPORT_MARKER: &str = "legacy_source_catalog_imported";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceCatalogImportReport {
    pub(crate) workspace_count: usize,
    pub(crate) source_count: usize,
}

pub(crate) async fn import_config_source_catalog(
    db: &CoralDb,
    config_store: &ConfigStore,
    layout: &AppStateLayout,
    now_unix_nanos: i64,
) -> Result<SourceCatalogImportReport, AppError> {
    let _state_lock = config_store.state_lock_exclusive()?;
    let entries = config_store
        .load_config_unlocked()?
        .source_catalog_entries();

    let mut tx = db.begin().await?;
    if tx
        .app_state_markers()
        .contains(LEGACY_SOURCE_CATALOG_IMPORT_MARKER)
        .await?
    {
        tx.commit().await?;
        import_filesystem_source_manifests(db, layout, now_unix_nanos).await?;
        import_filesystem_v4_materializations(db, layout, now_unix_nanos).await?;
        clear_legacy_source_catalog_config(config_store, entries.len());
        return Ok(SourceCatalogImportReport {
            workspace_count: 0,
            source_count: 0,
        });
    }

    let mut workspaces = BTreeSet::new();
    let mut source_count = 0;
    for (workspace_name, source) in &entries {
        if tx
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
        workspaces.insert(workspace_name.clone());
        tx.workspaces()
            .ensure(workspace_name.as_str(), now_unix_nanos)
            .await?;
        tx.sources()
            .upsert_source(workspace_name, source, now_unix_nanos)
            .await?;
        if let Some(manifest_yaml) = manifest_yaml {
            tx.source_manifests()
                .upsert(workspace_name, &source.name, &manifest_yaml, now_unix_nanos)
                .await?;
        }
        let imported = tx
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
    if !entries.is_empty() {
        tx.app_state_markers()
            .insert(LEGACY_SOURCE_CATALOG_IMPORT_MARKER, now_unix_nanos)
            .await?;
    }
    tx.commit().await?;

    import_filesystem_source_manifests(db, layout, now_unix_nanos).await?;
    import_filesystem_v4_materializations(db, layout, now_unix_nanos).await?;
    clear_legacy_source_catalog_config(config_store, entries.len());

    Ok(SourceCatalogImportReport {
        workspace_count: workspaces.len(),
        source_count,
    })
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
    source_name: &crate::sources::SourceName,
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
            let dir = layout.v4_materialized_dir(&workspace_name, &source.name);
            if !dir.exists() {
                continue;
            }
            if session
                .materializations()
                .get(&workspace_name, &source.name)
                .await?
                .is_some()
            {
                remove_v4_materialization_dir(&dir);
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
                materialization_record_from_dir(&source.name, &dir, now_unix_nanos),
                &workspace_name,
                &source.name,
            ) else {
                continue;
            };
            if v4_backfill_or_skip(
                load_v4_materialization_from_record(&source.name, &manifest_yaml, v4, &record),
                &workspace_name,
                &source.name,
            )
            .is_none()
            {
                continue;
            }
            upsert_imported_v4_materialization(db, &workspace_name, &source.name, &record).await?;
            remove_v4_materialization_dir(&dir);
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
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct PersistedEpisode {
    id: String,
    workspace: String,
    intent: String,
    parent_episode_id: Option<String>,
    created_at_unix_nanos: u128,
}

pub(crate) async fn import_filesystem_feedback_reports(
    db: &CoralDb,
    layout: &AppStateLayout,
) -> Result<usize, AppError> {
    let mut imported = 0;
    for workspace_name in filesystem_workspaces(layout)? {
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
        publish_status: None,
        publish_error: None,
        published_at_unix_nanos: None,
    })
}

pub(crate) async fn import_filesystem_episodes(
    db: &CoralDb,
    layout: &AppStateLayout,
) -> Result<usize, AppError> {
    import_filesystem_episodes_with_max_bytes(db, layout, MAX_EPISODE_BYTES_PER_WORKSPACE).await
}

async fn import_filesystem_episodes_with_max_bytes(
    db: &CoralDb,
    layout: &AppStateLayout,
    max_bytes: u64,
) -> Result<usize, AppError> {
    let mut imported = 0;
    for workspace_name in filesystem_workspaces(layout)? {
        let path = layout.episodes_file(&workspace_name);
        if !path.exists() {
            continue;
        }
        let raw = match fs::read(&path) {
            Ok(raw) => raw,
            Err(error) => {
                tracing::warn!(
                    path = %path.display(),
                    detail = %error,
                    "skipping legacy episode JSONL import because the file could not be read"
                );
                continue;
            }
        };
        let mut has_unimported_rows = false;
        let records = raw
            .split(|&byte| byte == b'\n')
            .enumerate()
            .filter_map(|(index, line)| {
                let Ok(text) = std::str::from_utf8(line) else {
                    tracing::warn!(
                        path = %path.display(),
                        line = index + 1,
                        "leaving episode record with invalid UTF-8 in legacy JSONL"
                    );
                    has_unimported_rows = true;
                    return None;
                };
                if text.trim().is_empty() {
                    return None;
                }
                if let Some(record) =
                    parse_legacy_episode_line(&path, workspace_name.as_str(), index + 1, text)
                {
                    Some(record)
                } else {
                    has_unimported_rows = true;
                    None
                }
            })
            .collect::<Vec<_>>();
        if !records.is_empty() {
            let (workspace_imported, has_conflicts) =
                import_workspace_episodes(db, &workspace_name, &path, &records, max_bytes).await?;
            imported += workspace_imported;
            has_unimported_rows |= has_conflicts;
        }
        if has_unimported_rows {
            tracing::warn!(
                path = %path.display(),
                "legacy episode JSONL retained because at least one row was not imported"
            );
        } else {
            remove_episodes_file(&path);
        }
    }
    Ok(imported)
}

async fn import_workspace_episodes(
    db: &CoralDb,
    workspace_name: &WorkspaceName,
    path: &std::path::Path,
    records: &[EpisodeRecord],
    max_bytes: u64,
) -> Result<(usize, bool), AppError> {
    let Some(first) = records.first() else {
        return Ok((0, false));
    };
    let mut tx = db.begin().await?;
    tx.workspaces()
        .ensure_write_locked(workspace_name.as_str(), first.created_at_unix_nanos)
        .await?;
    let mut episodes = tx
        .episodes()
        .list_workspace_episodes(workspace_name)
        .await?;
    let mut imported = 0;
    let mut has_conflicts = false;
    for record in records {
        if let Some(existing) = episodes.iter().find(|existing| existing.id == record.id) {
            if existing.intent == record.intent
                && existing.parent_episode_id == record.parent_episode_id
            {
                continue;
            }
            tracing::warn!(
                path = %path.display(),
                episode_id = %record.id,
                workspace = %workspace_name,
                "legacy episode JSONL retained because an episode row conflicts with the database"
            );
            has_conflicts = true;
            continue;
        }
        let (created_at_unix_nanos_raw, created_at_unix_nanos) =
            next_db_episode_created_at(&episodes).map_err(episode_store_error_to_app)?;
        let record_bytes = episode_record_bytes(
            workspace_name,
            &record.id,
            &record.intent,
            record.parent_episode_id.as_deref(),
            created_at_unix_nanos_raw,
        )
        .map_err(episode_store_error_to_app)?;
        let mut imported_record = record.clone();
        imported_record.created_at_unix_nanos = created_at_unix_nanos;
        imported_record.record_bytes = record_bytes;
        episodes.push(imported_record);
        imported += 1;
    }
    let kept = retain_episode_records_within_budget(episodes, max_bytes);
    tx.episodes()
        .replace_workspace_episodes(workspace_name, &kept)
        .await?;
    tx.commit().await?;
    Ok((imported, has_conflicts))
}

fn episode_store_error_to_app(error: EpisodeStoreError) -> AppError {
    match error {
        EpisodeStoreError::Persistence(error) => error,
        other => AppError::FailedPrecondition(other.to_string()),
    }
}

fn parse_legacy_episode_line(
    path: &std::path::Path,
    file_workspace: &str,
    line_number: usize,
    line: &str,
) -> Option<EpisodeRecord> {
    let record = match serde_json::from_str::<PersistedEpisode>(line) {
        Ok(record) => record,
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                line = line_number,
                %error,
                "leaving invalid episode record in legacy JSONL"
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
                "leaving episode with invalid workspace in legacy JSONL"
            );
            return None;
        }
    };
    if workspace.as_str() != file_workspace {
        tracing::warn!(
            path = %path.display(),
            line = line_number,
            episode_workspace = %workspace,
            file_workspace,
            "leaving episode stored under a different workspace in legacy JSONL"
        );
        return None;
    }
    if let Err(error) = EpisodeId::parse(&record.id) {
        tracing::warn!(
            path = %path.display(),
            line = line_number,
            %error,
            "leaving episode with invalid id in legacy JSONL"
        );
        return None;
    }
    let parent_episode_id = match record.parent_episode_id {
        Some(parent_episode_id) => {
            if let Err(error) = EpisodeId::parse(&parent_episode_id) {
                tracing::warn!(
                    path = %path.display(),
                    line = line_number,
                    %error,
                    "leaving episode with invalid parent id in legacy JSONL"
                );
                return None;
            }
            Some(parent_episode_id)
        }
        None => None,
    };
    let intent = record.intent.trim();
    if intent.is_empty() || intent.chars().count() > CORAL_EPISODE_INTENT_MAX_CHARS {
        tracing::warn!(
            path = %path.display(),
            line = line_number,
            "leaving episode with invalid intent in legacy JSONL"
        );
        return None;
    }
    let created_at_unix_nanos = match i64::try_from(record.created_at_unix_nanos) {
        Ok(created_at_unix_nanos) => created_at_unix_nanos,
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                line = line_number,
                %error,
                "leaving episode with out-of-range timestamp in legacy JSONL"
            );
            return None;
        }
    };
    Some(EpisodeRecord {
        id: record.id,
        intent: intent.to_string(),
        parent_episode_id,
        created_at_unix_nanos,
        record_bytes: 0,
    })
}

fn filesystem_workspaces(layout: &AppStateLayout) -> Result<Vec<WorkspaceName>, AppError> {
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
                "skipping legacy filesystem import for invalid workspace directory"
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

fn remove_episodes_file(path: &std::path::Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                detail = %error,
                "episodes imported into database but legacy JSONL cleanup failed"
            );
        }
    }
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
    source_name: &crate::sources::SourceName,
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
    source_name: &crate::sources::SourceName,
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
        SourceCatalogImportReport, import_config_source_catalog, import_filesystem_episodes,
        import_filesystem_episodes_with_max_bytes, import_filesystem_feedback_reports,
    };
    use crate::credentials::CredentialStorageKind;
    use crate::sources::SourceName;
    use crate::sources::materialization::{
        MaterializationInputs, build_v4_materialization_tmp, replace_v4_materialization,
    };
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::db::session::DbRepos;
    use crate::state::db::{
        CoralDb, DatabaseConfig, EpisodeRecord, FeedbackReportRecord, MaterializationRecord,
        MaterializationSurfaceRecord, ResolvedDatabaseConfig,
    };
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::WorkspaceName;
    use coral_spec::parse_source_manifest_yaml;

    const OPENAPI_FIXTURE: &str = r#"{"openapi":"3.0.3","servers":[{"url":"https://api.example.com"}],"paths":{"/issues":{"get":{"operationId":"issues/list","responses":{"200":{"content":{"application/json":{"schema":{"type":"array","items":{"type":"object","properties":{"id":{"type":"integer"},"title":{"type":"string"}}}}}}}}}}}}"#;

    #[tokio::test]
    async fn imports_config_source_catalog_into_database() {
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
            .upsert_source(&workspace, source.clone())
            .expect("write config source");
        let manifest_yaml = imported_manifest_yaml("github", "1.2.3");
        write_manifest_file(&layout, &workspace, &source.name, &manifest_yaml);
        let db = open_sqlite(&layout).await;

        let report = import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect("import source catalog");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                workspace_count: 1,
                source_count: 1,
            }
        );
        let mut session = &db;
        assert!(
            session
                .workspaces()
                .get(workspace.as_str())
                .await
                .expect("get workspace")
                .is_some()
        );
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
            !session
                .app_state_markers()
                .contains(super::LEGACY_SOURCE_CATALOG_IMPORT_MARKER)
                .await
                .expect("legacy marker should not be inserted")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_legacy_config_cleanup_does_not_reimport_stale_sources() {
        use std::fs;
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
        fs::set_permissions(layout.config_dir(), fs::Permissions::from_mode(0o500))
            .expect("make config dir read-only");
        let report = import_config_source_catalog(&db, &config_store, &layout, 11).await;
        fs::set_permissions(layout.config_dir(), fs::Permissions::from_mode(0o700))
            .expect("restore config dir permissions");
        assert_eq!(
            report.expect("cleanup failure should not fail committed import"),
            SourceCatalogImportReport {
                workspace_count: 1,
                source_count: 1,
            }
        );

        let mut stale_config_source = source.clone();
        stale_config_source
            .variables
            .insert("OWNER".to_string(), "coral".to_string());
        config_store
            .upsert_source(&workspace, stale_config_source)
            .expect("update config source");
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
            .expect("stale config should not reimport after marker");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                workspace_count: 0,
                source_count: 0,
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
        assert!(
            config_store
                .load_config_unlocked()
                .expect("load cleaned stale config")
                .source_catalog_entries()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn empty_config_import_preserves_existing_database_sources() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = source("github", None, [], [], None, SourceOrigin::Bundled);
        let db = open_sqlite(&layout).await;
        {
            let mut tx = db.begin().await.expect("begin tx");
            tx.workspaces()
                .ensure(workspace.as_str(), 11)
                .await
                .expect("ensure workspace");
            tx.sources()
                .upsert_source(&workspace, &source, 11)
                .await
                .expect("seed db source");
            tx.commit().await.expect("commit tx");
        }
        let report = import_config_source_catalog(&db, &config_store, &layout, 22)
            .await
            .expect("import empty source catalog");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                workspace_count: 0,
                source_count: 0,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("get preserved source"),
            Some(source)
        );
    }

    #[tokio::test]
    async fn partial_database_catalog_imports_missing_config_sources_without_overwriting_existing()
    {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let config_workspace = WorkspaceName::parse("other").expect("workspace");
        let db_source = source("github", None, [], [], None, SourceOrigin::Bundled);
        let mut stale_config_source = db_source.clone();
        stale_config_source.version = Some("9.9.9".to_string());
        stale_config_source
            .variables
            .insert("OWNER".to_string(), "stale".to_string());
        stale_config_source.origin = SourceOrigin::Imported;
        let config_source = source("slack", None, [], [], None, SourceOrigin::Bundled);
        config_store
            .upsert_source(&workspace, stale_config_source)
            .expect("write stale config source");
        config_store
            .create_workspace(&config_workspace)
            .expect("create config workspace");
        config_store
            .upsert_source(&config_workspace, config_source.clone())
            .expect("write config source");
        let db = open_sqlite(&layout).await;
        {
            let mut tx = db.begin().await.expect("begin tx");
            tx.workspaces()
                .ensure(workspace.as_str(), 11)
                .await
                .expect("ensure workspace");
            tx.sources()
                .upsert_source(&workspace, &db_source, 11)
                .await
                .expect("seed db source");
            tx.commit().await.expect("commit tx");
        }

        let report = import_config_source_catalog(&db, &config_store, &layout, 22)
            .await
            .expect("import source catalog");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                workspace_count: 1,
                source_count: 1,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &db_source.name)
                .await
                .expect("get existing db source"),
            Some(db_source)
        );
        assert_eq!(
            session
                .sources()
                .get_source(&config_workspace, &config_source.name)
                .await
                .expect("get imported config source"),
            Some(config_source)
        );
        assert!(
            config_store
                .load_config_unlocked()
                .expect("load cleaned config")
                .source_catalog_entries()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn empty_config_catalog_import_is_noop() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let db = open_sqlite(&layout).await;

        let report = import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect("import empty catalog");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                workspace_count: 0,
                source_count: 0,
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

    #[tokio::test]
    async fn empty_config_catalog_does_not_complete_legacy_import() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = source("github", None, [], [], None, SourceOrigin::Bundled);
        let db = open_sqlite(&layout).await;

        let empty_report = import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect("import empty catalog");
        config_store
            .upsert_source(&workspace, source.clone())
            .expect("write config source after empty import");
        let source_report = import_config_source_catalog(&db, &config_store, &layout, 99)
            .await
            .expect("import source catalog after empty import");

        assert_eq!(
            empty_report,
            SourceCatalogImportReport {
                workspace_count: 0,
                source_count: 0,
            }
        );
        assert_eq!(
            source_report,
            SourceCatalogImportReport {
                workspace_count: 1,
                source_count: 1,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("get source"),
            Some(source)
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

        let report = import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect("missing imported manifest should not block source catalog import");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                workspace_count: 1,
                source_count: 1,
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
        assert_eq!(
            config_store
                .load_config_unlocked()
                .expect("legacy config should be cleaned after source catalog import")
                .source_catalog_entries(),
            Vec::new()
        );

        let second_report = import_config_source_catalog(&db, &config_store, &layout, 22)
            .await
            .expect("missing imported manifest should not block later backfill attempts");
        assert_eq!(
            second_report,
            SourceCatalogImportReport {
                workspace_count: 0,
                source_count: 0,
            }
        );
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
            "name: {}\ndsl_version: 4\nsurfaces:\n- id: rest\n  type: openapi\n  file: {}\n",
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
        replace_v4_materialization(
            &layout,
            &workspace,
            &healthy.name,
            &build.expect("build materialization").temp_dir,
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
        tx.commit().await.expect("commit sources");

        import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect("invalid backfills should not fail startup import");

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
        tx.commit().await.expect("commit source");

        let report = import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect("backfill materialization");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                workspace_count: 0,
                source_count: 0,
            }
        );
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

        let report = import_config_source_catalog(&db, &config_store, &layout, 11)
            .await
            .expect("import source and materialization");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                workspace_count: 1,
                source_count: 1,
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
                .source_catalog_entries()
                .is_empty()
        );
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run concurrent Postgres backfill coverage"]
    async fn concurrent_v4_materialization_backfill_race_is_benign_postgres() {
        let Some(url) = crate::bootstrap::env_var("CORAL_TEST_POSTGRES_URL") else {
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
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run concurrent Postgres feedback import coverage"]
    async fn concurrent_feedback_report_import_race_is_benign_postgres() {
        let Some(url) = crate::bootstrap::env_var("CORAL_TEST_POSTGRES_URL") else {
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

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run concurrent Postgres episode import coverage"]
    async fn concurrent_episode_import_race_serializes_on_workspace_lock_postgres() {
        let Some(url) = crate::bootstrap::env_var("CORAL_TEST_POSTGRES_URL") else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let workspace = WorkspaceName::parse(&format!("episode{}", uuid::Uuid::new_v4().simple()))
            .expect("workspace");
        let episodes_file = write_episodes_file(
            &layout,
            &workspace,
            &episode_jsonl(workspace.as_str(), "ep_1", "task", 1),
        );
        let mut tx = db.begin().await.expect("begin lock tx");
        tx.workspaces()
            .ensure_write_locked(workspace.as_str(), 7)
            .await
            .expect("hold workspace lock");
        let first = import_filesystem_episodes(&db, &layout);
        let second = import_filesystem_episodes(&db, &layout);
        tokio::pin!(first);
        tokio::pin!(second);

        if tokio::time::timeout(std::time::Duration::from_millis(250), async {
            tokio::select! {
                result = &mut first => result,
                result = &mut second => result,
            }
        })
        .await
        .is_ok()
        {
            panic!("episode import completed before the workspace lock was released");
        }
        tx.commit().await.expect("release workspace lock");
        let (first, second) = tokio::join!(&mut first, &mut second);
        assert_eq!(
            first.expect("first import") + second.expect("second import"),
            1
        );
        assert!(!episodes_file.exists());
        let mut session = &db;
        let episodes = session
            .episodes()
            .list_workspace_episodes(&workspace)
            .await
            .expect("list episodes");
        assert_eq!(episodes.len(), 1);
        assert_eq!(episodes.first().expect("episode").id, "ep_1");
    }

    #[tokio::test]
    async fn episode_import_applies_budget_in_file_order() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        write_episodes_file(
            &layout,
            &workspace,
            &[
                episode_jsonl("default", "ep_z", "first file row", 300),
                episode_jsonl("default", "ep_y", "middle file row", 200),
                episode_jsonl("default", "ep_a", "last file row", 1),
            ]
            .concat(),
        );
        let db = open_sqlite(&layout).await;

        import_filesystem_episodes_with_max_bytes(&db, &layout, 1)
            .await
            .expect("import episodes");
        let mut session = &db;
        let episodes = session
            .episodes()
            .list_workspace_episodes(&workspace)
            .await
            .expect("list episodes");
        assert_eq!(episodes.len(), 1);
        let survivor = episodes.first().expect("survivor");
        assert_eq!(survivor.id, "ep_a", "legacy file order decides newest");
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "This integration-style import test keeps related retention cases in one setup."
    )]
    async fn episode_import_retains_legacy_jsonl_when_rows_are_not_imported() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let episodes_file = layout.episodes_file(&workspace);
        fs::create_dir_all(episodes_file.parent().expect("episodes parent"))
            .expect("create episodes parent");
        fs::write(
            &episodes_file,
            r#"{"id":"ep_1","workspace":"default","intent":"find the HR onboarding form","parent_episode_id":null,"created_at_unix_nanos":99}
not-json
{"id":"ep_other","workspace":"other","intent":"wrong workspace","parent_episode_id":null,"created_at_unix_nanos":100}
{"id":"ep_conflict","workspace":"default","intent":"legacy intent","parent_episode_id":null,"created_at_unix_nanos":101}
"#,
        )
        .expect("write episode JSONL");
        #[cfg(unix)]
        let blocked_file = {
            use std::os::unix::fs::PermissionsExt;
            let blocked = WorkspaceName::parse("blocked").expect("workspace");
            let file = write_episodes_file(
                &layout,
                &blocked,
                &episode_jsonl("blocked", "ep_blocked", "blocked", 1),
            );
            fs::set_permissions(&file, fs::Permissions::from_mode(0o000))
                .expect("make blocked file unreadable");
            file
        };
        let db = open_sqlite(&layout).await;
        let mut tx = db.begin().await.expect("begin seed tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("ensure workspace");
        tx.episodes()
            .insert(
                &workspace,
                &EpisodeRecord {
                    id: "ep_conflict".to_string(),
                    intent: "database intent".to_string(),
                    parent_episode_id: None,
                    created_at_unix_nanos: 1,
                    record_bytes: 1,
                },
            )
            .await
            .expect("seed existing episode");
        tx.commit().await.expect("commit seed");

        let imported = import_filesystem_episodes(&db, &layout)
            .await
            .expect("import episodes");

        #[cfg(unix)]
        assert!(blocked_file.exists());
        assert_eq!(imported, 1);
        assert!(
            episodes_file.exists(),
            "legacy episode JSONL with unimported rows must be preserved"
        );
        assert_eq!(
            import_filesystem_episodes(&db, &layout)
                .await
                .expect("reimport episodes"),
            0
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&blocked_file, fs::Permissions::from_mode(0o600))
                .expect("restore blocked file permissions");
        }
        let mut session = &db;
        assert!(
            session
                .workspaces()
                .get("other")
                .await
                .expect("get other workspace")
                .is_none(),
            "cross-workspace episode import must not create the embedded workspace"
        );
        let episodes = session
            .episodes()
            .list_workspace_episodes(&workspace)
            .await
            .expect("list episodes");
        assert_eq!(episodes.len(), 2);
        assert_eq!(
            episodes
                .iter()
                .find(|episode| episode.id == "ep_1")
                .expect("imported episode")
                .intent,
            "find the HR onboarding form"
        );
        assert_eq!(
            episodes
                .iter()
                .find(|episode| episode.id == "ep_conflict")
                .expect("seed episode")
                .intent,
            "database intent"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn import_succeeds_when_post_commit_config_cleanup_fails() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = source("github", None, [], [], None, SourceOrigin::Bundled);
        config_store
            .upsert_source(&workspace, source.clone())
            .expect("write config source");
        drop(
            config_store
                .state_lock_exclusive()
                .expect("create state lock before making config dir read-only"),
        );

        let db_path = temp.path().join("db").join("coral.db");
        fs::create_dir_all(db_path.parent().expect("db parent")).expect("create db dir");
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path: db_path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");

        let original_mode = fs::metadata(layout.config_dir())
            .expect("config dir metadata")
            .permissions()
            .mode();
        fs::set_permissions(layout.config_dir(), fs::Permissions::from_mode(0o500))
            .expect("make config dir read-only");

        let result = import_config_source_catalog(&db, &config_store, &layout, 11).await;

        fs::set_permissions(
            layout.config_dir(),
            fs::Permissions::from_mode(original_mode),
        )
        .expect("restore config dir permissions");

        assert_eq!(
            result.expect("cleanup failure should not fail committed import"),
            SourceCatalogImportReport {
                workspace_count: 1,
                source_count: 1,
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
        assert_eq!(
            config_store
                .load_config_unlocked()
                .expect("legacy config should still load after failed cleanup")
                .source_catalog_entries()
                .len(),
            1
        );

        let mut tx = db.begin().await.expect("begin delete tx");
        tx.sources()
            .remove_source(&workspace, &source.name)
            .await
            .expect("delete db source");
        tx.commit().await.expect("commit delete tx");

        let report = import_config_source_catalog(&db, &config_store, &layout, 99)
            .await
            .expect("stale config should not reimport after marker");

        assert_eq!(
            report,
            SourceCatalogImportReport {
                workspace_count: 0,
                source_count: 0,
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
        assert!(
            config_store
                .load_config_unlocked()
                .expect("load cleaned stale config")
                .source_catalog_entries()
                .is_empty()
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

    fn episode_jsonl(
        workspace: &str,
        id: &str,
        intent: &str,
        created_at_unix_nanos: u128,
    ) -> String {
        format!(
            r#"{{"id":"{id}","workspace":"{workspace}","intent":"{intent}","parent_episode_id":null,"created_at_unix_nanos":{created_at_unix_nanos}}}
"#
        )
    }

    fn write_episodes_file(
        layout: &AppStateLayout,
        workspace: &WorkspaceName,
        contents: &str,
    ) -> std::path::PathBuf {
        let episodes_file = layout.episodes_file(workspace);
        fs::create_dir_all(episodes_file.parent().expect("episodes parent"))
            .expect("create episodes parent");
        fs::write(&episodes_file, contents).expect("write episode JSONL");
        episodes_file
    }

    fn feedback_jsonl(workspace: &str, id: &str) -> String {
        format!(
            r#"{{"id":"{id}","workspace":"{workspace}","created_at":"2026-06-30T12:00:00Z","trying_to_do":"trying","tried":"tried","stuck":"stuck"}}
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
        replace_v4_materialization(layout, workspace, source_name, &build.temp_dir)
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
surfaces:
  - id: rest
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
}
