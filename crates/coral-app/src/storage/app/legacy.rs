use std::fs::{self, File};
use std::io::{BufRead as _, BufReader};
use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::warn;

use super::{AppStorageError, AppStore, StoredEpisode, StoredFeedbackReport};
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

const LEGACY_JSONL_IMPORT: &str = "legacy_jsonl_import";

#[derive(Debug, Deserialize, Serialize)]
struct LegacyEpisode {
    id: String,
    workspace: String,
    intent: String,
    parent_episode_id: Option<String>,
    created_at_unix_nanos: u128,
}

#[derive(Debug, Deserialize)]
struct LegacyFeedbackReport {
    id: String,
    created_at: String,
    trying_to_do: String,
    tried: String,
    stuck: String,
}

pub(super) fn migrate_jsonl(
    store: &AppStore,
    layout: &AppStateLayout,
) -> Result<(), AppStorageError> {
    if store.migration_applied(LEGACY_JSONL_IMPORT)? {
        return Ok(());
    }

    let workspaces = legacy_workspace_names(layout)?;
    let mut uow = store.begin_write()?;
    for workspace in workspaces {
        import_episodes(&mut uow.episodes(), layout, &workspace)?;
        import_feedback_reports(&mut uow.feedback(), layout, &workspace)?;
    }
    uow.mark_migration_applied(LEGACY_JSONL_IMPORT)?;
    uow.commit()
}

fn legacy_workspace_names(layout: &AppStateLayout) -> Result<Vec<WorkspaceName>, AppStorageError> {
    let entries = match fs::read_dir(layout.workspaces_root()) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };

    let mut workspaces = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let raw_name = entry.file_name();
        let Some(name) = raw_name.to_str() else {
            warn!(path = %entry.path().display(), "skipping legacy app-storage workspace with non-UTF-8 name");
            continue;
        };
        match WorkspaceName::parse(name) {
            Ok(workspace) => workspaces.push(workspace),
            Err(error) => {
                warn!(workspace = name, %error, "skipping legacy app-storage workspace with invalid name");
            }
        }
    }
    workspaces.sort();
    Ok(workspaces)
}

fn import_episodes(
    episodes: &mut super::EpisodeRepository<'_>,
    layout: &AppStateLayout,
    workspace: &WorkspaceName,
) -> Result<(), AppStorageError> {
    let path = layout.episodes_file(workspace);
    stream_jsonl::<LegacyEpisode>(&path, "episode", |legacy| {
        let Some(record) = stored_episode(legacy, workspace)? else {
            return Ok(());
        };
        episodes.import_episode(&record)
    })
}

fn stored_episode(
    legacy: LegacyEpisode,
    workspace: &WorkspaceName,
) -> Result<Option<StoredEpisode>, AppStorageError> {
    let created_at_unix_nanos = match i64::try_from(legacy.created_at_unix_nanos).map_err(
        |_error| AppStorageError::ValueOutOfRange {
            field: "created_at_unix_nanos",
            value: legacy.created_at_unix_nanos.to_string(),
        },
    ) {
        Ok(created_at) => created_at,
        Err(error) => {
            warn!(episode_id = %legacy.id, workspace = workspace.as_str(), %error, "skipping legacy episode with out-of-range timestamp");
            return Ok(None);
        }
    };
    let record_for_size = LegacyEpisode {
        workspace: workspace.as_str().to_string(),
        ..legacy
    };
    Ok(Some(StoredEpisode {
        workspace: record_for_size.workspace.clone(),
        id: record_for_size.id.clone(),
        intent: record_for_size.intent.clone(),
        parent_episode_id: record_for_size.parent_episode_id.clone(),
        created_at_unix_nanos,
        record_bytes: serde_json::to_vec(&record_for_size)?.len() as u64 + 1,
    }))
}

fn import_feedback_reports(
    feedback: &mut super::FeedbackRepository<'_>,
    layout: &AppStateLayout,
    workspace: &WorkspaceName,
) -> Result<(), AppStorageError> {
    let path = layout.feedback_reports_file(workspace);
    stream_jsonl::<LegacyFeedbackReport>(&path, "feedback report", |legacy| {
        let record = StoredFeedbackReport {
            id: legacy.id,
            workspace: workspace.as_str().to_string(),
            created_at_rfc3339: legacy.created_at,
            trying_to_do: legacy.trying_to_do,
            tried: legacy.tried,
            stuck: legacy.stuck,
        };
        feedback.import_report(&record)
    })
}

fn stream_jsonl<T>(
    path: &Path,
    kind: &'static str,
    mut visit: impl FnMut(T) -> Result<(), AppStorageError>,
) -> Result<(), AppStorageError>
where
    T: for<'de> Deserialize<'de>,
{
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let mut reader = BufReader::new(file);
    let mut raw_line = Vec::new();
    loop {
        raw_line.clear();
        if reader.read_until(b'\n', &mut raw_line)? == 0 {
            break;
        }
        if raw_line.ends_with(b"\n") {
            raw_line.pop();
        }
        if raw_line.ends_with(b"\r") {
            raw_line.pop();
        }
        let Ok(line) = std::str::from_utf8(&raw_line) else {
            warn!(path = %path.display(), "skipping legacy {kind} record with invalid UTF-8");
            continue;
        };
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<T>(line) {
            Ok(record) => visit(record)?,
            Err(error) => {
                warn!(path = %path.display(), %error, "skipping unparsable legacy {kind} record");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::migrate_jsonl;
    use crate::state::AppStateLayout;
    use crate::storage::app::{AppStore, StoredFeedbackReport};
    use crate::workspaces::WorkspaceName;

    #[test]
    fn imports_legacy_episode_and_feedback_jsonl_once() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral-config")))
            .expect("layout should resolve");
        let workspace = WorkspaceName::default();
        let episode_path = layout.episodes_file(&workspace);
        let feedback_path = layout.feedback_reports_file(&workspace);
        std::fs::create_dir_all(episode_path.parent().expect("episode parent")).expect("mkdir");
        std::fs::create_dir_all(feedback_path.parent().expect("feedback parent")).expect("mkdir");
        std::fs::write(
            &episode_path,
            r#"{"id":"ep_legacy","workspace":"default","intent":"legacy task","parent_episode_id":null,"created_at_unix_nanos":123}
{"id":"broken"
"#,
        )
        .expect("write legacy episodes");
        std::fs::write(
            &feedback_path,
            r#"{"id":"fb_legacy","workspace":"default","created_at":"2026-06-26T00:00:00Z","trying_to_do":"trying","tried":"tried","stuck":"stuck"}
"#,
        )
        .expect("write legacy feedback");

        let store = AppStore::sqlite(layout.app_database_file()).expect("sqlite store");
        migrate_jsonl(&store, &layout).expect("first migration");
        migrate_jsonl(&store, &layout).expect("second migration is skipped");

        let episode = store
            .test_read_episode("default", "ep_legacy")
            .expect("read episode")
            .expect("episode imported");
        assert_eq!(episode.intent, "legacy task");
        assert_eq!(episode.created_at_unix_nanos, 123);
        let feedback = store
            .test_read_feedback_reports("default")
            .expect("read feedback");
        assert_eq!(
            feedback,
            vec![StoredFeedbackReport {
                id: "fb_legacy".to_string(),
                workspace: "default".to_string(),
                created_at_rfc3339: "2026-06-26T00:00:00Z".to_string(),
                trying_to_do: "trying".to_string(),
                tried: "tried".to_string(),
                stuck: "stuck".to_string(),
            }]
        );
    }
}
