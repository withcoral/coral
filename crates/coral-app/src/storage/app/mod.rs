//! Application-owned durable storage.
//!
//! This module is the storage boundary for internal app state that should be
//! database-backed. Callers work through a unit of work and domain repositories;
//! backend-specific code stays below this layer.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use crate::state::AppStateLayout;

pub(crate) mod config;
mod legacy;
mod sqlite;

pub(crate) use config::{AppStorageBackend, AppStorageConfig};

#[derive(Debug, thiserror::Error)]
pub(crate) enum AppStorageError {
    #[error("app storage io: {0}")]
    Io(#[from] std::io::Error),
    #[error("app storage config: {0}")]
    Config(#[from] toml::de::Error),
    #[error("app storage sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("app storage json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("app storage mutex poisoned")]
    Poisoned,
    #[error("unsupported app storage backend '{backend}'")]
    UnsupportedBackend { backend: String },
    #[error("app storage value '{field}' is out of range: {value}")]
    ValueOutOfRange { field: &'static str, value: String },
    #[error("app storage is corrupt: {0}")]
    Corrupt(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredEpisode {
    pub(crate) workspace: String,
    pub(crate) id: String,
    pub(crate) intent: String,
    pub(crate) parent_episode_id: Option<String>,
    pub(crate) created_at_unix_nanos: i64,
    pub(crate) record_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenEpisodeResult {
    Opened,
    AlreadyOpen,
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredFeedbackReport {
    pub(crate) id: String,
    pub(crate) workspace: String,
    pub(crate) created_at_rfc3339: String,
    pub(crate) trying_to_do: String,
    pub(crate) tried: String,
    pub(crate) stuck: String,
}

#[derive(Clone)]
pub(crate) struct AppStore {
    backend: Arc<dyn AppStoreBackend>,
}

impl AppStore {
    pub(crate) fn open(
        layout: &AppStateLayout,
        config: &AppStorageConfig,
    ) -> Result<Self, AppStorageError> {
        let store = match config.backend {
            AppStorageBackend::Sqlite => Self::sqlite(config.sqlite_path(layout))?,
            AppStorageBackend::Postgres => Err(AppStorageError::UnsupportedBackend {
                backend: AppStorageBackend::Postgres.as_config_value().to_string(),
            })?,
        };
        legacy::migrate_jsonl(&store, layout)?;
        Ok(store)
    }

    pub(crate) fn sqlite(path: PathBuf) -> Result<Self, AppStorageError> {
        Ok(Self {
            backend: Arc::new(sqlite::SqliteAppStore::open(path)?),
        })
    }

    pub(crate) fn begin_write(&self) -> Result<AppWriteUnitOfWork<'_>, AppStorageError> {
        Ok(AppWriteUnitOfWork {
            transaction: Some(self.backend.begin_write()?),
        })
    }

    fn migration_applied(&self, name: &str) -> Result<bool, AppStorageError> {
        self.backend.migration_applied(name)
    }

    #[cfg(test)]
    pub(crate) fn test_read_episode(
        &self,
        workspace: &str,
        episode_id: &str,
    ) -> Result<Option<StoredEpisode>, AppStorageError> {
        self.backend.test_read_episode(workspace, episode_id)
    }

    #[cfg(test)]
    pub(crate) fn test_count_episodes(&self, workspace: &str) -> Result<usize, AppStorageError> {
        self.backend.test_count_episodes(workspace)
    }

    #[cfg(test)]
    pub(crate) fn test_episode_bytes(&self, workspace: &str) -> Result<u64, AppStorageError> {
        self.backend.test_episode_bytes(workspace)
    }

    #[cfg(test)]
    pub(crate) fn test_read_feedback_reports(
        &self,
        workspace: &str,
    ) -> Result<Vec<StoredFeedbackReport>, AppStorageError> {
        self.backend.test_read_feedback_reports(workspace)
    }
}

impl fmt::Debug for AppStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("AppStore").finish_non_exhaustive()
    }
}

pub(crate) struct AppWriteUnitOfWork<'a> {
    transaction: Option<Box<dyn AppStoreWriteTransaction + 'a>>,
}

impl<'a> AppWriteUnitOfWork<'a> {
    pub(crate) fn episodes(&mut self) -> EpisodeRepository<'_> {
        EpisodeRepository {
            transaction: self.transaction(),
        }
    }

    pub(crate) fn feedback(&mut self) -> FeedbackRepository<'_> {
        FeedbackRepository {
            transaction: self.transaction(),
        }
    }

    pub(crate) fn commit(mut self) -> Result<(), AppStorageError> {
        let transaction = self
            .transaction
            .take()
            .expect("unit of work transaction is present until commit");
        transaction.commit()
    }

    fn mark_migration_applied(&mut self, name: &str) -> Result<(), AppStorageError> {
        self.transaction().mark_migration_applied(name)
    }

    fn transaction(&mut self) -> &mut (dyn AppStoreWriteTransaction + 'a) {
        self.transaction
            .as_deref_mut()
            .expect("unit of work transaction is present until commit")
    }
}

pub(crate) struct EpisodeRepository<'a> {
    transaction: &'a mut dyn AppStoreWriteTransaction,
}

impl EpisodeRepository<'_> {
    pub(crate) fn open_episode(
        &mut self,
        episode: &StoredEpisode,
        max_bytes: u64,
    ) -> Result<OpenEpisodeResult, AppStorageError> {
        self.transaction.open_episode(episode, max_bytes)
    }

    fn import_episode(&mut self, episode: &StoredEpisode) -> Result<(), AppStorageError> {
        self.transaction.import_episode(episode)
    }
}

pub(crate) struct FeedbackRepository<'a> {
    transaction: &'a mut dyn AppStoreWriteTransaction,
}

impl FeedbackRepository<'_> {
    pub(crate) fn append_report(
        &mut self,
        report: &StoredFeedbackReport,
    ) -> Result<(), AppStorageError> {
        self.transaction.append_feedback_report(report)
    }

    fn import_report(&mut self, report: &StoredFeedbackReport) -> Result<(), AppStorageError> {
        self.transaction.import_feedback_report(report)
    }
}

trait AppStoreBackend: Send + Sync {
    fn begin_write(&self) -> Result<Box<dyn AppStoreWriteTransaction + '_>, AppStorageError>;

    fn migration_applied(&self, name: &str) -> Result<bool, AppStorageError>;

    #[cfg(test)]
    fn test_read_episode(
        &self,
        workspace: &str,
        episode_id: &str,
    ) -> Result<Option<StoredEpisode>, AppStorageError>;

    #[cfg(test)]
    fn test_count_episodes(&self, workspace: &str) -> Result<usize, AppStorageError>;

    #[cfg(test)]
    fn test_episode_bytes(&self, workspace: &str) -> Result<u64, AppStorageError>;

    #[cfg(test)]
    fn test_read_feedback_reports(
        &self,
        workspace: &str,
    ) -> Result<Vec<StoredFeedbackReport>, AppStorageError>;
}

trait AppStoreWriteTransaction {
    fn open_episode(
        &mut self,
        episode: &StoredEpisode,
        max_bytes: u64,
    ) -> Result<OpenEpisodeResult, AppStorageError>;

    fn import_episode(&mut self, episode: &StoredEpisode) -> Result<(), AppStorageError>;

    fn append_feedback_report(
        &mut self,
        report: &StoredFeedbackReport,
    ) -> Result<(), AppStorageError>;

    fn import_feedback_report(
        &mut self,
        report: &StoredFeedbackReport,
    ) -> Result<(), AppStorageError>;

    fn mark_migration_applied(&mut self, name: &str) -> Result<(), AppStorageError>;

    fn commit(self: Box<Self>) -> Result<(), AppStorageError>;
}
