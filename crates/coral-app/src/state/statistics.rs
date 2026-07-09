//! Workspace-scoped persisted column statistics.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use coral_engine::{
    ColumnStatistics, SourceStatistics, StatisticsObservation, StatisticsObservationScope,
    StatisticsProfile, TableStatistics,
};
use serde::{Deserialize, Serialize};

use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::state::AppStateLayout;
use crate::storage::fs::{self as storage_fs, FileLock};
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone)]
pub(crate) struct StatisticsStore {
    layout: AppStateLayout,
}

#[derive(Debug, Clone)]
pub(crate) struct StatisticsObservationRecord {
    pub(crate) observation: StatisticsObservation,
    pub(crate) trace_end_unix_nanos: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedStatisticsProfile {
    version: u32,
    #[serde(default)]
    sources: BTreeMap<String, SourceStatistics>,
    #[serde(default)]
    source_observation_watermarks: BTreeMap<String, i64>,
}

impl Default for PersistedStatisticsProfile {
    fn default() -> Self {
        let profile = StatisticsProfile::empty();
        Self {
            version: profile.version,
            sources: profile.sources,
            source_observation_watermarks: BTreeMap::new(),
        }
    }
}

impl PersistedStatisticsProfile {
    fn empty() -> Self {
        Self::default()
    }

    fn into_runtime_profile(self) -> StatisticsProfile {
        StatisticsProfile {
            version: self.version,
            sources: self.sources,
        }
    }
}

impl StatisticsStore {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    pub(crate) fn load_profile(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<StatisticsProfile, AppError> {
        let _lock = FileLock::shared(self.layout.state_lock())?;
        load_profile_unlocked(&self.layout, workspace_name)
    }

    pub(crate) fn rebuild_profile_from_observations(
        &self,
        workspace_name: &WorkspaceName,
        observations: &[StatisticsObservationRecord],
        selected_schema_names: impl IntoIterator<Item = String>,
    ) -> Result<(), AppError> {
        let _lock = FileLock::exclusive(self.layout.state_lock())?;
        let selected_schema_names = selected_schema_names.into_iter().collect::<Vec<_>>();
        let mut persisted = match load_persisted_profile_unlocked(&self.layout, workspace_name) {
            Ok(profile) => profile,
            Err(error) => {
                tracing::warn!(
                    workspace = %workspace_name,
                    detail = %error,
                    "discarding unreadable statistics profile during profile rebuild"
                );
                PersistedStatisticsProfile::empty()
            }
        };
        let mut profile = StatisticsProfile::empty();
        for record in observations {
            if record_is_after_source_watermark(record, &persisted.source_observation_watermarks) {
                merge_observation(&mut profile, &record.observation);
            }
        }
        profile.retain_sources(selected_schema_names.iter().map(String::as_str));
        persisted.sources = profile.sources;
        save_persisted_profile_unlocked(&self.layout, workspace_name, &persisted)
    }

    #[cfg(test)]
    pub(crate) fn merge_observations(
        &self,
        workspace_name: &WorkspaceName,
        observations: &[StatisticsObservation],
    ) -> Result<(), AppError> {
        let selected = observations
            .iter()
            .map(|observation| observation.schema_name.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let records = observations
            .iter()
            .cloned()
            .map(|observation| StatisticsObservationRecord {
                observation,
                trace_end_unix_nanos: i64::MAX,
            })
            .collect::<Vec<_>>();
        self.rebuild_profile_from_observations(workspace_name, &records, selected)
    }

    #[cfg(test)]
    pub(crate) fn invalidate_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<(), AppError> {
        let _lock = FileLock::exclusive(self.layout.state_lock())?;
        invalidate_source_unlocked(&self.layout, workspace_name, source_name)
    }
}

pub(super) fn invalidate_source_unlocked(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
) -> Result<(), AppError> {
    invalidate_source_unlocked_at(layout, workspace_name, source_name, current_unix_nanos())
}

fn invalidate_source_unlocked_at(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    invalidated_at_unix_nanos: i64,
) -> Result<(), AppError> {
    let mut profile = match load_persisted_profile_unlocked(layout, workspace_name) {
        Ok(profile) => profile,
        Err(error) => {
            tracing::warn!(
                workspace = %workspace_name,
                source = %source_name,
                detail = %error,
                "discarding unreadable statistics profile during source invalidation"
            );
            PersistedStatisticsProfile::empty()
        }
    };
    profile.sources.remove(source_name.as_str());
    profile
        .source_observation_watermarks
        .insert(source_name.as_str().to_string(), invalidated_at_unix_nanos);
    save_persisted_profile_unlocked(layout, workspace_name, &profile)
}

fn load_profile_unlocked(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
) -> Result<StatisticsProfile, AppError> {
    load_persisted_profile_unlocked(layout, workspace_name)
        .map(PersistedStatisticsProfile::into_runtime_profile)
}

fn load_persisted_profile_unlocked(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
) -> Result<PersistedStatisticsProfile, AppError> {
    let path = layout.statistics_profile_file(workspace_name);
    if !path.exists() {
        return Ok(PersistedStatisticsProfile::empty());
    }

    let raw = std::fs::read_to_string(&path)?;
    let profile: PersistedStatisticsProfile = serde_json::from_str(&raw)?;
    if profile.version != PersistedStatisticsProfile::empty().version {
        tracing::warn!(
            workspace = %workspace_name,
            version = profile.version,
            "ignoring unsupported statistics profile version"
        );
        return Ok(PersistedStatisticsProfile::empty());
    }
    Ok(profile)
}

fn save_persisted_profile_unlocked(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    profile: &PersistedStatisticsProfile,
) -> Result<(), AppError> {
    let path = layout.statistics_profile_file(workspace_name);
    if let Some(parent) = path.parent() {
        storage_fs::ensure_dir(parent)?;
    }
    let raw = serde_json::to_vec_pretty(profile)?;
    storage_fs::write_atomic(&path, &raw)?;
    Ok(())
}

fn record_is_after_source_watermark(
    record: &StatisticsObservationRecord,
    source_observation_watermarks: &BTreeMap<String, i64>,
) -> bool {
    source_observation_watermarks
        .get(&record.observation.schema_name)
        .is_none_or(|watermark| record.trace_end_unix_nanos > *watermark)
}

fn current_unix_nanos() -> i64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    i64::try_from(nanos).unwrap_or(i64::MAX)
}

fn merge_observation(profile: &mut StatisticsProfile, observation: &StatisticsObservation) {
    if observation.scope != StatisticsObservationScope::TableGlobal {
        return;
    }

    let source_stats = profile
        .sources
        .entry(observation.schema_name.clone())
        .or_insert_with(|| SourceStatistics {
            schema_name: observation.schema_name.clone(),
            source_version: observation.source_version.clone(),
            tables: BTreeMap::default(),
        });
    if source_stats.source_version != observation.source_version {
        source_stats.tables.clear();
    }
    source_stats
        .source_version
        .clone_from(&observation.source_version);

    let mut columns = BTreeMap::new();
    for column in &observation.columns {
        columns.insert(
            column.column_name.clone(),
            ColumnStatistics {
                column_name: column.column_name.clone(),
                sample_count: column.sample_count,
                null_count: column.null_count.clone(),
                approx_distinct_count: column.approx_distinct_count.clone(),
                observed_at: Some(observation.observed_at.clone()),
            },
        );
    }

    source_stats.tables.insert(
        observation.table_name.clone(),
        TableStatistics {
            schema_name: observation.schema_name.clone(),
            table_name: observation.table_name.clone(),
            source_version: observation.source_version.clone(),
            schema_signature: observation.schema_signature.clone(),
            columns,
        },
    );
}

#[cfg(test)]
mod tests {
    use coral_engine::{
        ColumnSchemaSignature, ColumnStatisticsObservation, StatisticsObservation,
        StatisticsObservationScope, TableSchemaSignature,
    };
    use tempfile::tempdir;

    use super::{
        StatisticsObservationRecord, StatisticsStore, invalidate_source_unlocked_at,
        load_persisted_profile_unlocked,
    };
    use crate::sources::SourceName;
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;

    fn workspace() -> WorkspaceName {
        WorkspaceName::parse("default").expect("workspace")
    }

    fn store() -> (tempfile::TempDir, StatisticsStore) {
        let temp = tempdir().expect("tempdir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        (temp, StatisticsStore::new(layout))
    }

    fn signature(nullable: bool) -> TableSchemaSignature {
        TableSchemaSignature {
            columns: vec![ColumnSchemaSignature {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
                nullable,
                is_virtual: false,
                is_required_filter: false,
            }],
            required_filters: Vec::new(),
        }
    }

    fn observation(scope: StatisticsObservationScope) -> StatisticsObservation {
        StatisticsObservation {
            schema_name: "local".to_string(),
            table_name: "events".to_string(),
            source_version: Some("0.1.0".to_string()),
            schema_signature: signature(true),
            scope,
            observed_at: "2026-05-06T00:00:00Z".to_string(),
            columns: vec![ColumnStatisticsObservation {
                column_name: "name".to_string(),
                sample_count: 3,
                null_count: Some(coral_engine::StatisticValue {
                    value: 1,
                    precision: coral_engine::StatisticPrecision::ObservedSample,
                }),
                approx_distinct_count: Some(coral_engine::StatisticValue {
                    value: 2,
                    precision: coral_engine::StatisticPrecision::ObservedSample,
                }),
            }],
        }
    }

    fn observation_record(
        observation: StatisticsObservation,
        trace_end_unix_nanos: i64,
    ) -> StatisticsObservationRecord {
        StatisticsObservationRecord {
            observation,
            trace_end_unix_nanos,
        }
    }

    fn event_table(profile: &coral_engine::StatisticsProfile) -> &coral_engine::TableStatistics {
        profile
            .sources
            .get("local")
            .expect("local source")
            .tables
            .get("events")
            .expect("events table")
    }

    fn name_column(profile: &coral_engine::StatisticsProfile) -> &coral_engine::ColumnStatistics {
        event_table(profile)
            .columns
            .get("name")
            .expect("name column")
    }

    #[test]
    fn missing_profile_loads_as_empty() {
        let (_temp, store) = store();
        let profile = store.load_profile(&workspace()).expect("profile");

        assert_eq!(profile.version, 1);
        assert!(profile.sources.is_empty());
    }

    #[test]
    fn profile_save_load_round_trips() {
        let (_temp, store) = store();
        let workspace = workspace();
        store
            .merge_observations(
                &workspace,
                &[observation(StatisticsObservationScope::TableGlobal)],
            )
            .expect("merge");

        let profile = store.load_profile(&workspace).expect("profile");

        assert_eq!(name_column(&profile).sample_count, 3);
    }

    #[test]
    fn non_table_global_observations_are_ignored() {
        let (_temp, store) = store();
        let workspace = workspace();
        store
            .merge_observations(
                &workspace,
                &[observation(StatisticsObservationScope::Filtered {
                    filter_columns: vec!["status".to_string()],
                })],
            )
            .expect("merge");

        let profile = store.load_profile(&workspace).expect("profile");

        assert!(profile.sources.is_empty());
    }

    #[test]
    fn matching_table_global_observations_replace_prior_snapshot() {
        let (_temp, store) = store();
        let workspace = workspace();
        let observation = observation(StatisticsObservationScope::TableGlobal);
        store
            .merge_observations(&workspace, std::slice::from_ref(&observation))
            .expect("first merge");
        store
            .merge_observations(&workspace, &[observation])
            .expect("second merge");

        let profile = store.load_profile(&workspace).expect("profile");
        let column = name_column(&profile);

        assert_eq!(column.sample_count, 3);
        assert_eq!(column.null_count.as_ref().unwrap().value, 1);
        assert_eq!(column.approx_distinct_count.as_ref().unwrap().value, 2);
    }

    #[test]
    fn source_version_change_replaces_source_tables() {
        let (_temp, store) = store();
        let workspace = workspace();
        let first = observation(StatisticsObservationScope::TableGlobal);
        store
            .merge_observations(&workspace, &[first])
            .expect("first merge");
        let mut second = observation(StatisticsObservationScope::TableGlobal);
        second.source_version = Some("0.2.0".to_string());
        second.table_name = "new_events".to_string();
        store
            .merge_observations(&workspace, &[second])
            .expect("second merge");

        let profile = store.load_profile(&workspace).expect("profile");
        let source = profile.sources.get("local").expect("local source");

        assert_eq!(source.source_version.as_deref(), Some("0.2.0"));
        assert!(!source.tables.contains_key("events"));
        assert!(source.tables.contains_key("new_events"));
    }

    #[test]
    fn schema_signature_mismatch_replaces_old_table_stats() {
        let (_temp, store) = store();
        let workspace = workspace();
        let mut first = observation(StatisticsObservationScope::TableGlobal);
        first.schema_signature = signature(false);
        store
            .merge_observations(&workspace, &[first])
            .expect("first merge");
        store
            .merge_observations(
                &workspace,
                &[observation(StatisticsObservationScope::TableGlobal)],
            )
            .expect("second merge");

        let profile = store.load_profile(&workspace).expect("profile");
        let table = event_table(&profile);
        let column = table.columns.get("name").expect("name column");

        assert_eq!(column.sample_count, 3);
        assert_eq!(table.schema_signature, signature(true));
    }

    #[test]
    fn invalidate_source_removes_persisted_source_statistics() {
        let (_temp, store) = store();
        let workspace = workspace();
        store
            .merge_observations(
                &workspace,
                &[observation(StatisticsObservationScope::TableGlobal)],
            )
            .expect("merge");

        store
            .invalidate_source(
                &workspace,
                &SourceName::parse("local").expect("source name"),
            )
            .expect("invalidate source");

        let profile = store.load_profile(&workspace).expect("profile");
        assert!(!profile.sources.contains_key("local"));
    }

    #[test]
    fn invalidate_source_records_observation_watermark() {
        let (_temp, store) = store();
        let workspace = workspace();
        let source_name = SourceName::parse("local").expect("source name");

        invalidate_source_unlocked_at(&store.layout, &workspace, &source_name, 100)
            .expect("invalidate source");

        let profile =
            load_persisted_profile_unlocked(&store.layout, &workspace).expect("persisted profile");
        assert_eq!(
            profile.source_observation_watermarks.get("local").copied(),
            Some(100)
        );
    }

    #[test]
    fn rebuild_ignores_observations_before_source_watermark() {
        let (_temp, store) = store();
        let workspace = workspace();
        let source_name = SourceName::parse("local").expect("source name");
        invalidate_source_unlocked_at(&store.layout, &workspace, &source_name, 100)
            .expect("invalidate source");

        store
            .rebuild_profile_from_observations(
                &workspace,
                &[observation_record(
                    observation(StatisticsObservationScope::TableGlobal),
                    99,
                )],
                ["local".to_string()],
            )
            .expect("rebuild");

        let profile = store.load_profile(&workspace).expect("profile");
        assert!(!profile.sources.contains_key("local"));

        let mut fresh_observation = observation(StatisticsObservationScope::TableGlobal);
        fresh_observation
            .columns
            .first_mut()
            .expect("sample column")
            .sample_count = 7;
        store
            .rebuild_profile_from_observations(
                &workspace,
                &[
                    observation_record(observation(StatisticsObservationScope::TableGlobal), 99),
                    observation_record(fresh_observation, 101),
                ],
                ["local".to_string()],
            )
            .expect("rebuild with fresh observation");

        let profile = store.load_profile(&workspace).expect("profile");
        assert_eq!(name_column(&profile).sample_count, 7);
    }

    #[test]
    fn invalidate_source_discards_unreadable_profile() {
        let (_temp, store) = store();
        let workspace = workspace();
        let path = store.layout.statistics_profile_file(&workspace);
        std::fs::create_dir_all(path.parent().expect("profile parent")).expect("profile dir");
        std::fs::write(&path, "{not-json").expect("write corrupt profile");

        store
            .invalidate_source(
                &workspace,
                &SourceName::parse("local").expect("source name"),
            )
            .expect("invalidate should tolerate corrupt profile");

        let profile = store.load_profile(&workspace).expect("profile");
        assert!(profile.sources.is_empty());
    }
}
