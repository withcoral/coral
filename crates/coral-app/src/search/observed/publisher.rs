//! Source-scan observed-values publisher wiring.

#![allow(
    dead_code,
    reason = "observed-values provider substrate is staged before app wiring in the next PR"
)]

use std::collections::HashMap;
use std::sync::Arc;

use coral_engine::{
    EngineExtensions, QuerySource, SourceObservationPublisher, SourceObservationSurfaceKind,
    SourceScanObservation,
};

use crate::bootstrap::AppError;
use crate::search::observed::collector::ObservedValuesCollector;
use crate::search::observed::source_scope::{
    ObservedSourceSurfaceScope, SourceScopeSeed, SurfaceKey, source_surface_scopes,
};
use crate::search::observed::sqlite_queue::{ObservedValuesEpoch, ObservedValuesSurfaceKind};
use crate::search::observed::sqlite_store::SqliteObservedValuesStore;
use crate::search::observed::writer::{
    ObservedValuesTryReserveError, ObservedValuesWrite, ObservedValuesWriter,
    payload_json_with_budget,
};
use crate::search::sqlite_store::SqliteSearchError;
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone)]
pub(crate) struct SearchObservationHandle {
    store: SqliteObservedValuesStore,
    writer: ObservedValuesWriter,
    collector: ObservedValuesCollector,
}

impl SearchObservationHandle {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        let store = SqliteObservedValuesStore::new(layout);
        let writer = ObservedValuesWriter::start(store.clone());
        Self {
            store,
            writer,
            collector: ObservedValuesCollector::default(),
        }
    }

    pub(crate) fn extensions_for(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[QuerySource],
    ) -> EngineExtensions {
        let mut scopes = HashMap::new();
        for source in selected_sources {
            let epoch = match self
                .store
                .capture_epoch(workspace_name, source.source_name())
            {
                Ok(epoch) => epoch,
                Err(error) => {
                    tracing::debug!(
                        workspace = %workspace_name.as_str(),
                        source = %source.source_name(),
                        error = %error,
                        "skipping observed-values publisher for source"
                    );
                    continue;
                }
            };
            for scope in source_surface_scopes(source, SourceScopeSeed::PRE_ACTIVATION) {
                scopes.insert(scope.key(), RegisteredSurface { scope, epoch });
            }
        }
        if scopes.is_empty() {
            return EngineExtensions::default();
        }

        let mut extensions = EngineExtensions::default();
        extensions.source_observation_publishers.push(Arc::new(
            SourceScanObservedValuesPublisher {
                workspace_name: workspace_name.clone(),
                writer: self.writer.clone(),
                collector: self.collector.clone(),
                scopes,
            },
        ));
        extensions
    }

    #[expect(
        dead_code,
        reason = "clear-all observed-values governance is staged before its transport RPC"
    )]
    pub(crate) fn clear_workspace(&self, workspace_name: &WorkspaceName) -> Result<(), AppError> {
        self.store
            .clear_workspace_and_advance_epoch(workspace_name)
            .map_err(|error| observed_values_store_error(&error))
    }

    pub(crate) fn clear_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &str,
    ) -> Result<(), AppError> {
        self.store
            .clear_source_and_advance_epoch(workspace_name, source_name)
            .map_err(|error| observed_values_store_error(&error))
    }
}

fn observed_values_store_error(error: &SqliteSearchError) -> AppError {
    let detail = if error.is_lock_contention() {
        "observed-values SQLite store is locked".to_string()
    } else {
        format!("observed-values SQLite store is unavailable: {error}")
    };
    AppError::Unavailable(detail)
}

struct SourceScanObservedValuesPublisher {
    workspace_name: WorkspaceName,
    writer: ObservedValuesWriter,
    collector: ObservedValuesCollector,
    scopes: HashMap<SurfaceKey, RegisteredSurface>,
}

struct RegisteredSurface {
    scope: ObservedSourceSurfaceScope,
    epoch: ObservedValuesEpoch,
}

impl SourceObservationPublisher for SourceScanObservedValuesPublisher {
    fn publish_source_scan(&self, observation: SourceScanObservation<'_>) {
        self.publish(observation);
    }
}

impl SourceScanObservedValuesPublisher {
    fn publish(&self, observation: SourceScanObservation<'_>) {
        let surface_kind = observed_surface_kind(observation.surface_kind);
        let key = SurfaceKey {
            source_name: observation.source_name.to_string(),
            surface_kind,
            surface_name: observation.surface_name.to_string(),
        };
        let Some(registered) = self.scopes.get(&key) else {
            tracing::debug!(
                workspace = %self.workspace_name.as_str(),
                source = %observation.source_name,
                surface = %observation.surface_name,
                "observed-values source-scan observation did not match a known source surface"
            );
            return;
        };
        let scope = &registered.scope;

        let permit = match self.writer.try_reserve() {
            Ok(permit) => permit,
            Err(ObservedValuesTryReserveError::Full) => {
                tracing::debug!(
                    workspace = %self.workspace_name.as_str(),
                    source = %scope.source_name,
                    source_owner = %scope.owner_source_name,
                    surface = %observation.surface_name,
                    "dropping observed-values source-scan observation because writer queue is full"
                );
                return;
            }
            Err(ObservedValuesTryReserveError::Disconnected) => {
                tracing::debug!(
                    workspace = %self.workspace_name.as_str(),
                    source = %scope.source_name,
                    source_owner = %scope.owner_source_name,
                    surface = %observation.surface_name,
                    "dropping observed-values source-scan observation because writer is stopped"
                );
                return;
            }
        };

        let payload = self.collector.collect_batch(observation.batch);
        if payload.is_empty() {
            return;
        }
        let payload_json = match payload_json_with_budget(
            payload,
            self.collector.budget().job_bytes_limit,
        ) {
            Ok(Some(payload_json)) => payload_json,
            Ok(None) => {
                tracing::debug!(
                    workspace = %self.workspace_name.as_str(),
                    source = %scope.source_name,
                    surface = %observation.surface_name,
                    "dropping observed-values source-scan observation because no candidate fits the serialized job budget"
                );
                return;
            }
            Err(error) => {
                tracing::debug!(
                    workspace = %self.workspace_name.as_str(),
                    source = %scope.source_name,
                    surface = %observation.surface_name,
                    error = %error,
                    "failed to serialize observed-values source-scan observation"
                );
                return;
            }
        };
        permit.send(ObservedValuesWrite {
            workspace_name: self.workspace_name.clone(),
            owner_source_name: scope.owner_source_name.clone(),
            source_name: scope.source_name.clone(),
            source_scope_id: scope.source_scope_id.clone(),
            surface_kind,
            surface_name: observation.surface_name.to_string(),
            payload_json,
            epoch: registered.epoch,
        });
    }
}

fn observed_surface_kind(kind: SourceObservationSurfaceKind) -> ObservedValuesSurfaceKind {
    match kind {
        SourceObservationSurfaceKind::Table => ObservedValuesSurfaceKind::Table,
        SourceObservationSurfaceKind::Function => ObservedValuesSurfaceKind::Function,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use coral_engine::{
        QuerySource, RuntimeSourceComponent, RuntimeSourcePackage, SourceObservationSurfaceKind,
        SourceScanObservation,
    };
    use coral_spec::parse_source_manifest_yaml;
    use serde_json::json;
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::SearchObservationHandle;
    use crate::search::observed::source_scope::{SourceScopeSeed, source_surface_scopes};
    use crate::search::observed::sqlite_queue::{
        ObservedValueCandidate, ObservedValuesQueuePayload,
    };
    use crate::search::observed::sqlite_store::SqliteObservedValuesStore;
    use crate::search::observed::writer::payload_json_with_budget;
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;

    #[test]
    fn opaque_scope_seed_is_deterministic() {
        let source = http_query_source("/issues");

        let first_scope = source_surface_scopes(&source, SourceScopeSeed::PRE_ACTIVATION)
            .pop()
            .expect("first scope");
        let second_scope = source_surface_scopes(&source, SourceScopeSeed::PRE_ACTIVATION)
            .pop()
            .expect("second scope");

        assert_eq!(first_scope.source_scope_id, second_scope.source_scope_id);
    }

    #[test]
    fn opaque_runtime_contract_and_credential_inputs_partition_scopes() {
        let source = http_query_source("/issues");
        let first_scope = source_surface_scopes(
            &source,
            SourceScopeSeed::new("runtime-contract-a", Uuid::from_u128(1)),
        )
        .pop()
        .expect("first scope");
        let contract_changed = source_surface_scopes(
            &source,
            SourceScopeSeed::new("runtime-contract-b", Uuid::from_u128(1)),
        )
        .pop()
        .expect("contract scope");
        let credential_changed = source_surface_scopes(
            &source,
            SourceScopeSeed::new("runtime-contract-a", Uuid::from_u128(2)),
        )
        .pop()
        .expect("credential scope");

        assert_ne!(
            first_scope.source_scope_id,
            contract_changed.source_scope_id
        );
        assert_ne!(
            first_scope.source_scope_id,
            credential_changed.source_scope_id
        );
    }

    #[test]
    fn publisher_does_not_store_source_secrets_or_sensitive_values() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let handle = SearchObservationHandle::new(layout.clone());
        let source = secret_input_query_source();
        let extensions = handle.extensions_for(&workspace, &[source]);
        let publisher = extensions
            .source_observation_publishers
            .first()
            .expect("publisher");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("api_token", DataType::Utf8, false),
                Field::new("note", DataType::Utf8, false),
                Field::new("metadata", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["Grace"])),
                Arc::new(StringArray::from(vec!["ghp_supersecret"])),
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(StringArray::from(vec![
                    r#"{"project":"coral","api_key":"literal-secret"}"#,
                ])),
            ],
        )
        .expect("batch");

        publisher.publish_source_scan(SourceScanObservation {
            source_name: "github",
            surface_kind: SourceObservationSurfaceKind::Table,
            surface_name: "issues",
            batch: &batch,
        });

        let payloads = wait_for_payloads(&layout, &workspace).join("\n");
        assert!(payloads.contains("Grace"));
        assert!(payloads.contains("hello"));
        assert!(payloads.contains("project"));
        assert!(payloads.contains("coral"));
        assert!(!payloads.contains("literal-secret"));
        assert!(!payloads.contains("ghp_supersecret"));
    }

    #[test]
    fn publisher_preserves_owner_and_query_schema_for_multi_surface_v4_package() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let handle = SearchObservationHandle::new(layout.clone());
        let source = multi_surface_v4_query_source();
        let extensions = handle.extensions_for(&workspace, &[source]);
        let publisher = extensions
            .source_observation_publishers
            .first()
            .expect("publisher");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "title",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec!["Fix the bug"]))],
        )
        .expect("batch");

        for source_name in ["github_v4_rest", "github_v4_mcp"] {
            publisher.publish_source_scan(SourceScanObservation {
                source_name,
                surface_kind: SourceObservationSurfaceKind::Table,
                surface_name: "list_issues",
                batch: &batch,
            });
        }

        let identities = wait_for_source_identities(&layout, &workspace, 2);
        assert_eq!(
            identities,
            [
                (
                    "github_v4".to_string(),
                    "github_v4_rest".to_string(),
                    "list_issues".to_string(),
                ),
                (
                    "github_v4".to_string(),
                    "github_v4_mcp".to_string(),
                    "list_issues".to_string(),
                ),
            ]
        );

        let store = SqliteObservedValuesStore::new(layout);
        store
            .clear_source_and_advance_epoch(&workspace, "github_v4")
            .expect("clear logical source owner");
        assert_eq!(
            store
                .pending_queue_job_count(&workspace)
                .expect("queue count"),
            0
        );
    }

    #[test]
    fn payload_budget_keeps_largest_serialized_prefix() {
        let payload = ObservedValuesQueuePayload {
            values: vec![
                observed_candidate("Ada"),
                observed_candidate("Grace"),
                observed_candidate("Katherine"),
            ],
        };
        let two_value_json = serde_json::to_string(&json!({
            "values": [
                {
                    "column_name": "name",
                    "display_value": "Ada",
                    "search_text": "ada",
                    "value_key": "key"
                },
                {
                    "column_name": "name",
                    "display_value": "Grace",
                    "search_text": "grace",
                    "value_key": "key"
                }
            ]
        }))
        .expect("json");

        let payload_json =
            payload_json_with_budget(payload, two_value_json.len()).expect("payload budget");
        let payload_json = payload_json.expect("budget should keep two values");
        let decoded: ObservedValuesQueuePayload =
            serde_json::from_str(&payload_json).expect("payload json");
        let display_values = decoded
            .values
            .iter()
            .map(|candidate| candidate.display_value.as_str())
            .collect::<Vec<_>>();

        assert_eq!(display_values, ["Ada", "Grace"]);
    }

    fn http_query_source(path: &str) -> QuerySource {
        let yaml = format!(
            r"
dsl_version: 3
name: github
version: 0.1.0
backend: http
base_url: https://api.github.com
tables:
  - name: issues
    description: Issues
    request:
      path: {path}
    columns:
      - name: title
        type: Utf8
"
        );
        let manifest = parse_source_manifest_yaml(&yaml).expect("manifest");
        QuerySource::from_manifest(&manifest, BTreeMap::new(), BTreeMap::new())
    }

    fn secret_input_query_source() -> QuerySource {
        let yaml = r"
dsl_version: 3
name: github
version: 0.1.0
backend: http
base_url: https://api.github.com
inputs:
  api_key:
    kind: secret
    required: true
tables:
  - name: issues
    description: Issues
    request:
      path: /issues
    columns:
      - name: name
        type: Utf8
      - name: api_token
        type: Utf8
      - name: note
        type: Utf8
";
        let manifest = parse_source_manifest_yaml(yaml).expect("manifest");
        QuerySource::from_manifest(
            &manifest,
            BTreeMap::new(),
            BTreeMap::from([("api_key".to_string(), "literal-secret".to_string())]),
        )
    }

    fn multi_surface_v4_query_source() -> QuerySource {
        // Multi-surface v4 sources reach the engine through this backend-ready
        // package shape: one installed owner with one schema per component.
        QuerySource::from_runtime_components(
            RuntimeSourcePackage {
                source_name: "github_v4".to_string(),
                authored_version: None,
                description: String::new(),
                declared_inputs: Vec::new(),
                test_queries: Vec::new(),
                components: vec![
                    http_component("github_v4_rest"),
                    http_component("github_v4_mcp"),
                ],
            },
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("multi-surface query source")
    }

    fn http_component(source_name: &str) -> RuntimeSourceComponent {
        let yaml = format!(
            r"
dsl_version: 3
name: {source_name}
version: 0.1.0
backend: http
base_url: https://api.github.com
tables:
  - name: list_issues
    description: Issues
    request:
      path: /issues
    columns:
      - name: title
        type: Utf8
"
        );
        let manifest = parse_source_manifest_yaml(&yaml).expect("component manifest");
        RuntimeSourceComponent::Http(manifest.as_http().expect("HTTP component").clone())
    }

    fn wait_for_payloads(layout: &AppStateLayout, workspace: &WorkspaceName) -> Vec<String> {
        let store = SqliteObservedValuesStore::new(layout.clone());
        for _ in 0..100 {
            let payloads = store.queue_payloads(workspace).expect("payloads");
            if !payloads.is_empty() {
                return payloads;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("observed-values writer did not enqueue payload");
    }

    fn wait_for_source_identities(
        layout: &AppStateLayout,
        workspace: &WorkspaceName,
        expected_count: usize,
    ) -> Vec<(String, String, String)> {
        let store = SqliteObservedValuesStore::new(layout.clone());
        for _ in 0..100 {
            let identities = store
                .queue_source_identities(workspace)
                .expect("queue source identities");
            if identities.len() == expected_count {
                return identities;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("observed-values writer did not enqueue expected source identities");
    }

    fn observed_candidate(display_value: &str) -> ObservedValueCandidate {
        ObservedValueCandidate {
            column_name: "name".to_string(),
            display_value: display_value.to_string(),
            search_text: display_value.to_ascii_lowercase(),
            value_key: "key".to_string(),
        }
    }
}
