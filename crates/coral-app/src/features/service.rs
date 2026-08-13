//! Implements the gRPC `FeatureService` for runtime feature inspection and
//! configuration.

use coral_api::v1::feature_service_server::FeatureService as FeatureServiceApi;
use coral_api::v1::{
    FeatureConfiguredState as ProtoFeatureConfiguredState, FeatureStatus as ProtoFeatureStatus,
    ListFeaturesRequest, ListFeaturesResponse, SetFeatureRequest, SetFeatureResponse,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::features::{FeatureConfiguredState, FeatureStatus, FeatureStore, Features};
use crate::transport::{grpc_span, instrument_grpc};

/// Serves the runtime feature registry over gRPC.
///
/// Runtime features are read once, when the server starts, so this service
/// reports two different truths, each from one source: `enabled` from local
/// config, read on every call, and `active` from the state this server actually
/// booted with, launch flags included.
#[derive(Clone)]
pub(crate) struct FeatureService {
    store: FeatureStore,
    active: Features,
}

impl FeatureService {
    pub(crate) fn new(store: FeatureStore, active: Features) -> Self {
        Self { store, active }
    }

    fn status_to_proto(&self, status: &FeatureStatus) -> ProtoFeatureStatus {
        ProtoFeatureStatus {
            key: status.key.to_string(),
            description: status.description.to_string(),
            default_enabled: status.default_enabled,
            configured: configured_state_to_proto(status.configured).into(),
            enabled: status.enabled,
            active: self.active.enabled(status.feature),
        }
    }
}

#[tonic::async_trait]
impl FeatureServiceApi for FeatureService {
    async fn list_features(
        &self,
        request: Request<ListFeaturesRequest>,
    ) -> Result<Response<ListFeaturesResponse>, Status> {
        let span = grpc_span(&request);
        instrument_grpc(span, async move {
            let features = self
                .store
                .statuses()
                .map_err(app_status)?
                .iter()
                .map(|status| self.status_to_proto(status))
                .collect();
            Ok(Response::new(ListFeaturesResponse { features }))
        })
        .await
    }

    async fn set_feature(
        &self,
        request: Request<SetFeatureRequest>,
    ) -> Result<Response<SetFeatureResponse>, Status> {
        let span = grpc_span(&request);
        instrument_grpc(span, async move {
            let request = request.into_inner();
            if request.enabled {
                self.store.enable(&request.key).map_err(app_status)?;
            } else {
                self.store.disable(&request.key).map_err(app_status)?;
            }
            let statuses = self.store.statuses().map_err(app_status)?;
            let status = statuses
                .iter()
                .find(|status| status.key == request.key)
                .ok_or_else(|| {
                    Status::internal("persisted feature is missing from the registry")
                })?;
            Ok(Response::new(SetFeatureResponse {
                feature: Some(self.status_to_proto(status)),
            }))
        })
        .await
    }
}

fn configured_state_to_proto(state: FeatureConfiguredState) -> ProtoFeatureConfiguredState {
    match state {
        FeatureConfiguredState::Default => ProtoFeatureConfiguredState::Default,
        FeatureConfiguredState::Enabled => ProtoFeatureConfiguredState::Enabled,
        FeatureConfiguredState::Disabled => ProtoFeatureConfiguredState::Disabled,
        FeatureConfiguredState::InvalidValue => ProtoFeatureConfiguredState::InvalidValue,
        FeatureConfiguredState::InvalidContainer => ProtoFeatureConfiguredState::InvalidContainer,
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use tonic::Code;

    use super::*;
    use crate::features::{Feature, FeatureOverrides};

    fn service_for(config_dir: &std::path::Path, active: Features) -> FeatureService {
        let store = FeatureStore::discover(Some(config_dir.to_path_buf())).expect("feature store");
        FeatureService::new(store, active)
    }

    #[tokio::test]
    async fn list_features_reports_every_registry_entry() {
        let temp = TempDir::new().expect("temp dir");

        let response = service_for(&temp.path().join("config"), Features::default())
            .list_features(Request::new(ListFeaturesRequest {}))
            .await
            .expect("list features")
            .into_inner();

        let keys = response
            .features
            .iter()
            .map(|feature| feature.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec!["database_sources", "feedback", "observed_values_search"]
        );
        assert!(response.features.iter().all(|feature| {
            !feature.enabled
                && !feature.active
                && feature.configured == i32::from(ProtoFeatureConfiguredState::Default)
        }));
    }

    #[tokio::test]
    async fn set_feature_persists_the_override_and_reports_the_pending_restart() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("config");
        // A server that booted before the override still runs with the feature off.
        let service = service_for(&config_dir, Features::default());

        let response = service
            .set_feature(Request::new(SetFeatureRequest {
                key: "feedback".to_string(),
                enabled: true,
            }))
            .await
            .expect("set feature")
            .into_inner();

        let feature = response.feature.expect("feature");
        assert!(feature.enabled);
        assert!(!feature.active);
        assert_eq!(
            feature.configured,
            i32::from(ProtoFeatureConfiguredState::Enabled)
        );
        let config = std::fs::read_to_string(config_dir.join("config.toml")).expect("config file");
        assert!(config.contains("[features]"));
        assert!(config.contains("feedback = true"));
    }

    #[tokio::test]
    async fn list_features_reports_the_running_state_separately_from_config() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("config");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        // Config says on; this server booted with it off.
        std::fs::write(
            config_dir.join("config.toml"),
            "[features]\nobserved_values_search = true\n",
        )
        .expect("write config");

        let response = service_for(&config_dir, Features::default())
            .list_features(Request::new(ListFeaturesRequest {}))
            .await
            .expect("list features")
            .into_inner();

        let feature = response
            .features
            .iter()
            .find(|feature| feature.key == "observed_values_search")
            .expect("observed values search status");
        assert!(feature.enabled);
        assert!(!feature.active);
    }

    #[tokio::test]
    async fn list_features_reports_active_state_from_the_boot_snapshot() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("config");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        std::fs::write(
            config_dir.join("config.toml"),
            "[features]\nfeedback = true\n",
        )
        .expect("write config");
        let store = FeatureStore::discover(Some(config_dir.clone())).expect("feature store");
        let active = store
            .load_with_overrides(&FeatureOverrides::default())
            .expect("boot features");
        assert!(active.enabled(Feature::Feedback));

        let response = FeatureService::new(store, active)
            .list_features(Request::new(ListFeaturesRequest {}))
            .await
            .expect("list features")
            .into_inner();

        let feature = response
            .features
            .iter()
            .find(|feature| feature.key == "feedback")
            .expect("feedback status");
        assert!(feature.enabled);
        assert!(feature.active);
    }

    #[tokio::test]
    async fn list_features_ignores_launch_flags_when_resolving_config() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("config");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        std::fs::write(
            config_dir.join("config.toml"),
            "[features]\nfeedback = true\n",
        )
        .expect("write config");
        let store = FeatureStore::discover(Some(config_dir)).expect("feature store");
        // This server booted with `--disable-feedback`, so it runs with the
        // feature off while config says on.
        let mut overrides = FeatureOverrides::default();
        overrides.set(Feature::Feedback, false);
        let active = store
            .load_with_overrides(&overrides)
            .expect("boot features");
        assert!(!active.enabled(Feature::Feedback));

        let response = FeatureService::new(store, active)
            .list_features(Request::new(ListFeaturesRequest {}))
            .await
            .expect("list features")
            .into_inner();

        // `enabled` reports config, which a restart without the flag applies.
        let feature = response
            .features
            .iter()
            .find(|feature| feature.key == "feedback")
            .expect("feedback status");
        assert!(feature.enabled);
        assert!(!feature.active);
    }

    #[tokio::test]
    async fn set_feature_rejects_an_unknown_key() {
        let temp = TempDir::new().expect("temp dir");

        let status = service_for(&temp.path().join("config"), Features::default())
            .set_feature(Request::new(SetFeatureRequest {
                key: "nope".to_string(),
                enabled: true,
            }))
            .await
            .expect_err("unknown feature");

        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("unknown feature 'nope'"));
    }
}
