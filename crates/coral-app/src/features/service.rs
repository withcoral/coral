//! Implements the gRPC `FeatureService` for runtime feature inspection and
//! configuration.

use coral_api::v1::feature_service_server::FeatureService as FeatureServiceApi;
use coral_api::v1::{
    FeatureConfiguredState as ProtoFeatureConfiguredState, FeatureStatus as ProtoFeatureStatus,
    ListFeaturesRequest, ListFeaturesResponse, SetFeatureRequest, SetFeatureResponse,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::{AppError, app_status};
use crate::features::{FeatureConfiguredState, FeatureStatus, FeatureStore, Features};
use crate::transport::{grpc_span, instrument_grpc, request_context};
use crate::workspaces::LocalPrincipalPolicy;

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
    local_principal: LocalPrincipalPolicy,
}

impl FeatureService {
    pub(crate) fn new(
        store: FeatureStore,
        active: Features,
        local_principal: LocalPrincipalPolicy,
    ) -> Self {
        Self {
            store,
            active,
            local_principal,
        }
    }

    fn authorize_local_host<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let principal = request_context(request)?.principal();
        if self.local_principal.is_implicit_owner() && principal.is_local() {
            Ok(())
        } else {
            Err(app_status(AppError::PermissionDenied(
                "runtime feature control is available only to the local host".to_string(),
            )))
        }
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
            self.authorize_local_host(&request)?;
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
            self.authorize_local_host(&request)?;
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
    use crate::identity::{Principal, PrincipalKind};
    use crate::request_context::RequestContext;

    fn authenticated<T>(message: T, principal: Principal) -> Request<T> {
        let mut request = Request::new(message);
        request
            .extensions_mut()
            .insert(RequestContext::new(principal));
        request
    }

    fn local_request<T>(message: T) -> Request<T> {
        authenticated(message, Principal::local())
    }

    fn service_for(config_dir: &std::path::Path, active: Features) -> FeatureService {
        let store = FeatureStore::discover(Some(config_dir.to_path_buf())).expect("feature store");
        FeatureService::new(store, active, LocalPrincipalPolicy::ImplicitOwner)
    }

    fn service_for_policy(
        config_dir: &std::path::Path,
        local_principal: LocalPrincipalPolicy,
    ) -> FeatureService {
        let store = FeatureStore::discover(Some(config_dir.to_path_buf())).expect("feature store");
        FeatureService::new(store, Features::default(), local_principal)
    }

    #[tokio::test]
    async fn list_features_reports_every_registry_entry() {
        let temp = TempDir::new().expect("temp dir");

        let response = service_for(&temp.path().join("config"), Features::default())
            .list_features(local_request(ListFeaturesRequest {}))
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
            .set_feature(local_request(SetFeatureRequest {
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
            .list_features(local_request(ListFeaturesRequest {}))
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

        let response = FeatureService::new(store, active, LocalPrincipalPolicy::ImplicitOwner)
            .list_features(local_request(ListFeaturesRequest {}))
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

        let response = FeatureService::new(store, active, LocalPrincipalPolicy::ImplicitOwner)
            .list_features(local_request(ListFeaturesRequest {}))
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
            .set_feature(local_request(SetFeatureRequest {
                key: "nope".to_string(),
                enabled: true,
            }))
            .await
            .expect_err("unknown feature");

        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("unknown feature 'nope'"));
    }

    #[tokio::test]
    async fn feature_control_requires_an_implicit_local_principal_before_state_access() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("config");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        let original = "invalid = [toml";
        std::fs::write(config_dir.join("config.toml"), original).expect("write config");
        let user = Principal::parse("federated-user", PrincipalKind::User).expect("user");
        let agent = Principal::parse("federated-agent", PrincipalKind::Agent).expect("agent");
        let cases = [
            (LocalPrincipalPolicy::NoLocalPrincipal, Principal::local()),
            (LocalPrincipalPolicy::NoLocalPrincipal, user.clone()),
            (LocalPrincipalPolicy::NoLocalPrincipal, agent.clone()),
            (LocalPrincipalPolicy::ImplicitOwner, user),
            (LocalPrincipalPolicy::ImplicitOwner, agent),
        ];

        for (policy, principal) in cases {
            let service = service_for_policy(&config_dir, policy);
            let list = service
                .list_features(authenticated(ListFeaturesRequest {}, principal.clone()))
                .await
                .expect_err("caller cannot list runtime features");
            assert_local_host_denied(&list);
            for key in ["feedback", "nope"] {
                let set = service
                    .set_feature(authenticated(
                        SetFeatureRequest {
                            key: key.to_string(),
                            enabled: true,
                        },
                        principal.clone(),
                    ))
                    .await
                    .expect_err("caller cannot change runtime features");
                assert_local_host_denied(&set);
            }
        }

        assert_eq!(
            std::fs::read_to_string(config_dir.join("config.toml")).expect("read config"),
            original
        );
    }

    fn assert_local_host_denied(status: &Status) {
        assert_eq!(status.code(), Code::PermissionDenied);
        assert_eq!(
            status.message(),
            "permission denied: runtime feature control is available only to the local host"
        );
    }
}
