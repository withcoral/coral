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
use crate::identity::Principal;
use crate::transport::{grpc_span, instrument_grpc, request_context};
use crate::workspaces::authorization::WorkspaceAuthorizer;

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
    authorizer: WorkspaceAuthorizer,
}

impl FeatureService {
    pub(crate) const fn new(
        store: FeatureStore,
        active: Features,
        authorizer: WorkspaceAuthorizer,
    ) -> Self {
        Self {
            store,
            active,
            authorizer,
        }
    }

    /// Settles who may change host-global feature state.
    ///
    /// Features configure the machine this server runs on rather than any one
    /// workspace, so no workspace role can entitle a caller to change them and
    /// a shared deployment has no superuser to entrust them to. The rule
    /// itself lives with its siblings on the authorizer; this is only where
    /// its refusal becomes a gRPC status.
    fn authorize_host_global(&self, principal: &Principal) -> Result<(), Status> {
        self.authorizer
            .authorize_host_global(principal)
            .map_err(app_status)
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
        let principal = request_context(&request)?.principal().clone();
        instrument_grpc(span, async move {
            // Reading is open to every caller this deployment admits: the
            // response carries the same feature keys, descriptions, and
            // enabled state for all of them, and a page that cannot read it
            // cannot explain why its switches do nothing. Changing that state
            // is what stays with the host. Settled before the feature store is
            // read at all, so an unadmitted caller learns nothing from it, not
            // even whether its config file parses.
            self.authorizer.admit(&principal).map_err(app_status)?;
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
        let principal = request_context(&request)?.principal().clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            // Settled before the key is looked up, so a refused caller cannot
            // use the registry's own complaint to enumerate feature keys, and
            // writes nothing to the host's config file.
            self.authorize_host_global(&principal)?;
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
    use std::sync::Arc;

    use tempfile::TempDir;
    use tonic::Code;

    use super::*;
    use crate::features::{Feature, FeatureOverrides};
    use crate::identity::PrincipalKind;
    use crate::request_context::RequestContext;
    use crate::state::db::{CoralDb, ResolvedDatabaseConfig};
    use crate::workspaces::authorization::LocalPrincipalPolicy;

    /// An authorizer over a database that was never migrated, under the policy
    /// a single-user deployment resolves. Every membership query against it
    /// fails, so a decision this service still reaches was reached without one.
    async fn local_authorizer(temp: &TempDir) -> WorkspaceAuthorizer {
        authorizer_with(temp, LocalPrincipalPolicy::ImplicitOwner).await
    }

    async fn authorizer_with(temp: &TempDir, policy: LocalPrincipalPolicy) -> WorkspaceAuthorizer {
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite");
        WorkspaceAuthorizer::with_local_principal_policy(Arc::new(db), policy)
    }

    fn service_for(
        config_dir: &std::path::Path,
        active: Features,
        authorizer: WorkspaceAuthorizer,
    ) -> FeatureService {
        let store = FeatureStore::discover(Some(config_dir.to_path_buf())).expect("feature store");
        FeatureService::new(store, active, authorizer)
    }

    fn request<T>(message: T, principal: Principal) -> Request<T> {
        let mut request = Request::new(message);
        request
            .extensions_mut()
            .insert(RequestContext::new(principal));
        request
    }

    fn local<T>(message: T) -> Request<T> {
        request(message, Principal::local())
    }

    #[tokio::test]
    async fn list_features_reports_every_registry_entry() {
        let temp = TempDir::new().expect("temp dir");
        let authorizer = local_authorizer(&temp).await;

        let response = service_for(&temp.path().join("config"), Features::default(), authorizer)
            .list_features(local(ListFeaturesRequest {}))
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
        let authorizer = local_authorizer(&temp).await;
        // A server that booted before the override still runs with the feature off.
        let service = service_for(&config_dir, Features::default(), authorizer);

        let response = service
            .set_feature(local(SetFeatureRequest {
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
        let authorizer = local_authorizer(&temp).await;

        let response = service_for(&config_dir, Features::default(), authorizer)
            .list_features(local(ListFeaturesRequest {}))
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

        let response = FeatureService::new(store, active, local_authorizer(&temp).await)
            .list_features(local(ListFeaturesRequest {}))
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

        let response = FeatureService::new(store, active, local_authorizer(&temp).await)
            .list_features(local(ListFeaturesRequest {}))
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
        let authorizer = local_authorizer(&temp).await;

        let status = service_for(&temp.path().join("config"), Features::default(), authorizer)
            .set_feature(local(SetFeatureRequest {
                key: "nope".to_string(),
                enabled: true,
            }))
            .await
            .expect_err("unknown feature");

        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("unknown feature 'nope'"));
    }

    /// Changing feature state has no workspace to scope to, so it reaches only
    /// the local principal, and only where the deployment admits it: a
    /// federated caller is refused even under the policy that hands
    /// `coral:local` everything, and `coral:local` itself is refused on a
    /// shared deployment.
    ///
    /// The probes are what make each refusal an absence rather than an error
    /// code: `nope` is a key the registry would reject, and the config file is
    /// unparseable, so a caller that reached the store at all would choke on
    /// it. Every refusal answers `PermissionDenied` instead and the file keeps
    /// the bytes it started with.
    #[tokio::test]
    async fn changing_feature_state_reaches_only_a_local_principal_the_deployment_admits() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("config");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        let config_file = config_dir.join("config.toml");
        std::fs::write(&config_file, "[features\n").expect("write unparseable config");
        let someone = Principal::parse("someone", PrincipalKind::User).expect("federated user");
        let single_user = service_for(
            &config_dir,
            Features::default(),
            local_authorizer(&temp).await,
        );
        let shared = service_for(
            &config_dir,
            Features::default(),
            authorizer_with(&temp, LocalPrincipalPolicy::NoLocalPrincipal).await,
        );

        for (service, principal) in [
            (&single_user, someone.clone()),
            (&shared, someone),
            (&shared, Principal::local()),
        ] {
            assert_eq!(
                service
                    .set_feature(request(
                        SetFeatureRequest {
                            key: "nope".to_string(),
                            enabled: true,
                        },
                        principal,
                    ))
                    .await
                    .expect_err("this caller mutates nothing")
                    .code(),
                Code::PermissionDenied
            );
        }
        assert_eq!(
            std::fs::read_to_string(&config_file).expect("config file"),
            "[features\n",
            "a refused caller must not have rewritten the host's config"
        );
    }

    /// Reading feature state is the half that is not host-global: the page
    /// that shows the switches has to say which ones are on, so every caller
    /// this deployment admits reaches it. Hitting the unparseable file is what
    /// proves the read was attempted rather than refused early — and the
    /// principal the deployment does not admit is still turned away before it.
    #[tokio::test]
    async fn listing_feature_state_reaches_every_caller_the_deployment_admits() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("config");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        std::fs::write(config_dir.join("config.toml"), "[features\n")
            .expect("write unparseable config");
        let someone = Principal::parse("someone", PrincipalKind::User).expect("federated user");
        let single_user = service_for(
            &config_dir,
            Features::default(),
            local_authorizer(&temp).await,
        );
        let shared = service_for(
            &config_dir,
            Features::default(),
            authorizer_with(&temp, LocalPrincipalPolicy::NoLocalPrincipal).await,
        );

        for (service, principal) in [
            (&single_user, someone.clone()),
            (&single_user, Principal::local()),
            (&shared, someone),
        ] {
            assert_eq!(
                service
                    .list_features(request(ListFeaturesRequest {}, principal))
                    .await
                    .expect_err("an admitted caller reaches the file")
                    .code(),
                Code::Internal
            );
        }
        assert_eq!(
            shared
                .list_features(local(ListFeaturesRequest {}))
                .await
                .expect_err("a shared deployment admits the local principal to nothing")
                .code(),
            Code::PermissionDenied
        );
    }
}
