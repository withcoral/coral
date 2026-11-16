//! Native gRPC projection: reachability here, current authority in the domain owners.
mod accounts;
mod activity;
mod agents;
mod authored_tools;
mod memberships;
mod organization_memberships;
mod organization_workspaces;
mod policies;
mod private_tools;
mod providers;
mod releases;
mod secret_stores;
mod shared_tools;
mod usage_data;
mod workspaces;
use std::sync::Arc;

use crate::coral::{internal::v1 as internal, management::v1 as management};
use coral_auth::{PrincipalAuthentication, PrivateAudience};
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId, WorkspaceModule};
use tonic::{Request, Response, Status};

/// Application-constructed domain modules, sharing the application's state and identities.
/// The adapter never opens storage or constructs a replacement domain graph.
pub struct Dependencies {
    /// Organization Membership inspection.
    pub organization: coral_workspace::OrganizationModule,
    /// Workspace and Organization authority.
    pub workspaces: WorkspaceModule,
    /// Provider setup, publication, and lifecycle.
    pub providers: coral_provider::ProviderModule,
    /// Provider publication, removal, and rename, one owned transaction each.
    pub provider_workflow: coral_administration::ProviderWorkflow,
    /// Workspace creation with its Built-in Provider, in one owned transaction.
    pub workspace_workflow: coral_administration::WorkspaceWorkflow,
    /// Provider readiness inspection.
    /// Discovery supplies current policy and Tool readiness evidence.
    pub provider_readiness: coral_search::UniversalSearchModule,
    /// Tool access rules.
    pub policies: coral_policy::ToolPolicyModule,
    /// Shared credential and authorization workflows; absent when unavailable.
    pub accounts: Option<coral_provider::account::AccountModule>,
    /// Retained execution inspection and summary queries.
    pub runs: coral_run::RunModule,
    /// Task inspection.
    pub tasks: coral_task::TaskModule,
    /// Current Private Tool inspection: the caller's own, and an Agent's for an Owner.
    pub private_tools: coral_tools::PrivateToolInspection,
    /// Private Tool deletion, the caller's own and an Agent's for an Owner,
    /// which removes the Private Tool's exact rule and tells Universal Search.
    pub private_tool_workflow: coral_administration::PrivateToolWorkflow,
    /// The Workspace's Shared Tools: reads.
    pub shared_tools: coral_tools::SharedToolModule,
    /// Shared Tool publishing and removal, which tell Universal Search.
    pub shared_tool_workflow: coral_administration::SharedToolWorkflow,
    /// Atomic membership and Agent removal, retaining execution history.
    pub access_lifecycle: coral_administration::AccessLifecycleModule,
    /// The newest published release; absent when the operator turned the check off.
    pub releases: Option<coral_release::ReleaseCheck>,
    /// The deployment's usage data; absent where the application composes none.
    pub usage: Option<coral_usage::UsageData>,
    /// The Secret Stores the operator defined and their grants to Workspaces.
    pub secret_stores: coral_administration::SecretStoreAdministration,
}

/// Resolved transport context supplied by application composition.
pub struct TransportContext {
    /// Principal authentication. With [`PrincipalAuthentication::local`] the
    /// application must bind to loopback.
    pub authentication: PrincipalAuthentication,
    /// Canonical, validated MCP origin, published once after the companion binds.
    /// Until publication, Workspace responses contain an empty MCP endpoint.
    pub mcp_origin: Arc<std::sync::OnceLock<String>>,
    /// Application-owned readiness calculation, including caching and bounded waiting.
    pub readiness: crate::ReadinessProbe,
    /// True once the application stops the listener; a dropped sender reads
    /// the same. A stream ends on it, so no client holds the listener open.
    pub stopping: tokio::sync::watch::Receiver<bool>,
}

/// Constructs the complete private route set without binding a listener.
///
/// Owns service registration and protocol message limits. Requests enforce private
/// audience reachability, then delegate current resource authority to the injected
/// domain modules. Failures become bounded statuses, never raw domain error text.
#[must_use]
pub fn routes(dependencies: Dependencies, context: TransportContext) -> tonic::service::Routes {
    let services = GrpcServices {
        organization: dependencies.organization,
        authentication: context.authentication,
        mcp_origin: context.mcp_origin,
        stopping: context.stopping,
        workspaces: dependencies.workspaces,
        providers: dependencies.providers,
        provider_workflow: dependencies.provider_workflow,
        workspace_workflow: dependencies.workspace_workflow,
        provider_readiness: dependencies.provider_readiness,
        policies: dependencies.policies,
        accounts: dependencies.accounts,
        tasks: dependencies.tasks,
        private_tools: dependencies.private_tools,
        private_tool_workflow: dependencies.private_tool_workflow,
        shared_tools: dependencies.shared_tools,
        shared_tool_workflow: dependencies.shared_tool_workflow,
        runs: dependencies.runs,
        access_lifecycle: dependencies.access_lifecycle,
        releases: dependencies.releases,
        usage: dependencies.usage,
        secret_stores: dependencies.secret_stores,
    };
    let mut routes = tonic::service::Routes::default()
        .add_service(
            management::activity_service_server::ActivityServiceServer::new(services.clone())
                // Exact retained output and binary Invocation values exceed tonic's 4 MiB default.
                .max_encoding_message_size(128 << 20),
        )
        .add_service(
            management::organization_membership_service_server::OrganizationMembershipServiceServer::new(services.clone()),
        )
        .add_service(
            management::organization_workspace_service_server::OrganizationWorkspaceServiceServer::new(services.clone()),
        )
        .add_service(
            management::provider_setup_service_server::ProviderSetupServiceServer::new(
                services.clone(),
            )
            .max_decoding_message_size(coral_provider::OPENAPI_DOCUMENT_MAX_BYTES + 64 * 1024),
        )
        .add_service(
            management::provider_service_server::ProviderServiceServer::new(services.clone()),
        )
        .add_service(
            management::provider_account_service_server::ProviderAccountServiceServer::new(
                services.clone(),
            ),
        )
        .add_service(
            management::tool_policy_service_server::ToolPolicyServiceServer::new(
                services.clone(),
            ),
        )
        .add_service(management::agent_service_server::AgentServiceServer::new(
            services.clone(),
        ))
        .add_service(
            management::workspace_membership_service_server::WorkspaceMembershipServiceServer::new(
                services.clone(),
            ),
        )
        .add_service(
            management::workspace_service_server::WorkspaceServiceServer::new(services.clone()),
        )
        .add_service(management::release_service_server::ReleaseServiceServer::new(
            services.clone(),
        ))
        .add_service(
            management::usage_data_service_server::UsageDataServiceServer::new(services.clone()),
        )
        .add_service(
            management::secret_store_service_server::SecretStoreServiceServer::new(
                services.clone(),
            ),
        )
        .add_service(
            management::private_tool_service_server::PrivateToolServiceServer::new(
                services.clone(),
            ),
        )
        .add_service(
            management::shared_tool_service_server::SharedToolServiceServer::new(services.clone()),
        )
        .add_service(
            management::agent_registration_service_server::AgentRegistrationServiceServer::new(
                services.clone(),
            ),
        )
        .add_service(management::task_service_server::TaskServiceServer::new(
            services.clone(),
        ))
        .add_service(internal::task_service_server::TaskServiceServer::new(services));
    let dispatch = routes.axum_router_mut();
    *dispatch = coral_telemetry::trace_grpc_routes(dispatch.clone());
    // Health probes are added after tracing, so they open no span.
    routes.add_service(tonic_health::pb::health_server::HealthServer::new(
        crate::health::AggregateHealthService::new(context.readiness),
    ))
}

#[derive(Clone)]
struct GrpcServices {
    organization: coral_workspace::OrganizationModule,
    authentication: PrincipalAuthentication,
    mcp_origin: Arc<std::sync::OnceLock<String>>,
    stopping: tokio::sync::watch::Receiver<bool>,
    workspaces: WorkspaceModule,
    providers: coral_provider::ProviderModule,
    provider_workflow: coral_administration::ProviderWorkflow,
    workspace_workflow: coral_administration::WorkspaceWorkflow,
    provider_readiness: coral_search::UniversalSearchModule,
    policies: coral_policy::ToolPolicyModule,
    accounts: Option<coral_provider::account::AccountModule>,
    tasks: coral_task::TaskModule,
    private_tools: coral_tools::PrivateToolInspection,
    private_tool_workflow: coral_administration::PrivateToolWorkflow,
    shared_tools: coral_tools::SharedToolModule,
    shared_tool_workflow: coral_administration::SharedToolWorkflow,
    runs: coral_run::RunModule,
    access_lifecycle: coral_administration::AccessLifecycleModule,
    releases: Option<coral_release::ReleaseCheck>,
    usage: Option<coral_usage::UsageData>,
    secret_stores: coral_administration::SecretStoreAdministration,
}

impl GrpcServices {
    async fn task<T>(
        &self,
        request: &Request<T>,
        name: &str,
        audience: PrivateAudience,
    ) -> Result<Response<internal::Task>, Status> {
        let (workspace, id) = resource(name, "tasks")?;
        let id = coral_task::TaskId::parse(id)
            .map_err(|_error| Status::invalid_argument("invalid Task name"))?;
        let principal = self.principal(request, Some(&workspace), audience).await?;
        let task =
            self.tasks
                .get(&principal, &workspace, &id)
                .await
                .map_err(|error| match error {
                    coral_task::Error::TaskNotFound
                    | coral_task::Error::WorkspaceNotFound
                    | coral_task::Error::InteractionNotFound => {
                        Status::not_found("resource not found")
                    }
                    coral_task::Error::PermissionDenied(_) => {
                        Status::permission_denied("permission denied")
                    }
                    coral_task::Error::InvalidInput(_) => {
                        Status::invalid_argument("invalid Task request")
                    }
                    coral_task::Error::TaskClosed => Status::failed_precondition("Task closed"),
                    coral_task::Error::SnapshotTooLarge
                    | coral_task::Error::Search(_)
                    | coral_task::Error::Tools(_)
                    | coral_task::Error::Run(_)
                    | coral_task::Error::Clock
                    | coral_task::Error::IdentifierIssuance(_)
                    | coral_task::Error::Workspace(_)
                    | coral_task::Error::CorruptData(_)
                    | coral_task::Error::Database(_) => Status::unavailable("resource unavailable"),
                })?;
        Ok(Response::new(internal::Task {
            name: name.to_owned(),
            intent: task.intent().as_str().to_owned(),
        }))
    }

    async fn principal<T>(
        &self,
        request: &Request<T>,
        workspace: Option<&WorkspaceId>,
        allowed: PrivateAudience,
    ) -> Result<AuthenticatedPrincipal, Status> {
        let audience = single_header(request.metadata(), "coral-audience")?
            .and_then(PrivateAudience::parse)
            .ok_or_else(|| Status::unauthenticated("private audience required"))?;
        if audience != allowed {
            return Err(Status::permission_denied(
                "method is not reachable by this audience",
            ));
        }
        let bearer = single_header(request.metadata(), "authorization")?
            .map(|header| {
                header
                    .split_once(' ')
                    .filter(|(scheme, token)| {
                        scheme.eq_ignore_ascii_case("Bearer")
                            && !token.is_empty()
                            && !token.contains(char::is_whitespace)
                    })
                    .map(|(_, token)| token)
                    .ok_or_else(|| Status::unauthenticated("invalid authentication metadata"))
            })
            .transpose()?;
        // An opaque Registration has one fixed access surface: mcp. The reef
        // audience authenticates Users only, whatever the caller's metadata.
        match audience {
            PrivateAudience::Mcp => {
                self.authentication
                    .authenticate_scoped(
                        workspace.ok_or_else(|| Status::invalid_argument("Workspace required"))?,
                        bearer,
                        audience.as_str(),
                    )
                    .await
            }
            PrivateAudience::Reef => {
                self.authentication
                    .authenticate_user(bearer, audience.as_str())
                    .await
            }
        }
        .map_err(authentication_error)
    }
}

fn authentication_error(error: coral_auth::AuthenticationError) -> Status {
    match error {
        // Neither entry point lets the credential fix the Workspace.
        coral_auth::AuthenticationError::Unauthenticated
        | coral_auth::AuthenticationError::WorkspaceUrlRequired => {
            Status::unauthenticated("authentication required")
        }
        coral_auth::AuthenticationError::Unavailable => {
            Status::unavailable("authentication unavailable")
        }
    }
}

fn single_header<'a>(
    metadata: &'a tonic::metadata::MetadataMap,
    name: &'static str,
) -> Result<Option<&'a str>, Status> {
    let mut values = metadata.get_all(name).iter();
    let value = values.next();
    if values.next().is_some() {
        return Err(Status::unauthenticated("ambiguous authentication metadata"));
    }
    value
        .map(|value| {
            value
                .to_str()
                .map_err(|_error| Status::unauthenticated("invalid authentication metadata"))
        })
        .transpose()
}

fn resource<'a>(name: &'a str, collection: &str) -> Result<(WorkspaceId, &'a str), Status> {
    let mut parts = name.split('/');
    let (Some("workspaces"), Some(workspace), Some(kind), Some(id), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return Err(Status::invalid_argument("invalid resource name"));
    };
    if kind != collection || id.is_empty() {
        return Err(Status::invalid_argument("invalid resource name"));
    }
    let workspace = WorkspaceId::parse(workspace)
        .map_err(|_error| Status::invalid_argument("invalid Workspace name"))?;
    Ok((workspace, id))
}

#[tonic::async_trait]
impl management::task_service_server::TaskService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.get_task")]
    async fn get_task(
        &self,
        request: Request<management::GetTaskRequest>,
    ) -> Result<Response<internal::Task>, Status> {
        self.task(&request, &request.get_ref().name, PrivateAudience::Reef)
            .await
    }
}

#[tonic::async_trait]
impl internal::task_service_server::TaskService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.get_task")]
    async fn get_task(
        &self,
        request: Request<internal::GetTaskRequest>,
    ) -> Result<Response<internal::Task>, Status> {
        self.task(&request, &request.get_ref().name, PrivateAudience::Mcp)
            .await
    }
}

fn now() -> Result<i64, Status> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_error| Status::unavailable("clock unavailable"))?
        .as_nanos();
    i64::try_from(nanos).map_err(|_error| Status::unavailable("clock unavailable"))
}
