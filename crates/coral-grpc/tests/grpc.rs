//! Actual native gRPC over the same SQLite/Postgres domain owners as production.
#![allow(
    clippy::unwrap_used,
    reason = "test fixture construction fails the contract test if setup cannot complete"
)]
#![allow(
    unused_crate_dependencies,
    reason = "transport tests exercise a subset of the library dependencies"
)]
#[path = "grpc/activity.rs"]
mod activity;
#[path = "grpc/completion_feedback.rs"]
mod completion_feedback;
#[path = "grpc/health.rs"]
mod health;
#[path = "grpc/listener.rs"]
mod listener;
#[path = "grpc/load.rs"]
mod load;
#[path = "grpc/management_contracts.rs"]
mod management_contracts;
#[path = "grpc/organization_memberships.rs"]
mod organization_memberships;
#[path = "grpc/organization_workspaces.rs"]
mod organization_workspaces;
#[path = "grpc/private_access.rs"]
mod private_access;
#[path = "grpc/providers.rs"]
mod providers;
#[path = "grpc/saved_functions.rs"]
mod saved_functions;
use coral_auth::{PrincipalAuthentication, SessionTokenIssuer};
use coral_grpc::coral::{internal::v1 as internal, management::v1 as management};
use coral_outbound::DestinationPolicy;
use coral_test_support::TestDatabase;
use coral_workspace::{
    AgentDisplayName, AuthenticatedPrincipal, AuthenticatedUser, ChangeContext, OrganizationModule,
    OrganizationRole, TraceId, UserId, Workspace, WorkspaceDisplayName, WorkspaceModule,
    WorkspaceRole,
};
use listener::TestServer;
use std::sync::{Arc, OnceLock};
use tonic::transport::Channel;

struct Fixture {
    database: TestDatabase,
    _vault_dir: tempfile::TempDir,
    _server: TestServer,
    channel: Channel,
    mcp_origin: Arc<OnceLock<String>>,
    ready: Arc<std::sync::atomic::AtomicBool>,
    issuer: SessionTokenIssuer,
    owner: AuthenticatedUser,
    member: AuthenticatedUser,
    workspace: Workspace,
    workspaces: WorkspaceModule,
    /// The Provider module the server composes, for arrangement only.
    provider_module: coral_provider::ProviderModule,
    tasks: coral_task::TaskModule,
    runs: coral_run::RunModule,
    saved_functions: coral_functions::SavedFunctionModule,
    /// The no-op index the Provider workflow and the Task commands notify.
    search_index: coral_search::SearchIndex,
}

fn change(user: &AuthenticatedUser) -> ChangeContext {
    ChangeContext::new(user.clone(), 1, TraceId::current_or_new())
}

fn request<T>(message: T, token: &str, audience: &str) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
        .metadata_mut()
        .insert("coral-audience", audience.parse().unwrap());
    request
}

/// The `google.rpc.ErrorInfo` a management failure carries: the stable reason Reef reads.
#[derive(prost::Message)]
struct ErrorInfo {
    #[prost(string, tag = "1")]
    reason: String,
    #[prost(string, tag = "2")]
    domain: String,
    #[prost(map = "string, string", tag = "3")]
    metadata: std::collections::HashMap<String, String>,
}

fn error_info(status: &tonic::Status) -> ErrorInfo {
    use prost::Message as _;
    #[derive(prost::Message)]
    struct RpcStatus {
        #[prost(message, repeated, tag = "3")]
        details: Vec<prost_types::Any>,
    }
    let details = RpcStatus::decode(status.details()).unwrap().details;
    let [any] = details.as_slice() else {
        panic!("one ErrorInfo detail: {details:?}")
    };
    assert_eq!(any.type_url, "type.googleapis.com/google.rpc.ErrorInfo");
    ErrorInfo::decode(any.value.as_slice()).unwrap()
}

fn error_reason(status: &tonic::Status) -> String {
    error_info(status).reason
}

impl Fixture {
    async fn new() -> Self {
        Self::with_session_auth(true).await
    }

    #[expect(
        clippy::too_many_lines,
        reason = "composes the real domain graph and authenticated test principals once"
    )]
    async fn with_session_auth(enabled: bool) -> Self {
        let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let database = fixture.database();
        let issuer = SessionTokenIssuer::new(
            Some("https://auth.example.test"),
            coral_test_support::p256_pkcs8(),
            std::time::Duration::from_hours(1),
        )
        .unwrap();
        let organization = OrganizationModule::new(database.clone());
        let owner = AuthenticatedUser::organization_member(
            UserId::parse("owner").unwrap(),
            OrganizationRole::Owner,
        );
        let member = AuthenticatedUser::organization_member(
            UserId::parse("member").unwrap(),
            OrganizationRole::Member,
        );
        organization.provision_login(&change(&owner)).await.unwrap();
        organization
            .provision_login(&change(&member))
            .await
            .unwrap();
        let workspaces = WorkspaceModule::new(database.clone());
        let workspace = workspaces
            .create(
                &change(&owner),
                WorkspaceDisplayName::parse("private").unwrap(),
            )
            .await
            .unwrap();
        workspaces
            .set_membership(
                &change(&owner),
                workspace.id(),
                &member,
                WorkspaceRole::Member,
            )
            .await
            .unwrap();
        let tasks = coral_task::TaskModule::new(database.clone(), workspaces.clone());
        let authentication = enabled
            .then(|| PrincipalAuthentication::new(&issuer, organization, workspaces.clone()));
        let vault_dir = tempfile::tempdir().unwrap();
        let vault =
            coral_credentials::CredentialVault::open(&vault_dir.path().join("key")).unwrap();
        let providers = coral_provider::ProviderModule::with_running_catalog(
            DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
            database.clone(),
            workspaces.clone(),
        );
        let accounts = coral_provider::account::AccountModule::new(providers.clone(), vault)
            .with_client_id_metadata_document(
                "https://reef.example/.well-known/oauth-client".into(),
            );
        let runs = coral_run::RunModule::new(
            DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
            database.clone(),
            workspaces.clone(),
            providers.clone(),
            coral_run::RuntimeLimits::default(),
            coral_run::CoralInstanceId::generate(),
        )
        .unwrap()
        .with_accounts(accounts.clone());
        let saved_functions = coral_functions::SavedFunctionModule::new(
            database.clone(),
            workspaces.clone(),
            coral_functions::IsolateLimits::default(),
        )
        .unwrap();
        // Search notification tests drive the workflow at its owner.
        let search_index = coral_search::SearchIndex::inert(database.clone());
        let mcp_origin = Arc::new(OnceLock::new());
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let readiness = ready.clone();
        let server = TestServer::start(coral_grpc::routes(
            coral_grpc::Dependencies {
                organization: OrganizationModule::new(database.clone()),
                providers: providers.clone(),
                runs: runs.clone(),
                accounts: Some(accounts),
                provider_readiness: coral_search::UniversalSearchModule::new(
                    database.clone(),
                    workspaces.clone(),
                ),
                policies: coral_policy::OperationPolicyModule::new(
                    database.clone(),
                    workspaces.clone(),
                ),
                access_lifecycle: coral_administration::AccessLifecycleModule::new(
                    database.clone(),
                    workspaces.clone(),
                ),
                provider_workflow: coral_administration::ProviderWorkflow::new(
                    database.clone(),
                    providers.clone(),
                    search_index.clone(),
                ),
                workspace_workflow: coral_administration::WorkspaceWorkflow::new(
                    database.clone(),
                    workspaces.clone(),
                    providers.clone(),
                    search_index.clone(),
                ),
                workspaces: workspaces.clone(),
                tasks: tasks.clone(),
                saved_functions: coral_functions::SavedFunctionInspection::new(
                    database.clone(),
                    workspaces.clone(),
                ),
                saved_function_deletion: coral_functions::SavedFunctionDeletion::new(
                    database.clone(),
                    workspaces.clone(),
                ),
            },
            coral_grpc::TransportContext {
                authentication,
                mcp_origin: mcp_origin.clone(),
                readiness: Arc::new(move || {
                    Box::pin(std::future::ready(
                        readiness.load(std::sync::atomic::Ordering::SeqCst),
                    ))
                }),
            },
        ))
        .await;
        Self {
            database: fixture,
            _vault_dir: vault_dir,
            channel: server.channel(),
            _server: server,
            mcp_origin,
            ready,
            issuer,
            owner,
            member,
            workspace,
            workspaces,
            provider_module: providers,
            runs,
            tasks,
            saved_functions,
            search_index,
        }
    }

    /// Gives the Workspace its Built-in Provider, as creating one in
    /// production does.
    async fn install_builtin(&self) {
        let database = self.database.database();
        let mut tx = database.begin().await.unwrap();
        self.provider_module
            .install_builtin_in_transaction(&mut tx, &change(&self.owner), self.workspace.id())
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    fn token(&self, user: &AuthenticatedUser, audience: &str) -> String {
        if audience == "mcp" {
            self.issuer
                .issue_workspace_access_token(
                    user.user_id().as_str(),
                    "test",
                    audience,
                    self.workspace.id(),
                )
                .unwrap()
                .access_token
        } else {
            self.issuer
                .issue_access_token(user.user_id().as_str(), "test", audience)
                .unwrap()
                .access_token
        }
    }

    fn workspace_service(
        &self,
    ) -> management::workspace_service_client::WorkspaceServiceClient<Channel> {
        management::workspace_service_client::WorkspaceServiceClient::new(self.channel.clone())
    }

    fn memberships(
        &self,
    ) -> management::workspace_membership_service_client::WorkspaceMembershipServiceClient<Channel>
    {
        management::workspace_membership_service_client::WorkspaceMembershipServiceClient::new(
            self.channel.clone(),
        )
    }

    fn organization_memberships(
        &self,
    ) -> management::organization_membership_service_client::OrganizationMembershipServiceClient<
        Channel,
    > {
        management::organization_membership_service_client::OrganizationMembershipServiceClient::new(
            self.channel.clone(),
        )
    }

    fn agents(&self) -> management::agent_service_client::AgentServiceClient<Channel> {
        management::agent_service_client::AgentServiceClient::new(self.channel.clone())
    }

    fn registrations(
        &self,
    ) -> management::agent_registration_service_client::AgentRegistrationServiceClient<Channel>
    {
        management::agent_registration_service_client::AgentRegistrationServiceClient::new(
            self.channel.clone(),
        )
    }

    fn task_service(&self) -> management::task_service_client::TaskServiceClient<Channel> {
        management::task_service_client::TaskServiceClient::new(self.channel.clone())
    }

    fn internal_task_service(&self) -> internal::task_service_client::TaskServiceClient<Channel> {
        internal::task_service_client::TaskServiceClient::new(self.channel.clone())
    }

    fn activity(&self) -> management::activity_service_client::ActivityServiceClient<Channel> {
        management::activity_service_client::ActivityServiceClient::new(self.channel.clone())
    }

    fn setups(
        &self,
    ) -> management::provider_setup_service_client::ProviderSetupServiceClient<Channel> {
        management::provider_setup_service_client::ProviderSetupServiceClient::new(
            self.channel.clone(),
        )
    }

    fn providers(&self) -> management::provider_service_client::ProviderServiceClient<Channel> {
        management::provider_service_client::ProviderServiceClient::new(self.channel.clone())
    }

    fn accounts(
        &self,
    ) -> management::provider_account_service_client::ProviderAccountServiceClient<Channel> {
        management::provider_account_service_client::ProviderAccountServiceClient::new(
            self.channel.clone(),
        )
    }

    fn saved_functions(
        &self,
    ) -> management::saved_function_service_client::SavedFunctionServiceClient<Channel> {
        management::saved_function_service_client::SavedFunctionServiceClient::new(
            self.channel.clone(),
        )
    }

    fn policies(
        &self,
    ) -> management::operation_policy_service_client::OperationPolicyServiceClient<Channel> {
        management::operation_policy_service_client::OperationPolicyServiceClient::new(
            self.channel.clone(),
        )
    }

    fn organization_workspaces(
        &self,
    ) -> management::organization_workspace_service_client::OrganizationWorkspaceServiceClient<
        Channel,
    > {
        management::organization_workspace_service_client::OrganizationWorkspaceServiceClient::new(
            self.channel.clone(),
        )
    }

    /// Provisions another User with a current Organization Membership.
    async fn organization_user(&self, id: &str, role: OrganizationRole) -> AuthenticatedUser {
        coral_test_support::provision_user(self.database.database(), id, role).await
    }

    /// Records verified login profiles the way `coral-auth` does at sign-in.
    async fn sign_in_profiles(&self, profiles: &[(&AuthenticatedUser, &str, &str, bool)]) {
        let database = self.database.database();
        let mut tx = database.begin().await.unwrap();
        OrganizationModule::hold_profile_updates_in_transaction(&mut tx)
            .await
            .unwrap();
        for (user, name, email, verified) in profiles {
            OrganizationModule::refresh_profile_in_transaction(
                &mut tx,
                user.user_id(),
                coral_workspace::VerifiedUserProfile::new(
                    Some(name),
                    Some(email),
                    Some(*verified),
                    None,
                ),
                1,
            )
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();
    }

    /// Waits on the Run's change notifications, not a poll, until it is terminal.
    async fn settle(&self, actor: &AuthenticatedPrincipal, run: &coral_run::RunId) {
        let settled = self
            .runs
            .observe(
                actor,
                self.workspace.id(),
                run,
                &coral_run::RunObservation::initial(std::time::Duration::from_secs(30)).unwrap(),
            )
            .await
            .unwrap();
        assert!(settled.state().is_terminal(), "{:?}", settled.state());
    }

    async fn task(&self, actor: &AuthenticatedPrincipal) -> coral_task::Task {
        self.tasks
            .start(
                actor,
                self.workspace.id(),
                coral_task::TaskIntent::parse("private-intent").unwrap(),
                1,
            )
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn workspace_management_requires_a_membership_even_for_an_organization_owner() {
    let f = Fixture::new().await;
    let mut client = f.workspace_service();
    let creator = f.token(&f.member, "reef");
    let created = client
        .create_workspace(request(
            management::CreateWorkspaceRequest {
                display_name: "Alice's Workspace".into(),
                request_id: uuid::Uuid::new_v4().to_string(),
            },
            &creator,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(created.display_name, "Alice's Workspace");
    assert!(created.mcp_endpoint.is_empty());
    assert_eq!(
        created.effective_role,
        management::WorkspaceRole::Owner as i32
    );
    f.mcp_origin.set("https://mcp.example.test".into()).unwrap();
    let get = management::GetWorkspaceRequest {
        name: created.name.clone(),
    };
    let inspected = client
        .get_workspace(request(get.clone(), &creator, "reef"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(inspected.state, management::WorkspaceState::Active as i32);
    assert_eq!(
        inspected.mcp_endpoint,
        format!("https://mcp.example.test/{}/mcp", inspected.name)
    );
    assert_eq!(
        inspected.effective_role,
        management::WorkspaceRole::Owner as i32
    );
    assert_eq!(inspected.current_user, "users/member");
    assert!(!inspected.etag.is_empty());
    let rename = management::UpdateWorkspaceRequest {
        workspace: Some(management::Workspace {
            display_name: "Renamed".into(),
            ..inspected.clone()
        }),
        update_mask: Some(prost_types::FieldMask {
            paths: vec!["display_name".into()],
        }),
    };
    // The Organization Owner holds no Membership here: the Workspace is concealed.
    let outsider = f.token(&f.owner, "reef");
    let denied = [
        client
            .get_workspace(request(get.clone(), &outsider, "reef"))
            .await
            .unwrap_err(),
        client
            .update_workspace(request(rename.clone(), &outsider, "reef"))
            .await
            .unwrap_err(),
    ];
    for error in denied {
        assert_eq!(
            (error.code(), error_reason(&error).as_str()),
            (tonic::Code::NotFound, "NOT_FOUND")
        );
    }
    assert_eq!(
        client
            .get_workspace(request(get, &creator, "reef"))
            .await
            .unwrap()
            .into_inner(),
        inspected,
        "the denied rename changed nothing"
    );
    let changed = client
        .update_workspace(request(rename, &creator, "reef"))
        .await
        .unwrap()
        .into_inner();
    assert_ne!(changed.etag, inspected.etag);
    let stale = client
        .archive_workspace(request(
            management::ArchiveWorkspaceRequest {
                name: created.name,
                etag: inspected.etag,
            },
            &creator,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), tonic::Code::Aborted);
    assert_eq!(error_reason(&stale), "ETAG_MISMATCH");
}

/// coral-workspace owns who is listed. This checks the `enabled` projection of
/// both listings and that a Member reads the Owner directory but not the Members.
#[tokio::test]
async fn membership_listings_project_enabled_and_hide_members_from_members() {
    let f = Fixture::new().await;
    let mut client = f.memberships();
    let parent = format!("workspaces/{}", f.workspace.id());
    let disabled = f
        .organization_user("disabled", OrganizationRole::Member)
        .await;
    f.workspaces
        .set_membership(
            &change(&f.owner),
            f.workspace.id(),
            &disabled,
            WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    OrganizationModule::new(f.database.database())
        .set_enabled(&change(&f.owner), disabled.user_id(), 1, false)
        .await
        .unwrap();
    let listed = client
        .list_workspace_memberships(request(
            management::ListWorkspaceMembershipsRequest {
                parent: parent.clone(),
                ..Default::default()
            },
            &f.token(&f.owner, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let mut memberships: Vec<_> = listed
        .memberships
        .iter()
        .map(|m| (m.user.as_str(), m.role, m.enabled))
        .collect();
    memberships.sort_unstable();
    let (owner_role, member_role) = (
        management::WorkspaceRole::Owner as i32,
        management::WorkspaceRole::Member as i32,
    );
    assert_eq!(
        memberships,
        [
            ("users/disabled", owner_role, false),
            ("users/member", member_role, true),
            ("users/owner", owner_role, true),
        ]
    );
    assert_eq!(
        client
            .list_workspace_memberships(request(
                management::ListWorkspaceMembershipsRequest {
                    parent: parent.clone(),
                    ..Default::default()
                },
                &f.token(&f.member, "reef"),
                "reef"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let owners = client
        .list_workspace_owners(request(
            management::ListWorkspaceOwnersRequest {
                parent: parent.clone(),
                ..Default::default()
            },
            &f.token(&f.member, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let mut owners: Vec<_> = owners
        .owners
        .iter()
        .map(|owner| {
            let profile = owner.profile.as_ref().unwrap();
            assert_eq!(profile.user, owner.user);
            (owner.user.as_str(), owner.enabled)
        })
        .collect();
    owners.sort_unstable();
    assert_eq!(owners, [("users/disabled", false), ("users/owner", true)]);
    let mine = client
        .get_workspace_membership(request(
            management::GetWorkspaceMembershipRequest {
                name: format!("{parent}/memberships/member"),
            },
            &f.token(&f.member, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(mine.role, management::WorkspaceRole::Member as i32);
    assert!(mine.enabled);
}

/// coral-workspace owns rotation and lifecycle rules
/// (`rotation_preserves_identity_and_lifecycle_loss_blocks_stale_authority`).
/// Over the wire, each credential a management RPC returns is the one the MCP
/// audience accepts: an accepted Agent finds no Task, a rejected one is unauthenticated.
#[tokio::test]
async fn agent_management_rotates_once_and_revokes_real_credentials() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let issued = f
        .agents()
        .create_agent(request(
            management::CreateAgentRequest {
                parent: format!("workspaces/{}", f.workspace.id()),
                display_name: "Worker".into(),
                request_id: uuid::Uuid::new_v4().to_string(),
                expire_time: None,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let task = format!(
        "workspaces/{}/tasks/{}",
        f.workspace.id(),
        uuid::Uuid::new_v4()
    );
    let probe = async |credential: &str| {
        f.internal_task_service()
            .get_task(request(
                internal::GetTaskRequest { name: task.clone() },
                credential,
                "mcp",
            ))
            .await
            .unwrap_err()
            .code()
    };
    assert_eq!(probe(&issued.credential).await, tonic::Code::NotFound);
    let registration = issued.registration.unwrap();
    let rotate = management::RotateAgentCredentialRequest {
        name: registration.name,
        etag: registration.etag,
        request_id: uuid::Uuid::new_v4().to_string(),
    };
    let rotated = f
        .registrations()
        .rotate_agent_credential(request(rotate.clone(), &token, "reef"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        probe(&issued.credential).await,
        tonic::Code::Unauthenticated
    );
    assert_eq!(probe(&rotated.credential).await, tonic::Code::NotFound);
    let again = f
        .registrations()
        .rotate_agent_credential(request(rotate, &token, "reef"))
        .await
        .unwrap_err();
    assert_eq!(
        (again.code(), error_reason(&again).as_str()),
        (tonic::Code::Aborted, "ETAG_MISMATCH")
    );
    assert_eq!(
        probe(&rotated.credential).await,
        tonic::Code::NotFound,
        "a refused rotation changed nothing"
    );
    let mut agent = rotated.agent.unwrap();
    agent.state = management::AgentState::Disabled.into();
    f.agents()
        .update_agent(request(
            management::UpdateAgentRequest {
                agent: Some(agent),
                update_mask: Some(prost_types::FieldMask {
                    paths: vec!["state".into()],
                }),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap();
    assert_eq!(
        probe(&rotated.credential).await,
        tonic::Code::Unauthenticated
    );
}

#[tokio::test]
async fn local_agent_authentication_reports_database_outages_as_unavailable() {
    let f = Fixture::with_session_auth(false).await;
    let issued = f
        .workspaces
        .create_agent(
            &change(&f.owner),
            f.workspace.id(),
            AgentDisplayName::parse("agent").unwrap(),
            None,
        )
        .await
        .unwrap();
    let principal = f
        .workspaces
        .authenticate_agent(f.workspace.id(), issued.credential.expose(), 1)
        .await
        .unwrap();
    let task = f
        .tasks
        .start(
            &principal,
            f.workspace.id(),
            coral_task::TaskIntent::parse("Private goal").unwrap(),
            1,
        )
        .await
        .unwrap();
    let name = format!("workspaces/{}/tasks/{}", f.workspace.id(), task.id());
    let mut client = f.internal_task_service();
    let get = |credential: &str| {
        request(
            internal::GetTaskRequest { name: name.clone() },
            credential,
            "mcp",
        )
    };
    assert_eq!(
        client.get_task(get("invalid")).await.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        client
            .get_task(get(issued.credential.expose()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );

    f.database.database().close().await;
    let error = client
        .get_task(get(issued.credential.expose()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(error.message(), "authentication unavailable");
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one transport scenario includes authority changes and a retained credential"
)]
async fn audience_reachability_is_not_principal_authority() {
    let f = Fixture::new().await;
    let issued = f
        .workspaces
        .create_agent(
            &change(&f.owner),
            f.workspace.id(),
            AgentDisplayName::parse("agent").unwrap(),
            None,
        )
        .await
        .unwrap();
    let name = format!(
        "workspaces/{}/agents/{}/registration",
        f.workspace.id(),
        issued.agent.principal().id()
    );
    let mut client = f.registrations();
    for (user, token_audience, claimed_audience, expected) in [
        (&f.owner, "mcp", "mcp", tonic::Code::PermissionDenied),
        (&f.owner, "mcp", "reef", tonic::Code::Unauthenticated),
        (
            &f.owner,
            "https://reef.example.test",
            "reef",
            tonic::Code::Unauthenticated,
        ),
        (&f.member, "reef", "reef", tonic::Code::NotFound),
    ] {
        let error = client
            .get_agent_registration(request(
                management::GetAgentRegistrationRequest { name: name.clone() },
                &f.token(user, token_audience),
                claimed_audience,
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), expected);
    }
    assert_eq!(
        client
            .get_agent_registration(management::GetAgentRegistrationRequest { name: name.clone() })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    let registered = client
        .get_agent_registration(request(
            management::GetAgentRegistrationRequest { name: name.clone() },
            &f.token(&f.owner, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(registered.name, name);
    assert_eq!(registered.uid, issued.registration.id().to_string());
    assert!(registered.enabled);
    // The same token does not retain a role after the owning domain changes it.
    let retained = f.token(&f.member, "reef");
    f.workspaces
        .set_membership(
            &change(&f.owner),
            f.workspace.id(),
            &f.member,
            WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    client
        .get_agent_registration(request(
            management::GetAgentRegistrationRequest { name: name.clone() },
            &retained,
            "reef",
        ))
        .await
        .unwrap();
    // An Organization Owner who leaves the Workspace keeps no authority in it.
    coral_administration::AccessLifecycleModule::new(f.database.database(), f.workspaces.clone())
        .remove_workspace_member(&change(&f.owner), f.workspace.id(), f.owner.user_id())
        .await
        .unwrap();
    let left = client
        .get_agent_registration(request(
            management::GetAgentRegistrationRequest { name: name.clone() },
            &f.token(&f.owner, "reef"),
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(
        (left.code(), error_reason(&left).as_str()),
        (tonic::Code::NotFound, "NOT_FOUND")
    );
    f.workspaces
        .set_membership(
            &change(&f.member),
            f.workspace.id(),
            &f.owner,
            WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    f.workspaces
        .set_membership(
            &change(&f.owner),
            f.workspace.id(),
            &f.member,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    assert_eq!(
        client
            .get_agent_registration(request(
                management::GetAgentRegistrationRequest { name },
                &retained,
                "reef"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "both real transports share one confidentiality and revocation scenario"
)]
async fn task_concealment_and_live_membership_survive_both_audiences() {
    let f = Fixture::new().await;
    // The designated Owner cannot read a Member's private Task.
    let task = f
        .task(&AuthenticatedPrincipal::user(f.member.clone()))
        .await;
    let name = format!("workspaces/{}/tasks/{}", f.workspace.id(), task.id());
    let mut reef = f.task_service();
    let hidden = reef
        .get_task(request(
            management::GetTaskRequest { name: name.clone() },
            &f.token(&f.owner, "reef"),
            "reef",
        ))
        .await
        .unwrap_err();
    let absent = reef
        .get_task(request(
            management::GetTaskRequest {
                name: format!(
                    "workspaces/{}/tasks/{}",
                    f.workspace.id(),
                    uuid::Uuid::new_v4()
                ),
            },
            &f.token(&f.owner, "reef"),
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(hidden.code(), tonic::Code::NotFound);
    assert_eq!(
        (hidden.code(), hidden.message()),
        (absent.code(), absent.message())
    );
    let visible = reef
        .get_task(request(
            management::GetTaskRequest { name: name.clone() },
            &f.token(&f.member, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(visible.intent, "private-intent");
    let mut mcp = f.internal_task_service();
    let scoped = f.token(&f.member, "mcp");
    assert_eq!(
        mcp.get_task(request(
            internal::GetTaskRequest { name: name.clone() },
            &scoped,
            "mcp"
        ))
        .await
        .unwrap()
        .into_inner()
        .intent,
        "private-intent"
    );
    let hidden = mcp
        .get_task(request(
            internal::GetTaskRequest { name: name.clone() },
            &f.token(&f.owner, "mcp"),
            "mcp",
        ))
        .await
        .unwrap_err();
    assert_eq!(hidden.code(), tonic::Code::NotFound);
    let unscoped = f
        .issuer
        .issue_access_token(f.member.user_id().as_str(), "test", "mcp")
        .unwrap()
        .access_token;
    for token in [
        unscoped,
        f.token(&f.member, "reef"),
        f.token(&f.member, "https://mcp.example.test"),
    ] {
        assert_eq!(
            mcp.get_task(request(
                internal::GetTaskRequest { name: name.clone() },
                &token,
                "mcp"
            ))
            .await
            .unwrap_err()
            .code(),
            tonic::Code::Unauthenticated
        );
    }
    assert_eq!(
        mcp.get_task(request(
            internal::GetTaskRequest { name: name.clone() },
            &f.token(&f.member, "reef"),
            "reef"
        ))
        .await
        .unwrap_err()
        .code(),
        tonic::Code::PermissionDenied
    );
    let other = f
        .workspaces
        .create(
            &change(&f.member),
            WorkspaceDisplayName::parse("other").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        mcp.get_task(request(
            internal::GetTaskRequest {
                name: format!("workspaces/{}/tasks/{}", other.id(), task.id())
            },
            &scoped,
            "mcp"
        ))
        .await
        .unwrap_err()
        .code(),
        tonic::Code::Unauthenticated
    );
    coral_administration::AccessLifecycleModule::new(f.database.database(), f.workspaces.clone())
        .remove_workspace_member(&change(&f.owner), f.workspace.id(), f.member.user_id())
        .await
        .unwrap();
    assert_eq!(
        mcp.get_task(request(internal::GetTaskRequest { name }, &scoped, "mcp"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
}
