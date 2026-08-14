//! Implements the gRPC `WorkspaceService` for workspace lifecycle APIs.

use coral_api::v1::workspace_service_server::WorkspaceService as WorkspaceServiceApi;
use coral_api::v1::{
    AddWorkspaceMemberRequest, AddWorkspaceMemberResponse, CreateWorkspaceRequest,
    CreateWorkspaceResponse, DeleteWorkspaceRequest, DeleteWorkspaceResponse,
    ListWorkspaceMembersRequest, ListWorkspaceMembersResponse, ListWorkspacesRequest,
    ListWorkspacesResponse, RemoveWorkspaceMemberRequest, RemoveWorkspaceMemberResponse, Workspace,
    WorkspaceMember, WorkspaceRole,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::{AppError, app_status};
use crate::identity::LOCAL_PRINCIPAL_ID;
use crate::state::db::WorkspaceMemberRecord;
use crate::transport::{
    grpc_span, instrument_grpc, request_context, workspace_name_from_proto, workspace_to_proto,
};
use crate::workspaces::authorization::{WorkspaceAction, WorkspaceAuthorizer};
use crate::workspaces::{MemberRole, WorkspaceManager, WorkspaceRecord};

#[derive(Clone)]
pub(crate) struct WorkspaceService {
    workspaces: WorkspaceManager,
    authorizer: WorkspaceAuthorizer,
}

impl WorkspaceService {
    pub(crate) const fn new(
        workspace_manager: WorkspaceManager,
        authorizer: WorkspaceAuthorizer,
    ) -> Self {
        Self {
            workspaces: workspace_manager,
            authorizer,
        }
    }
}

#[tonic::async_trait]
impl WorkspaceServiceApi for WorkspaceService {
    async fn list_workspaces(
        &self,
        request: Request<ListWorkspacesRequest>,
    ) -> Result<Response<ListWorkspacesResponse>, Status> {
        let span = grpc_span(&request);
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let workspaces = workspaces
                .list_workspaces()
                .await
                .map_err(app_status)?
                .iter()
                .map(workspace_record_to_proto)
                .collect();
            Ok(Response::new(ListWorkspacesResponse { workspaces }))
        })
        .await
    }

    async fn create_workspace(
        &self,
        request: Request<CreateWorkspaceRequest>,
    ) -> Result<Response<CreateWorkspaceResponse>, Status> {
        let span = grpc_span(&request);
        let workspaces = self.workspaces.clone();
        let authorizer = self.authorizer.clone();
        let principal = request_context(&request)?.principal().clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorizer
                .authorize_creation(&principal)
                .map_err(app_status)?;
            let creator = principal.id().as_str();
            let workspace = if creator == LOCAL_PRINCIPAL_ID {
                // Being admitted at all means this deployment treats the local
                // principal as owner of everything, and it has no directory row
                // an ownership grant could reference.
                workspaces.create_workspace(&workspace_name).await
            } else {
                workspaces
                    .create_workspace_for_user(&workspace_name, creator)
                    .await
            }
            .map_err(app_status)?;
            Ok(Response::new(CreateWorkspaceResponse {
                workspace: Some(workspace_record_to_proto(&workspace)),
            }))
        })
        .await
    }

    async fn delete_workspace(
        &self,
        request: Request<DeleteWorkspaceRequest>,
    ) -> Result<Response<DeleteWorkspaceResponse>, Status> {
        let span = grpc_span(&request);
        let workspaces = self.workspaces.clone();
        let authorizer = self.authorizer.clone();
        let principal = request_context(&request)?.principal().clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorizer
                .authorize(&principal, &workspace_name, WorkspaceAction::Manage)
                .await
                .map_err(app_status)?;
            let workspace = workspaces
                .delete_workspace(&workspace_name)
                .await
                .map_err(app_status)?;
            Ok(Response::new(DeleteWorkspaceResponse {
                workspace: Some(workspace_record_to_proto(&workspace)),
            }))
        })
        .await
    }

    /// Lists who may reach one workspace.
    ///
    /// The roster is `Manage` rather than `Read`: it is the access-control
    /// state itself, so reading it is a control-plane act. A member reads the
    /// workspace's contents without learning who else holds a key to it, and
    /// an agent credential — which the control-plane restriction turns away
    /// before any role is read — cannot enumerate the people behind it.
    async fn list_workspace_members(
        &self,
        request: Request<ListWorkspaceMembersRequest>,
    ) -> Result<Response<ListWorkspaceMembersResponse>, Status> {
        let span = grpc_span(&request);
        let workspaces = self.workspaces.clone();
        let authorizer = self.authorizer.clone();
        let principal = request_context(&request)?.principal().clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorizer
                .authorize(&principal, &workspace_name, WorkspaceAction::Manage)
                .await
                .map_err(app_status)?;
            let members = workspaces
                .list_workspace_members(&workspace_name)
                .await
                .map_err(app_status)?
                .into_iter()
                .map(member_to_proto)
                .collect();
            Ok(Response::new(ListWorkspaceMembersResponse { members }))
        })
        .await
    }

    async fn add_workspace_member(
        &self,
        request: Request<AddWorkspaceMemberRequest>,
    ) -> Result<Response<AddWorkspaceMemberResponse>, Status> {
        let span = grpc_span(&request);
        let workspaces = self.workspaces.clone();
        let authorizer = self.authorizer.clone();
        let principal = request_context(&request)?.principal().clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            // Authorization answers before the request body is inspected any
            // further, so a caller who may not reach this workspace learns
            // nothing from how well-formed the rest of their request was.
            authorizer
                .authorize(&principal, &workspace_name, WorkspaceAction::Manage)
                .await
                .map_err(app_status)?;
            let role = member_role_from_proto(request.role)?;
            let member = workspaces
                .add_workspace_member(&workspace_name, &request.user_id, role)
                .await
                .map_err(app_status)?;
            Ok(Response::new(AddWorkspaceMemberResponse {
                member: Some(member_to_proto(member)),
            }))
        })
        .await
    }

    async fn remove_workspace_member(
        &self,
        request: Request<RemoveWorkspaceMemberRequest>,
    ) -> Result<Response<RemoveWorkspaceMemberResponse>, Status> {
        let span = grpc_span(&request);
        let workspaces = self.workspaces.clone();
        let authorizer = self.authorizer.clone();
        let principal = request_context(&request)?.principal().clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorizer
                .authorize(&principal, &workspace_name, WorkspaceAction::Manage)
                .await
                .map_err(app_status)?;
            workspaces
                .remove_workspace_member(&workspace_name, &request.user_id)
                .await
                .map_err(app_status)?;
            Ok(Response::new(RemoveWorkspaceMemberResponse {}))
        })
        .await
    }
}

fn workspace_record_to_proto(record: &WorkspaceRecord) -> Workspace {
    workspace_to_proto(&record.name)
}

fn member_to_proto(member: WorkspaceMemberRecord) -> WorkspaceMember {
    WorkspaceMember {
        user_id: member.user_id,
        role: member_role_to_proto(member.role).into(),
        display_name: member.display_name.unwrap_or_default(),
    }
}

const fn member_role_to_proto(role: MemberRole) -> WorkspaceRole {
    match role {
        MemberRole::Owner => WorkspaceRole::Owner,
        MemberRole::Member => WorkspaceRole::Member,
    }
}

/// Reads the requested role, recognizing nothing outside the closed set.
///
/// An unspecified role is caller input rather than a default: silently granting
/// the lesser one would let a malformed client hand out access nobody asked for.
fn member_role_from_proto(role: i32) -> Result<MemberRole, Status> {
    match WorkspaceRole::try_from(role) {
        Ok(WorkspaceRole::Owner) => Ok(MemberRole::Owner),
        Ok(WorkspaceRole::Member) => Ok(MemberRole::Member),
        Ok(WorkspaceRole::Unspecified) | Err(_) => Err(app_status(AppError::InvalidInput(
            "membership role is required".to_string(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use coral_api::v1::{
        AddWorkspaceMemberRequest, CreateWorkspaceRequest, DeleteWorkspaceRequest,
        ListWorkspaceMembersRequest, ListWorkspacesRequest, RemoveWorkspaceMemberRequest,
        WorkspaceMember, WorkspaceRole,
    };
    use tempfile::TempDir;
    use tonic::{Code, Request, Status};
    use tonic_types::{ErrorDetail, StatusExt as _};

    use super::{WorkspaceService, WorkspaceServiceApi};
    use crate::credentials::{CredentialManager, CredentialStore};
    use crate::identity::{LOCAL_PRINCIPAL_ID, Principal, PrincipalKind};
    use crate::request_context::RequestContext;
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbRepos, LoginIdentity, LoginProvisioning, ResolvedDatabaseConfig,
    };
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::transport::workspace_to_proto;
    use crate::workspaces::authorization::WorkspaceAuthorizer;
    use crate::workspaces::{MemberRole, WorkspaceManager, WorkspaceName};

    /// One workspace nobody in these tests is a member of, used wherever a name
    /// that was never created is needed.
    const ABSENT: &str = "absent";

    #[tokio::test]
    async fn creating_a_workspace_makes_its_caller_the_owner() {
        let deployment = shared_deployment().await;
        let ada = deployment.seed_user("ada").await;

        deployment
            .create(&federated(&ada), "team")
            .await
            .expect("a provisioned caller creates a workspace");

        assert_eq!(
            deployment.role_of("team", &ada).await,
            Some(MemberRole::Owner)
        );
        // The listing keeps its host-wide shape until the membership flip.
        assert_eq!(
            deployment
                .service
                .list_workspaces(request(ListWorkspacesRequest {}, federated(&ada)))
                .await
                .expect("list workspaces")
                .into_inner()
                .workspaces
                .into_iter()
                .map(|workspace| workspace.name)
                .collect::<Vec<_>>(),
            vec!["team".to_string()],
        );
    }

    /// A caller with no directory row cannot be made owner, so the workspace is
    /// not created at all rather than created ownerless.
    #[tokio::test]
    async fn a_caller_the_directory_does_not_know_creates_nothing() {
        let deployment = shared_deployment().await;

        let status = deployment
            .create(&federated("never-logged-in"), "team")
            .await
            .expect_err("an unprovisioned caller cannot own a workspace");

        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(deployment.role_of("team", "never-logged-in").await, None);
    }

    /// Concealment is only real if it survives the adapter: every management RPC
    /// must answer a non-member about a workspace that exists exactly as it
    /// answers about one that never did — same code, same message once the
    /// caller's own echoed name is normalized, same structured details.
    #[tokio::test]
    async fn a_nonmember_is_answered_about_an_existing_workspace_as_about_a_missing_one() {
        let deployment = shared_deployment().await;
        let owner = deployment.seed_user("owner").await;
        let outsider = federated(&deployment.seed_user("outsider").await);
        deployment
            .create(&federated(&owner), "private")
            .await
            .expect("create");

        for (concealed, missing) in [
            (
                deployment.delete(&outsider, "private").await,
                deployment.delete(&outsider, ABSENT).await,
            ),
            (
                deployment
                    .add(&outsider, "private", &owner, WorkspaceRole::Member)
                    .await
                    .map(|_| ()),
                deployment
                    .add(&outsider, ABSENT, &owner, WorkspaceRole::Member)
                    .await
                    .map(|_| ()),
            ),
            (
                deployment.remove(&outsider, "private", &owner).await,
                deployment.remove(&outsider, ABSENT, &owner).await,
            ),
        ] {
            let concealed = concealed.expect_err("a non-member reaches nothing");
            let missing = missing.expect_err("a missing workspace reaches nothing");
            assert_eq!(concealed.code(), Code::NotFound);
            assert_eq!(concealed.code(), missing.code());
            assert_eq!(
                normalize(concealed.message(), "private"),
                normalize(missing.message(), ABSENT),
            );
            assert_eq!(reasons(&concealed), reasons(&missing));
            assert!(
                !reasons(&concealed).is_empty(),
                "both answers must carry the ordinary workspace-miss reason"
            );
        }
    }

    #[tokio::test]
    async fn an_owner_grants_promotes_demotes_and_revokes_membership() {
        let deployment = shared_deployment().await;
        let owner = federated(&deployment.seed_user("owner").await);
        let ada = deployment.seed_user("ada").await;
        deployment.create(&owner, "team").await.expect("create");

        let granted = deployment
            .add(&owner, "team", &ada, WorkspaceRole::Member)
            .await
            .expect("grant membership");
        assert_eq!(
            granted,
            WorkspaceMember {
                user_id: ada.clone(),
                role: WorkspaceRole::Member.into(),
                display_name: "Seeded ada".to_string(),
            }
        );
        // A retried invitation reads exactly like the first one.
        assert_eq!(
            deployment
                .add(&owner, "team", &ada, WorkspaceRole::Member)
                .await
                .expect("a repeated grant succeeds"),
            granted,
        );

        deployment
            .add(&owner, "team", &ada, WorkspaceRole::Owner)
            .await
            .expect("promote");
        assert_eq!(
            deployment.role_of("team", &ada).await,
            Some(MemberRole::Owner)
        );
        deployment
            .add(&owner, "team", &ada, WorkspaceRole::Member)
            .await
            .expect("demote, because the workspace keeps another owner");
        assert_eq!(
            deployment.role_of("team", &ada).await,
            Some(MemberRole::Member)
        );

        assert_eq!(
            deployment
                .add(&owner, "team", "nobody", WorkspaceRole::Member)
                .await
                .expect_err("an unknown person holds nothing to grant")
                .code(),
            Code::NotFound,
        );
        assert_eq!(
            deployment
                .add(&owner, "team", &ada, WorkspaceRole::Unspecified)
                .await
                .expect_err("an unspecified role is not a default")
                .code(),
            Code::InvalidArgument,
        );

        deployment
            .remove(&owner, "team", &ada)
            .await
            .expect("revoke");
        assert_eq!(deployment.role_of("team", &ada).await, None);
        assert_eq!(
            deployment
                .remove(&owner, "team", &ada)
                .await
                .expect_err("a revoked membership is not there to revoke twice")
                .code(),
            Code::NotFound,
        );
    }

    /// The owner floor is the same rule whether the last owner is revoked or
    /// merely demoted, so both must be refused over the wire.
    #[tokio::test]
    async fn the_last_owner_can_neither_be_demoted_nor_revoked() {
        let deployment = shared_deployment().await;
        let owner_id = deployment.seed_user("owner").await;
        let owner = federated(&owner_id);
        deployment.create(&owner, "team").await.expect("create");

        for status in [
            deployment
                .add(&owner, "team", &owner_id, WorkspaceRole::Member)
                .await
                .map(|_| ())
                .expect_err("demoting the last owner strands the workspace"),
            deployment
                .remove(&owner, "team", &owner_id)
                .await
                .expect_err("revoking the last owner strands the workspace"),
        ] {
            assert_eq!(status.code(), Code::FailedPrecondition);
        }
        assert_eq!(
            deployment.role_of("team", &owner_id).await,
            Some(MemberRole::Owner)
        );
    }

    #[tokio::test]
    async fn deleting_a_workspace_is_owner_only_and_takes_its_memberships_with_it() {
        let deployment = shared_deployment().await;
        let owner_id = deployment.seed_user("owner").await;
        let owner = federated(&owner_id);
        let ada = deployment.seed_user("ada").await;
        deployment.create(&owner, "team").await.expect("create");
        deployment
            .add(&owner, "team", &ada, WorkspaceRole::Member)
            .await
            .expect("grant membership");

        // A member already knows the workspace, so they are denied rather than
        // told it is absent.
        assert_eq!(
            deployment
                .delete(&federated(&ada), "team")
                .await
                .expect_err("a member may not delete the workspace")
                .code(),
            Code::PermissionDenied,
        );

        deployment.delete(&owner, "team").await.expect("delete");

        assert_eq!(deployment.role_of("team", &owner_id).await, None);
        assert_eq!(deployment.role_of("team", &ada).await, None);
    }

    /// The roster is control-plane state, so it answers the same three ways
    /// every other management RPC does: owners read it, members are denied it,
    /// and outsiders are told the workspace is not there.
    #[tokio::test]
    async fn the_member_roster_is_owner_only_and_concealed_from_outsiders() {
        let deployment = shared_deployment().await;
        let owner_id = deployment.seed_user("owner").await;
        let owner = federated(&owner_id);
        let ada = deployment.seed_user("ada").await;
        let outsider = federated(&deployment.seed_user("outsider").await);
        deployment.create(&owner, "team").await.expect("create");
        deployment
            .add(&owner, "team", &ada, WorkspaceRole::Member)
            .await
            .expect("grant membership");

        let mut expected = vec![
            WorkspaceMember {
                user_id: owner_id.clone(),
                role: WorkspaceRole::Owner.into(),
                display_name: "Seeded owner".to_string(),
            },
            WorkspaceMember {
                user_id: ada.clone(),
                role: WorkspaceRole::Member.into(),
                display_name: "Seeded ada".to_string(),
            },
        ];
        expected.sort_by(|left, right| left.user_id.cmp(&right.user_id));
        assert_eq!(
            deployment
                .members(&owner, "team")
                .await
                .expect("an owner reads the roster"),
            expected,
        );

        assert_eq!(
            deployment
                .members(&federated(&ada), "team")
                .await
                .expect_err("a member reads the workspace, not who else holds a key to it")
                .code(),
            Code::PermissionDenied,
        );
        for name in ["team", ABSENT] {
            assert_eq!(
                deployment
                    .members(&outsider, name)
                    .await
                    .expect_err("an outsider reaches no roster")
                    .code(),
                Code::NotFound,
            );
        }

        // An agent credential carries the person's read access and none of
        // their control-plane authority, so the roster is closed to it even
        // though the person behind it owns the workspace.
        assert_eq!(
            deployment
                .members(
                    &Principal::parse(&owner_id, PrincipalKind::Agent).expect("agent"),
                    "team",
                )
                .await
                .expect_err("an agent credential may not enumerate the workspace's people")
                .code(),
            Code::PermissionDenied,
        );
    }

    /// Creation is the one control-plane act with no existing workspace to
    /// gate, and an agent that reached it would be made Owner of what it
    /// created — the exact authority no workspace role may grant it.
    #[tokio::test]
    async fn an_agent_credential_cannot_create_a_workspace() {
        let deployment = shared_deployment().await;
        let ada = deployment.seed_user("ada").await;

        assert_eq!(
            deployment
                .create(
                    &Principal::parse(&ada, PrincipalKind::Agent).expect("agent"),
                    "team",
                )
                .await
                .expect_err("an agent credential holds no control-plane authority")
                .code(),
            Code::PermissionDenied,
        );
        assert_eq!(
            deployment.role_of("team", &ada).await,
            None,
            "the refused creation must leave no workspace and no ownership behind"
        );

        deployment
            .create(&federated(&ada), "team")
            .await
            .expect("the same person's own credential still creates the workspace");
        assert_eq!(
            deployment.role_of("team", &ada).await,
            Some(MemberRole::Owner)
        );
    }

    #[tokio::test]
    async fn the_local_principal_creates_only_where_the_deployment_admits_it() {
        let shared = shared_deployment().await;
        assert_eq!(
            shared
                .create(&Principal::local(), "team")
                .await
                .expect_err("a shared deployment does not admit the local principal")
                .code(),
            Code::PermissionDenied,
        );

        let local = local_deployment().await;
        local
            .create(&Principal::local(), "team")
            .await
            .expect("the implicit owner creates without a directory row");
        assert_eq!(local.role_of("team", LOCAL_PRINCIPAL_ID).await, None);
    }

    /// The adapter reads its caller from request context, so a request that
    /// never passed the authenticating layer must fail closed.
    #[tokio::test]
    async fn a_request_without_authenticated_context_is_refused() {
        let deployment = shared_deployment().await;

        assert_eq!(
            deployment
                .service
                .create_workspace(Request::new(CreateWorkspaceRequest {
                    workspace: Some(workspace_to_proto(&workspace("team"))),
                }))
                .await
                .expect_err("no principal was selected for this request")
                .code(),
            Code::Internal,
        );
    }

    struct Deployment {
        _temp: TempDir,
        db: Arc<CoralDb>,
        service: WorkspaceService,
    }

    impl Deployment {
        async fn create(&self, principal: &Principal, name: &str) -> Result<(), Status> {
            self.service
                .create_workspace(request(
                    CreateWorkspaceRequest {
                        workspace: Some(workspace_to_proto(&workspace(name))),
                    },
                    principal.clone(),
                ))
                .await
                .map(|_| ())
        }

        async fn delete(&self, principal: &Principal, name: &str) -> Result<(), Status> {
            self.service
                .delete_workspace(request(
                    DeleteWorkspaceRequest {
                        workspace: Some(workspace_to_proto(&workspace(name))),
                    },
                    principal.clone(),
                ))
                .await
                .map(|_| ())
        }

        async fn members(
            &self,
            principal: &Principal,
            name: &str,
        ) -> Result<Vec<WorkspaceMember>, Status> {
            self.service
                .list_workspace_members(request(
                    ListWorkspaceMembersRequest {
                        workspace: Some(workspace_to_proto(&workspace(name))),
                    },
                    principal.clone(),
                ))
                .await
                .map(|response| response.into_inner().members)
        }

        async fn add(
            &self,
            principal: &Principal,
            name: &str,
            user_id: &str,
            role: WorkspaceRole,
        ) -> Result<WorkspaceMember, Status> {
            self.service
                .add_workspace_member(request(
                    AddWorkspaceMemberRequest {
                        workspace: Some(workspace_to_proto(&workspace(name))),
                        user_id: user_id.to_string(),
                        role: role.into(),
                    },
                    principal.clone(),
                ))
                .await
                .map(|response| response.into_inner().member.expect("granted membership"))
        }

        async fn remove(
            &self,
            principal: &Principal,
            name: &str,
            user_id: &str,
        ) -> Result<(), Status> {
            self.service
                .remove_workspace_member(request(
                    RemoveWorkspaceMemberRequest {
                        workspace: Some(workspace_to_proto(&workspace(name))),
                        user_id: user_id.to_string(),
                    },
                    principal.clone(),
                ))
                .await
                .map(|_| ())
        }

        async fn role_of(&self, name: &str, user_id: &str) -> Option<MemberRole> {
            let mut session = self.db.as_ref();
            session
                .workspace_members()
                .role_for_user_id(name, user_id)
                .await
                .expect("read role")
        }

        /// Provisions one directory user through the production login seam, so
        /// the `user_id` the service is handed is the one a real login carries.
        async fn seed_user(&self, subject: &str) -> String {
            let provisioned = self
                .db
                .user_state()
                .provision_login(LoginIdentity {
                    issuer: "https://issuer.test/workspace-service",
                    subject,
                    display_name: Some(&format!("Seeded {subject}")),
                    principal_claim: subject,
                    now_unix_nanos: 1,
                })
                .await
                .expect("provision user");
            match provisioned {
                LoginProvisioning::Provisioned(user) => user.user_id,
                LoginProvisioning::IssuerMismatch { stored_issuer } => {
                    panic!("expected a provisioned user, got a mismatch with {stored_issuer}")
                }
            }
        }
    }

    async fn shared_deployment() -> Deployment {
        deployment(WorkspaceAuthorizer::new).await
    }

    async fn local_deployment() -> Deployment {
        deployment(WorkspaceAuthorizer::trusting_local_principal).await
    }

    async fn deployment(
        authorizer: impl FnOnce(Arc<CoralDb>) -> WorkspaceAuthorizer,
    ) -> Deployment {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("the default test config should be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        let db = Arc::new(db);
        let manager = WorkspaceManager::new_for_tests(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            layout,
            None,
            Arc::clone(&db),
        );
        Deployment {
            _temp: temp,
            service: WorkspaceService::new(manager, authorizer(Arc::clone(&db))),
            db,
        }
    }

    fn request<T>(message: T, principal: Principal) -> Request<T> {
        let mut request = Request::new(message);
        request
            .extensions_mut()
            .insert(RequestContext::new(principal));
        request
    }

    fn federated(user_id: &str) -> Principal {
        Principal::parse(user_id, PrincipalKind::User).expect("federated principal")
    }

    fn workspace(name: &str) -> WorkspaceName {
        WorkspaceName::parse(name).expect("workspace name")
    }

    /// Replaces the workspace name the caller themselves asked about, so two
    /// answers about different names are still comparable.
    fn normalize(message: &str, name: &str) -> String {
        message.replace(name, "<workspace>")
    }

    fn reasons(status: &Status) -> Vec<String> {
        status
            .get_error_details_vec()
            .into_iter()
            .filter_map(|detail| match detail {
                ErrorDetail::ErrorInfo(info) => Some(info.reason),
                _ => None,
            })
            .collect()
    }
}
