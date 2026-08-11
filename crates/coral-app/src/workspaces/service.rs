//! Implements the gRPC `WorkspaceService` for workspace lifecycle APIs.

use coral_api::v1::workspace_service_server::WorkspaceService as WorkspaceServiceApi;
use coral_api::v1::{
    AddWorkspaceMemberRequest, AddWorkspaceMemberResponse, CreateWorkspaceRequest,
    CreateWorkspaceResponse, DeleteWorkspaceRequest, DeleteWorkspaceResponse,
    ListWorkspaceMembersRequest, ListWorkspaceMembersResponse, ListWorkspacesRequest,
    ListWorkspacesResponse, RemoveWorkspaceMemberRequest, RemoveWorkspaceMemberResponse, Workspace,
    WorkspaceMember, WorkspaceMembership, WorkspaceRole,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::{AppError, app_status};
use crate::identity::Principal;
use crate::transport::{
    grpc_span, instrument_grpc, request_context, workspace_name_from_proto, workspace_to_proto,
};
use crate::workspaces::{
    MemberRole, WorkspaceAction, WorkspaceAuthorizer, WorkspaceManager, WorkspaceRecord,
};

#[derive(Clone)]
pub(crate) struct WorkspaceService {
    workspaces: WorkspaceManager,
    authorizer: Option<WorkspaceAuthorizer>,
}

impl WorkspaceService {
    pub(crate) fn new(workspace_manager: WorkspaceManager) -> Self {
        Self {
            workspaces: workspace_manager,
            authorizer: None,
        }
    }

    pub(crate) fn with_authorizer(mut self, authorizer: WorkspaceAuthorizer) -> Self {
        self.authorizer = Some(authorizer);
        self
    }

    async fn authorize(
        &self,
        principal: &Principal,
        workspace: &crate::workspaces::WorkspaceName,
    ) -> Result<(), AppError> {
        if let Some(authorizer) = &self.authorizer {
            return authorizer
                .authorize(principal, workspace, WorkspaceAction::Manage)
                .await;
        }
        if principal.is_local() {
            Ok(())
        } else {
            Err(AppError::PermissionDenied(
                "workspace authorization is unavailable".to_string(),
            ))
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
        let principal = request_context(&request)?.principal().clone();
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let memberships = workspaces
                .list_workspaces_for(&principal)
                .await
                .map_err(app_status)?
                .into_iter()
                .map(|(workspace, role)| WorkspaceMembership {
                    workspace: Some(workspace_record_to_proto(&workspace)),
                    role: member_role_to_proto(role) as i32,
                })
                .collect();
            Ok(Response::new(ListWorkspacesResponse { memberships }))
        })
        .await
    }

    async fn create_workspace(
        &self,
        request: Request<CreateWorkspaceRequest>,
    ) -> Result<Response<CreateWorkspaceResponse>, Status> {
        let span = grpc_span(&request);
        let principal = request_context(&request)?.principal().clone();
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            // A creator with no directory row cannot be recorded as owner, and a
            // workspace nobody owns is unreachable by everyone. Refuse instead
            // of creating one — on a shared deployment that is a host process,
            // which has no business creating workspaces nobody can open.
            let workspace = workspaces
                .create_workspace_for_user(&workspace_name, &principal)
                .await
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
        let principal = request_context(&request)?.principal().clone();
        let service = self.clone();
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            service
                .authorize(&principal, &workspace_name)
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

    async fn list_workspace_members(
        &self,
        request: Request<ListWorkspaceMembersRequest>,
    ) -> Result<Response<ListWorkspaceMembersResponse>, Status> {
        let span = grpc_span(&request);
        let principal = request_context(&request)?.principal().clone();
        let service = self.clone();
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            service
                .authorize(&principal, &workspace_name)
                .await
                .map_err(app_status)?;
            let members = workspaces
                .list_workspace_members(&workspace_name)
                .await
                .map_err(app_status)?
                .into_iter()
                .map(workspace_member_to_proto)
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
        let principal = request_context(&request)?.principal().clone();
        let service = self.clone();
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            service
                .authorize(&principal, &workspace_name)
                .await
                .map_err(app_status)?;
            let member = request.member.ok_or_else(|| {
                app_status(AppError::InvalidInput(
                    "missing workspace member".to_string(),
                ))
            })?;
            let role = member_role_from_proto(member.role).map_err(app_status)?;
            let member = workspaces
                .add_workspace_member(&workspace_name, &member.user_id, role)
                .await
                .map_err(app_status)?;
            Ok(Response::new(AddWorkspaceMemberResponse {
                member: Some(workspace_member_to_proto(member)),
            }))
        })
        .await
    }

    async fn remove_workspace_member(
        &self,
        request: Request<RemoveWorkspaceMemberRequest>,
    ) -> Result<Response<RemoveWorkspaceMemberResponse>, Status> {
        let span = grpc_span(&request);
        let principal = request_context(&request)?.principal().clone();
        let service = self.clone();
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            service
                .authorize(&principal, &workspace_name)
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

fn member_role_from_proto(role: i32) -> Result<MemberRole, AppError> {
    match WorkspaceRole::try_from(role) {
        Ok(WorkspaceRole::Owner) => Ok(MemberRole::Owner),
        Ok(WorkspaceRole::Member) => Ok(MemberRole::Member),
        Ok(WorkspaceRole::Unspecified) | Err(_) => Err(AppError::InvalidInput(
            "workspace member role must be OWNER or MEMBER".to_string(),
        )),
    }
}

fn member_role_to_proto(role: MemberRole) -> WorkspaceRole {
    match role {
        MemberRole::Owner => WorkspaceRole::Owner,
        MemberRole::Member => WorkspaceRole::Member,
    }
}

fn workspace_member_to_proto(member: crate::state::db::WorkspaceMemberView) -> WorkspaceMember {
    WorkspaceMember {
        user_id: member.user_id,
        role: member_role_to_proto(member.role) as i32,
        display_name: member.display_name.unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use coral_api::v1::{AddWorkspaceMemberRequest, Workspace, WorkspaceMember, WorkspaceRole};
    use tempfile::TempDir;
    use tonic::{Code, Request};

    use super::{
        WorkspaceService, WorkspaceServiceApi, member_role_from_proto, member_role_to_proto,
    };
    use crate::credentials::{CredentialManager, CredentialStore};
    use crate::identity::{Principal, PrincipalKind};
    use crate::request_context::RequestContext;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig, UpsertLoginOutcome};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::{MemberRole, WorkspaceAuthorizer, WorkspaceManager, WorkspaceName};

    #[test]
    fn workspace_member_roles_are_strict_at_the_transport_edge() {
        assert_eq!(
            member_role_from_proto(WorkspaceRole::Owner as i32).expect("owner role"),
            MemberRole::Owner
        );
        assert_eq!(
            member_role_from_proto(WorkspaceRole::Member as i32).expect("member role"),
            MemberRole::Member
        );
        member_role_from_proto(WorkspaceRole::Unspecified as i32)
            .expect_err("unspecified role must be rejected");
        member_role_from_proto(i32::MAX).expect_err("unknown role must be rejected");
        assert_eq!(
            member_role_to_proto(MemberRole::Owner),
            WorkspaceRole::Owner
        );
        assert_eq!(
            member_role_to_proto(MemberRole::Member),
            WorkspaceRole::Member
        );
    }

    #[tokio::test]
    async fn add_workspace_member_enforces_authorization_and_frozen_add_semantics() {
        let fixture = service_fixture().await;

        let added = WorkspaceServiceApi::add_workspace_member(
            &fixture.service,
            add_request(
                &fixture,
                fixture.owner.clone(),
                &fixture.member_id,
                WorkspaceRole::Member as i32,
            ),
        )
        .await
        .expect("owner adds member")
        .into_inner()
        .member
        .expect("added member");
        assert_eq!(added.user_id, fixture.member_id);
        assert_eq!(added.role, WorkspaceRole::Member as i32);
        assert_eq!(added.display_name, "Member");

        WorkspaceServiceApi::add_workspace_member(
            &fixture.service,
            add_request(
                &fixture,
                fixture.owner.clone(),
                &fixture.member_id,
                WorkspaceRole::Member as i32,
            ),
        )
        .await
        .expect("identical add remains idempotent");
        let promoted = WorkspaceServiceApi::add_workspace_member(
            &fixture.service,
            add_request(
                &fixture,
                fixture.owner.clone(),
                &fixture.member_id,
                WorkspaceRole::Owner as i32,
            ),
        )
        .await
        .expect("adding an existing member with a new role moves them to it")
        .into_inner()
        .member
        .expect("promoted membership");
        assert_eq!(promoted.role, WorkspaceRole::Owner as i32);
        let demoted = WorkspaceServiceApi::add_workspace_member(
            &fixture.service,
            add_request(
                &fixture,
                fixture.owner.clone(),
                &fixture.member_id,
                WorkspaceRole::Member as i32,
            ),
        )
        .await
        .expect("and moves them back, since the workspace keeps another owner")
        .into_inner()
        .member
        .expect("demoted membership");
        assert_eq!(demoted.role, WorkspaceRole::Member as i32);

        let member = Principal::parse(&fixture.member_id, PrincipalKind::User).expect("member");
        let denied = WorkspaceServiceApi::add_workspace_member(
            &fixture.service,
            add_request(
                &fixture,
                member,
                &fixture.unknown_user_id,
                WorkspaceRole::Unspecified as i32,
            ),
        )
        .await
        .expect_err("member cannot manage membership");
        assert_eq!(denied.code(), Code::PermissionDenied);

        let invalid_role = WorkspaceServiceApi::add_workspace_member(
            &fixture.service,
            add_request(
                &fixture,
                fixture.owner.clone(),
                &fixture.member_id,
                WorkspaceRole::Unspecified as i32,
            ),
        )
        .await
        .expect_err("unspecified role is invalid");
        assert_eq!(invalid_role.code(), Code::InvalidArgument);

        let unknown_user = WorkspaceServiceApi::add_workspace_member(
            &fixture.service,
            add_request(
                &fixture,
                fixture.owner.clone(),
                &fixture.unknown_user_id,
                WorkspaceRole::Member as i32,
            ),
        )
        .await
        .expect_err("unknown user is not addable");
        assert_eq!(unknown_user.code(), Code::NotFound);
    }

    struct ServiceFixture {
        _temp: TempDir,
        service: WorkspaceService,
        workspace: Workspace,
        owner: Principal,
        member_id: String,
        unknown_user_id: String,
    }

    async fn service_fixture() -> ServiceFixture {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("test database must be SQLite")
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open database"),
        );
        db.migrate().await.expect("migrate database");
        let owner_id = provision_user(&db, "owner", Some("Owner")).await;
        let member_id = provision_user(&db, "member", Some("Member")).await;
        let owner = Principal::parse(&owner_id, PrincipalKind::User).expect("owner");
        let workspace_name = WorkspaceName::parse("team").expect("workspace");
        let manager = WorkspaceManager::new_for_tests(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            layout,
            None,
            Arc::clone(&db),
        );
        manager
            .create_workspace_for_user(&workspace_name, &owner)
            .await
            .expect("create workspace");

        ServiceFixture {
            _temp: temp,
            service: WorkspaceService::new(manager).with_authorizer(WorkspaceAuthorizer::new(db)),
            workspace: Workspace {
                name: workspace_name.to_string(),
            },
            owner,
            member_id,
            unknown_user_id: uuid::Uuid::new_v4().to_string(),
        }
    }

    async fn provision_user(db: &CoralDb, subject: &str, display_name: Option<&str>) -> String {
        let UpsertLoginOutcome::Upserted(user) = db
            .upsert_user_and_ensure_default_workspace("issuer", subject, display_name, 1)
            .await
            .expect("provision user")
        else {
            panic!("new subject should create a user")
        };
        user.user_id
    }

    fn add_request(
        fixture: &ServiceFixture,
        principal: Principal,
        user_id: &str,
        role: i32,
    ) -> Request<AddWorkspaceMemberRequest> {
        let mut request = Request::new(AddWorkspaceMemberRequest {
            workspace: Some(fixture.workspace.clone()),
            member: Some(WorkspaceMember {
                user_id: user_id.to_string(),
                role,
                display_name: String::new(),
            }),
        });
        request
            .extensions_mut()
            .insert(RequestContext::new(principal));
        request
    }
}
