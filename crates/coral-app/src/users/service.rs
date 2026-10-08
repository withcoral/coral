//! Implements the gRPC `UserService` for directory reads.

use coral_api::v1::user_service_server::UserService as UserServiceApi;
use coral_api::v1::{
    GetCurrentUserRequest, GetCurrentUserResponse, ListUsersRequest, ListUsersResponse,
    User as ProtoUser,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::transport::{grpc_span, instrument_grpc, request_context};
use crate::users::manager::UserManager;
use crate::users::model::User;

#[derive(Clone)]
pub(crate) struct UserService {
    users: UserManager,
}

impl UserService {
    pub(crate) const fn new(user_manager: UserManager) -> Self {
        Self {
            users: user_manager,
        }
    }
}

#[tonic::async_trait]
impl UserServiceApi for UserService {
    async fn get_current_user(
        &self,
        request: Request<GetCurrentUserRequest>,
    ) -> Result<Response<GetCurrentUserResponse>, Status> {
        let span = grpc_span(&request);
        let users = self.users.clone();
        let principal = request_context(&request)?.principal().clone();
        instrument_grpc(span, async move {
            let user = users.current_user(&principal).await.map_err(app_status)?;
            Ok(Response::new(GetCurrentUserResponse {
                user: Some(user_to_proto(user)),
            }))
        })
        .await
    }

    async fn list_users(
        &self,
        request: Request<ListUsersRequest>,
    ) -> Result<Response<ListUsersResponse>, Status> {
        let span = grpc_span(&request);
        let users = self.users.clone();
        let principal = request_context(&request)?.principal().clone();
        instrument_grpc(span, async move {
            let directory = users.list_users(&principal).await.map_err(app_status)?;
            Ok(Response::new(ListUsersResponse {
                users: directory.into_iter().map(user_to_proto).collect(),
            }))
        })
        .await
    }
}

/// Projects a directory view onto the wire. The domain view already carries
/// nothing but the two client-visible fields, so this cannot widen it.
fn user_to_proto(user: User) -> ProtoUser {
    ProtoUser {
        user_id: user.user_id,
        display_name: user.display_name.unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use coral_api::v1::{GetCurrentUserRequest, ListUsersRequest};
    use tempfile::{TempDir, tempdir};
    use tonic::{Code, Request};
    use tonic_types::StatusExt as _;

    use super::{UserService, UserServiceApi};
    use crate::identity::{Principal, PrincipalKind};
    use crate::request_context::RequestContext;
    use crate::state::db::{CoralDb, LoginIdentity, LoginProvisioning, ResolvedDatabaseConfig};
    use crate::users::manager::UserManager;
    use crate::workspaces::authorization::WorkspaceAuthorizer;

    const ISSUER: &str = "https://issuer.test/authorization";

    #[tokio::test]
    async fn the_current_user_response_carries_the_internal_id_and_nothing_upstream() {
        let (_temp, db) = migrated_database().await;
        let ada = seed_user(&db, "ada-subject", Some("Ada"), 10).await;
        let service = shared_deployment(&db);

        let response = service
            .get_current_user(request(GetCurrentUserRequest {}, federated(&ada)))
            .await
            .expect("a provisioned caller reads their own entry")
            .into_inner();

        let user = response.user.expect("the response carries the caller");
        assert_eq!(user.user_id, ada);
        assert_eq!(user.display_name, "Ada");
        // The stored row also holds the issuer and subject that authenticate
        // Ada; the id she is handed is neither.
        assert_ne!(user.user_id, "ada-subject");
    }

    /// A caller this deployment does not admit must be denied without the
    /// denial doubling as a directory oracle, so it carries no Coral reason.
    #[tokio::test]
    async fn an_injected_local_principal_is_denied_on_both_reads() {
        let (_temp, db) = migrated_database().await;
        let service = shared_deployment(&db);

        for status in [
            service
                .get_current_user(request(GetCurrentUserRequest {}, Principal::local()))
                .await
                .expect_err("the local principal is not admitted here"),
            service
                .list_users(request(ListUsersRequest {}, Principal::local()))
                .await
                .expect_err("the local principal is not admitted here"),
        ] {
            assert_eq!(status.code(), Code::PermissionDenied);
            assert!(status.get_error_details_vec().is_empty());
        }
    }

    #[tokio::test]
    async fn a_provisioned_caller_without_a_directory_row_is_reported_absent() {
        let (_temp, db) = migrated_database().await;
        let service = shared_deployment(&db);

        let status = service
            .get_current_user(request(
                GetCurrentUserRequest {},
                federated("never-logged-in"),
            ))
            .await
            .expect_err("an unprovisioned caller has no entry");

        assert_eq!(status.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn only_an_owner_reads_the_directory_over_the_wire() {
        let (_temp, db) = migrated_database().await;
        let owner = seed_user(&db, "owner-subject", Some("Owner"), 10).await;
        let stranger = seed_user(&db, "stranger-subject", None, 20).await;
        create_owned_workspace(&db, "team", &owner).await;
        let service = shared_deployment(&db);

        let response = service
            .list_users(request(ListUsersRequest {}, federated(&owner)))
            .await
            .expect("an owner reads the directory")
            .into_inner();

        // Pinning every field of every row is what proves the projection: no
        // issuer or subject rode along, and a missing display name is the
        // empty string rather than a synthesized one.
        assert_eq!(
            response
                .users
                .iter()
                .map(|user| (user.user_id.as_str(), user.display_name.as_str()))
                .collect::<Vec<_>>(),
            vec![(owner.as_str(), "Owner"), (stranger.as_str(), "")],
        );

        assert_eq!(
            service
                .list_users(request(ListUsersRequest {}, federated(&stranger)))
                .await
                .expect_err("a caller who owns no workspace is denied")
                .code(),
            Code::PermissionDenied,
        );
    }

    /// The adapter reads its caller from request context, so a request that
    /// never passed the authenticating layer must fail closed rather than
    /// default to anybody.
    #[tokio::test]
    async fn a_request_without_authenticated_context_is_refused() {
        let (_temp, db) = migrated_database().await;
        let service = shared_deployment(&db);

        assert_eq!(
            service
                .list_users(Request::new(ListUsersRequest {}))
                .await
                .expect_err("no principal was selected for this request")
                .code(),
            Code::Internal,
        );
    }

    fn shared_deployment(db: &Arc<CoralDb>) -> UserService {
        UserService::new(UserManager::new(
            Arc::clone(db),
            WorkspaceAuthorizer::new(Arc::clone(db)),
        ))
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

    async fn migrated_database() -> (TempDir, Arc<CoralDb>) {
        let temp = tempdir().expect("temp dir");
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        (temp, Arc::new(db))
    }

    /// Provisions one directory user through the production login seam, so the
    /// rows under test are the ones a real login would write. Distinct login
    /// times are what fix the listing order the assertions above rely on.
    async fn seed_user(
        db: &CoralDb,
        subject: &str,
        display_name: Option<&str>,
        created_at_unix_nanos: i64,
    ) -> String {
        let provisioned = db
            .user_state()
            .provision_login(LoginIdentity {
                issuer: ISSUER,
                subject,
                display_name,
                principal_claim: subject,
                now_unix_nanos: created_at_unix_nanos,
            })
            .await
            .expect("provision user");
        match provisioned {
            LoginProvisioning::Provisioned(user) => user.user_id,
            LoginProvisioning::IssuerMismatch { stored_issuer } => {
                panic!("expected a provisioned user, got a mismatch with issuer {stored_issuer}")
            }
        }
    }

    async fn create_owned_workspace(db: &CoralDb, workspace_id: &str, owner_user_id: &str) {
        db.workspace_state()
            .create_owned_by(workspace_id, owner_user_id, 1)
            .await
            .expect("create owned workspace");
    }
}
