//! Implements the gRPC `UserService` without exposing upstream identity data.

use coral_api::v1::user_service_server::UserService as UserServiceApi;
use coral_api::v1::{
    GetCurrentUserRequest, GetCurrentUserResponse, ListUsersRequest, ListUsersResponse,
    User as ProtoUser,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::transport::{grpc_span, instrument_grpc, request_context, workspace_to_proto};
use crate::users::{CurrentUser, UserManager, UserView};

#[derive(Clone)]
pub(crate) struct UserService {
    users: UserManager,
}

impl UserService {
    pub(crate) fn new(users: UserManager) -> Self {
        Self { users }
    }
}

#[tonic::async_trait]
impl UserServiceApi for UserService {
    async fn list_users(
        &self,
        request: Request<ListUsersRequest>,
    ) -> Result<Response<ListUsersResponse>, Status> {
        let span = grpc_span(&request);
        let principal = request_context(&request)?.principal().clone();
        let users = self.users.clone();
        instrument_grpc(span, async move {
            let users = users
                .list_users(&principal)
                .await
                .map_err(app_status)?
                .into_iter()
                .map(user_to_proto)
                .collect();
            Ok(Response::new(ListUsersResponse { users }))
        })
        .await
    }

    async fn get_current_user(
        &self,
        request: Request<GetCurrentUserRequest>,
    ) -> Result<Response<GetCurrentUserResponse>, Status> {
        let span = grpc_span(&request);
        let principal = request_context(&request)?.principal().clone();
        let users = self.users.clone();
        instrument_grpc(span, async move {
            let current = users
                .get_current_user(&principal)
                .await
                .map_err(app_status)?;
            Ok(Response::new(current_user_to_proto(current)))
        })
        .await
    }
}

fn user_to_proto(user: UserView) -> ProtoUser {
    ProtoUser {
        user_id: user.user_id,
        display_name: user.display_name.unwrap_or_default(),
    }
}

fn current_user_to_proto(current: CurrentUser) -> GetCurrentUserResponse {
    GetCurrentUserResponse {
        user: Some(user_to_proto(current.user)),
        default_workspace: Some(workspace_to_proto(&current.default_workspace)),
    }
}
