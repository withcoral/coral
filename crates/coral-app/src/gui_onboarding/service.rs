//! Implements the gRPC `GuiOnboardingService`.

use coral_api::v1::gui_onboarding_service_server::GuiOnboardingService as GuiOnboardingServiceApi;
use coral_api::v1::{
    CompleteGuiOnboardingRequest, CompleteGuiOnboardingResponse, GetGuiOnboardingStateRequest,
    GetGuiOnboardingStateResponse,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::gui_onboarding::manager::GuiOnboardingManager;
use crate::identity::{Principal, PrincipalKind};
use crate::transport::{grpc_span, instrument_grpc, request_context};

#[derive(Clone)]
pub(crate) struct GuiOnboardingService {
    gui_onboarding: GuiOnboardingManager,
}

impl GuiOnboardingService {
    pub(crate) fn new(gui_onboarding: GuiOnboardingManager) -> Self {
        Self { gui_onboarding }
    }
}

#[tonic::async_trait]
impl GuiOnboardingServiceApi for GuiOnboardingService {
    async fn get_gui_onboarding_state(
        &self,
        request: Request<GetGuiOnboardingStateRequest>,
    ) -> Result<Response<GetGuiOnboardingStateResponse>, Status> {
        let span = grpc_span(&request);
        let gui_onboarding = self.gui_onboarding.clone();
        instrument_grpc(span, async move {
            let principal = request_user_principal(&request)?;
            let completed = gui_onboarding
                .is_completed(&principal)
                .await
                .map_err(app_status)?;
            Ok(Response::new(GetGuiOnboardingStateResponse { completed }))
        })
        .await
    }

    async fn complete_gui_onboarding(
        &self,
        request: Request<CompleteGuiOnboardingRequest>,
    ) -> Result<Response<CompleteGuiOnboardingResponse>, Status> {
        let span = grpc_span(&request);
        let gui_onboarding = self.gui_onboarding.clone();
        instrument_grpc(span, async move {
            let principal = request_user_principal(&request)?;
            gui_onboarding
                .complete(&principal)
                .await
                .map_err(app_status)?;
            Ok(Response::new(CompleteGuiOnboardingResponse {}))
        })
        .await
    }
}

fn request_user_principal<T>(request: &Request<T>) -> Result<Principal, Status> {
    let principal = request_context(request)?.principal().clone();
    if principal.kind() != PrincipalKind::User {
        return Err(Status::permission_denied(
            "GUI onboarding is only available to user principals",
        ));
    }
    Ok(principal)
}
