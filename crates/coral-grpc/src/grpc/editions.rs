use super::{GrpcServices, management as wire};
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl wire::edition_service_server::EditionService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.get_edition")]
    async fn get_edition(
        &self,
        request: Request<wire::GetEditionRequest>,
    ) -> Result<Response<wire::GetEditionResponse>, Status> {
        self.management_user(&request).await?;
        let report = self.edition.report();
        Ok(Response::new(wire::GetEditionResponse {
            edition: edition(report.edition).into(),
            features: report
                .features
                .into_iter()
                .map(|(feature, state)| wire::EnterpriseFeature {
                    id: feature.id().as_str().to_owned(),
                    display_name: feature.display_name().to_owned(),
                    state: feature_state(state).into(),
                })
                .collect(),
        }))
    }
}

fn edition(edition: coral_edition::Edition) -> wire::Edition {
    match edition {
        coral_edition::Edition::Community => wire::Edition::Community,
        coral_edition::Edition::Enterprise => wire::Edition::Enterprise,
    }
}

fn feature_state(state: coral_edition::FeatureState) -> wire::EnterpriseFeatureState {
    match state {
        coral_edition::FeatureState::Enabled => wire::EnterpriseFeatureState::Enabled,
        coral_edition::FeatureState::NeedsEnterprise => {
            wire::EnterpriseFeatureState::NeedsEnterprise
        }
    }
}
