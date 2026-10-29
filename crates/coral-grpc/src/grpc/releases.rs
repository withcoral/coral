use super::{GrpcServices, management as wire};
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl wire::release_service_server::ReleaseService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.get_newer_release")]
    async fn get_newer_release(
        &self,
        request: Request<wire::GetNewerReleaseRequest>,
    ) -> Result<Response<wire::GetNewerReleaseResponse>, Status> {
        self.management_user(&request).await?;
        let release = self
            .releases
            .as_ref()
            .and_then(coral_release::ReleaseCheck::newer_release)
            .map(|release| wire::NewerRelease {
                version: release.version().to_string(),
                notes_url: release.notes_url().to_owned(),
            });
        Ok(Response::new(wire::GetNewerReleaseResponse { release }))
    }
}
