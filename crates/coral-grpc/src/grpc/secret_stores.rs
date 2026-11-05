use super::workspaces::error;
use super::{GrpcServices, management as wire};
use coral_secret_store::SecretStoreKind;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl wire::secret_store_service_server::SecretStoreService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.list_secret_stores")]
    async fn list_secret_stores(
        &self,
        request: Request<wire::ListSecretStoresRequest>,
    ) -> Result<Response<wire::ListSecretStoresResponse>, Status> {
        let actor = self.management_user(&request).await?;
        let secret_stores = self
            .secret_stores
            .list(&actor)
            .await
            .map_err(error)?
            .into_iter()
            .map(|(name, kind)| wire::SecretStore {
                name: format!("secretStores/{}", name.as_str()),
                kind: match kind {
                    SecretStoreKind::KvV2 => wire::SecretStoreKind::KvV2,
                }
                .into(),
            })
            .collect();
        Ok(Response::new(wire::ListSecretStoresResponse {
            secret_stores,
        }))
    }
}
