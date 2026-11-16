use super::workspaces::{administration_error, change, error, invalid, state, workspace_id};
use super::{GrpcServices, management as wire};
use coral_administration::GrantedSecretStore;
use coral_secret_store::{ReferenceForm, SecretStoreKind, SecretStoreName, StoreStatus};
use coral_workspace::ChangeContext;
use tonic::{Request, Response, Status};

fn name(store: &str) -> String {
    format!("secretStores/{store}")
}

const fn status(status: StoreStatus) -> wire::SecretStoreState {
    match status {
        StoreStatus::Available => wire::SecretStoreState::Available,
        StoreStatus::NeedsEnterprise => wire::SecretStoreState::NeedsEnterprise,
        StoreStatus::KindMissing => wire::SecretStoreState::KindMissing,
    }
}

fn defined(store: &str, kind: &SecretStoreKind, state: StoreStatus) -> wire::SecretStore {
    wire::SecretStore {
        name: name(store),
        kind: kind.name().to_owned(),
        kind_label: kind.label().to_owned(),
        state: status(state).into(),
        reference_form: None,
    }
}

fn reference_form(form: &ReferenceForm) -> wire::SecretReferenceForm {
    wire::SecretReferenceForm {
        path_label: form.path_label.clone().into_owned(),
        path_prefix: form.path_prefix.clone().unwrap_or_default(),
        key_label: form
            .key
            .as_ref()
            .map_or_default(|key| key.label.clone().into_owned()),
        key_required: form.key.as_ref().is_some_and(|key| key.required),
    }
}

fn store_name(name: &str) -> Result<SecretStoreName, Status> {
    name.strip_prefix("secretStores/")
        .and_then(|store| SecretStoreName::parse(store).ok())
        .ok_or_else(|| invalid("secret store name is invalid"))
}

fn granted(store: &GrantedSecretStore) -> wire::SecretStore {
    match &store.defined {
        Some(described) => wire::SecretStore {
            reference_form: Some(reference_form(&described.reference_form)),
            ..defined(&store.name, &described.kind, described.status)
        },
        None => wire::SecretStore {
            name: name(&store.name),
            state: wire::SecretStoreState::Removed.into(),
            ..wire::SecretStore::default()
        },
    }
}

impl GrpcServices {
    async fn request_change<T>(
        &self,
        request: &Request<T>,
        request_id: &str,
    ) -> Result<ChangeContext, Status> {
        change(self.management_user(request).await?)?
            .with_request_id(request_id)
            .map_err(error)
    }
}

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
            .map(|(store, kind, state)| defined(store.as_str(), &kind, state))
            .collect();
        Ok(Response::new(wire::ListSecretStoresResponse {
            secret_stores,
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.list_secret_store_grants")]
    async fn list_secret_store_grants(
        &self,
        request: Request<wire::ListSecretStoreGrantsRequest>,
    ) -> Result<Response<wire::ListSecretStoreGrantsResponse>, Status> {
        let actor = self.management_user(&request).await?;
        let grants = self
            .secret_stores
            .list_grants(&actor)
            .await
            .map_err(error)?
            .into_iter()
            .map(|grant| wire::SecretStoreGrant {
                workspace: format!("workspaces/{}", grant.workspace.id()),
                workspace_display_name: grant.workspace.display_name().as_str().into(),
                workspace_state: state(grant.workspace.state()).into(),
                secret_store: Some(granted(&grant.store)),
            })
            .collect();
        Ok(Response::new(wire::ListSecretStoreGrantsResponse {
            grants,
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.list_workspace_secret_stores")]
    async fn list_workspace_secret_stores(
        &self,
        request: Request<wire::ListWorkspaceSecretStoresRequest>,
    ) -> Result<Response<wire::ListWorkspaceSecretStoresResponse>, Status> {
        let actor = self.management_user(&request).await?;
        let workspace = workspace_id(&request.get_ref().parent)?;
        let secret_stores = self
            .secret_stores
            .list_for_workspace(&actor, &workspace)
            .await
            .map_err(error)?
            .iter()
            .map(granted)
            .collect();
        Ok(Response::new(wire::ListWorkspaceSecretStoresResponse {
            secret_stores,
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.grant_secret_store")]
    async fn grant_secret_store(
        &self,
        request: Request<wire::GrantSecretStoreRequest>,
    ) -> Result<Response<wire::GrantSecretStoreResponse>, Status> {
        let context = self
            .request_change(&request, &request.get_ref().request_id)
            .await?;
        let input = request.into_inner();
        self.secret_stores
            .grant(
                &context,
                &workspace_id(&input.workspace)?,
                &store_name(&input.secret_store)?,
            )
            .await
            .map_err(administration_error)?;
        Ok(Response::new(wire::GrantSecretStoreResponse {}))
    }

    #[tracing::instrument(skip_all, name = "coral.private.revoke_secret_store")]
    async fn revoke_secret_store(
        &self,
        request: Request<wire::RevokeSecretStoreRequest>,
    ) -> Result<Response<wire::RevokeSecretStoreResponse>, Status> {
        let context = self
            .request_change(&request, &request.get_ref().request_id)
            .await?;
        let input = request.into_inner();
        self.secret_stores
            .revoke(
                &context,
                &workspace_id(&input.workspace)?,
                &store_name(&input.secret_store)?,
            )
            .await
            .map_err(administration_error)?;
        Ok(Response::new(wire::RevokeSecretStoreResponse {}))
    }
}
