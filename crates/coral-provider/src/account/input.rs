//! Test fixture choreography through acquisition, confirmation and authorization interfaces.
use coral_catalog::AuthenticationConfiguration;
use coral_credentials::{CredentialValues, Secret};
use coral_workspace::ChangeContext;
use coral_workspace::WorkspaceId;

use super::AccountModule;
use crate::{Error, ProviderSetup, ProviderSetupSource};

impl AccountModule {
    pub(crate) async fn prepare_remote_fixture(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        source: ProviderSetupSource,
        credential: Option<Secret>,
    ) -> Result<ProviderSetup, Error> {
        if matches!(source, ProviderSetupSource::OpenApiUrl(_)) {
            return self
                .providers
                .prepare_setup(context, owner, source, None)
                .await;
        }
        let setup = self
            .providers
            .prepare_setup(context, owner, source, None)
            .await?;
        let review = self
            .providers
            .review_setup_configuration(context.actor(), owner, setup.id())
            .await?;
        let mut values = review.document.ok_or(Error::AccountInput)?.suggested;
        if credential.is_some() || values.authentication.is_none() {
            values.authentication = Some(AuthenticationConfiguration::Bearer);
        }
        let confirmed = self
            .providers
            .confirm_setup_configuration(context, owner, setup.id(), review.revision, values)
            .await?;
        assert!(
            confirmed.errors.is_empty(),
            "fixture configuration: {:?}",
            confirmed.errors
        );
        if let Some(secret) = credential {
            Box::pin(self.authorize_setup(
                context,
                owner,
                setup.id(),
                confirmed.revision,
                CredentialValues::token(secret),
            ))
            .await?;
        }
        self.providers
            .get_setup(context.actor(), owner, setup.id())
            .await
    }
}
