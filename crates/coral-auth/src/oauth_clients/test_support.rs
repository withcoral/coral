//! OAuth Clients for this crate's tests, authenticated the way a token
//! request authenticates them.

use std::sync::Arc;

use coral_db::Database;

use super::{
    AuthenticatedClient, ClientMetadataError, ClientMetadataResolver, OAuthClientRegistration,
    OAuthClients, PresentedCredential,
};

/// A metadata resolver for tests that must never fetch a document.
pub(crate) struct NoFetch;

#[async_trait::async_trait]
impl ClientMetadataResolver for NoFetch {
    async fn resolve(&self, _: &str) -> Result<OAuthClientRegistration, ClientMetadataError> {
        panic!("registration and client authentication must never fetch URLs")
    }
}

/// The public URL client `client_id`, authenticated as a token request that
/// presents no credential authenticates it.
pub(crate) async fn authenticated(database: Arc<Database>, client_id: &str) -> AuthenticatedClient {
    OAuthClients::new(database, Arc::new(NoFetch))
        .authenticate(client_id, &PresentedCredential::None)
        .await
        .expect("a URL client authenticates as a public client")
}
