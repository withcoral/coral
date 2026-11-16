//! OAuth Clients for this crate's tests, authenticated the way a token
//! request authenticates them.

use std::sync::Arc;

use coral_db::Database;
use coral_test_support::{DatabaseSystem, TestDatabase};

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

/// Runs `update` on `table` past its CHECK constraints, to store a row the
/// schema forbids, and returns the rows it changed. `SQLite` switches the
/// checks off for the fixture's own connection. Postgres has no such switch,
/// so `table` loses its checks.
pub(crate) async fn rewrite_past_checks(database: &TestDatabase, table: &str, update: &str) -> u64 {
    let sql = match coral_test_support::selected_database().unwrap() {
        DatabaseSystem::Sqlite => format!("PRAGMA ignore_check_constraints = ON; {update}"),
        DatabaseSystem::Postgres => format!(
            "DO $$ DECLARE name text; BEGIN \
             FOR name IN SELECT conname FROM pg_constraint \
             WHERE conrelid = '{table}'::regclass AND contype = 'c' LOOP \
             EXECUTE format('ALTER TABLE {table} DROP CONSTRAINT %I', name); \
             END LOOP; END $$; {update}"
        ),
    };
    database.execute_fixture_sql(&sql).await.unwrap()
}
