//! Single-use, bounded in-memory state for OAuth authorization handshakes.

use std::collections::HashMap;
use std::time::Duration;

use ring::rand::SecureRandom;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio::time::Instant;
use zeroize::{Zeroize as _, Zeroizing};

const OAUTH_STATE_TTL: Duration = Duration::from_mins(5);
const MAX_OAUTH_STATE_ENTRIES_PER_KIND: usize = 4_096;

/// The most approvals one client may hold awaiting confirmation.
///
/// [`MAX_OAUTH_STATE_ENTRIES_PER_KIND`] on its own lets a single client take
/// the whole approval budget. An approval is written when the confirmation
/// page is rendered — before the user has done anything and before the
/// provider is contacted — so anyone holding a resolvable `client_id` and one
/// of its redirect URIs, both public by construction, mints an entry per
/// request. Bounding each client separately keeps that inside the client it
/// came from. DCR makes new client IDs inexpensive, so the deployment-wide
/// approval budget remains authoritative independently of this per-client cap.
///
/// The bound is per client rather than per browser binding because a caller
/// that sends no cookie is handed a fresh binding on every request, so a
/// per-binding limit would bound only the browsers that were never the
/// problem.
const MAX_OAUTH_APPROVALS_PER_CLIENT: usize = 64;

type SecretHash = [u8; 32];

/// Single-use secret naming a stored authorization approval.
///
/// New tickets come from [`Self::generate`], which fills the secret in place
/// from a caller-supplied CSPRNG, so a minted ticket's bytes never exist
/// outside the value that zeroizes them on drop. [`Self::from_bytes`] exists
/// only to rebuild a ticket the browser sent back.
pub(crate) struct OAuthAuthorizationApprovalTicket([u8; 32]);

impl OAuthAuthorizationApprovalTicket {
    /// Draws a fresh ticket from `random`.
    ///
    /// # Errors
    ///
    /// Returns [`StateStoreError::Randomness`] when `random` cannot fill the
    /// ticket.
    pub(super) fn generate(random: &dyn SecureRandom) -> Result<Self, StateStoreError> {
        let mut ticket = Self([0; 32]);
        random
            .fill(&mut ticket.0)
            .map_err(|_error| StateStoreError::Randomness)?;
        Ok(ticket)
    }

    /// Rebuilds a ticket from the bytes carried by a confirmation submission.
    pub(super) fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(super) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Drop for OAuthAuthorizationApprovalTicket {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

pub(crate) struct OAuthAuthorizationApprovalBrowserBinding([u8; 32]);

impl OAuthAuthorizationApprovalBrowserBinding {
    pub(super) fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(super) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Drop for OAuthAuthorizationApprovalBrowserBinding {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct OAuthAuthorizationApprovalRecord {
    pub(crate) client_id: String,
    pub(crate) client_name: String,
    pub(crate) redirect_uri: String,
    pub(crate) client_state: Option<String>,
    pub(crate) code_challenge: String,
    pub(crate) resource: String,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct OAuthAuthorizationSessionRecord {
    pub(crate) client_id: String,
    pub(crate) redirect_uri: String,
    pub(crate) client_state: Option<String>,
    pub(crate) code_challenge: String,
    pub(crate) resource: String,
    pub(crate) oidc_code_verifier: Zeroizing<String>,
    pub(crate) oidc_nonce: String,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct OAuthAuthorizationCodeRecord {
    pub(crate) user_id: String,
    pub(crate) client_id: String,
    pub(crate) redirect_uri: String,
    pub(crate) code_challenge: String,
    pub(crate) resource: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum StateStoreError {
    #[error("OAuth state secrets must not be empty")]
    EmptySecret,
    #[error("OAuth state store has reached its {max_entries}-entry limit")]
    CapacityExceeded { max_entries: usize },
    #[error("OAuth client has reached its {max_entries}-approval limit")]
    ClientApprovalsExceeded { max_entries: usize },
    #[error("OAuth state expiry exceeds the process clock range")]
    ExpiryOverflow,
    #[error("OAuth approval ticket generation failed")]
    Randomness,
}

/// Storage for approvals awaiting the user's confirmation.
#[async_trait::async_trait]
pub(crate) trait ApprovalStore: Send + Sync {
    /// Holds `approval` until the browser bound to it confirms or cancels.
    ///
    /// # Errors
    ///
    /// Returns [`StateStoreError::ClientApprovalsExceeded`] once the client
    /// named by the approval holds too many outstanding entries, and
    /// [`StateStoreError::CapacityExceeded`] once approvals as a whole do. An
    /// implementation is expected to bound both: the entry is written before
    /// the user has acted, so an approval that is never confirmed still costs
    /// storage until it expires.
    async fn store_authorization_approval(
        &self,
        ticket: &OAuthAuthorizationApprovalTicket,
        browser_binding: &OAuthAuthorizationApprovalBrowserBinding,
        approval: OAuthAuthorizationApprovalRecord,
    ) -> Result<(), StateStoreError>;

    async fn take_authorization_approval(
        &self,
        ticket: &OAuthAuthorizationApprovalTicket,
        browser_binding: &OAuthAuthorizationApprovalBrowserBinding,
    ) -> Result<Option<OAuthAuthorizationApprovalRecord>, StateStoreError>;
}

/// Storage for handshakes in flight at the identity provider.
#[async_trait::async_trait]
pub(crate) trait SessionStore: Send + Sync {
    async fn store_authorization_session(
        &self,
        oidc_state: &str,
        session: OAuthAuthorizationSessionRecord,
    ) -> Result<(), StateStoreError>;

    async fn take_authorization_session(
        &self,
        oidc_state: &str,
    ) -> Result<Option<OAuthAuthorizationSessionRecord>, StateStoreError>;
}

/// Storage for authorization codes awaiting redemption.
#[async_trait::async_trait]
pub(crate) trait CodeStore: Send + Sync {
    async fn store_authorization_code(
        &self,
        code: &str,
        authorization: OAuthAuthorizationCodeRecord,
    ) -> Result<(), StateStoreError>;

    async fn take_authorization_code_for_request(
        &self,
        code: &str,
        client_id: &str,
        redirect_uri: &str,
        code_challenge: &str,
        resource: &str,
    ) -> Result<Option<OAuthAuthorizationCodeRecord>, StateStoreError>;
}

pub(crate) struct InMemoryStateStore {
    inner: Mutex<StateMaps>,
    ttl: Duration,
    max_entries_per_kind: usize,
    max_approvals_per_client: usize,
}

impl Default for InMemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStateStore {
    pub(crate) fn new() -> Self {
        Self::with_options(
            OAUTH_STATE_TTL,
            MAX_OAUTH_STATE_ENTRIES_PER_KIND,
            MAX_OAUTH_APPROVALS_PER_CLIENT,
        )
    }

    fn with_options(
        ttl: Duration,
        max_entries_per_kind: usize,
        max_approvals_per_client: usize,
    ) -> Self {
        Self {
            inner: Mutex::new(StateMaps::default()),
            ttl,
            max_entries_per_kind,
            max_approvals_per_client,
        }
    }

    fn expires_at(&self, now: Instant) -> Result<Instant, StateStoreError> {
        now.checked_add(self.ttl)
            .ok_or(StateStoreError::ExpiryOverflow)
    }
}

#[async_trait::async_trait]
impl ApprovalStore for InMemoryStateStore {
    async fn store_authorization_approval(
        &self,
        ticket: &OAuthAuthorizationApprovalTicket,
        browser_binding: &OAuthAuthorizationApprovalBrowserBinding,
        approval: OAuthAuthorizationApprovalRecord,
    ) -> Result<(), StateStoreError> {
        let key = approval_key(ticket, browser_binding);
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        let expires_at = self.expires_at(now)?;
        inner.purge_expired(now);
        let replaces_existing = inner.approvals.contains_key(&key);
        ensure_capacity(
            inner.approvals.len(),
            self.max_entries_per_kind,
            replaces_existing,
        )?;
        if !replaces_existing
            && inner.approvals_for_client(&approval.client_id) >= self.max_approvals_per_client
        {
            return Err(StateStoreError::ClientApprovalsExceeded {
                max_entries: self.max_approvals_per_client,
            });
        }
        inner.approvals.insert(
            key,
            Expiring {
                value: approval,
                expires_at,
            },
        );
        Ok(())
    }

    async fn take_authorization_approval(
        &self,
        ticket: &OAuthAuthorizationApprovalTicket,
        browser_binding: &OAuthAuthorizationApprovalBrowserBinding,
    ) -> Result<Option<OAuthAuthorizationApprovalRecord>, StateStoreError> {
        let key = approval_key(ticket, browser_binding);
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        let entry = inner.approvals.remove(&key);
        Ok(entry
            .filter(|entry| entry.expires_at > now)
            .map(|entry| entry.value))
    }
}

#[async_trait::async_trait]
impl SessionStore for InMemoryStateStore {
    async fn store_authorization_session(
        &self,
        oidc_state: &str,
        session: OAuthAuthorizationSessionRecord,
    ) -> Result<(), StateStoreError> {
        let key = secret_hash(oidc_state)?;
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        let expires_at = self.expires_at(now)?;
        inner.purge_expired(now);
        ensure_capacity(
            inner.sessions.len(),
            self.max_entries_per_kind,
            inner.sessions.contains_key(&key),
        )?;
        inner.sessions.insert(
            key,
            Expiring {
                value: session,
                expires_at,
            },
        );
        Ok(())
    }

    async fn take_authorization_session(
        &self,
        oidc_state: &str,
    ) -> Result<Option<OAuthAuthorizationSessionRecord>, StateStoreError> {
        let key = secret_hash(oidc_state)?;
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        let entry = inner.sessions.remove(&key);
        Ok(entry
            .filter(|entry| entry.expires_at > now)
            .map(|entry| entry.value))
    }
}

#[async_trait::async_trait]
impl CodeStore for InMemoryStateStore {
    async fn store_authorization_code(
        &self,
        code: &str,
        authorization: OAuthAuthorizationCodeRecord,
    ) -> Result<(), StateStoreError> {
        let key = secret_hash(code)?;
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        let expires_at = self.expires_at(now)?;
        inner.purge_expired(now);
        ensure_capacity(
            inner.codes.len(),
            self.max_entries_per_kind,
            inner.codes.contains_key(&key),
        )?;
        inner.codes.insert(
            key,
            Expiring {
                value: authorization,
                expires_at,
            },
        );
        Ok(())
    }

    async fn take_authorization_code_for_request(
        &self,
        code: &str,
        client_id: &str,
        redirect_uri: &str,
        code_challenge: &str,
        resource: &str,
    ) -> Result<Option<OAuthAuthorizationCodeRecord>, StateStoreError> {
        let key = secret_hash(code)?;
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        let Some(entry) = inner.codes.get(&key) else {
            return Ok(None);
        };
        if entry.expires_at <= now {
            inner.codes.remove(&key);
            return Ok(None);
        }
        let authorization = &entry.value;
        if authorization.client_id != client_id
            || authorization.redirect_uri != redirect_uri
            || authorization.code_challenge != code_challenge
            || authorization.resource != resource
        {
            return Ok(None);
        }
        Ok(inner.codes.remove(&key).map(|entry| entry.value))
    }
}

#[derive(Default)]
struct StateMaps {
    approvals: HashMap<SecretHash, Expiring<OAuthAuthorizationApprovalRecord>>,
    sessions: HashMap<SecretHash, Expiring<OAuthAuthorizationSessionRecord>>,
    codes: HashMap<SecretHash, Expiring<OAuthAuthorizationCodeRecord>>,
}

impl StateMaps {
    /// Counts the approvals `client_id` is holding.
    ///
    /// Walking the map costs no more than the [`Self::purge_expired`] that
    /// precedes every store, and a count taken from the entries themselves
    /// cannot drift out of step with them the way a maintained tally can.
    fn approvals_for_client(&self, client_id: &str) -> usize {
        self.approvals
            .values()
            .filter(|entry| entry.value.client_id == client_id)
            .count()
    }

    fn purge_expired(&mut self, now: Instant) {
        self.approvals.retain(|_, entry| entry.expires_at > now);
        self.sessions.retain(|_, entry| entry.expires_at > now);
        self.codes.retain(|_, entry| entry.expires_at > now);
    }
}

fn ensure_capacity(
    entries: usize,
    max_entries: usize,
    replaces_existing: bool,
) -> Result<(), StateStoreError> {
    if !replaces_existing && entries >= max_entries {
        return Err(StateStoreError::CapacityExceeded { max_entries });
    }
    Ok(())
}

struct Expiring<T> {
    value: T,
    expires_at: Instant,
}

fn secret_hash(secret: &str) -> Result<SecretHash, StateStoreError> {
    if secret.is_empty() {
        return Err(StateStoreError::EmptySecret);
    }
    Ok(hash_bytes(secret.as_bytes()))
}

fn hash_bytes(secret: &[u8]) -> SecretHash {
    Sha256::digest(secret).into()
}

fn approval_key(
    ticket: &OAuthAuthorizationApprovalTicket,
    browser_binding: &OAuthAuthorizationApprovalBrowserBinding,
) -> SecretHash {
    let mut hash = Sha256::new();
    hash.update(b"coral-oauth-browser-bound-approval-v1\0");
    hash.update(ticket.as_bytes());
    hash.update(browser_binding.as_bytes());
    hash.finalize().into()
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;

    use ring::rand::SystemRandom;
    use tokio::sync::Barrier;

    use super::*;

    fn approval(id: &str) -> OAuthAuthorizationApprovalRecord {
        OAuthAuthorizationApprovalRecord {
            client_id: "client".to_string(),
            client_name: format!("Client {id}"),
            redirect_uri: "http://127.0.0.1/callback".to_string(),
            client_state: Some(id.to_string()),
            code_challenge: "challenge".to_string(),
            resource: "https://mcp.example.test/mcp".to_string(),
        }
    }

    fn client_approval(client_id: &str, id: &str) -> OAuthAuthorizationApprovalRecord {
        OAuthAuthorizationApprovalRecord {
            client_id: client_id.to_string(),
            ..approval(id)
        }
    }

    fn browser_binding() -> OAuthAuthorizationApprovalBrowserBinding {
        OAuthAuthorizationApprovalBrowserBinding::from_bytes([0x42; 32])
    }

    fn session(id: &str) -> OAuthAuthorizationSessionRecord {
        OAuthAuthorizationSessionRecord {
            client_id: "client".to_string(),
            redirect_uri: "http://127.0.0.1/callback".to_string(),
            client_state: Some(id.to_string()),
            code_challenge: "challenge".to_string(),
            resource: "https://mcp.example.test/mcp".to_string(),
            oidc_code_verifier: Zeroizing::new("verifier".to_string()),
            oidc_nonce: "nonce".to_string(),
        }
    }

    fn code(id: &str) -> OAuthAuthorizationCodeRecord {
        OAuthAuthorizationCodeRecord {
            user_id: id.to_string(),
            client_id: "client".to_string(),
            redirect_uri: "http://127.0.0.1/callback".to_string(),
            code_challenge: "challenge".to_string(),
            resource: "https://mcp.example.test/mcp".to_string(),
        }
    }

    /// Runs a burst of takes that all start together and returns what each took.
    async fn take_burst<T, F>(start_take: impl Fn() -> F) -> Vec<Option<T>>
    where
        T: Send + 'static,
        F: Future<Output = Result<Option<T>, StateStoreError>> + Send + 'static,
    {
        const BURST: usize = 16;
        let barrier = Arc::new(Barrier::new(BURST));
        let takes = (0..BURST)
            .map(|_index| {
                let barrier = Arc::clone(&barrier);
                let take = start_take();
                tokio::spawn(async move {
                    barrier.wait().await;
                    take.await
                })
            })
            .collect::<Vec<_>>();
        let mut outcomes = Vec::with_capacity(BURST);
        for take in takes {
            outcomes.push(take.await.expect("join").expect("take"));
        }
        outcomes
    }

    fn only_winner<T>(taken: Vec<Option<T>>) -> T {
        let mut winners = taken.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(winners.len(), 1, "exactly one take may win");
        winners.pop().expect("one winner")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn authorization_approvals_are_browser_bound_and_atomically_single_use() {
        let store = Arc::new(InMemoryStateStore::new());
        let ticket = Arc::new(OAuthAuthorizationApprovalTicket::from_bytes([0x5a; 32]));
        let binding = browser_binding();
        store
            .store_authorization_approval(&ticket, &binding, approval("approval"))
            .await
            .unwrap();
        let wrong_binding = OAuthAuthorizationApprovalBrowserBinding::from_bytes([0x43; 32]);
        assert!(
            store
                .take_authorization_approval(&ticket, &wrong_binding)
                .await
                .unwrap()
                .is_none()
        );

        let winner = only_winner(
            take_burst(|| {
                let store = Arc::clone(&store);
                let ticket = Arc::clone(&ticket);
                async move {
                    store
                        .take_authorization_approval(&ticket, &browser_binding())
                        .await
                }
            })
            .await,
        );
        assert!(winner == approval("approval"));
        assert!(
            store
                .take_authorization_approval(&ticket, &binding)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn generated_tickets_are_unpredictable_and_address_distinct_approvals() {
        let random = SystemRandom::new();
        let ticket = OAuthAuthorizationApprovalTicket::generate(&random).expect("ticket");
        let other = OAuthAuthorizationApprovalTicket::generate(&random).expect("ticket");
        assert_ne!(ticket.as_bytes(), &[0; 32]);
        assert_ne!(ticket.as_bytes(), other.as_bytes());

        let store = InMemoryStateStore::new();
        let binding = browser_binding();
        store
            .store_authorization_approval(&ticket, &binding, approval("generated"))
            .await
            .unwrap();
        assert!(
            store
                .take_authorization_approval(&other, &binding)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .take_authorization_approval(&ticket, &binding)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn authorization_approval_capacity_allows_only_same_ticket_replacement() {
        let store =
            InMemoryStateStore::with_options(OAUTH_STATE_TTL, 1, MAX_OAUTH_APPROVALS_PER_CLIENT);
        let ticket = OAuthAuthorizationApprovalTicket::from_bytes([1; 32]);
        let other_ticket = OAuthAuthorizationApprovalTicket::from_bytes([2; 32]);
        store
            .store_authorization_approval(&ticket, &browser_binding(), approval("original"))
            .await
            .unwrap();
        assert_eq!(
            store
                .store_authorization_approval(&other_ticket, &browser_binding(), approval("other"),)
                .await,
            Err(StateStoreError::CapacityExceeded { max_entries: 1 })
        );
        store
            .store_authorization_approval(&ticket, &browser_binding(), approval("replacement"))
            .await
            .unwrap();
        let stored = store
            .take_authorization_approval(&ticket, &browser_binding())
            .await
            .unwrap()
            .expect("replacement approval");
        assert!(stored == approval("replacement"));
    }

    /// The shared budget here is eight times the per-client one, so every
    /// rejection below is the per-client bound and not the budget standing in
    /// for it.
    #[tokio::test]
    async fn approvals_are_bounded_per_client_beneath_the_shared_budget() {
        const CLIENT: &str = "https://a.example.test/client.json";

        let store = InMemoryStateStore::with_options(OAUTH_STATE_TTL, 16, 2);
        let ticket = |byte: u8| OAuthAuthorizationApprovalTicket::from_bytes([byte; 32]);
        for byte in [1, 2] {
            store
                .store_authorization_approval(
                    &ticket(byte),
                    &browser_binding(),
                    client_approval(CLIENT, "held"),
                )
                .await
                .unwrap();
        }
        assert_eq!(
            store
                .store_authorization_approval(
                    &ticket(3),
                    &browser_binding(),
                    client_approval(CLIENT, "over"),
                )
                .await,
            Err(StateStoreError::ClientApprovalsExceeded { max_entries: 2 })
        );

        // The cost stays inside the client that caused it.
        store
            .store_authorization_approval(
                &ticket(4),
                &browser_binding(),
                client_approval("https://b.example.test/client.json", "other"),
            )
            .await
            .unwrap();

        // A ticket the client already holds is a replacement, not a new entry.
        store
            .store_authorization_approval(
                &ticket(1),
                &browser_binding(),
                client_approval(CLIENT, "replacement"),
            )
            .await
            .unwrap();

        // Confirming or cancelling gives the client its slot back.
        store
            .take_authorization_approval(&ticket(2), &browser_binding())
            .await
            .unwrap()
            .expect("held approval");
        store
            .store_authorization_approval(
                &ticket(3),
                &browser_binding(),
                client_approval(CLIENT, "after release"),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cheap_dcr_identities_cannot_bypass_the_global_pending_approval_budget() {
        let store = InMemoryStateStore::new();
        for index in 0..4096_u64 {
            let mut bytes = [0; 32];
            bytes
                .get_mut(..8)
                .unwrap()
                .copy_from_slice(&index.to_be_bytes());
            store
                .store_authorization_approval(
                    &OAuthAuthorizationApprovalTicket::from_bytes(bytes),
                    &browser_binding(),
                    client_approval(&format!("dcr_{index:032x}"), "held"),
                )
                .await
                .unwrap();
        }
        assert_eq!(
            store
                .store_authorization_approval(
                    &OAuthAuthorizationApprovalTicket::from_bytes([255; 32]),
                    &browser_binding(),
                    client_approval("dcr_ffffffffffffffffffffffffffffffff", "over"),
                )
                .await,
            Err(StateStoreError::CapacityExceeded { max_entries: 4096 })
        );
    }

    #[tokio::test(start_paused = true)]
    async fn authorization_approvals_expire_and_release_capacity() {
        let store =
            InMemoryStateStore::with_options(OAUTH_STATE_TTL, 1, MAX_OAUTH_APPROVALS_PER_CLIENT);
        let expired_ticket = OAuthAuthorizationApprovalTicket::from_bytes([1; 32]);
        let other_ticket = OAuthAuthorizationApprovalTicket::from_bytes([2; 32]);
        store
            .store_authorization_approval(&expired_ticket, &browser_binding(), approval("expired"))
            .await
            .unwrap();
        store
            .store_authorization_session("session", session("session"))
            .await
            .unwrap();
        store
            .store_authorization_code("code", code("code"))
            .await
            .unwrap();
        assert_eq!(
            store
                .store_authorization_approval(&other_ticket, &browser_binding(), approval("other"),)
                .await,
            Err(StateStoreError::CapacityExceeded { max_entries: 1 })
        );

        tokio::time::advance(OAUTH_STATE_TTL).await;
        let replacement_ticket = OAuthAuthorizationApprovalTicket::from_bytes([3; 32]);
        store
            .store_authorization_approval(
                &replacement_ticket,
                &browser_binding(),
                approval("replacement"),
            )
            .await
            .unwrap();
        assert!(
            store
                .take_authorization_approval(&expired_ticket, &browser_binding())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .take_authorization_approval(&replacement_ticket, &browser_binding())
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn authorization_sessions_are_atomically_single_use() {
        let store = Arc::new(InMemoryStateStore::new());
        store
            .store_authorization_session("secret-state", session("state"))
            .await
            .unwrap();

        let winner = only_winner(
            take_burst(|| {
                let store = Arc::clone(&store);
                async move { store.take_authorization_session("secret-state").await }
            })
            .await,
        );
        assert_eq!(winner.client_state.as_deref(), Some("state"));
        assert!(
            store
                .take_authorization_session("secret-state")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn authorization_codes_are_atomically_single_use() {
        let store = Arc::new(InMemoryStateStore::new());
        store
            .store_authorization_code("secret-code", code("user"))
            .await
            .unwrap();

        let winner = only_winner(
            take_burst(|| {
                let store = Arc::clone(&store);
                async move {
                    store
                        .take_authorization_code_for_request(
                            "secret-code",
                            "client",
                            "http://127.0.0.1/callback",
                            "challenge",
                            "https://mcp.example.test/mcp",
                        )
                        .await
                }
            })
            .await,
        );
        assert_eq!(winner.user_id, "user");
    }

    #[tokio::test]
    async fn authorization_codes_are_consumed_only_after_all_bindings_match() {
        let store = InMemoryStateStore::new();
        store
            .store_authorization_code("code", code("user"))
            .await
            .unwrap();
        for (client_id, redirect_uri, challenge, resource) in [
            (
                "other",
                "http://127.0.0.1/callback",
                "challenge",
                "https://mcp.example.test/mcp",
            ),
            (
                "client",
                "http://127.0.0.1/other",
                "challenge",
                "https://mcp.example.test/mcp",
            ),
            (
                "client",
                "http://127.0.0.1/callback",
                "other",
                "https://mcp.example.test/mcp",
            ),
            (
                "client",
                "http://127.0.0.1/callback",
                "challenge",
                "https://other.example.test/mcp",
            ),
        ] {
            assert!(
                store
                    .take_authorization_code_for_request(
                        "code",
                        client_id,
                        redirect_uri,
                        challenge,
                        resource,
                    )
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        let redeemed = store
            .take_authorization_code_for_request(
                "code",
                "client",
                "http://127.0.0.1/callback",
                "challenge",
                "https://mcp.example.test/mcp",
            )
            .await
            .unwrap()
            .expect("matching code");
        assert!(redeemed == code("user"));
        assert!(
            store
                .take_authorization_code_for_request(
                    "code",
                    "client",
                    "http://127.0.0.1/callback",
                    "challenge",
                    "https://mcp.example.test/mcp",
                )
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn expiration_is_lazy_and_capacity_is_bounded_per_record_type() {
        let expired =
            InMemoryStateStore::with_options(Duration::ZERO, 1, MAX_OAUTH_APPROVALS_PER_CLIENT);
        expired
            .store_authorization_session("old", session("old"))
            .await
            .unwrap();
        assert!(
            expired
                .take_authorization_session("old")
                .await
                .unwrap()
                .is_none()
        );
        expired
            .store_authorization_code("new", code("new"))
            .await
            .unwrap();

        let bounded =
            InMemoryStateStore::with_options(OAUTH_STATE_TTL, 1, MAX_OAUTH_APPROVALS_PER_CLIENT);
        bounded
            .store_authorization_session("one", session("one"))
            .await
            .unwrap();
        bounded
            .store_authorization_code("two", code("two"))
            .await
            .unwrap();
        bounded
            .store_authorization_session("one", session("replacement"))
            .await
            .expect("same session key may be replaced at capacity");
        bounded
            .store_authorization_code("two", code("replacement"))
            .await
            .expect("same code key may be replaced at capacity");
        assert_eq!(
            bounded
                .store_authorization_session("three", session("three"))
                .await,
            Err(StateStoreError::CapacityExceeded { max_entries: 1 })
        );
        assert_eq!(
            bounded.store_authorization_code("four", code("four")).await,
            Err(StateStoreError::CapacityExceeded { max_entries: 1 })
        );
        assert_eq!(
            bounded
                .store_authorization_session("", session("empty"))
                .await,
            Err(StateStoreError::EmptySecret)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn expiry_is_measured_at_atomic_store_and_take() {
        let store = Arc::new(InMemoryStateStore::new());
        let lock = store.inner.lock().await;
        let delayed_store = {
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                store
                    .store_authorization_session("delayed", session("delayed"))
                    .await
            })
        };
        tokio::task::yield_now().await;
        tokio::time::advance(OAUTH_STATE_TTL + Duration::from_secs(1)).await;
        drop(lock);
        delayed_store.await.unwrap().unwrap();
        assert!(
            store
                .take_authorization_session("delayed")
                .await
                .unwrap()
                .is_some()
        );

        store
            .store_authorization_code("expiring", code("expiring"))
            .await
            .unwrap();
        let lock = store.inner.lock().await;
        let delayed_take = {
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                store
                    .take_authorization_code_for_request(
                        "expiring",
                        "client",
                        "http://127.0.0.1/callback",
                        "challenge",
                        "https://mcp.example.test/mcp",
                    )
                    .await
            })
        };
        tokio::task::yield_now().await;
        tokio::time::advance(OAUTH_STATE_TTL + Duration::from_secs(1)).await;
        drop(lock);
        assert!(delayed_take.await.unwrap().unwrap().is_none());
    }
}
