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

type SecretHash = [u8; 32];

/// Single-use secret naming a stored authorization approval.
///
/// Outside tests the only way to build one is [`Self::generate`], which fills
/// the secret in place from a caller-supplied CSPRNG. The bytes therefore never
/// exist outside the value that zeroizes them on drop, and no caller can mint a
/// ticket with attacker-guessable content.
pub(crate) struct OAuthAuthorizationApprovalTicket([u8; 32]);

impl OAuthAuthorizationApprovalTicket {
    /// Draws a fresh ticket from `random`.
    ///
    /// # Errors
    ///
    /// Returns [`StateStoreError::Randomness`] when `random` cannot fill the
    /// ticket.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "wired by the stacked authorization approval flow")
    )]
    pub(super) fn generate(random: &dyn SecureRandom) -> Result<Self, StateStoreError> {
        let mut ticket = Self([0; 32]);
        random
            .fill(&mut ticket.0)
            .map_err(|_error| StateStoreError::Randomness)?;
        Ok(ticket)
    }

    #[cfg(test)]
    fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Drop for OAuthAuthorizationApprovalTicket {
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
    #[error("OAuth state expiry exceeds the process clock range")]
    ExpiryOverflow,
    #[error("OAuth approval ticket generation failed")]
    Randomness,
}

/// Storage for approvals awaiting the user's confirmation.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "wired by the stacked authorization approval flow")
)]
#[async_trait::async_trait]
pub(crate) trait ApprovalStore: Send + Sync {
    async fn store_authorization_approval(
        &self,
        ticket: &OAuthAuthorizationApprovalTicket,
        approval: OAuthAuthorizationApprovalRecord,
    ) -> Result<(), StateStoreError>;

    async fn take_authorization_approval(
        &self,
        ticket: &OAuthAuthorizationApprovalTicket,
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
}

impl Default for InMemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStateStore {
    pub(crate) fn new() -> Self {
        Self::with_options(OAUTH_STATE_TTL, MAX_OAUTH_STATE_ENTRIES_PER_KIND)
    }

    fn with_options(ttl: Duration, max_entries_per_kind: usize) -> Self {
        Self {
            inner: Mutex::new(StateMaps::default()),
            ttl,
            max_entries_per_kind,
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
        approval: OAuthAuthorizationApprovalRecord,
    ) -> Result<(), StateStoreError> {
        let key = hash_bytes(ticket.as_bytes());
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        let expires_at = self.expires_at(now)?;
        inner.purge_expired(now);
        ensure_capacity(
            inner.approvals.len(),
            self.max_entries_per_kind,
            inner.approvals.contains_key(&key),
        )?;
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
    ) -> Result<Option<OAuthAuthorizationApprovalRecord>, StateStoreError> {
        let key = hash_bytes(ticket.as_bytes());
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

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;

    use ring::rand::SystemRandom;
    use tokio::sync::Notify;
    use tokio::task::JoinHandle;

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

    /// Parks both takes on the store mutex and returns their join handles.
    ///
    /// The caller holds the store lock across this call, so each take is polled
    /// once, observed to be `Pending`, and left queued on the mutex. Releasing
    /// the lock afterwards forces the two takes to contend, which `join!` alone
    /// only manages by luck of the scheduler.
    async fn park_takes_on_store_lock<T: Send + 'static>(
        first: impl Future<Output = T> + Send + 'static,
        second: impl Future<Output = T> + Send + 'static,
    ) -> (JoinHandle<T>, JoinHandle<T>) {
        let parked = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(Notify::new());
        let handles = (
            spawn_parked_take(first, Arc::clone(&parked), Arc::clone(&notify)),
            spawn_parked_take(second, Arc::clone(&parked), Arc::clone(&notify)),
        );
        while parked.load(Ordering::SeqCst) < 2 {
            notify.notified().await;
        }
        handles
    }

    fn spawn_parked_take<T: Send + 'static>(
        take: impl Future<Output = T> + Send + 'static,
        parked: Arc<AtomicUsize>,
        notify: Arc<Notify>,
    ) -> JoinHandle<T> {
        tokio::spawn(async move {
            let mut take = Box::pin(take);
            poll_fn(|context| match take.as_mut().poll(context) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("take completed while the store lock was held"),
            })
            .await;
            parked.fetch_add(1, Ordering::SeqCst);
            notify.notify_one();
            take.await
        })
    }

    #[tokio::test]
    async fn authorization_approvals_are_hashed_and_atomically_single_use() {
        let store = Arc::new(InMemoryStateStore::new());
        let raw_ticket = [0x5a; 32];
        let ticket = Arc::new(OAuthAuthorizationApprovalTicket::from_bytes(raw_ticket));
        store
            .store_authorization_approval(&ticket, approval("approval"))
            .await
            .unwrap();
        let inner = store.inner.lock().await;
        let hash = hash_bytes(&raw_ticket);
        assert!(inner.approvals.contains_key(&hash));
        assert!(!inner.approvals.contains_key(&raw_ticket));

        let take = |store: &Arc<InMemoryStateStore>,
                    ticket: &Arc<OAuthAuthorizationApprovalTicket>| {
            let store = Arc::clone(store);
            let ticket = Arc::clone(ticket);
            async move { store.take_authorization_approval(&ticket).await }
        };
        let (first, second) =
            park_takes_on_store_lock(take(&store, &ticket), take(&store, &ticket)).await;
        drop(inner);
        let (first, second) = tokio::join!(first, second);
        let first = first.unwrap().unwrap();
        let second = second.unwrap().unwrap();
        assert_eq!(
            usize::from(first.is_some()) + usize::from(second.is_some()),
            1
        );
        let winner = first.or(second).expect("one approval winner");
        assert!(winner == approval("approval"));
        assert!(
            store
                .take_authorization_approval(&ticket)
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
        store
            .store_authorization_approval(&ticket, approval("generated"))
            .await
            .unwrap();
        assert!(
            store
                .take_authorization_approval(&other)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .take_authorization_approval(&ticket)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn authorization_approval_capacity_allows_only_same_ticket_replacement() {
        let store = InMemoryStateStore::with_options(OAUTH_STATE_TTL, 1);
        let ticket = OAuthAuthorizationApprovalTicket::from_bytes([1; 32]);
        let other_ticket = OAuthAuthorizationApprovalTicket::from_bytes([2; 32]);
        store
            .store_authorization_approval(&ticket, approval("original"))
            .await
            .unwrap();
        assert_eq!(
            store
                .store_authorization_approval(&other_ticket, approval("other"))
                .await,
            Err(StateStoreError::CapacityExceeded { max_entries: 1 })
        );
        store
            .store_authorization_approval(&ticket, approval("replacement"))
            .await
            .unwrap();
        let stored = store
            .take_authorization_approval(&ticket)
            .await
            .unwrap()
            .expect("replacement approval");
        assert!(stored == approval("replacement"));
    }

    #[tokio::test(start_paused = true)]
    async fn authorization_approvals_expire_and_release_capacity() {
        let store = InMemoryStateStore::with_options(OAUTH_STATE_TTL, 1);
        let expired_ticket = OAuthAuthorizationApprovalTicket::from_bytes([1; 32]);
        let other_ticket = OAuthAuthorizationApprovalTicket::from_bytes([2; 32]);
        store
            .store_authorization_approval(&expired_ticket, approval("expired"))
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
                .store_authorization_approval(&other_ticket, approval("other"))
                .await,
            Err(StateStoreError::CapacityExceeded { max_entries: 1 })
        );

        tokio::time::advance(OAUTH_STATE_TTL).await;
        let replacement_ticket = OAuthAuthorizationApprovalTicket::from_bytes([3; 32]);
        store
            .store_authorization_approval(&replacement_ticket, approval("replacement"))
            .await
            .unwrap();
        assert!(
            store
                .take_authorization_approval(&expired_ticket)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .take_authorization_approval(&replacement_ticket)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn authorization_sessions_are_hashed_and_atomically_single_use() {
        let store = Arc::new(InMemoryStateStore::new());
        store
            .store_authorization_session("secret-state", session("state"))
            .await
            .unwrap();
        let inner = store.inner.lock().await;
        let hash = secret_hash("secret-state").unwrap();
        assert!(inner.sessions.contains_key(&hash));
        assert_ne!(hash.as_slice(), b"secret-state");

        let take = |store: &Arc<InMemoryStateStore>| {
            let store = Arc::clone(store);
            async move { store.take_authorization_session("secret-state").await }
        };
        let (first, second) = park_takes_on_store_lock(take(&store), take(&store)).await;
        drop(inner);
        let (first, second) = tokio::join!(first, second);
        assert_eq!(
            usize::from(first.unwrap().unwrap().is_some())
                + usize::from(second.unwrap().unwrap().is_some()),
            1
        );
        assert!(
            store
                .take_authorization_session("secret-state")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn authorization_codes_are_atomically_single_use() {
        let store = Arc::new(InMemoryStateStore::new());
        store
            .store_authorization_code("secret-code", code("user"))
            .await
            .unwrap();
        let inner = store.inner.lock().await;

        let take = |store: &Arc<InMemoryStateStore>| {
            let store = Arc::clone(store);
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
        };
        let (first, second) = park_takes_on_store_lock(take(&store), take(&store)).await;
        drop(inner);
        let (first, second) = tokio::join!(first, second);
        assert_eq!(
            usize::from(first.unwrap().unwrap().is_some())
                + usize::from(second.unwrap().unwrap().is_some()),
            1
        );
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
        let expired = InMemoryStateStore::with_options(Duration::ZERO, 1);
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

        let bounded = InMemoryStateStore::with_options(OAUTH_STATE_TTL, 1);
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
