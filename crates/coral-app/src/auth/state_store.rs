//! Single-use, bounded in-memory state for OAuth authorization handshakes.

use std::collections::HashMap;
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio::time::Instant;
use zeroize::Zeroizing;

const OAUTH_STATE_TTL: Duration = Duration::from_mins(5);
const MAX_OAUTH_STATE_ENTRIES_PER_KIND: usize = 4_096;

type SecretHash = [u8; 32];

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct OAuthAuthorizationSessionRecord {
    pub(crate) provider_id: String,
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
    pub(crate) provider_id: String,
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
}

#[async_trait::async_trait]
pub(crate) trait StateStore: Send + Sync {
    async fn store_authorization_session(
        &self,
        oidc_state: &str,
        session: OAuthAuthorizationSessionRecord,
    ) -> Result<(), StateStoreError>;

    async fn take_authorization_session(
        &self,
        oidc_state: &str,
    ) -> Result<Option<OAuthAuthorizationSessionRecord>, StateStoreError>;

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
impl StateStore for InMemoryStateStore {
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
    sessions: HashMap<SecretHash, Expiring<OAuthAuthorizationSessionRecord>>,
    codes: HashMap<SecretHash, Expiring<OAuthAuthorizationCodeRecord>>,
}

impl StateMaps {
    fn purge_expired(&mut self, now: Instant) {
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
    Ok(Sha256::digest(secret.as_bytes()).into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn session(id: &str) -> OAuthAuthorizationSessionRecord {
        OAuthAuthorizationSessionRecord {
            provider_id: "oidc".to_string(),
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
            provider_id: "oidc".to_string(),
            user_id: id.to_string(),
            client_id: "client".to_string(),
            redirect_uri: "http://127.0.0.1/callback".to_string(),
            code_challenge: "challenge".to_string(),
            resource: "https://mcp.example.test/mcp".to_string(),
        }
    }

    #[tokio::test]
    async fn authorization_sessions_are_hashed_and_single_use() {
        let store = Arc::new(InMemoryStateStore::new());
        store
            .store_authorization_session("secret-state", session("state"))
            .await
            .unwrap();
        let inner = store.inner.lock().await;
        let hash = secret_hash("secret-state").unwrap();
        assert!(inner.sessions.contains_key(&hash));
        assert_ne!(hash.as_slice(), b"secret-state");
        drop(inner);

        let first_store = Arc::clone(&store);
        let second_store = Arc::clone(&store);
        let first = first_store.take_authorization_session("secret-state");
        let second = second_store.take_authorization_session("secret-state");
        let (first, second) = tokio::join!(first, second);
        assert_eq!(
            usize::from(first.unwrap().is_some()) + usize::from(second.unwrap().is_some()),
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
