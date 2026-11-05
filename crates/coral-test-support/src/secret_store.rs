//! An in-memory Secret Store for other crates' tests.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use coral_secret_store::{Error, Secret, SecretReference, SecretStore};

/// Holds entries of named keys, like a KV store, and fails on request.
#[derive(Default)]
pub struct InMemorySecretStore {
    entries: Mutex<BTreeMap<String, BTreeMap<String, String>>>,
    failure: Mutex<Option<Error>>,
    reads: AtomicUsize,
}

impl InMemorySecretStore {
    /// A store with no entries.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets, or rotates, one key's value.
    ///
    /// # Panics
    /// When an earlier test thread panicked while it held the store.
    pub fn set(&self, path: &str, key: &str, value: &str) {
        self.entries
            .lock()
            .expect("store lock")
            .entry(path.to_owned())
            .or_default()
            .insert(key.to_owned(), value.to_owned());
    }

    /// Deletes one entry.
    ///
    /// # Panics
    /// When an earlier test thread panicked while it held the store.
    pub fn remove(&self, path: &str) {
        self.entries.lock().expect("store lock").remove(path);
    }

    /// Makes every read fail this way, or work again with `None`.
    ///
    /// # Panics
    /// When an earlier test thread panicked while it held the store.
    pub fn fail_with(&self, failure: Option<Error>) {
        *self.failure.lock().expect("store lock") = failure;
    }

    /// How many reads and checks reached the store.
    #[must_use]
    pub fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl SecretStore for InMemorySecretStore {
    async fn read(&self, reference: &SecretReference) -> Result<Secret, Error> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if let Some(failure) = *self.failure.lock().expect("store lock") {
            return Err(failure);
        }
        let entries = self.entries.lock().expect("store lock");
        let entry = entries.get(reference.path()).ok_or(Error::EntryNotFound)?;
        let value = reference
            .key()
            .and_then(|key| entry.get(key))
            .ok_or(Error::KeyNotFound)?;
        Secret::new(value.clone()).map_err(|_error| Error::MalformedResponse)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(path: &str, key: Option<&str>) -> SecretReference {
        SecretReference::new("store".into(), path.into(), key.map(str::to_owned)).unwrap()
    }

    #[tokio::test]
    async fn it_answers_like_a_store() {
        let store = InMemorySecretStore::new();
        let token = reference("team/jira", Some("token"));
        assert_eq!(store.read(&token).await.unwrap_err(), Error::EntryNotFound);

        store.set("team/jira", "token", "first");
        assert_eq!(store.read(&token).await.unwrap().expose(), "first");
        store.set("team/jira", "token", "rotated");
        assert_eq!(store.read(&token).await.unwrap().expose(), "rotated");
        store.check(&token).await.unwrap();
        for missing in [Some("other"), None] {
            assert_eq!(
                store
                    .read(&reference("team/jira", missing))
                    .await
                    .unwrap_err(),
                Error::KeyNotFound
            );
        }

        store.fail_with(Some(Error::PermissionDenied));
        assert_eq!(
            store.check(&token).await.unwrap_err(),
            Error::PermissionDenied
        );
        store.fail_with(None);
        store.remove("team/jira");
        let before = store.reads();
        assert_eq!(store.read(&token).await.unwrap_err(), Error::EntryNotFound);
        assert_eq!(store.reads(), before + 1);
    }
}
