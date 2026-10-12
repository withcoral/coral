//! Host-only credential encryption. Bootstrap explicitly selects the key: a
//! filesystem key file it owns, or key material a cloud secret manager hands
//! the process.
//!
//! Database ciphertext is authenticated to its owning record identity. The key
//! never enters persistence, diagnostic formatting, or a Program. Losing the key
//! fails closed; an existing invalid key is never silently replaced.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use ring::{
    aead,
    rand::{SecureRandom as _, SystemRandom},
};
use std::{
    fs::OpenOptions,
    io::{Read as _, Write as _},
    path::Path,
    sync::Arc,
};
use zeroize::Zeroizing;

/// A bounded host credential, deliberately without Debug or serialization.
pub struct Secret(Zeroizing<String>);
impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret([redacted])")
    }
}
impl Secret {
    /// Accepts a nonempty bounded secret without control characters.
    /// # Errors
    /// Rejects unsafe header values and unbounded credential input.
    pub fn new(value: String) -> Result<Self, Error> {
        let value = Zeroizing::new(value);
        if value.is_empty() || value.len() > 16 * 1024 || value.chars().any(char::is_control) {
            return Err(Error::InvalidSecret);
        }
        Ok(Self(value))
    }
    /// Provides credential material only to host credential injection.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

/// The Credential Field values of one account, keyed by field name. A bearer
/// token or OAuth access token is the one field [`CredentialValues::TOKEN`].
pub struct CredentialValues(std::collections::BTreeMap<String, Secret>);
impl std::fmt::Debug for CredentialValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.0.keys()).finish()
    }
}
impl CredentialValues {
    /// The field that carries a bearer token or an OAuth access token.
    pub const TOKEN: &'static str = "token";
    /// The most fields one account can hold.
    pub const MAX_FIELDS: usize = 16;

    /// Accepts between one and [`Self::MAX_FIELDS`] values with lowercase field names.
    /// # Errors
    /// Rejects no values, too many, or a name that is not a lowercase name of at most 64 bytes.
    pub fn new(values: std::collections::BTreeMap<String, Secret>) -> Result<Self, Error> {
        let named = |name: &str| {
            name.len() <= 64
                && name.starts_with(|c: char| c.is_ascii_lowercase())
                && name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        };
        if values.is_empty()
            || values.len() > Self::MAX_FIELDS
            || !values.keys().all(|name| named(name))
        {
            return Err(Error::InvalidSecret);
        }
        Ok(Self(values))
    }
    /// One bearer token or OAuth access token.
    #[must_use]
    pub fn token(secret: Secret) -> Self {
        Self(std::collections::BTreeMap::from([(
            Self::TOKEN.into(),
            secret,
        )]))
    }
    /// The value of one field.
    #[must_use]
    pub fn get(&self, field: &str) -> Option<&Secret> {
        self.0.get(field)
    }
    /// Field names, in order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }
}

/// Filesystem-key-backed authenticated encryption used by the host.
#[derive(Clone)]
pub struct CredentialVault(Arc<aead::LessSafeKey>);
impl CredentialVault {
    /// The length of a key in bytes: AES-256-GCM.
    pub const KEY_LEN: usize = 32;

    /// Opens or exclusively creates a private key file at a bootstrap-selected path.
    /// The parent directory must already exist and be host controlled.
    /// # Errors
    /// Fails on inaccessible, malformed, or insecure key files.
    pub fn open(path: &Path) -> Result<Self, Error> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        match options.open(path) {
            Ok(mut file) => {
                let mut key = Zeroizing::new([0_u8; Self::KEY_LEN]);
                SystemRandom::new()
                    .fill(key.as_mut())
                    .map_err(|_error| Error::Cryptography)?;
                file.write_all(key.as_ref())?;
                file.sync_all()?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        let metadata = std::fs::symlink_metadata(path)?;
        if !metadata.is_file() || metadata.len() != Self::KEY_LEN as u64 {
            return Err(Error::InvalidKey);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(Error::InvalidKey);
            }
        }
        let mut key = Zeroizing::new([0_u8; Self::KEY_LEN]);
        std::fs::File::open(path)?.read_exact(key.as_mut())?;
        Self::from_key_bytes(key.as_ref())
    }

    /// Binds key material that bootstrap read from a secret source, such as an
    /// environment variable a cloud secret manager fills: the standard base64
    /// encoding of exactly [`Self::KEY_LEN`] random bytes. Surrounding
    /// whitespace is ignored, because secret stores often append a newline.
    /// The decoded bytes are the same key [`Self::open`] stores, so a key file
    /// can move into a secret and back without re-sealing anything.
    /// # Errors
    /// Fails on material that is not base64 or does not decode to [`Self::KEY_LEN`] bytes.
    pub fn from_encoded_key(encoded: &str) -> Result<Self, Error> {
        // Decode into a zeroizing buffer rather than taking the `Vec` that
        // `decode` allocates: a malformed value fails partway through, and
        // that buffer would be freed still holding leading key bytes.
        let mut key = Zeroizing::new(Vec::with_capacity(Self::KEY_LEN));
        STANDARD
            .decode_vec(encoded.trim(), &mut key)
            .map_err(|_error| Error::InvalidKey)?;
        if key.len() != Self::KEY_LEN {
            return Err(Error::InvalidKey);
        }
        Self::from_key_bytes(&key)
    }

    /// Builds the AEAD key from raw key bytes that one of the constructors
    /// above has already read and length-checked.
    fn from_key_bytes(key: &[u8]) -> Result<Self, Error> {
        let key =
            aead::UnboundKey::new(&aead::AES_256_GCM, key).map_err(|_error| Error::Cryptography)?;
        Ok(Self(Arc::new(aead::LessSafeKey::new(key))))
    }

    /// Encrypts private host workflow material, bound to an exact durable record.
    /// # Errors
    /// Fails if encryption or secure randomness fails.
    pub fn seal(&self, record: &str, plaintext: &str) -> Result<String, Error> {
        let mut nonce = [0_u8; 12];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_error| Error::Cryptography)?;
        let mut ciphertext = plaintext.as_bytes().to_vec();
        self.0
            .seal_in_place_append_tag(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(record),
                &mut ciphertext,
            )
            .map_err(|_error| Error::Cryptography)?;
        let mut encoded = nonce.to_vec();
        encoded.extend(ciphertext);
        Ok(STANDARD.encode(encoded))
    }

    /// Decrypts private host workflow material for its exact durable record.
    /// # Errors
    /// Fails closed on wrong keys, changed ciphertext, or another record identity.
    pub fn open_sealed(&self, record: &str, ciphertext: &str) -> Result<Zeroizing<String>, Error> {
        let mut bytes = Zeroizing::new(
            STANDARD
                .decode(ciphertext)
                .map_err(|_error| Error::Cryptography)?,
        );
        let (nonce, body) = bytes.split_at_mut_checked(12).ok_or(Error::Cryptography)?;
        let nonce: [u8; 12] = nonce.try_into().map_err(|_error| Error::Cryptography)?;
        let plaintext = self
            .0
            .open_in_place(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(record),
                body,
            )
            .map_err(|_error| Error::Cryptography)?;
        Ok(Zeroizing::new(
            std::str::from_utf8(plaintext)
                .map_err(|_error| Error::Cryptography)?
                .to_owned(),
        ))
    }
}

/// Safe host credential failure; never includes material or upstream responses.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid secret input.
    #[error("invalid credential material")]
    InvalidSecret,
    /// Invalid or unsafe persisted key.
    #[error("invalid filesystem credential key")]
    InvalidKey,
    /// Authenticated encryption failed.
    #[error("credential cryptography failed")]
    Cryptography,
    /// Filesystem failure.
    #[error("credential storage unavailable")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::{CredentialVault, Error, STANDARD};
    use base64::Engine as _;

    fn vault(dir: &tempfile::TempDir, name: &str) -> CredentialVault {
        CredentialVault::open(&dir.path().join(name)).expect("open vault")
    }

    #[test]
    fn sealed_material_reopens_only_for_its_record_under_its_key() {
        let dir = tempfile::tempdir().unwrap();
        let sealing = vault(&dir, "credential.key");
        let sealed = sealing.seal("accounts/one", "secret-value").unwrap();
        assert!(!sealed.contains("secret-value"));
        assert_ne!(
            sealing.seal("accounts/one", "secret-value").unwrap(),
            sealed
        );

        // Reopening the same key file restores the same key.
        let reopened = vault(&dir, "credential.key");
        assert_eq!(
            reopened
                .open_sealed("accounts/one", &sealed)
                .unwrap()
                .as_str(),
            "secret-value"
        );

        let mut tampered = STANDARD.decode(&sealed).unwrap();
        *tampered.last_mut().unwrap() ^= 1;
        let tampered = STANDARD.encode(tampered);
        let short = STANDARD.encode([0_u8; 11]);
        let other_key = vault(&dir, "other.key");
        for (vault, record, ciphertext) in [
            (&reopened, "accounts/two", sealed.as_str()),
            (&reopened, "accounts/one", tampered.as_str()),
            (&reopened, "accounts/one", short.as_str()),
            (&reopened, "accounts/one", "not base64!"),
            (&other_key, "accounts/one", sealed.as_str()),
        ] {
            assert!(
                matches!(
                    vault.open_sealed(record, ciphertext),
                    Err(Error::Cryptography)
                ),
                "{record}: {ciphertext}"
            );
        }
    }

    /// A key file's bytes, base64-encoded into a secret, are the same key:
    /// material sealed under either source opens under the other.
    #[test]
    fn encoded_key_material_is_the_same_key_as_its_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = vault(&dir, "credential.key");
        let bytes = std::fs::read(dir.path().join("credential.key")).unwrap();
        // Secret stores often append a newline; it is not part of the key.
        let encoded = format!("{}\n", STANDARD.encode(&bytes));
        let secret = CredentialVault::from_encoded_key(&encoded).unwrap();

        let sealed_by_file = file.seal("accounts/one", "secret-value").unwrap();
        let sealed_by_secret = secret.seal("accounts/one", "secret-value").unwrap();
        assert_eq!(
            secret
                .open_sealed("accounts/one", &sealed_by_file)
                .unwrap()
                .as_str(),
            "secret-value"
        );
        assert_eq!(
            file.open_sealed("accounts/one", &sealed_by_secret)
                .unwrap()
                .as_str(),
            "secret-value"
        );

        let other = CredentialVault::from_encoded_key(&STANDARD.encode([9_u8; 32])).unwrap();
        assert!(matches!(
            other.open_sealed("accounts/one", &sealed_by_file),
            Err(Error::Cryptography)
        ));
    }

    #[test]
    fn malformed_encoded_key_material_fails_closed() {
        for encoded in [
            "",
            "not base64!",
            STANDARD.encode([7_u8; 31]).as_str(),
            STANDARD.encode([7_u8; 33]).as_str(),
            // Hex of 32 bytes is valid base64 of 48 bytes, not a key.
            "0000000000000000000000000000000000000000000000000000000000000000",
        ] {
            assert!(
                matches!(
                    CredentialVault::from_encoded_key(encoded),
                    Err(Error::InvalidKey)
                ),
                "{encoded:?}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn unsafe_existing_keys_fail_closed_and_are_never_replaced() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().unwrap();
        let created = dir.path().join("created.key");
        vault(&dir, "created.key");
        assert_eq!(
            std::fs::metadata(&created).unwrap().permissions().mode() & 0o777,
            0o600
        );

        let readable = dir.path().join("readable.key");
        std::fs::write(&readable, [7_u8; 32]).unwrap();
        std::fs::set_permissions(&readable, std::fs::Permissions::from_mode(0o640)).unwrap();
        let short = dir.path().join("short.key");
        std::fs::write(&short, [7_u8; 31]).unwrap();
        std::fs::set_permissions(&short, std::fs::Permissions::from_mode(0o600)).unwrap();
        let link = dir.path().join("link.key");
        std::os::unix::fs::symlink(&created, &link).unwrap();

        for path in [&readable, &short, &link] {
            let before = std::fs::read(path).unwrap();
            assert!(
                matches!(CredentialVault::open(path), Err(Error::InvalidKey)),
                "{}",
                path.display()
            );
            assert_eq!(std::fs::read(path).unwrap(), before, "{}", path.display());
        }
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }
}
