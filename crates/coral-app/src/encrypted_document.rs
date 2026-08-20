//! Shared envelope-encrypted document representation.

use std::fmt;

/// Opaque envelope-encrypted bytes and the metadata needed to decrypt them.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct EncryptedEnvelopeDocument {
    pub(crate) ciphertext: Vec<u8>,
    pub(crate) nonce: Vec<u8>,
    pub(crate) wrapped_dek: Vec<u8>,
    pub(crate) wrapped_dek_nonce: Vec<u8>,
    pub(crate) key_id: String,
    pub(crate) algorithm: String,
    pub(crate) binding_version: i64,
}

impl EncryptedEnvelopeDocument {
    /// Build an envelope after validating its storage-independent shape.
    pub(crate) fn new(
        ciphertext: Vec<u8>,
        nonce: Vec<u8>,
        wrapped_dek: Vec<u8>,
        wrapped_dek_nonce: Vec<u8>,
        key_id: impl Into<String>,
        algorithm: impl Into<String>,
        binding_version: i64,
    ) -> Result<Self, EncryptedEnvelopeError> {
        let envelope = Self {
            ciphertext,
            nonce,
            wrapped_dek,
            wrapped_dek_nonce,
            key_id: key_id.into(),
            algorithm: algorithm.into(),
            binding_version,
        };
        envelope.validate()?;
        Ok(envelope)
    }

    /// Validate structural envelope invariants without enforcing crypto policy.
    pub(crate) fn validate(&self) -> Result<(), EncryptedEnvelopeError> {
        if self.ciphertext.is_empty()
            || self.nonce.is_empty()
            || self.wrapped_dek.is_empty()
            || self.wrapped_dek_nonce.is_empty()
        {
            return Err(EncryptedEnvelopeError::EmptyEncryptedField);
        }
        if self.key_id.trim().is_empty()
            || self.algorithm.trim().is_empty()
            || self.binding_version < 1
        {
            return Err(EncryptedEnvelopeError::InvalidMetadata);
        }
        Ok(())
    }
}

impl fmt::Debug for EncryptedEnvelopeDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncryptedEnvelopeDocument")
            .field("ciphertext_len", &self.ciphertext.len())
            .field("nonce_len", &self.nonce.len())
            .field("wrapped_dek_len", &self.wrapped_dek.len())
            .field("wrapped_dek_nonce_len", &self.wrapped_dek_nonce.len())
            .finish_non_exhaustive()
    }
}

/// Invalid structural envelope data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum EncryptedEnvelopeError {
    #[error("encrypted document envelope has an empty encrypted byte field")]
    EmptyEncryptedField,
    #[error("encrypted document envelope has invalid metadata")]
    InvalidMetadata,
}

#[cfg(test)]
mod tests {
    use super::EncryptedEnvelopeDocument;

    #[test]
    fn envelope_validation_and_debug_are_shared_and_secret_safe() {
        let present = || vec![1];
        for (bytes, key_id, algorithm, binding_version) in [
            (
                [Vec::new(), present(), present(), present()],
                "key",
                "algorithm",
                1,
            ),
            (
                [present(), Vec::new(), present(), present()],
                "key",
                "algorithm",
                1,
            ),
            (
                [present(), present(), Vec::new(), present()],
                "key",
                "algorithm",
                1,
            ),
            (
                [present(), present(), present(), Vec::new()],
                "key",
                "algorithm",
                1,
            ),
            (
                [present(), present(), present(), present()],
                " ",
                "algorithm",
                1,
            ),
            ([present(), present(), present(), present()], "key", "", 1),
            (
                [present(), present(), present(), present()],
                "key",
                "algorithm",
                0,
            ),
        ] {
            let [ciphertext, nonce, wrapped_dek, wrapped_dek_nonce] = bytes;
            EncryptedEnvelopeDocument::new(
                ciphertext,
                nonce,
                wrapped_dek,
                wrapped_dek_nonce,
                key_id,
                algorithm,
                binding_version,
            )
            .expect_err("invalid envelope shape must fail");
        }

        let envelope = EncryptedEnvelopeDocument::new(
            b"sentinel-secret".to_vec(),
            b"sentinel-nonce".to_vec(),
            b"sentinel-wrapped".to_vec(),
            b"sentinel-wrap-nonce".to_vec(),
            "sentinel-key",
            "sentinel-algorithm",
            99,
        )
        .expect("positive binding versions are structurally valid");
        assert!(!format!("{envelope:?}").contains("sentinel"));
    }
}
