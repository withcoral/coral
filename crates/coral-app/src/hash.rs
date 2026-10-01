//! Small hashing helpers shared inside the app crate.

use sha2::{Digest as _, Sha256};

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
