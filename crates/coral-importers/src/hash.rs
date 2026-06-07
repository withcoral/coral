use std::fmt::Write as _;

use sha2::{Digest as _, Sha256};

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().fold(String::new(), |mut out, byte| {
        write!(out, "{byte:02x}").expect("writing to String cannot fail");
        out
    })
}
