//! Reads the cookies a browser sent back.

use axum::http::{HeaderMap, header};
use zeroize::Zeroizing;

/// Returns the value of the one cookie named `name`.
///
/// The scan works in bytes rather than through `HeaderValue::to_str` because
/// that rejects every octet outside visible ASCII, and a browser sends a cookie
/// value however a script set it. Reading the header as text would therefore
/// let one unrelated cookie on this host decide the answer. Only the value
/// stored under `name` has to be well formed, and its caller's decoding is
/// what establishes that.
///
/// Returning `None` for a repeated name is what keeps a shadowing cookie from
/// choosing which value is read, so every occurrence must be seen, not just
/// the first.
pub(super) fn single_value(headers: &HeaderMap, name: &str) -> Option<Zeroizing<Vec<u8>>> {
    let mut found = None;
    for header_value in headers.get_all(header::COOKIE) {
        for pair in header_value.as_bytes().split(|byte| *byte == b';') {
            let Some((candidate, value)) = split_at_byte(pair.trim_ascii(), b'=') else {
                continue;
            };
            if candidate != name.as_bytes() {
                continue;
            }
            if found.is_some() {
                return None;
            }
            found = Some(Zeroizing::new(value.to_vec()));
        }
    }
    found
}

/// Splits `value` at the first occurrence of `byte`, dropping the separator.
fn split_at_byte(value: &[u8], byte: u8) -> Option<(&[u8], &[u8])> {
    let mut parts = value.splitn(2, |candidate| *candidate == byte);
    Some((parts.next()?, parts.next()?))
}
