//! The Description Snippet of a Search Hit.
//!
//! A snippet is the beginning of the description, at most
//! [`SNIPPET_MAX_BYTES`]. A cut text ends at its last whole word, or at a
//! character boundary when no whitespace comes before the bound. A marker shows
//! where it was cut.

use crate::hit::{CUT_MARKER, floor_char_boundary};

#[cfg(test)]
mod tests;

/// Largest snippet, in UTF-8 bytes, the marker included.
pub(crate) const SNIPPET_MAX_BYTES: usize = 512;

/// The beginning of `text`, cut to [`SNIPPET_MAX_BYTES`].
pub(crate) fn snippet(text: &str) -> String {
    if text.len() <= SNIPPET_MAX_BYTES {
        return text.to_string();
    }
    let mut end = floor_char_boundary(text, SNIPPET_MAX_BYTES - CUT_MARKER.len());
    // Close at the last whole word; a text with no word boundary before the
    // bound keeps its bytes.
    if let Some(space) = text
        .get(..end)
        .and_then(|head| head.rfind(char::is_whitespace))
        .filter(|space| *space > 0)
    {
        end = space;
    }
    format!("{}{CUT_MARKER}", text.get(..end).unwrap_or_default().trim())
}
