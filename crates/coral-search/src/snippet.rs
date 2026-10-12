//! The Description Snippet and Guidance Snippet of a Search Hit.
//!
//! A snippet is at most [`SNIPPET_MAX_BYTES`] of the text. When a query term
//! occurs in it, the window opens a third of the budget before the leftmost
//! occurrence and runs two thirds after it; otherwise it is the beginning.
//! Both ends move to word boundaries, and a marker shows where text was cut.

use crate::hit::{CUT_MARKER, floor_char_boundary};

#[cfg(test)]
mod tests;

/// Largest snippet, in UTF-8 bytes, markers included.
pub(crate) const SNIPPET_MAX_BYTES: usize = 512;

/// The snippet of `text` around the byte offset of the leftmost match.
pub(crate) fn snippet(text: &str, matched_at: Option<usize>) -> String {
    if text.len() <= SNIPPET_MAX_BYTES {
        return text.to_string();
    }
    let window = SNIPPET_MAX_BYTES - 2 * CUT_MARKER.len();
    let mut start = floor_char_boundary(
        text,
        matched_at.map_or(0, |at| at.saturating_sub(window / 3)),
    );
    let mut end = floor_char_boundary(text, (start + window).min(text.len()));
    let opens_mid_word = text
        .get(..start)
        .and_then(|before| before.chars().next_back())
        .is_some_and(|previous| !previous.is_whitespace());
    if opens_mid_word {
        let matched_at = matched_at.unwrap_or(start);
        // Open at the next whole word, but never after the match.
        if let Some(space) = text
            .get(start..end)
            .and_then(|window| window.find(char::is_whitespace))
            && start + space < matched_at
        {
            start += space;
        }
    }
    // Close at the last whole word; a window with no later word boundary
    // keeps its text.
    if end < text.len()
        && let Some(space) = text
            .get(start..end)
            .and_then(|window| window.rfind(char::is_whitespace))
            .filter(|space| *space > 0)
    {
        end = start + space;
    }
    let body = text.get(start..end).unwrap_or_default().trim();
    format!(
        "{}{body}{}",
        if start > 0 { CUT_MARKER } else { "" },
        if end < text.len() { CUT_MARKER } else { "" },
    )
}
