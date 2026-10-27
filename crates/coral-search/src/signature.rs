//! The signature line of a Search Hit, derived from the authoritative
//! TypeScript declaration at hydration.
//!
//! The `input` object's members appear inline one level deep. A nested
//! object appears as `{…}`, and a named type appears by name, unless it is a
//! shallow alias: an alias of scalars, literals, or arrays of those, which is
//! inlined, so a Tool's generated parameter aliases do not hide plain
//! types.
//!
//! A Tool's result is its envelope with the `data` types of its
//! success responses, by name. A Saved Function's result is shown as its
//! input is: an object's members one level deep, named or not. The line
//! looks inside the result's `Promise` and one level of array. The compiler
//! infers most Saved Function results, so they have no name to show. A
//! result object the line does not show is the marker `{…}`, and a Saved
//! Function result type the line cannot read is the marker `…`.
//!
//! The line is cut at [`SIGNATURE_LINE_MAX_BYTES`] at a word boundary with a
//! marker. A line with no marker and no bare type name in its input is
//! complete.
//!
//! `syntax` reads the declaration with oxc into the small type tree of `tree`;
//! `render` and `envelope` print the line from that tree, and `abridge`
//! shortens the declaration `describe` returns from it. What the tree
//! cannot show becomes the marker, never an error: a hit without a readable
//! signature still tells the caller to describe first.

mod abridge;
mod envelope;
mod render;
mod syntax;
#[cfg(test)]
mod tests;
mod tree;

pub(crate) use abridge::abridge;

use crate::hit::{CUT_MARKER, floor_char_boundary};

/// Largest signature line, in UTF-8 bytes, marker included.
pub(crate) const SIGNATURE_LINE_MAX_BYTES: usize = 512;

/// What the declaration declares, and the name the line calls it by: the
/// last segment of its Call Path.
#[derive(Clone, Copy)]
pub(crate) enum Callable<'a> {
    /// A catalog-projected Tool: its result is a union of envelopes,
    /// one per declared success: `{ data; metadata }` objects, or
    /// `HttpResult` for an HTTP Tool.
    Tool { leaf: &'a str },
    /// A Saved Function's compiler-emitted declaration with one default
    /// callable export.
    SavedFunction { leaf: &'a str },
}

impl Callable<'_> {
    const fn leaf(&self) -> &str {
        match self {
            Self::Tool { leaf } | Self::SavedFunction { leaf } => leaf,
        }
    }
}

/// The bounded signature line of the callable in `declaration`.
pub(crate) fn signature_line(declaration: &str, callable: Callable<'_>) -> String {
    let line = if let Some(declarations) = syntax::read(declaration) {
        render::render(&declarations, callable)
    } else {
        tracing::debug!("a declaration has no readable callable; its signature line is cut");
        format!("{}({CUT_MARKER})", callable.leaf())
    };
    cut(&line, SIGNATURE_LINE_MAX_BYTES)
}

/// `text` whole when it fits `max_bytes`; otherwise its longest prefix that
/// ends at a word boundary and fits with the marker, then the marker.
pub(crate) fn cut(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    let end = floor_char_boundary(text, max_bytes.saturating_sub(CUT_MARKER.len()));
    let prefix = text.get(..end).unwrap_or_default();
    let kept = prefix
        .rfind(char::is_whitespace)
        .and_then(|boundary| prefix.get(..boundary))
        .unwrap_or(prefix);
    format!("{}{CUT_MARKER}", kept.trim_end())
}
