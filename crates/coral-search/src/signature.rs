//! The signature line of a Search Hit, derived from the authoritative
//! TypeScript declaration at hydration, and whether it is complete enough to
//! call from.
//!
//! The `input` object's members appear inline one level deep, required ones
//! first and long enums last. A nested object appears as `{…}`, and a named
//! type appears by name, unless it is a shallow alias: an alias of scalars,
//! literals, or arrays of those, which is inlined, so a Tool's generated
//! parameter aliases do not hide plain types.
//!
//! A Tool's result is its envelope with the `data` types of its success
//! responses: the top-level field names of an object, named or not, when
//! they fit, and the types by name otherwise. An MCP-backed Tool without a
//! declared output shows that its result is JSON text in
//! `data.content[0].text`. A Saved Function's result is shown as its input
//! is: an object's members one level deep, named or not. The line looks
//! inside the result's `Promise` and one level of array. The compiler infers
//! most Saved Function results, so they have no name to show. A result
//! object the line does not show is the marker `{…}`, and a Saved Function
//! result type the line cannot read is the marker `…`.
//!
//! The line is at most [`SIGNATURE_LINE_MAX_BYTES`]. The result keeps up to
//! [`RESULT_RESERVED_BYTES`] of it in its most detailed form that fits, so a
//! long input never cuts it; the input's last members give way first, to the
//! marker. A line with no marker whose input names no type it does not show
//! is complete.
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

use render::{Shown, braced};

use crate::hit::{CUT_MARKER, floor_char_boundary};

/// Largest signature line, in UTF-8 bytes, marker included.
pub(crate) const SIGNATURE_LINE_MAX_BYTES: usize = 512;

/// Bytes of the line the result may keep however long the input is.
pub(crate) const RESULT_RESERVED_BYTES: usize = 120;

/// The text between the callable and its result.
const RESULT_SEPARATOR: &str = ": ";

/// A Search Hit's signature line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SignatureLine {
    pub(crate) text: String,
    /// Whether the line has no cut marker and its input names no type the
    /// line does not show, so a caller can call from it.
    pub(crate) complete: bool,
}

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
pub(crate) fn signature_line(declaration: &str, callable: Callable<'_>) -> SignatureLine {
    let (text, names_unshown_type) = if let Some(declarations) = syntax::read(declaration) {
        let shown = render::render(&declarations, callable);
        (
            fit(&shown, SIGNATURE_LINE_MAX_BYTES),
            shown.names_unshown_type,
        )
    } else {
        tracing::debug!("a declaration has no readable callable; its signature line is cut");
        (format!("{}({CUT_MARKER})", callable.leaf()), false)
    };
    SignatureLine {
        complete: !names_unshown_type && !text.contains(CUT_MARKER),
        text,
    }
}

/// The line within `max_bytes`: the result in its most detailed form that
/// fits what the input leaves, or [`RESULT_RESERVED_BYTES`] when the input
/// leaves less, then the input in what remains.
fn fit(shown: &Shown, max_bytes: usize) -> String {
    let call = format!("{}({})", shown.leaf, shown.parameters.whole);
    let room = max_bytes
        .saturating_sub(call.len() + RESULT_SEPARATOR.len())
        .max(RESULT_RESERVED_BYTES);
    let result = shown
        .results
        .iter()
        .find(|result| result.len() <= room)
        .or_else(|| shown.results.last())
        .map_or(CUT_MARKER, String::as_str);
    let budget = max_bytes.saturating_sub(result.len() + RESULT_SEPARATOR.len());
    let call = if call.len() <= budget {
        call
    } else {
        cut_call(shown, budget)
    };
    cut(&format!("{call}{RESULT_SEPARATOR}{result}"), max_bytes)
}

/// The callable within `budget`: the input object's members from the first,
/// as many as fit, then the marker; or, when not even the marker alone fits
/// or the input is not one object, its text cut with the marker.
fn cut_call(shown: &Shown, budget: usize) -> String {
    let leaf = &shown.leaf;
    let parameters = &shown.parameters;
    if let Some((head, members)) = &parameters.object {
        for kept in (0..members.len()).rev() {
            let mut members = members.get(..kept).unwrap_or_default().to_vec();
            members.push(CUT_MARKER.to_string());
            let call = format!("{leaf}({head}{}{})", braced(&members), parameters.rest);
            if call.len() <= budget {
                return call;
            }
        }
    }
    let mut call = cut(
        &format!("{leaf}({}", parameters.whole),
        budget.saturating_sub(1),
    );
    call.push(')');
    call
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
