//! The text of a Search Hit, derived at hydration from the authoritative
//! material the hydrate read loads, never from the index row.

use crate::signature::{Callable, signature_line};
use crate::snippet::snippet;
use crate::types::HitText;

/// Where text was cut or a nested type was left out.
pub(crate) const CUT_MARKER: &str = "…";

/// The largest character boundary of `text` at or before `index`.
pub(crate) fn floor_char_boundary(text: &str, mut index: usize) -> usize {
    while !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

/// What one hit shows of one Search Entity's material.
pub(crate) struct HitMaterial<'a> {
    pub(crate) declaration: &'a str,
    pub(crate) callable: Callable<'a>,
    pub(crate) description: &'a str,
}

/// The hit text: the signature line and the Description Snippet. A query
/// changes neither.
pub(crate) fn hit_text(material: &HitMaterial<'_>) -> HitText {
    HitText {
        signature: signature_line(material.declaration, material.callable),
        description_snippet: snippet(material.description),
    }
}
