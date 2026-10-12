//! The text of a Search Hit, derived at hydration from the authoritative
//! material the hydrate read loads, never from the index row.

use crate::evidence::{EvidenceTerms, Matched};
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
    pub(crate) guidance: Option<&'a str>,
    pub(crate) tags: &'a [String],
}

/// The hit text for a listing, or for a query with its terms and what they
/// matched. The Guidance Snippet and the tags appear only on a match.
pub(crate) fn hit_text(
    material: &HitMaterial<'_>,
    query: Option<(&EvidenceTerms, &Matched)>,
) -> HitText {
    let signature_line = signature_line(material.declaration, material.callable);
    let Some((terms, matched)) = query else {
        return HitText {
            signature_line,
            description_snippet: snippet(material.description, None),
            guidance_snippet: None,
            matched_tags: Vec::new(),
        };
    };
    let guidance_matched = matched
        .evidence
        .fields()
        .iter()
        .any(|field| field == "guidance");
    HitText {
        signature_line,
        description_snippet: snippet(material.description, terms.leftmost(material.description)),
        guidance_snippet: material
            .guidance
            .filter(|_guidance| guidance_matched)
            .map(|guidance| snippet(guidance, terms.leftmost(guidance))),
        matched_tags: material
            .tags
            .iter()
            .filter(|tag| matched.tags.contains(tag))
            .cloned()
            .collect(),
    }
}
