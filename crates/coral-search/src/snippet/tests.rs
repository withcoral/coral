use super::{SNIPPET_MAX_BYTES, snippet};
use crate::hit::CUT_MARKER;

#[test]
fn a_snippet_is_the_beginning_cut_at_a_word() {
    let short = "Lists the issues of one repository.";
    let long = (0..200)
        .map(|index| format!("word{index}"))
        .collect::<Vec<_>>()
        .join(" ");

    assert_eq!(snippet(short), short, "a short text is whole");
    let cut = snippet(&long);
    assert!(cut.len() <= SNIPPET_MAX_BYTES, "{cut}");
    assert!(cut.starts_with("word0 word1 "), "{cut}");
    assert!(cut.ends_with(CUT_MARKER), "{cut}");
    let body = cut.trim_end_matches(CUT_MARKER);
    assert!(
        body.split(' ')
            .all(|word| long.split(' ').any(|whole| whole == word)),
        "a word was cut: {cut}"
    );
    assert!(!cut.contains("word199"), "the end is not shown: {cut}");
}

#[test]
fn a_text_without_a_word_boundary_keeps_whole_characters_to_the_bound() {
    for text in ["x".repeat(900), "é".repeat(900)] {
        let cut = snippet(&text);

        assert!(cut.len() <= SNIPPET_MAX_BYTES, "{cut}");
        assert!(cut.ends_with(CUT_MARKER), "{cut}");
        assert!(
            cut.len() > SNIPPET_MAX_BYTES - 2 * CUT_MARKER.len(),
            "the cut uses the bound: {} bytes",
            cut.len()
        );
    }
}
