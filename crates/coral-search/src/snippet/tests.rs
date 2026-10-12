use super::{SNIPPET_MAX_BYTES, snippet};
use crate::hit::CUT_MARKER;

#[test]
fn snippets_open_around_the_match_or_at_the_beginning() {
    let short = "Lists the issues of one repository.";
    let words = (0..200)
        .map(|index| format!("word{index}"))
        .collect::<Vec<_>>();
    let long = words.join(" ");
    let matched_at = long.find("word150 ").expect("fixture word");

    for (text, matched_at, starts_with, ends_with, contains) in [
        (short, Some(10), short, short, "issues"),
        (short, None, short, short, "Lists"),
        (long.as_str(), None, "word0 word1 ", CUT_MARKER, "word1 "),
        (
            long.as_str(),
            Some(matched_at),
            CUT_MARKER,
            CUT_MARKER,
            " word150 ",
        ),
        (
            long.as_str(),
            Some(long.find("word199").expect("last word")),
            CUT_MARKER,
            "word199",
            "word199",
        ),
    ] {
        let snippet = snippet(text, matched_at);
        assert!(snippet.len() <= SNIPPET_MAX_BYTES, "{snippet}");
        assert!(snippet.starts_with(starts_with), "{snippet}");
        assert!(snippet.ends_with(ends_with), "{snippet}");
        assert!(snippet.contains(contains), "{snippet}");
        let body = snippet
            .trim_start_matches(CUT_MARKER)
            .trim_end_matches(CUT_MARKER);
        assert!(
            body.split(' ')
                .all(|word| text.split(' ').any(|whole| whole == word)),
            "a word was cut: {snippet}"
        );
    }
    let around = snippet(&long, Some(matched_at));
    let before = around.find("word150").expect("match kept");
    assert!(
        (100..=200).contains(&before),
        "the window opens about a third before the match: {before}"
    );
}

#[test]
fn a_window_that_opens_at_a_word_start_keeps_that_word() {
    // Eight-byte words, so a third of the window before a word start is a
    // word start too.
    let text = (0..200)
        .map(|index| format!("w{index:06}"))
        .collect::<Vec<_>>()
        .join(" ");
    let matched_at = text.find("w000100").expect("fixture word");
    let first_kept = (matched_at - (SNIPPET_MAX_BYTES - 2 * CUT_MARKER.len()) / 3) / 8;
    assert_eq!(
        text.get(first_kept * 8 - 1..first_kept * 8),
        Some(" "),
        "the fixture opens the window at a word start"
    );

    let snippet = snippet(&text, Some(matched_at));

    assert!(
        snippet.starts_with(&format!("{CUT_MARKER}w{first_kept:06} ")),
        "{snippet}"
    );
}

#[test]
fn a_window_without_a_later_word_boundary_keeps_its_text() {
    let text = format!("{} {}", "y".repeat(300), "x".repeat(600));
    let matched_at = 301;

    let snippet = snippet(&text, Some(matched_at));

    assert!(snippet.len() <= SNIPPET_MAX_BYTES, "{snippet}");
    assert!(snippet.contains(&"x".repeat(100)), "{snippet}");
}
