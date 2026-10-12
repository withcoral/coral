use super::{QueryPlan, Statistics, WordStatistics};

/// The Workspace's words, as `search_terms` holds them.
const CATALOG: [&str; 9] = [
    "color",
    "center",
    "search",
    "may",
    "when",
    "gists",
    "lists",
    "workspace2",
    "workspaces",
];

fn statistics(word: &str, ndoc: i64) -> Statistics {
    Statistics {
        words: vec![WordStatistics {
            word: word.to_string(),
            ndoc,
        }],
        catalog_words: CATALOG
            .iter()
            .map(|word| WordStatistics {
                word: (*word).to_string(),
                ndoc: 3,
            })
            .collect(),
        total_docs: 100,
        avgdl: 10.0,
    }
}

#[test]
fn a_word_no_row_holds_reaches_the_catalog_words_one_edit_away() {
    for (word, ndoc, expected, reason) in [
        ("colour", 0, &["color"][..], "one letter removed"),
        ("centre", 0, &["center"], "two adjacent letters swapped"),
        ("research", 0, &[], "two edits from `search`"),
        (
            "maya",
            0,
            &[],
            "under five letters, though one edit from `may`",
        ),
        (
            "lists",
            4,
            &[],
            "the catalog holds it, though one edit from `gists`",
        ),
        ("workspace1", 0, &[], "a changed digit is not a letter edit"),
        (
            "workspace",
            0,
            &["workspaces"],
            "an added letter is, and an added digit (`workspace2`) is not",
        ),
    ] {
        let plan = QueryPlan::build(&[word.to_string()]).expect("a plan");
        let scoring = plan.scoring(&statistics(word, ndoc));
        let [scored] = scoring.words.as_slice() else {
            panic!("{word}: one scored word, got {}", scoring.words.len());
        };
        let near = scored
            .spellings
            .iter()
            .map(|spelling| spelling.word.as_str())
            .filter(|spelling| *spelling != word)
            .collect::<Vec<_>>();
        assert_eq!(near, expected, "{word}: {reason}");
    }
}
