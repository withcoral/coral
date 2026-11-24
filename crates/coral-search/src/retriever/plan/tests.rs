use std::collections::BTreeSet;

use super::{QueryPlan, Statistics, WordStatistics, path_words};

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

/// The plan for `query`, from the terms the engine gives it: each word and
/// the whole query.
fn plan(query: &str) -> QueryPlan {
    let terms = query
        .split_whitespace()
        .map(str::to_string)
        .chain(std::iter::once(query.to_string()))
        .collect::<Vec<_>>();
    QueryPlan::build(&terms).expect("a plan")
}

#[test]
fn a_query_names_a_provider_by_its_namespace_or_every_word_of_its_display_name() {
    for (query, namespace, display_name, named, reason) in [
        (
            "search issues linear",
            Some("linear"),
            "Linear",
            true,
            "a term is the namespace",
        ),
        (
            "search linearb",
            Some("linear"),
            "Linear",
            false,
            "a term that holds the namespace is not it",
        ),
        (
            "events of google calendar",
            Some("gcal"),
            "Google Calendar",
            true,
            "the query holds every word of the display name",
        ),
        (
            "events of my calendar",
            Some("gcal"),
            "Google Calendar",
            false,
            "the query holds one word of two",
        ),
        (
            "search github",
            Some("gh"),
            "GitHub",
            true,
            "a camelCase display name is one word",
        ),
        (
            "list workspace members",
            None,
            "Coral",
            false,
            "the Built-in Provider has no namespace, so `workspace` names nothing",
        ),
        (
            "coral providers",
            None,
            "Coral",
            true,
            "its display name still names it",
        ),
    ] {
        assert_eq!(
            plan(query).names_provider(namespace, display_name),
            named,
            "{query}: {reason}"
        );
    }
}

#[test]
fn a_content_word_matches_where_it_starts_a_word_of_a_field() {
    for (query, field, words, reason) in [
        (
            "open issues",
            "triage.openIssues",
            &["issues", "open"][..],
            "a camelCase edge and a separator end words",
        ),
        (
            "issues",
            "reissues",
            &[],
            "a word inside a longer word is none",
        ),
        (
            "deploy status",
            "deployments.listStatuses",
            &["deploy", "status"],
            "a word that starts a longer word counts, as the Tool statement's prefix match does",
        ),
        ("issues", "ISSUES", &["issues"], "case does not count"),
        (
            "the issues issues",
            "the issues",
            &["issues"],
            "a filler word counts nothing, and a repeated word counts once",
        ),
        (
            "openissues",
            "openIssues",
            &["openissues"],
            "the whole run is a word too",
        ),
        (
            "list invoices",
            "billing.invoicesDue",
            &["invoices"],
            "only the words that start a word of the field are named",
        ),
    ] {
        assert_eq!(
            plan(query).content_words_starting([field]),
            words,
            "{query} in {field}: {reason}"
        );
    }
}

#[test]
fn a_path_holds_its_words_of_three_or_more_characters() {
    for (path, words, reason) in [
        (
            "github.pulls.listReviews",
            &["github", "list", "listreviews", "pulls", "reviews"][..],
            "the namespace and each segment, camelCase parts and the whole run",
        ),
        (
            "ab.v2.getById",
            &["get", "getbyid"][..],
            "a word of under three characters can start no content word",
        ),
        (
            "issues.getIssue",
            &["get", "getissue", "issue", "issues"][..],
            "each word once",
        ),
    ] {
        let expected = words
            .iter()
            .map(|word| (*word).to_string())
            .collect::<BTreeSet<_>>();
        assert_eq!(path_words(path), expected, "{path}: {reason}");
    }
}
