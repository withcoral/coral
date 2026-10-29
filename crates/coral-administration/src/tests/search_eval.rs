//! Universal Search smoke test. It checks that a change has not made search
//! unusable; it cannot rank two tunings, since a few dozen hand-written
//! queries leave gaps of 0.05 to 0.08 in the noise. Measure ranking decisions
//! in coral-benchmarks (`tools/universal-search`).
//!
//! Each run publishes one OpenAPI document, waits for the index, runs each
//! query in a query file through search, and prints where the first accepted
//! Tool ranks in the first page of hits.
//!
//! `github_search_stays_usable` runs in CI against a pinned copy of GitHub's
//! document and fails when hit@10 falls below a floor. `search_quality_eval`
//! runs by hand against any document, such as Microsoft Graph, which takes
//! minutes to index:
//!
//! ```sh
//! SEARCH_EVAL_DOC=graph.yaml \
//! SEARCH_EVAL_QUERIES=crates/coral-administration/src/tests/search_eval/graph.yaml \
//! cargo test --release -p coral-administration --lib search_quality_eval \
//!   -- --ignored --nocapture
//! ```
//!
//! Fetch the document from the `source` URL of its recipe under
//! `apps/reef/recipes`. Optional variables:
//!
//! - `SEARCH_EVAL_WRITE_LIMIT_SECS`: the index write limit; default one hour,
//!   so a Provider larger than the server's limit still indexes.
//! - `SEARCH_EVAL_DUMP`: a file to write every indexed Tool path to, one
//!   per line, for writing queries.
//!
//! A query file is a YAML list. Each item has a `query`, as an agent might
//! phrase it, and the Tool paths below the Provider namespace that it
//! `accept`s:
//!
//! ```yaml
//! - query: create an issue
//!   accept: [issues.create]
//! - query: forward an email
//!   accept: ["me.*messages.message.forward"]
//! ```
//!
//! A `*` matches any run of characters, dots included, so one pattern can
//! accept every path to the same action. Quote a pattern that has one, since
//! YAML reads a leading `*` as an alias. Every query must accept at least one
//! path, and every path or pattern must match at least one indexed Tool.
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::time::{Duration, Instant};

use super::change;
use super::providers::Fixture;

const CALL_PATH_PREFIX: &str = "coral.providers.weather.";

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    query: String,
    accept: Vec<String>,
}

/// Whether `path` matches `pattern`, where each `*` matches any run of
/// characters.
fn matches(pattern: &str, path: &str) -> bool {
    let mut parts = pattern.split('*');
    let first = parts.next().unwrap_or_default();
    let Some(mut rest) = path.strip_prefix(first) else {
        return false;
    };
    let mut parts = parts.collect::<Vec<_>>();
    let Some(last) = parts.pop() else {
        return rest.is_empty();
    };
    for part in parts {
        let Some((_, after)) = rest.split_once(part) else {
            return false;
        };
        rest = after;
    }
    rest.len() >= last.len() && rest.ends_with(last)
}

fn relative(call_path: &str) -> &str {
    call_path
        .strip_prefix(CALL_PATH_PREFIX)
        .unwrap_or(call_path)
}

#[expect(
    clippy::disallowed_methods,
    reason = "this hand-run eval is its own process bootstrap and reads its inputs once"
)]
fn variable(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// Where the first accepted Tool ranks, from 1, or `None` when no hit
/// on the first page is accepted.
type Rank = Option<usize>;

/// GitHub's document, pinned at github/rest-api-description
/// 507b88ecbdccb4789ce77228fa2f2fde4bdeaf1f. Keep it frozen: a new copy moves
/// the scores and adds 10 MB to history.
const GITHUB_DOCUMENT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/tests/search_eval/github.openapi.yaml"
);
const GITHUB_QUERIES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/tests/search_eval/github.yaml"
);
/// Well below today's GitHub hit@10, so only a change that breaks search
/// trips it.
const GITHUB_HIT_AT_10_FLOOR: f64 = 0.7;

#[tokio::test(flavor = "multi_thread")]
async fn github_search_stays_usable() {
    let scores = evaluate(GITHUB_DOCUMENT, GITHUB_QUERIES, None, None).await;
    assert!(
        scores.hit_at_10 >= GITHUB_HIT_AT_10_FLOOR,
        "GitHub hit@10 {:.2} is below the floor {GITHUB_HIT_AT_10_FLOOR}",
        scores.hit_at_10
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "search-quality eval; run by hand with SEARCH_EVAL_DOC and SEARCH_EVAL_QUERIES"]
async fn search_quality_eval() {
    let document_path = variable("SEARCH_EVAL_DOC").expect("SEARCH_EVAL_DOC names a document");
    let queries_path =
        variable("SEARCH_EVAL_QUERIES").expect("SEARCH_EVAL_QUERIES names a query file");
    let write_limit = Duration::from_secs(
        variable("SEARCH_EVAL_WRITE_LIMIT_SECS").map_or(3600, |secs| secs.parse().unwrap()),
    );
    evaluate(
        &document_path,
        &queries_path,
        Some(write_limit),
        variable("SEARCH_EVAL_DUMP"),
    )
    .await;
}

/// Publishes the document, runs every query, reports each rank, and returns
/// the scores. Index writes may run for `write_limit`, or for the index's
/// default when `None`; `dump` names a file for every indexed Tool path.
async fn evaluate(
    document_path: &str,
    queries_path: &str,
    write_limit: Option<Duration>,
    dump: Option<String>,
) -> Scores {
    let document = std::fs::read_to_string(document_path).unwrap();
    let cases: Vec<Case> =
        serde_yaml::from_str(&std::fs::read_to_string(queries_path).unwrap()).unwrap();

    let f = Fixture::with_write_limit(write_limit).await;
    let setup = f.setup(&document).await;
    let started = Instant::now();
    let publication = f
        .workflow
        .publish_workspace_provider(&change(&f.owner), f.creation(&setup))
        .await
        .unwrap();
    assert!(
        publication.published_provider().is_some(),
        "publication failed: {:?}",
        publication.outcome()
    );
    // Publication starts the index write before it returns, so time the two
    // together.
    f.index.settled().await;
    let indexed = started.elapsed();
    let states = f
        .generations()
        .await
        .into_iter()
        .map(|generation| generation.state)
        .collect::<Vec<_>>();
    assert_eq!(states, ["complete"], "the index write did not complete");

    let tools = f
        .tools()
        .await
        .into_iter()
        .map(|(call_path, _)| relative(&call_path).to_string())
        .collect::<BTreeSet<_>>();
    if let Some(dump) = dump {
        let lines = tools.iter().fold(String::new(), |mut lines, path| {
            writeln!(lines, "{path}").unwrap();
            lines
        });
        std::fs::write(dump, lines).unwrap();
    }
    let unanswerable = cases
        .iter()
        .filter(|case| case.accept.is_empty())
        .map(|case| &case.query)
        .collect::<Vec<_>>();
    assert!(
        unanswerable.is_empty(),
        "queries that accept no Tool: {unanswerable:?}"
    );
    let unknown = cases
        .iter()
        .flat_map(|case| &case.accept)
        .filter(|pattern| !tools.iter().any(|path| matches(pattern, path)))
        .collect::<Vec<_>>();
    assert!(
        unknown.is_empty(),
        "accepted paths or patterns that match no indexed Tool: {unknown:?}"
    );

    let mut lines = String::new();
    let mut ranks = Vec::with_capacity(cases.len());
    let started = Instant::now();
    for case in &cases {
        let hits = f.found(&case.query).await;
        let rank = hits
            .iter()
            .position(|(call_path, _)| {
                case.accept
                    .iter()
                    .any(|pattern| matches(pattern, relative(call_path)))
            })
            .map(|index| index + 1);
        ranks.push(rank);
        writeln!(
            lines,
            "{:>4}  {}  [top: {}]",
            rank.map_or("-".to_string(), |rank| rank.to_string()),
            case.query,
            hits.first()
                .map_or("none", |(call_path, _)| relative(call_path)),
        )
        .unwrap();
    }
    let searched = started.elapsed();
    let scores = Scores::of(&ranks);
    report(&format!(
        "\nsearch eval: {document_path}\n{} Tools, {} queries; publish and index {indexed:.1?}, search {searched:.1?} in all\n\nrank  query\n{lines}\n{scores}\n",
        tools.len(),
        cases.len(),
    ));
    scores
}

struct Scores {
    hit_at_1: f64,
    hit_at_5: f64,
    hit_at_10: f64,
    reciprocal_rank: f64,
}

impl Scores {
    #[expect(
        clippy::cast_precision_loss,
        reason = "query counts and ranks are small"
    )]
    fn of(ranks: &[Rank]) -> Self {
        let count = ranks.len() as f64;
        let share = |within: usize| {
            ranks
                .iter()
                .filter(|rank| rank.is_some_and(|rank| rank <= within))
                .count() as f64
                / count
        };
        Self {
            hit_at_1: share(1),
            hit_at_5: share(5),
            hit_at_10: share(10),
            reciprocal_rank: ranks
                .iter()
                .map(|rank| rank.map_or(0.0, |rank| 1.0 / rank as f64))
                .sum::<f64>()
                / count,
        }
    }
}

impl std::fmt::Display for Scores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "hit@1 {:.2}  hit@5 {:.2}  hit@10 {:.2}  MRR {:.3}",
            self.hit_at_1, self.hit_at_5, self.hit_at_10, self.reciprocal_rank,
        )
    }
}

#[expect(
    clippy::print_stderr,
    reason = "the eval's report is its output; run it with --nocapture"
)]
fn report(text: &str) {
    eprintln!("{text}");
}

#[test]
fn a_star_matches_any_run_of_characters_and_nothing_else_is_loose() {
    for (pattern, path, expected) in [
        ("issues.create", "issues.create", true),
        ("issues.create", "issues.createComment", false),
        ("issues.create", "x.issues.create", false),
        (
            "me.*messages.message.reply",
            "me.messages.message.reply",
            true,
        ),
        (
            "me.*messages.message.reply",
            "me.mailFolders.mailFolder.messages.message.reply",
            true,
        ),
        (
            "me.*messages.message.reply",
            "me.messages.message.replyAll",
            false,
        ),
        (
            "me.*messages.message.reply",
            "users.messages.message.reply",
            false,
        ),
        ("me.*.listEvents", "me.listEvents", false),
        ("*Events", "me.listEvents", true),
        ("a*b*b", "ab", false),
        ("a*b*b", "abb", true),
    ] {
        assert_eq!(matches(pattern, path), expected, "{pattern} vs {path}");
    }
}
