//! Search and describe helpers over workspace exports.

use std::collections::{BTreeMap, BTreeSet};

use coral_capabilities::{CapabilityId, CapabilityKind, EffectKind, SourceId, SupportStatus};
use serde::{Deserialize, Serialize};

use crate::exports::{Binding, CapabilityExport, ExportKind, WorkspaceExports};

const SEARCH_DESCRIPTION_PREVIEW_CHARS: usize = 320;

/// Explicit read-vs-write ranking intent for one search query.
///
/// A soft ordering hint consumed only by the read-vs-write sort tier (see
/// `rank_tier`) — it never filters results and never counts as an active
/// filter constraint. `Read` keeps reads-first tiering active even when the
/// query contains a write verb; `Write` and `Any` collapse every entry into
/// one neutral tier ordered by content score. When absent, intent falls back
/// to write-verb detection over the query tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchIntent {
    Read,
    Write,
    Any,
}

/// Search filter.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchFilter {
    pub source_key: Option<String>,
    pub display_name: Option<String>,
    pub kind: Option<ExportKind>,
    pub allowed_kinds: Vec<ExportKind>,
    pub capability_kind: Option<CapabilityKind>,
    pub effect: Option<EffectKind>,
    /// Soft ranking hint; never gates results or activates filtering.
    pub intent: Option<SearchIntent>,
}

/// Compact search result item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchResult {
    pub alias: Option<String>,
    pub full_path: Option<String>,
    /// One-line generated-call signature for `full_path`, e.g.
    /// `tools.slack.conversations.history({ channel: string, …+4 }) -> value:
    /// object`. Left `None` here: filling it needs `Capability` access, which
    /// this crate does not have, so capability-aware callers (coral-app's
    /// discovery manager) enrich it after scoring.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    pub capability_id: CapabilityId,
    pub refs: Vec<String>,
    pub source_id: SourceId,
    pub display_name: String,
    pub source_key: String,
    pub capability_kind: CapabilityKind,
    pub effects: Vec<EffectKind>,
    pub title: String,
    pub description: String,
    pub deprecated: bool,
    pub support_status: SupportStatus,
    pub available_bindings: Vec<ExportKind>,
    pub diagnostic_count: usize,
    pub score: u32,
    pub matched_fields: Vec<String>,
    pub matched_terms: Vec<String>,
    pub rank_reason: String,
}

/// Paged search result with total matches before pagination.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchResultsPage {
    pub items: Vec<SearchResult>,
    pub total: usize,
}

/// Describe resolution outcome.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
#[expect(
    clippy::large_enum_variant,
    reason = "Describe responses serialize as direct export objects; boxing would only obscure the artifact-facing shape."
)]
pub enum DescribeResolution {
    Found { entry: CapabilityExport },
    Ambiguous { candidates: Vec<CapabilityExport> },
    NotFound,
}

/// Searches workspace exports with deterministic exact-match precedence.
#[must_use]
pub fn search_exports(
    exports: &WorkspaceExports,
    query: &str,
    filter: &SearchFilter,
    limit: usize,
) -> Vec<SearchResult> {
    search_exports_page(exports, query, filter, limit, 0).items
}

/// Searches workspace exports and returns total matches before pagination.
#[must_use]
pub fn search_exports_page(
    exports: &WorkspaceExports,
    query: &str,
    filter: &SearchFilter,
    limit: usize,
    offset: usize,
) -> SearchResultsPage {
    let query = query.trim();
    let candidates = exports
        .entries
        .iter()
        .filter(|entry| matches_filter(entry, filter))
        .collect::<Vec<_>>();
    let mut scored = if query.is_empty() {
        if filter_has_active_constraints(filter) {
            candidates
                .into_iter()
                .map(|entry| (empty_query_score(entry, filter.intent), entry))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        }
    } else {
        // Inverse document frequency is computed over the candidate corpus so
        // common tokens (e.g. `search`, `list`, the source key) self-attenuate
        // without a hand-maintained stopword blacklist. It is a pure function of
        // (query, candidates), so ranking stays deterministic.
        let query_tokens = query_tokens(query);
        let idf = compute_idf(&candidates, &query_tokens);
        candidates
            .into_iter()
            .filter_map(|entry| {
                score_entry(entry, query, &idf, filter.intent).map(|score| (score, entry))
            })
            .collect::<Vec<_>>()
    };
    scored.sort_by(|(left_score, left), (right_score, right)| {
        left_score
            .tier
            .cmp(&right_score.tier)
            .then_with(|| right_score.value.cmp(&left_score.value))
            .then_with(|| left.capability_id.cmp(&right.capability_id))
    });
    let total = scored.len();
    let start = offset.min(total);
    let end = start.saturating_add(limit).min(total);
    let items = scored
        .into_iter()
        .skip(start)
        .take(end.saturating_sub(start))
        .map(|(score, entry)| search_result(entry, score))
        .collect();
    SearchResultsPage { items, total }
}

fn empty_query_score(entry: &CapabilityExport, intent: Option<SearchIntent>) -> SearchScore {
    // An empty query carries no write-intent tokens, so filtered browsing
    // lists reads before mutations unless an explicit intent says otherwise.
    SearchScore {
        value: 0,
        tier: rank_tier(entry, neutral_tier_ordering(intent, &BTreeSet::new())),
        matched_fields: Vec::new(),
        matched_terms: Vec::new(),
        rank_reason: "matched active filters".to_string(),
    }
}

/// Resolves whether tiering goes neutral (every entry tier 0) for one query.
///
/// An explicit [`SearchIntent`] always overrides token detection: `Read`
/// keeps reads-first tiering even for write-verb queries, while `Write` and
/// `Any` order every entry by content score alone. Absent intent falls back
/// to write-verb detection over the stemmed query tokens.
fn neutral_tier_ordering(intent: Option<SearchIntent>, query_tokens: &BTreeSet<String>) -> bool {
    match intent {
        Some(SearchIntent::Read) => false,
        Some(SearchIntent::Write | SearchIntent::Any) => true,
        None => query_tokens.iter().any(|token| is_write_intent(token)),
    }
}

fn filter_has_active_constraints(filter: &SearchFilter) -> bool {
    filter.source_key.is_some()
        || filter.display_name.is_some()
        || filter.kind.is_some()
        || !filter.allowed_kinds.is_empty()
        || filter.capability_kind.is_some()
        || filter.effect.is_some()
}

/// Describes one export by typed ref, capability id, or unambiguous alias.
#[must_use]
pub fn describe_export(exports: &WorkspaceExports, raw_ref: &str) -> DescribeResolution {
    let raw_ref = raw_ref.trim();
    if raw_ref.is_empty() {
        return DescribeResolution::NotFound;
    }
    let typed = raw_ref.contains(':');
    let mut exact = exports
        .entries
        .iter()
        .filter(|entry| {
            entry.capability_id.as_str() == raw_ref
                || entry
                    .bindings
                    .iter()
                    .any(|binding| binding.ref_().value == raw_ref)
                || (!typed
                    && entry
                        .bindings
                        .iter()
                        .any(|binding| binding.alias() == raw_ref))
                || entry
                    .bindings
                    .iter()
                    .any(|binding| binding.full_path().as_deref() == Some(raw_ref))
        })
        .cloned()
        .collect::<Vec<_>>();
    exact.sort_by(|left, right| left.capability_id.cmp(&right.capability_id));
    match exact.len() {
        0 => DescribeResolution::NotFound,
        1 => DescribeResolution::Found {
            entry: exact.remove(0),
        },
        _ => DescribeResolution::Ambiguous { candidates: exact },
    }
}

fn matches_filter(entry: &CapabilityExport, filter: &SearchFilter) -> bool {
    if filter
        .source_key
        .as_ref()
        .is_some_and(|source_key| source_key != entry.source_key.as_str())
    {
        return false;
    }
    if filter
        .display_name
        .as_ref()
        .is_some_and(|display_name| display_name != &entry.display_name)
    {
        return false;
    }
    if filter
        .capability_kind
        .is_some_and(|kind| kind != entry.effect_profile.capability_kind)
    {
        return false;
    }
    if filter
        .effect
        .is_some_and(|effect| !entry.effect_profile.effects.contains(&effect))
    {
        return false;
    }
    if filter.kind.is_some_and(|kind| {
        !entry
            .bindings
            .iter()
            .any(|binding| binding.ref_().kind == kind)
    }) {
        return false;
    }
    filter.allowed_kinds.is_empty()
        || entry
            .bindings
            .iter()
            .any(|binding| filter.allowed_kinds.contains(&binding.ref_().kind))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchScore {
    value: u32,
    /// Read-vs-write sort tier (lower sorts first). Content scores accumulate
    /// per matched field, so a parameter-heavy mutation can out-accumulate a
    /// concise read endpoint by thousands of points; the tier makes effect
    /// intent a hard ordering boundary instead of an additive nudge that
    /// content volume can swamp. See [`rank_tier`].
    tier: u8,
    matched_fields: Vec<String>,
    matched_terms: Vec<String>,
    rank_reason: String,
}

#[derive(Debug, Clone)]
struct SearchField {
    name: &'static str,
    text: String,
    weight: u32,
}

#[derive(Debug, Default)]
struct FieldScore {
    value: u32,
    matched_fields: BTreeSet<String>,
    matched_tokens: BTreeSet<String>,
    phrase_matched: bool,
}

fn score_entry(
    entry: &CapabilityExport,
    query: &str,
    idf: &BTreeMap<String, u32>,
    intent: Option<SearchIntent>,
) -> Option<SearchScore> {
    if query.is_empty() {
        return None;
    }
    if let Some(score) = exact_match_score(entry, query) {
        return Some(score);
    }

    let query_tokens = query_tokens(query);
    if query_tokens.is_empty() {
        return None;
    }
    let mut score = score_fields(entry, query, &query_tokens, idf);
    // Rank, never gate. A capability becomes a candidate as soon as a single
    // query token or the query phrase overlaps one of its fields. The score
    // only orders results from there; it can never drop a term-overlapping
    // capability. (Earlier builds dropped capabilities here via an all-tokens
    // coverage gate and via specialization penalties that could zero a genuine
    // match — both removed deliberately, since a search miss is unrecoverable
    // for the LLM consumer while an over-broad page is a cheap describe pass.)
    if score.matched_tokens.is_empty() && !score.phrase_matched {
        return None;
    }
    apply_rank_boosts(entry, &mut score, intent);
    Some(search_score(
        score,
        query_tokens.len(),
        rank_tier(entry, neutral_tier_ordering(intent, &query_tokens)),
    ))
}

fn exact_match_score(entry: &CapabilityExport, query: &str) -> Option<SearchScore> {
    if entry
        .bindings
        .iter()
        .any(|binding| binding.ref_().value == query)
    {
        return Some(exact_score(100_000, "refs", "exact typed ref"));
    }
    if entry.capability_id.as_str() == query {
        return Some(exact_score(95_000, "capability_id", "exact capability id"));
    }
    if entry
        .bindings
        .iter()
        .any(|binding| binding.full_path().as_deref() == Some(query))
    {
        return Some(exact_score(
            92_000,
            "full_path",
            "exact generated tool path",
        ));
    }
    if entry
        .bindings
        .iter()
        .any(|binding| binding.alias() == query)
    {
        return Some(exact_score(90_000, "alias", "exact alias"));
    }
    None
}

fn score_fields(
    entry: &CapabilityExport,
    query: &str,
    query_tokens: &BTreeSet<String>,
    idf: &BTreeMap<String, u32>,
) -> FieldScore {
    let query_phrase = normalized_phrase(query);
    let mut score = FieldScore::default();
    for field in search_fields(entry) {
        score_field(&mut score, &field, &query_phrase, query_tokens, idf);
    }
    score
}

fn score_field(
    score: &mut FieldScore,
    field: &SearchField,
    query_phrase: &str,
    query_tokens: &BTreeSet<String>,
    idf: &BTreeMap<String, u32>,
) {
    let field_phrase = normalized_phrase(&field.text);
    if field_phrase == query_phrase {
        score.value = score
            .value
            .saturating_add(3_000 + field.weight.saturating_mul(100));
        score.matched_fields.insert(field.name.to_string());
        score.matched_tokens.extend(query_tokens.iter().cloned());
        score.phrase_matched = true;
        return;
    }
    if !query_phrase.is_empty() && field_phrase.contains(query_phrase) {
        score.value = score
            .value
            .saturating_add(1_200 + field.weight.saturating_mul(80));
        score.matched_fields.insert(field.name.to_string());
        score.matched_tokens.extend(query_tokens.iter().cloned());
        score.phrase_matched = true;
    }
    score_token_matches(score, field, query_tokens, idf);
}

fn score_token_matches(
    score: &mut FieldScore,
    field: &SearchField,
    query_tokens: &BTreeSet<String>,
    idf: &BTreeMap<String, u32>,
) {
    let field_tokens = token_set(&field.text);
    let mut field_value = 0_u32;
    let mut matched_any = false;
    for token in query_tokens {
        if field_tokens.contains(token) {
            matched_any = true;
            score.matched_tokens.insert(token.clone());
            let weight = idf.get(token).copied().unwrap_or(1);
            field_value =
                field_value.saturating_add(field.weight.saturating_mul(10).saturating_mul(weight));
        }
    }
    if matched_any {
        score.value = score.value.saturating_add(field_value);
        score.matched_fields.insert(field.name.to_string());
    }
}

/// Bucketed inverse document frequency for the query tokens, computed over the
/// candidate corpus. Buckets are small integers so ranking stays bit-exact and
/// deterministic; common tokens (e.g. `search`, `list`, the source key)
/// contribute less than rare ones without a hand-maintained stopword blacklist.
fn compute_idf(
    candidates: &[&CapabilityExport],
    query_tokens: &BTreeSet<String>,
) -> BTreeMap<String, u32> {
    let total = candidates.len();
    let mut document_frequency = query_tokens
        .iter()
        .map(|token| (token.clone(), 0_usize))
        .collect::<BTreeMap<String, usize>>();
    for entry in candidates {
        let mut entry_tokens = BTreeSet::new();
        for field in search_fields(entry) {
            entry_tokens.extend(token_set(&field.text));
        }
        for token in query_tokens {
            if entry_tokens.contains(token)
                && let Some(count) = document_frequency.get_mut(token)
            {
                *count += 1;
            }
        }
    }
    document_frequency
        .into_iter()
        .map(|(token, frequency)| (token, idf_bucket(frequency, total)))
        .collect()
}

fn idf_bucket(document_frequency: usize, total: usize) -> u32 {
    if total == 0 || document_frequency == 0 {
        return 3;
    }
    let percent = document_frequency.saturating_mul(100) / total;
    if percent <= 10 {
        3
    } else if percent <= 50 {
        2
    } else {
        1
    }
}

fn apply_rank_boosts(
    entry: &CapabilityExport,
    score: &mut FieldScore,
    intent: Option<SearchIntent>,
) {
    if matches!(intent, Some(SearchIntent::Write | SearchIntent::Any)) {
        return;
    }
    // Read-vs-write ordering is owned by the sort tier (see `rank_tier`); these
    // small boosts only order entries WITHIN a tier, e.g. under write intent
    // (where every entry is tier 0) or between Read-effect and Query-kind
    // entries that landed in the same tier. The old -25 mutation penalty is
    // gone: it was redundant next to the tier and too small to matter anyway.
    if entry.effect_profile.effects.contains(&EffectKind::Read) {
        score.value = score.value.saturating_add(100);
    }
    if entry.effect_profile.capability_kind == CapabilityKind::Query {
        score.value = score.value.saturating_add(50);
    }
}

/// Read-vs-write sort tier for one entry under the query's intent.
///
/// Under read intent (no write-intent token in the query), reads sort ahead of
/// everything else as a hard boundary: entries with any Write/Delete effect are
/// tier 2 (the most write-ish effect wins — a capability that CAN write is a
/// mutation for ranking purposes), Read-effect or Query-kind entries are
/// tier 0, and Unknown/other entries sit between at tier 1.
///
/// Under write intent every entry is tier 0: write tasks still need read
/// lookups, so the ordering goes neutral rather than inverting. An explicit
/// [`SearchIntent`] on the filter overrides token detection entirely (see
/// [`neutral_tier_ordering`]).
///
/// Exact matches (typed ref, capability id, generated tool path, alias) never
/// consult this — the agent named the thing, so they are always tier 0.
fn rank_tier(entry: &CapabilityExport, write_intent: bool) -> u8 {
    if write_intent {
        return 0;
    }
    let profile = &entry.effect_profile;
    if profile
        .effects
        .iter()
        .any(|effect| matches!(effect, EffectKind::Write | EffectKind::Delete))
    {
        return 2;
    }
    if profile.effects.contains(&EffectKind::Read)
        || profile.capability_kind == CapabilityKind::Query
    {
        return 0;
    }
    1
}

fn search_score(score: FieldScore, query_token_count: usize, tier: u8) -> SearchScore {
    let matched_token_count = score.matched_tokens.len();
    let mut matched_fields = score.matched_fields.into_iter().collect::<Vec<_>>();
    matched_fields.sort();
    let matched_terms = score.matched_tokens.into_iter().collect::<Vec<_>>();
    SearchScore {
        value: score.value,
        tier,
        rank_reason: format!(
            "matched {} of {} token(s) in {}",
            matched_token_count,
            query_token_count,
            matched_fields.join(", ")
        ),
        matched_fields,
        matched_terms,
    }
}

fn exact_score(value: u32, field: &str, reason: &str) -> SearchScore {
    SearchScore {
        value,
        // The agent named the thing; an exact match is never reordered by
        // read-vs-write tiering, even for a mutation under a read-intent query.
        tier: 0,
        matched_fields: vec![field.to_string()],
        matched_terms: Vec::new(),
        rank_reason: reason.to_string(),
    }
}

fn search_fields(entry: &CapabilityExport) -> Vec<SearchField> {
    // Human-readable surfaces (title, parameter text via `search_text`,
    // description) outweigh machine identifiers. Weak models paraphrase the
    // title, not the camelCase identifier path, so identifiers are demoted and
    // indexed once rather than three times.
    let mut fields = vec![
        SearchField {
            name: "title",
            text: entry.title.clone(),
            weight: 10,
        },
        SearchField {
            name: "operation_id",
            text: entry.operation_id.clone(),
            weight: 7,
        },
        SearchField {
            name: "display_name",
            text: entry.display_name.clone(),
            weight: 6,
        },
        SearchField {
            name: "interface_id",
            text: entry.interface_id.clone(),
            weight: 5,
        },
        SearchField {
            name: "description",
            text: entry.description.clone(),
            weight: 5,
        },
        SearchField {
            name: "source_key",
            text: entry.source_key.as_str().to_string(),
            weight: 4,
        },
    ];
    // `search_text` carries the enriched document, including input-parameter
    // names and descriptions and output field names (see `base_search_text`).
    for text in &entry.search_text {
        fields.push(SearchField {
            name: "search_text",
            text: text.clone(),
            weight: 4,
        });
    }
    for binding in &entry.bindings {
        match binding {
            Binding::Typescript(binding) => {
                if !binding.path.is_empty() {
                    fields.push(SearchField {
                        name: "typescript_path",
                        text: format!("tools.{}", binding.path.join(".")),
                        weight: 3,
                    });
                }
            }
            Binding::Sql(binding) => {
                fields.push(SearchField {
                    name: "sql_reference",
                    text: binding.sql_reference.clone(),
                    weight: 3,
                });
                for column in &binding.projection.columns {
                    fields.push(SearchField {
                        name: "sql_column",
                        text: column.name.clone(),
                        weight: 3,
                    });
                    if !column.description.trim().is_empty() {
                        fields.push(SearchField {
                            name: "sql_column",
                            text: column.description.clone(),
                            weight: 2,
                        });
                    }
                }
                for input in &binding.projection.inputs {
                    fields.push(SearchField {
                        name: "sql_input",
                        text: input.name.clone(),
                        weight: 3,
                    });
                }
            }
        }
    }
    fields
}

fn query_tokens(query: &str) -> BTreeSet<String> {
    raw_tokens(query)
        .into_iter()
        .filter(|token| !is_query_stopword(token))
        .map(|token| stem(&token))
        .collect()
}

fn token_set(text: &str) -> BTreeSet<String> {
    raw_tokens(text)
        .into_iter()
        .map(|token| stem(&token))
        .collect()
}

fn raw_tokens(text: &str) -> Vec<String> {
    let mut normalized = String::with_capacity(text.len());
    let mut previous = None;
    for ch in text.chars() {
        if ch.is_ascii_uppercase()
            && previous.is_some_and(|previous: char| {
                previous.is_ascii_lowercase() || previous.is_ascii_digit()
            })
        {
            normalized.push(' ');
        }
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
        } else {
            normalized.push(' ');
        }
        previous = Some(ch);
    }
    normalized
        .split_whitespace()
        .filter(|token| !token.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn normalized_phrase(text: &str) -> String {
    raw_tokens(text).join(" ")
}

/// Folds a token to a canonical stem. Applied identically at index time and
/// query time so that `messages` and `message` collapse to the same term on
/// both sides — earlier builds expanded a token to multiple forms and then
/// required every form to match, which silently dropped pluralized queries.
/// Provider-specific vocabulary (e.g. `pr` → `pull request`) is intentionally
/// absent: domain synonyms live in the indexed document, never in the scorer.
fn stem(token: &str) -> String {
    if token.len() > 3
        && let Some(prefix) = token.strip_suffix('s')
    {
        return prefix.to_string();
    }
    token.to_string()
}

/// True function words only. Intent- and domain-bearing words (`find`, `count`,
/// `open`, `review`, `coral`, …) are no longer stripped: inverse document
/// frequency down-weights common tokens, and ranking never gates on coverage,
/// so a non-matching token is simply inert rather than fatal.
fn is_query_stopword(token: &str) -> bool {
    matches!(
        token,
        "a" | "an" | "and" | "for" | "i" | "in" | "of" | "or" | "the" | "to"
    )
}

/// Unambiguous write-intent verbs, stored in STEMMED form — the same [`stem`]
/// applied to query tokens — so membership compares like with like (e.g. the
/// query token `dismiss` stems to `dismis`, and so must the entry here; a unit
/// test asserts this list equals the stems of the natural verb forms, so a
/// future entry stored unstemmed fails loudly). Ambiguous nouns such as
/// `comment` and `open` are deliberately excluded: they appear in far more
/// read queries than write ones.
const WRITE_INTENT_STEMS: &[&str] = &[
    "add",
    "approve",
    "archive",
    "assign",
    "cancel",
    "close",
    "create",
    "delete",
    "dismis",
    "edit",
    "invite",
    "lock",
    "merge",
    "move",
    "mutate",
    "mutation",
    "pin",
    "post",
    "publish",
    "remove",
    "rename",
    "reopen",
    "resolve",
    "schedule",
    "send",
    "set",
    "submit",
    "transfer",
    "unarchive",
    "unassign",
    "unlock",
    "unpin",
    "update",
    "upload",
    "write",
];

fn is_write_intent(token: &str) -> bool {
    WRITE_INTENT_STEMS.contains(&token)
}

fn search_result(entry: &CapabilityExport, score: SearchScore) -> SearchResult {
    let refs = entry
        .bindings
        .iter()
        .map(|binding| binding.ref_().value.clone())
        .collect::<Vec<_>>();
    let available_bindings = entry
        .bindings
        .iter()
        .map(|binding| binding.ref_().kind)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    SearchResult {
        alias: entry.bindings.first().map(Binding::alias),
        full_path: entry.bindings.iter().find_map(Binding::full_path),
        signature: None,
        capability_id: entry.capability_id.clone(),
        refs,
        source_id: entry.source_id.clone(),
        display_name: entry.display_name.clone(),
        source_key: entry.source_key.as_str().to_string(),
        capability_kind: entry.effect_profile.capability_kind,
        effects: entry.effect_profile.effects.clone(),
        title: entry.title.clone(),
        description: search_description_preview(&entry.description),
        deprecated: entry.deprecated,
        support_status: entry.support_status,
        available_bindings,
        diagnostic_count: entry.diagnostics.len(),
        score: score.value,
        matched_fields: score.matched_fields,
        matched_terms: score.matched_terms,
        rank_reason: score.rank_reason,
    }
}

fn search_description_preview(description: &str) -> String {
    let preview = description
        .trim()
        .split("\n\n")
        .find(|paragraph| !paragraph.trim().is_empty())
        .unwrap_or_default()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if preview.chars().count() <= SEARCH_DESCRIPTION_PREVIEW_CHARS {
        return preview;
    }
    let mut truncated = preview
        .chars()
        .take(SEARCH_DESCRIPTION_PREVIEW_CHARS)
        .collect::<String>();
    truncated.truncate(truncated.trim_end().len());
    truncated.push_str(" ...");
    truncated
}

#[cfg(test)]
mod tests {
    use coral_capabilities::{
        Capability, EffectKind, EffectProfile, FileFormatDescriptor, FileScanBinding,
        InvocationSchema, ProviderOrigin, ProviderOriginKind, SourceCapabilitySet, SourceId,
        SupportStatus, UpstreamBinding,
    };

    use crate::contributors::TypescriptBindingContributor;
    use crate::exports::{BindingBuildContext, build_source_exports, compose_workspace_exports};
    use crate::package::SourceKey;

    use super::{
        DescribeResolution, SEARCH_DESCRIPTION_PREVIEW_CHARS, SearchFilter, SearchIntent,
        describe_export, search_exports, search_exports_page,
    };

    fn workspace() -> crate::WorkspaceExports {
        let source_id = SourceId("src_demo".to_string());
        let mut capability = Capability::new(
            source_id.clone(),
            "files",
            "issues",
            ProviderOrigin {
                kind: ProviderOriginKind::FileRelation,
                snapshot_ref: "interfaces/files/provider-snapshot.yaml#/files/issues".to_string(),
                provider_name: "issues".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::FileRead(FileScanBinding {
                file_refs: Vec::new(),
                format: FileFormatDescriptor::Jsonl,
                schema_ref: None,
            }),
        );
        capability.effect_profile = EffectProfile::read();
        capability.display.title = "Issues".to_string();
        let set = SourceCapabilitySet::new(source_id.clone(), vec![capability]);
        let exports = build_source_exports(
            &set,
            &BindingBuildContext {
                source_id,
                display_name: "Demo".to_string(),
                source_key: SourceKey("demo".to_string()),
            },
            &[&TypescriptBindingContributor::new()],
        )
        .expect("source exports");
        compose_workspace_exports("default", &[exports]).expect("workspace exports")
    }

    fn github_workspace() -> crate::WorkspaceExports {
        let source_id = SourceId("src_github".to_string());
        let ctx = BindingBuildContext {
            source_id: source_id.clone(),
            display_name: "GitHub".to_string(),
            source_key: SourceKey("github".to_string()),
        };
        let set = SourceCapabilitySet::new(source_id.clone(), github_capabilities(&source_id));
        let exports = build_source_exports(&set, &ctx, &[&TypescriptBindingContributor::new()])
            .expect("source exports");
        compose_workspace_exports("default", &[exports]).expect("workspace exports")
    }

    #[test]
    fn empty_query_lists_filtered_source_entries() {
        let workspace = github_workspace();
        let page = search_exports_page(
            &workspace,
            "",
            &SearchFilter {
                source_key: Some("github".to_string()),
                ..Default::default()
            },
            10,
            0,
        );

        assert!(
            page.total > 0,
            "source filter with an empty query should enumerate matching entries"
        );
        assert!(
            page.items.iter().all(|item| item.source_key == "github"),
            "expected only github entries: {:?}",
            page.items
        );
        assert!(
            page.items
                .iter()
                .all(|item| item.rank_reason == "matched active filters"),
            "expected empty-query rank reason: {:?}",
            page.items
        );
    }

    fn github_capabilities(source_id: &SourceId) -> Vec<Capability> {
        vec![
            github_rest_capability(
                source_id,
                "actionsApproveWorkflowRun",
                "actions",
                "Approve workflow run for a fork pull request",
                "Approve a workflow run for a fork pull request",
                EffectProfile::write(),
            ),
            github_rest_capability(
                source_id,
                "pullsList",
                "pulls",
                "List pull requests",
                "List GitHub pull requests for a repository",
                EffectProfile::read(),
            ),
            github_rest_capability(
                source_id,
                "pullsListReviews",
                "pullsListReviews",
                "List reviews for a pull request",
                "List reviews for a GitHub pull request",
                EffectProfile::read(),
            ),
            github_rest_capability(
                source_id,
                "pullsMerge",
                "pulls",
                "Merge a pull request",
                "Merge a GitHub pull request",
                EffectProfile::write(),
            ),
            // A mutation whose identifier and prose contain no write-intent
            // verbs, so exact-ref queries for it read as read-intent.
            github_rest_capability(
                source_id,
                "reposDeployPages",
                "repos",
                "Deploy a GitHub Pages site",
                "Deploy a GitHub Pages site build",
                EffectProfile::write(),
            ),
            github_rest_capability(
                source_id,
                "reactionsListForPullRequestReviewComment",
                "reactions",
                "List reactions for a pull request review comment",
                "List reactions for a pull request review comment",
                EffectProfile::read(),
            ),
            github_rest_capability(
                source_id,
                "reposGetPullRequestReviewProtection",
                "repos",
                "Get pull request review protection",
                "Get pull request review protection for a repository branch",
                EffectProfile::read(),
            ),
            github_rest_capability(
                source_id,
                "searchIssuesAndPullRequests",
                "searchIssuesAndPullRequests",
                "Search issues and pull requests",
                "Search GitHub issues and pull requests",
                EffectProfile::read(),
            ),
            github_rest_capability(
                source_id,
                "searchCode",
                "searchCode",
                "Search code",
                "Search code across repositories, issues, pull requests, users, and commits",
                EffectProfile::read(),
            ),
            github_rest_capability(
                source_id,
                "usersGetAuthenticated",
                "usersGetAuthenticated",
                "Get the authenticated user",
                "Get the authenticated GitHub user profile, including login and username.",
                EffectProfile::read(),
            ),
            github_rest_capability(
                source_id,
                "usersListBlockedByAuthenticatedUser",
                "usersListBlockedByAuthenticatedUser",
                "List users blocked by the authenticated user",
                "List the users blocked by the authenticated GitHub user.",
                EffectProfile::read(),
            ),
            github_rest_capability(
                source_id,
                "packagesGetPackageForAuthenticatedUser",
                "packagesGetPackageForAuthenticatedUser",
                "Get a package for the authenticated user",
                "Gets a package owned by the authenticated user.",
                EffectProfile::read(),
            ),
        ]
    }

    fn github_rest_capability(
        source_id: &SourceId,
        operation_id: &str,
        provider_name: &str,
        title: &str,
        description: &str,
        effect_profile: EffectProfile,
    ) -> Capability {
        let mut capability = Capability::new(
            source_id.clone(),
            "rest",
            operation_id,
            ProviderOrigin {
                kind: ProviderOriginKind::RestOperation,
                snapshot_ref: format!("interfaces/rest/provider-snapshot.yaml#/{operation_id}"),
                provider_name: provider_name.to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::FileRead(FileScanBinding {
                file_refs: Vec::new(),
                format: FileFormatDescriptor::Jsonl,
                schema_ref: None,
            }),
        );
        capability.effect_profile = effect_profile;
        capability.display.title = title.to_string();
        capability.display.description = description.to_string();
        capability
    }

    #[test]
    fn search_exact_typed_ref_wins() {
        let workspace = workspace();
        let hits = search_exports(
            &workspace,
            "typescript:demo.files.issues",
            &SearchFilter::default(),
            10,
        );
        assert_eq!(hits.len(), 1);
        assert_eq!(
            hits.first().and_then(|hit| hit.alias.as_deref()),
            Some("demo.files.issues")
        );
    }

    #[test]
    fn search_exact_generated_tool_path_wins() {
        let workspace = workspace();
        let hits = search_exports(
            &workspace,
            "tools.demo.files.issues",
            &SearchFilter::default(),
            10,
        );

        assert_eq!(hits.len(), 1);
        assert_eq!(
            hits.first().and_then(|hit| hit.full_path.as_deref()),
            Some("tools.demo.files.issues")
        );
        assert_eq!(
            hits.first().map(|hit| hit.rank_reason.as_str()),
            Some("exact generated tool path")
        );
    }

    #[test]
    fn search_and_describe_expose_deprecated_status() {
        let mut workspace = workspace();
        let entry = workspace.entries.first_mut().expect("workspace entry");
        entry.deprecated = true;
        entry.support_status = SupportStatus::Deprecated;

        let hits = search_exports(&workspace, "issues", &SearchFilter::default(), 10);

        let hit = hits.first().expect("search hit");
        assert!(hit.deprecated);
        assert_eq!(hit.support_status, SupportStatus::Deprecated);
        let DescribeResolution::Found { entry } =
            describe_export(&workspace, "typescript:demo.files.issues")
        else {
            panic!("expected describe hit");
        };
        assert!(entry.deprecated);
        assert_eq!(entry.support_status, SupportStatus::Deprecated);
    }

    #[test]
    fn describe_untyped_alias_resolves_when_unambiguous() {
        let workspace = workspace();
        let resolution = describe_export(&workspace, "demo.files.issues");
        assert!(matches!(resolution, DescribeResolution::Found { .. }));
    }

    #[test]
    fn describe_generated_tool_path_resolves_when_unambiguous() {
        let workspace = workspace();
        let resolution = describe_export(&workspace, "tools.demo.files.issues");
        assert!(matches!(resolution, DescribeResolution::Found { .. }));
    }

    #[test]
    fn search_page_reports_total_before_pagination() {
        let workspace = workspace();
        let page = search_exports_page(&workspace, "issues", &SearchFilter::default(), 0, 0);
        assert_eq!(page.total, 1);
        assert!(page.items.is_empty());
    }

    #[test]
    fn empty_search_query_returns_no_matches() {
        let workspace = workspace();
        let hits = search_exports(&workspace, "", &SearchFilter::default(), 10);

        assert!(hits.is_empty());
    }

    #[test]
    fn natural_language_search_tokenizes_query_text() {
        let workspace = github_workspace();
        let hits = search_exports(
            &workspace,
            "github pull requests repository review requested open",
            &SearchFilter::default(),
            10,
        );

        assert!(
            hits.iter()
                .any(|hit| hit.capability_id.as_str().contains("operation/pullsList")),
            "expected pullsList in hits: {hits:#?}"
        );
        assert!(
            hits.iter().any(|hit| hit
                .capability_id
                .as_str()
                .contains("operation/searchIssuesAndPullRequests")),
            "expected searchIssuesAndPullRequests in hits: {hits:#?}"
        );
        assert!(
            hits.iter().all(|hit| hit.score > 0),
            "scores should be visible and positive: {hits:#?}"
        );
        assert!(
            hits.iter().any(|hit| !hit.matched_terms.is_empty()),
            "matched terms should explain why search results matched: {hits:#?}"
        );
    }

    fn is_mutation_hit(hit: &super::SearchResult) -> bool {
        hit.effects
            .iter()
            .any(|effect| matches!(effect, EffectKind::Write | EffectKind::Delete))
    }

    /// Asserts the read-vs-write tier: under a read-intent query, EVERY read
    /// hit must rank before EVERY mutation hit, no matter how much content
    /// score the mutation accumulated.
    fn assert_reads_before_mutations(hits: &[super::SearchResult]) {
        let first_mutation = hits.iter().position(is_mutation_hit);
        let last_read = hits.iter().rposition(|hit| !is_mutation_hit(hit));
        if let (Some(first_mutation), Some(last_read)) = (first_mutation, last_read) {
            assert!(
                last_read < first_mutation,
                "every read hit must rank before every mutation hit: {hits:#?}"
            );
        }
    }

    #[test]
    fn broad_pull_request_search_prefers_read_capabilities_over_write_actions() {
        let workspace = github_workspace();
        let hits = search_exports(&workspace, "pull request", &SearchFilter::default(), 10);

        let first = hits.first().expect("first hit");
        assert!(first.effects.contains(&EffectKind::Read));
        assert!(first.matched_fields.contains(&"title".to_string()));
        assert!(
            hits.iter().any(is_mutation_hit),
            "mutations must still be returned for read-intent queries, just last: {hits:#?}"
        );
        assert_reads_before_mutations(&hits);
    }

    #[test]
    fn slack_read_intent_query_ranks_every_read_before_the_mutation() {
        let workspace = slack_workspace();
        let hits = search_exports(&workspace, "slack messages", &SearchFilter::default(), 10);

        assert!(
            hits.iter().any(|hit| hit
                .capability_id
                .as_str()
                .contains("operation/slack_schedule_message")),
            "the schedule mutation must still match: {hits:#?}"
        );
        assert_reads_before_mutations(&hits);
    }

    #[test]
    fn write_intent_query_surfaces_the_mutation_first() {
        // "merge" is a write-intent verb, so every entry sits in tier 0 and the
        // merge mutation wins on content score instead of being tier-buried.
        let workspace = github_workspace();
        let hits = search_exports(
            &workspace,
            "merge pull request",
            &SearchFilter::default(),
            10,
        );

        let first = hits.first().expect("first hit");
        assert!(
            first
                .capability_id
                .as_str()
                .contains("operation/pullsMerge"),
            "write-intent query must surface the merge mutation first: {hits:#?}"
        );
        assert!(is_mutation_hit(first));
    }

    #[test]
    fn exact_ref_for_a_mutation_ranks_first_despite_read_intent() {
        let workspace = github_workspace();
        let reference = workspace
            .entries
            .iter()
            .find(|entry| {
                entry
                    .capability_id
                    .as_str()
                    .contains("operation/reposDeployPages")
            })
            .and_then(|entry| entry.bindings.first())
            .map(|binding| binding.ref_().value.clone())
            .expect("deploy pages typed ref");
        // None of the ref's tokens (typescript, github, rest, repo, deploy,
        // page) are write-intent verbs, so without the exact-match exemption
        // this mutation would be tiered behind every read hit.
        assert!(
            !super::query_tokens(&reference)
                .iter()
                .any(|token| super::is_write_intent(token)),
            "fixture ref must stay free of write-intent tokens: {reference}"
        );

        let hits = search_exports(&workspace, &reference, &SearchFilter::default(), 10);

        let first = hits.first().expect("first hit");
        assert!(
            first
                .capability_id
                .as_str()
                .contains("operation/reposDeployPages"),
            "exact ref must rank the named mutation first: {hits:#?}"
        );
        assert!(is_mutation_hit(first));
        assert_eq!(first.rank_reason, "exact typed ref");
        assert!(
            hits.iter().skip(1).any(|hit| !is_mutation_hit(hit)),
            "read hits matching the ref's tokens should still trail the exact match: {hits:#?}"
        );
    }

    #[test]
    fn explicit_write_intent_overrides_token_detection() {
        // None of the query tokens (deploy, github, page, site, build) is a
        // write-intent verb, so token detection tiers the reposDeployPages
        // mutation behind every read hit that shares the "github" token.
        let workspace = github_workspace();
        let query = "deploy a github pages site build";
        let default_hits = search_exports(&workspace, query, &SearchFilter::default(), 10);
        assert!(
            default_hits.iter().any(|hit| hit
                .capability_id
                .as_str()
                .contains("operation/reposDeployPages")),
            "the deploy mutation must still match: {default_hits:#?}"
        );
        assert!(
            default_hits.iter().any(|hit| !is_mutation_hit(hit)),
            "reads sharing the github token must match so the tier is observable: {default_hits:#?}"
        );
        assert_reads_before_mutations(&default_hits);

        // An explicit write (or any) intent collapses the tiers, so the
        // mutation wins on content score despite the read-intent tokens.
        for intent in [SearchIntent::Write, SearchIntent::Any] {
            let hits = search_exports(
                &workspace,
                query,
                &SearchFilter {
                    intent: Some(intent),
                    ..Default::default()
                },
                10,
            );
            let first = hits.first().expect("first hit");
            assert!(
                first
                    .capability_id
                    .as_str()
                    .contains("operation/reposDeployPages"),
                "intent {intent:?} must lift the mutation demotion: {hits:#?}"
            );
            assert!(is_mutation_hit(first));
        }
    }

    #[test]
    fn explicit_neutral_intents_skip_read_query_boosts() {
        let workspace = github_workspace();
        let read_entry = workspace
            .entries
            .iter()
            .find(|entry| {
                entry.effect_profile.effects.contains(&EffectKind::Read)
                    && entry.effect_profile.capability_kind
                        == coral_capabilities::CapabilityKind::Query
            })
            .expect("read query entry");
        let mut score = super::FieldScore {
            value: 1000,
            ..Default::default()
        };

        for intent in [SearchIntent::Write, SearchIntent::Any] {
            score.value = 1000;
            super::apply_rank_boosts(read_entry, &mut score, Some(intent));
            assert_eq!(score.value, 1000, "intent {intent:?} must be neutral");
        }

        super::apply_rank_boosts(read_entry, &mut score, Some(SearchIntent::Read));
        assert_eq!(score.value, 1150);
    }

    #[test]
    fn explicit_read_intent_demotes_write_verb_queries() {
        // "merge" is a write-intent verb, so token detection would order
        // neutrally; an explicit read intent overrides it and keeps every
        // read ahead of the merge mutation.
        let workspace = github_workspace();
        let hits = search_exports(
            &workspace,
            "merge pull request",
            &SearchFilter {
                intent: Some(SearchIntent::Read),
                ..Default::default()
            },
            10,
        );

        assert!(
            hits.iter()
                .any(|hit| hit.capability_id.as_str().contains("operation/pullsMerge")),
            "intent never filters; the merge mutation must still be returned: {hits:#?}"
        );
        assert_reads_before_mutations(&hits);
    }

    #[test]
    fn write_intent_stems_are_stored_in_stemmed_form() {
        // The natural verb forms an agent would type. `is_write_intent` sees
        // ALREADY-STEMMED query tokens, so the production list must hold
        // exactly the stems of these verbs — not the verbs themselves. `stem`
        // is not idempotent (stem("dismiss") == "dismis", but stem("dismis")
        // == "dismi"), so the invariant is set equality against the stems of
        // this table: a future entry stored unstemmed (e.g. "dismiss") or a
        // stem added without its natural form fails loudly here.
        let natural_forms = [
            "add",
            "approve",
            "archive",
            "assign",
            "cancel",
            "close",
            "create",
            "delete",
            "dismiss",
            "edit",
            "invite",
            "lock",
            "merge",
            "move",
            "mutate",
            "mutation",
            "pin",
            "post",
            "publish",
            "remove",
            "rename",
            "reopen",
            "resolve",
            "schedule",
            "send",
            "set",
            "submit",
            "transfer",
            "unarchive",
            "unassign",
            "unlock",
            "unpin",
            "update",
            "upload",
            "write",
        ];
        let mut expected = natural_forms
            .iter()
            .map(|verb| super::stem(verb))
            .collect::<Vec<_>>();
        expected.sort();
        expected.dedup();
        let mut stored = super::WRITE_INTENT_STEMS
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        stored.sort();
        assert_eq!(
            stored, expected,
            "WRITE_INTENT_STEMS must be exactly the stemmed query forms of the natural verb list"
        );
        for verb in natural_forms {
            assert!(
                super::is_write_intent(&super::stem(verb)),
                "query verb {verb:?} must be detected as write intent after stemming"
            );
        }
    }

    fn slack_workspace() -> crate::WorkspaceExports {
        let source_id = SourceId("src_slack".to_string());
        let ctx = BindingBuildContext {
            source_id: source_id.clone(),
            display_name: "Slack".to_string(),
            source_key: SourceKey("slack".to_string()),
        };
        let set = SourceCapabilitySet::new(source_id.clone(), slack_capabilities(&source_id));
        let exports = build_source_exports(&set, &ctx, &[&TypescriptBindingContributor::new()])
            .expect("source exports");
        compose_workspace_exports("default", &[exports]).expect("workspace exports")
    }

    fn slack_capabilities(source_id: &SourceId) -> Vec<Capability> {
        // The search tool's description mentions "Reaction filters". Under the
        // old scorer the GitHub-flavored `reaction` penalty (-20_000) floored
        // its score to zero and `score_entry` returned `None`, dropping it from
        // every query that lacked the word "reaction" — including "messages" and
        // "search". This corpus is the regression guard for that bug.
        let mut search = github_rest_capability(
            source_id,
            "slack_search_public_and_private",
            "search",
            "Search messages and files",
            "Searches for messages, files in ALL Slack channels, including public and private channels and DMs. Modifiers include has::emoji: Reaction filters and is:thread.",
            EffectProfile::read(),
        );
        search.input_schema = InvocationSchema::new(serde_json::json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query using Slack search syntax."
                },
                "content_types": {
                    "type": "string",
                    "description": "A comma-separated list of messages, files."
                }
            }
        }));
        let read = github_rest_capability(
            source_id,
            "slack_read_channel",
            "read",
            "Read channel messages",
            "Reads messages from a Slack channel in reverse chronological order.",
            EffectProfile::read(),
        );
        let schedule = github_rest_capability(
            source_id,
            "slack_schedule_message",
            "schedule",
            "Schedule message",
            "Schedules a message for future delivery to a Slack channel.",
            EffectProfile::write(),
        );
        vec![search, read, schedule]
    }

    #[test]
    fn term_overlap_capability_is_never_dropped() {
        // P1: a capability whose title contains a query term is always returned
        // with a positive score — never gated away before ranking.
        let workspace = github_workspace();
        let hits = search_exports(&workspace, "authenticated", &SearchFilter::default(), 20);
        assert!(
            hits.iter().any(|hit| hit
                .capability_id
                .as_str()
                .contains("operation/usersGetAuthenticated")),
            "title-term match must never be dropped: {hits:#?}"
        );
        assert!(hits.iter().all(|hit| hit.score > 0));
    }

    #[test]
    fn off_topic_description_token_never_excludes_a_match() {
        // P5 / regression for the reaction-penalty bug: the Slack message-search
        // tool must be returned for "messages", "search", and "search messages".
        let workspace = slack_workspace();
        for query in ["messages", "search", "search messages"] {
            let hits = search_exports(&workspace, query, &SearchFilter::default(), 20);
            let hit = hits
                .iter()
                .find(|hit| {
                    hit.capability_id
                        .as_str()
                        .contains("operation/slack_search_public_and_private")
                })
                .unwrap_or_else(|| {
                    panic!("slack search tool must be returned for {query:?}: {hits:#?}")
                });
            assert!(hit.score > 0, "score must be positive for {query:?}");
        }
    }

    #[test]
    fn parameter_text_is_indexed_for_recall() {
        // Enrichment: "comma-separated" appears only in the `content_types`
        // parameter description, not in the title or top-line description.
        let workspace = slack_workspace();
        let hits = search_exports(&workspace, "comma separated", &SearchFilter::default(), 20);
        assert!(
            hits.iter().any(|hit| hit
                .capability_id
                .as_str()
                .contains("operation/slack_search_public_and_private")),
            "input-parameter descriptions must be searchable: {hits:#?}"
        );
    }

    #[test]
    fn pluralized_query_matches_singular_index_term() {
        // Symmetric stemming: "messages" and "message" resolve to the same tool.
        let workspace = slack_workspace();
        let target = "operation/slack_search_public_and_private";
        for query in ["messages", "message"] {
            let hits = search_exports(&workspace, query, &SearchFilter::default(), 20);
            assert!(
                hits.iter()
                    .any(|hit| hit.capability_id.as_str().contains(target)),
                "query {query:?} should match the message-search tool: {hits:#?}"
            );
        }
    }

    #[test]
    fn every_search_hit_resolves_via_describe() {
        // P4 round-trip: each returned ref must resolve to Found via describe.
        let workspace = github_workspace();
        let hits = search_exports(
            &workspace,
            "pull request review",
            &SearchFilter::default(),
            20,
        );
        assert!(!hits.is_empty());
        for hit in &hits {
            let reference = hit.refs.first().expect("hit exposes a ref");
            assert!(
                matches!(
                    describe_export(&workspace, reference),
                    DescribeResolution::Found { .. }
                ),
                "search hit {reference} must resolve via describe"
            );
        }
    }

    #[test]
    fn search_result_description_is_a_short_preview() {
        let mut workspace = github_workspace();
        let long_description = format!(
            "{}\n\n{}",
            "Search GitHub issues and pull requests. ".repeat(30),
            "Second paragraph should stay out of the search item."
        );
        let entry = workspace
            .entries
            .iter_mut()
            .find(|entry| {
                entry
                    .capability_id
                    .as_str()
                    .contains("operation/searchIssuesAndPullRequests")
            })
            .expect("search capability");
        entry.description = long_description.clone();

        let hits = search_exports(
            &workspace,
            "search issues pull requests",
            &SearchFilter::default(),
            10,
        );
        let first = hits.first().expect("first hit");

        assert!(first.description.len() < long_description.len());
        assert!(first.description.ends_with(" ..."));
        assert!(!first.description.contains("Second paragraph"));
        assert!(first.description.chars().count() <= SEARCH_DESCRIPTION_PREVIEW_CHARS + 4);
    }
}
