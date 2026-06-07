//! Search and describe helpers over workspace exports.

use std::collections::BTreeSet;

use coral_capabilities::{CapabilityId, CapabilityKind, EffectKind, SourceId, SupportStatus};
use serde::{Deserialize, Serialize};

use crate::exports::{Binding, CapabilityExport, ExportKind, WorkspaceExports};

const SEARCH_DESCRIPTION_PREVIEW_CHARS: usize = 320;

/// Search filter.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchFilter {
    pub source_id: Option<SourceId>,
    pub source_key: Option<String>,
    pub display_name: Option<String>,
    pub kind: Option<ExportKind>,
    pub allowed_kinds: Vec<ExportKind>,
    pub capability_kind: Option<CapabilityKind>,
    pub effect: Option<EffectKind>,
}

/// Compact search result item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchResult {
    pub alias: Option<String>,
    pub full_path: Option<String>,
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
    let mut scored = exports
        .entries
        .iter()
        .filter(|entry| matches_filter(entry, filter))
        .filter_map(|entry| {
            if query.is_empty() {
                filter_has_active_constraints(filter).then_some((empty_query_score(), entry))
            } else {
                score_entry(entry, query).map(|score| (score, entry))
            }
        })
        .collect::<Vec<_>>();
    scored.sort_by(|(left_score, left), (right_score, right)| {
        right_score
            .value
            .cmp(&left_score.value)
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

fn empty_query_score() -> SearchScore {
    SearchScore {
        value: 0,
        matched_fields: Vec::new(),
        rank_reason: "matched active filters".to_string(),
    }
}

fn filter_has_active_constraints(filter: &SearchFilter) -> bool {
    filter.source_id.is_some()
        || filter.source_key.is_some()
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
        .source_id
        .as_ref()
        .is_some_and(|source_id| source_id != &entry.source_id)
    {
        return false;
    }
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
    matched_fields: Vec<String>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RankIntent {
    CountSearch,
    ReviewSearch,
    PullRequestSearch,
    AuthenticatedUserLookup,
}

fn score_entry(entry: &CapabilityExport, query: &str) -> Option<SearchScore> {
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
    let raw_query_tokens = raw_tokens(query);
    let has_count_search_intent = raw_query_tokens
        .iter()
        .any(|token| is_count_search_intent(token));
    let has_explicit_search_intent = raw_query_tokens
        .iter()
        .any(|token| is_explicit_search_intent(token));
    let has_review_search_intent = has_review_search_intent(&raw_query_tokens, &query_tokens);
    let has_pull_request_search_intent = query_tokens.contains("pull")
        && query_tokens.contains("request")
        && (has_count_search_intent || has_explicit_search_intent || has_review_search_intent);
    let mut intents = BTreeSet::new();
    if has_count_search_intent {
        intents.insert(RankIntent::CountSearch);
    }
    if has_review_search_intent {
        intents.insert(RankIntent::ReviewSearch);
    }
    if has_pull_request_search_intent {
        intents.insert(RankIntent::PullRequestSearch);
    }
    if has_authenticated_user_lookup_intent(&raw_query_tokens, &query_tokens) {
        intents.insert(RankIntent::AuthenticatedUserLookup);
    }
    let mut score = score_fields(entry, query, &query_tokens);
    if !has_required_token_coverage(&score, &query_tokens) {
        return None;
    }

    apply_rank_boosts(entry, &query_tokens, &intents, &mut score);
    if score.value == 0 {
        return None;
    }
    Some(search_score(score, query_tokens.len()))
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
) -> FieldScore {
    let query_phrase = normalized_phrase(query);
    let mut score = FieldScore::default();
    for field in search_fields(entry) {
        score_field(&mut score, &field, &query_phrase, query_tokens);
    }
    score
}

fn score_field(
    score: &mut FieldScore,
    field: &SearchField,
    query_phrase: &str,
    query_tokens: &BTreeSet<String>,
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
    score_token_matches(score, field, query_tokens);
}

fn score_token_matches(
    score: &mut FieldScore,
    field: &SearchField,
    query_tokens: &BTreeSet<String>,
) {
    let field_tokens = token_set(&field.text);
    let mut field_matches = 0_u32;
    for token in query_tokens {
        if field_tokens.contains(token) {
            field_matches = field_matches.saturating_add(1);
            score.matched_tokens.insert(token.clone());
        }
    }
    if field_matches > 0 {
        score.value = score.value.saturating_add(
            field_matches
                .saturating_mul(field.weight)
                .saturating_mul(10),
        );
        score.matched_fields.insert(field.name.to_string());
    }
}

fn has_required_token_coverage(score: &FieldScore, query_tokens: &BTreeSet<String>) -> bool {
    if score.matched_tokens.is_empty() {
        return false;
    }
    if score.phrase_matched {
        return true;
    }
    if query_tokens.len() <= 2 {
        return score.matched_tokens.len() == query_tokens.len();
    }
    score.matched_tokens.len().saturating_mul(10) >= query_tokens.len().saturating_mul(3)
}

fn apply_rank_boosts(
    entry: &CapabilityExport,
    query_tokens: &BTreeSet<String>,
    intents: &BTreeSet<RankIntent>,
    score: &mut FieldScore,
) {
    if entry.effect_profile.effects.contains(&EffectKind::Read) {
        score.value = score.value.saturating_add(100);
    }
    if entry
        .effect_profile
        .effects
        .iter()
        .any(|effect| matches!(effect, EffectKind::Write | EffectKind::Delete))
        && !query_tokens.iter().any(|token| is_write_intent(token))
    {
        score.value = score.value.saturating_sub(250);
    }
    if entry.effect_profile.capability_kind == CapabilityKind::Query {
        score.value = score.value.saturating_add(50);
    }
    if intents.contains(&RankIntent::CountSearch) && entry_mentions_search(entry) {
        score.value = score.value.saturating_add(1_000);
        score.matched_fields.insert("search_intent".to_string());
    }
    if intents.contains(&RankIntent::ReviewSearch) && entry_mentions_search(entry) {
        score.value = score.value.saturating_add(12_000);
        score
            .matched_fields
            .insert("review_search_intent".to_string());
    }
    if intents.contains(&RankIntent::PullRequestSearch) && entry_mentions_pull_request_search(entry)
    {
        score.value = score.value.saturating_add(20_000);
        score
            .matched_fields
            .insert("pull_request_search_intent".to_string());
    }
    if intents.contains(&RankIntent::AuthenticatedUserLookup)
        && entry_mentions_authenticated_user_lookup(entry)
    {
        score.value = score.value.saturating_add(16_000);
        score
            .matched_fields
            .insert("authenticated_user_lookup_intent".to_string());
    }
    if !intents.contains(&RankIntent::ReviewSearch)
        && !intents.contains(&RankIntent::PullRequestSearch)
        && query_tokens.contains("pull")
        && query_tokens.contains("request")
        && query_tokens.contains("review")
        && entry_mentions_direct_review_listing(entry)
    {
        score.value = score.value.saturating_add(8_000);
        score
            .matched_fields
            .insert("direct_review_intent".to_string());
    }
    apply_unrequested_specialization_penalty(entry, query_tokens, score);
}

fn search_score(score: FieldScore, query_token_count: usize) -> SearchScore {
    let matched_token_count = score.matched_tokens.len();
    let mut matched_fields = score.matched_fields.into_iter().collect::<Vec<_>>();
    matched_fields.sort();
    SearchScore {
        value: score.value,
        rank_reason: format!(
            "matched {} of {} token(s) in {}",
            matched_token_count,
            query_token_count,
            matched_fields.join(", ")
        ),
        matched_fields,
    }
}

fn exact_score(value: u32, field: &str, reason: &str) -> SearchScore {
    SearchScore {
        value,
        matched_fields: vec![field.to_string()],
        rank_reason: reason.to_string(),
    }
}

fn search_fields(entry: &CapabilityExport) -> Vec<SearchField> {
    let mut fields = vec![
        SearchField {
            name: "source_key",
            text: entry.source_key.as_str().to_string(),
            weight: 8,
        },
        SearchField {
            name: "display_name",
            text: entry.display_name.clone(),
            weight: 8,
        },
        SearchField {
            name: "interface_id",
            text: entry.interface_id.clone(),
            weight: 8,
        },
        SearchField {
            name: "operation_id",
            text: entry.operation_id.clone(),
            weight: 10,
        },
        SearchField {
            name: "title",
            text: entry.title.clone(),
            weight: 10,
        },
        SearchField {
            name: "description",
            text: entry.description.clone(),
            weight: 5,
        },
    ];
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
                        name: "typescript_full_path",
                        text: format!("tools.{}", binding.path.join(".")),
                        weight: 12,
                    });
                }
                fields.push(SearchField {
                    name: "typescript_path",
                    text: binding.ref_.value.clone(),
                    weight: 12,
                });
                for segment in &binding.path {
                    fields.push(SearchField {
                        name: "typescript_path",
                        text: segment.clone(),
                        weight: 12,
                    });
                }
            }
            Binding::Sql(binding) => {
                fields.push(SearchField {
                    name: "sql_reference",
                    text: binding.ref_.value.clone(),
                    weight: 12,
                });
                fields.push(SearchField {
                    name: "sql_reference",
                    text: binding.sql_reference.clone(),
                    weight: 12,
                });
            }
        }
    }
    fields
}

fn query_tokens(query: &str) -> BTreeSet<String> {
    raw_tokens(query)
        .into_iter()
        .filter(|token| !is_query_stopword(token))
        .flat_map(|token| query_token_expansions(&token))
        .collect()
}

fn token_set(text: &str) -> BTreeSet<String> {
    raw_tokens(text)
        .into_iter()
        .flat_map(|token| token_forms(&token))
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

fn query_token_expansions(token: &str) -> Vec<String> {
    match token {
        "pr" | "prs" => vec!["pull".to_string(), "request".to_string()],
        "repo" => vec!["repo".to_string(), "repository".to_string()],
        _ => token_forms(token),
    }
}

fn token_forms(token: &str) -> Vec<String> {
    let mut forms = vec![token.to_string()];
    if token.len() > 3 && token.ends_with('s') {
        forms.push(token.trim_end_matches('s').to_string());
    }
    if matches!(token, "reviewed" | "reviewing" | "reviews") {
        forms.push("review".to_string());
    }
    if token == "pulls" {
        forms.push("pull".to_string());
    }
    forms
}

fn is_query_stopword(token: &str) -> bool {
    matches!(
        token,
        "a" | "an"
            | "and"
            | "coral"
            | "count"
            | "did"
            | "find"
            | "for"
            | "get"
            | "how"
            | "i"
            | "in"
            | "last"
            | "many"
            | "my"
            | "of"
            | "open"
            | "or"
            | "show"
            | "the"
            | "to"
            | "use"
            | "using"
            | "when"
    )
}

fn is_write_intent(token: &str) -> bool {
    matches!(
        token,
        "approve" | "create" | "delete" | "mutate" | "mutation" | "remove" | "update" | "write"
    )
}

fn is_count_search_intent(token: &str) -> bool {
    matches!(
        token,
        "count" | "find" | "how" | "many" | "number" | "total"
    )
}

fn is_explicit_search_intent(token: &str) -> bool {
    matches!(token, "search" | "searches" | "searching")
}

fn has_review_search_intent(raw_tokens: &[String], query_tokens: &BTreeSet<String>) -> bool {
    let mentions_plural_prs = raw_tokens
        .iter()
        .any(|token| matches!(token.as_str(), "pr" | "prs" | "requests"));
    mentions_plural_prs
        && query_tokens.contains("pull")
        && query_tokens.contains("request")
        && query_tokens.contains("review")
}

fn has_authenticated_user_lookup_intent(
    raw_tokens: &[String],
    query_tokens: &BTreeSet<String>,
) -> bool {
    let lookup_word = raw_tokens.iter().any(|token| {
        matches!(
            token.as_str(),
            "username" | "login" | "me" | "my" | "current"
        )
    });
    lookup_word && query_tokens.contains("authenticated") && query_tokens.contains("user")
}

fn entry_mentions_search(entry: &CapabilityExport) -> bool {
    [
        entry.operation_id.as_str(),
        entry.title.as_str(),
        entry.description.as_str(),
    ]
    .into_iter()
    .chain(entry.search_text.iter().map(String::as_str))
    .flat_map(raw_tokens)
    .any(|token| token == "search")
}

fn entry_mentions_direct_review_listing(entry: &CapabilityExport) -> bool {
    let tokens = entry_ranking_tokens(entry);
    tokens.contains("pull")
        && tokens.contains("request")
        && tokens.contains("review")
        && tokens.contains("list")
        && !tokens.contains("comment")
        && !tokens.contains("reaction")
        && !tokens.contains("protection")
}

fn entry_mentions_pull_request_search(entry: &CapabilityExport) -> bool {
    let tokens = entry_identity_tokens(entry);
    tokens.contains("search")
        && tokens.contains("issue")
        && tokens.contains("pull")
        && tokens.contains("request")
}

fn entry_mentions_authenticated_user_lookup(entry: &CapabilityExport) -> bool {
    normalized_phrase(&entry.title) == "get the authenticated user"
        || raw_tokens(&entry.operation_id)
            == [
                "users".to_string(),
                "get".to_string(),
                "authenticated".to_string(),
            ]
}

fn entry_identity_tokens(entry: &CapabilityExport) -> BTreeSet<String> {
    [entry.operation_id.as_str(), entry.title.as_str()]
        .into_iter()
        .flat_map(token_set)
        .collect()
}

fn apply_unrequested_specialization_penalty(
    entry: &CapabilityExport,
    query_tokens: &BTreeSet<String>,
    score: &mut FieldScore,
) {
    let entry_tokens = entry_ranking_tokens(entry);
    for (token, penalty) in [
        ("code", 15_000),
        ("comment", 20_000),
        ("protection", 25_000),
        ("reaction", 20_000),
        ("workflow", 15_000),
    ] {
        if entry_tokens.contains(token) && !query_tokens.contains(token) {
            score.value = score.value.saturating_sub(penalty);
        }
    }
}

fn entry_ranking_tokens(entry: &CapabilityExport) -> BTreeSet<String> {
    [
        entry.operation_id.as_str(),
        entry.title.as_str(),
        entry.description.as_str(),
    ]
    .into_iter()
    .flat_map(token_set)
    .collect()
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
        ProviderOrigin, ProviderOriginKind, SourceCapabilitySet, SourceId, SupportStatus,
        UpstreamBinding,
    };

    use crate::contributors::TypescriptBindingContributor;
    use crate::exports::{BindingBuildContext, build_source_exports, compose_workspace_exports};
    use crate::package::SourceKey;

    use super::{
        DescribeResolution, SEARCH_DESCRIPTION_PREVIEW_CHARS, SearchFilter, describe_export,
        search_exports, search_exports_page,
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
    }

    #[test]
    fn broad_pull_request_search_prefers_read_capabilities_over_write_actions() {
        let workspace = github_workspace();
        let hits = search_exports(&workspace, "pull request", &SearchFilter::default(), 10);

        let first = hits.first().expect("first hit");
        assert!(
            !first
                .capability_id
                .as_str()
                .contains("operation/actionsApproveWorkflowRun")
        );
        assert!(first.effects.contains(&EffectKind::Read));
        assert!(first.matched_fields.contains(&"title".to_string()));
    }

    #[test]
    fn review_count_query_prefers_search_over_review_comment_helpers() {
        let workspace = github_workspace();
        let hits = search_exports(
            &workspace,
            "use coral to find how many PRs to review in withcoral/coral",
            &SearchFilter::default(),
            10,
        );

        let first = hits.first().expect("first hit");
        assert!(
            first
                .capability_id
                .as_str()
                .contains("operation/searchIssuesAndPullRequests"),
            "expected GitHub search as first hit: {hits:#?}"
        );
        assert!(first.matched_fields.contains(&"search_intent".to_string()));
    }

    #[test]
    fn pull_request_review_search_prefers_direct_review_listing() {
        let workspace = github_workspace();
        let hits = search_exports(
            &workspace,
            "pull request review",
            &SearchFilter::default(),
            10,
        );

        let first = hits.first().expect("first hit");
        assert!(
            first
                .capability_id
                .as_str()
                .contains("operation/pullsListReviews"),
            "expected direct review listing as first hit: {hits:#?}"
        );
        assert!(
            hits.iter()
                .take(3)
                .all(|hit| !hit.capability_id.as_str().contains("ReviewComment")),
            "review-comment helpers should not dominate broad review search: {hits:#?}"
        );
        assert!(
            first
                .matched_fields
                .contains(&"direct_review_intent".to_string())
        );
    }

    #[test]
    fn plural_pr_review_search_prefers_search_endpoint() {
        let workspace = github_workspace();
        let hits = search_exports(
            &workspace,
            "github PR review pull requests",
            &SearchFilter::default(),
            10,
        );

        let first = hits.first().expect("first hit");
        assert!(
            first
                .capability_id
                .as_str()
                .contains("operation/searchIssuesAndPullRequests"),
            "expected GitHub search as first hit: {hits:#?}"
        );
        assert!(
            first
                .matched_fields
                .contains(&"review_search_intent".to_string())
        );
        assert!(
            first
                .matched_fields
                .contains(&"pull_request_search_intent".to_string())
        );
    }

    #[test]
    fn explicit_pr_review_search_prefers_issue_pr_search_over_code_search() {
        let workspace = github_workspace();
        let hits = search_exports(
            &workspace,
            "github pull request review search",
            &SearchFilter::default(),
            10,
        );

        let first = hits.first().expect("first hit");
        assert!(
            first
                .capability_id
                .as_str()
                .contains("operation/searchIssuesAndPullRequests"),
            "expected GitHub issue/PR search as first hit: {hits:#?}"
        );
        assert!(
            !hits
                .iter()
                .take(3)
                .any(|hit| hit.capability_id.as_str().contains("operation/searchCode")),
            "code search should not dominate PR review search: {hits:#?}"
        );
    }

    #[test]
    fn review_required_pull_requests_prefers_issue_pr_search_over_code_search() {
        let workspace = github_workspace();
        let hits = search_exports(
            &workspace,
            "review required pull requests",
            &SearchFilter::default(),
            10,
        );

        let first = hits.first().expect("first hit");
        assert!(
            first
                .capability_id
                .as_str()
                .contains("operation/searchIssuesAndPullRequests"),
            "expected GitHub issue/PR search as first hit: {hits:#?}"
        );
    }

    #[test]
    fn last_reviewed_pr_query_prefers_issue_pr_search() {
        let workspace = github_workspace();
        let hits = search_exports(
            &workspace,
            "when did I last review a PR",
            &SearchFilter::default(),
            10,
        );

        let first = hits.first().expect("first hit");
        assert!(
            first
                .capability_id
                .as_str()
                .contains("operation/searchIssuesAndPullRequests"),
            "expected GitHub issue/PR search as first hit: {hits:#?}"
        );
        assert!(
            first
                .matched_fields
                .contains(&"pull_request_search_intent".to_string())
        );
    }

    #[test]
    fn authenticated_user_username_query_prefers_get_authenticated_user() {
        let workspace = github_workspace();
        let hits = search_exports(
            &workspace,
            "authenticated user username github",
            &SearchFilter::default(),
            10,
        );

        let first = hits.first().expect("first hit");
        assert!(
            first
                .capability_id
                .as_str()
                .contains("operation/usersGetAuthenticated"),
            "expected get authenticated user first: {hits:#?}"
        );
        assert!(
            first
                .matched_fields
                .contains(&"authenticated_user_lookup_intent".to_string())
        );
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
