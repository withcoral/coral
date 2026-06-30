//! Catalog provider relevance ranking.

use std::cmp::Reverse;

use crate::search::catalog::sqlite_index::{CatalogIndexDocumentKind, CatalogSearchHit};

const PARENT_CATALOG_SURFACE_RELEVANCE_BOOST: u32 = 20_000;
const COLUMN_HINT_RELEVANCE_BOOST: u32 = 0;
const EXACT_QUALIFIED_NAME_TERM_RELEVANCE_BOOST: u32 = 12_000;
const EXACT_SOURCE_NAME_TERM_RELEVANCE_BOOST: u32 = 9_000;
const EXACT_TITLE_SURFACE_OR_FIELD_TERM_RELEVANCE_BOOST: u32 = 8_000;
const SOURCE_IDENTIFIER_TOKEN_TERM_RELEVANCE_BOOST: u32 = 7_000;
const TITLE_SURFACE_OR_FIELD_IDENTIFIER_TOKEN_TERM_RELEVANCE_BOOST: u32 = 6_000;
const QUALIFIED_NAME_IDENTIFIER_TOKEN_TERM_RELEVANCE_BOOST: u32 = 4_500;
const QUALIFIED_NAME_PREFIX_TERM_RELEVANCE_BOOST: u32 = 3_500;
const SOURCE_NAME_PREFIX_TERM_RELEVANCE_BOOST: u32 = 3_000;
const TITLE_SURFACE_FIELD_OR_QUALIFIED_NAME_IDENTIFIER_PREFIX_TERM_RELEVANCE_BOOST: u32 = 3_000;
const QUALIFIED_NAME_CONTAINS_TERM_RELEVANCE_BOOST: u32 = 1_500;
const SEARCHABLE_TEXT_CONTAINS_TERM_RELEVANCE_BOOST: u32 = 800;
const DESCRIPTION_CONTAINS_TERM_RELEVANCE_BOOST: u32 = 600;
const ALL_QUERY_TERMS_MATCHED_RELEVANCE_BOOST: u32 = 12_000;
const PARENT_CATALOG_SURFACE_RANK_ORDER: u8 = 0;
const COLUMN_HINT_RANK_ORDER: u8 = 1;

#[derive(Debug, Clone)]
pub(crate) struct RankedCatalogHit {
    pub(crate) hit: CatalogSearchHit,
    pub(crate) score: u32,
}

pub(crate) fn rank_catalog_hits(
    hits: Vec<CatalogSearchHit>,
    terms: &[String],
) -> Vec<RankedCatalogHit> {
    let mut ranked = hits
        .into_iter()
        .map(|hit| {
            let score = hit
                .retrieval_score
                .saturating_add(catalog_relevance_score(&hit, terms));
            RankedCatalogHit { hit, score }
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        (
            Reverse(left.score),
            doc_kind_order(left.hit.doc_kind),
            left.hit.doc_id.as_str(),
        )
            .cmp(&(
                Reverse(right.score),
                doc_kind_order(right.hit.doc_kind),
                right.hit.doc_id.as_str(),
            ))
    });
    ranked
}

fn catalog_relevance_score(hit: &CatalogSearchHit, terms: &[String]) -> u32 {
    let source_name = hit.source_name.to_ascii_lowercase();
    let surface_name = hit.surface_name.to_ascii_lowercase();
    let field_name = hit.field_name.to_ascii_lowercase();
    let title = if field_name.is_empty() {
        surface_name.as_str()
    } else {
        field_name.as_str()
    };
    let qualified_name = qualified_name(hit);
    let description = hit.description.to_ascii_lowercase();
    let searchable_text = searchable_text(hit, &description);
    let mut score = doc_kind_boost(hit.doc_kind);
    let required_term_count = terms
        .iter()
        .filter(|term| is_required_query_term(term))
        .count();
    let mut matched_required_terms = 0_usize;

    for term in terms {
        let mut term_score = 0_u32;
        if qualified_name == *term {
            term_score = term_score.saturating_add(EXACT_QUALIFIED_NAME_TERM_RELEVANCE_BOOST);
        }
        if source_name == *term {
            term_score = term_score.saturating_add(EXACT_SOURCE_NAME_TERM_RELEVANCE_BOOST);
        }
        if title == term || surface_name == *term || field_name == *term {
            term_score =
                term_score.saturating_add(EXACT_TITLE_SURFACE_OR_FIELD_TERM_RELEVANCE_BOOST);
        }
        if identifier_token_matches(&source_name, term) {
            term_score = term_score.saturating_add(SOURCE_IDENTIFIER_TOKEN_TERM_RELEVANCE_BOOST);
        }
        if identifier_token_matches(&surface_name, term)
            || identifier_token_matches(&field_name, term)
            || identifier_token_matches(title, term)
        {
            term_score = term_score
                .saturating_add(TITLE_SURFACE_OR_FIELD_IDENTIFIER_TOKEN_TERM_RELEVANCE_BOOST);
        }
        if identifier_token_matches(&qualified_name, term) {
            term_score =
                term_score.saturating_add(QUALIFIED_NAME_IDENTIFIER_TOKEN_TERM_RELEVANCE_BOOST);
        }
        if qualified_name.starts_with(term) {
            term_score = term_score.saturating_add(QUALIFIED_NAME_PREFIX_TERM_RELEVANCE_BOOST);
        }
        if source_name.starts_with(term) {
            term_score = term_score.saturating_add(SOURCE_NAME_PREFIX_TERM_RELEVANCE_BOOST);
        }
        if identifier_token_starts_with(&surface_name, term)
            || identifier_token_starts_with(&field_name, term)
            || identifier_token_starts_with(title, term)
            || identifier_token_starts_with(&qualified_name, term)
        {
            term_score = term_score.saturating_add(
                TITLE_SURFACE_FIELD_OR_QUALIFIED_NAME_IDENTIFIER_PREFIX_TERM_RELEVANCE_BOOST,
            );
        }
        if qualified_name.contains(term) {
            term_score = term_score.saturating_add(QUALIFIED_NAME_CONTAINS_TERM_RELEVANCE_BOOST);
        }
        if searchable_text.contains(term) {
            term_score = term_score.saturating_add(SEARCHABLE_TEXT_CONTAINS_TERM_RELEVANCE_BOOST);
        }
        if description.contains(term) {
            term_score = term_score.saturating_add(DESCRIPTION_CONTAINS_TERM_RELEVANCE_BOOST);
        }
        if term_score > 0 {
            if is_required_query_term(term) {
                matched_required_terms += 1;
            }
            score = score.saturating_add(term_score);
        }
    }

    if required_term_count > 1 && matched_required_terms == required_term_count {
        score = score.saturating_add(ALL_QUERY_TERMS_MATCHED_RELEVANCE_BOOST);
    }

    score
}

fn qualified_name(hit: &CatalogSearchHit) -> String {
    if hit.field_name.is_empty() {
        format!("{}.{}", hit.source_name, hit.surface_name).to_ascii_lowercase()
    } else {
        format!(
            "{}.{}.{}",
            hit.source_name, hit.surface_name, hit.field_name
        )
        .to_ascii_lowercase()
    }
}

fn searchable_text(hit: &CatalogSearchHit, description: &str) -> String {
    format!(
        "{} {} {} {} {}",
        hit.source_name, hit.surface_name, hit.field_name, hit.surface_kind, description
    )
    .to_ascii_lowercase()
}

fn identifier_token_matches(value: &str, term: &str) -> bool {
    let compact_term = compact_identifier(term);
    !compact_term.is_empty()
        && (value == term
            || compact_identifier(value) == compact_term
            || identifier_tokens(value)
                .any(|token| token == term || compact_identifier(token) == compact_term))
}

fn identifier_token_starts_with(value: &str, term: &str) -> bool {
    let compact_term = compact_identifier(term);
    !compact_term.is_empty()
        && (value.starts_with(term)
            || compact_identifier(value).starts_with(&compact_term)
            || identifier_tokens(value).any(|token| {
                token.starts_with(term) || compact_identifier(token).starts_with(&compact_term)
            }))
}

fn identifier_tokens(value: &str) -> impl Iterator<Item = &str> {
    value
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
}

fn compact_identifier(value: &str) -> String {
    value.chars().filter(char::is_ascii_alphanumeric).collect()
}

fn is_required_query_term(term: &str) -> bool {
    !term.chars().any(char::is_whitespace)
}

fn doc_kind_boost(kind: CatalogIndexDocumentKind) -> u32 {
    match kind {
        CatalogIndexDocumentKind::CatalogTable | CatalogIndexDocumentKind::CatalogTableFunction => {
            PARENT_CATALOG_SURFACE_RELEVANCE_BOOST
        }
        CatalogIndexDocumentKind::ColumnHint => COLUMN_HINT_RELEVANCE_BOOST,
    }
}

fn doc_kind_order(kind: CatalogIndexDocumentKind) -> u8 {
    match kind {
        CatalogIndexDocumentKind::CatalogTable | CatalogIndexDocumentKind::CatalogTableFunction => {
            PARENT_CATALOG_SURFACE_RANK_ORDER
        }
        CatalogIndexDocumentKind::ColumnHint => COLUMN_HINT_RANK_ORDER,
    }
}

#[cfg(test)]
mod tests {
    use super::rank_catalog_hits;
    use crate::search::catalog::sqlite_index::{CatalogIndexDocumentKind, CatalogSearchHit};

    const EQUAL_RETRIEVAL_SCORE_FIXTURE: u32 = 5_000;

    #[test]
    fn multi_term_source_intent_beats_generic_exact_surface() {
        let hits = rank_catalog_hits(
            vec![
                hit(HitInput {
                    doc_id: "catalog:table:notion.search",
                    doc_kind: CatalogIndexDocumentKind::CatalogTable,
                    source_name: "notion",
                    surface_kind: "table",
                    surface_name: "search",
                    field_name: "",
                    field_role: "",
                    description: "Pages and data sources shared with the integration",
                    matched_fields: vec!["qualified_name", "title"],
                    retrieval_score: EQUAL_RETRIEVAL_SCORE_FIXTURE,
                }),
                hit(HitInput {
                    doc_id: "catalog:function:github.search_code",
                    doc_kind: CatalogIndexDocumentKind::CatalogTableFunction,
                    source_name: "github",
                    surface_kind: "table_function",
                    surface_name: "search_code",
                    field_name: "",
                    field_role: "",
                    description: "Search GitHub code.",
                    matched_fields: vec!["description", "qualified_name", "title"],
                    retrieval_score: EQUAL_RETRIEVAL_SCORE_FIXTURE,
                }),
                hit(HitInput {
                    doc_id: "argument:function:github.search_code:q",
                    doc_kind: CatalogIndexDocumentKind::ColumnHint,
                    source_name: "github",
                    surface_kind: "table_function",
                    surface_name: "search_code",
                    field_name: "q",
                    field_role: "table_function_argument",
                    description: "Table function argument",
                    matched_fields: vec!["qualified_name"],
                    retrieval_score: EQUAL_RETRIEVAL_SCORE_FIXTURE,
                }),
            ],
            &["github".to_string(), "search".to_string()],
        );

        assert_eq!(
            hits.first().expect("top ranked hit").hit.doc_id,
            "catalog:function:github.search_code"
        );
    }

    #[test]
    fn multi_term_identifier_prefix_intent_beats_same_source_noise() {
        let hits = rank_catalog_hits(
            vec![
                hit(HitInput {
                    doc_id: "catalog:function:datadog.logs",
                    doc_kind: CatalogIndexDocumentKind::CatalogTableFunction,
                    source_name: "datadog",
                    surface_kind: "table_function",
                    surface_name: "logs",
                    field_name: "",
                    field_role: "",
                    description: "Datadog logs within a time range.",
                    matched_fields: vec!["description", "qualified_name"],
                    retrieval_score: EQUAL_RETRIEVAL_SCORE_FIXTURE,
                }),
                hit(HitInput {
                    doc_id: "catalog:function:datadog.search_monitors",
                    doc_kind: CatalogIndexDocumentKind::CatalogTableFunction,
                    source_name: "datadog",
                    surface_kind: "table_function",
                    surface_name: "search_monitors",
                    field_name: "",
                    field_role: "",
                    description: "Search Datadog monitors.",
                    matched_fields: vec!["description", "qualified_name", "title"],
                    retrieval_score: EQUAL_RETRIEVAL_SCORE_FIXTURE,
                }),
                hit(HitInput {
                    doc_id: "argument:function:datadog.search_monitors:query",
                    doc_kind: CatalogIndexDocumentKind::ColumnHint,
                    source_name: "datadog",
                    surface_kind: "table_function",
                    surface_name: "search_monitors",
                    field_name: "query",
                    field_role: "table_function_argument",
                    description: "Table function argument",
                    matched_fields: vec!["qualified_name"],
                    retrieval_score: EQUAL_RETRIEVAL_SCORE_FIXTURE,
                }),
            ],
            &["datadog".to_string(), "monitor".to_string()],
        );

        assert_eq!(
            hits.first().expect("top ranked hit").hit.doc_id,
            "catalog:function:datadog.search_monitors"
        );
    }

    #[test]
    fn exact_field_match_beats_prefix_match() {
        let hits = rank_catalog_hits(
            vec![
                hit(HitInput {
                    doc_id: "a-prefix-identifier",
                    doc_kind: CatalogIndexDocumentKind::ColumnHint,
                    source_name: "fixture",
                    surface_kind: "table",
                    surface_name: "users",
                    field_name: "identity",
                    field_role: "table_column",
                    description: "",
                    matched_fields: vec!["field_name", "qualified_name"],
                    retrieval_score: EQUAL_RETRIEVAL_SCORE_FIXTURE,
                }),
                hit(HitInput {
                    doc_id: "z-exact-id",
                    doc_kind: CatalogIndexDocumentKind::ColumnHint,
                    source_name: "fixture",
                    surface_kind: "table",
                    surface_name: "users",
                    field_name: "id",
                    field_role: "table_column",
                    description: "",
                    matched_fields: vec!["field_name", "qualified_name"],
                    retrieval_score: EQUAL_RETRIEVAL_SCORE_FIXTURE,
                }),
            ],
            &["id".to_string()],
        );

        assert_eq!(
            hits.first().expect("top ranked hit").hit.doc_id,
            "z-exact-id"
        );
    }

    #[test]
    fn full_query_phrase_does_not_block_all_token_match_boost() {
        let hits = rank_catalog_hits(
            vec![
                hit(HitInput {
                    doc_id: "catalog:table:github.issues",
                    doc_kind: CatalogIndexDocumentKind::CatalogTable,
                    source_name: "github",
                    surface_kind: "table",
                    surface_name: "issues",
                    field_name: "",
                    field_role: "",
                    description: "Bug database tracker",
                    matched_fields: vec!["description", "qualified_name"],
                    retrieval_score: EQUAL_RETRIEVAL_SCORE_FIXTURE,
                }),
                hit(HitInput {
                    doc_id: "catalog:table:github.issue_templates",
                    doc_kind: CatalogIndexDocumentKind::CatalogTable,
                    source_name: "github",
                    surface_kind: "table",
                    surface_name: "issue_templates",
                    field_name: "",
                    field_role: "",
                    description: "Template archive",
                    matched_fields: vec!["qualified_name"],
                    retrieval_score: EQUAL_RETRIEVAL_SCORE_FIXTURE,
                }),
            ],
            &[
                "issue".to_string(),
                "issue tracker".to_string(),
                "tracker".to_string(),
            ],
        );

        assert_eq!(
            hits.first().expect("top ranked hit").hit.doc_id,
            "catalog:table:github.issues"
        );
    }

    #[test]
    fn preserved_identifier_punctuation_matches_catalog_identifiers() {
        let hits = rank_catalog_hits(
            vec![
                hit(HitInput {
                    doc_id: "catalog:table:stripe.payments_api",
                    doc_kind: CatalogIndexDocumentKind::CatalogTable,
                    source_name: "stripe",
                    surface_kind: "table",
                    surface_name: "payments_api",
                    field_name: "",
                    field_role: "",
                    description: "",
                    matched_fields: vec!["qualified_name"],
                    retrieval_score: EQUAL_RETRIEVAL_SCORE_FIXTURE,
                }),
                hit(HitInput {
                    doc_id: "catalog:table:stripe.payments",
                    doc_kind: CatalogIndexDocumentKind::CatalogTable,
                    source_name: "stripe",
                    surface_kind: "table",
                    surface_name: "payments",
                    field_name: "",
                    field_role: "",
                    description: "",
                    matched_fields: vec!["qualified_name"],
                    retrieval_score: EQUAL_RETRIEVAL_SCORE_FIXTURE,
                }),
            ],
            &["payments-api".to_string()],
        );

        assert_eq!(
            hits.first().expect("top ranked hit").hit.doc_id,
            "catalog:table:stripe.payments_api"
        );
    }

    struct HitInput<'a> {
        doc_id: &'a str,
        doc_kind: CatalogIndexDocumentKind,
        source_name: &'a str,
        surface_kind: &'a str,
        surface_name: &'a str,
        field_name: &'a str,
        field_role: &'a str,
        description: &'a str,
        matched_fields: Vec<&'a str>,
        retrieval_score: u32,
    }

    fn hit(input: HitInput<'_>) -> CatalogSearchHit {
        CatalogSearchHit {
            doc_id: input.doc_id.to_string(),
            doc_kind: input.doc_kind,
            source_name: input.source_name.to_string(),
            surface_kind: input.surface_kind.to_string(),
            surface_name: input.surface_name.to_string(),
            field_name: input.field_name.to_string(),
            field_role: input.field_role.to_string(),
            description: input.description.to_string(),
            matched_fields: input
                .matched_fields
                .into_iter()
                .map(str::to_string)
                .collect(),
            retrieval_score: input.retrieval_score,
        }
    }
}
