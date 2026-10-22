//! The retrievers' statements over the search-owned tables.
//!
//! One ranked statement per backend serves both kinds; a [`Scope`] adds each
//! kind's visibility conditions. The statements read no table of another
//! crate: what the owners decide arrives as binds. Every function joins the
//! caller's read snapshot. Query text travels only as binds.

use std::collections::HashSet;

use coral_catalog::{OperationKey, ProviderGenerationId};
use coral_db::{DatabaseSystem, Error, Transaction};
use coral_functions::SavedFunctionId;
use coral_provider::ProviderId;
use coral_workspace::{Principal, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, Query};

use super::RetrievalError;
use super::plan::{
    QueryPlan, ScoringPlan, Statistics, WordStatistics, contains_pattern, prefix_pattern,
};
use crate::CallPath;

/// The `search_entities` columns the coverage reads name, for sea-query.
#[derive(Iden)]
enum SearchEntities {
    Table,
    WorkspaceId,
    Kind,
    SavedFunctionId,
    PrincipalId,
    PrincipalKind,
}

/// The `search_index_states` columns the coverage reads name, for sea-query.
#[derive(Iden)]
enum SearchIndexStates {
    Table,
    WorkspaceId,
    TargetId,
    Kind,
    State,
}

const OPERATION: &str = "operation";
const SAVED_FUNCTION: &str = "saved_function";
const COMPLETE: &str = "complete";

/// The rows one kind's statement may rank, as its visibility step decided.
enum Scope<'a> {
    /// Operations of the visible, ready Providers' active Generations, less
    /// the Workspace's disabled Operations and those the caller does not
    /// reach. coral-provider names each Provider's active Generation; the
    /// statement does not read it again.
    Operations {
        workspace_id: &'a WorkspaceId,
        provider_ids: &'a [String],
        generation_ids: &'a [String],
        excluded: &'a [(String, String)],
    },
    /// The Principal's own Saved Functions.
    SavedFunctions {
        workspace_id: &'a WorkspaceId,
        principal: &'a Principal,
    },
}

impl Scope<'_> {
    const fn workspace_id(&self) -> &WorkspaceId {
        match self {
            Self::Operations { workspace_id, .. } | Self::SavedFunctions { workspace_id, .. } => {
                workspace_id
            }
        }
    }

    /// The conditions over the alias `d`, with their binds.
    fn sql(&self, binds: &mut Binds) -> String {
        let workspace = binds.push(self.workspace_id().to_string());
        match self {
            Self::Operations {
                provider_ids,
                generation_ids,
                excluded,
                ..
            } => {
                let providers = binds.push(json(provider_ids));
                let generations = binds.push(json(generation_ids));
                let excluded = binds.push(json(excluded));
                let (providers, generations, excluded) = match binds.system {
                    DatabaseSystem::Postgres => (
                        format!("SELECT jsonb_array_elements_text({providers}::jsonb)"),
                        format!("SELECT jsonb_array_elements_text({generations}::jsonb)"),
                        format!(
                            "SELECT 1 FROM jsonb_array_elements({excluded}::jsonb) AS x(v) \
                             WHERE x.v->>0 = d.provider_id AND x.v->>1 = d.operation_key"
                        ),
                    ),
                    DatabaseSystem::Sqlite => (
                        format!("SELECT value FROM json_each({providers})"),
                        format!("SELECT value FROM json_each({generations})"),
                        format!(
                            "SELECT 1 FROM json_each({excluded}) AS x \
                             WHERE json_extract(x.value, '$[0]') = d.provider_id \
                             AND json_extract(x.value, '$[1]') = d.operation_key"
                        ),
                    ),
                };
                // The Generation condition keeps only each Provider's active
                // Generation, so a superseded Generation never ranks, even
                // before its rows go. A Generation id belongs to one Provider,
                // so two sets select the same rows as the pairs would; the
                // Provider condition stays for its index.
                format!(
                    "d.workspace_id = {workspace} AND d.kind = '{OPERATION}' \
                     AND d.provider_id IN ({providers}) \
                     AND d.provider_generation_id IN ({generations}) \
                     AND NOT EXISTS ({excluded})"
                )
            }
            Self::SavedFunctions { principal, .. } => {
                let principal_id = binds.push(principal.id().as_str().to_string());
                let principal_kind = binds.push(principal.kind().as_storage_str().to_string());
                format!(
                    "d.workspace_id = {workspace} AND d.kind = '{SAVED_FUNCTION}' \
                     AND d.principal_id = {principal_id} \
                     AND d.principal_kind = {principal_kind}"
                )
            }
        }
    }
}

/// One ranked row as the statement returns it: the Call Path and the owner
/// keys its kind sets. Decoded here into a typed ranked Entity.
#[derive(sqlx::FromRow)]
struct RankedRow {
    call_path: String,
    provider_id: Option<String>,
    operation_key: Option<String>,
    saved_function_id: Option<String>,
}

/// One ranked Operation: its Provider, its key, and its Call Path.
pub(super) struct RankedOperation {
    pub(super) provider_id: ProviderId,
    pub(super) operation_key: OperationKey,
    pub(super) call_path: CallPath,
}

/// One ranked Saved Function: its identity and its Call Path.
pub(super) struct RankedFunction {
    pub(super) saved_function_id: SavedFunctionId,
    pub(super) call_path: CallPath,
}

/// What one ranked statement found: its Entities, best first, and the
/// catalog words it matched as near-spellings of query words.
pub(super) struct Ranking<T> {
    pub(super) entities: Vec<T>,
    pub(super) near_spellings: Vec<String>,
}

/// Ranks the Operations of `providers` for the plan, best first, at most
/// `limit`. Only rows of `generations`, the Providers' active Generations,
/// rank. The `excluded` Operations are left out.
pub(super) async fn rank_operations(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    providers: &[ProviderId],
    generations: &[ProviderGenerationId],
    excluded: &[(ProviderId, OperationKey)],
    plan: &QueryPlan,
    limit: usize,
) -> Result<Ranking<RankedOperation>, RetrievalError> {
    let provider_ids = providers
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let generation_ids = generations
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let excluded = excluded
        .iter()
        .map(|(provider, key)| (provider.to_string(), key.as_str().to_string()))
        .collect::<Vec<_>>();
    let scope = Scope::Operations {
        workspace_id,
        provider_ids: &provider_ids,
        generation_ids: &generation_ids,
        excluded: &excluded,
    };
    let (rows, near_spellings) = rank(transaction, &scope, plan, limit).await?;
    let entities = rows
        .into_iter()
        .map(|row| {
            let (Some(provider_id), Some(operation_key)) = (row.provider_id, row.operation_key)
            else {
                return Err(RetrievalError::Corrupt(
                    "an Operation row has no owner keys",
                ));
            };
            Ok(RankedOperation {
                provider_id: ProviderId::parse(&provider_id)
                    .map_err(|_invalid| RetrievalError::Corrupt("invalid Provider id"))?,
                operation_key: OperationKey::parse(&operation_key)
                    .map_err(|_invalid| RetrievalError::Corrupt("invalid Operation Key"))?,
                call_path: call_path(&row.call_path)?,
            })
        })
        .collect::<Result<_, RetrievalError>>()?;
    Ok(Ranking {
        entities,
        near_spellings,
    })
}

/// Ranks the Principal's own Saved Functions for the plan, best first, at
/// most `limit`.
pub(super) async fn rank_functions(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    plan: &QueryPlan,
    limit: usize,
) -> Result<Ranking<RankedFunction>, RetrievalError> {
    let scope = Scope::SavedFunctions {
        workspace_id,
        principal,
    };
    let (rows, near_spellings) = rank(transaction, &scope, plan, limit).await?;
    let entities = rows
        .into_iter()
        .map(|row| {
            let saved_function_id = row.saved_function_id.ok_or(RetrievalError::Corrupt(
                "a Saved Function row has no function id",
            ))?;
            Ok(RankedFunction {
                saved_function_id: SavedFunctionId::parse(&saved_function_id)
                    .map_err(|_invalid| RetrievalError::Corrupt("invalid Saved Function id"))?,
                call_path: call_path(&row.call_path)?,
            })
        })
        .collect::<Result<_, RetrievalError>>()?;
    Ok(Ranking {
        entities,
        near_spellings,
    })
}

fn call_path(value: &str) -> Result<CallPath, RetrievalError> {
    CallPath::parse(value).map_err(|_invalid| RetrievalError::Corrupt("invalid Call Path"))
}

/// Ranks the scope's rows for the plan, best first, at most `limit` rows,
/// with the near-spellings the statement matched. `SQLite` matches none.
async fn rank(
    transaction: &mut Transaction<'_>,
    scope: &Scope<'_>,
    plan: &QueryPlan,
    limit: usize,
) -> Result<(Vec<RankedRow>, Vec<String>), Error> {
    match transaction.system() {
        DatabaseSystem::Postgres => rank_postgres(transaction, scope, plan, limit).await,
        DatabaseSystem::Sqlite => Ok((
            rank_sqlite(transaction, scope, plan, limit).await?,
            Vec::new(),
        )),
    }
}

/// Candidates by prefix `tsquery` over the spellings, or by substring over
/// `all_text`, in a `MATERIALIZED` CTE so the planner keeps the index scans
/// apart from the score. Then the approximate BM25 of the reference
/// implementation, with two caps per spelling: the field-weighted frequency
/// of every lexeme it prefix-matches in the name and the title (name 8,
/// title 6), and in the body (description and Provider name 2, schema text
/// 0.25), each saturated with `k1 = 1.2` and `b = 0.4`, added, times the
/// spelling's IDF. A word scores by its best spelling. The order is phrase
/// first, then score, then Call Path, then document id.
async fn rank_postgres(
    transaction: &mut Transaction<'_>,
    scope: &Scope<'_>,
    plan: &QueryPlan,
    limit: usize,
) -> Result<(Vec<RankedRow>, Vec<String>), Error> {
    let statistics = statistics(transaction, scope.workspace_id(), plan).await?;
    let scoring = plan.scoring(&statistics);
    if scoring.words.is_empty() && plan.wordless.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let mut binds = Binds::new(DatabaseSystem::Postgres);
    let conditions = scope.sql(&mut binds);
    let matches = postgres_candidates(&mut binds, plan, &scoring);
    let score_sql = postgres_score(&mut binds, &scoring);
    let phrase = phrase_points(&mut binds, "c", plan);
    let limit = binds.push(limit_value(limit));
    let sql = format!(
        "WITH candidates AS MATERIALIZED ( \
             SELECT d.doc_id, d.call_path, d.provider_id, d.operation_key, d.saved_function_id, \
                    d.qualified_name, d.title, d.description, d.provider_name, d.tsv, d.doc_len \
             FROM search_entities AS d \
             WHERE {conditions} AND ({matches}) \
         ) \
         SELECT c.call_path, c.provider_id, c.operation_key, c.saved_function_id \
         FROM candidates AS c \
         CROSS JOIN LATERAL ({score_sql}) AS s \
         ORDER BY \
             {phrase} DESC, \
             s.score DESC, \
             c.call_path COLLATE \"C\" ASC, \
             c.doc_id COLLATE \"C\" ASC \
         LIMIT {limit}"
    );
    let rows = transaction.fetch_all_sql(&sql, binds.values).await?;
    Ok((rows, scoring.near_spellings()))
}

/// The candidate disjuncts over the alias `d`: the prefix `tsquery` over the
/// spellings, and a substring pattern per spelling and per wordless term.
fn postgres_candidates(binds: &mut Binds, plan: &QueryPlan, scoring: &ScoringPlan) -> String {
    let spellings = scoring
        .words
        .iter()
        .flat_map(|word| &word.spellings)
        .map(|spelling| &spelling.word)
        .collect::<Vec<_>>();
    let mut matches = Vec::new();
    if !spellings.is_empty() {
        let tsquery = spellings
            .iter()
            .map(|spelling| format!("{spelling}:*"))
            .collect::<Vec<_>>()
            .join(" | ");
        matches.push(format!(
            "d.tsv @@ to_tsquery('simple', {})",
            binds.push(tsquery)
        ));
    }
    for term in spellings.into_iter().chain(&plan.wordless) {
        matches.push(format!(
            "d.all_text ILIKE {}",
            binds.push(contains_pattern(term))
        ));
    }
    matches.join(" OR ")
}

/// The per-candidate score over the alias `c`: for each word, the best of its
/// spellings. A spelling adds up the field weights of every position of every
/// lexeme it prefix-matches, in two groups that each saturate once: the name
/// and the title (weight classes `A` 8 and `B` 6), and the body (`C` 2 for
/// the description and the Provider name, `D` 0.25 for the schema text).
/// So `remove` earns at most one cap from the name and the title and one
/// from the body, however many forms (`removelabels`, `removes`) or
/// mentions each holds. Schema text holds each word once, so only many forms
/// of a word in it come near the value of one name match.
/// The vector splits camelCase names, so `comments` meets the name word of
/// `listComments`.
fn postgres_score(binds: &mut Binds, scoring: &ScoringPlan) -> String {
    if scoring.words.is_empty() {
        return "SELECT 0::float8 AS score".to_string();
    }
    let avgdl = binds.push(scoring.avgdl);
    let spellings = scoring
        .words
        .iter()
        .enumerate()
        .flat_map(|(position, word)| {
            word.spellings
                .iter()
                .map(move |spelling| (position, spelling))
        })
        .map(|(position, spelling)| {
            format!(
                "({}::int, {}::text, {}::float8)",
                binds.push(i32::try_from(position).unwrap_or(i32::MAX)),
                binds.push(prefix_pattern(&spelling.word)),
                binds.push(spelling.idf)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let saturation = |f: &str| {
        format!(
            "q.idf * ({f} * (1.2 + 1)) \
             / ({f} + 1.2 * (1 - 0.4 + 0.4 * coalesce(c.doc_len::float8, {avgdl}) / {avgdl}))"
        )
    };
    format!(
        "SELECT coalesce(sum(b.best), 0)::float8 AS score FROM ( \
             SELECT max({name_group} + {body_group}) AS best \
             FROM (VALUES {spellings}) AS q(word_position, pattern, idf) \
             CROSS JOIN LATERAL ( \
                 SELECT \
                     coalesce(sum(CASE w WHEN 'A' THEN 8::float8 WHEN 'B' THEN 6 ELSE 0 END), 0) AS name_group, \
                     coalesce(sum(CASE w WHEN 'C' THEN 2::float8 WHEN 'D' THEN 0.25 ELSE 0 END), 0) AS body_group \
                 FROM unnest(c.tsv) AS u(lexeme, positions, weights) \
                 CROSS JOIN LATERAL unnest(u.weights) AS w \
                 WHERE u.lexeme LIKE q.pattern \
             ) AS tf \
             GROUP BY q.word_position \
         ) AS b",
        name_group = saturation("tf.name_group"),
        body_group = saturation("tf.body_group"),
    )
}

/// An FTS5 `MATCH` over every term of three or more characters as a quoted
/// phrase, the same phrase-first order as Postgres, then the sum of two
/// `bm25()` calls, then Call Path, then document id. As on Postgres, each
/// term saturates once in the name and the title (name 8, title 6), and once
/// in the body (description and Provider name 2, schema text 1). A call adds
/// up its columns' weighted hits before it saturates, and a column weighted 0
/// adds nothing. So a long body needs many mentions to come near the value of
/// one name match. Two numbers differ from Postgres: FTS5 fixes `b` at 0.75,
/// not 0.4, and schema text weighs 1, not 0.25, because 0.25 ranked lower on
/// one of three SQLite benchmark corpora.
async fn rank_sqlite(
    transaction: &mut Transaction<'_>,
    scope: &Scope<'_>,
    plan: &QueryPlan,
    limit: usize,
) -> Result<Vec<RankedRow>, Error> {
    let mut binds = Binds::new(DatabaseSystem::Sqlite);
    let conditions = scope.sql(&mut binds);
    let match_query = binds.push(
        plan.match_terms
            .iter()
            .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(" OR "),
    );
    let phrase = phrase_points(&mut binds, "d", plan);
    let limit = binds.push(limit_value(limit));
    let sql = format!(
        "SELECT d.call_path, d.provider_id, d.operation_key, d.saved_function_id \
         FROM search_entities_fts AS f \
         JOIN search_entities AS d ON d.workspace_id = f.workspace_id AND d.doc_id = f.doc_id \
         WHERE f.workspace_id = d.workspace_id AND search_entities_fts MATCH {match_query} \
         AND {conditions} \
         ORDER BY \
             {phrase} DESC, \
             bm25(search_entities_fts, 0.0, 0.0, 8.0, 6.0, 0.0, 0.0, 0.0) \
                 + bm25(search_entities_fts, 0.0, 0.0, 0.0, 0.0, 2.0, 2.0, 1.0) ASC, \
             d.call_path ASC, \
             d.doc_id ASC \
         LIMIT {limit}"
    );
    transaction.fetch_all_sql(&sql, binds.values).await
}

/// The phrase points over the alias's name, title, description and Provider
/// name, the first order key on both backends. Each field adds its weight
/// once when it holds the phrase's words in order, and twice when it is the
/// whole phrase. Schema text adds none: it holds each word once, at its first
/// occurrence, so two words in order there are chance. One pattern pair
/// serves Postgres's `~` and `SQLite`'s `REGEXP` alike.
fn phrase_points(binds: &mut Binds, alias: &str, plan: &QueryPlan) -> String {
    let phrase = &plan.phrase;
    let operator = match binds.system {
        DatabaseSystem::Postgres => "~",
        DatabaseSystem::Sqlite => "REGEXP",
    };
    let whole = binds.push(phrase.whole.clone());
    let contained = binds.push(phrase.contained.clone());
    [
        ("qualified_name", 8),
        ("title", 6),
        ("description", 2),
        ("provider_name", 2),
    ]
    .iter()
    .map(|(column, weight)| {
        format!(
            "(CASE WHEN {alias}.{column} {operator} {whole} THEN {} \
             WHEN {alias}.{column} {operator} {contained} THEN {weight} ELSE 0 END)",
            weight * 2
        )
    })
    .collect::<Vec<_>>()
    .join(" + ")
}

/// The Workspace's Postgres statistics for each of the plan's words, and the
/// Workspace's words within one letter of the length of each word that a
/// near-spelling may start from. Statistics never built read as an empty
/// corpus, which degrades to unweighted scoring.
async fn statistics(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    plan: &QueryPlan,
) -> Result<Statistics, Error> {
    #[derive(sqlx::FromRow)]
    struct Totals {
        total_docs: i64,
        avgdl: f64,
    }
    let totals: Vec<Totals> = transaction
        .fetch_all_sql(
            "SELECT total_docs, avgdl FROM search_stats WHERE workspace_id = $1",
            sea_query::Values(vec![workspace_id.to_string().into()]),
        )
        .await?;
    let words = word_statistics(
        transaction,
        "SELECT w.term, coalesce(t.ndoc, 0)::bigint AS ndoc \
         FROM jsonb_array_elements_text($2::jsonb) AS w(term) \
         LEFT JOIN search_terms AS t ON t.workspace_id = $1 AND t.term = w.term",
        workspace_id,
        &plan.words,
    )
    .await?;
    let unknown = plan.near_spelling_words(&words);
    let catalog_words = if unknown.is_empty() {
        Vec::new()
    } else {
        word_statistics(
            transaction,
            "SELECT t.term, t.ndoc::bigint AS ndoc FROM search_terms AS t \
             WHERE t.workspace_id = $1 AND EXISTS ( \
                 SELECT 1 FROM jsonb_array_elements_text($2::jsonb) AS w(word) \
                 WHERE abs(char_length(t.term) - char_length(w.word)) <= 1 \
             )",
            workspace_id,
            &unknown,
        )
        .await?
    };
    let (total_docs, avgdl) = totals
        .first()
        .map_or((0, 0.0), |totals| (totals.total_docs, totals.avgdl));
    Ok(Statistics {
        words,
        catalog_words,
        total_docs,
        avgdl,
    })
}

/// Runs one statement over `search_terms` that binds the Workspace and a JSON
/// list of words, and returns its terms with their document counts.
async fn word_statistics(
    transaction: &mut Transaction<'_>,
    sql: &str,
    workspace_id: &WorkspaceId,
    words: &[String],
) -> Result<Vec<WordStatistics>, Error> {
    #[derive(sqlx::FromRow)]
    struct Word {
        term: String,
        ndoc: i64,
    }
    let rows: Vec<Word> = transaction
        .fetch_all_sql(
            sql,
            sea_query::Values(vec![workspace_id.to_string().into(), json(words).into()]),
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|word| WordStatistics {
            word: word.term,
            ndoc: word.ndoc,
        })
        .collect())
}

/// Opens the savepoint one retriever's query runs in. A failed ranked
/// statement rolls back to it, so on Postgres the snapshot stays usable for
/// the other retrievers and for hydration.
pub(crate) async fn begin_query(transaction: &mut Transaction<'_>) -> Result<(), Error> {
    transaction
        .execute_script("SAVEPOINT search_retriever")
        .await
}

/// Keeps what the retriever's query read.
pub(crate) async fn end_query(transaction: &mut Transaction<'_>) -> Result<(), Error> {
    transaction
        .execute_script("RELEASE SAVEPOINT search_retriever")
        .await
}

/// Undoes the failed query back to its savepoint.
pub(crate) async fn abandon_query(transaction: &mut Transaction<'_>) -> Result<(), Error> {
    transaction
        .execute_script("ROLLBACK TO SAVEPOINT search_retriever")
        .await
}

/// The Generations of the Workspace whose index state is `complete`.
pub(super) async fn complete_generations(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<HashSet<String>, Error> {
    let rows: Vec<(String,)> = transaction
        .fetch_all(
            Query::select()
                .column(SearchIndexStates::TargetId)
                .from(SearchIndexStates::Table)
                .and_where(Expr::col(SearchIndexStates::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(SearchIndexStates::Kind).eq(OPERATION))
                .and_where(Expr::col(SearchIndexStates::State).eq(COMPLETE))
                .to_owned(),
        )
        .await?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// The Principal's Saved Functions that have a row in the index.
pub(super) async fn indexed_functions(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
) -> Result<HashSet<String>, Error> {
    let rows: Vec<(String,)> = transaction
        .fetch_all(
            Query::select()
                .column(SearchEntities::SavedFunctionId)
                .from(SearchEntities::Table)
                .and_where(Expr::col(SearchEntities::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(SearchEntities::Kind).eq(SAVED_FUNCTION))
                .and_where(Expr::col(SearchEntities::PrincipalId).eq(principal.id().as_str()))
                .and_where(
                    Expr::col(SearchEntities::PrincipalKind).eq(principal.kind().as_storage_str()),
                )
                .to_owned(),
        )
        .await?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// A statement that always fails, for a test that makes one retriever's
/// ranking fail through the same path a real statement failure takes.
#[cfg(test)]
pub(super) async fn failing_statement(transaction: &mut Transaction<'_>) -> Result<(), Error> {
    transaction
        .execute_sql(
            "SELECT 1 FROM search_injected_fault",
            sea_query::Values(Vec::new()),
        )
        .await
        .map(|_rows| ())
}

/// Positional binds in the placeholder syntax of one backend.
struct Binds {
    system: DatabaseSystem,
    values: sea_query::Values,
}

impl Binds {
    const fn new(system: DatabaseSystem) -> Self {
        Self {
            system,
            values: sea_query::Values(Vec::new()),
        }
    }

    /// Adds one bind and returns its placeholder.
    fn push(&mut self, value: impl Into<sea_query::Value>) -> String {
        self.values.0.push(value.into());
        let position = self.values.0.len();
        match self.system {
            DatabaseSystem::Postgres => format!("${position}"),
            DatabaseSystem::Sqlite => format!("?{position}"),
        }
    }
}

fn json(value: &(impl serde::Serialize + ?Sized)) -> String {
    serde_json::to_string(value).expect("a list of strings serializes to JSON")
}

fn limit_value(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}
