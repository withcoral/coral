//! The retrievers' statements over the search-owned tables.
//!
//! One ranked statement per backend serves every kind; a [`Scope`] adds each
//! kind's visibility conditions. The statements read no table of another
//! crate: what the owners decide arrives as binds. Every function joins the
//! caller's read snapshot. Query text travels only as binds.

use std::collections::{HashMap, HashSet};

use coral_catalog::{AuthoredToolKind, ProviderGenerationId, ToolKey};
use coral_db::{DatabaseSystem, Error, Transaction, UnixNanos};
use coral_provider::ProviderId;
use coral_tools::AuthoredToolId;
use coral_workspace::{Principal, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, Query};

use super::RetrievalError;
use super::plan::{
    QueryPlan, ScoringPlan, Statistics, WordStatistics, contains_pattern, prefix_pattern,
};
use super::vector_list::{CurrentRow, NearestVector};
use crate::CallPath;

/// The `search_entities` columns the coverage reads name, for sea-query.
#[derive(Iden)]
enum SearchEntities {
    Table,
    WorkspaceId,
    Kind,
    AuthoredToolId,
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

const TOOL: &str = "provider_tool";
const COMPLETE: &str = "complete";

/// The rows one kind's statement may rank, as its visibility step decided.
enum Scope<'a> {
    /// Tools of the visible, ready Providers' active Generations, less
    /// the Workspace's disabled Tools and those the caller does not
    /// reach. coral-provider names each Provider's active Generation; the
    /// statement does not read it again. The Tools of the `named` Providers
    /// rank first within their phrase points.
    Tools {
        workspace_id: &'a WorkspaceId,
        provider_ids: &'a [String],
        generation_ids: &'a [String],
        excluded: &'a [(String, String)],
        named: &'a [String],
    },
    /// The Authored Tools of one kind that the Principal can discover, in
    /// the entity table.
    AuthoredTools {
        workspace_id: &'a WorkspaceId,
        tools: AuthoredToolScope<'a>,
    },
    /// The same Authored Tools in the vector table. That table names no
    /// Principal, so their ids alone hold the Principal's rule.
    AuthoredToolVectors {
        workspace_id: &'a WorkspaceId,
        tools: AuthoredToolScope<'a>,
    },
}

/// The Authored Tools one statement may read: the Private Tools of
/// `principal` alone, or every Shared Tool of the Workspace, whose rows name
/// no Principal. Of these, only `ids` rank.
#[derive(Clone, Copy)]
pub(super) struct AuthoredToolScope<'a> {
    pub(super) kind: AuthoredToolKind,
    pub(super) principal: &'a Principal,
    /// The Tools of the kind that the caller discovers and that policy
    /// lets it call now. A row that a delete left, or that policy hides,
    /// takes no place in either list.
    pub(super) ids: &'a [AuthoredToolId],
}

impl AuthoredToolScope<'_> {
    /// The Principal whose rows alone the statement reads, for a kind whose
    /// rows name one.
    const fn owner(&self) -> Option<&Principal> {
        match self.kind {
            AuthoredToolKind::Private => Some(self.principal),
            AuthoredToolKind::Shared => None,
        }
    }

    /// The rows' keys: an Authored Tool's row is keyed by its id.
    fn doc_ids(&self) -> Vec<String> {
        self.ids.iter().map(ToString::to_string).collect()
    }
}

impl Scope<'_> {
    const fn workspace_id(&self) -> &WorkspaceId {
        match self {
            Self::Tools { workspace_id, .. }
            | Self::AuthoredTools { workspace_id, .. }
            | Self::AuthoredToolVectors { workspace_id, .. } => workspace_id,
        }
    }

    /// The conditions over the alias `d`, with their binds.
    fn sql(&self, binds: &mut Binds) -> String {
        let workspace = binds.push(self.workspace_id().to_string());
        match self {
            Self::Tools {
                provider_ids,
                generation_ids,
                excluded,
                ..
            } => {
                let providers = binds.list(provider_ids);
                let generations = binds.list(generation_ids);
                let excluded = binds.push(json(excluded));
                let excluded = match binds.system {
                    DatabaseSystem::Postgres => format!(
                        "SELECT 1 FROM jsonb_array_elements({excluded}::jsonb) AS x(v) \
                         WHERE x.v->>0 = d.provider_id AND x.v->>1 = d.tool_key"
                    ),
                    DatabaseSystem::Sqlite => format!(
                        "SELECT 1 FROM json_each({excluded}) AS x \
                         WHERE json_extract(x.value, '$[0]') = d.provider_id \
                         AND json_extract(x.value, '$[1]') = d.tool_key"
                    ),
                };
                // The Generation condition keeps only each Provider's active
                // Generation, so a superseded Generation never ranks, even
                // before its rows go. A Generation id belongs to one Provider,
                // so two sets select the same rows as the pairs would; the
                // Provider condition stays for its index.
                format!(
                    "d.workspace_id = {workspace} AND d.kind = '{TOOL}' \
                     AND d.provider_id IN ({providers}) \
                     AND d.provider_generation_id IN ({generations}) \
                     AND NOT EXISTS ({excluded})"
                )
            }
            Self::AuthoredTools { tools, .. } => {
                let kind = crate::EntityKind::authored(tools.kind).as_str();
                let ids = binds.list(&tools.doc_ids());
                let Some(principal) = tools.owner() else {
                    return format!(
                        "d.workspace_id = {workspace} AND d.kind = '{kind}' \
                         AND d.doc_id IN ({ids})"
                    );
                };
                let principal_id = binds.push(principal.id().as_str().to_string());
                let principal_kind = binds.push(principal.kind().as_storage_str().to_string());
                format!(
                    "d.workspace_id = {workspace} AND d.kind = '{kind}' \
                     AND d.principal_id = {principal_id} \
                     AND d.principal_kind = {principal_kind} \
                     AND d.doc_id IN ({ids})"
                )
            }
            Self::AuthoredToolVectors { tools, .. } => {
                let kind = crate::EntityKind::authored(tools.kind).as_str();
                let ids = binds.list(&tools.doc_ids());
                format!(
                    "d.workspace_id = {workspace} AND d.kind = '{kind}' \
                     AND d.doc_id IN ({ids})"
                )
            }
        }
    }

    /// The order key that puts the named Providers' Tools first, with its
    /// trailing comma, over `alias`; empty when no Provider is named.
    fn named_key(&self, binds: &mut Binds, alias: &str) -> String {
        let Self::Tools { named, .. } = self else {
            return String::new();
        };
        if named.is_empty() {
            return String::new();
        }
        let named = binds.list(named);
        format!("(CASE WHEN {alias}.provider_id IN ({named}) THEN 1 ELSE 0 END) DESC, ")
    }
}

/// One ranked row as the statement returns it: its key, the Call Path, the
/// owner keys its kind sets, its phrase points, and its name and title.
/// Decoded here into a typed ranked Entity.
#[derive(sqlx::FromRow)]
struct RankedRow {
    doc_id: String,
    call_path: String,
    provider_id: Option<String>,
    tool_key: Option<String>,
    authored_tool_id: Option<String>,
    #[sqlx(flatten)]
    matched: RowMatch,
}

/// What a ranked row's statement saw that its retriever's strong-match rule
/// reads: the phrase points the statement ordered by first, and the row's
/// name and title. No score leaves the retriever.
#[derive(sqlx::FromRow)]
pub(super) struct RowMatch {
    pub(super) phrase_points: i64,
    #[sqlx(rename = "qualified_name")]
    pub(super) name: String,
    pub(super) title: String,
}

/// One ranked Tool: its Search Entity key, its Provider, its Tool Key, its
/// Call Path, and what its statement saw.
pub(super) struct RankedTool {
    pub(super) doc_id: String,
    pub(super) provider_id: ProviderId,
    pub(super) tool_key: ToolKey,
    pub(super) call_path: CallPath,
    pub(super) matched: RowMatch,
}

/// One ranked Authored Tool: its identity, its Call Path, and what its
/// statement saw.
pub(super) struct RankedAuthoredTool {
    pub(super) id: AuthoredToolId,
    pub(super) call_path: CallPath,
    pub(super) matched: RowMatch,
}

/// The Tools a ranked statement may rank, and the Providers the query names.
pub(super) struct ToolScope<'a> {
    /// The searched Providers.
    pub(super) providers: &'a [ProviderId],
    /// Their active Generations: only these rows rank.
    pub(super) generations: &'a [ProviderGenerationId],
    /// Tools left out.
    pub(super) excluded: &'a [(ProviderId, ToolKey)],
    /// Searched Providers whose Tools rank first within their phrase points.
    pub(super) named: &'a [ProviderId],
}

/// Ranks the Tools of the scope for the plan, best first, at most `limit`.
pub(super) async fn rank_tools(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    tools: &ToolScope<'_>,
    plan: &QueryPlan,
    limit: usize,
) -> Result<Vec<RankedTool>, RetrievalError> {
    let binds = ToolBinds::new(tools);
    rank(transaction, &binds.scope(workspace_id), plan, limit)
        .await?
        .into_iter()
        .map(ranked_tool)
        .collect()
}

/// The vector list of the Tools of the scope: the Search Entity key and
/// source stamp of each vector of `revision`, nearest to `vector` first, at
/// most `limit`. Ties break by key. On Postgres it reads the vector table
/// alone, whose columns carry the scope's conditions; on `SQLite` the
/// scope's conditions pick the rowids that sqlite-vec scans.
pub(super) async fn nearest_tools(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    tools: &ToolScope<'_>,
    revision: &str,
    vector: &[f32],
    limit: usize,
) -> Result<Vec<NearestVector>, Error> {
    let binds = ToolBinds::new(tools);
    nearest(
        transaction,
        &binds.scope(workspace_id),
        revision,
        vector,
        limit,
    )
    .await
}

/// The vector list of the Authored Tools of the scope, as [`nearest_tools`]
/// gives the vector list of Tools.
pub(super) async fn nearest_authored_tools(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    tools: AuthoredToolScope<'_>,
    revision: &str,
    vector: &[f32],
    limit: usize,
) -> Result<Vec<NearestVector>, Error> {
    let scope = Scope::AuthoredToolVectors {
        workspace_id,
        tools,
    };
    nearest(transaction, &scope, revision, vector, limit).await
}

async fn nearest(
    transaction: &mut Transaction<'_>,
    scope: &Scope<'_>,
    revision: &str,
    vector: &[f32],
    limit: usize,
) -> Result<Vec<NearestVector>, Error> {
    let (sql, values) = nearest_statement(transaction.system(), scope, revision, vector, limit);
    let rows: Vec<(String, UnixNanos)> = transaction.fetch_all_sql(&sql, values).await?;
    Ok(rows
        .into_iter()
        .map(|(doc_id, source_indexed_at)| NearestVector {
            doc_id,
            source_indexed_at,
        })
        .collect())
}

/// The Workspace's Tool rows among `doc_ids`, each by its key with its
/// row's stamp, and what the plan's strong rule reads from it, as the ranked
/// statement would see it. One keyed read of the entity table: the vector
/// query already kept only the rows the scope may rank.
pub(super) async fn current_tools(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    plan: &QueryPlan,
    doc_ids: &[&str],
) -> Result<HashMap<String, CurrentRow<RankedTool>>, RetrievalError> {
    current_rows(transaction, workspace_id, plan, doc_ids, ranked_tool).await
}

/// The Workspace's Authored Tool rows of `kind` among `doc_ids`, as
/// [`current_tools`] reads Tool rows.
pub(super) async fn current_authored_tools(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    kind: AuthoredToolKind,
    plan: &QueryPlan,
    doc_ids: &[&str],
) -> Result<HashMap<String, CurrentRow<RankedAuthoredTool>>, RetrievalError> {
    current_rows(transaction, workspace_id, plan, doc_ids, |row| {
        ranked_authored_tool(kind, row)
    })
    .await
}

/// The rows among `doc_ids` by key, each with its stamp, decoded by
/// `decode`.
async fn current_rows<T>(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    plan: &QueryPlan,
    doc_ids: &[&str],
    decode: impl Fn(RankedRow) -> Result<T, RetrievalError>,
) -> Result<HashMap<String, CurrentRow<T>>, RetrievalError> {
    #[derive(sqlx::FromRow)]
    struct StampedRow {
        indexed_at_unix_nanos: UnixNanos,
        #[sqlx(flatten)]
        row: RankedRow,
    }
    let mut binds = Binds::new(transaction.system());
    let workspace = binds.push(workspace_id.to_string());
    let columns = ranked_columns(&mut binds, "d", plan);
    let doc_ids = binds.list(doc_ids);
    let sql = format!(
        "SELECT {columns}, d.indexed_at_unix_nanos \
         FROM search_entities AS d \
         WHERE d.workspace_id = {workspace} AND d.doc_id IN ({doc_ids})"
    );
    let rows: Vec<StampedRow> = transaction.fetch_all_sql(&sql, binds.values).await?;
    rows.into_iter()
        .map(|stamped| {
            Ok((
                stamped.row.doc_id.clone(),
                CurrentRow {
                    indexed_at: stamped.indexed_at_unix_nanos,
                    entry: decode(stamped.row)?,
                },
            ))
        })
        .collect()
}

/// The columns a [`RankedRow`] reads, over `alias`: its key, its Call Path,
/// its owner keys, its name and title, and its phrase points for the plan.
fn ranked_columns(binds: &mut Binds, alias: &str, plan: &QueryPlan) -> String {
    let phrase = phrase_points(binds, alias, plan);
    format!(
        "{}, CAST({phrase} AS BIGINT) AS phrase_points",
        row_columns(alias)
    )
}

/// The columns a [`RankedRow`] reads but its phrase points, over `alias`.
fn row_columns(alias: &str) -> String {
    format!(
        "{alias}.doc_id, {alias}.call_path, {alias}.provider_id, {alias}.tool_key, \
         {alias}.authored_tool_id, {alias}.qualified_name, {alias}.title"
    )
}

/// The vector query over the scope, with its binds.
///
/// On Postgres the vector binds as `halfvec`, the column's type, so the
/// distance needs no cast per row, and the scan is exact: the table has no
/// approximate (HNSW) index.
///
/// On `SQLite` the scope's conditions select, through the key table, the
/// rowids of the current revision's vectors, and `rowid IN (...)` hands
/// them to sqlite-vec, which scans only those, exactly, before it keeps the
/// nearest `limit`. The result orders ties by key, as on Postgres, but a tie
/// across that cut keeps the vector sqlite-vec scanned first, not the lower
/// key: its order of chunks and slots, the same for every request while the
/// index does not change.
fn nearest_statement(
    system: DatabaseSystem,
    scope: &Scope<'_>,
    revision: &str,
    vector: &[f32],
    limit: usize,
) -> (String, sea_query::Values) {
    let mut binds = Binds::new(system);
    let conditions = scope.sql(&mut binds);
    let revision = binds.push(revision.to_owned());
    let vector = binds.push(crate::storage::vector_literal(vector));
    let limit = binds.push(limit_value(limit));
    let sql = match system {
        DatabaseSystem::Postgres => format!(
            "SELECT d.doc_id, d.source_indexed_at_unix_nanos \
             FROM search_entity_embeddings AS d \
             WHERE {conditions} AND d.embedder_revision = {revision} \
             ORDER BY d.embedding <=> CAST({vector} AS halfvec), d.doc_id COLLATE \"C\" ASC \
             LIMIT {limit}"
        ),
        DatabaseSystem::Sqlite => format!(
            "WITH nearest AS MATERIALIZED (\
                SELECT v.rowid AS vector_rowid, v.distance \
                FROM search_entity_embedding_vectors AS v \
                WHERE v.embedding MATCH {vector} AND v.k = {limit} \
                AND v.rowid IN (\
                    SELECT e.vector_rowid FROM search_entity_embeddings AS e \
                    JOIN search_entities AS d \
                        ON d.workspace_id = e.workspace_id AND d.doc_id = e.doc_id \
                    WHERE {conditions} AND e.embedder_revision = {revision})) \
             SELECT e.doc_id, e.source_indexed_at_unix_nanos \
             FROM nearest AS n \
             JOIN search_entity_embeddings AS e ON e.vector_rowid = n.vector_rowid \
             ORDER BY n.distance, e.doc_id"
        ),
    };
    (sql, binds.values)
}

/// The plan of the vector query over `tools`, one line per row, for a
/// test that checks which tables it reads (Postgres) or what sqlite-vec
/// receives (`SQLite`).
#[cfg(test)]
pub(super) async fn explain_nearest_tools(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    tools: &ToolScope<'_>,
    revision: &str,
    vector: &[f32],
) -> Result<Vec<String>, Error> {
    let binds = ToolBinds::new(tools);
    explain_nearest(transaction, &binds.scope(workspace_id), revision, vector).await
}

/// The plan of the vector query over the Authored Tools of the scope, one
/// line per row, for the same tests.
#[cfg(test)]
pub(super) async fn explain_nearest_authored_tools(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    tools: AuthoredToolScope<'_>,
    revision: &str,
    vector: &[f32],
) -> Result<Vec<String>, Error> {
    let scope = Scope::AuthoredToolVectors {
        workspace_id,
        tools,
    };
    explain_nearest(transaction, &scope, revision, vector).await
}

#[cfg(test)]
async fn explain_nearest(
    transaction: &mut Transaction<'_>,
    scope: &Scope<'_>,
    revision: &str,
    vector: &[f32],
) -> Result<Vec<String>, Error> {
    let system = transaction.system();
    let (sql, values) = nearest_statement(system, scope, revision, vector, 100);
    Ok(match system {
        DatabaseSystem::Postgres => {
            let plan: Vec<(String,)> = transaction
                .fetch_all_sql(&format!("EXPLAIN {sql}"), values)
                .await?;
            plan.into_iter().map(|(line,)| line).collect()
        }
        DatabaseSystem::Sqlite => {
            let plan: Vec<(i64, i64, i64, String)> = transaction
                .fetch_all_sql(&format!("EXPLAIN QUERY PLAN {sql}"), values)
                .await?;
            plan.into_iter().map(|(_, _, _, line)| line).collect()
        }
    })
}

/// A [`ToolScope`]'s values as the statements bind them.
struct ToolBinds {
    provider_ids: Vec<String>,
    generation_ids: Vec<String>,
    excluded: Vec<(String, String)>,
    named: Vec<String>,
}

impl ToolBinds {
    fn new(tools: &ToolScope<'_>) -> Self {
        Self {
            provider_ids: tools.providers.iter().map(ToString::to_string).collect(),
            generation_ids: tools.generations.iter().map(ToString::to_string).collect(),
            excluded: tools
                .excluded
                .iter()
                .map(|(provider, key)| (provider.to_string(), key.as_str().to_string()))
                .collect(),
            named: tools.named.iter().map(ToString::to_string).collect(),
        }
    }

    fn scope<'a>(&'a self, workspace_id: &'a WorkspaceId) -> Scope<'a> {
        Scope::Tools {
            workspace_id,
            provider_ids: &self.provider_ids,
            generation_ids: &self.generation_ids,
            excluded: &self.excluded,
            named: &self.named,
        }
    }
}

fn ranked_tool(row: RankedRow) -> Result<RankedTool, RetrievalError> {
    let (Some(provider_id), Some(tool_key)) = (row.provider_id, row.tool_key) else {
        return Err(RetrievalError::Corrupt("a Tool row has no owner keys"));
    };
    Ok(RankedTool {
        doc_id: row.doc_id,
        provider_id: ProviderId::parse(&provider_id)
            .map_err(|_invalid| RetrievalError::Corrupt("invalid Provider id"))?,
        tool_key: ToolKey::parse(&tool_key)
            .map_err(|_invalid| RetrievalError::Corrupt("invalid Tool Key"))?,
        call_path: call_path(&row.call_path)?,
        matched: row.matched,
    })
}

/// Ranks the Authored Tools of the scope for the plan, best first, at most
/// `limit`.
pub(super) async fn rank_authored_tools(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    tools: AuthoredToolScope<'_>,
    plan: &QueryPlan,
    limit: usize,
) -> Result<Vec<RankedAuthoredTool>, RetrievalError> {
    let scope = Scope::AuthoredTools {
        workspace_id,
        tools,
    };
    rank(transaction, &scope, plan, limit)
        .await?
        .into_iter()
        .map(|row| ranked_authored_tool(tools.kind, row))
        .collect()
}

fn ranked_authored_tool(
    kind: AuthoredToolKind,
    row: RankedRow,
) -> Result<RankedAuthoredTool, RetrievalError> {
    let id = row.authored_tool_id.ok_or(RetrievalError::Corrupt(
        "an Authored Tool row has no Tool id",
    ))?;
    Ok(RankedAuthoredTool {
        id: AuthoredToolId::parse(kind, &id)
            .map_err(|_invalid| RetrievalError::Corrupt("invalid Authored Tool id"))?,
        call_path: call_path(&row.call_path)?,
        matched: row.matched,
    })
}

fn call_path(value: &str) -> Result<CallPath, RetrievalError> {
    CallPath::parse(value).map_err(|_invalid| RetrievalError::Corrupt("invalid Call Path"))
}

/// Ranks the scope's rows for the plan, best first, at most `limit` rows.
async fn rank(
    transaction: &mut Transaction<'_>,
    scope: &Scope<'_>,
    plan: &QueryPlan,
    limit: usize,
) -> Result<Vec<RankedRow>, Error> {
    match transaction.system() {
        DatabaseSystem::Postgres => rank_postgres(transaction, scope, plan, limit).await,
        DatabaseSystem::Sqlite => rank_sqlite(transaction, scope, plan, limit).await,
    }
}

/// How many candidates the Postgres statement scores exactly: the best by
/// phrase points, a named Provider and the estimate. The estimate is cheap
/// and the exact score is not, so a large Workspace's statement spends its
/// time on these alone. A candidate past them takes no place in the window.
const EXACT_SCORE_LIMIT: usize = 1_000;

/// Candidates by prefix `tsquery` over the spellings, or by substring over
/// `all_text`, in a `MATERIALIZED` CTE so the planner keeps the index scans
/// apart from the score. Then two stages.
///
/// The first estimates every candidate's score from its name, title,
/// description and Provider name, and from whether its schema text holds
/// each spelling, and keeps the best [`EXACT_SCORE_LIMIT`] of them, or the
/// window if that is larger, in the final order with the estimate in place
/// of the score. The estimate is the score with each spelling's schema text
/// counted once, so it is never above the score.
///
/// The second gives those candidates the approximate BM25 of the reference
/// implementation, with two caps per spelling: the field-weighted frequency
/// of every lexeme it prefix-matches in the name and the title (name 8,
/// title 6), and in the body (description and Provider name 2, schema text
/// 0.25), each saturated with `k1 = 1.2` and `b = 0.4`, added, times the
/// spelling's IDF. A word scores by its best spelling. The order is phrase
/// first, then a named Provider, then score, then Call Path, then document
/// id.
///
/// The plan decides from the Workspace's statistics which words select
/// candidates, but the scope can be narrower: a Provider filter, or the
/// Authored Tools of one kind. A rarer word that only rows outside it hold
/// would leave the list short. So when some word selects nothing and the
/// list holds fewer rows than `limit`, the statement runs again with every
/// word selecting: the cap cuts the tail of a full list, never the list.
async fn rank_postgres(
    transaction: &mut Transaction<'_>,
    scope: &Scope<'_>,
    plan: &QueryPlan,
    limit: usize,
) -> Result<Vec<RankedRow>, Error> {
    let statistics = statistics(transaction, scope.workspace_id(), plan).await?;
    let mut scoring = plan.scoring(&statistics);
    if scoring.words.is_empty() && plan.wordless.is_empty() {
        return Ok(Vec::new());
    }
    let (sql, values) = postgres_rank_statement(scope, plan, &scoring, limit);
    let rows: Vec<RankedRow> = transaction.fetch_all_sql(&sql, values).await?;
    if rows.len() >= limit || !scoring.select_by_every_word() {
        return Ok(rows);
    }
    let (sql, values) = postgres_rank_statement(scope, plan, &scoring, limit);
    transaction.fetch_all_sql(&sql, values).await
}

/// The two-stage statement of [`rank_postgres`], with its binds.
fn postgres_rank_statement(
    scope: &Scope<'_>,
    plan: &QueryPlan,
    scoring: &ScoringPlan,
    limit: usize,
) -> (String, sea_query::Values) {
    let mut binds = Binds::new(DatabaseSystem::Postgres);
    let conditions = scope.sql(&mut binds);
    let matches = postgres_candidates(&mut binds, plan, scoring);
    let estimate_sql = postgres_score(&mut binds, scoring, ScoreKind::Estimate);
    let columns = ranked_columns(&mut binds, "c", plan);
    let estimated_named = scope.named_key(&mut binds, "c");
    let exact_limit = binds.push(limit_value(limit.max(EXACT_SCORE_LIMIT)));
    let score_sql = postgres_score(&mut binds, scoring, ScoreKind::Exact);
    let named = scope.named_key(&mut binds, "c");
    let row_columns = row_columns("c");
    let limit = binds.push(limit_value(limit));
    let sql = format!(
        "WITH candidates AS MATERIALIZED ( \
             SELECT d.doc_id, d.call_path, d.provider_id, d.tool_key, d.authored_tool_id, \
                    d.qualified_name, d.title, d.description, d.provider_name, d.tsv, d.tsv_length, \
                    ts_filter(d.tsv, '{{a,b,c}}') AS fields \
             FROM search_entities AS d \
             WHERE {conditions} AND ({matches}) \
         ), \
         estimated AS MATERIALIZED ( \
             SELECT {columns}, c.tsv, c.tsv_length \
             FROM candidates AS c \
             CROSS JOIN LATERAL ({estimate_sql}) AS s \
             ORDER BY \
                 phrase_points DESC, \
                 {estimated_named}s.score DESC, \
                 c.call_path COLLATE \"C\" ASC, \
                 c.doc_id COLLATE \"C\" ASC \
             LIMIT {exact_limit} \
         ) \
         SELECT {row_columns}, c.phrase_points \
         FROM estimated AS c \
         CROSS JOIN LATERAL ({score_sql}) AS s \
         ORDER BY \
             c.phrase_points DESC, \
             {named}s.score DESC, \
             c.call_path COLLATE \"C\" ASC, \
             c.doc_id COLLATE \"C\" ASC \
         LIMIT {limit}"
    );
    (sql, binds.values)
}

/// The candidate disjuncts over the alias `d`: the prefix `tsquery` over the
/// spellings of the words that select, and a substring pattern per such
/// spelling that no row holds as a lexeme and per wordless term.
///
/// A held spelling selects by its lexemes alone. Its substring would add
/// only rows that hold it inside a longer lexeme, which the score gives
/// nothing for it, and each pattern costs a trigram scan and a recheck of
/// every matched row's whole text. The phrase test can still find the word
/// in such a row, at a camelCase edge of a title or description word
/// (`pageToken`): that row now leaves the list while another row holds the
/// word. A spelling no row holds keeps its pattern, so `enchmark` still
/// meets `benchmark_runs`.
fn postgres_candidates(binds: &mut Binds, plan: &QueryPlan, scoring: &ScoringPlan) -> String {
    let spellings = scoring
        .words
        .iter()
        .filter(|word| word.selects)
        .flat_map(|word| &word.spellings)
        .collect::<Vec<_>>();
    let mut matches = Vec::new();
    if !spellings.is_empty() {
        let tsquery = spellings
            .iter()
            .map(|spelling| format!("{}:*", spelling.word))
            .collect::<Vec<_>>()
            .join(" | ");
        matches.push(format!(
            "d.tsv @@ to_tsquery('simple', {})",
            binds.push(tsquery)
        ));
    }
    let unheld = spellings
        .into_iter()
        .filter(|spelling| !spelling.held)
        .map(|spelling| &spelling.word);
    for term in unheld.chain(&plan.wordless) {
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
///
/// The estimate reads the same weights from `c.fields`, the vector without
/// schema text, and adds a spelling's schema-text weight once when
/// `c.tsv` holds it there, however many lexemes it prefix-matches. Schema
/// text holds most of a row's lexemes, so the estimate unnests far fewer.
fn postgres_score(binds: &mut Binds, scoring: &ScoringPlan, kind: ScoreKind) -> String {
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
            let schema_text = match kind {
                ScoreKind::Exact => String::new(),
                ScoreKind::Estimate => format!(
                    ", {}::tsquery",
                    binds.push(schema_text_prefix_query(&spelling.word))
                ),
            };
            format!(
                "({}::int, {}::text, {}::float8{schema_text})",
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
             / ({f} + 1.2 * (1 - 0.4 + 0.4 * c.tsv_length::float8 / {avgdl}))"
        )
    };
    let (columns, lexemes, body_weights) = match kind {
        ScoreKind::Exact => (
            "word_position, pattern, idf",
            "c.tsv",
            "coalesce(sum(CASE w WHEN 'C' THEN 2::float8 WHEN 'D' THEN 0.25 ELSE 0 END), 0)",
        ),
        ScoreKind::Estimate => (
            "word_position, pattern, idf, schema_text",
            "c.fields",
            "coalesce(sum(CASE w WHEN 'C' THEN 2::float8 ELSE 0 END), 0) \
             + CASE WHEN c.tsv @@ q.schema_text THEN 0.25 ELSE 0 END",
        ),
    };
    format!(
        "SELECT coalesce(sum(b.best), 0)::float8 AS score FROM ( \
             SELECT max({name_group} + {body_group}) AS best \
             FROM (VALUES {spellings}) AS q({columns}) \
             CROSS JOIN LATERAL ( \
                 SELECT \
                     coalesce(sum(CASE w WHEN 'A' THEN 8::float8 WHEN 'B' THEN 6 ELSE 0 END), 0) AS name_group, \
                     {body_weights} AS body_group \
                 FROM unnest({lexemes}) AS u(lexeme, positions, weights) \
                 CROSS JOIN LATERAL unnest(u.weights) AS w \
                 WHERE u.lexeme LIKE q.pattern \
             ) AS tf \
             GROUP BY q.word_position \
         ) AS b",
        name_group = saturation("tf.name_group"),
        body_group = saturation("tf.body_group"),
    )
}

/// Which score a statement computes: the exact one, or the first stage's
/// estimate of it.
#[derive(Clone, Copy)]
enum ScoreKind {
    Exact,
    Estimate,
}

/// The `tsquery` that holds when a vector's schema text (weight `D`) holds a
/// lexeme that starts with `word`: the word quoted, so the parser leaves it
/// whole, as the score's `LIKE` does.
fn schema_text_prefix_query(word: &str) -> String {
    let mut quoted = String::with_capacity(word.len() + 6);
    quoted.push('\'');
    for character in word.chars() {
        if matches!(character, '\'' | '\\') {
            quoted.push('\\');
        }
        quoted.push(character);
    }
    quoted.push_str("':*D");
    quoted
}

/// An FTS5 `MATCH` over every term of three or more characters as a quoted
/// phrase, the same phrase-first order as Postgres, then a named Provider,
/// then the sum of two `bm25()` calls, then Call Path, then document id. As
/// on Postgres, each term saturates once in the name and the title (name 8,
/// title 6), and once in the body (description and Provider name 2, schema
/// text 1). A call adds up its columns' weighted hits before it saturates,
/// and a column weighted 0 adds nothing. So a long body needs many mentions
/// to come near the value of one name match. Two numbers differ from
/// Postgres: FTS5 fixes `b` at 0.75, not 0.4, and schema text weighs 1, not
/// 0.25, because 0.25 ranked lower on one of three SQLite benchmark corpora.
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
    let columns = ranked_columns(&mut binds, "d", plan);
    let named = scope.named_key(&mut binds, "d");
    let limit = binds.push(limit_value(limit));
    let sql = format!(
        "SELECT {columns} \
         FROM search_entities_fts AS f \
         JOIN search_entities AS d ON d.workspace_id = f.workspace_id AND d.doc_id = f.doc_id \
         WHERE f.workspace_id = d.workspace_id AND search_entities_fts MATCH {match_query} \
         AND {conditions} \
         ORDER BY \
             phrase_points DESC, \
             {named}bm25(search_entities_fts, 0.0, 0.0, 8.0, 6.0, 0.0, 0.0, 0.0) \
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

/// Opens the savepoint the vector side of one retriever's query runs in,
/// inside the retriever's own. A failed vector query rolls back to it, so
/// the retriever keeps its lexical list and the snapshot stays usable.
pub(super) async fn begin_vector_query(transaction: &mut Transaction<'_>) -> Result<(), Error> {
    transaction.execute_script("SAVEPOINT search_vector").await
}

/// Keeps what the vector side read.
pub(super) async fn end_vector_query(transaction: &mut Transaction<'_>) -> Result<(), Error> {
    transaction
        .execute_script("RELEASE SAVEPOINT search_vector")
        .await
}

/// Undoes the failed vector side back to its savepoint.
pub(super) async fn abandon_vector_query(transaction: &mut Transaction<'_>) -> Result<(), Error> {
    transaction
        .execute_script("ROLLBACK TO SAVEPOINT search_vector")
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
                .and_where(Expr::col(SearchIndexStates::Kind).eq(TOOL))
                .and_where(Expr::col(SearchIndexStates::State).eq(COMPLETE))
                .to_owned(),
        )
        .await?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// How many of a Workspace's Provider Tool paths hold a word that starts
/// with each of some words. It counts the paths of the given Generations
/// that the index holds complete: the last segment of each Provider Call
/// Root and the Tool-relative Path.
pub(super) struct PathWordCounts {
    /// The Provider Tools of those Generations.
    pub(super) tools: i64,
    /// How many of those Generations hold no counts: an earlier indexer
    /// wrote them, and a reconcile pass has not indexed them again yet.
    pub(super) uncounted: i64,
    /// Each word asked for, and at most how many of those Tools hold a word
    /// that starts with it.
    pub(super) words: Vec<PathWordCount>,
}

/// One word's count in [`PathWordCounts`].
pub(super) struct PathWordCount {
    pub(super) word: String,
    /// The sum of the Tool counts of the stored words that start with
    /// `word`. A Tool whose path holds two such words counts twice, so the
    /// sum is an upper bound, and it can be more than all the Tools. This is
    /// usual: a camelCase run is a word and so is its first part, so
    /// `getIssue` holds both `get` and `getissue`.
    pub(super) tools_at_most: i64,
}

/// The byte after `z` and after each digit. Each stored word and each word
/// asked for is ASCII letters and digits in lowercase, so the stored words
/// that start with `w` are those from `w` up to, not including, `w` followed
/// by this character, when words compare byte by byte.
const AFTER_EVERY_WORD_CHARACTER: &str = "{";

/// Counts the Provider Tool paths of `generations`, Generations of the
/// Workspace, for each of `words`, as [`PathWordCounts`] describes, in one
/// statement. Each word reads only its own range of the primary key.
pub(super) async fn path_word_counts(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    generations: &[String],
    words: &[&str],
) -> Result<PathWordCounts, Error> {
    #[derive(sqlx::FromRow)]
    struct Row {
        word: String,
        tools_at_most: i64,
        tools: i64,
        uncounted: i64,
    }
    let mut binds = Binds::new(transaction.system());
    let workspace = binds.push(workspace_id.to_string());
    let list = binds.push(json(words));
    let tool = binds.push(TOOL);
    let complete = binds.push(COMPLETE);
    let after = binds.push(AFTER_EVERY_WORD_CHARACTER);
    let counted = binds.list(generations);
    let (asked, collate) = match transaction.system() {
        DatabaseSystem::Postgres => (
            format!("SELECT jsonb_array_elements_text({list}::jsonb) AS word"),
            " COLLATE \"C\"",
        ),
        DatabaseSystem::Sqlite => (format!("SELECT value AS word FROM json_each({list})"), ""),
    };
    let complete_states = format!(
        "FROM search_index_states AS s \
         WHERE s.workspace_id = {workspace} AND s.kind = {tool} AND s.state = {complete} \
             AND s.target_id IN ({counted})"
    );
    let sql = format!(
        "SELECT q.word AS word, \
             CAST(coalesce(( \
                 SELECT sum(w.tool_count) FROM search_path_words AS w \
                 WHERE w.workspace_id = {workspace} \
                     AND w.word >= q.word{collate} \
                     AND w.word < (q.word || {after}){collate} \
                     AND w.provider_generation_id IN (SELECT s.target_id {complete_states}) \
             ), 0) AS BIGINT) AS tools_at_most, \
             CAST(coalesce((SELECT sum(s.tool_count) {complete_states}), 0) AS BIGINT) AS tools, \
             CAST(( \
                 SELECT count(*) {complete_states} AND s.tool_count IS NULL \
             ) AS BIGINT) AS uncounted \
         FROM ({asked}) AS q"
    );
    let rows: Vec<Row> = transaction.fetch_all_sql(&sql, binds.values).await?;
    Ok(PathWordCounts {
        tools: rows.first().map_or(0, |row| row.tools),
        uncounted: rows.first().map_or(0, |row| row.uncounted),
        words: rows
            .into_iter()
            .map(|row| PathWordCount {
                word: row.word,
                tools_at_most: row.tools_at_most,
            })
            .collect(),
    })
}

/// The Authored Tools of the scope that have a row in the index.
pub(super) async fn indexed_authored_tools(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    tools: AuthoredToolScope<'_>,
) -> Result<HashSet<String>, Error> {
    let mut statement = Query::select()
        .column(SearchEntities::AuthoredToolId)
        .from(SearchEntities::Table)
        .and_where(Expr::col(SearchEntities::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(
            Expr::col(SearchEntities::Kind).eq(crate::EntityKind::authored(tools.kind).as_str()),
        )
        .to_owned();
    if let Some(principal) = tools.owner() {
        statement
            .and_where(Expr::col(SearchEntities::PrincipalId).eq(principal.id().as_str()))
            .and_where(
                Expr::col(SearchEntities::PrincipalKind).eq(principal.kind().as_storage_str()),
            );
    }
    let rows: Vec<(String,)> = transaction.fetch_all(statement).await?;
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

    /// Adds `values` as one bind and returns a subquery that gives each
    /// value as a row, for an `IN` condition.
    fn list(&mut self, values: &[impl serde::Serialize]) -> String {
        let list = self.push(json(values));
        match self.system {
            DatabaseSystem::Postgres => format!("SELECT jsonb_array_elements_text({list}::jsonb)"),
            DatabaseSystem::Sqlite => format!("SELECT value FROM json_each({list})"),
        }
    }
}

fn json(value: &(impl serde::Serialize + ?Sized)) -> String {
    serde_json::to_string(value).expect("a list of strings serializes to JSON")
}

fn limit_value(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}
