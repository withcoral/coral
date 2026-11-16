//! The Embedder's statements: which Search Entities have no current vector,
//! their text, the vector write and the orphan sweep.
//!
//! On Postgres one table holds each vector beside its key, revision and
//! source stamp. On `SQLite` the table of that name holds the key, revision,
//! stamp and the rowid of the vector in the sqlite-vec table
//! `search_entity_embedding_vectors`, so the statements that read only
//! revisions and stamps are the same on both.
//!
//! Every function joins the caller's transaction; the Embedder owns when
//! each transaction begins, commits or rolls back.

use std::fmt::Write as _;

use coral_db::{DatabaseSystem, Error, Transaction};
use coral_workspace::WorkspaceId;
use sea_query::{Value, Values};

use crate::indexer::IndexError;
use crate::storage::vector_literal;
use crate::types::EntityKind;

/// One Search Entity without a current vector: its key and the stamp of its
/// row.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct StaleEntity {
    pub(super) workspace_id: WorkspaceId,
    pub(super) doc_id: String,
    pub(super) indexed_at_unix_nanos: i64,
}

/// The columns of one Search Entity's row that its text reads.
pub(super) struct EntityText {
    pub(super) doc_id: String,
    pub(super) kind: EntityKind,
    pub(super) qualified_name: String,
    pub(super) title: String,
    pub(super) description: String,
    pub(super) provider_name: String,
    pub(super) field_names: String,
    pub(super) indexed_at_unix_nanos: i64,
}

/// One vector to write for the row of `doc_id` stamped
/// `indexed_at_unix_nanos`.
pub(super) struct EntityVector<'a> {
    pub(super) doc_id: &'a str,
    pub(super) indexed_at_unix_nanos: i64,
    pub(super) vector: &'a [f32],
}

/// The FROM and WHERE clauses that select, as `s`, the Search Entities
/// whose vector is missing, of another revision than `revision_bind`
/// binds, or of another source stamp than the row's. A row whose field
/// names an older indexer did not write, or did not update with the rest of
/// the row, waits until a reconcile pass indexes it again.
fn from_stale_entities(revision_bind: &str) -> String {
    format!(
        "FROM search_entities s \
         LEFT JOIN search_entity_embeddings e \
             ON e.workspace_id = s.workspace_id AND e.doc_id = s.doc_id \
         WHERE s.field_names_fingerprint = s.fingerprint \
         AND (e.doc_id IS NULL \
             OR e.embedder_revision <> {revision_bind} \
             OR e.source_indexed_at_unix_nanos <> s.indexed_at_unix_nanos)"
    )
}

/// How many Search Entities need a vector of `revision`. The count reads
/// the entity table and the vector table whole.
pub(super) async fn count_stale(
    transaction: &mut Transaction<'_>,
    revision: &str,
) -> Result<u64, IndexError> {
    let stale = from_stale_entities(&bind(transaction, 1));
    let rows: Vec<(i64,)> = transaction
        .fetch_all_sql(
            &format!("SELECT count(*) {stale}"),
            Values(vec![revision.into()]),
        )
        .await?;
    let (stale,) = rows
        .first()
        .ok_or(IndexError::Corrupt("a count returned no row"))?;
    u64::try_from(*stale).map_err(|_negative| IndexError::Corrupt("a negative count"))
}

/// The Search Entities that need a vector of `revision`, at most `limit`,
/// oldest write first.
pub(super) async fn stale_entities(
    transaction: &mut Transaction<'_>,
    revision: &str,
    limit: usize,
) -> Result<Vec<StaleEntity>, IndexError> {
    let [revision_bind, limit_bind] = [1, 2].map(|position| bind(transaction, position));
    let rows: Vec<(String, String, i64)> = transaction
        .fetch_all_sql(
            &format!(
                "SELECT s.workspace_id, s.doc_id, s.indexed_at_unix_nanos {} \
                 ORDER BY s.indexed_at_unix_nanos, s.workspace_id, s.doc_id \
                 LIMIT {limit_bind}",
                from_stale_entities(&revision_bind)
            ),
            Values(vec![
                revision.into(),
                i64::try_from(limit).unwrap_or(i64::MAX).into(),
            ]),
        )
        .await?;
    rows.into_iter()
        .map(|(workspace_id, doc_id, indexed_at_unix_nanos)| {
            Ok(StaleEntity {
                workspace_id: WorkspaceId::parse(&workspace_id)
                    .map_err(|_error| IndexError::Corrupt("search entity workspace id"))?,
                doc_id,
                indexed_at_unix_nanos,
            })
        })
        .collect()
}

/// The kind whose stable name a search row stores as `kind`.
fn entity_kind(kind: &str) -> Option<EntityKind> {
    [
        EntityKind::ProviderTool,
        EntityKind::PrivateTool,
        EntityKind::SharedTool,
    ]
    .into_iter()
    .find(|candidate| candidate.as_str() == kind)
}

/// The text columns of the Workspace's rows among `doc_ids` whose field
/// names are of the row's fingerprint, as they stand now.
pub(super) async fn entity_texts(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    doc_ids: &[&str],
) -> Result<Vec<EntityText>, IndexError> {
    let mut sql = format!(
        "SELECT doc_id, kind, qualified_name, title, description, provider_name, field_names, \
         indexed_at_unix_nanos \
         FROM search_entities \
         WHERE workspace_id = {} AND field_names_fingerprint = fingerprint AND doc_id IN (",
        bind(transaction, 1)
    );
    let mut values = vec![Value::from(workspace_id.to_string())];
    for (position, doc_id) in doc_ids.iter().enumerate() {
        if position > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&bind(transaction, position + 2));
        values.push((*doc_id).into());
    }
    sql.push(')');
    let rows: Vec<EntityTextRow> = transaction.fetch_all_sql(&sql, Values(values)).await?;
    rows.into_iter()
        .map(
            |(
                doc_id,
                kind,
                qualified_name,
                title,
                description,
                provider_name,
                field_names,
                indexed_at_unix_nanos,
            )| {
                Ok(EntityText {
                    doc_id,
                    kind: entity_kind(&kind).ok_or(IndexError::Corrupt("search entity kind"))?,
                    qualified_name,
                    title,
                    description,
                    provider_name,
                    field_names,
                    indexed_at_unix_nanos,
                })
            },
        )
        .collect()
}

/// `doc_id`, `kind`, `qualified_name`, `title`, `description`,
/// `provider_name`, `field_names` and `indexed_at_unix_nanos`.
type EntityTextRow = (String, String, String, String, String, String, String, i64);

/// Writes each vector of `vectors` with `revision`, when its row still holds
/// the stamp the vector was made from and the stored vector is still stale.
/// The filter columns come from the row. Returns the vectors written: a
/// vector that another Embedder wrote first, or whose row changed or went,
/// is not.
pub(super) async fn write_vectors(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    revision: &str,
    vectors: &[EntityVector<'_>],
) -> Result<u64, Error> {
    match transaction.system() {
        DatabaseSystem::Postgres => {
            write_postgres_vectors(transaction, workspace_id, revision, vectors).await
        }
        DatabaseSystem::Sqlite => {
            write_sqlite_vectors(transaction, workspace_id, revision, vectors).await
        }
    }
}

async fn write_postgres_vectors(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    revision: &str,
    vectors: &[EntityVector<'_>],
) -> Result<u64, Error> {
    if vectors.is_empty() {
        return Ok(0);
    }
    let mut rows = String::new();
    let mut values = vec![Value::from(workspace_id.to_string()), Value::from(revision)];
    for (position, vector) in vectors.iter().enumerate() {
        let first = values.len() + 1;
        if position > 0 {
            rows.push_str(", ");
        }
        write!(rows, "(${first}, ${}, ${})", first + 1, first + 2)
            .expect("writing to a String does not fail");
        values.push(vector.doc_id.into());
        values.push(vector.indexed_at_unix_nanos.into());
        values.push(vector_literal(vector.vector).into());
    }
    let sql = format!(
        "INSERT INTO search_entity_embeddings (\
            workspace_id, doc_id, kind, provider_id, provider_generation_id, tool_key, \
            embedder_revision, source_indexed_at_unix_nanos, embedding) \
         SELECT s.workspace_id, s.doc_id, s.kind, s.provider_id, s.provider_generation_id, \
            s.tool_key, $2, s.indexed_at_unix_nanos, v.embedding::halfvec \
         FROM (VALUES {rows}) AS v(doc_id, indexed_at_unix_nanos, embedding) \
         JOIN search_entities s \
             ON s.workspace_id = $1 \
             AND s.doc_id = v.doc_id \
             AND s.indexed_at_unix_nanos = v.indexed_at_unix_nanos \
         ON CONFLICT (workspace_id, doc_id) DO UPDATE SET \
            kind = EXCLUDED.kind, \
            provider_id = EXCLUDED.provider_id, \
            provider_generation_id = EXCLUDED.provider_generation_id, \
            tool_key = EXCLUDED.tool_key, \
            embedder_revision = EXCLUDED.embedder_revision, \
            source_indexed_at_unix_nanos = EXCLUDED.source_indexed_at_unix_nanos, \
            embedding = EXCLUDED.embedding \
         WHERE search_entity_embeddings.embedder_revision <> EXCLUDED.embedder_revision \
         OR search_entity_embeddings.source_indexed_at_unix_nanos \
             <> EXCLUDED.source_indexed_at_unix_nanos"
    );
    transaction.execute_sql(&sql, Values(values)).await
}

/// Writes each vector on `SQLite`, one row at a time: the key row first,
/// under the same conditions as on Postgres, then the vector at the key
/// row's rowid. sqlite-vec 0.1.9 has no upsert, and a rowid that a removed
/// key row left can hold an old vector, so the vector row is deleted before
/// it is written.
async fn write_sqlite_vectors(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    revision: &str,
    vectors: &[EntityVector<'_>],
) -> Result<u64, Error> {
    let mut written = 0;
    for vector in vectors {
        let keyed: Vec<(i64,)> = transaction
            .fetch_all_sql(
                "INSERT INTO search_entity_embeddings (\
                    workspace_id, doc_id, embedder_revision, source_indexed_at_unix_nanos) \
                 SELECT s.workspace_id, s.doc_id, ?3, s.indexed_at_unix_nanos \
                 FROM search_entities AS s \
                 WHERE s.workspace_id = ?1 AND s.doc_id = ?2 AND s.indexed_at_unix_nanos = ?4 \
                 ON CONFLICT (workspace_id, doc_id) DO UPDATE SET \
                    embedder_revision = excluded.embedder_revision, \
                    source_indexed_at_unix_nanos = excluded.source_indexed_at_unix_nanos \
                 WHERE search_entity_embeddings.embedder_revision <> excluded.embedder_revision \
                 OR search_entity_embeddings.source_indexed_at_unix_nanos \
                     <> excluded.source_indexed_at_unix_nanos \
                 RETURNING vector_rowid",
                Values(vec![
                    workspace_id.to_string().into(),
                    vector.doc_id.into(),
                    revision.into(),
                    vector.indexed_at_unix_nanos.into(),
                ]),
            )
            .await?;
        let Some((vector_rowid,)) = keyed.into_iter().next() else {
            continue;
        };
        transaction
            .execute_sql(
                "DELETE FROM search_entity_embedding_vectors WHERE rowid = ?1",
                Values(vec![vector_rowid.into()]),
            )
            .await?;
        transaction
            .execute_sql(
                "INSERT INTO search_entity_embedding_vectors (rowid, embedding) VALUES (?1, ?2)",
                Values(vec![
                    vector_rowid.into(),
                    vector_literal(vector.vector).into(),
                ]),
            )
            .await?;
        written += 1;
    }
    Ok(written)
}

/// Deletes each vector whose Search Entity is gone, with its Workspace or
/// alone. Returns the vectors deleted.
pub(super) async fn delete_orphaned_vectors(
    transaction: &mut Transaction<'_>,
) -> Result<u64, Error> {
    match transaction.system() {
        DatabaseSystem::Postgres => {
            transaction
                .execute_sql(
                    "DELETE FROM search_entity_embeddings e \
                     WHERE NOT EXISTS (\
                         SELECT 1 FROM search_entities s \
                         WHERE s.workspace_id = e.workspace_id AND s.doc_id = e.doc_id)",
                    Values(Vec::new()),
                )
                .await
        }
        DatabaseSystem::Sqlite => {
            transaction
                .execute_sql(
                    "DELETE FROM search_entity_embeddings \
                     WHERE NOT EXISTS (\
                         SELECT 1 FROM search_entities s \
                         WHERE s.workspace_id = search_entity_embeddings.workspace_id \
                         AND s.doc_id = search_entity_embeddings.doc_id)",
                    Values(Vec::new()),
                )
                .await?;
            transaction
                .execute_sql(
                    "DELETE FROM search_entity_embedding_vectors \
                     WHERE rowid NOT IN (SELECT vector_rowid FROM search_entity_embeddings)",
                    Values(Vec::new()),
                )
                .await
        }
    }
}

/// The placeholder of the bind at `position`, from 1, in this transaction's
/// backend.
fn bind(transaction: &Transaction<'_>, position: usize) -> String {
    match transaction.system() {
        DatabaseSystem::Postgres => format!("${position}"),
        DatabaseSystem::Sqlite => format!("?{position}"),
    }
}
