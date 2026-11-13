//! The Embedder's statements on Postgres: which Search Entities have no
//! current vector, their text, the vector write and the orphan sweep.
//!
//! Every function joins the caller's transaction; the Embedder owns when
//! each transaction begins, commits or rolls back.

use std::fmt::Write as _;

use coral_db::{Error, Transaction};
use coral_workspace::WorkspaceId;
use sea_query::{Value, Values};

use crate::indexer::IndexError;
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

/// The Search Entities whose vector is missing, of another revision than
/// `revision`, or of another source stamp than the row's, at most `limit`,
/// oldest write first. A row whose field names an older indexer did not
/// write, or did not update with the rest of the row, waits until a
/// reconcile pass indexes it again.
pub(super) async fn stale_entities(
    transaction: &mut Transaction<'_>,
    revision: &str,
    limit: usize,
) -> Result<Vec<StaleEntity>, IndexError> {
    let rows: Vec<(String, String, i64)> = transaction
        .fetch_all_sql(
            "SELECT s.workspace_id, s.doc_id, s.indexed_at_unix_nanos \
             FROM search_entities s \
             LEFT JOIN search_entity_embeddings e \
                 ON e.workspace_id = s.workspace_id AND e.doc_id = s.doc_id \
             WHERE s.field_names_fingerprint = s.fingerprint \
             AND (e.doc_id IS NULL \
                 OR e.embedder_revision <> $1 \
                 OR e.source_indexed_at_unix_nanos <> s.indexed_at_unix_nanos) \
             ORDER BY s.indexed_at_unix_nanos, s.workspace_id, s.doc_id \
             LIMIT $2",
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
    let mut sql = String::from(
        "SELECT doc_id, kind, qualified_name, title, description, provider_name, field_names, \
         indexed_at_unix_nanos \
         FROM search_entities \
         WHERE workspace_id = $1 AND field_names_fingerprint = fingerprint AND doc_id IN (",
    );
    let mut values = vec![Value::from(workspace_id.to_string())];
    for (position, doc_id) in doc_ids.iter().enumerate() {
        if position > 0 {
            sql.push_str(", ");
        }
        write!(sql, "${}", position + 2).expect("writing to a String does not fail");
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

/// Deletes each vector whose Search Entity is gone, with its Workspace or
/// alone. Returns the vectors deleted.
pub(super) async fn delete_orphaned_vectors(
    transaction: &mut Transaction<'_>,
) -> Result<u64, Error> {
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

/// A vector as pgvector reads it: `[0.5,0.25,1]`.
fn vector_literal(vector: &[f32]) -> String {
    let mut literal = String::from("[");
    for (position, value) in vector.iter().enumerate() {
        if position > 0 {
            literal.push(',');
        }
        write!(literal, "{value}").expect("writing to a String does not fail");
    }
    literal.push(']');
    literal
}
