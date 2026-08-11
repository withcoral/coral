use sea_query::{Condition, Expr, ExprTrait, OnConflict, Order, Query};

use crate::state::db::clock::TraceSearchResponseRetentionBounds;
use crate::state::db::schema::TraceSearchResponses;
use crate::state::db::{CoralTx, DbError, DbSession};

#[derive(Clone, PartialEq, Eq, sqlx::FromRow)]
pub(in crate::state::db) struct TraceSearchResponseRow {
    pub(in crate::state::db) response_proto: Option<Vec<u8>>,
    pub(in crate::state::db) oversized_bytes: Option<i64>,
}

pub(crate) struct TraceSearchResponsesRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> TraceSearchResponsesRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "next stack layer wires trace reads")
    )]
    pub(in crate::state::db) async fn get(
        &mut self,
        workspace_id: &str,
        trace_id: &str,
        search_span_id: &str,
        retention_bounds: TraceSearchResponseRetentionBounds,
    ) -> Result<Option<TraceSearchResponseRow>, DbError> {
        let statement = Query::select()
            .columns([
                TraceSearchResponses::ResponseProto,
                TraceSearchResponses::OversizedBytes,
            ])
            .from(TraceSearchResponses::Table)
            .and_where(Expr::col(TraceSearchResponses::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(TraceSearchResponses::TraceId).eq(trace_id))
            .and_where(Expr::col(TraceSearchResponses::SearchSpanId).eq(search_span_id))
            .and_where(
                Expr::col(TraceSearchResponses::RecordedAtUnixNanos)
                    .gte(retention_bounds.oldest_inclusive_unix_nanos),
            )
            .and_where(
                Expr::col(TraceSearchResponses::RecordedAtUnixNanos)
                    .lte(retention_bounds.newest_inclusive_unix_nanos),
            )
            .to_owned();
        self.session.fetch_optional(statement).await
    }

    pub(crate) async fn next_out_of_retention_workspace_id(
        &mut self,
        retention_bounds: TraceSearchResponseRetentionBounds,
        after_workspace_id: Option<&str>,
    ) -> Result<Option<String>, DbError> {
        let statement =
            Query::select()
                .column(TraceSearchResponses::WorkspaceId)
                .from(TraceSearchResponses::Table)
                .cond_where(
                    Condition::any()
                        .add(
                            Expr::col(TraceSearchResponses::RecordedAtUnixNanos)
                                .lt(retention_bounds.oldest_inclusive_unix_nanos),
                        )
                        .add(
                            Expr::col(TraceSearchResponses::RecordedAtUnixNanos)
                                .gt(retention_bounds.newest_inclusive_unix_nanos),
                        ),
                )
                .and_where_option(after_workspace_id.map(|workspace_id| {
                    Expr::col(TraceSearchResponses::WorkspaceId).gt(workspace_id)
                }))
                .order_by(TraceSearchResponses::WorkspaceId, Order::Asc)
                .limit(1)
                .to_owned();
        let row: Option<(String,)> = self.session.fetch_optional(statement).await?;
        Ok(row.map(|(workspace_id,)| workspace_id))
    }
}

impl TraceSearchResponsesRepo<'_, CoralTx<'_>> {
    pub(crate) async fn insert_first_write_wins(
        &mut self,
        workspace_id: &str,
        trace_id: &str,
        search_span_id: &str,
        recorded_at_unix_nanos: i64,
        response_proto: Option<Vec<u8>>,
        oversized_bytes: Option<i64>,
    ) -> Result<bool, DbError> {
        let statement = Query::insert()
            .into_table(TraceSearchResponses::Table)
            .columns([
                TraceSearchResponses::WorkspaceId,
                TraceSearchResponses::TraceId,
                TraceSearchResponses::SearchSpanId,
                TraceSearchResponses::RecordedAtUnixNanos,
                TraceSearchResponses::ResponseProto,
                TraceSearchResponses::OversizedBytes,
            ])
            .values_panic([
                Expr::val(workspace_id.to_string()),
                Expr::val(trace_id.to_string()),
                Expr::val(search_span_id.to_string()),
                Expr::val(recorded_at_unix_nanos),
                Expr::val(response_proto),
                Expr::val(oversized_bytes),
            ])
            .on_conflict(
                OnConflict::columns([
                    TraceSearchResponses::WorkspaceId,
                    TraceSearchResponses::TraceId,
                    TraceSearchResponses::SearchSpanId,
                ])
                .do_nothing()
                .to_owned(),
            )
            .to_owned();
        Ok(self.session.execute_rows_affected(statement).await? == 1)
    }

    pub(crate) async fn delete_out_of_retention_batch(
        &mut self,
        workspace_id: &str,
        retention_bounds: TraceSearchResponseRetentionBounds,
        max_rows: u64,
    ) -> Result<u64, DbError> {
        let expired_keys = Query::select()
            .columns([
                TraceSearchResponses::TraceId,
                TraceSearchResponses::SearchSpanId,
            ])
            .from(TraceSearchResponses::Table)
            .and_where(Expr::col(TraceSearchResponses::WorkspaceId).eq(workspace_id))
            .cond_where(
                Condition::any()
                    .add(
                        Expr::col(TraceSearchResponses::RecordedAtUnixNanos)
                            .lt(retention_bounds.oldest_inclusive_unix_nanos),
                    )
                    .add(
                        Expr::col(TraceSearchResponses::RecordedAtUnixNanos)
                            .gt(retention_bounds.newest_inclusive_unix_nanos),
                    ),
            )
            .order_by(TraceSearchResponses::RecordedAtUnixNanos, Order::Asc)
            .order_by(TraceSearchResponses::TraceId, Order::Asc)
            .order_by(TraceSearchResponses::SearchSpanId, Order::Asc)
            .limit(max_rows)
            .to_owned();
        let statement = Query::delete()
            .from_table(TraceSearchResponses::Table)
            .and_where(Expr::col(TraceSearchResponses::WorkspaceId).eq(workspace_id))
            .and_where(
                Expr::tuple([
                    Expr::col(TraceSearchResponses::TraceId),
                    Expr::col(TraceSearchResponses::SearchSpanId),
                ])
                .in_subquery(expired_keys),
            )
            .to_owned();
        self.session.execute_rows_affected(statement).await
    }
}
