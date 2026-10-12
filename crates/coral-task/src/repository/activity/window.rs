//! Snapshot-local keyset positions for the mixed root query, always newest first.
//! Payloads are loaded only for these bounded Workspace/entity identities.
use crate::Error;
use coral_db::Transaction;
use sea_query::{Alias, ColumnRef, Condition, Expr, ExprTrait as _, Order, Query, SelectStatement};

pub(super) struct PositionIdentity {
    pub workspace: String,
    pub id: String,
}
pub(super) enum PositionWindow {
    Edge,
    Before(PositionIdentity),
    After(PositionIdentity),
    At(PositionIdentity),
    Records(Vec<PositionIdentity>),
}
#[derive(sqlx::FromRow)]
struct Position {
    workspace_id: String,
    id: String,
    sort_key: i64,
}
pub(super) struct PositionPage {
    positions: Vec<Position>,
    pub has_before: bool,
    pub has_after: bool,
    pub anchor_unavailable: bool,
}
impl PositionPage {
    pub(super) fn restrict(
        &self,
        query: &mut SelectStatement,
        workspace: &ColumnRef,
        id: &ColumnRef,
    ) {
        if self.positions.is_empty() {
            query.and_where(Expr::val(false)).limit(0);
            return;
        }
        let mut identities = Condition::any();
        for position in &self.positions {
            identities = identities.add(
                Condition::all()
                    .add(Expr::col(workspace.clone()).eq(position.workspace_id.clone()))
                    .add(Expr::col(id.clone()).eq(position.id.clone())),
            );
        }
        query
            .cond_where(identities)
            .limit(self.positions.len() as u64);
    }
}
fn relative(
    base: &SelectStatement,
    position: &Position,
    after: bool,
    inclusive: bool,
) -> SelectStatement {
    let columns =
        Expr::tuple(["sort_key", "workspace_id", "id"].map(|name| Expr::col(Alias::new(name))));
    let values = Expr::tuple([
        Expr::val(position.sort_key),
        Expr::val(position.workspace_id.clone()),
        Expr::val(position.id.clone()),
    ]);
    base.clone()
        .and_where(if inclusive {
            columns.lte(values)
        } else if after {
            columns.lt(values)
        } else {
            columns.gt(values)
        })
        .to_owned()
}
async fn exists(tx: &mut Transaction<'_>, mut select: SelectStatement) -> Result<bool, Error> {
    select.clear_selects().expr(Expr::val(1_i64)).limit(1);
    Ok(tx.fetch_optional::<(i64,)>(select).await?.is_some())
}
pub(super) async fn select_window(
    tx: &mut Transaction<'_>,
    positions: SelectStatement,
    request: &PositionWindow,
    limit: u16,
) -> Result<PositionPage, Error> {
    if !(1..=100).contains(&limit) {
        return Err(Error::InvalidInput("invalid activity page size".into()));
    }
    let base = Query::select()
        .column(sea_query::Asterisk)
        .from_subquery(positions, Alias::new("history_positions"))
        .to_owned();
    let mut select = match request {
        PositionWindow::Edge => base.clone(),
        PositionWindow::Records(ids) => {
            if ids.len() > usize::from(limit) {
                return Err(Error::InvalidInput(
                    "too many retained activity records".into(),
                ));
            }
            let mut condition = Condition::any().add(Expr::val(false));
            for id in ids {
                condition = condition.add(
                    Condition::all()
                        .add(Expr::col(Alias::new("workspace_id")).eq(id.workspace.clone()))
                        .add(Expr::col(Alias::new("id")).eq(id.id.clone())),
                );
            }
            base.clone().cond_where(condition).to_owned()
        }
        PositionWindow::Before(id) | PositionWindow::After(id) | PositionWindow::At(id) => {
            let exact = base
                .clone()
                .and_where(Expr::col(Alias::new("workspace_id")).eq(id.workspace.clone()))
                .and_where(Expr::col(Alias::new("id")).eq(id.id.clone()))
                .to_owned();
            let Some(anchor) = tx.fetch_optional::<Position>(exact).await? else {
                return Ok(PositionPage {
                    positions: vec![],
                    has_before: false,
                    has_after: false,
                    anchor_unavailable: true,
                });
            };
            relative(
                &base,
                &anchor,
                matches!(request, PositionWindow::After(_)),
                matches!(request, PositionWindow::At(_)),
            )
        }
    };
    let reverse = matches!(request, PositionWindow::Before(_));
    let order = if reverse { Order::Asc } else { Order::Desc };
    select
        .order_by(Alias::new("sort_key"), order.clone())
        .order_by(Alias::new("workspace_id"), order.clone())
        .order_by(Alias::new("id"), order)
        .limit(u64::from(limit));
    let mut positions: Vec<Position> = tx.fetch_all(select).await?;
    if reverse {
        positions.reverse();
    }
    let has_before = if let Some(first) = positions.first() {
        exists(tx, relative(&base, first, false, false)).await?
    } else {
        false
    };
    let has_after = if let Some(last) = positions.last() {
        exists(tx, relative(&base, last, true, false)).await?
    } else {
        false
    };
    Ok(PositionPage {
        positions,
        has_before,
        has_after,
        anchor_unavailable: false,
    })
}
