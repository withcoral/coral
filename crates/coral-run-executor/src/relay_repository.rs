//! The relayed compile rows: `compile_requests(id, requester, call,
//! created_at, deadline, taken_by, answer)`. Times are Unix nanoseconds. A
//! row's call is an encoded `Compile` and its answer an encoded `Compiled`;
//! a probe has no call, and its answer is empty. Writes join the caller's
//! transaction.

use coral_db::{Database, RowLock, Transaction};
use prost::Message as _;
use sea_query::{Expr, ExprTrait as _, Iden, Order, Query};

use crate::compile_wire::ProtocolError;
use crate::wire::v1::compile::Call;
use crate::wire::v1::compiled::Answer;
use crate::wire::v1::{Compile, Compiled};

#[derive(Iden)]
enum CompileRequests {
    Table,
    Id,
    Requester,
    Call,
    CreatedAt,
    Deadline,
    TakenBy,
    Answer,
}

/// A row a requester writes.
pub(crate) struct NewRequest<'a> {
    pub(crate) id: &'a str,
    /// The Coral Instance that waits for the answer.
    pub(crate) requester: &'a str,
    /// None for a probe.
    pub(crate) call: Option<&'a Call>,
    pub(crate) created_at: i64,
    /// The caller's deadline: the requester waits to it once a pickup took
    /// the row, and a little less while nobody has.
    pub(crate) deadline: i64,
}

/// What a row asks for.
pub(crate) enum Request {
    /// Room on any Coral Instance: a probe.
    Room,
    Call(Call),
    /// A call no message decodes.
    Unreadable,
}

/// A row a pickup took.
pub(crate) struct Taken {
    pub(crate) id: String,
    pub(crate) requester: String,
    pub(crate) request: Request,
    pub(crate) deadline: i64,
}

/// A row's answer.
pub(crate) enum Answered {
    /// A probe's: the Coral Instance that took it had room.
    Room,
    Call(Answer),
}

/// Where a requester's row stands.
pub(crate) enum Status {
    /// No answer yet; `taken` while a pickup holds it. A pickup that gives
    /// it back clears it.
    Waiting { taken: bool },
    /// Answered by the Coral Instance `by`.
    Answered {
        answer: Result<Answered, ProtocolError>,
        by: Option<String>,
    },
    /// Deleted: a pickup found it left over past its deadline.
    Gone,
}

/// Writes `row`, which wakes the pickups.
pub(crate) async fn request(
    transaction: &mut Transaction<'_>,
    row: NewRequest<'_>,
) -> Result<(), coral_db::Error> {
    let call = row.call.map(|call| {
        Compile {
            number: 0,
            time_left_ms: 0,
            call: Some(call.clone()),
        }
        .encode_to_vec()
    });
    let insert = Query::insert()
        .into_table(CompileRequests::Table)
        .columns([
            CompileRequests::Id,
            CompileRequests::Requester,
            CompileRequests::Call,
            CompileRequests::CreatedAt,
            CompileRequests::Deadline,
        ])
        .values_panic([
            Expr::val(row.id),
            Expr::val(row.requester),
            Expr::val(call),
            Expr::val(row.created_at),
            Expr::val(row.deadline),
        ])
        .to_owned();
    transaction.execute_change(insert).await.map(drop)
}

/// Whether [`take_oldest`] would take a row for `take`, or a row has a
/// deadline at or before `expired`. One read.
pub(crate) async fn pending(
    database: &Database,
    take: &Take<'_>,
    expired: i64,
) -> Result<bool, coral_db::Error> {
    let pending = Query::select()
        .column(CompileRequests::Id)
        .from(CompileRequests::Table)
        .cond_where(takeable(take).add(Expr::col(CompileRequests::Deadline).lte(expired)))
        .limit(1)
        .to_owned();
    Ok(database
        .fetch_optional::<(String,)>(pending)
        .await?
        .is_some())
}

/// Deletes the rows whose deadline is at or before `expired`. Nobody waits
/// on them, so this wakes nobody.
pub(crate) async fn delete_past(
    transaction: &mut Transaction<'_>,
    expired: i64,
) -> Result<(), coral_db::Error> {
    let delete = Query::delete()
        .from_table(CompileRequests::Table)
        .and_where(Expr::col(CompileRequests::Deadline).lte(expired))
        .to_owned();
    transaction.execute(delete).await.map(drop)
}

/// Which row a pickup takes.
pub(crate) struct Take<'a> {
    /// The Coral Instance that takes it.
    pub(crate) taker: &'a str,
    /// The row's deadline must be after this.
    pub(crate) deadline_after: i64,
    /// Pass over probes.
    pub(crate) calls_only: bool,
}

/// The rows `take` takes, or any of them: untaken, with a deadline after
/// its bound, and calls only when it says so.
fn takeable(take: &Take<'_>) -> sea_query::Condition {
    let condition = sea_query::Condition::all()
        .add(Expr::col(CompileRequests::TakenBy).is_null())
        .add(Expr::col(CompileRequests::Deadline).gt(take.deadline_after));
    let condition = if take.calls_only {
        condition.add(Expr::col(CompileRequests::Call).is_not_null())
    } else {
        condition
    };
    sea_query::Condition::any().add(condition)
}

/// Takes the oldest untaken row that `take` names, passing over rows
/// another taker holds, and marks it taken.
pub(crate) async fn take_oldest(
    transaction: &mut Transaction<'_>,
    take: Take<'_>,
) -> Result<Option<Taken>, coral_db::Error> {
    let oldest = Query::select()
        .columns([
            CompileRequests::Id,
            CompileRequests::Requester,
            CompileRequests::Call,
            CompileRequests::Deadline,
        ])
        .from(CompileRequests::Table)
        .cond_where(takeable(&take))
        .order_by(CompileRequests::CreatedAt, Order::Asc)
        .order_by(CompileRequests::Id, Order::Asc)
        .limit(1)
        .to_owned();
    let Some((id, requester, call, deadline)) = transaction
        .fetch_optional_locked::<(String, String, Option<Vec<u8>>, i64)>(
            oldest,
            RowLock::SkipLocked,
        )
        .await?
    else {
        return Ok(None);
    };
    // The take mark keeps the row from other takers, and wakes the
    // requester, which waits longer once a pickup took its row.
    let mark = Query::update()
        .table(CompileRequests::Table)
        .value(CompileRequests::TakenBy, take.taker)
        .and_where(Expr::col(CompileRequests::Id).eq(id.as_str()))
        .to_owned();
    transaction.execute_change(mark).await?;
    let request = match call {
        None => Request::Room,
        Some(call) => Compile::decode(call.as_slice())
            .ok()
            .and_then(|compile| compile.call)
            .map_or(Request::Unreadable, Request::Call),
    };
    Ok(Some(Taken {
        id,
        requester,
        request,
        deadline,
    }))
}

/// Gives a taken, unanswered row back, which wakes the pickups.
pub(crate) async fn release(
    transaction: &mut Transaction<'_>,
    id: &str,
) -> Result<(), coral_db::Error> {
    let release = Query::update()
        .table(CompileRequests::Table)
        .value(CompileRequests::TakenBy, Option::<String>::None)
        .and_where(Expr::col(CompileRequests::Id).eq(id))
        .and_where(Expr::col(CompileRequests::Answer).is_null())
        .to_owned();
    transaction.execute_change(release).await.map(drop)
}

/// Writes the answer to row `id`, which wakes its requester.
pub(crate) async fn answer(
    transaction: &mut Transaction<'_>,
    id: &str,
    answered: Answered,
) -> Result<(), coral_db::Error> {
    let answer = match answered {
        Answered::Room => Vec::new(),
        Answered::Call(answer) => Compiled {
            number: 0,
            answer: Some(answer),
        }
        .encode_to_vec(),
    };
    let write = Query::update()
        .table(CompileRequests::Table)
        .value(CompileRequests::Answer, answer)
        .and_where(Expr::col(CompileRequests::Id).eq(id))
        .to_owned();
    transaction.execute_change(write).await.map(drop)
}

/// Reads where row `id` stands. One read.
pub(crate) async fn status(database: &Database, id: &str) -> Result<Status, coral_db::Error> {
    let read = Query::select()
        .columns([CompileRequests::Answer, CompileRequests::TakenBy])
        .from(CompileRequests::Table)
        .and_where(Expr::col(CompileRequests::Id).eq(id))
        .to_owned();
    let row = database
        .fetch_optional::<(Option<Vec<u8>>, Option<String>)>(read)
        .await?;
    Ok(match row {
        None => Status::Gone,
        Some((None, taken_by)) => Status::Waiting {
            taken: taken_by.is_some(),
        },
        Some((Some(answer), by)) => Status::Answered {
            answer: if answer.is_empty() {
                Ok(Answered::Room)
            } else {
                Compiled::decode(answer.as_slice())
                    .ok()
                    .and_then(|compiled| compiled.answer)
                    .map(Answered::Call)
                    .ok_or(ProtocolError)
            },
            by,
        },
    })
}

/// Deletes row `id`. Nobody waits on it once its requester stops waiting.
pub(crate) async fn withdraw(
    transaction: &mut Transaction<'_>,
    id: &str,
) -> Result<(), coral_db::Error> {
    let delete = Query::delete()
        .from_table(CompileRequests::Table)
        .and_where(Expr::col(CompileRequests::Id).eq(id))
        .to_owned();
    transaction.execute(delete).await.map(drop)
}

/// The rows left, for tests that check a row is gone.
#[cfg(test)]
pub(crate) async fn count(database: &Database) -> Result<i64, coral_db::Error> {
    let count = Query::select()
        .expr(Expr::col(CompileRequests::Id).count())
        .from(CompileRequests::Table)
        .to_owned();
    let row = database.fetch_optional::<(i64,)>(count).await?;
    Ok(row.map_or(0, |(count,)| count))
}
