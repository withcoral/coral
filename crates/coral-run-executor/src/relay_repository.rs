//! The relayed compile rows: `compile_requests(id, requester, call,
//! created_at, deadline, taken_by, answer)`. Times are Unix nanoseconds. A
//! row's call is an encoded `Compile` and its answer an encoded `Compiled`;
//! a probe has no call, and its answer is empty.

use coral_db::{Database, RowLock};
use prost::Message as _;
use sea_query::{Expr, ExprTrait as _, Iden, Order, Query, UpdateStatement};

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

/// What a row asks for.
pub(crate) enum Request {
    /// Room on any Coral Instance: a probe.
    Room,
    Call(Call),
    /// A call no message decodes.
    Unreadable,
}

/// A row a pickup took, with its requester's deadline.
pub(crate) struct Taken {
    pub(crate) id: String,
    pub(crate) request: Request,
    pub(crate) deadline: i64,
}

/// A row's answer.
pub(crate) enum Answered {
    /// A probe's: some Coral Instance had room.
    Room,
    Call(Answer),
}

/// Where a requester's row stands.
pub(crate) enum Status {
    /// No answer yet; `taken` once a pickup sent it down a stream.
    Waiting {
        taken: bool,
    },
    Answered(Result<Answered, ProtocolError>),
    /// Deleted: its deadline passed.
    Gone,
}

/// Writes the request `id` of Coral Instance `requester` for `call`, none
/// for a probe, which wakes the pickups.
pub(crate) async fn request(
    database: &Database,
    id: &str,
    requester: &str,
    call: Option<&Call>,
    now: i64,
    deadline: i64,
) -> Result<(), coral_db::Error> {
    let call = call.map(|call| {
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
            Expr::val(id),
            Expr::val(requester),
            Expr::val(call),
            Expr::val(now),
            Expr::val(deadline),
        ])
        .to_owned();
    let mut transaction = database.begin().await?;
    transaction.execute_change(insert).await?;
    transaction.commit().await
}

/// Whether a pickup has anything to do at `now`: an untaken row, or a row
/// past its deadline. One read, so an idle pickup takes no write lock.
pub(crate) async fn pending(database: &Database, now: i64) -> Result<bool, coral_db::Error> {
    let pending = Query::select()
        .column(CompileRequests::Id)
        .from(CompileRequests::Table)
        .cond_where(
            Expr::col(CompileRequests::TakenBy)
                .is_null()
                .or(Expr::col(CompileRequests::Deadline).lte(now)),
        )
        .limit(1)
        .to_owned();
    Ok(database
        .fetch_optional::<(String,)>(pending)
        .await?
        .is_some())
}

/// Takes, for Coral Instance `taker`, the oldest untaken row whose deadline
/// is more than `reserve` past `now`, passing over rows another taker
/// holds. It first deletes the rows past their deadline: their requesters
/// stopped waiting.
pub(crate) async fn take_oldest(
    database: &Database,
    taker: &str,
    now: i64,
    reserve: i64,
) -> Result<Option<Taken>, coral_db::Error> {
    let mut transaction = database.begin().await?;
    // Nobody waits on these, so the delete wakes nobody.
    transaction
        .execute(
            Query::delete()
                .from_table(CompileRequests::Table)
                .and_where(Expr::col(CompileRequests::Deadline).lte(now))
                .to_owned(),
        )
        .await?;
    let oldest = Query::select()
        .columns([
            CompileRequests::Id,
            CompileRequests::Call,
            CompileRequests::Deadline,
        ])
        .from(CompileRequests::Table)
        .and_where(Expr::col(CompileRequests::TakenBy).is_null())
        .and_where(Expr::col(CompileRequests::Deadline).gt(now.saturating_add(reserve)))
        .order_by(CompileRequests::CreatedAt, Order::Asc)
        .order_by(CompileRequests::Id, Order::Asc)
        .limit(1)
        .to_owned();
    let Some((id, call, deadline)) = transaction
        .fetch_optional_locked::<(String, Option<Vec<u8>>, i64)>(oldest, RowLock::SkipLocked)
        .await?
    else {
        transaction.commit().await?;
        return Ok(None);
    };
    // The take mark keeps the row from other takers; nobody waits on it.
    transaction
        .execute(
            Query::update()
                .table(CompileRequests::Table)
                .value(CompileRequests::TakenBy, taker)
                .and_where(Expr::col(CompileRequests::Id).eq(id.as_str()))
                .to_owned(),
        )
        .await?;
    transaction.commit().await?;
    let request = match call {
        None => Request::Room,
        Some(call) => Compile::decode(call.as_slice())
            .ok()
            .and_then(|compile| compile.call)
            .map_or(Request::Unreadable, Request::Call),
    };
    Ok(Some(Taken {
        id,
        request,
        deadline,
    }))
}

/// Gives a taken, unanswered row back, which wakes the pickups.
pub(crate) async fn release(database: &Database, id: &str) -> Result<(), coral_db::Error> {
    let release = Query::update()
        .table(CompileRequests::Table)
        .value(CompileRequests::TakenBy, Option::<String>::None)
        .and_where(Expr::col(CompileRequests::Id).eq(id))
        .and_where(Expr::col(CompileRequests::Answer).is_null())
        .to_owned();
    change(database, release).await
}

/// Writes the answer to row `id`, which wakes its requester.
pub(crate) async fn answer(
    database: &Database,
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
    change(database, write).await
}

/// Reads where row `id` stands.
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
        Some((Some(answer), _)) if answer.is_empty() => Status::Answered(Ok(Answered::Room)),
        Some((Some(answer), _)) => Status::Answered(
            Compiled::decode(answer.as_slice())
                .ok()
                .and_then(|compiled| compiled.answer)
                .map(Answered::Call)
                .ok_or(ProtocolError),
        ),
    })
}

/// Deletes row `id` once its requester stops waiting. Nobody waits on that.
pub(crate) async fn withdraw(database: &Database, id: &str) -> Result<(), coral_db::Error> {
    database
        .execute(
            Query::delete()
                .from_table(CompileRequests::Table)
                .and_where(Expr::col(CompileRequests::Id).eq(id))
                .to_owned(),
        )
        .await
        .map(drop)
}

/// Commits `statement` as an observable change, which wakes the waiters.
async fn change(database: &Database, statement: UpdateStatement) -> Result<(), coral_db::Error> {
    let mut transaction = database.begin().await?;
    transaction.execute_change(statement).await?;
    transaction.commit().await
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
