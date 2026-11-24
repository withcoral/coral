//! Run stream loop test doubles: row work that only records how it was
//! settled.
use crate::tests::*;

use crate::owner::ChunkWork;
use coral_db::Transaction;

/// How a [`ProbeWork`] was settled, once it was.
pub(crate) type Settled = Arc<Mutex<Option<Result<(), Error>>>>;

/// Row work that only records how it was settled.
#[derive(Debug)]
pub(crate) struct ProbeWork {
    settled: Settled,
}

#[async_trait]
impl ChunkWork for ProbeWork {
    async fn run(
        &mut self,
        _transaction: &mut Transaction<'_>,
        _run: &crate::lifecycle::RunRecord,
        _now_unix_nanos: i64,
    ) -> Result<Vec<crate::run_repository::OwnedEvent>, Error> {
        Ok(Vec::new())
    }

    fn settle(self: Box<Self>, recorded: Result<(), Error>) {
        *self.settled.lock().expect("settle lock") = Some(recorded);
    }
}

pub(crate) fn probe_work() -> (Box<dyn ChunkWork>, Settled) {
    let settled = Arc::new(Mutex::new(None));
    (
        Box::new(ProbeWork {
            settled: settled.clone(),
        }),
        settled,
    )
}
