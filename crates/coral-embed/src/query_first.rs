//! Turns on one value, where a query goes before a document.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};

/// Calls take turns on one value. A query goes before every document that
/// has not started, so a query waits for at most the one document that runs.
/// Documents wait while queries keep coming.
///
/// A query is a live search request; a document is the background
/// Embedder's work, which can wait.
pub(crate) struct QueryFirst<T> {
    value: Mutex<T>,
    /// The queries that wait for the value or run on it. A query adds
    /// itself before it waits for the lock, so a document can see it, and
    /// takes itself off while it holds the lock, so a document that saw it
    /// cannot miss `query_done`.
    queries: AtomicUsize,
    query_done: Condvar,
}

impl<T> QueryFirst<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            value: Mutex::new(value),
            queries: AtomicUsize::new(0),
            query_done: Condvar::new(),
        }
    }

    /// Runs `work` on the value before every document that has not started.
    pub(crate) fn query<R>(&self, work: impl FnOnce(&mut T) -> R) -> R {
        self.queries.fetch_add(1, Ordering::SeqCst);
        let mut turn = QueryTurn {
            gate: self,
            value: self.lock(),
        };
        work(&mut turn.value)
    }

    /// Runs `work` on the value when no query waits or runs.
    pub(crate) fn document<R>(&self, work: impl FnOnce(&mut T) -> R) -> R {
        let mut value = self
            .query_done
            .wait_while(self.lock(), |_| self.queries.load(Ordering::SeqCst) > 0)
            .unwrap_or_else(PoisonError::into_inner);
        work(&mut value)
    }

    // A panic in `work` leaves no state behind in the tokenizer or the
    // session, so a poisoned lock is still safe to use.
    fn lock(&self) -> MutexGuard<'_, T> {
        self.value.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A query's hold on the value. Dropping it takes the query off the count
/// before the lock opens, also when `work` panics.
struct QueryTurn<'a, T> {
    gate: &'a QueryFirst<T>,
    value: MutexGuard<'a, T>,
}

impl<T> Drop for QueryTurn<'_, T> {
    fn drop(&mut self) {
        // `drop` runs before the fields drop, so the lock is still held.
        if self.gate.queries.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.gate.query_done.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use super::*;

    /// The background Embedder takes the lock again right after each
    /// document. A plain mutex lets it, so the query waits for the batch.
    #[test]
    fn a_query_goes_before_the_next_document() {
        let gate = Arc::new(QueryFirst::new(Vec::new()));
        let (started, document_started) = mpsc::channel();
        let (release, document_released) = mpsc::channel::<()>();
        let embedder = thread::spawn({
            let gate = Arc::clone(&gate);
            move || {
                gate.document(|log| {
                    log.push("document 1");
                    started.send(()).unwrap();
                    document_released.recv().unwrap();
                });
                gate.document(|log| log.push("document 2"));
                gate.document(|log| log.push("document 3"));
            }
        });
        document_started.recv().unwrap();
        let query = thread::spawn({
            let gate = Arc::clone(&gate);
            move || gate.query(|log| log.push("query"))
        });
        while gate.queries.load(Ordering::SeqCst) == 0 {
            thread::yield_now();
        }
        release.send(()).unwrap();
        embedder.join().unwrap();
        query.join().unwrap();

        assert_eq!(
            *gate.lock(),
            ["document 1", "query", "document 2", "document 3"]
        );
    }

    #[test]
    fn a_query_that_panics_does_not_hold_documents_back() {
        let gate = Arc::new(QueryFirst::new(0));
        thread::spawn({
            let gate = Arc::clone(&gate);
            move || gate.query(|_| panic!("the query failed"))
        })
        .join()
        .expect_err("the query panics");
        let (done, document_done) = mpsc::channel();
        thread::spawn(move || {
            gate.document(|count| *count += 1);
            done.send(()).unwrap();
        });

        document_done
            .recv_timeout(Duration::from_secs(10))
            .expect("the document runs after the query panicked");
    }
}
