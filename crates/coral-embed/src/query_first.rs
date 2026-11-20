//! Turns on a set of values, where a query goes before a document.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};

/// Calls take turns on a set of values, one call on each value at a time. A
/// query goes before every document that has not started, so a query waits
/// for at most the first running document to end. Documents wait while
/// queries keep coming.
///
/// A query is a live search request; a document is the background
/// Embedder's work, which can wait.
pub(crate) struct QueryFirst<T> {
    turns: Mutex<Turns<T>>,
    /// The queries that wait for a value or run on one. A document starts
    /// only while there is none. A query takes itself off while it holds
    /// `turns`, so a document that saw it cannot miss `turn_ended`.
    queries: AtomicUsize,
    /// A call signals it each time it gives its value back.
    turn_ended: Condvar,
}

struct Turns<T> {
    /// The values that no call holds.
    free: Vec<T>,
    /// The calls that wait for their turn, so a test knows when one waits.
    #[cfg(test)]
    waiting: usize,
}

impl<T> QueryFirst<T> {
    /// Calls take turns on `values`.
    ///
    /// # Panics
    ///
    /// When `values` is empty: every call would wait forever.
    pub(crate) fn new(values: Vec<T>) -> Self {
        assert!(!values.is_empty(), "calls need a value to take turns on");
        Self {
            turns: Mutex::new(Turns {
                free: values,
                #[cfg(test)]
                waiting: 0,
            }),
            queries: AtomicUsize::new(0),
            turn_ended: Condvar::new(),
        }
    }

    /// Runs `work` on a value before every document that has not started.
    pub(crate) fn query<R>(&self, work: impl FnOnce(&mut T) -> R) -> R {
        // Counted before the lock: a call that gives its value back holds the
        // lock, and the next document could take it first.
        self.queries.fetch_add(1, Ordering::SeqCst);
        let turns = self.turns();
        let mut turn = Turn {
            value: Some(self.take(turns, || true)),
            gate: self,
            query: true,
        };
        work(turn.value())
    }

    /// Runs `work` on a value when no query waits or runs.
    pub(crate) fn document<R>(&self, work: impl FnOnce(&mut T) -> R) -> R {
        let mut turn = Turn {
            value: Some(self.take(self.turns(), || self.queries.load(Ordering::SeqCst) == 0)),
            gate: self,
            query: false,
        };
        work(turn.value())
    }

    /// Takes a free value once `may_start` allows it. Checks again after
    /// each wake-up: a `Condvar` may wake a caller at any time, and a query
    /// can come while a document waits.
    fn take(&self, mut turns: MutexGuard<'_, Turns<T>>, may_start: impl Fn() -> bool) -> T {
        loop {
            if may_start()
                && let Some(value) = turns.free.pop()
            {
                return value;
            }
            #[cfg(test)]
            {
                turns.waiting += 1;
            }
            turns = self
                .turn_ended
                .wait(turns)
                .unwrap_or_else(PoisonError::into_inner);
            #[cfg(test)]
            {
                turns.waiting -= 1;
            }
        }
    }

    // A caller's work never runs while this lock is held, so no panic in it
    // poisons the lock.
    fn turns(&self) -> MutexGuard<'_, Turns<T>> {
        self.turns.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A call's hold on one value. Dropping it gives the value back and takes a
/// query off the count, also when `work` panics: a panic leaves no state
/// behind in the tokenizer or the session, so the value stays usable.
struct Turn<'a, T> {
    /// `Some` until the turn drops.
    value: Option<T>,
    gate: &'a QueryFirst<T>,
    query: bool,
}

impl<T> Turn<'_, T> {
    fn value(&mut self) -> &mut T {
        self.value
            .as_mut()
            .unwrap_or_else(|| unreachable!("a turn holds its value until it drops"))
    }
}

impl<T> Drop for Turn<'_, T> {
    fn drop(&mut self) {
        let mut turns = self.gate.turns();
        if let Some(value) = self.value.take() {
            turns.free.push(value);
        }
        if self.query {
            self.gate.queries.fetch_sub(1, Ordering::SeqCst);
        }
        drop(turns);
        self.gate.turn_ended.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;

    /// The background Embedder takes the lock again right after each
    /// document. A plain mutex lets it, so the query waits for the batch.
    #[test]
    fn a_query_goes_before_the_next_document() {
        let gate = Arc::new(QueryFirst::new(vec![Vec::new()]));
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
            gate.turns().free,
            [["document 1", "query", "document 2", "document 3"]]
        );
    }

    /// A call that gives its value back holds the lock for a moment, and the
    /// next document can take the lock first. A query that comes in that
    /// moment counts itself before it waits for the lock, so the next
    /// document sees it and waits.
    #[test]
    fn a_query_that_waits_for_the_lock_goes_before_the_next_document() {
        let gate = Arc::new(QueryFirst::new(vec![Vec::new()]));
        let held = gate.turns();
        let query = thread::spawn({
            let gate = Arc::clone(&gate);
            move || gate.query(|log| log.push("query"))
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        while gate.queries.load(Ordering::SeqCst) == 0 {
            assert!(
                Instant::now() < deadline,
                "the query counts itself before it waits for the lock"
            );
            thread::yield_now();
        }
        drop(held);

        gate.document(|log| log.push("document"));
        query.join().unwrap();

        assert_eq!(gate.turns().free, [["query", "document"]]);
    }

    /// Each value runs one document. With two values, a second document
    /// starts while the first still runs.
    #[test]
    fn documents_run_at_once_on_as_many_values() {
        let gate = Arc::new(QueryFirst::new(vec![(), ()]));
        let (release, first_released) = mpsc::channel::<()>();
        let (started, first_started) = mpsc::channel();
        let first = thread::spawn({
            let gate = Arc::clone(&gate);
            move || {
                gate.document(|()| {
                    started.send(()).unwrap();
                    first_released.recv().unwrap();
                });
            }
        });
        first_started.recv().unwrap();
        let (done, second_done) = mpsc::channel();
        thread::spawn({
            let gate = Arc::clone(&gate);
            move || {
                gate.document(|()| {});
                done.send(()).unwrap();
            }
        });

        second_done
            .recv_timeout(Duration::from_secs(10))
            .expect("the second document runs while the first holds its value");
        release.send(()).unwrap();
        first.join().unwrap();
    }

    /// Every value is busy, a document waits, and a query comes. The first
    /// value to come free goes to the query, and the document waits on.
    #[test]
    fn a_query_goes_before_a_waiting_document_when_every_value_is_busy() {
        let gate = Arc::new(QueryFirst::new(vec![(), ()]));
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut holds = Vec::new();
        let mut running = Vec::new();
        for name in ["document 1", "document 2"] {
            let (release, released) = mpsc::channel::<()>();
            let (started, has_started) = mpsc::channel();
            running.push(thread::spawn({
                let gate = Arc::clone(&gate);
                let order = Arc::clone(&order);
                move || {
                    gate.document(|()| {
                        order.lock().unwrap().push(name);
                        started.send(()).unwrap();
                        released.recv().unwrap();
                    });
                }
            }));
            has_started.recv().unwrap();
            holds.push(release);
        }
        let waiting = thread::spawn({
            let gate = Arc::clone(&gate);
            let order = Arc::clone(&order);
            move || gate.document(|()| order.lock().unwrap().push("document 3"))
        });
        while gate.turns().waiting == 0 {
            thread::yield_now();
        }
        let query = thread::spawn({
            let gate = Arc::clone(&gate);
            let order = Arc::clone(&order);
            move || gate.query(|()| order.lock().unwrap().push("query"))
        });
        while gate.queries.load(Ordering::SeqCst) == 0 {
            thread::yield_now();
        }

        let [first, second] = <[_; 2]>::try_from(holds).unwrap();
        first.send(()).unwrap();
        query.join().unwrap();
        second.send(()).unwrap();
        waiting.join().unwrap();
        for document in running {
            document.join().unwrap();
        }

        assert_eq!(
            *order.lock().unwrap(),
            ["document 1", "document 2", "query", "document 3"]
        );
    }

    #[test]
    fn a_query_that_panics_does_not_hold_documents_back() {
        let gate = Arc::new(QueryFirst::new(vec![0]));
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

    /// A document can wake while a query waits: a `Condvar` may wake it at
    /// any time, and a new query can come after the last one ended. The
    /// document must check again. A document that panicked before gives its
    /// value back, so the last document runs on it.
    #[test]
    fn a_woken_document_waits_again_while_a_query_waits() {
        let gate = Arc::new(QueryFirst::new(vec![0]));
        thread::spawn({
            let gate = Arc::clone(&gate);
            move || gate.document(|_| panic!("a document failed"))
        })
        .join()
        .expect_err("the document panics");
        gate.queries.fetch_add(1, Ordering::SeqCst);
        let (done, document_done) = mpsc::channel();
        thread::spawn({
            let gate = Arc::clone(&gate);
            move || {
                gate.document(|count| *count += 1);
                done.send(()).unwrap();
            }
        });
        for _ in 0..50 {
            gate.turn_ended.notify_all();
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(
            document_done.try_recv(),
            Err(mpsc::TryRecvError::Empty),
            "the document ran while a query waited"
        );

        // As a query does when it ends: off the count while it holds the lock.
        {
            let _held = gate.turns();
            gate.queries.fetch_sub(1, Ordering::SeqCst);
        }
        gate.turn_ended.notify_all();
        document_done
            .recv_timeout(Duration::from_secs(10))
            .expect("the document runs once no query waits");
    }
}
