use std::sync::atomic::{AtomicU64, Ordering};

/// How many statements one database handle and its transactions have sent.
///
/// A test reads the count around an operation to bound the statements that
/// operation sends, on either database. The count is kept on every build, so
/// the code a test measures is the code production runs; only the
/// `test-support` feature can read it.
#[derive(Default)]
pub(crate) struct StatementCount(AtomicU64);

impl StatementCount {
    /// Records one statement on its way to the database.
    pub(crate) fn sent(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(feature = "test-support")]
    pub(crate) fn total(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}
