use std::sync::atomic::{AtomicUsize, Ordering};

static COMPILER_THREADS: AtomicUsize = AtomicUsize::new(0);

/// The compiler threads running in this process.
#[must_use]
pub fn compiler_threads() -> usize {
    COMPILER_THREADS.load(Ordering::SeqCst)
}

/// Counts one compiler thread while it runs.
pub(crate) struct CountedThread;

impl CountedThread {
    pub(crate) fn start() -> Self {
        COMPILER_THREADS.fetch_add(1, Ordering::SeqCst);
        Self
    }
}

impl Drop for CountedThread {
    fn drop(&mut self) {
        COMPILER_THREADS.fetch_sub(1, Ordering::SeqCst);
    }
}
