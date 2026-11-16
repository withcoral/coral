//! The Run CPU Budget: the CPU time one Program may use on its own thread.
//!
//! One watchdog thread asks each budgeted isolate for a V8 interrupt every
//! 100 ms. The interrupt runs on the Program's thread, reads that
//! thread's CPU clock, and terminates the isolate once the budget is spent.
//! A Program that reaches its budget between two checks fails when it
//! returns. A thread that waits for a Tool parks, so waiting spends none of
//! it.

use std::cell::RefCell;
use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

use crate::Error;
use crate::runtime::thread_cpu_time;

const CHECK_INTERVAL: Duration = Duration::from_millis(100);

/// Whether this platform can enforce a CPU budget. Windows has no safe call
/// for a thread's CPU clock, so a caller refuses a budget there.
#[must_use]
pub const fn cpu_budget_supported() -> bool {
    cfg!(unix)
}

struct Budgeted {
    handle: v8::IsolateHandle,
    started: Duration,
    budget: Duration,
    /// An interrupt is queued and has not run yet. A parked Program runs no
    /// interrupt, so the watchdog queues at most one for it.
    asked: AtomicBool,
    exceeded: AtomicBool,
}

impl Budgeted {
    fn ask(&self) {
        if !self.asked.swap(true, Ordering::SeqCst) {
            let _queued = self
                .handle
                .request_interrupt(check_budget, std::ptr::null_mut());
        }
    }

    /// Whether the current thread has used the whole budget.
    fn spent(&self) -> bool {
        thread_cpu_time().is_some_and(|now| now.saturating_sub(self.started) >= self.budget)
    }

    fn check(&self) {
        self.asked.store(false, Ordering::SeqCst);
        if self.spent() {
            self.exceeded.store(true, Ordering::SeqCst);
            let _terminated = self.handle.terminate_execution();
        }
    }
}

struct Watchdog {
    budgeted: Mutex<Vec<Arc<Budgeted>>>,
    changed: Condvar,
}

static WATCHDOG: Watchdog = Watchdog {
    budgeted: Mutex::new(Vec::new()),
    changed: Condvar::new(),
};

impl Watchdog {
    fn budgeted(&self) -> MutexGuard<'_, Vec<Arc<Budgeted>>> {
        self.budgeted.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn watch(&self) {
        let mut budgeted = self.budgeted();
        loop {
            while budgeted.is_empty() {
                budgeted = self
                    .changed
                    .wait(budgeted)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            drop(budgeted);
            std::thread::sleep(CHECK_INTERVAL);
            budgeted = self.budgeted();
            for entry in budgeted.iter() {
                entry.ask();
            }
        }
    }

    fn start() -> Result<(), Error> {
        static STARTED: OnceLock<bool> = OnceLock::new();
        let started = *STARTED.get_or_init(|| {
            std::thread::Builder::new()
                .name("coral-v8-cpu-budget".to_owned())
                .spawn(|| WATCHDOG.watch())
                .is_ok()
        });
        if started {
            Ok(())
        } else {
            Err(Error::Initialization(
                "CPU budget watchdog thread could not start".to_owned(),
            ))
        }
    }
}

thread_local! {
    static ACTIVE_BUDGET: RefCell<Option<Arc<Budgeted>>> = const { RefCell::new(None) };
}

/// Registers the executing isolate with the watchdog until dropped.
struct ActiveBudget {
    entry: Arc<Budgeted>,
    previous: Option<Arc<Budgeted>>,
}

impl ActiveBudget {
    fn install(entry: Arc<Budgeted>) -> Self {
        let previous = ACTIVE_BUDGET.with_borrow_mut(|active| active.replace(entry.clone()));
        let mut budgeted = WATCHDOG.budgeted();
        budgeted.push(entry.clone());
        if budgeted.len() == 1 {
            WATCHDOG.changed.notify_one();
        }
        Self { entry, previous }
    }
}

impl Drop for ActiveBudget {
    fn drop(&mut self) {
        WATCHDOG
            .budgeted()
            .retain(|entry| !Arc::ptr_eq(entry, &self.entry));
        ACTIVE_BUDGET.with_borrow_mut(|active| *active = self.previous.take());
    }
}

/// Runs `work` under `budget` of the current thread's CPU time, counted from
/// `started`. Over it, the isolate is terminated, which Program JavaScript
/// cannot catch, and the result is [`Error::CpuLimitExceeded`]; a Program
/// that reaches the budget and returns before a check fails the same way.
/// `None` for
/// either runs `work` unbudgeted, as on a platform without
/// [`cpu_budget_supported`].
pub(crate) fn with_cpu_budget<T>(
    isolate: &v8::IsolateHandle,
    budget: Option<Duration>,
    started: Option<Duration>,
    work: impl FnOnce() -> Result<T, Error>,
) -> Result<T, Error> {
    let (Some(budget), Some(started)) = (budget, started) else {
        return work();
    };
    Watchdog::start()?;
    let entry = Arc::new(Budgeted {
        handle: isolate.clone(),
        started,
        budget,
        asked: AtomicBool::new(false),
        exceeded: AtomicBool::new(false),
    });
    let active = ActiveBudget::install(entry.clone());
    let result = work();
    drop(active);
    // A Program that crosses its budget between two checks has spent it too.
    if entry.exceeded.load(Ordering::SeqCst) || entry.spent() {
        Err(Error::CpuLimitExceeded)
    } else {
        result
    }
}

/// Whether the Program executing on this thread spent its CPU budget. Its
/// isolate is terminated, so no more of its JavaScript may run.
pub(crate) fn budget_exceeded() -> bool {
    ACTIVE_BUDGET.with_borrow(|active| {
        active
            .as_ref()
            .is_some_and(|entry| entry.exceeded.load(Ordering::SeqCst))
    })
}

// V8 runs an interrupt on the thread that executes the isolate, so the
// thread-local entry is that isolate's. No pointer is dereferenced, and this
// safe function coerces to V8's unsafe callback type.
extern "C" fn check_budget(_isolate: v8::UnsafeRawIsolatePtr, _data: *mut c_void) {
    ACTIVE_BUDGET.with_borrow(|active| {
        if let Some(entry) = active {
            entry.check();
        }
    });
}
