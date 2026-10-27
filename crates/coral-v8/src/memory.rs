use std::cell::{Cell, RefCell};
use std::ffi::c_void;
use std::rc::Rc;

use crate::Error;

const MIB: usize = 1024 * 1024;
// Request one fixed increase so V8 can observe termination. This does not bound
// large-object allocations or total process memory.
const TERMINATION_HEADROOM_BYTES: usize = 64 * MIB;

/// Deployment-selected V8 heap budgets, independent of durable output limits.
///
/// Each isolate has its own heap-pressure threshold. Termination requests add
/// 64 MiB of headroom, but these settings are not hard allocation or process caps.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IsolateLimits {
    execution_heap_bytes: usize,
    compiler_heap_bytes: usize,
}

impl IsolateLimits {
    /// Selects heap budgets in bytes for execution and compiler isolates.
    ///
    /// # Errors
    ///
    /// Rejects budgets below 8 MiB, which cannot safely bootstrap an isolate,
    /// and values that leave no representable termination reserve.
    pub fn new(execution_heap_bytes: usize, compiler_heap_bytes: usize) -> Result<Self, Error> {
        for bytes in [execution_heap_bytes, compiler_heap_bytes] {
            if bytes < 8 * MIB || bytes.checked_add(TERMINATION_HEADROOM_BYTES).is_none() {
                return Err(Error::InvalidIsolateLimits);
            }
        }
        Ok(Self {
            execution_heap_bytes,
            compiler_heap_bytes,
        })
    }

    /// Heap budget for each fresh Program isolate.
    #[must_use]
    pub const fn execution_heap_bytes(self) -> usize {
        self.execution_heap_bytes
    }

    /// Heap budget for each warm compiler isolate.
    #[must_use]
    pub const fn compiler_heap_bytes(self) -> usize {
        self.compiler_heap_bytes
    }

    /// Default budgets: 128 MiB per Program and 512 MiB per compiler.
    #[must_use]
    pub const fn defaults() -> Self {
        Self {
            execution_heap_bytes: 128 * MIB,
            compiler_heap_bytes: 512 * MIB,
        }
    }
}

impl Default for IsolateLimits {
    fn default() -> Self {
        Self::defaults()
    }
}

struct HeapLimitState {
    handle: v8::IsolateHandle,
    exceeded: Cell<bool>,
}

thread_local! {
    static ACTIVE_LIMIT: RefCell<Option<Rc<HeapLimitState>>> = const { RefCell::new(None) };
}

struct ActiveLimit(Option<Rc<HeapLimitState>>);

impl Drop for ActiveLimit {
    fn drop(&mut self) {
        ACTIVE_LIMIT.with_borrow_mut(|active| *active = self.0.take());
    }
}

pub(crate) fn with_heap_limit<T>(
    isolate: &mut v8::OwnedIsolate,
    work: impl FnOnce(&mut v8::OwnedIsolate) -> Result<T, Error>,
) -> Result<T, Error> {
    let state = Rc::new(HeapLimitState {
        handle: isolate.thread_safe_handle(),
        exceeded: Cell::new(false),
    });
    let _active = ActiveLimit(ACTIVE_LIMIT.with_borrow_mut(|active| active.replace(state.clone())));
    isolate.add_near_heap_limit_callback(near_heap_limit, std::ptr::null_mut());
    let result = work(isolate);
    isolate.remove_near_heap_limit_callback(near_heap_limit, 0);
    if state.exceeded.get() {
        Err(Error::MemoryLimitExceeded)
    } else {
        result
    }
}

// V8's fatal OOM handler cannot recover: api.cc aborts if it returns. Terminate
// from the earlier near-limit callback instead. No pointer is dereferenced and
// this safe function coerces to V8's unsafe callback type. The initialized TLS
// belongs to the executing thread; the callback allocates no Rust or V8 values.
extern "C" fn near_heap_limit(
    _data: *mut c_void,
    _current_heap_limit: usize,
    initial_heap_limit: usize,
) -> usize {
    ACTIVE_LIMIT.with_borrow(|active| {
        if let Some(state) = active {
            state.exceeded.set(true);
            let _terminated = state.handle.terminate_execution();
        }
    });
    initial_heap_limit.saturating_add(TERMINATION_HEADROOM_BYTES)
}
