//! Thread CPU samples bracket each synchronous poll, excluding pending time: a
//! resumed future may run on a different thread. Unsupported clocks omit CPU.
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

pub(super) struct CpuTime {
    total: Mutex<Option<Duration>>,
    enabled: bool,
}

impl CpuTime {
    pub(super) fn new(enabled: bool) -> Self {
        Self {
            total: Mutex::new(if enabled {
                thread_cpu_time().map(|_| Duration::ZERO)
            } else {
                None
            }),
            enabled,
        }
    }

    pub(super) fn elapsed(&self) -> Option<Duration> {
        if !self.enabled {
            return None;
        }
        *self
            .total
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn measure<T>(&self, work: impl FnOnce() -> T) -> T {
        if !self.enabled {
            return work();
        }
        let _sample = Sample {
            owner: self,
            started: thread_cpu_time(),
        };
        work()
    }

    pub(super) async fn track<F: Future>(&self, future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        std::future::poll_fn(|context| self.measure(|| future.as_mut().poll(context))).await
    }
}

struct Sample<'a> {
    owner: &'a CpuTime,
    started: Option<Duration>,
}
impl Drop for Sample<'_> {
    fn drop(&mut self) {
        let elapsed = self
            .started
            .zip(thread_cpu_time())
            .and_then(|(start, end)| end.checked_sub(start));
        let mut total = self
            .owner
            .total
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *total = (*total)
            .zip(elapsed)
            .and_then(|(total, elapsed)| total.checked_add(elapsed));
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn thread_cpu_time() -> Option<Duration> {
    let time = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    Some(Duration::new(
        u64::try_from(time.tv_sec).ok()?,
        u32::try_from(time.tv_nsec).ok()?,
    ))
}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
const fn thread_cpu_time() -> Option<Duration> {
    None
}
