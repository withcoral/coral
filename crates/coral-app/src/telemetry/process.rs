//! Current-process memory observation.

use std::sync::Mutex;

pub(crate) trait ProcessMemoryReader: Send + Sync {
    fn resident_bytes(&self) -> Option<u64>;
}

pub(crate) struct SystemProcessMemoryReader {
    sampler: Mutex<Option<Box<dyn ResidentMemorySampler>>>,
}

impl SystemProcessMemoryReader {
    pub(crate) fn new() -> Self {
        Self::with_sampler(platform_sampler())
    }

    fn with_sampler(sampler: Option<Box<dyn ResidentMemorySampler>>) -> Self {
        Self {
            sampler: Mutex::new(sampler),
        }
    }
}

impl Default for SystemProcessMemoryReader {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessMemoryReader for SystemProcessMemoryReader {
    fn resident_bytes(&self) -> Option<u64> {
        self.sampler.try_lock().ok()?.as_mut()?.sample().ok()
    }
}

#[derive(Debug)]
struct SampleUnavailable;

trait ResidentMemorySampler: Send {
    fn sample(&mut self) -> Result<u64, SampleUnavailable>;
}

#[cfg(target_os = "linux")]
fn platform_sampler() -> Option<Box<dyn ResidentMemorySampler>> {
    LinuxResidentMemorySampler::new()
        .map(|sampler| Box::new(sampler) as Box<dyn ResidentMemorySampler>)
}

#[cfg(target_os = "linux")]
struct LinuxResidentMemorySampler {
    statm: std::fs::File,
    page_size: u64,
    buffer: String,
}

#[cfg(target_os = "linux")]
impl LinuxResidentMemorySampler {
    fn new() -> Option<Self> {
        let page_size = u64::try_from(page_size::get())
            .ok()
            .filter(|size| *size > 0)?;
        Some(Self {
            statm: std::fs::File::open("/proc/self/statm").ok()?,
            page_size,
            buffer: String::new(),
        })
    }
}

#[cfg(target_os = "linux")]
impl ResidentMemorySampler for LinuxResidentMemorySampler {
    fn sample(&mut self) -> Result<u64, SampleUnavailable> {
        use std::io::{Read, Seek};

        self.statm.rewind().map_err(|_| SampleUnavailable)?;
        self.buffer.clear();
        self.statm
            .read_to_string(&mut self.buffer)
            .map_err(|_| SampleUnavailable)?;
        parse_linux_statm(&self.buffer, self.page_size)
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_linux_statm(input: &str, page_size: u64) -> Result<u64, SampleUnavailable> {
    let mut fields = input.split_ascii_whitespace();
    fields.next().ok_or(SampleUnavailable)?;
    let resident_pages = fields
        .next()
        .ok_or(SampleUnavailable)?
        .parse::<u64>()
        .ok()
        .ok_or(SampleUnavailable)?;
    resident_pages
        .checked_mul(page_size)
        .ok_or(SampleUnavailable)
}

#[cfg(any(target_os = "macos", windows))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "keep one constructor shape across platform cfg variants"
)]
fn platform_sampler() -> Option<Box<dyn ResidentMemorySampler>> {
    Some(Box::new(NativeResidentMemorySampler))
}

#[cfg(any(target_os = "macos", windows))]
struct NativeResidentMemorySampler;

#[cfg(any(target_os = "macos", windows))]
impl ResidentMemorySampler for NativeResidentMemorySampler {
    fn sample(&mut self) -> Result<u64, SampleUnavailable> {
        let memory = memory_stats::memory_stats().ok_or(SampleUnavailable)?;
        u64::try_from(memory.physical_mem)
            .ok()
            .ok_or(SampleUnavailable)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn platform_sampler() -> Option<Box<dyn ResidentMemorySampler>> {
    None
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use super::{
        ProcessMemoryReader, ResidentMemorySampler, SampleUnavailable, SystemProcessMemoryReader,
    };

    struct SequenceSampler {
        samples: VecDeque<Result<u64, SampleUnavailable>>,
    }

    impl ResidentMemorySampler for SequenceSampler {
        fn sample(&mut self) -> Result<u64, SampleUnavailable> {
            self.samples.pop_front().unwrap_or(Err(SampleUnavailable))
        }
    }

    fn sequence_reader(
        samples: impl IntoIterator<Item = Result<u64, SampleUnavailable>>,
    ) -> SystemProcessMemoryReader {
        SystemProcessMemoryReader::with_sampler(Some(Box::new(SequenceSampler {
            samples: samples.into_iter().collect(),
        })))
    }

    #[test]
    fn success_followed_by_failure_omits_instead_of_replaying() {
        let reader = sequence_reader([Ok(42), Err(SampleUnavailable)]);

        assert_eq!(reader.resident_bytes(), Some(42));
        assert_eq!(reader.resident_bytes(), None);
    }

    #[test]
    fn successful_zero_sample_is_preserved() {
        let reader = sequence_reader([Ok(0)]);

        assert_eq!(reader.resident_bytes(), Some(0));
    }

    #[test]
    fn lock_contention_omits_process_memory() {
        let reader = SystemProcessMemoryReader::new();
        let _guard = reader.sampler.lock().expect("sampler lock should succeed");

        assert_eq!(reader.resident_bytes(), None);
    }

    #[test]
    fn unavailable_sampler_supports_shared_reader_injection() {
        let reader: Arc<dyn ProcessMemoryReader> =
            Arc::new(SystemProcessMemoryReader::with_sampler(None));

        assert_eq!(reader.resident_bytes(), None);
    }

    #[test]
    fn current_process_reader_returns_a_fresh_value() {
        let reader = SystemProcessMemoryReader::new();

        assert!(reader.resident_bytes().is_some());
    }

    #[test]
    fn linux_statm_parser_preserves_zero_and_rejects_invalid_samples() {
        assert_eq!(
            super::parse_linux_statm("10 0 3", 4_096).expect("zero is a valid sample"),
            0
        );
        super::parse_linux_statm("10", 4_096).expect_err("missing resident field must fail");
        super::parse_linux_statm("10 invalid", 4_096)
            .expect_err("invalid resident field must fail");
        super::parse_linux_statm("10 2", u64::MAX).expect_err("resident byte overflow must fail");
    }
}
