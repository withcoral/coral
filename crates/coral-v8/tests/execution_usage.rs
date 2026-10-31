//! What a Program execution uses, as its metrics record it.

use coral_telemetry::metric::CompilerRole;
use std::sync::{Arc, Mutex};

use coral_telemetry::testing::MetricCapture;
use coral_v8::{
    AuthoredLocation, ConsoleLevel, Error, IsolateLimits, ProgramCompiler, ProgramExecutor,
    ProgramHost, ProgramOutputHostError, ToolFuture, ToolHostError,
};
use deno_core_icudata as _;
#[cfg(unix)]
use rustix as _;
use serde as _;
use serde_json as _;
use sourcemap as _;
use thiserror as _;
use v8 as _;

const HEAP_LIMIT: usize = 16 * 1024 * 1024;

/// Each execution records the CPU time its thread used, where the platform
/// measures it, and the most heap it held, whether it returns or runs out of memory; one that runs out held
/// at least its limit.
#[test]
fn an_execution_records_its_cpu_time_and_heap_peak() {
    let compiler =
        ProgramCompiler::new(IsolateLimits::default(), CompilerRole::Run).expect("start compiler");
    let executor =
        ProgramExecutor::new(IsolateLimits::new(HEAP_LIMIT, 512 * 1024 * 1024).expect("limits"));
    for (source, exceeds) in [
        (
            "let total = 0; for (let i = 0; i < 1e6; i++) { total += i; } return total;",
            false,
        ),
        (
            "const retained = []; for (let i = 0; i < 128; i++) { retained.push(new Array(131072).fill(i)); } return retained.length;",
            true,
        ),
    ] {
        let program = compiler
            .compile(
                std::time::Instant::now() + coral_v8::COMPILATION_TIMEOUT,
                source,
            )
            .expect("compile");
        let capture = MetricCapture::default();
        let result =
            tracing::subscriber::with_default(capture.subscriber(), || executor.execute(&program));
        assert_eq!(
            matches!(result, Err(Error::MemoryLimitExceeded)),
            exceeds,
            "{result:?}"
        );
        let heap = capture.named("coral.program.execution.heap.peak");
        let [heap] = heap.as_slice() else {
            panic!("one heap peak: {heap:?}");
        };
        assert_eq!(heap.count, 1);
        assert!(heap.value > 0.0);
        if exceeds {
            assert!(
                heap.value >= f64::from(u32::try_from(HEAP_LIMIT).expect("small limit")),
                "{heap:?}"
            );
        }
        let cpu = capture.named("coral.program.execution.cpu.duration");
        if cfg!(unix) {
            let [cpu] = cpu.as_slice() else {
                panic!("one CPU time: {cpu:?}");
            };
            assert_eq!(cpu.count, 1);
            assert!(cpu.value > 0.0, "{cpu:?}");
        }
    }
}

/// Keeps every Emission that reaches it, within a small host crossing.
#[derive(Default)]
struct KeepingHost {
    emissions: Mutex<Vec<String>>,
}

impl ProgramHost for KeepingHost {
    fn value_limits(&self) -> coral_value::ValueLimits {
        coral_value::ValueLimits::new(64, 64, 64, 1024).expect("limits")
    }

    fn invoke(
        self: Arc<Self>,
        _call_path: String,
        _arguments_json: String,
        _location: Option<AuthoredLocation>,
    ) -> ToolFuture {
        Box::pin(async { Err(ToolHostError::Fatal) })
    }

    fn emit(
        &self,
        value_json: &str,
        _location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        self.emissions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(value_json.to_string());
        Ok(())
    }

    fn log(
        &self,
        _level: ConsoleLevel,
        _message: &str,
        _location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        Ok(())
    }
}

/// An output the runtime refuses before the host sees it is counted once,
/// by its kind.
#[test]
fn an_output_refused_before_the_host_is_counted() {
    let compiler =
        ProgramCompiler::new(IsolateLimits::default(), CompilerRole::Run).expect("start compiler");
    let executor = ProgramExecutor::new(IsolateLimits::default());
    for (source, rejected) in [
        (
            "try { await coral.emit(1n); } catch {} return null;",
            "emission",
        ),
        (
            "try { await coral.emit('x'.repeat(128)); } catch {} return null;",
            "emission",
        ),
        ("return 1n;", "final_value"),
        (
            "const cycle = {}; cycle.self = cycle; return cycle;",
            "final_value",
        ),
        ("return 'x'.repeat(128);", "final_value"),
    ] {
        let program = compiler
            .compile(
                std::time::Instant::now() + coral_v8::COMPILATION_TIMEOUT,
                source,
            )
            .expect("compile");
        let host = Arc::new(KeepingHost::default());
        let capture = MetricCapture::default();
        let _result = tracing::subscriber::with_default(capture.subscriber(), || {
            executor.execute_with_host(&program, host.clone())
        });
        let counted: Vec<_> = capture
            .named("coral.output.rejections")
            .into_iter()
            .map(|point| (point.attributes.get("output.kind").cloned(), point.value))
            .collect();
        assert_eq!(counted, [(Some(rejected.to_string()), 1.0)], "{source}");
        assert!(
            host.emissions.lock().expect("emissions").is_empty(),
            "{source}"
        );
    }
}
