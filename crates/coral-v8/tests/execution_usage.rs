//! What a Program execution uses, as its metrics record it.

use coral_telemetry::testing::MetricCapture;
use coral_v8::{Error, IsolateLimits, ProgramCompiler, ProgramExecutor};
use coral_value as _;
use deno_core_icudata as _;
#[cfg(unix)]
use rustix as _;
use serde as _;
use serde_json as _;
use sourcemap as _;
use thiserror as _;
use v8 as _;

const HEAP_LIMIT: usize = 16 * 1024 * 1024;

/// Each execution records the CPU time its thread used and the most heap it
/// held, whether it returns or runs out of memory; one that runs out held
/// at least its limit.
#[test]
fn an_execution_records_its_cpu_time_and_heap_peak() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
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
