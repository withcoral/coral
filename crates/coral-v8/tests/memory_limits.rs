//! Memory failures through the compiler and executor Interfaces.

use coral_telemetry as _;
use coral_telemetry::metric::CompilerRole;
use coral_v8::{Error, IsolateLimits, ProgramCompiler, ProgramExecutor};
use coral_value as _;
use deno_core_icudata as _;
#[cfg(unix)]
use rustix as _;
use serde as _;
use serde_json as _;
use sourcemap as _;
use thiserror as _;
use tracing as _;
use v8 as _;

fn deadline() -> std::time::Instant {
    std::time::Instant::now() + coral_v8::COMPILATION_TIMEOUT
}

#[test]
fn memory_limit_fails_only_the_allocating_program() {
    let compiler =
        ProgramCompiler::new(IsolateLimits::default(), CompilerRole::Run).expect("start compiler");
    let program = compiler
        .compile(
            deadline(),
            "const retained = []; for (let i = 0; i < 128; i++) { retained.push(new Array(131072).fill(i)); } return retained.length;",
        )
        .expect("compile allocating Program");
    let limits =
        IsolateLimits::new(16 * 1024 * 1024, 512 * 1024 * 1024).expect("valid isolate limits");
    let executor = ProgramExecutor::new(limits);

    let result = executor.execute(&program);
    assert!(
        matches!(result, Err(Error::MemoryLimitExceeded)),
        "{result:?}"
    );

    let healthy = compiler
        .compile(deadline(), "return 42;")
        .expect("compile healthy Program");
    assert_eq!(
        executor
            .execute(&healthy)
            .expect("process survives")
            .final_value_json(),
        Some("42")
    );
}

#[test]
fn compiler_memory_failure_allows_the_next_request_to_succeed() {
    let limits =
        IsolateLimits::new(16 * 1024 * 1024, 64 * 1024 * 1024).expect("valid isolate limits");
    let compiler = ProgramCompiler::new(limits, CompilerRole::Run).expect("start bounded compiler");
    // The module text alone fills the compiler's heap budget, and its empty
    // statements make the parser allocate from the first token, so V8 trips
    // the limit during parsing, before any type checking runs. A source that
    // outgrew the budget through type checking (30,000 object literals) spent
    // up to COMPILATION_TIMEOUT in TypeScript on a loaded CI runner and
    // reported CompilationTimedOut instead.
    let source = format!(
        "export default function values() {{ return 1; }}\n{}",
        ";".repeat(limits.compiler_heap_bytes()),
    );
    let result = compiler.prepare_saved_function(deadline(), &source);
    assert!(
        matches!(result, Err(Error::MemoryLimitExceeded)),
        "{result:?}"
    );

    let healthy = compiler
        .compile(deadline(), "return 42;")
        .expect("compiler recovers");
    assert_eq!(
        ProgramExecutor::new(IsolateLimits::default())
            .execute(&healthy)
            .expect("execute after compiler failure")
            .final_value_json(),
        Some("42")
    );
}
