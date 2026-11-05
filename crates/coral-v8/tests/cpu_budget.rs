//! The Run CPU Budget through the executor Interface.
//!
//! Only a platform with a thread CPU clock enforces the budget.

use coral_telemetry as _;
use coral_test_support as _;
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

#[cfg(unix)]
mod on_unix {
    use coral_telemetry::metric::CompilerRole;
    use coral_telemetry::testing::MetricCapture;
    use coral_v8::{
        AuthoredLocation, ConsoleLevel, Error, IsolateLimits, ProgramCompiler,
        ProgramExecutionControl, ProgramExecutor, ProgramHost, ProgramOutputHostError, ToolFuture,
    };
    use std::sync::{Arc, Mutex};
    use std::task::{Poll, Waker};
    use std::time::Duration;

    const BUDGET: Duration = Duration::from_secs(1);

    /// A host with a CPU budget whose one Tool answers after `tool_wait`.
    struct BudgetedHost {
        budget: Duration,
        tool_wait: Duration,
    }

    impl ProgramHost for BudgetedHost {
        fn cpu_budget(&self) -> Option<Duration> {
            Some(self.budget)
        }

        fn invoke(
            self: Arc<Self>,
            _call_path: String,
            _arguments_json: String,
            _location: Option<AuthoredLocation>,
        ) -> ToolFuture {
            let answered = Arc::new(Mutex::new((false, None::<Waker>)));
            let timer = answered.clone();
            std::thread::spawn(move || {
                std::thread::sleep(self.tool_wait);
                let mut answered = timer.lock().expect("Tool timer");
                answered.0 = true;
                if let Some(waker) = answered.1.take() {
                    waker.wake();
                }
            });
            Box::pin(std::future::poll_fn(move |context| {
                let mut answered = answered.lock().expect("Tool state");
                if answered.0 {
                    Poll::Ready(Ok(r#"{"data":42,"metadata":{}}"#.to_string()))
                } else {
                    answered.1 = Some(context.waker().clone());
                    Poll::Pending
                }
            }))
        }

        fn emit(
            &self,
            _value_json: &str,
            _location: Option<AuthoredLocation>,
        ) -> Result<(), ProgramOutputHostError> {
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

    /// Catches: a budget that never ends a busy Program, a Program that catches
    /// its end, a wait for a Tool counted as CPU time, and one Program's end
    /// that ends another on the same executor.
    #[test]
    fn the_cpu_budget_ends_a_busy_program_and_leaves_a_waiting_one() {
        let compiler =
            ProgramCompiler::new(IsolateLimits::default(), CompilerRole::Run).expect("compiler");
        let compile = |source| {
            compiler
                .compile(
                    std::time::Instant::now() + coral_test_support::TEST_COMPILATION_TIMEOUT,
                    source,
                )
                .expect("compile")
        };
        let busy = compile(r#"try { while (true) {} } catch { return "caught"; }"#);
        let waiting =
            compile("const answer = await coral.providers.test.wait(); return answer.data;");
        let executor = ProgramExecutor::new(IsolateLimits::default());

        let waiting = std::thread::spawn(move || {
            executor.execute_with_host(
                &waiting,
                Arc::new(BudgetedHost {
                    budget: BUDGET,
                    tool_wait: BUDGET + Duration::from_millis(500),
                }),
            )
        });
        // Bounds the busy Program, so a budget that never ends it fails the
        // assertion below instead of hanging the test.
        let bound = ProgramExecutionControl::new();
        let stop = bound.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(10));
            let _stopped = stop.terminate();
        });
        let capture = MetricCapture::default();
        let busy = tracing::subscriber::with_default(capture.subscriber(), || {
            executor.execute_with_host_control(
                &busy,
                Arc::new(BudgetedHost {
                    budget: BUDGET,
                    tool_wait: Duration::ZERO,
                }),
                &bound,
            )
        });

        assert!(matches!(busy, Err(Error::CpuLimitExceeded)), "{busy:?}");
        let cpu = capture.named("coral.program.execution.cpu.duration");
        let [cpu] = cpu.as_slice() else {
            panic!("one CPU time: {cpu:?}");
        };
        assert!(
            (BUDGET.as_secs_f64()..BUDGET.as_secs_f64() + 0.5).contains(&cpu.value),
            "the Program ends soon after its budget: {cpu:?}"
        );
        let waiting = waiting.join().expect("waiting Program thread");
        assert_eq!(
            waiting
                .expect("waiting spends none of the budget")
                .final_value_json(),
            Some("42")
        );
    }

    /// Catches: a Program that spends its budget and returns before the
    /// watchdog's next check keeps its result.
    #[test]
    fn a_program_that_returns_over_its_budget_fails() {
        let program = ProgramCompiler::new(IsolateLimits::default(), CompilerRole::Run)
            .expect("compiler")
            .compile(
                std::time::Instant::now() + coral_test_support::TEST_COMPILATION_TIMEOUT,
                "return 42;",
            )
            .expect("compile");
        let result = ProgramExecutor::new(IsolateLimits::default()).execute_with_host(
            &program,
            Arc::new(BudgetedHost {
                budget: Duration::from_nanos(1),
                tool_wait: Duration::ZERO,
            }),
        );
        assert!(matches!(result, Err(Error::CpuLimitExceeded)), "{result:?}");
    }
}
