//! Interface tests for Middleware chains in isolated Program execution.

use coral_value as _;
use deno_core_icudata as _;
use serde as _;
use sourcemap as _;
use thiserror as _;
use v8 as _;

use std::sync::{Arc, Mutex};

use coral_v8::{
    AuthoredLocation, AuthoredSource, ConsoleLevel, Error, InvocationOrigin, IsolateLimits,
    LinkedMiddlewareSource, LinkedSavedFunctionSource, MiddlewareExecutionOutcome,
    MiddlewareExecutionStart, MiddlewareScope, OperationFuture, OperationHostError,
    OperationPolicy, ProgramCompiler, ProgramExecutor, ProgramHost, ProgramOutputHostError,
};

fn deadline() -> std::time::Instant {
    std::time::Instant::now() + coral_v8::COMPILATION_TIMEOUT
}

#[derive(Clone, Copy, Default)]
struct Scopes {
    run: bool,
    function: bool,
    operation: bool,
}

const RUN: Scopes = Scopes {
    run: true,
    function: false,
    operation: false,
};
const FUNCTION: Scopes = Scopes {
    run: false,
    function: true,
    operation: false,
};
const OPERATION: Scopes = Scopes {
    run: false,
    function: false,
    operation: true,
};

fn middleware(name: &str, scopes: Scopes, source: &str) -> LinkedMiddlewareSource {
    LinkedMiddlewareSource::new(
        name.to_string(),
        source.to_string(),
        scopes.run,
        scopes.function,
        scopes.operation,
    )
}

fn function(path: &str, source: &str) -> LinkedSavedFunctionSource {
    LinkedSavedFunctionSource::new(
        path.split('.').map(str::to_string).collect(),
        source.to_string(),
    )
}

/// Records host-visible Middleware and Operation facts in order.
#[derive(Default)]
struct RecordingHost {
    facts: Mutex<Vec<String>>,
    locations: Mutex<Vec<String>>,
    next_execution: Mutex<u32>,
}

impl RecordingHost {
    fn record(&self, fact: String) {
        self.facts.lock().expect("facts").push(fact);
    }

    fn facts(&self) -> Vec<String> {
        self.facts.lock().expect("facts").clone()
    }

    fn locate(&self, call: &str, location: Option<&AuthoredLocation>) {
        let at = location.map_or_else(
            || "-".to_string(),
            |location| {
                let source = match &location.source {
                    AuthoredSource::Program => "program".to_string(),
                    AuthoredSource::SavedFunction { path } => path.clone(),
                };
                format!("{source}:{}:{}", location.line, location.column)
            },
        );
        self.locations
            .lock()
            .expect("locations")
            .push(format!("{call} @ {at}"));
    }
}

impl ProgramHost for RecordingHost {
    fn invoke(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        location: Option<AuthoredLocation>,
    ) -> OperationFuture {
        self.invoke_with_origin(
            call_path,
            arguments_json,
            InvocationOrigin::Program { policy: None },
            location,
        )
    }

    fn invoke_with_origin(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        origin: InvocationOrigin,
        location: Option<AuthoredLocation>,
    ) -> OperationFuture {
        self.locate(&call_path, location.as_ref());
        let origin = match origin {
            InvocationOrigin::Program { policy: None } => "program".to_string(),
            InvocationOrigin::Program {
                policy: Some(policy),
            } => format!("program:{}", policy.as_str()),
            InvocationOrigin::Middleware { execution_id } => format!("helper:{execution_id}"),
        };
        self.record(format!("invoke {call_path} {arguments_json} {origin}"));
        Box::pin(async move {
            if call_path.ends_with(".denied") && origin == "program:disabled" {
                return Err(OperationHostError::Rejected {
                    code: "operation_disabled",
                    message: "Operation is disabled",
                    trace_id: None,
                });
            }
            Ok(format!(
                r#"{{"data":{{"path":"{call_path}","origin":"{origin}"}},"metadata":{{"kind":"http"}}}}"#
            ))
        })
    }

    fn operation_policy(self: Arc<Self>, call_path: String) -> OperationFuture {
        self.record(format!("policy {call_path}"));
        Box::pin(async {
            Ok(format!(
                "\"{}\"",
                OperationPolicy::ApprovalRequired.as_str()
            ))
        })
    }

    fn run_id(&self) -> Option<String> {
        Some("run-1".to_string())
    }

    fn middleware_started(
        &self,
        start: &MiddlewareExecutionStart,
    ) -> Result<String, ProgramOutputHostError> {
        let mut next = self
            .next_execution
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *next += 1;
        let scope = match start.scope {
            MiddlewareScope::Run => "run",
            MiddlewareScope::Function => "function",
            MiddlewareScope::Operation => "operation",
        };
        self.record(format!(
            "start m{next} #{} {scope} {}",
            start.position,
            start.target.as_deref().unwrap_or("-")
        ));
        Ok(format!("m{next}"))
    }

    fn middleware_continued(&self, execution_id: &str) -> Result<(), ProgramOutputHostError> {
        self.record(format!("exit {execution_id}"));
        Ok(())
    }

    fn middleware_ended(
        &self,
        execution_id: &str,
        outcome: MiddlewareExecutionOutcome,
    ) -> Result<(), ProgramOutputHostError> {
        self.record(match outcome {
            MiddlewareExecutionOutcome::Succeeded { preview_json } => format!(
                "end {execution_id} ok {}",
                preview_json.as_deref().unwrap_or("undefined")
            ),
            MiddlewareExecutionOutcome::Failed { summary } => {
                format!("end {execution_id} failed {summary}")
            }
        });
        Ok(())
    }

    fn emit(
        &self,
        value_json: &str,
        location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        self.locate(&format!("emit {value_json}"), location.as_ref());
        self.record(format!("emit {value_json}"));
        Ok(())
    }

    fn middleware_emit(
        &self,
        value_json: &str,
        execution_id: &str,
    ) -> Result<(), ProgramOutputHostError> {
        self.locate(&format!("emit {value_json}"), None);
        self.record(format!("emit {value_json} by {execution_id}"));
        Ok(())
    }

    fn log(
        &self,
        _level: ConsoleLevel,
        message: &str,
        _location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        self.record(format!("log {message}"));
        Ok(())
    }
}

fn run(
    body: &str,
    functions: &[LinkedSavedFunctionSource],
    chain: &[LinkedMiddlewareSource],
) -> (Result<Option<String>, Error>, Vec<String>) {
    let (result, host) = run_on_host(body, functions, chain);
    (result, host.facts())
}

fn run_on_host(
    body: &str,
    functions: &[LinkedSavedFunctionSource],
    chain: &[LinkedMiddlewareSource],
) -> (Result<Option<String>, Error>, Arc<RecordingHost>) {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("compiler");
    let program = compiler
        .compile_linked(deadline(), body, functions, chain)
        .expect("compile linked package");
    let host = Arc::new(RecordingHost::default());
    let result = ProgramExecutor::new(IsolateLimits::default())
        .execute_with_host(&program, host.clone())
        .map(coral_v8::ExecutionOutput::into_final_value_json);
    (result, host)
}

#[test]
fn operation_middleware_wraps_outermost_first_and_sees_downstream_edits() {
    let outer = r#"
        export default async function outer(ctx: CoralMiddleware.Invocation, next: CoralMiddleware.Next) {
            if (ctx.kind !== "operation") return next();
            ctx.args = { ...ctx.args, by: "outer" };
            ctx.currentPolicy = "enabled";
            const result: any = await next();
            return { wrapped: result.data.path };
        }
    "#;
    let inner = r"
        export default async (ctx: any, next: CoralMiddleware.Next) => {
            console.log(`inner sees ${JSON.stringify(ctx.args)} ${ctx.originalPolicy} ${ctx.currentPolicy}`);
            return next();
        };
    ";
    let (result, facts) = run(
        r"return await coral.providers.ledger.list({ page: 1 });",
        &[],
        &[
            middleware("outer", OPERATION, outer),
            middleware("inner", OPERATION, inner),
        ],
    );
    assert_eq!(
        result.expect("run").as_deref(),
        Some(r#"{"wrapped":"coral.providers.ledger.list"}"#)
    );
    assert_eq!(
        facts,
        [
            "policy coral.providers.ledger.list",
            "start m1 #0 operation coral.providers.ledger.list",
            "start m2 #1 operation coral.providers.ledger.list",
            r#"log inner sees {"page":1,"by":"outer"} approval_required enabled"#,
            r#"invoke coral.providers.ledger.list {"page":1,"by":"outer"} program:enabled"#,
            "exit m2",
            r#"end m2 ok {"data":{"path":"coral.providers.ledger.list","origin":"program:enabled"},"metadata":{"kind":"http"}}"#,
            "exit m1",
            r#"end m1 ok {"wrapped":"coral.providers.ledger.list"}"#,
        ]
    );
}

#[test]
fn contexts_are_private_per_middleware_and_scoped_to_run_call_and_operation() {
    let counter = r#"
        export default async function count(ctx: CoralMiddleware.Invocation, next: CoralMiddleware.Next) {
            if (ctx.kind !== "operation") return next();
            const { run, function: call, operation } = ctx.contexts;
            run.count = (run.count ?? 0) + 1;
            if (call) call.count = (call.count ?? 0) + 1;
            operation.seen = (operation.seen ?? 0) + 1;
            await next();
            return { run: run.count, call: call?.count ?? null, operation: operation.seen };
        }
    "#;
    let other = r"
        export default async function other(ctx: any, next: any) {
            ctx.contexts.run.count = 100;
            return next();
        }
    ";
    let helper = r"
        export default async function twice() {
            await coral.providers.ledger.first();
            await Promise.resolve();
            return [await coral.providers.ledger.second(), await coral.functions.inner()];
        }
    ";
    let inner = r"
        export default async function inner() {
            return coral.providers.ledger.third();
        }
    ";
    let (result, _facts) = run(
        r"
            const top = await coral.providers.ledger.top();
            const [second, third] = await coral.functions.twice();
            return { top, second, third };
        ",
        &[function("inner", inner), function("twice", helper)],
        &[
            middleware("counter", OPERATION, counter),
            middleware("other", OPERATION, other),
        ],
    );
    let value: serde_json::Value =
        serde_json::from_str(&result.expect("run").expect("final value")).expect("json");
    assert_eq!(
        value,
        serde_json::json!({
            "top": { "run": 1, "call": null, "operation": 1 },
            "second": { "run": 3, "call": 2, "operation": 1 },
            "third": { "run": 4, "call": 1, "operation": 1 },
        })
    );
}

#[test]
fn helper_calls_are_privileged_only_inside_their_own_middleware() {
    let sentry = r#"
        export default async function sentry(ctx: any, next: any) {
            if (ctx.kind !== "operation") return next();
            const result = await next();
            await coral.providers.sentry.capture({ path: ctx.path });
            ctx.contexts.run.leak = () => coral.providers.sentry.capture({ leaked: true });
            return result;
        }
    "#;
    let (result, facts) = run(
        r#"
            await coral.providers.ledger.list();
            return "done";
        "#,
        &[],
        &[middleware("sentry", OPERATION, sentry)],
    );
    assert_eq!(result.expect("run").as_deref(), Some(r#""done""#));
    assert!(
        facts.contains(&r#"invoke coral.providers.sentry.capture {"path":"coral.providers.ledger.list"} helper:m1"#.to_string()),
        "{facts:?}"
    );
    assert!(
        !facts.iter().any(|fact| fact.starts_with("start m2")),
        "helper calls are not intercepted: {facts:?}"
    );
}

#[test]
fn leaked_helper_capability_is_rejected_outside_its_middleware() {
    let leaker = r#"
        export default async function leaker(ctx: any, next: any) {
            if (ctx.kind === "function") ctx.args.push(coral.providers.sentry.capture);
            return next();
        }
    "#;
    let receiver = r#"
        export default async function receive(capture: () => Promise<unknown>) {
            try {
                await capture();
                return "privileged";
            } catch (error) {
                return String(error);
            }
        }
    "#;
    let (result, facts) = run(
        r"return await coral.functions.receive();",
        &[function("receive", receiver)],
        &[middleware("leaker", FUNCTION, leaker)],
    );
    assert_eq!(
        result.expect("run").as_deref(),
        Some(
            r#""TypeError: A Middleware's coral.providers works only while that Middleware runs""#
        )
    );
    assert!(
        !facts.iter().any(|fact| fact.starts_with("invoke")),
        "{facts:?}"
    );
}

#[test]
fn next_runs_at_most_once_and_must_settle_before_the_middleware() {
    let twice = r"
        export default async function twice(ctx: any, next: any) {
            await next();
            return next();
        }
    ";
    let (result, _facts) = run(
        r"return await coral.providers.ledger.list();",
        &[],
        &[middleware("twice", OPERATION, twice)],
    );
    assert!(
        matches!(&result, Err(Error::ProgramRejected(error)) if error.summary().contains("next() can be called at most once")),
        "{result:?}"
    );

    let detached = r#"
        export default function detached(ctx: any, next: any) {
            next();
            return "early";
        }
    "#;
    let (result, facts) = run(
        r"return await coral.providers.ledger.list();",
        &[],
        &[middleware("detached", OPERATION, detached)],
    );
    assert!(
        matches!(&result, Err(Error::ProgramRejected(error)) if error.summary().contains("finished before its next() continuation settled")),
        "{result:?}"
    );
    assert!(
        facts
            .iter()
            .any(|fact| fact.starts_with("end m1 failed Error: Middleware finished")),
        "{facts:?}"
    );
}

#[test]
fn outer_middleware_can_replace_a_policy_denial_with_a_fallback() {
    let fallback = r"
        export default async function fallback(ctx: any, next: any) {
            try {
                return await next();
            } catch (error: any) {
                return { fallback: error.code };
            }
        }
    ";
    let deny = r#"
        export default async function deny(ctx: any, next: any) {
            ctx.currentPolicy = "disabled";
            return next();
        }
    "#;
    let (result, _facts) = run(
        r"return await coral.providers.ledger.denied();",
        &[],
        &[
            middleware("fallback", OPERATION, fallback),
            middleware("deny", OPERATION, deny),
        ],
    );
    assert_eq!(
        result.expect("run").as_deref(),
        Some(r#"{"fallback":"operation_disabled"}"#)
    );
}

#[test]
fn intercepted_saved_functions_become_async_and_cover_aliases() {
    let double = r#"
        export default async function double(ctx: CoralMiddleware.Invocation, next: CoralMiddleware.Next) {
            if (ctx.kind !== "function") return next();
            ctx.args = [ctx.args[0] as number * 2];
            return next();
        }
    "#;
    let identity = r"export default function identity(value: number) { return value; }";
    let (result, facts) = run(
        r"
            const alias = coral.functions.identity;
            const direct = coral.functions.identity(1);
            const viaAlias = alias(2);
            return [direct instanceof Promise, await direct, await viaAlias, alias === coral.functions.identity];
        ",
        &[function("identity", identity)],
        &[middleware("double", FUNCTION, double)],
    );
    assert_eq!(result.expect("run").as_deref(), Some("[true,2,4,true]"));
    assert!(
        facts.contains(&"start m1 #0 function coral.functions.identity".to_string()),
        "{facts:?}"
    );
}

#[test]
fn unintercepted_saved_functions_stay_synchronous() {
    let observe = r"export default async (ctx: any, next: any) => next();";
    let identity = r"export default function identity(value: number) { return value; }";
    let (result, _facts) = run(
        r"return coral.functions.identity(7);",
        &[function("identity", identity)],
        &[middleware("observe", OPERATION, observe)],
    );
    assert_eq!(result.expect("run").as_deref(), Some("7"));
}

#[test]
fn run_middleware_wraps_module_initialization_and_the_program() {
    let run_wrapper = r#"
        export default async function wrap(ctx: any, next: any) {
            if (ctx.kind === "run") {
                ctx.contexts.run.phase = "init";
                const value = await next();
                return { value, program: ctx.program };
            }
            return { operation: ctx.contexts.function === undefined, run: ctx.contexts.run.phase };
        }
    "#;
    let module = r"
        const initial = await coral.providers.ledger.boot();
        export default function read() { return initial; }
    ";
    let mut wrapper_scopes = RUN;
    wrapper_scopes.operation = true;
    let (result, facts) = run(
        r"return coral.functions.read();",
        &[function("read", module)],
        &[middleware("wrap", wrapper_scopes, run_wrapper)],
    );
    let value: serde_json::Value =
        serde_json::from_str(&result.expect("run").expect("final value")).expect("json");
    assert_eq!(
        value,
        serde_json::json!({
            "value": { "operation": true, "run": "init" },
            "program": { "runId": "run-1", "linkedFunctions": ["coral.functions.read"] },
        })
    );
    assert_eq!(facts.first().map(String::as_str), Some("start m1 #0 run -"));
}

#[test]
fn middleware_errors_fail_the_invocation_and_record_a_safe_summary() {
    let failing = r#"
        export default async function failing() {
            throw new RangeError("budget exceeded");
        }
    "#;
    let (result, facts) = run(
        r"return await coral.providers.ledger.list();",
        &[],
        &[middleware("failing", OPERATION, failing)],
    );
    assert!(
        matches!(result, Err(Error::ProgramRejected(_))),
        "{result:?}"
    );
    assert!(
        facts.contains(&"end m1 failed RangeError: budget exceeded".to_string()),
        "{facts:?}"
    );
    assert!(
        !facts.iter().any(|fact| fact.starts_with("invoke")),
        "{facts:?}"
    );
}

#[test]
fn final_value_previews_never_execute_program_code() {
    let reveal = r#"
        export default async function reveal(ctx: any, next: any) {
            await next();
            return {
                get secret() { console.log("getter ran"); return 1; },
                toJSON() { console.log("toJSON ran"); return {}; },
                nested: [1, "two", { deep: true }],
            };
        }
    "#;
    let (_result, facts) = run(
        r"await coral.providers.ledger.list(); return null;",
        &[],
        &[middleware("reveal", OPERATION, reveal)],
    );
    assert!(
        facts.contains(
            &r#"end m1 ok {"secret":"[Getter]","toJSON":"[Function]","nested":[1,"two",{"deep":true}]}"#.to_string()
        ),
        "{facts:?}"
    );
    assert!(!facts.iter().any(|fact| fact.contains("ran")), "{facts:?}");
}

#[test]
fn program_cannot_bypass_operation_middleware_through_global_this() {
    let block = r#"
        export default async function block(ctx: any) {
            return "blocked";
        }
    "#;
    let (result, facts) = run(
        r#"return await (globalThis as any)["co" + "ral"].providers.ledger.list();"#,
        &[],
        &[middleware("block", OPERATION, block)],
    );
    assert_eq!(result.expect("run").as_deref(), Some(r#""blocked""#));
    assert!(
        !facts.iter().any(|fact| fact.starts_with("invoke")),
        "{facts:?}"
    );
}

#[test]
fn middleware_authoring_accepts_one_effect_free_default_callable() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("compiler");
    compiler
        .prepare_middleware(
            deadline(),
            r#"
                const LIMIT = 10;
                const labels = { ok: "ok", nested: [1, -2] } as const;
                function bump(context: CoralMiddleware.Context) { context.count = (context.count ?? 0) + 1; }
                export default async function limit(ctx: CoralMiddleware.Invocation, next: CoralMiddleware.Next) {
                    bump(ctx.contexts.run);
                    if (ctx.contexts.run.count > LIMIT) throw new Error(labels.ok);
                    return next();
                }
            "#,
        )
        .expect("valid Middleware");

    for (source, expected) in [
        (
            "const now = Date.now();\nexport default async (ctx: any, next: any) => next();",
            "top-level code must not have effects at 1:1",
        ),
        (
            "await coral.providers.x.y();\nexport default async (ctx: any, next: any) => next();",
            "top-level code must not have effects at 1:1",
        ),
        (
            "export default async (ctx: any, next: any) => coral.functions.helper();",
            "cannot reference Saved Functions",
        ),
        (
            "export default async (ctx: any, next: any) => globalThis.coral.providers.x.y();",
            "must use its own coral namespace",
        ),
        (
            "import x from 'y';\nexport default async (ctx: any, next: any) => next();",
            "authored imports are not available",
        ),
        ("export const value = 1;", "exactly one default callable"),
        ("export default 1;", "default export must be callable"),
    ] {
        let error = compiler
            .prepare_middleware(deadline(), source)
            .expect_err(source);
        assert!(error.to_string().contains(expected), "{source}: {error}");
    }
}

#[test]
fn a_stored_next_cannot_run_after_its_middleware_finished() {
    let keep = r#"
        export default async function keep(ctx: any, next: CoralMiddleware.Next) {
            if (ctx.args.step === "first") {
                ctx.contexts.run.stored = next;
                return "replacement";
            }
            try {
                await ctx.contexts.run.stored();
                return "ran";
            } catch (error) {
                return String(error);
            }
        }
    "#;
    let (result, facts) = run(
        r#"
            const first = await coral.providers.ledger.list({ step: "first" });
            const second = await coral.providers.ledger.list({ step: "second" });
            return [first, second];
        "#,
        &[],
        &[middleware("keep", OPERATION, keep)],
    );
    assert_eq!(
        result.expect("run").as_deref(),
        Some(
            r#"["replacement","Error: Middleware next() cannot run after the Middleware finished"]"#
        )
    );
    assert!(
        !facts.iter().any(|fact| fact.starts_with("invoke")),
        "{facts:?}"
    );
}

#[test]
fn helper_privilege_ends_with_its_middleware_execution() {
    let detach = r#"
        export default async function detach(ctx: any, next: CoralMiddleware.Next) {
            if (ctx.args.step === "first") {
                let release!: () => void;
                const gate = new Promise<void>((resolve) => { release = resolve; });
                ctx.contexts.run.release = release;
                ctx.contexts.run.attempt = (async () => {
                    await gate;
                    try {
                        await coral.providers.sentry.capture({});
                        return "privileged";
                    } catch (error) {
                        return String(error);
                    }
                })();
                return "first";
            }
            ctx.contexts.run.release();
            return ctx.contexts.run.attempt;
        }
    "#;
    let (result, facts) = run(
        r#"
            await coral.providers.ledger.list({ step: "first" });
            return await coral.providers.ledger.list({ step: "second" });
        "#,
        &[],
        &[middleware("detach", OPERATION, detach)],
    );
    assert_eq!(
        result.expect("run").as_deref(),
        Some(
            r#""TypeError: A Middleware's coral.providers works only while that Middleware runs""#
        )
    );
    assert!(
        !facts.iter().any(|fact| fact.starts_with("invoke")),
        "{facts:?}"
    );
}

#[test]
fn program_prototype_accessors_cannot_reach_middleware_contexts() {
    let counter = r#"
        export default async function counter(ctx: any, next: CoralMiddleware.Next) {
            if (ctx.kind !== "operation") return next();
            ctx.contexts.run.count = (ctx.contexts.run.count ?? 0) + 1;
            ctx.contexts.operation.secret = "kept";
            await next();
            return ctx.contexts.run.count;
        }
    "#;
    let (result, _facts) = run(
        r#"
            const seen: unknown[] = [];
            for (const key of ["count", "secret"]) {
                Object.defineProperty(Object.prototype, key, {
                    get() { seen.push(`read ${key}`); return 41; },
                    set(value) { seen.push(`wrote ${key}=${value}`); },
                    configurable: true,
                });
            }
            Object.defineProperty(Array.prototype, 0, {
                get() { seen.push("read context slot"); return undefined; },
                configurable: true,
            });
            const counted = await coral.providers.ledger.list();
            return { counted, seen };
        "#,
        &[],
        &[middleware("counter", OPERATION, counter)],
    );
    assert_eq!(
        result.expect("run").as_deref(),
        Some(r#"{"counted":1,"seen":[]}"#)
    );
}

#[test]
fn emit_is_an_operation_that_middleware_can_edit_block_or_replace() {
    let redact = r#"
        export default async function redact(ctx: any, next: CoralMiddleware.Next) {
            if (ctx.path !== "coral.emit") return next();
            console.log(`emit ${ctx.kind} ${"originalPolicy" in ctx} ${"currentPolicy" in ctx}`);
            if (ctx.args.block) return "replacement";
            ctx.args = { ...ctx.args, secret: "[redacted]" };
            return next();
        }
    "#;
    let (result, facts) = run(
        r#"
            const kept = await coral.emit({ secret: "s3cret", note: "kept" });
            const blocked = await coral.emit({ block: true });
            return [kept === undefined, blocked === undefined];
        "#,
        &[],
        &[middleware("redact", OPERATION, redact)],
    );
    assert_eq!(result.expect("run").as_deref(), Some("[true,true]"));
    let emitted: Vec<_> = facts
        .iter()
        .filter(|fact| fact.starts_with("emit ") || fact.starts_with("log "))
        .cloned()
        .collect();
    assert_eq!(
        emitted,
        [
            "log emit operation false false",
            r#"emit {"secret":"[redacted]","note":"kept"}"#,
            "log emit operation false false",
        ]
    );
    assert!(
        facts.contains(&"start m1 #0 operation coral.emit".to_string()),
        "{facts:?}"
    );
}

#[test]
fn emits_from_functions_and_global_this_cannot_bypass_output_middleware() {
    let block = r#"
        export default async function block(ctx: any, next: CoralMiddleware.Next) {
            if (ctx.path === "coral.emit") {
                return `${ctx.contexts.function ? "function" : "run"}:${JSON.stringify(ctx.args)}`;
            }
            return next();
        }
    "#;
    let module = r#"
        await coral.emit("module init");
        export default async function report(value: number) {
            await coral.emit({ value });
            return value;
        }
    "#;
    let (result, facts) = run(
        r#"
            await (globalThis as any)["co" + "ral"].emit("global");
            return await coral.functions.report(7);
        "#,
        &[function("report", module)],
        &[middleware("block", OPERATION, block)],
    );
    assert_eq!(result.expect("run").as_deref(), Some("7"));
    assert!(
        !facts.iter().any(|fact| fact.starts_with("emit ")),
        "{facts:?}"
    );
    let previews: Vec<_> = facts
        .iter()
        .filter(|fact| fact.starts_with("end "))
        .cloned()
        .collect();
    assert_eq!(
        previews,
        [
            r#"end m1 ok "run:\"module init\"""#,
            r#"end m2 ok "run:\"global\"""#,
            r#"end m3 ok "function:{\"value\":7}""#,
        ]
    );
}

#[test]
fn intercepted_emits_keep_the_program_call_order() {
    let slow_first = r#"
        export default async function slow(ctx: any, next: CoralMiddleware.Next) {
            if (ctx.path === "coral.emit" && ctx.args === 1) {
                for (let turn = 0; turn < 20; turn += 1) await Promise.resolve();
            }
            return next();
        }
    "#;
    let (result, facts) = run(
        r#"
            coral.emit(1);
            coral.emit(2);
            await coral.emit(3);
            return "done";
        "#,
        &[],
        &[middleware("slow", OPERATION, slow_first)],
    );
    assert_eq!(result.expect("run").as_deref(), Some(r#""done""#));
    let emitted: Vec<_> = facts
        .iter()
        .filter(|fact| fact.starts_with("emit "))
        .cloned()
        .collect();
    assert_eq!(emitted, ["emit 1", "emit 2", "emit 3"]);
}

#[test]
fn middleware_emits_bypass_interception_and_stay_attributed() {
    let audit = r#"
        export default async function audit(ctx: any, next: CoralMiddleware.Next) {
            if (ctx.path === "coral.emit") return next();
            const result = await next();
            await coral.emit({ audited: ctx.path });
            return result;
        }
    "#;
    let (result, facts) = run(
        r#"await coral.providers.ledger.list(); return "done";"#,
        &[],
        &[middleware("audit", OPERATION, audit)],
    );
    assert_eq!(result.expect("run").as_deref(), Some(r#""done""#));
    assert!(
        facts.contains(&r#"emit {"audited":"coral.providers.ledger.list"} by m1"#.to_string()),
        "{facts:?}"
    );
    assert!(
        !facts.iter().any(|fact| fact.contains("start m2")),
        "a Middleware emit is not intercepted: {facts:?}"
    );
}

#[test]
fn intercepted_calls_keep_the_program_call_site_and_helper_calls_have_none() {
    // Awaiting before next() moves the wrapped call off the Program's stack.
    let audit = r#"
        export default async function audit(ctx: any, next: CoralMiddleware.Next) {
            await null;
            if (ctx.path !== "coral.emit") await coral.providers.audit.log({});
            return next();
        }
    "#;
    let fetch = "export default async function fetch() {\n  coral.emit(2);\n  return coral.providers.ledger.get({});\n}";
    let body = "const queued = coral.emit(1);\nawait coral.providers.ledger.list({});\nawait queued;\nreturn await coral.functions.debug.fetch();";
    let functions = [function("debug.fetch", fetch)];

    let (plain, plain_host) = run_on_host(body, &functions, &[]);
    let (wrapped, wrapped_host) =
        run_on_host(body, &functions, &[middleware("audit", OPERATION, audit)]);

    assert!(plain.is_ok() && wrapped.is_ok(), "{plain:?} {wrapped:?}");
    let program_calls = [
        "emit 1 @ program:1:22",
        "coral.providers.ledger.list @ program:2:30",
        "emit 2 @ debug.fetch:2:9",
        "coral.providers.ledger.get @ debug.fetch:3:33",
    ];
    let mut plain_locations = plain_host.locations.lock().expect("locations").clone();
    plain_locations.sort();
    let mut expected = program_calls.to_vec();
    expected.sort_unstable();
    assert_eq!(plain_locations, expected);
    let wrapped_locations = wrapped_host.locations.lock().expect("locations").clone();
    for call in program_calls {
        assert!(
            wrapped_locations.iter().any(|location| location == call),
            "{call} lost its call site under Middleware: {wrapped_locations:?}"
        );
    }
    assert_eq!(
        wrapped_locations
            .iter()
            .filter(|location| location.starts_with("coral.providers.audit.log"))
            .collect::<Vec<_>>(),
        ["coral.providers.audit.log @ -"; 2],
        "Middleware source is not the Run's authored source"
    );
}
