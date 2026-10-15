use super::*;

use coral_middleware::{
    MiddlewareChangeContext, MiddlewareDescription, MiddlewareModule, MiddlewareName,
    MiddlewareSave, MiddlewareScopes, MiddlewareSource,
};

use crate::{MiddlewareExecutionStatus, MiddlewarePhase, MiddlewareScope, RunEntry};

const RUN: MiddlewareScopes = MiddlewareScopes {
    run: true,
    function: false,
    operation: false,
};
const OPERATION: MiddlewareScopes = MiddlewareScopes {
    run: false,
    function: false,
    operation: true,
};

struct MiddlewareFixture {
    modules: TestModules,
    operation: OperationFixture,
    middleware: MiddlewareModule,
    upstream: Arc<ScriptedUpstream>,
}

impl MiddlewareFixture {
    async fn new(user: &str) -> Self {
        let upstream = Arc::new(ScriptedUpstream::json(
            200,
            serde_json::json!({"temperature": 21}),
        ));
        let modules = TestModules::with_upstream(
            Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
            upstream.clone(),
        )
        .await;
        let operation = modules.user_operation(user, "Middleware").await;
        let middleware = MiddlewareModule::new(
            modules.database.clone(),
            modules.workspaces.clone(),
            crate::IsolateLimits::default(),
        )
        .unwrap();
        Self {
            modules,
            operation,
            middleware,
            upstream,
        }
    }

    fn context(&self, at: i64) -> MiddlewareChangeContext {
        MiddlewareChangeContext::new(self.operation.principal.clone(), at, trace('e'))
    }

    async fn enable(&self, name: &str, scopes: MiddlewareScopes, source: &str) {
        let saved = self
            .middleware
            .save(
                &self.context(60),
                &self.operation.workspace_id,
                MiddlewareSave::new(
                    MiddlewareName::parse(name).unwrap(),
                    MiddlewareDescription::parse("Test Middleware").unwrap(),
                    MiddlewareSource::parse(source).unwrap(),
                ),
            )
            .await
            .unwrap();
        self.middleware
            .set_scopes(
                &self.context(61),
                &self.operation.workspace_id,
                saved.id(),
                scopes,
            )
            .await
            .unwrap();
        self.middleware
            .set_enabled(
                &self.context(62),
                &self.operation.workspace_id,
                saved.id(),
                true,
            )
            .await
            .unwrap();
    }

    async fn run(&self, program: &str) -> crate::RunUpdate {
        let run = self
            .modules
            .runs
            .start(
                &self.operation.principal,
                &self.operation.workspace_id,
                submission(program),
            )
            .await
            .unwrap();
        wait_for_terminal(
            &self.modules.runs,
            &self.operation.principal,
            &self.operation.workspace_id,
            run.run_id(),
        )
        .await
    }

    async fn entries(&self, run: &crate::RunId) -> Vec<RunEntry> {
        self.modules
            .runs
            .activity_entries(
                &self.operation.principal,
                &self.operation.workspace_id,
                run,
                &crate::RunEntryWindow::Start,
                100,
                None,
            )
            .await
            .unwrap()
            .entries
    }
}

fn trace(digit: char) -> TraceId {
    TraceId::parse(&digit.to_string().repeat(32)).unwrap()
}

fn final_json(update: &crate::RunUpdate) -> serde_json::Value {
    match update.terminal_outcome() {
        Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value),
        }) => value.as_json().clone(),
        other => panic!("Run did not succeed with a value: {other:?}"),
    }
}

/// Operation Middleware can enable an approval-required Invocation for that
/// one call, and a Middleware helper call bypasses Workspace policy. Activity
/// shows one row per Middleware execution and marks the helper Invocation.
#[tokio::test]
async fn middleware_selects_policy_and_helper_calls_bypass_it() {
    let f = MiddlewareFixture::new("user:middleware-policy").await;
    f.modules.require_approval(&f.operation).await;
    f.enable(
        "audit",
        RUN,
        r#"
            export default async function audit(ctx: CoralMiddleware.Invocation, next: CoralMiddleware.Next) {
                const value = await next();
                const checked: any = await coral.providers.weather.getForecast({ city: "helper" });
                return { value, checked: checked.data.temperature };
            }
        "#,
    )
    .await;
    f.enable(
        "gate",
        OPERATION,
        r#"
            export default async function gate(ctx: any, next: CoralMiddleware.Next) {
                if (ctx.args.city === "auto") ctx.currentPolicy = "enabled";
                return next();
            }
        "#,
    )
    .await;

    let terminal = f
        .run(
            r#"
                const result: any = await coral.providers.weather.getForecast({ city: "auto" });
                return result.data.temperature;
            "#,
        )
        .await;
    assert_eq!(
        final_json(&terminal),
        serde_json::json!({"value": 21, "checked": 21})
    );
    assert_eq!(f.upstream.sends.load(Ordering::SeqCst), 2);
    assert!(terminal.required_actions().is_empty());

    let entries = f.entries(terminal.run_id()).await;
    let middleware: Vec<_> = entries
        .iter()
        .filter_map(|entry| match entry {
            RunEntry::Middleware(row) => Some((
                row.name.as_str(),
                row.scope,
                row.target.as_deref(),
                row.phase,
                row.status,
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        middleware,
        [
            (
                "audit",
                MiddlewareScope::Run,
                None,
                MiddlewarePhase::Exit,
                MiddlewareExecutionStatus::Succeeded,
            ),
            (
                "gate",
                MiddlewareScope::Operation,
                Some("coral.providers.weather.getForecast"),
                MiddlewarePhase::Exit,
                MiddlewareExecutionStatus::Succeeded,
            ),
        ]
    );
    let helper_rows: Vec<_> = entries
        .iter()
        .filter_map(|entry| match entry {
            RunEntry::Invocation(row) => Some(row.middleware_execution.is_some()),
            _ => None,
        })
        .collect();
    assert_eq!(helper_rows, [false, true]);
}

/// Each Run pins the enabled chain at admission. A save disables the changed
/// Middleware, so only the earlier Run is wrapped.
#[tokio::test]
async fn each_run_pins_the_chain_enabled_at_admission() {
    let f = MiddlewareFixture::new("user:middleware-pin").await;
    let wrap = r"
        export default async (ctx: any, next: CoralMiddleware.Next) => ({ wrapped: await next() });
    ";
    f.enable("wrap", RUN, wrap).await;
    assert_eq!(
        final_json(&f.run("return 1;").await),
        serde_json::json!({"wrapped": 1})
    );

    f.middleware
        .save(
            &f.context(70),
            &f.operation.workspace_id,
            MiddlewareSave::new(
                MiddlewareName::parse("wrap").unwrap(),
                MiddlewareDescription::parse("Changed").unwrap(),
                MiddlewareSource::parse(wrap).unwrap(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(final_json(&f.run("return 1;").await), serde_json::json!(1));
}

/// An unhandled Middleware error fails the wrapped invocation closed: the
/// Operation never dispatches and Activity keeps the failed execution.
#[tokio::test]
async fn an_unhandled_middleware_error_fails_closed() {
    let f = MiddlewareFixture::new("user:middleware-failure").await;
    f.enable(
        "deny",
        OPERATION,
        r#"export default async () => { throw new Error("blocked by review"); };"#,
    )
    .await;
    let terminal = f
        .run("await coral.providers.weather.getForecast({ city: \"x\" }); return 1;")
        .await;
    assert!(
        matches!(
            terminal.terminal_outcome(),
            Some(RunTerminalOutcome::Failed(failure))
                if failure.code() == RunFailureCode::ProgramRejected
        ),
        "{terminal:?}"
    );
    assert_eq!(f.upstream.sends.load(Ordering::SeqCst), 0);
    let failed: Vec<_> = f
        .entries(terminal.run_id())
        .await
        .into_iter()
        .filter_map(|entry| match entry {
            RunEntry::Middleware(row) => Some((row.status, row.failure_summary)),
            _ => None,
        })
        .collect();
    assert_eq!(
        failed,
        [(
            MiddlewareExecutionStatus::Failed,
            Some("Error: blocked by review".to_string())
        )]
    );
}

/// `coral.emit` is an Operation for Middleware: output control edits or blocks
/// Program Emissions durably, and a Middleware's own Emission is attributed to
/// its execution.
#[tokio::test]
async fn output_middleware_controls_program_emissions_and_attributes_its_own() {
    let f = MiddlewareFixture::new("user:middleware-emit").await;
    f.enable(
        "output",
        OPERATION,
        r#"
            export default async function output(ctx: any, next: CoralMiddleware.Next) {
                if (ctx.path !== "coral.emit") return next();
                if (ctx.args.drop) return "dropped";
                ctx.args = { ...ctx.args, token: "[redacted]" };
                await next();
                await coral.emit({ reviewed: true });
            }
        "#,
    )
    .await;
    let terminal = f
        .run(
            r#"
                await coral.emit({ token: "s3cret" });
                await coral.emit({ drop: true });
                return null;
            "#,
        )
        .await;
    let emissions: Vec<_> = retained_events(
        &f.modules.runs,
        &f.operation.principal,
        &f.operation.workspace_id,
        terminal.run_id(),
    )
    .await
    .into_iter()
    .filter_map(|event| match event.kind() {
        RunEventKind::ProgramEmitted {
            value,
            middleware_execution,
        } => Some((value.as_json().clone(), middleware_execution.is_some())),
        _ => None,
    })
    .collect();
    assert_eq!(
        emissions,
        [
            (serde_json::json!({"token": "[redacted]"}), false),
            (serde_json::json!({"reviewed": true}), true),
        ]
    );
}
