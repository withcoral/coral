use super::*;

use coral_middleware::{
    MiddlewareChangeContext, MiddlewareDescription, MiddlewareModule, MiddlewareName,
    MiddlewareSave, MiddlewareScopes, MiddlewareSource,
};

use crate::approval::ApprovalState;
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

const REVIEW: &str = r#"
    export default async function review(ctx: CoralMiddleware.Invocation, next: CoralMiddleware.Next) {
        if (ctx.kind !== "operation" || ctx.path !== "coral.emit") return next();
        const args = ctx.args as Record<string, unknown>;
        if (args.review) {
            ctx.currentPolicy = "approval_required";
            ctx.args = { ...args, reviewed: "final" };
        }
        return next();
    }
"#;

impl MiddlewareFixture {
    async fn start(&self, program: &str) -> crate::RunId {
        self.modules
            .runs
            .start(
                &self.operation.principal,
                &self.operation.workspace_id,
                submission(program),
            )
            .await
            .unwrap()
            .run_id()
            .clone()
    }

    /// The Run's one pending Approval Request, once Run Updates show it.
    async fn pending_approval(&self, run: &crate::RunId) -> crate::approval::ApprovalId {
        let update = wait_for_actions(
            &self.modules.runs,
            &self.operation.principal,
            &self.operation.workspace_id,
            run,
            1,
            0,
        )
        .await;
        let action = update
            .required_actions()
            .first()
            .expect("one required action");
        match &action.request {
            crate::RequiredActionRequest::InvocationApproval(id) => id.clone(),
            other @ crate::RequiredActionRequest::AccountConnection(_) => {
                panic!("expected an Approval Request: {other:?}")
            }
        }
    }

    async fn decide(
        &self,
        id: &crate::approval::ApprovalId,
        decision: crate::approval::ApprovalDecision,
    ) -> crate::approval::ApprovalRequest {
        self.modules
            .runs
            .decide_approval(
                &self.operation.principal,
                &self.operation.workspace_id,
                id,
                decision,
                None,
                &trace('d'),
            )
            .await
            .unwrap()
    }

    async fn inspect(&self, id: &crate::approval::ApprovalId) -> crate::approval::ApprovalRequest {
        self.modules
            .runs
            .inspect_approval(&self.operation.principal, &self.operation.workspace_id, id)
            .await
            .unwrap()
    }

    async fn emissions(&self, run: &crate::RunId) -> Vec<serde_json::Value> {
        retained_events(
            &self.modules.runs,
            &self.operation.principal,
            &self.operation.workspace_id,
            run,
        )
        .await
        .into_iter()
        .filter_map(|event| match event.kind() {
            RunEventKind::ProgramEmitted { value, .. } => Some(value.as_json().clone()),
            _ => None,
        })
        .collect()
    }

    async fn terminal(&self, run: &crate::RunId) -> crate::RunUpdate {
        wait_for_terminal(
            &self.modules.runs,
            &self.operation.principal,
            &self.operation.workspace_id,
            run,
        )
        .await
    }
}

// Catches: an approval-gated Emission written before a human decides, an approval
// bound to the pre-Middleware value, or one another Workspace Member can read or decide.
#[tokio::test]
async fn an_approved_emission_appends_exactly_the_final_payload_once() {
    let f = MiddlewareFixture::new("user:emit-approve").await;
    f.enable("review", OPERATION, REVIEW).await;
    let run = f
        .start(
            r#"
                await coral.emit({ review: true, secret: "s3cret" });
                return "emitted";
            "#,
        )
        .await;
    let id = f.pending_approval(&run).await;
    assert!(
        f.emissions(&run).await.is_empty(),
        "nothing emits while it waits"
    );
    let request = f.inspect(&id).await;
    assert_eq!(request.call_path, "coral.emit");
    assert!(matches!(
        request.subject,
        crate::ApprovalSubject::Emission { .. }
    ));
    assert_eq!(
        request.arguments,
        serde_json::json!({"review": true, "secret": "s3cret", "reviewed": "final"})
    );
    assert_eq!(request.state, ApprovalState::Pending);
    assert!(request.actionable);

    let member = AuthenticatedPrincipal::user(
        f.modules
            .workspace_user(
                &f.operation.workspace_id,
                &f.operation.user,
                "user:emit-other-member",
                WorkspaceRole::Member,
            )
            .await,
    );
    let concealed = f
        .modules
        .runs
        .decide_approval(
            &member,
            &f.operation.workspace_id,
            &id,
            crate::approval::ApprovalDecision::Approve,
            None,
            &trace('b'),
        )
        .await;
    assert!(
        matches!(concealed, Err(Error::ApprovalNotFound)),
        "{concealed:?}"
    );
    assert!(matches!(
        f.modules
            .runs
            .inspect_approval(&member, &f.operation.workspace_id, &id)
            .await,
        Err(Error::ApprovalNotFound)
    ));
    assert_eq!(f.inspect(&id).await.state, ApprovalState::Pending);
    assert!(f.emissions(&run).await.is_empty());

    let decided = f
        .decide(&id, crate::approval::ApprovalDecision::Approve)
        .await;
    assert_eq!(decided.state, ApprovalState::Approved);
    let terminal = f.terminal(&run).await;
    assert_eq!(final_json(&terminal), serde_json::json!("emitted"));
    assert_eq!(
        f.emissions(&run).await,
        [serde_json::json!({"review": true, "secret": "s3cret", "reviewed": "final"})]
    );
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Decline,
    Expire,
    Cancel,
    LoseAuthority,
}

// Catches: an Emission written after a decline, an expiry, the Run's cancellation or
// the invoking User's loss of Workspace authority, or a waiting Program that settles
// before the durable outcome.
#[tokio::test]
async fn a_declined_expired_or_canceled_emission_emits_nothing() {
    for (outcome, code, state) in [
        (
            Outcome::Decline,
            Some("approval_declined"),
            ApprovalState::Declined,
        ),
        (
            Outcome::Expire,
            Some("approval_expired"),
            ApprovalState::Expired,
        ),
        (Outcome::Cancel, None, ApprovalState::Closed),
        (
            Outcome::LoseAuthority,
            Some("dispatch_authority_lost"),
            ApprovalState::Closed,
        ),
    ] {
        let f = MiddlewareFixture::new(&format!("user:emit-{outcome:?}").to_lowercase()).await;
        f.enable("review", OPERATION, REVIEW).await;
        let run = f
            .start(
                r#"
                    try {
                        await coral.emit({ review: true });
                        return "emitted";
                    } catch (error: any) {
                        return error.code;
                    }
                "#,
            )
            .await;
        let id = f.pending_approval(&run).await;
        match outcome {
            Outcome::Decline => {
                f.decide(&id, crate::approval::ApprovalDecision::Decline)
                    .await;
            }
            Outcome::Expire => {
                f.modules
                    .inject_fault(&format!(
                        "UPDATE emission_approvals SET deadline_at_unix_nanos = created_at_unix_nanos WHERE id = '{id}'"
                    ))
                    .await;
            }
            Outcome::LoseAuthority => {
                f.modules
                    .workspaces
                    .archive(
                        &change(f.operation.user.clone(), 70, '7'),
                        &f.operation.workspace_id,
                    )
                    .await
                    .unwrap();
            }
            Outcome::Cancel => {
                f.modules
                    .runs
                    .cancel(
                        &f.operation.principal,
                        &f.operation.workspace_id,
                        &run,
                        &immediate(None, 100),
                    )
                    .await
                    .unwrap();
            }
        }
        let terminal = f.terminal(&run).await;
        match code {
            Some(code) => assert_eq!(
                final_json(&terminal),
                serde_json::json!(code),
                "{outcome:?}"
            ),
            None => assert_eq!(terminal.state(), RunState::Canceled, "{outcome:?}"),
        }
        assert!(f.emissions(&run).await.is_empty(), "{outcome:?}");
        assert_eq!(f.inspect(&id).await.state, state, "{outcome:?}");
        assert!(!f.inspect(&id).await.actionable, "{outcome:?}");
    }
}

// Catches: an emit gated by the Workspace, a disabled Emission written anyway, or an
// earlier Middleware's policy winning over a later one.
#[tokio::test]
async fn emit_policy_starts_enabled_and_the_innermost_change_decides() {
    let f = MiddlewareFixture::new("user:emit-compose").await;
    f.enable(
        "lock",
        OPERATION,
        r#"
            export default async function lock(ctx: CoralMiddleware.Invocation, next: CoralMiddleware.Next) {
                if (ctx.kind !== "operation" || ctx.path !== "coral.emit") return next();
                const args = ctx.args as Record<string, unknown>;
                if (ctx.originalPolicy !== "enabled") throw new Error("emit starts enabled");
                if (args.lock) ctx.currentPolicy = "disabled";
                return next();
            }
        "#,
    )
    .await;
    f.enable(
        "unlock",
        OPERATION,
        r#"
            export default async function unlock(ctx: CoralMiddleware.Invocation, next: CoralMiddleware.Next) {
                if (ctx.kind !== "operation" || ctx.path !== "coral.emit") return next();
                const args = ctx.args as Record<string, unknown>;
                if (args.unlock) ctx.currentPolicy = "enabled";
                return next();
            }
        "#,
    )
    .await;
    let terminal = f
        .run(
            r#"
                await coral.emit({ plain: true });
                let locked = "emitted";
                try {
                    await coral.emit({ lock: true });
                } catch (error: any) {
                    locked = error.code;
                }
                await coral.emit({ lock: true, unlock: true });
                return locked;
            "#,
        )
        .await;
    assert_eq!(
        final_json(&terminal),
        serde_json::json!("operation_disabled")
    );
    assert_eq!(
        f.emissions(terminal.run_id()).await,
        [
            serde_json::json!({"plain": true}),
            serde_json::json!({"lock": true, "unlock": true}),
        ]
    );
    assert!(
        f.modules
            .approvals()
            .list_for_run(
                &f.operation.principal,
                &f.operation.workspace_id,
                terminal.run_id()
            )
            .await
            .unwrap()
            .is_empty(),
        "no policy here needs a human"
    );
}

// Catches: a later emit, including one the Program does not await, overtaking an
// earlier emit that waits for approval.
#[tokio::test]
async fn emits_keep_the_program_call_order_while_one_awaits_approval() {
    let f = MiddlewareFixture::new("user:emit-order").await;
    f.enable("review", OPERATION, REVIEW).await;
    let run = f
        .start(
            r#"
                coral.emit({ review: true, n: 1 });
                coral.emit({ n: 2 });
                await coral.emit({ n: 3 });
                return "done";
            "#,
        )
        .await;
    let id = f.pending_approval(&run).await;
    assert!(
        f.emissions(&run).await.is_empty(),
        "later emits wait behind the first"
    );
    f.decide(&id, crate::approval::ApprovalDecision::Approve)
        .await;
    let terminal = f.terminal(&run).await;
    assert_eq!(final_json(&terminal), serde_json::json!("done"));
    assert_eq!(
        f.emissions(&run).await,
        [
            serde_json::json!({"review": true, "n": 1, "reviewed": "final"}),
            serde_json::json!({"n": 2}),
            serde_json::json!({"n": 3}),
        ]
    );
}
