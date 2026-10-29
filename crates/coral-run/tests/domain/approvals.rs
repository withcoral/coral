use super::*;
use crate::approval::{ApprovalDecision, ApprovalModule, ApprovalState};
use coral_outbound::DestinationPolicy;

#[tokio::test]
async fn approval_waits_for_the_invoking_user_and_commits_one_decision() {
    let upstream = Arc::new(ScriptedUpstream::json(
        200,
        serde_json::json!({"temperature": 21}),
    ));
    let fixture = TestModules::with_upstream(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        upstream.clone(),
    )
    .await;
    let tool = fixture.user_tool("user:approver", "Approve weather").await;
    let mut tx = fixture.database.begin().await.unwrap();
    coral_workspace::OrganizationModule::hold_profile_updates_in_transaction(&mut tx)
        .await
        .unwrap();
    coral_workspace::OrganizationModule::refresh_profile_in_transaction(
        &mut tx,
        tool.user.user_id(),
        coral_workspace::VerifiedUserProfile::new(Some("Reviewing user"), None, None, None),
        40,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    fixture.require_approval(&tool).await;
    let approvals = fixture.approvals();
    let accepted = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("invoke-tool"),
        )
        .await
        .unwrap();
    let request = wait_for_approval(
        &approvals,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert_eq!(request.state, ApprovalState::Pending);
    assert_eq!(request.provider_id, tool.provider_id);
    assert_eq!(request.invoking_principal, *tool.principal.principal());
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
    let decided = approvals
        .decide(
            &tool.principal,
            &tool.workspace_id,
            &request.id,
            ApprovalDecision::Approve,
            None,
            &TraceId::parse(&"a".repeat(32)).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(decided.state, ApprovalState::Approved);
    assert_eq!(decided.approver_name.as_deref(), Some("Reviewing user"));
    assert_eq!(
        approvals
            .decide(
                &tool.principal,
                &tool.workspace_id,
                &request.id,
                ApprovalDecision::Decline,
                None,
                &TraceId::parse(&"b".repeat(32)).unwrap()
            )
            .await
            .unwrap(),
        decided
    );
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert_eq!(terminal.state(), RunState::Succeeded);
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
    let events = approvals
        .list_lifecycle_events(&tool.principal, &tool.workspace_id, &request.id)
        .await
        .unwrap();
    let [event] = events.as_slice() else {
        panic!("one decision event: {events:?}")
    };
    assert_eq!(event.actor, Some(tool.principal.principal().clone()));
    assert_eq!(event.state, ApprovalState::Approved);
    assert_eq!(event.changed_field_paths, ["state"]);
}

#[tokio::test]
async fn declining_commits_invocation_failure_with_the_winning_decision() {
    let fixture = TestModules::new(Arc::new(
        crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
    ))
    .await;
    let tool = fixture
        .user_tool("user:decliner", "Decline exact invocation")
        .await;
    fixture.require_approval(&tool).await;
    let approvals = fixture.approvals();
    let run = fixture.runs.start(&tool.principal, &tool.workspace_id, submission("try { await coral.providers.weather.getForecast({city:'argument-canary'}); } catch (e) { return e.code; }"
    )).await.unwrap();
    let request = wait_for_approval(
        &approvals,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert_eq!(
        request.arguments,
        serde_json::json!({"city":"argument-canary"})
    );
    approvals
        .decide(
            &tool.principal,
            &tool.workspace_id,
            &request.id,
            ApprovalDecision::Decline,
            Some("comment-canary"),
            &TraceId::parse(&"d".repeat(32)).unwrap(),
        )
        .await
        .unwrap();
    let update = fixture
        .runs
        .observe(
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
            &immediate(None, 100),
        )
        .await
        .unwrap();
    assert!(
        retained_events(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            update.run_id()
        )
        .await
        .iter()
        .any(|event| matches!(event.kind(), RunEventKind::InvocationEnded { .. })),
        "decline durably settles its child before returning"
    );
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert_final_code(&terminal, "approval_declined");
    let events = approvals
        .list_lifecycle_events(&tool.principal, &tool.workspace_id, &request.id)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert!(!format!("{events:?}").contains("canary"));
}

/// An approval-gated Workspace with every kind of Principal that might see an
/// approval: an Organization Owner without a Membership, a designated Workspace
/// Owner, a Member and an Agent, besides the Workspace's creator.
struct ApprovalAudience {
    fixture: TestModules,
    tool: ToolFixture,
    approvals: crate::approval::ApprovalModule,
    org_owner: AuthenticatedPrincipal,
    designated: AuthenticatedPrincipal,
    member: AuthenticatedPrincipal,
    agent: AuthenticatedPrincipal,
    trace: TraceId,
}

impl ApprovalAudience {
    async fn new() -> Self {
        let fixture = TestModules::new(Arc::new(InvokingEngine::new(
            "coral.providers.weather.getForecast",
        )))
        .await;
        let tool = fixture.user_tool("user:private", "Private approvals").await;
        fixture.require_approval(&tool).await;
        let org_owner = AuthenticatedPrincipal::user(
            organization_user(
                fixture.database.clone(),
                "user:org-owner",
                OrganizationRole::Owner,
            )
            .await,
        );
        let mut members = Vec::new();
        for (id, role) in [
            ("user:designated", WorkspaceRole::Owner),
            ("user:member", WorkspaceRole::Member),
        ] {
            let user =
                organization_user(fixture.database.clone(), id, OrganizationRole::Member).await;
            fixture
                .workspaces
                .set_membership(
                    &change(tool.user.clone(), 50, '5'),
                    &tool.workspace_id,
                    &user,
                    role,
                )
                .await
                .unwrap();
            members.push(AuthenticatedPrincipal::user(user));
        }
        let [designated, member] = <[_; 2]>::try_from(members).unwrap();
        let agent =
            registered_agent(&fixture.workspaces, &tool.principal, &tool.workspace_id).await;
        Self {
            approvals: fixture.approvals(),
            fixture,
            tool,
            org_owner,
            designated,
            member,
            agent,
            trace: TraceId::parse(&"a".repeat(32)).unwrap(),
        }
    }

    /// Starts a Run as `actor` and returns its pending approval, read by `reader`.
    async fn pending(
        &self,
        actor: &AuthenticatedPrincipal,
        reader: &AuthenticatedPrincipal,
    ) -> (crate::RunId, crate::approval::ApprovalRequest) {
        let run = self
            .fixture
            .runs
            .start(actor, &self.tool.workspace_id, submission("invoke"))
            .await
            .unwrap()
            .run_id()
            .clone();
        let request =
            wait_for_approval(&self.approvals, reader, &self.tool.workspace_id, &run).await;
        (run, request)
    }

    async fn decide(
        &self,
        actor: &AuthenticatedPrincipal,
        id: &crate::approval::ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<crate::approval::ApprovalRequest, Error> {
        self.approvals
            .decide(
                actor,
                &self.tool.workspace_id,
                id,
                decision,
                None,
                &self.trace,
            )
            .await
    }

    async fn set_workspace_role(
        &self,
        actor: &AuthenticatedPrincipal,
        user: &AuthenticatedUser,
        role: WorkspaceRole,
        at: i64,
    ) {
        self.fixture
            .workspaces
            .set_membership(
                &change(actor.authenticated_user().unwrap().clone(), at, '6'),
                &self.tool.workspace_id,
                user,
                role,
            )
            .await
            .unwrap();
    }
}

/// A User's approval, and whether it exists, is hidden from every other
/// Principal: Owners, Members and Agents alike. A denial decides nothing.
#[tokio::test]
async fn a_users_approval_is_concealed_from_everyone_else() {
    let audience = ApprovalAudience::new().await;
    let workspace = &audience.tool.workspace_id;
    let (run, request) = audience
        .pending(&audience.tool.principal, &audience.tool.principal)
        .await;
    let absent =
        crate::approval::ApprovalId::parse("00000000-0000-0000-0000-000000000001").unwrap();
    for actor in [
        &audience.designated,
        &audience.org_owner,
        &audience.member,
        &audience.agent,
    ] {
        for id in [&request.id, &absent] {
            assert!(matches!(
                audience.approvals.get(actor, workspace, id).await,
                Err(Error::ApprovalNotFound)
            ));
            assert!(matches!(
                audience.decide(actor, id, ApprovalDecision::Approve).await,
                Err(Error::ApprovalNotFound)
            ));
            assert!(matches!(
                audience
                    .approvals
                    .list_lifecycle_events(actor, workspace, id)
                    .await,
                Err(Error::ApprovalNotFound)
            ));
        }
        assert!(matches!(
            audience
                .approvals
                .list_for_run(actor, workspace, &run)
                .await,
            Err(Error::ApprovalNotFound)
        ));
    }
    let (elsewhere, _) = audience
        .fixture
        .user_scope("user:elsewhere", "Other Workspace")
        .await;
    assert!(matches!(
        audience
            .approvals
            .get(&audience.org_owner, &elsewhere, &request.id)
            .await,
        Err(Error::ApprovalNotFound)
    ));
    assert_eq!(
        audience
            .approvals
            .get(&audience.tool.principal, workspace, &request.id)
            .await
            .unwrap(),
        request,
        "denied decisions change nothing"
    );
}

/// The invoking User decides their own approval even without an Owner role.
#[tokio::test]
async fn a_user_decides_their_own_approval_as_an_ordinary_member() {
    let audience = ApprovalAudience::new().await;
    let user = &audience.tool.principal;
    let (run, request) = audience.pending(user, user).await;
    audience
        .set_workspace_role(user, &audience.tool.user, WorkspaceRole::Member, 60)
        .await;
    let declined = audience
        .decide(user, &request.id, ApprovalDecision::Decline)
        .await
        .unwrap();
    assert_eq!(declined.state, crate::approval::ApprovalState::Declined);
    wait_for_terminal(
        &audience.fixture.runs,
        user,
        &audience.tool.workspace_id,
        &run,
    )
    .await;
}

/// Only a current designated Workspace Owner sees and decides an Agent's
/// approval. The Agent, a Member, an Organization Owner without a Membership
/// and a demoted Owner get nothing.
#[tokio::test]
async fn an_agents_approval_belongs_to_current_workspace_owners() {
    let audience = ApprovalAudience::new().await;
    let workspace = &audience.tool.workspace_id;
    let creator = &audience.tool.principal;
    let (_, request) = audience.pending(&audience.agent, creator).await;
    assert_eq!(
        audience
            .approvals
            .get(&audience.designated, workspace, &request.id)
            .await
            .unwrap(),
        request
    );
    for actor in [&audience.agent, &audience.member, &audience.org_owner] {
        assert!(matches!(
            audience.approvals.get(actor, workspace, &request.id).await,
            Err(Error::ApprovalNotFound)
        ));
        assert!(matches!(
            audience
                .decide(actor, &request.id, ApprovalDecision::Approve)
                .await,
            Err(Error::ApprovalNotFound)
        ));
    }
    audience
        .set_workspace_role(
            creator,
            audience.designated.authenticated_user().unwrap(),
            WorkspaceRole::Member,
            80,
        )
        .await;
    assert!(matches!(
        audience
            .decide(&audience.designated, &request.id, ApprovalDecision::Approve)
            .await,
        Err(Error::ApprovalNotFound)
    ));
    assert_eq!(
        audience
            .approvals
            .get(creator, workspace, &request.id)
            .await
            .unwrap(),
        request,
        "denied decisions change nothing"
    );
}

/// A retained Owner context cannot approve Agent work once the Workspace
/// demotes that User. Organization Owner authority does not restore it; only
/// the designated Owner role does.
#[tokio::test]
async fn organization_ownership_does_not_restore_agent_approval_to_a_demoted_owner() {
    let audience = ApprovalAudience::new().await;
    let workspace = &audience.tool.workspace_id;
    let creator = &audience.tool.principal;
    let demoted = &audience.designated;
    let user = demoted.authenticated_user().unwrap();
    let (run, request) = audience.pending(&audience.agent, demoted).await;
    audience
        .set_workspace_role(creator, user, WorkspaceRole::Member, 81)
        .await;
    let organization = coral_workspace::OrganizationModule::new(audience.fixture.database.clone());
    let administrator = organization_admin(&audience.fixture).await;
    organization
        .set_role(&administrator, user.user_id(), 1, OrganizationRole::Owner)
        .await
        .unwrap();
    assert!(matches!(
        audience
            .decide(demoted, &request.id, ApprovalDecision::Approve)
            .await,
        Err(Error::ApprovalNotFound)
    ));
    assert_eq!(
        audience
            .approvals
            .get(creator, workspace, &request.id)
            .await
            .unwrap(),
        request,
        "the denied decision changes nothing"
    );
    audience
        .set_workspace_role(creator, user, WorkspaceRole::Owner, 82)
        .await;
    audience
        .decide(demoted, &request.id, ApprovalDecision::Decline)
        .await
        .unwrap();
    let event = audience
        .approvals
        .list_lifecycle_events(demoted, workspace, &request.id)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(event.actor, Some(demoted.principal().clone()));
    wait_for_terminal(&audience.fixture.runs, &audience.agent, workspace, &run).await;
}

#[tokio::test]
async fn destructive_lifecycle_keeps_exact_agent_invocation_and_approval_history() {
    let fixture = TestModules::new(Arc::new(
        crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
    ))
    .await;
    let tool = fixture
        .user_tool("user:retained-approval", "Retain approvals")
        .await;
    fixture.require_approval(&tool).await;
    let agent = registered_agent(&fixture.workspaces, &tool.principal, &tool.workspace_id).await;
    let approvals = fixture.approvals();
    let run = fixture
        .runs
        .start(
            &agent,
            &tool.workspace_id,
            submission("await coral.providers.weather.getForecast({city:'private-argument'});"),
        )
        .await
        .unwrap();
    let request = wait_for_approval(
        &approvals,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    let decision = approvals
        .decide(
            &tool.principal,
            &tool.workspace_id,
            &request.id,
            ApprovalDecision::Decline,
            Some("private-comment"),
            &TraceId::parse(&"a".repeat(32)).unwrap(),
        )
        .await
        .unwrap();
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    let events = approvals
        .list_lifecycle_events(&tool.principal, &tool.workspace_id, &request.id)
        .await
        .unwrap();
    let context = change(tool.user.clone(), 50, '5');
    coral_administration::AccessLifecycleModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
    )
    .delete_agent(&context, &tool.workspace_id, agent.principal().id())
    .await
    .unwrap();
    fixture
        .providers
        .delete_provider(&context, &tool.workspace_id.clone(), &tool.provider_id)
        .await
        .unwrap();
    assert_eq!(
        approvals
            .get(&tool.principal, &tool.workspace_id, &request.id)
            .await
            .unwrap(),
        crate::approval::ApprovalRequest {
            provider_label: None,
            ..decision.clone()
        }
    );
    assert_eq!(decision.invoking_principal, *agent.principal());
    assert_eq!(
        approvals
            .list_lifecycle_events(&tool.principal, &tool.workspace_id, &request.id)
            .await
            .unwrap(),
        events
    );
    assert_eq!(
        wait_for_terminal(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            run.run_id()
        )
        .await,
        terminal
    );
}

#[tokio::test]
async fn racing_decisions_have_one_durable_winner_and_event() {
    let upstream = Arc::new(ScriptedUpstream::json(
        200,
        serde_json::json!({"temperature": 21}),
    ));
    let fixture = TestModules::with_upstream(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        upstream.clone(),
    )
    .await;
    let tool = fixture.user_tool("user:race", "Approval race").await;
    fixture.require_approval(&tool).await;
    let approvals = fixture.approvals();
    let run = fixture
        .runs
        .start(&tool.principal, &tool.workspace_id, submission("invoke"))
        .await
        .unwrap();
    let request = wait_for_approval(
        &approvals,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    let second = approvals.clone();
    let trace = TraceId::parse(&"a".repeat(32)).unwrap();
    let (approved, declined) = tokio::join!(
        approvals.decide(
            &tool.principal,
            &tool.workspace_id,
            &request.id,
            ApprovalDecision::Approve,
            Some("first-canary"),
            &trace
        ),
        second.decide(
            &tool.principal,
            &tool.workspace_id,
            &request.id,
            ApprovalDecision::Decline,
            Some("second-canary"),
            &trace
        ),
    );
    let winner = approved.unwrap();
    assert_eq!(winner, declined.unwrap());
    assert!(matches!(
        winner.state,
        ApprovalState::Approved | ApprovalState::Declined
    ));
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert_eq!(
        upstream.sends.load(Ordering::SeqCst),
        usize::from(winner.state == ApprovalState::Approved)
    );
    assert_eq!(
        retained_events(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            terminal.run_id()
        )
        .await
        .iter()
        .filter(|event| matches!(event.kind(), RunEventKind::InvocationEnded { .. }))
        .count(),
        1
    );
    assert_eq!(
        approvals
            .list_lifecycle_events(&tool.principal, &tool.workspace_id, &request.id)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn approval_deadline_follows_the_run_timeout_and_stays_decidable_until_it() {
    let upstream = Arc::new(ScriptedUpstream::json(
        200,
        serde_json::json!({"temperature": 21}),
    ));
    let mut fixture = TestModules::with_upstream(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        upstream.clone(),
    )
    .await;
    // Advance one shared clock without turning this into an unrelated lost-worker test.
    fixture.runs.execution_policy =
        crate::execution::ExecutionPolicy::testing_with_claim_lease(Duration::from_mins(15));
    let tool = fixture
        .user_tool("user:human-review-budget", "Review within the Run timeout")
        .await;
    fixture.require_approval(&tool).await;
    let approvals = fixture.approvals();
    let run = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            RunSubmission::new(
                ProgramSource::parse("invoke-tool").unwrap(),
                RunTimeout::from_millis(900_000).unwrap(),
            ),
        )
        .await
        .unwrap();
    let pending = wait_for_approval(
        &approvals,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    let accepted_at = fixture
        .runs
        .inspect_run(
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
            &crate::RunInspection {
                event_limit: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .summary
        .started_at;
    assert_eq!(
        pending.deadline_at_unix_nanos,
        accepted_at + 900_000_000_000
    );
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
    fixture
        .clock
        .next
        .store(accepted_at + 70_000_000_000, Ordering::SeqCst);
    let review = approvals
        .get(&tool.principal, &tool.workspace_id, &pending.id)
        .await
        .unwrap();
    assert_eq!(review.state, ApprovalState::Pending);
    assert!(review.actionable);
    assert_eq!(
        review.deadline_at_unix_nanos, pending.deadline_at_unix_nanos,
        "inspection does not renew the accepted budget"
    );
    let decided = approvals
        .decide(
            &tool.principal,
            &tool.workspace_id,
            &pending.id,
            ApprovalDecision::Approve,
            None,
            &TraceId::parse(&"a".repeat(32)).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(decided.state, ApprovalState::Approved);
    let decided_at = decided.decided_at_unix_nanos.unwrap();
    assert!(decided_at > accepted_at + 60_000_000_000);
    assert!(decided_at < pending.deadline_at_unix_nanos);
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert_eq!(terminal.state(), RunState::Succeeded);
    assert_eq!(
        upstream.sends.load(Ordering::SeqCst),
        1,
        "the same accepted call dispatches once without replay"
    );
}

/// The owner's timer for an Approval Request expires it: the request is
/// recorded Expired with no approver, dated at its deadline, its Invocation
/// ends `approval_expired`, every reader shows the record (no action, not
/// actionable), and a decision that arrives after the timer fired is
/// answered with the recorded Expired; no Approved decision is written.
#[tokio::test]
async fn an_approval_deadline_timer_expires_the_request_and_a_later_decision_reads_expired() {
    let mut fixture = TestModules::new(Arc::new(
        crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
    ))
    .await;
    // The request's own deadline, well inside the Run's and the worker lease.
    let approval_wait = Duration::from_millis(200);
    fixture.runs.execution_policy =
        crate::execution::ExecutionPolicy::testing_with_approval_wait(approval_wait);
    let tool = fixture.user_tool("user:expiry", "Approval expiry").await;
    fixture.require_approval(&tool).await;
    let approvals = fixture.approvals();
    let started = fixture.clock.next.load(Ordering::SeqCst);
    let run = fixture.runs.start(&tool.principal, &tool.workspace_id, RunSubmission::new(
        ProgramSource::parse("try { await coral.providers.weather.getForecast({}); } catch (e) { return e.code; }").unwrap(),
        RunTimeout::from_millis(900_000).unwrap())).await.unwrap();
    let request = wait_for_approval(
        &approvals,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    // The deadline is the approval wait from the gate's clock sample, which
    // lies between the start and this read on a clock that only ticks.
    let approval_wait_nanos = i64::try_from(approval_wait.as_nanos()).unwrap();
    let created = request.deadline_at_unix_nanos - approval_wait_nanos;
    assert!(
        (started..=fixture.clock.next.load(Ordering::SeqCst)).contains(&created),
        "the request's deadline is the approval wait from its creation: {created} not in {started}..="
    );
    // The timer fires on the host clock; the test clock stays before the
    // deadline, so nothing but the owner's record can show the expiry.
    let read = || approvals.get(&tool.principal, &tool.workspace_id, &request.id);
    wait_until_async_within(Duration::from_secs(5), async || {
        read().await.unwrap().state != ApprovalState::Pending
    })
    .await;
    let expired = read().await.unwrap();
    assert_eq!(expired.state, ApprovalState::Expired);
    assert_eq!(expired.approver, None);
    assert!(!expired.actionable);
    assert_eq!(
        expired.decided_at_unix_nanos,
        Some(request.deadline_at_unix_nanos),
        "the expiry is dated at the deadline"
    );
    let queue = fixture
        .runs
        .required_actions(&tool.principal, None, None, 1)
        .await
        .unwrap();
    assert_eq!(queue.action_count, 0);
    assert_eq!(
        fixture
            .runs
            .required_action_count(&tool.principal, None)
            .await
            .unwrap(),
        0
    );
    assert!(queue.actions.is_empty());
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert_final_code(&terminal, "approval_expired");
    assert!(terminal.required_actions().is_empty());
    let exact = fixture
        .runs
        .inspect_run(
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
            &crate::RunInspection::default(),
        )
        .await
        .unwrap();
    let page = exact.summary.action_page.unwrap();
    assert_eq!(page.total, 0);
    assert!(page.actions.is_empty());
    // A decision after the timer: answered with the recorded Expired, and
    // nothing more is written.
    let decided = approvals
        .decide(
            &tool.principal,
            &tool.workspace_id,
            &request.id,
            ApprovalDecision::Approve,
            None,
            &TraceId::parse(&"e".repeat(32)).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(decided, expired);
    let events = approvals
        .list_lifecycle_events(&tool.principal, &tool.workspace_id, &request.id)
        .await
        .unwrap();
    let [event] = events.as_slice() else {
        panic!("one expiry event, no approval: {events:?}")
    };
    assert_eq!(event.actor, None);
    assert_eq!(event.state, ApprovalState::Expired);
    assert_eq!(event.occurred_at_unix_nanos, request.deadline_at_unix_nanos);
}

/// A request whose deadline passed on the clock but whose timer the owner
/// has not applied reads as recorded: pending, with its deadline, in the
/// request itself, the required actions and the Run's action page. No
/// reader derives Expired from the clock.
#[tokio::test]
async fn a_request_past_its_deadline_reads_pending_until_the_owner_records_its_expiry() {
    let mut fixture = TestModules::new(Arc::new(
        crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
    ))
    .await;
    // Isolate the request's deadline from the Run's: its timer (30 s on the
    // host clock) does not fire within this test.
    fixture.runs.execution_policy =
        crate::execution::ExecutionPolicy::testing_with_approval_wait(Duration::from_secs(30));
    let tool = fixture
        .user_tool("user:late-reader", "Approval past its deadline")
        .await;
    fixture.require_approval(&tool).await;
    let approvals = fixture.approvals();
    let run = fixture.runs.start(&tool.principal, &tool.workspace_id, RunSubmission::new(
        ProgramSource::parse("try { await coral.providers.weather.getForecast({}); } catch (e) { return e.code; }").unwrap(),
        RunTimeout::from_millis(900_000).unwrap())).await.unwrap();
    let request = wait_for_approval(
        &approvals,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    // The clock passes the deadline; the request's timer does not fire
    // within this test.
    fixture
        .clock
        .next
        .store(request.deadline_at_unix_nanos + 1, Ordering::SeqCst);
    let read = approvals
        .get(&tool.principal, &tool.workspace_id, &request.id)
        .await
        .unwrap();
    assert_eq!(read.state, ApprovalState::Pending);
    assert!(read.actionable, "the record decides, not the clock");
    assert_eq!(read.deadline_at_unix_nanos, request.deadline_at_unix_nanos);
    assert_eq!(read.decided_at_unix_nanos, None);
    let queue = fixture
        .runs
        .required_actions(&tool.principal, None, None, 1)
        .await
        .unwrap();
    assert_eq!(queue.action_count, 1);
    let [queued] = queue.actions.as_slice() else {
        panic!("the request is still a current action: {queue:?}")
    };
    assert_eq!(
        queued.action.request,
        crate::RequiredActionRequest::InvocationApproval(request.id.clone())
    );
    assert_eq!(
        queued.action.deadline_at_unix_nanos, request.deadline_at_unix_nanos,
        "the deadline is shown beside the recorded state"
    );
    let page = fixture
        .runs
        .inspect_run(
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
            &crate::RunInspection::default(),
        )
        .await
        .unwrap()
        .summary
        .action_page
        .unwrap();
    assert_eq!(page.total, 1);
    let observed = fixture
        .runs
        .observe(
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
            &immediate(None, 1),
        )
        .await
        .unwrap();
    let [action] = observed.required_actions() else {
        panic!("one required action: {observed:?}")
    };
    assert_eq!(
        action.deadline_at_unix_nanos,
        request.deadline_at_unix_nanos
    );
    assert_eq!(
        approvals
            .list_lifecycle_events(&tool.principal, &tool.workspace_id, &request.id)
            .await
            .unwrap(),
        Vec::new(),
        "reading writes nothing"
    );
}

/// A decision sent to the other Coral Instance and applied at the request
/// deadline on the owner's clock, before the request's timer fired, expires
/// the request instead of approving it: the owner applies the expiry first,
/// as its timer would have, and the decision reads the record.
#[tokio::test]
async fn a_decision_at_the_request_deadline_expires() {
    let mut fixture = TestModules::new(Arc::new(
        crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
    ))
    .await;
    // Isolate the request's deadline from the Run's: its timer (30 s on the
    // host clock) does not fire within this test.
    fixture.runs.execution_policy =
        crate::execution::ExecutionPolicy::testing_with_approval_wait(Duration::from_secs(30));
    let tool = fixture
        .user_tool("user:late-expiry", "Approval expiry under holds")
        .await;
    fixture.require_approval(&tool).await;
    let approvals = fixture.approvals();
    let run = fixture.runs.start(&tool.principal, &tool.workspace_id, RunSubmission::new(
        ProgramSource::parse("try { await coral.providers.weather.getForecast({}); } catch (e) { return e.code; }").unwrap(),
        RunTimeout::from_millis(900_000).unwrap())).await.unwrap();
    let request = wait_for_approval(
        &approvals,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    // The owner's clock reaches the deadline before the decision is applied;
    // the request's timer has not fired.
    fixture
        .clock
        .next
        .store(request.deadline_at_unix_nanos, Ordering::SeqCst);
    let expired = fixture
        .next_process()
        .approvals()
        .decide(
            &tool.principal,
            &tool.workspace_id,
            &request.id,
            ApprovalDecision::Approve,
            None,
            &TraceId::parse(&"f".repeat(32)).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(expired.state, ApprovalState::Expired);
    assert_eq!(expired.approver, None);
    assert_eq!(
        expired.decided_at_unix_nanos,
        Some(request.deadline_at_unix_nanos)
    );
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert_final_code(&terminal, "approval_expired");
    let events = approvals
        .list_lifecycle_events(&tool.principal, &tool.workspace_id, &request.id)
        .await
        .unwrap();
    assert!(
        matches!(events.as_slice(), [event] if event.state == ApprovalState::Expired),
        "no Approved decision is written: {events:?}"
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one wait lifecycle matrix verifies closure, policy relaxation and cancellation without touching required-action projections"
)]
async fn pending_approval_rechecks_live_policy_authority_and_run_termination() {
    for action in ["enable", "disable", "revoke", "archive", "cancel"] {
        let upstream = Arc::new(ScriptedUpstream::json(
            200,
            serde_json::json!({"temperature":21}),
        ));
        let mut fixture = TestModules::with_upstream(
            Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
            upstream.clone(),
        )
        .await;
        // Outlast the test: only a dispatched Invocation may hold cancellation open.
        fixture.runs.execution_policy =
            crate::execution::ExecutionPolicy::testing_with_shutdown_grace(Duration::from_mins(15));
        let tool = fixture
            .user_tool("user:wait-races", "Pending approval lifecycle")
            .await;
        fixture.require_approval(&tool).await;
        let approvals = fixture.approvals();
        let run = fixture.runs.start(&tool.principal, &tool.workspace_id, submission("try { await coral.providers.weather.getForecast({}); return 'sent'; } catch(e) { return e.code; }")).await.unwrap();
        let request = wait_for_approval(
            &approvals,
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
        )
        .await;
        let context = change(tool.user.clone(), 50, '5');
        match action {
            "enable" | "disable" => {
                fixture
                    .policies
                    .set_access_state(
                        &context,
                        tool.policy_target.clone(),
                        if action == "enable" {
                            ToolAccessState::Enabled
                        } else {
                            ToolAccessState::Disabled
                        },
                    )
                    .await
                    .unwrap();
            }
            "revoke" => {
                fixture
                    .providers
                    .disable_provider(&context, &tool.workspace_id.clone(), &tool.provider_id)
                    .await
                    .unwrap();
            }
            "archive" => {
                fixture
                    .workspaces
                    .archive(&context, &tool.workspace_id)
                    .await
                    .unwrap();
            }
            "cancel" => {
                let canceled = tokio::time::timeout(
                    Duration::from_secs(5),
                    fixture.runs.cancel(
                        &tool.principal,
                        &tool.workspace_id,
                        run.run_id(),
                        &immediate(None, 100),
                    ),
                )
                .await
                .expect("an undispatched Invocation does not hold cancellation open")
                .unwrap();
                assert_eq!(canceled.state(), RunState::Canceled);
            }
            _ => unreachable!(),
        }
        let terminal = wait_for_terminal(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
        )
        .await;
        if action == "cancel" {
            assert_eq!(terminal.state(), RunState::Canceled);
        } else {
            assert_final_code(
                &terminal,
                match action {
                    "enable" => "sent",
                    "disable" => "tool_disabled",
                    _ => "dispatch_authority_lost",
                },
            );
        }
        assert_eq!(
            upstream.sends.load(Ordering::SeqCst),
            usize::from(action == "enable")
        );
        assert_eq!(
            approvals
                .get(&tool.principal, &tool.workspace_id, &request.id)
                .await
                .unwrap()
                .state,
            ApprovalState::Closed
        );
        assert_eq!(
            approvals
                .list_lifecycle_events(&tool.principal, &tool.workspace_id, &request.id)
                .await
                .unwrap()
                .len(),
            1
        );
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one Run seam scenario covers Account-before-approval for both credential routes and forbids approval reuse"
)]
async fn approval_binds_the_selected_account_and_is_not_reusable_by_another_invocation() {
    use coral_credentials::Secret;
    for fixed in [false, true] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/forecast"))
            .and(wiremock::matchers::header(
                "authorization",
                "Bearer approval-secret-canary",
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"temperature":21})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let mut fixture = TestModules::with_upstream(
            Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
            Arc::new(
                crate::invocation::ProductionUpstream::new(
                    DestinationPolicy::default()
                        .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
                )
                .unwrap(),
            ),
        )
        .await;
        let mut tool = fixture
            .user_tool_at(
                "user:account-approval",
                "Select before approval",
                &server.uri(),
            )
            .await;
        let (_directory, accounts) = if fixed {
            Box::pin(configure_shared_account(
                &fixture,
                &mut tool,
                &server.uri(),
                "approval-secret-canary",
            ))
            .await
        } else {
            configure_user_caller(&fixture, &tool, &server.uri()).await
        };
        fixture.require_approval(&tool).await;
        fixture.runs = fixture.runs.clone().with_accounts(accounts.clone());
        let approvals = fixture.approvals();
        let context = change(tool.user.clone(), 50, '5');
        let scope = tool.workspace_id.clone();
        let shared_account_id = if fixed {
            let account_id = accounts
                .get_account(
                    &context,
                    &scope,
                    &tool.provider_id,
                    &coral_provider::account::AccountScope::Shared,
                )
                .await
                .unwrap()
                .account
                .unwrap()
                .id;
            Some(account_id)
        } else {
            None
        };
        let source = "return await coral.providers.weather.getForecast({});";
        let run = fixture
            .runs
            .start(&tool.principal, &tool.workspace_id, submission(source))
            .await
            .unwrap();
        let account_id = if let Some(account_id) = shared_account_id {
            account_id
        } else {
            wait_for_connection(&accounts, &tool).await;
            assert!(
                approvals
                    .list_for_run(&tool.principal, &tool.workspace_id, run.run_id())
                    .await
                    .unwrap()
                    .is_empty()
            );
            let authorization = accounts
                .start_user_authorization(
                    &context,
                    &tool.workspace_id,
                    &tool.provider_id,
                    ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                    None,
                )
                .await
                .unwrap();
            accounts
                .submit_secret(
                    &context,
                    &scope,
                    authorization.id,
                    coral_credentials::CredentialValues::token(
                        Secret::new("approval-secret-canary".into()).unwrap(),
                    ),
                )
                .await
                .unwrap()
                .account_id
                .unwrap()
        };
        let request = wait_for_approval(
            &approvals,
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
        )
        .await;
        assert_eq!(request.provider_account_id, Some(account_id));
        assert_eq!(
            request.credential_route,
            if fixed {
                CredentialRoute::Fixed
            } else {
                CredentialRoute::Caller
            }
        );
        assert!(server.received_requests().await.unwrap().is_empty());
        approvals
            .decide(
                &tool.principal,
                &tool.workspace_id,
                &request.id,
                ApprovalDecision::Approve,
                None,
                context.trace_id(),
            )
            .await
            .unwrap();
        let terminal = wait_for_terminal(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
        )
        .await;
        assert_eq!(terminal.state(), RunState::Succeeded);
        assert!(!format!("{terminal:?}{request:?}").contains("approval-secret-canary"));
        let later = fixture
            .runs
            .start(&tool.principal, &tool.workspace_id, submission(source))
            .await
            .unwrap();
        let new_request = wait_for_approval(
            &approvals,
            &tool.principal,
            &tool.workspace_id,
            later.run_id(),
        )
        .await;
        assert_ne!(new_request.id, request.id);
        assert_ne!(new_request.invocation_id, request.invocation_id);
        assert_eq!(new_request.state, ApprovalState::Pending);
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        fixture
            .runs
            .cancel(
                &tool.principal,
                &tool.workspace_id,
                later.run_id(),
                &immediate(None, 100),
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn approval_does_not_bypass_a_later_restrictive_policy_at_the_enforcement_point() {
    let upstream = Arc::new(ScriptedUpstream::json(
        200,
        serde_json::json!({"temperature":21}),
    ));
    let (gate, release) = EnforcementGate::blocking();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        gate.clone(),
    )
    .await;
    let tool = fixture
        .user_tool("user:approved-policy", "Approved but disabled")
        .await;
    fixture.require_approval(&tool).await;
    let approvals = fixture.approvals();
    let run = fixture.runs.start(&tool.principal, &tool.workspace_id, submission("try { await coral.providers.weather.getForecast({}); } catch (e) { return e.code; }")).await.unwrap();
    let request = wait_for_approval(
        &approvals,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    approvals
        .decide(
            &tool.principal,
            &tool.workspace_id,
            &request.id,
            ApprovalDecision::Approve,
            None,
            &TraceId::parse(&"a".repeat(32)).unwrap(),
        )
        .await
        .unwrap();
    wait_until(|| gate.entered()).await;
    fixture
        .policies
        .set_access_state(
            &change(tool.user.clone(), 50, '5'),
            tool.policy_target.clone(),
            ToolAccessState::Disabled,
        )
        .await
        .unwrap();
    release.send(()).unwrap();
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert_final_code(&terminal, "tool_disabled");
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
    assert_eq!(
        approvals
            .get(&tool.principal, &tool.workspace_id, &request.id)
            .await
            .unwrap()
            .state,
        ApprovalState::Approved
    );
}

/// A definite fault in a decision's write fails the owner's chunk, as any
/// row work's does: nothing of the decision is written (no decided
/// lifecycle event, no `invocation.ended` of its own), the Run ends
/// Interrupted with the request Closed by the terminal write, and the
/// approver's wait ends with the Run, answered from the record.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one decided event seeds the fault; the rest is the one scenario"
)]
async fn a_decision_persistence_fault_ends_the_run_and_records_no_partial_decision() {
    let fixture = TestModules::new(Arc::new(
        crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
    ))
    .await;
    let tool = fixture
        .user_tool("user:approval-fault", "Atomic approval evidence")
        .await;
    fixture.require_approval(&tool).await;
    let approvals = fixture.approvals();
    let source =
        "try { await coral.providers.weather.getForecast({}); } catch (e) { return e.code; }";
    let trace = TraceId::parse(&"f".repeat(32)).unwrap();
    // One decided lifecycle event in the Workspace, for the fault to collide
    // with.
    let first = fixture
        .runs
        .start(&tool.principal, &tool.workspace_id, submission(source))
        .await
        .unwrap();
    let request = wait_for_approval(
        &approvals,
        &tool.principal,
        &tool.workspace_id,
        first.run_id(),
    )
    .await;
    approvals
        .decide(
            &tool.principal,
            &tool.workspace_id,
            &request.id,
            ApprovalDecision::Decline,
            None,
            &trace,
        )
        .await
        .unwrap();
    wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        first.run_id(),
    )
    .await;
    approval_event_fault(&fixture, true).await;
    let run = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            RunSubmission::new(
                ProgramSource::parse(source).unwrap(),
                RunTimeout::from_millis(900_000).unwrap(),
            ),
        )
        .await
        .unwrap();
    let pending = wait_for_approval(
        &approvals,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;

    let closed = approvals
        .decide(
            &tool.principal,
            &tool.workspace_id,
            &pending.id,
            ApprovalDecision::Approve,
            None,
            &trace,
        )
        .await
        .expect("the record answers once the Run ends");
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert_eq!(terminal.state(), RunState::Interrupted);
    assert_eq!(closed.state, ApprovalState::Closed);
    assert_eq!(closed.approver, None, "no decision was recorded");
    assert!(
        approvals
            .list_lifecycle_events(&tool.principal, &tool.workspace_id, &pending.id)
            .await
            .unwrap()
            .iter()
            .map(|event| event.state)
            .eq([ApprovalState::Closed]),
        "the terminal write closed the request; the decision wrote nothing"
    );
    assert!(
        retained_events(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            run.run_id()
        )
        .await
        .iter()
        .filter_map(|event| match event.kind() {
            RunEventKind::InvocationEnded { result_kind, .. } => Some(*result_kind),
            _ => None,
        })
        .eq([crate::InvocationResultKind::RunTerminated]),
        "the Invocation ended with the Run, not with the decision"
    );
    approval_event_fault(&fixture, false).await;
}

/// Rejects a second decided (not Closed) lifecycle event in a Workspace:
/// the decision's own write fails, the terminal write's Closed does not.
async fn approval_event_fault(fixture: &TestModules, enabled: bool) {
    fixture
        .inject_fault(if enabled {
            "CREATE UNIQUE INDEX reject_second_decided_approval_event ON approval_lifecycle_events(workspace_id) WHERE state <> 'closed'"
        } else {
            "DROP INDEX reject_second_decided_approval_event"
        })
        .await;
}

/// Holds the enforcement point and, when set, moves time to `expire_at_checks`
/// once the enforcement point has loaded its final checks.
struct ApprovalEnforcementExpiry {
    /// Holds the Invocation as it is about to wait for the decision.
    wait: Arc<EnforcementGate>,
    enforcement: Arc<EnforcementGate>,
    clock: Arc<TestClock>,
    expire_at_checks: Option<i64>,
}

#[async_trait]
impl InvocationObserver for ApprovalEnforcementExpiry {
    fn enforcement_check_finished(&self, permit: bool, durable_start: bool) {
        self.enforcement
            .enforcement_check_finished(permit, durable_start);
    }

    async fn approval_waiting(&self) -> Result<(), Error> {
        self.wait.before_enforcement().await
    }

    async fn before_enforcement(&self) -> Result<(), Error> {
        self.enforcement.before_enforcement().await
    }

    async fn enforcement_checks_loaded(&self) {
        if let Some(at) = self.expire_at_checks {
            self.clock.next.store(at, Ordering::SeqCst);
        }
    }
}

/// An approved Invocation cannot dispatch once its approval expires, whether
/// the approval expired before the held gate read it or, with the deadline
/// the gate recorded in owner memory, while the enforcement point loaded its
/// final checks. The approval itself stays approved.
#[tokio::test]
async fn an_approval_that_expires_before_dispatch_cannot_dispatch() {
    const APPROVAL_DEADLINE: i64 = 1_000_000;
    for during_final_checks in [false, true] {
        let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
        let mut fixture = TestModules::with_upstream(
            Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
            upstream.clone(),
        )
        .await;
        let (wait, release_wait) = EnforcementGate::blocking();
        let (enforcement, release_enforcement) = EnforcementGate::blocking();
        fixture.runs.invocation_observer = Arc::new(ApprovalEnforcementExpiry {
            wait: wait.clone(),
            enforcement: enforcement.clone(),
            clock: fixture.clock.clone(),
            expire_at_checks: during_final_checks.then_some(APPROVAL_DEADLINE),
        });
        let tool = fixture
            .user_tool("user:approval-expiry", "Approval expiry")
            .await;
        fixture.require_approval(&tool).await;
        let approvals = fixture.approvals();
        let run = fixture.runs.start(&tool.principal, &tool.workspace_id, submission("try { await coral.providers.weather.getForecast({}); } catch (e) { return e.code; } return 'unexpected dispatch';")).await.unwrap();
        let request = wait_for_approval(
            &approvals,
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
        )
        .await;
        wait_until(|| wait.entered()).await;
        // Isolate approval expiry from the longer Run and worker-claim deadlines.
        // While the enforcement point loads its checks, the deadline the gate
        // records is the one the enforcement point's clock sample passes;
        // before the gate, the deadline has already passed when the gate reads
        // the approval.
        if during_final_checks {
            crate::approval_repository::set_approval_deadline(
                &fixture.database,
                &tool.workspace_id,
                &request.id,
                APPROVAL_DEADLINE,
            )
            .await
            .unwrap();
        }
        approvals
            .decide(
                &tool.principal,
                &tool.workspace_id,
                &request.id,
                ApprovalDecision::Approve,
                None,
                &TraceId::parse(&"a".repeat(32)).unwrap(),
            )
            .await
            .unwrap();
        if !during_final_checks {
            crate::approval_repository::set_approval_deadline(
                &fixture.database,
                &tool.workspace_id,
                &request.id,
                fixture.clock.next.load(Ordering::SeqCst),
            )
            .await
            .unwrap();
        }
        release_wait.send(()).unwrap();
        if during_final_checks {
            wait_until(|| enforcement.entered()).await;
        }
        release_enforcement.send(()).unwrap();
        let terminal = wait_for_terminal(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
        )
        .await;
        enforcement.enforcement_checks.assert_no_dispatch();
        assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
        assert_final_code(&terminal, "approval_expired");
        assert_eq!(
            approvals
                .get(&tool.principal, &tool.workspace_id, &request.id)
                .await
                .unwrap()
                .state,
            ApprovalState::Approved,
            "during final checks: {during_final_checks}"
        );
    }
}

struct ApprovalFailureGate(Arc<EnforcementGate>);
#[async_trait]
impl InvocationObserver for ApprovalFailureGate {
    async fn before_failure_commit(&self) -> Result<(), Error> {
        self.0.before_enforcement().await
    }
}

/// A decision reads no policy: a decline made after the Tool was
/// disabled is recorded as the decline, and the Invocation ends declined
/// even though the enforcement point already judged the policy failure.
#[tokio::test]
async fn a_decline_after_a_policy_change_is_recorded_as_the_decline() {
    let (gate, release) = EnforcementGate::blocking();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        Arc::new(RejectingUpstream),
        Arc::new(ApprovalFailureGate(gate.clone())),
    )
    .await;
    let tool = fixture
        .user_tool("user:decline-completion-race", "Recover winning rejection")
        .await;
    fixture.require_approval(&tool).await;
    let approvals = fixture.approvals();
    let run = fixture.runs.start(&tool.principal, &tool.workspace_id, submission("try { await coral.providers.weather.getForecast({}); } catch (e) { return e.code; }")).await.unwrap();
    let request = wait_for_approval(
        &approvals,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    fixture
        .policies
        .set_access_state(
            &change(tool.user.clone(), 50, '5'),
            tool.policy_target.clone(),
            ToolAccessState::Disabled,
        )
        .await
        .unwrap();
    wait_until(|| gate.entered()).await;
    approvals
        .decide(
            &tool.principal,
            &tool.workspace_id,
            &request.id,
            ApprovalDecision::Decline,
            None,
            &TraceId::parse(&"d".repeat(32)).unwrap(),
        )
        .await
        .unwrap();
    release.send(()).unwrap();
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert_final_code(&terminal, "approval_declined");
    assert_eq!(
        retained_events(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            terminal.run_id()
        )
        .await
        .iter()
        .filter(|event| matches!(event.kind(), RunEventKind::InvocationEnded { .. }))
        .count(),
        1
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one deterministic blocked-worker matrix checks both decision orders for policy changes and invoking Agent disablement"
)]
async fn first_durable_decision_wins_against_policy_changes_before_the_worker_observes_them() {
    for (policy_first, agent_case) in [(false, false), (true, false), (false, true), (true, true)] {
        let (gate, release) = EnforcementGate::blocking();
        let fixture = TestModules::with_invocation_adapters(
            Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
            Arc::new(RejectingUpstream),
            Arc::new(ApprovalWaitGate(gate.clone())),
        )
        .await;
        let tool = fixture
            .user_tool("user:decline-winner", "Keep durable denial")
            .await;
        let invoker = if agent_case {
            registered_agent(&fixture.workspaces, &tool.principal, &tool.workspace_id).await
        } else {
            tool.principal.clone()
        };
        fixture.require_approval(&tool).await;
        let approvals = fixture.approvals();
        let run = fixture.runs.start(&invoker, &tool.workspace_id, submission("try { await coral.providers.weather.getForecast({}); } catch (e) { return e.code; }")).await.unwrap();
        wait_until(|| gate.entered()).await;
        let request = wait_for_approval(
            &approvals,
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
        )
        .await;
        let context = change(tool.user.clone(), 50, '5');
        let disable = || async {
            if agent_case {
                fixture
                    .workspaces
                    .set_agent_state(
                        &context,
                        &tool.workspace_id,
                        invoker.principal().id(),
                        coral_workspace::AgentState::Disabled,
                    )
                    .await
                    .unwrap();
            } else {
                fixture
                    .policies
                    .set_access_state(
                        &context,
                        tool.policy_target.clone(),
                        ToolAccessState::Disabled,
                    )
                    .await
                    .unwrap();
            }
        };
        if policy_first {
            disable().await;
            assert_eq!(
                fixture
                    .runs
                    .required_actions(&tool.principal, None, None, 10)
                    .await
                    .unwrap()
                    .action_count,
                0
            );
            assert!(
                fixture
                    .runs
                    .inspect_run(
                        &tool.principal,
                        &tool.workspace_id,
                        run.run_id(),
                        &crate::RunInspection {
                            event_limit: 1,
                            ..Default::default()
                        }
                    )
                    .await
                    .unwrap()
                    .summary
                    .action_page
                    .unwrap()
                    .actions
                    .is_empty()
            );
            assert!(
                !approvals
                    .get(&tool.principal, &tool.workspace_id, &request.id)
                    .await
                    .unwrap()
                    .actionable
            );
        }
        let result = approvals
            .decide(
                &tool.principal,
                &tool.workspace_id,
                &request.id,
                ApprovalDecision::Decline,
                None,
                &TraceId::parse(&"d".repeat(32)).unwrap(),
            )
            .await
            .unwrap();
        // The decision reads no policy: the decline is the durable decision
        // in both orders, and the Invocation ends declined.
        assert_eq!(result.state, ApprovalState::Declined);
        if !policy_first {
            disable().await;
        }
        release.send(()).unwrap();
        let terminal = wait_for_terminal(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
        )
        .await;
        assert_final_code(&terminal, "approval_declined");
    }
}

/// While an Approval Request is pending, its Invocation is parked with its
/// Run Owner and holds nothing. Commits elsewhere ring the content-free
/// change signal, but an owner subscribes only for a connection wait, so
/// they wake nothing, read nothing and enforce nothing: the decision comes
/// through the owner, and the periodic precheck (15 s away here) is the
/// ceiling for everything else. Polling a snapshot per parked wait on every
/// commit used to starve the pool.
#[tokio::test]
async fn an_unrelated_commit_while_an_approval_is_pending_causes_no_enforcement_check_and_no_read()
{
    let wake_gap = Duration::from_secs(1);
    let (fixture, probe) = probed_fixture(
        crate::execution::ExecutionPolicy::testing()
            .with_wakeups(Duration::from_secs(15), wake_gap),
    )
    .await;
    let tool = fixture
        .user_tool("user:quiet-wait", "Wait without holding the Run")
        .await;
    let bystander = fixture
        .user_tool("user:quiet-bystander", "Commit elsewhere")
        .await;
    fixture.require_approval(&tool).await;
    let run = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("await coral.providers.weather.getForecast({});"),
        )
        .await
        .unwrap();
    // The held gate requests the approval and parks the Invocation.
    probe.until_parked(1).await;
    let (held, enforcement_checks) = (probe.held(), probe.enforcement_checks());
    for at in 0..3 {
        commit_elsewhere(&fixture, &bystander, at).await;
    }
    // Past the wake gap: a subscribed owner would have read by now.
    tokio::time::sleep(wake_gap + Duration::from_millis(500)).await;

    assert_eq!(probe.held() - held, 0, "held rechecks");
    assert_eq!(
        probe.enforcement_checks() - enforcement_checks,
        0,
        "enforcement checks"
    );
    assert_eq!(probe.parked(), 1, "the Invocation stays parked");
    assert_eq!(probe.prechecks(), Vec::new(), "snapshot reads");
    fixture
        .runs
        .cancel(
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
            &immediate(None, 100),
        )
        .await
        .unwrap();
}

/// A wait that became moot with no wake-up (the Workspace archived while a
/// decision is pending) is found by the periodic precheck alone: its
/// snapshot finds the authority lost, and the Invocation goes back to its
/// held gate, which refuses it under the enforcement point's shared holds and
/// closes the request. No enforcement check ever runs. The owner's timers run
/// on the host clock, so the test shortens the precheck's period instead of
/// advancing a clock 15 s.
#[tokio::test]
async fn a_wait_that_became_moot_is_found_by_the_periodic_precheck_and_its_request_closed() {
    let (fixture, probe) = probed_fixture(
        crate::execution::ExecutionPolicy::testing_with_claim_lease(Duration::from_secs(60))
            .with_wakeups(Duration::from_millis(300), Duration::from_secs(1)),
    )
    .await;
    let tool = fixture
        .user_tool("user:moot-wait", "A wait that became moot")
        .await;
    fixture.require_approval(&tool).await;
    let approvals = fixture.approvals();
    let run = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission(
                "try { await coral.providers.weather.getForecast({}); } catch (e) { return e.code; }",
            ),
        )
        .await
        .unwrap();
    let request = wait_for_approval(
        &approvals,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    probe.until_parked(1).await;
    let held = probe.held();
    fixture
        .workspaces
        .archive(&change(tool.user.clone(), 50, '5'), &tool.workspace_id)
        .await
        .unwrap();

    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert_final_code(&terminal, "dispatch_authority_lost");
    let prechecks = probe.prechecks();
    assert!(
        prechecks
            .iter()
            .all(|(check, _)| *check == crate::owner::Check::Periodic),
        "no change wake-up reads an approval wait: {prechecks:?}"
    );
    assert_eq!(
        prechecks.last(),
        Some(&(crate::owner::Check::Periodic, 1)),
        "the periodic precheck found the moot wait: {prechecks:?}"
    );
    assert_eq!(
        probe.held() - held,
        1,
        "one held recheck, after the finding"
    );
    assert_eq!(probe.enforcement_checks(), 0, "no enforcement check");
    assert_eq!(
        approvals
            .get(&tool.principal, &tool.workspace_id, &request.id)
            .await
            .unwrap()
            .state,
        ApprovalState::Closed
    );
}

/// A parked approval wait ends when the owner's timer records the expiry:
/// the chunk that records it ends the Invocation and wakes the parked wait,
/// with no read, long before the periodic precheck. The wait parks once.
#[tokio::test]
async fn a_pending_approval_expires_at_its_deadline_by_the_owners_timer() {
    // Isolate approval expiry from the capped Run deadline and worker-claim
    // expiry: the request expires two seconds after its creation, long
    // before the periodic precheck.
    let (fixture, probe) = probed_fixture(
        crate::execution::ExecutionPolicy::testing_with_approval_wait(Duration::from_secs(2))
            .with_wakeups(Duration::from_secs(15), Duration::from_secs(1)),
    )
    .await;
    let tool = fixture
        .user_tool("user:quiet-expiry", "Expire by the timer")
        .await;
    fixture.require_approval(&tool).await;
    let approvals = fixture.approvals();
    let run = fixture.runs.start(&tool.principal, &tool.workspace_id, RunSubmission::new(
        ProgramSource::parse("try { await coral.providers.weather.getForecast({}); } catch (e) { return e.code; }").unwrap(),
        RunTimeout::from_millis(900_000).unwrap())).await.unwrap();
    let request = wait_for_approval(
        &approvals,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    probe.until_parked(1).await;

    // Without the timer's chunk, the wait would sleep until the precheck.
    wait_until_async_within(Duration::from_secs(3), async || {
        fixture
            .run_record(&tool.workspace_id, run.run_id())
            .await
            .state
            .is_terminal()
    })
    .await;
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert_final_code(&terminal, "approval_expired");
    assert_eq!(
        approvals
            .get(&tool.principal, &tool.workspace_id, &request.id)
            .await
            .unwrap()
            .state,
        ApprovalState::Expired
    );
    assert_eq!(probe.parked(), 1, "parked once, woken once");
    assert_eq!(probe.prechecks(), Vec::new(), "no read woke it");
}

/// A fixture under `policy` whose Invocations and Run Owners one
/// [`WaitProbe`] watches; approval-gated calls go to an upstream that
/// rejects them.
async fn probed_fixture(
    policy: crate::execution::ExecutionPolicy,
) -> (TestModules, Arc<WaitProbe>) {
    let probe = WaitProbe::new();
    let mut fixture = TestModules::with_execution_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        Arc::new(RejectingUpstream),
        probe.clone(),
        probe.clone(),
    )
    .await;
    fixture.runs.execution_policy = policy;
    (fixture, probe)
}

/// Commits a policy change in another Workspace. Each commit rings every
/// change subscriber, as a busy deployment does.
async fn commit_elsewhere(fixture: &TestModules, bystander: &ToolFixture, at: i64) {
    fixture
        .policies
        .set_access_state(
            &change(bystander.user.clone(), 60 + at, 'b'),
            bystander.policy_target.clone(),
            if at % 2 == 0 {
                ToolAccessState::Disabled
            } else {
                ToolAccessState::Enabled
            },
        )
        .await
        .unwrap();
}

/// A fixture whose owner commits nothing on its own (a long lease, no idle
/// tick) and whose chunk commits a test can hold: one approval-gated call
/// through `engine`, sent to `upstream`.
async fn quiet_approval_fixture(
    engine: Arc<dyn ProgramEngine>,
    upstream: Arc<dyn UpstreamSender>,
    label: &str,
) -> (TestModules, ToolFixture, ApprovalModule, Arc<CommitProbe>) {
    let probe = CommitProbe::new();
    let mut fixture = TestModules::with_execution_adapters(
        engine,
        upstream,
        Arc::new(NoopInvocationObserver),
        probe.clone(),
    )
    .await;
    fixture.runs.execution_policy =
        crate::execution::ExecutionPolicy::testing_with_claim_lease(Duration::from_secs(60));
    let tool = fixture
        .user_tool(&format!("user:{label}"), "Owner-recorded decisions")
        .await;
    fixture.require_approval(&tool).await;
    let approvals = fixture.approvals();
    (fixture, tool, approvals, probe)
}

/// Two deciders on one Approval Request, one on each Coral Instance: the
/// first inserted wins and rides one chunk, the second finds its row
/// waiting, waits for that chunk and receives the first's recorded answer
/// (the decision, who made it, when), and so does a third, after the row is
/// gone. Nothing else changes: one lifecycle event, one dispatch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_deciders_on_one_request_are_answered_in_insert_order_and_the_loser_learns_the_winner()
{
    let upstream = Arc::new(ScriptedUpstream::json(
        200,
        serde_json::json!({"temperature": 21}),
    ));
    let (fixture, tool, approvals, probe) = quiet_approval_fixture(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        upstream.clone(),
        "two-deciders",
    )
    .await;
    let run = fixture
        .runs
        .start(&tool.principal, &tool.workspace_id, submission("invoke"))
        .await
        .unwrap();
    let request = wait_for_approval(
        &approvals,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    let trace = TraceId::parse(&"a".repeat(32)).unwrap();
    let release = probe.hold_next_commit();
    let elsewhere = fixture.next_process().approvals();
    let decide = |approvals: &ApprovalModule, decision: ApprovalDecision, comment: &'static str| {
        let approvals = approvals.clone();
        let actor = tool.principal.clone();
        let workspace = tool.workspace_id.clone();
        let id = request.id.clone();
        let trace = trace.clone();
        tokio::spawn(async move {
            approvals
                .decide(&actor, &workspace, &id, decision, Some(comment), &trace)
                .await
        })
    };
    let first = decide(&approvals, ApprovalDecision::Approve, "first");
    // The first decision's chunk is held before its transaction: the owner
    // has applied the first, and the second finds its row waiting.
    wait_until(|| probe.held()).await;
    let second = decide(&elsewhere, ApprovalDecision::Decline, "second");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !first.is_finished() && !second.is_finished(),
        "both are answered at Recorded"
    );
    release.send(()).expect("release the decision chunk");
    let first = first.await.expect("join").expect("the first decision");
    let second = second.await.expect("join").expect("the second decision");
    assert_eq!(first.state, ApprovalState::Approved);
    assert_eq!(first.approver.as_ref(), Some(tool.principal.principal()));
    assert_eq!(first.comment.as_deref(), Some("first"));
    assert!(first.decided_at_unix_nanos.is_some());
    assert_eq!(
        second, first,
        "the loser receives the winner's recorded answer"
    );
    assert_eq!(
        fixture.inbox_rows(&tool.workspace_id, run.run_id()).await,
        0
    );
    let third = elsewhere
        .decide(
            &tool.principal,
            &tool.workspace_id,
            &request.id,
            ApprovalDecision::Decline,
            Some("third"),
            &trace,
        )
        .await
        .expect("a repeat after the record");
    assert_eq!(third, first);
    let events = approvals
        .list_lifecycle_events(&tool.principal, &tool.workspace_id, &request.id)
        .await
        .unwrap();
    assert_eq!(
        events.iter().map(|event| event.state).collect::<Vec<_>>(),
        [ApprovalState::Approved],
        "one decision is recorded"
    );
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert_eq!(terminal.state(), RunState::Succeeded);
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
}

/// A decision on a terminated Run closes the obsolete request and records
/// no new Approved decision: while the owner settles the cancellation (a
/// dispatch of the other call still in flight), and once the Run is
/// terminal, where the record answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "two cases of one rule: a decision while the owner settles, and once the Run is terminal"
)]
async fn a_decision_on_a_terminated_run_closes_the_obsolete_request_and_records_no_approval() {
    // While settling: two gated calls, the first approved and held upstream.
    {
        let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(
            200,
            serde_json::json!({"temperature": 21}),
        )));
        let mut fixture = TestModules::with_upstream(
            Arc::new(
                crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default())
                    .expect("V8 engine"),
            ),
            upstream.clone(),
        )
        .await;
        fixture.runs.execution_policy =
            crate::execution::ExecutionPolicy::testing_with_shutdown_grace(Duration::from_secs(30));
        let tool = fixture
            .user_tool("user:decide-settling", "Decide while settling")
            .await;
        fixture.require_approval(&tool).await;
        let approvals = fixture.approvals();
        let run = fixture
            .runs
            .start(
                &tool.principal,
                &tool.workspace_id,
                submission(
                    "await Promise.all([coral.providers.weather.getForecast({}), coral.providers.weather.getForecast({})]); return 'done';",
                ),
            )
            .await
            .unwrap();
        wait_for_actions(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
            2,
            2,
        )
        .await;
        let requests = approvals
            .list_for_run(&tool.principal, &tool.workspace_id, run.run_id())
            .await
            .unwrap();
        let [dispatched, obsolete] = requests.as_slice() else {
            panic!("two requests: {requests:?}");
        };
        let trace = TraceId::parse(&"b".repeat(32)).unwrap();
        approvals
            .decide(
                &tool.principal,
                &tool.workspace_id,
                &dispatched.id,
                ApprovalDecision::Approve,
                None,
                &trace,
            )
            .await
            .unwrap();
        wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;
        let runs = fixture.runs.clone();
        let actor = tool.principal.clone();
        let workspace = tool.workspace_id.clone();
        let run_id = run.run_id().clone();
        let cancel = tokio::spawn(async move {
            runs.cancel(&actor, &workspace, &run_id, &immediate(None, 100))
                .await
        });
        wait_until_async(async || {
            fixture
                .run_record(&tool.workspace_id, run.run_id())
                .await
                .termination
                .is_some()
        })
        .await;
        let closed = approvals
            .decide(
                &tool.principal,
                &tool.workspace_id,
                &obsolete.id,
                ApprovalDecision::Approve,
                None,
                &trace,
            )
            .await
            .expect("the decision is answered from the record");
        assert_eq!(closed.state, ApprovalState::Closed);
        assert_eq!(closed.approver, None);
        assert!(!closed.actionable);
        assert_eq!(
            approvals
                .list_lifecycle_events(&tool.principal, &tool.workspace_id, &obsolete.id)
                .await
                .unwrap()
                .iter()
                .map(|event| event.state)
                .collect::<Vec<_>>(),
            [ApprovalState::Closed]
        );
        upstream.release.store(true, Ordering::SeqCst);
        let canceled = cancel.await.expect("join").expect("cancel");
        assert_eq!(canceled.state(), RunState::Canceled);
        assert_eq!(
            approvals
                .get(&tool.principal, &tool.workspace_id, &obsolete.id)
                .await
                .unwrap()
                .state,
            ApprovalState::Closed
        );
        assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
    }

    // Once terminal: the terminal chunk closed the request; the decision
    // reads it.
    {
        let (fixture, tool, approvals, _probe) = quiet_approval_fixture(
            Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
            Arc::new(RejectingUpstream),
            "decide-terminal",
        )
        .await;
        let run = fixture
            .runs
            .start(&tool.principal, &tool.workspace_id, submission("invoke"))
            .await
            .unwrap();
        let request = wait_for_approval(
            &approvals,
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
        )
        .await;
        let canceled = fixture
            .runs
            .cancel(
                &tool.principal,
                &tool.workspace_id,
                run.run_id(),
                &immediate(None, 100),
            )
            .await
            .unwrap();
        assert_eq!(canceled.state(), RunState::Canceled);
        let before = fixture.run_record(&tool.workspace_id, run.run_id()).await;
        let closed = approvals
            .decide(
                &tool.principal,
                &tool.workspace_id,
                &request.id,
                ApprovalDecision::Approve,
                None,
                &TraceId::parse(&"c".repeat(32)).unwrap(),
            )
            .await
            .expect("a terminal Run answers from the record");
        assert_eq!(closed.state, ApprovalState::Closed);
        assert_eq!(closed.approver, None);
        assert_eq!(
            approvals
                .list_lifecycle_events(&tool.principal, &tool.workspace_id, &request.id)
                .await
                .unwrap()
                .iter()
                .map(|event| event.state)
                .collect::<Vec<_>>(),
            [ApprovalState::Closed]
        );
        assert_eq!(
            fixture.run_record(&tool.workspace_id, run.run_id()).await,
            before,
            "nothing is written for a terminal Run"
        );
    }
}

/// An owner that dies right after its decision chunk landed: the requester
/// reads the decision from the record, and the supervisor interrupts the
/// Run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_owner_that_dies_after_recording_a_decision_leaves_its_requester_the_record() {
    let (fixture, tool, approvals, probe) = quiet_approval_fixture(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        Arc::new(ScriptedUpstream::json(200, serde_json::json!({}))),
        "owner-dies",
    )
    .await;
    let run = fixture
        .runs
        .start(&tool.principal, &tool.workspace_id, submission("invoke"))
        .await
        .unwrap();
    let request = wait_for_approval(
        &approvals,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    let release = probe.hold_next_commit();
    let deciding = {
        let approvals = approvals.clone();
        let actor = tool.principal.clone();
        let workspace = tool.workspace_id.clone();
        let id = request.id.clone();
        tokio::spawn(async move {
            approvals
                .decide(
                    &actor,
                    &workspace,
                    &id,
                    ApprovalDecision::Approve,
                    None,
                    &TraceId::parse(&"d".repeat(32)).unwrap(),
                )
                .await
        })
    };
    wait_until(|| probe.held()).await;
    // The next message the owner dequeues is its own commit's answer.
    probe.panic_on_next_message();
    release.send(()).expect("release the decision chunk");
    let recorded = deciding.await.expect("join").expect("the record answers");
    assert_eq!(recorded.state, ApprovalState::Approved);
    assert_eq!(recorded.approver.as_ref(), Some(tool.principal.principal()));
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert_eq!(terminal.state(), RunState::Interrupted);
}
