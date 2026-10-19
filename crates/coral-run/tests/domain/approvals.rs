use super::*;
use crate::approval::{ApprovalDecision, ApprovalState};
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
    let operation = fixture
        .user_operation("user:approver", "Approve weather")
        .await;
    let mut tx = fixture.database.begin().await.unwrap();
    coral_workspace::OrganizationModule::hold_profile_updates_in_transaction(&mut tx)
        .await
        .unwrap();
    coral_workspace::OrganizationModule::refresh_profile_in_transaction(
        &mut tx,
        operation.user.user_id(),
        coral_workspace::VerifiedUserProfile::new(Some("Reviewing user"), None, None, None),
        40,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    fixture.require_approval(&operation).await;
    let approvals = fixture.approvals();
    let accepted = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            submission("invoke-operation"),
        )
        .await
        .unwrap();
    let request = wait_for_approval(
        &approvals,
        &operation.principal,
        &operation.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert_eq!(request.state, ApprovalState::Pending);
    assert_eq!(request.provider_id, operation.provider_id);
    assert_eq!(request.invoking_principal, *operation.principal.principal());
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
    let decided = approvals
        .decide(
            &operation.principal,
            &operation.workspace_id,
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
                &operation.principal,
                &operation.workspace_id,
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
        &operation.principal,
        &operation.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert_eq!(terminal.state(), RunState::Succeeded);
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
    let events = approvals
        .list_lifecycle_events(&operation.principal, &operation.workspace_id, &request.id)
        .await
        .unwrap();
    let [event] = events.as_slice() else {
        panic!("one decision event: {events:?}")
    };
    assert_eq!(event.actor, Some(operation.principal.principal().clone()));
    assert_eq!(event.state, ApprovalState::Approved);
    assert_eq!(event.changed_field_paths, ["state"]);
}

#[tokio::test]
async fn declining_commits_invocation_failure_with_the_winning_decision() {
    let fixture = TestModules::new(Arc::new(
        crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
    ))
    .await;
    let operation = fixture
        .user_operation("user:decliner", "Decline exact invocation")
        .await;
    fixture.require_approval(&operation).await;
    let approvals = fixture.approvals();
    let run = fixture.runs.start(&operation.principal, &operation.workspace_id, submission("try { await coral.providers.weather.getForecast({city:'argument-canary'}); } catch (e) { return e.code; }"
    )).await.unwrap();
    let request = wait_for_approval(
        &approvals,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    assert_eq!(
        request.arguments,
        serde_json::json!({"city":"argument-canary"})
    );
    approvals
        .decide(
            &operation.principal,
            &operation.workspace_id,
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
            &operation.principal,
            &operation.workspace_id,
            run.run_id(),
            &immediate(None, 100),
        )
        .await
        .unwrap();
    assert!(
        retained_events(
            &fixture.runs,
            &operation.principal,
            &operation.workspace_id,
            update.run_id()
        )
        .await
        .iter()
        .any(|event| matches!(event.kind(), RunEventKind::InvocationEnded { .. })),
        "decline durably settles its child before returning"
    );
    let terminal = wait_for_terminal(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    assert_final_code(&terminal, "approval_declined");
    let events = approvals
        .list_lifecycle_events(&operation.principal, &operation.workspace_id, &request.id)
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
    operation: OperationFixture,
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
        let operation = fixture
            .user_operation("user:private", "Private approvals")
            .await;
        fixture.require_approval(&operation).await;
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
                    &change(operation.user.clone(), 50, '5'),
                    &operation.workspace_id,
                    &user,
                    role,
                )
                .await
                .unwrap();
            members.push(AuthenticatedPrincipal::user(user));
        }
        let [designated, member] = <[_; 2]>::try_from(members).unwrap();
        let agent = registered_agent(
            &fixture.workspaces,
            &operation.principal,
            &operation.workspace_id,
        )
        .await;
        Self {
            approvals: fixture.approvals(),
            fixture,
            operation,
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
            .start(actor, &self.operation.workspace_id, submission("invoke"))
            .await
            .unwrap()
            .run_id()
            .clone();
        let request =
            wait_for_approval(&self.approvals, reader, &self.operation.workspace_id, &run).await;
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
                &self.operation.workspace_id,
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
                &self.operation.workspace_id,
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
    let workspace = &audience.operation.workspace_id;
    let (run, request) = audience
        .pending(&audience.operation.principal, &audience.operation.principal)
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
            .get(&audience.operation.principal, workspace, &request.id)
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
    let user = &audience.operation.principal;
    let (run, request) = audience.pending(user, user).await;
    audience
        .set_workspace_role(user, &audience.operation.user, WorkspaceRole::Member, 60)
        .await;
    let declined = audience
        .decide(user, &request.id, ApprovalDecision::Decline)
        .await
        .unwrap();
    assert_eq!(declined.state, crate::approval::ApprovalState::Declined);
    wait_for_terminal(
        &audience.fixture.runs,
        user,
        &audience.operation.workspace_id,
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
    let workspace = &audience.operation.workspace_id;
    let creator = &audience.operation.principal;
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
    let workspace = &audience.operation.workspace_id;
    let creator = &audience.operation.principal;
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
    let operation = fixture
        .user_operation("user:retained-approval", "Retain approvals")
        .await;
    fixture.require_approval(&operation).await;
    let agent = registered_agent(
        &fixture.workspaces,
        &operation.principal,
        &operation.workspace_id,
    )
    .await;
    let approvals = fixture.approvals();
    let run = fixture
        .runs
        .start(
            &agent,
            &operation.workspace_id,
            submission("await coral.providers.weather.getForecast({city:'private-argument'});"),
        )
        .await
        .unwrap();
    let request = wait_for_approval(
        &approvals,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    let decision = approvals
        .decide(
            &operation.principal,
            &operation.workspace_id,
            &request.id,
            ApprovalDecision::Decline,
            Some("private-comment"),
            &TraceId::parse(&"a".repeat(32)).unwrap(),
        )
        .await
        .unwrap();
    let terminal = wait_for_terminal(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    let events = approvals
        .list_lifecycle_events(&operation.principal, &operation.workspace_id, &request.id)
        .await
        .unwrap();
    let context = change(operation.user.clone(), 50, '5');
    coral_administration::AccessLifecycleModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
    )
    .delete_agent(&context, &operation.workspace_id, agent.principal().id())
    .await
    .unwrap();
    fixture
        .providers
        .delete_provider(
            &context,
            &operation.workspace_id.clone(),
            &operation.provider_id,
        )
        .await
        .unwrap();
    assert_eq!(
        approvals
            .get(&operation.principal, &operation.workspace_id, &request.id)
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
            .list_lifecycle_events(&operation.principal, &operation.workspace_id, &request.id)
            .await
            .unwrap(),
        events
    );
    assert_eq!(
        wait_for_terminal(
            &fixture.runs,
            &operation.principal,
            &operation.workspace_id,
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
    let operation = fixture.user_operation("user:race", "Approval race").await;
    fixture.require_approval(&operation).await;
    let approvals = fixture.approvals();
    let run = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            submission("invoke"),
        )
        .await
        .unwrap();
    let request = wait_for_approval(
        &approvals,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    let second = approvals.clone();
    let trace = TraceId::parse(&"a".repeat(32)).unwrap();
    let (approved, declined) = tokio::join!(
        approvals.decide(
            &operation.principal,
            &operation.workspace_id,
            &request.id,
            ApprovalDecision::Approve,
            Some("first-canary"),
            &trace
        ),
        second.decide(
            &operation.principal,
            &operation.workspace_id,
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
        &operation.principal,
        &operation.workspace_id,
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
            &operation.principal,
            &operation.workspace_id,
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
            .list_lifecycle_events(&operation.principal, &operation.workspace_id, &request.id)
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
    let operation = fixture
        .user_operation("user:human-review-budget", "Review within the Run timeout")
        .await;
    fixture.require_approval(&operation).await;
    let approvals = fixture.approvals();
    let run = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            RunSubmission::new(
                ProgramSource::parse("invoke-operation").unwrap(),
                RunTimeout::from_millis(900_000).unwrap(),
            ),
        )
        .await
        .unwrap();
    let pending = wait_for_approval(
        &approvals,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    let accepted_at = fixture
        .runs
        .inspect_run(
            &operation.principal,
            &operation.workspace_id,
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
        .get(&operation.principal, &operation.workspace_id, &pending.id)
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
            &operation.principal,
            &operation.workspace_id,
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
        &operation.principal,
        &operation.workspace_id,
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

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "expiry agreement across Run counts, current cards, caller actions, exact decisions and terminal evidence"
)]
async fn expired_approval_cannot_be_approved_and_settles_with_a_safe_failure() {
    let fixture = TestModules::new(Arc::new(
        crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
    ))
    .await;
    let operation = fixture
        .user_operation("user:expiry", "Approval expiry")
        .await;
    fixture.require_approval(&operation).await;
    let approvals = fixture.approvals();
    let run = fixture.runs.start(&operation.principal, &operation.workspace_id, RunSubmission::new(
        ProgramSource::parse("try { await coral.providers.weather.getForecast({}); } catch (e) { return e.code; }").unwrap(),
        RunTimeout::from_millis(900_000).unwrap())).await.unwrap();
    let request = wait_for_approval(
        &approvals,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    // Isolate approval expiry from the capped Run deadline and worker-claim expiry.
    let approval_deadline = 1_000_000;
    crate::approval_repository::set_approval_deadline(
        &fixture.database,
        &operation.workspace_id,
        &request.id,
        approval_deadline,
    )
    .await
    .unwrap();
    // Advance the decision clock to the Request deadline, not the later Run deadline.
    // The worker remains alive on its own clock so this tests expiry, not lost claims.
    let approvals = approvals.with_clock(Arc::new(TestClock::starting_at(approval_deadline)));
    let stale = approvals
        .get(&operation.principal, &operation.workspace_id, &request.id)
        .await
        .unwrap();
    assert_eq!(
        stale.state,
        ApprovalState::Pending,
        "reading does not fabricate a terminal decision"
    );
    assert!(
        !stale.actionable,
        "durable pending is not current action availability"
    );
    // Sample every current projection after the same deadline without expiring the worker lease.
    fixture
        .clock
        .next
        .store(approval_deadline, Ordering::SeqCst);
    let queue = fixture
        .runs
        .required_actions(&operation.principal, None, None, 1)
        .await
        .unwrap();
    assert_eq!(queue.action_count, 0);
    assert_eq!(
        fixture
            .runs
            .required_action_count(&operation.principal, None)
            .await
            .unwrap(),
        0
    );
    assert!(queue.actions.is_empty());
    let exact = fixture
        .runs
        .inspect_run(
            &operation.principal,
            &operation.workspace_id,
            run.run_id(),
            &crate::RunInspection::default(),
        )
        .await
        .unwrap();
    let page = exact.summary.action_page.unwrap();
    assert_eq!(page.total, 0);
    assert!(page.actions.is_empty());
    assert!(
        fixture
            .runs
            .observe(
                &operation.principal,
                &operation.workspace_id,
                run.run_id(),
                &immediate(None, 1)
            )
            .await
            .unwrap()
            .required_actions()
            .is_empty()
    );
    let expired = approvals
        .decide(
            &operation.principal,
            &operation.workspace_id,
            &request.id,
            ApprovalDecision::Approve,
            None,
            &TraceId::parse(&"e".repeat(32)).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(expired.state, ApprovalState::Expired);
    assert_eq!(expired.approver, None);
    let terminal = wait_for_terminal(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    assert_final_code(&terminal, "approval_expired");
    assert_eq!(
        approvals
            .get(&operation.principal, &operation.workspace_id, &request.id)
            .await
            .unwrap(),
        expired
    );
    let events = approvals
        .list_lifecycle_events(&operation.principal, &operation.workspace_id, &request.id)
        .await
        .unwrap();
    let [event] = events.as_slice() else {
        panic!("one expiry event")
    };
    assert_eq!(event.actor, None);
    assert_eq!(event.state, ApprovalState::Expired);
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
        let operation = fixture
            .user_operation("user:wait-races", "Pending approval lifecycle")
            .await;
        fixture.require_approval(&operation).await;
        let approvals = fixture.approvals();
        let run = fixture.runs.start(&operation.principal, &operation.workspace_id, submission("try { await coral.providers.weather.getForecast({}); return 'sent'; } catch(e) { return e.code; }")).await.unwrap();
        let request = wait_for_approval(
            &approvals,
            &operation.principal,
            &operation.workspace_id,
            run.run_id(),
        )
        .await;
        let context = change(operation.user.clone(), 50, '5');
        match action {
            "enable" | "disable" => {
                fixture
                    .policies
                    .set_access_state(
                        &context,
                        operation.policy_target.clone(),
                        if action == "enable" {
                            OperationAccessState::Enabled
                        } else {
                            OperationAccessState::Disabled
                        },
                    )
                    .await
                    .unwrap();
            }
            "revoke" => {
                fixture
                    .providers
                    .disable_provider(
                        &context,
                        &operation.workspace_id.clone(),
                        &operation.provider_id,
                    )
                    .await
                    .unwrap();
            }
            "archive" => {
                fixture
                    .workspaces
                    .archive(&context, &operation.workspace_id)
                    .await
                    .unwrap();
            }
            "cancel" => {
                let canceled = tokio::time::timeout(
                    Duration::from_secs(5),
                    fixture.runs.cancel(
                        &operation.principal,
                        &operation.workspace_id,
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
            &operation.principal,
            &operation.workspace_id,
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
                    "disable" => "operation_disabled",
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
                .get(&operation.principal, &operation.workspace_id, &request.id)
                .await
                .unwrap()
                .state,
            ApprovalState::Closed
        );
        assert_eq!(
            approvals
                .list_lifecycle_events(&operation.principal, &operation.workspace_id, &request.id)
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
        let mut operation = fixture
            .user_operation_at(
                "user:account-approval",
                "Select before approval",
                &server.uri(),
            )
            .await;
        let (_directory, accounts) = if fixed {
            Box::pin(configure_shared_account(
                &fixture,
                &mut operation,
                &server.uri(),
                "approval-secret-canary",
            ))
            .await
        } else {
            configure_user_caller(&fixture, &operation, &server.uri()).await
        };
        fixture.require_approval(&operation).await;
        fixture.runs = fixture.runs.clone().with_accounts(accounts.clone());
        let approvals = fixture.approvals();
        let context = change(operation.user.clone(), 50, '5');
        let scope = operation.workspace_id.clone();
        let shared_account_id = if fixed {
            let account_id = accounts
                .get_account(
                    &context,
                    &scope,
                    &operation.provider_id,
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
            .start(
                &operation.principal,
                &operation.workspace_id,
                submission(source),
            )
            .await
            .unwrap();
        let account_id = if let Some(account_id) = shared_account_id {
            account_id
        } else {
            wait_for_connection(&accounts, &operation).await;
            assert!(
                approvals
                    .list_for_run(&operation.principal, &operation.workspace_id, run.run_id())
                    .await
                    .unwrap()
                    .is_empty()
            );
            let authorization = accounts
                .start_user_authorization(
                    &context,
                    &operation.workspace_id,
                    &operation.provider_id,
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
            &operation.principal,
            &operation.workspace_id,
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
                &operation.principal,
                &operation.workspace_id,
                &request.id,
                ApprovalDecision::Approve,
                None,
                context.trace_id(),
            )
            .await
            .unwrap();
        let terminal = wait_for_terminal(
            &fixture.runs,
            &operation.principal,
            &operation.workspace_id,
            run.run_id(),
        )
        .await;
        assert_eq!(terminal.state(), RunState::Succeeded);
        assert!(!format!("{terminal:?}{request:?}").contains("approval-secret-canary"));
        let later = fixture
            .runs
            .start(
                &operation.principal,
                &operation.workspace_id,
                submission(source),
            )
            .await
            .unwrap();
        let new_request = wait_for_approval(
            &approvals,
            &operation.principal,
            &operation.workspace_id,
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
                &operation.principal,
                &operation.workspace_id,
                later.run_id(),
                &immediate(None, 100),
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn approval_does_not_bypass_a_later_restrictive_policy_at_the_fence() {
    let upstream = Arc::new(ScriptedUpstream::json(
        200,
        serde_json::json!({"temperature":21}),
    ));
    let (gate, release) = FenceGate::blocking();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        gate.clone(),
    )
    .await;
    let operation = fixture
        .user_operation("user:approved-policy", "Approved but disabled")
        .await;
    fixture.require_approval(&operation).await;
    let approvals = fixture.approvals();
    let run = fixture.runs.start(&operation.principal, &operation.workspace_id, submission("try { await coral.providers.weather.getForecast({}); } catch (e) { return e.code; }")).await.unwrap();
    let request = wait_for_approval(
        &approvals,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    approvals
        .decide(
            &operation.principal,
            &operation.workspace_id,
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
            &change(operation.user.clone(), 50, '5'),
            operation.policy_target.clone(),
            OperationAccessState::Disabled,
        )
        .await
        .unwrap();
    release.send(()).unwrap();
    let terminal = wait_for_terminal(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    assert_final_code(&terminal, "operation_disabled");
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
    assert_eq!(
        approvals
            .get(&operation.principal, &operation.workspace_id, &request.id)
            .await
            .unwrap()
            .state,
        ApprovalState::Approved
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one persistence fault proves atomic rollback for approval, decline, and expiry through the public seam"
)]
async fn approval_event_failure_rolls_back_decision_and_invocation_settlement() {
    let fixture = TestModules::new(Arc::new(
        crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
    ))
    .await;
    let operation = fixture
        .user_operation("user:approval-fault", "Atomic approval evidence")
        .await;
    fixture.require_approval(&operation).await;
    let approvals = fixture.approvals();
    let source =
        "try { await coral.providers.weather.getForecast({}); } catch (e) { return e.code; }";
    let first = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            submission(source),
        )
        .await
        .unwrap();
    let request = wait_for_approval(
        &approvals,
        &operation.principal,
        &operation.workspace_id,
        first.run_id(),
    )
    .await;
    let trace = TraceId::parse(&"f".repeat(32)).unwrap();
    approvals
        .decide(
            &operation.principal,
            &operation.workspace_id,
            &request.id,
            ApprovalDecision::Decline,
            None,
            &trace,
        )
        .await
        .unwrap();
    wait_for_terminal(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
        first.run_id(),
    )
    .await;
    approval_event_fault(&fixture, true).await;
    let run = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            RunSubmission::new(
                ProgramSource::parse(source).unwrap(),
                RunTimeout::from_millis(900_000).unwrap(),
            ),
        )
        .await
        .unwrap();
    let pending = wait_for_approval(
        &approvals,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    // Isolate approval expiry from the capped Run deadline and worker-claim expiry.
    crate::approval_repository::set_approval_deadline(
        &fixture.database,
        &operation.workspace_id,
        &pending.id,
        1_000_000,
    )
    .await
    .unwrap();
    let pending = approvals
        .get(&operation.principal, &operation.workspace_id, &pending.id)
        .await
        .unwrap();
    for decision in [ApprovalDecision::Approve, ApprovalDecision::Decline] {
        assert!(matches!(
            approvals
                .decide(
                    &operation.principal,
                    &operation.workspace_id,
                    &pending.id,
                    decision,
                    None,
                    &trace
                )
                .await,
            Err(Error::Database(_))
        ));
        assert_eq!(
            approvals
                .get(&operation.principal, &operation.workspace_id, &pending.id)
                .await
                .unwrap(),
            pending
        );
    }
    let expired_clock = approvals
        .clone()
        .with_clock(Arc::new(TestClock::starting_at(
            pending.deadline_at_unix_nanos,
        )));
    assert!(matches!(
        expired_clock
            .decide(
                &operation.principal,
                &operation.workspace_id,
                &pending.id,
                ApprovalDecision::Approve,
                None,
                &trace
            )
            .await,
        Err(Error::Database(_))
    ));
    assert_eq!(
        approvals
            .get(&operation.principal, &operation.workspace_id, &pending.id)
            .await
            .unwrap(),
        pending
    );
    assert!(
        approvals
            .list_lifecycle_events(&operation.principal, &operation.workspace_id, &pending.id)
            .await
            .unwrap()
            .is_empty()
    );
    let update = fixture
        .runs
        .observe(
            &operation.principal,
            &operation.workspace_id,
            run.run_id(),
            &immediate(None, 100),
        )
        .await
        .unwrap();
    assert!(
        !retained_events(
            &fixture.runs,
            &operation.principal,
            &operation.workspace_id,
            update.run_id()
        )
        .await
        .iter()
        .any(|event| matches!(event.kind(), RunEventKind::InvocationEnded { .. }))
    );
    approval_event_fault(&fixture, false).await;
    approvals
        .decide(
            &operation.principal,
            &operation.workspace_id,
            &pending.id,
            ApprovalDecision::Decline,
            None,
            &trace,
        )
        .await
        .unwrap();
    assert_final_code(
        &wait_for_terminal(
            &fixture.runs,
            &operation.principal,
            &operation.workspace_id,
            run.run_id(),
        )
        .await,
        "approval_declined",
    );
}

async fn approval_event_fault(fixture: &TestModules, enabled: bool) {
    fixture
        .inject_fault(if enabled {
            "CREATE UNIQUE INDEX reject_second_approval_event ON approval_lifecycle_events(workspace_id)"
        } else {
            "DROP INDEX reject_second_approval_event"
        })
        .await;
}

/// Holds the fence and, when set, moves time to `expire_at_checks` once the fence
/// has loaded its final checks.
struct ApprovalFenceExpiry {
    gate: Arc<FenceGate>,
    clock: Arc<TestClock>,
    expire_at_checks: Option<i64>,
}

#[async_trait]
impl InvocationObserver for ApprovalFenceExpiry {
    fn fence_attempt_finished(&self, permit: bool, durable_start: bool) {
        self.gate.fence_attempt_finished(permit, durable_start);
    }

    async fn before_fence(&self) -> Result<(), Error> {
        self.gate.before_fence().await
    }

    fn fence_checks_loaded(&self) {
        if let Some(at) = self.expire_at_checks {
            self.clock.next.store(at, Ordering::SeqCst);
        }
    }
}

/// An approved Invocation cannot dispatch once its approval expires, whether the
/// approval expired before the fence or while the fence loaded its final checks.
/// The approval itself stays approved.
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
        let (gate, release) = FenceGate::blocking();
        fixture.runs.invocation_observer = Arc::new(ApprovalFenceExpiry {
            gate: gate.clone(),
            clock: fixture.clock.clone(),
            expire_at_checks: during_final_checks.then_some(APPROVAL_DEADLINE),
        });
        let operation = fixture
            .user_operation("user:approval-expiry", "Approval expiry")
            .await;
        fixture.require_approval(&operation).await;
        let approvals = fixture.approvals();
        let run = fixture.runs.start(&operation.principal, &operation.workspace_id, submission("try { await coral.providers.weather.getForecast({}); } catch (e) { return e.code; } return 'unexpected dispatch';")).await.unwrap();
        let request = wait_for_approval(
            &approvals,
            &operation.principal,
            &operation.workspace_id,
            run.run_id(),
        )
        .await;
        approvals
            .decide(
                &operation.principal,
                &operation.workspace_id,
                &request.id,
                ApprovalDecision::Approve,
                None,
                &TraceId::parse(&"a".repeat(32)).unwrap(),
            )
            .await
            .unwrap();
        wait_until(|| gate.entered()).await;
        // Isolate approval expiry from the longer Run and worker-claim deadlines.
        let deadline = if during_final_checks {
            APPROVAL_DEADLINE
        } else {
            fixture.clock.next.load(Ordering::SeqCst)
        };
        crate::approval_repository::set_approval_deadline(
            &fixture.database,
            &operation.workspace_id,
            &request.id,
            deadline,
        )
        .await
        .unwrap();
        release.send(()).unwrap();
        let terminal = wait_for_terminal(
            &fixture.runs,
            &operation.principal,
            &operation.workspace_id,
            run.run_id(),
        )
        .await;
        gate.fences.assert_no_dispatch();
        assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
        assert_final_code(&terminal, "approval_expired");
        assert_eq!(
            approvals
                .get(&operation.principal, &operation.workspace_id, &request.id)
                .await
                .unwrap()
                .state,
            ApprovalState::Approved,
            "during final checks: {during_final_checks}"
        );
    }
}

struct ApprovalFailureGate(Arc<FenceGate>);
#[async_trait]
impl InvocationObserver for ApprovalFailureGate {
    async fn before_failure_commit(&self) -> Result<(), Error> {
        self.0.before_fence().await
    }
}

#[tokio::test]
async fn stale_decline_cannot_replace_a_detected_policy_failure_before_worker_commit() {
    let (gate, release) = FenceGate::blocking();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        Arc::new(RejectingUpstream),
        Arc::new(ApprovalFailureGate(gate.clone())),
    )
    .await;
    let operation = fixture
        .user_operation("user:decline-completion-race", "Recover winning rejection")
        .await;
    fixture.require_approval(&operation).await;
    let approvals = fixture.approvals();
    let run = fixture.runs.start(&operation.principal, &operation.workspace_id, submission("try { await coral.providers.weather.getForecast({}); } catch (e) { return e.code; }")).await.unwrap();
    let request = wait_for_approval(
        &approvals,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    fixture
        .policies
        .set_access_state(
            &change(operation.user.clone(), 50, '5'),
            operation.policy_target.clone(),
            OperationAccessState::Disabled,
        )
        .await
        .unwrap();
    wait_until(|| gate.entered()).await;
    approvals
        .decide(
            &operation.principal,
            &operation.workspace_id,
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
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    assert_final_code(&terminal, "operation_disabled");
    assert_eq!(
        retained_events(
            &fixture.runs,
            &operation.principal,
            &operation.workspace_id,
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
        let (gate, release) = FenceGate::blocking();
        let fixture = TestModules::with_invocation_adapters(
            Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
            Arc::new(RejectingUpstream),
            Arc::new(ApprovalWaitGate(gate.clone())),
        )
        .await;
        let operation = fixture
            .user_operation("user:decline-winner", "Keep durable denial")
            .await;
        let invoker = if agent_case {
            registered_agent(
                &fixture.workspaces,
                &operation.principal,
                &operation.workspace_id,
            )
            .await
        } else {
            operation.principal.clone()
        };
        fixture.require_approval(&operation).await;
        let approvals = fixture.approvals();
        let run = fixture.runs.start(&invoker, &operation.workspace_id, submission("try { await coral.providers.weather.getForecast({}); } catch (e) { return e.code; }")).await.unwrap();
        wait_until(|| gate.entered()).await;
        let request = wait_for_approval(
            &approvals,
            &operation.principal,
            &operation.workspace_id,
            run.run_id(),
        )
        .await;
        let context = change(operation.user.clone(), 50, '5');
        let disable = || async {
            if agent_case {
                fixture
                    .workspaces
                    .set_agent_state(
                        &context,
                        &operation.workspace_id,
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
                        operation.policy_target.clone(),
                        OperationAccessState::Disabled,
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
                    .required_actions(&operation.principal, None, None, 10)
                    .await
                    .unwrap()
                    .action_count,
                0
            );
            assert!(
                fixture
                    .runs
                    .inspect_run(
                        &operation.principal,
                        &operation.workspace_id,
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
                    .get(&operation.principal, &operation.workspace_id, &request.id)
                    .await
                    .unwrap()
                    .actionable
            );
        }
        let result = approvals
            .decide(
                &operation.principal,
                &operation.workspace_id,
                &request.id,
                ApprovalDecision::Decline,
                None,
                &TraceId::parse(&"d".repeat(32)).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            result.state,
            if policy_first {
                ApprovalState::Closed
            } else {
                ApprovalState::Declined
            }
        );
        if !policy_first {
            disable().await;
        }
        release.send(()).unwrap();
        let terminal = wait_for_terminal(
            &fixture.runs,
            &operation.principal,
            &operation.workspace_id,
            run.run_id(),
        )
        .await;
        assert_final_code(
            &terminal,
            if policy_first {
                if agent_case {
                    "dispatch_authority_lost"
                } else {
                    "operation_disabled"
                }
            } else {
                "approval_declined"
            },
        );
    }
}

#[tokio::test]
async fn a_pending_approval_takes_no_held_recheck_for_unrelated_commits() {
    let probe = ApprovalWaitProbe::new();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        Arc::new(RejectingUpstream),
        probe.clone(),
    )
    .await;
    let operation = fixture
        .user_operation("user:quiet-wait", "Wait without holding the Run")
        .await;
    let bystander = fixture
        .user_operation("user:quiet-bystander", "Commit elsewhere")
        .await;
    fixture.require_approval(&operation).await;
    let run = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            submission("await coral.providers.weather.getForecast({});"),
        )
        .await
        .unwrap();
    // The first held recheck requests the approval before the wait parks.
    probe.until_parked(1).await;
    let before = probe.held();
    // Every commit wakes every waiter. Only the snapshot precheck keeps these
    // wake-ups from becoming held rechecks.
    for at in 0..3 {
        let parked = probe.parked();
        commit_elsewhere(&fixture, &bystander, at).await;
        probe.until_parked(parked + 1).await;
    }

    // Each held recheck locks the Run, Workspace, User, Provider, policy and
    // Account rows. Polling them on a short timer starved the connection pool.
    assert_eq!(
        probe.held() - before,
        0,
        "held rechecks while only another Workspace changed"
    );
    fixture
        .runs
        .cancel(
            &operation.principal,
            &operation.workspace_id,
            run.run_id(),
            &immediate(None, 100),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn a_pending_approval_expires_at_its_deadline_without_a_commit() {
    let (probe, release) = ApprovalWaitProbe::gated();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        Arc::new(RejectingUpstream),
        probe.clone(),
    )
    .await;
    let operation = fixture
        .user_operation("user:quiet-expiry", "Expire without a commit")
        .await;
    fixture.require_approval(&operation).await;
    let approvals = fixture.approvals();
    let run = fixture.runs.start(&operation.principal, &operation.workspace_id, RunSubmission::new(
        ProgramSource::parse("try { await coral.providers.weather.getForecast({}); } catch (e) { return e.code; }").unwrap(),
        RunTimeout::from_millis(900_000).unwrap())).await.unwrap();
    let request = wait_for_approval(
        &approvals,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    // Isolate approval expiry from the capped Run deadline and worker-claim expiry.
    // The gate keeps the wait from taking its first snapshot until the deadline is set.
    let approval_deadline = 1_000_000;
    crate::approval_repository::set_approval_deadline(
        &fixture.database,
        &operation.workspace_id,
        &request.id,
        approval_deadline,
    )
    .await
    .unwrap();
    release.send(()).unwrap();
    // The approval request's own commit wakes the wait once. After that round,
    // only the deadline or the fallback poll wakes it.
    probe.until_parked(2).await;
    let held = probe.held();
    fixture
        .clock
        .next
        .store(approval_deadline, Ordering::SeqCst);

    // Without the deadline wake-up, the wait would sleep until its 5 s fallback poll.
    probe.until_held(held + 1, Duration::from_secs(3)).await;
    let terminal = wait_for_terminal(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    assert_final_code(&terminal, "approval_expired");
}

/// Commits a policy change in another Workspace. Each commit wakes every
/// change subscriber, as a busy deployment does.
async fn commit_elsewhere(fixture: &TestModules, bystander: &OperationFixture, at: i64) {
    fixture
        .policies
        .set_access_state(
            &change(bystander.user.clone(), 60 + at, 'b'),
            bystander.policy_target.clone(),
            if at % 2 == 0 {
                OperationAccessState::Disabled
            } else {
                OperationAccessState::Enabled
            },
        )
        .await
        .unwrap();
}
