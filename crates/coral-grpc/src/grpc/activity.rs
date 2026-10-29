mod execution;
mod tree;
use super::{GrpcServices, management as wire, resource};
use coral_auth::PrivateAudience;
use coral_run::approval::ApprovalState as S;
use coral_task::{
    AttemptEvidence, AttemptKind, AttemptOutcome, InteractionId, TaskIdentity, TaskOutcome,
};
use coral_workspace::{HistoryScope, PrincipalId, WorkspaceId};
use tonic::{Request, Response, Status};
use wire::inspect_task_interaction_response::Evidence;

fn approval_state(state: S) -> &'static str {
    match state {
        S::Pending => "pending",
        S::Approved => "approved",
        S::Declined => "declined",
        S::Expired => "expired",
        S::Closed => "closed",
    }
}
fn error(error: coral_task::Error) -> Status {
    match error {
        coral_task::Error::TaskNotFound
        | coral_task::Error::InteractionNotFound
        | coral_task::Error::WorkspaceNotFound
        | coral_task::Error::Workspace(
            coral_workspace::Error::WorkspaceNotFound | coral_workspace::Error::AgentNotFound,
        ) => Status::not_found("history unavailable"),
        coral_task::Error::PermissionDenied(_)
        | coral_task::Error::Workspace(coral_workspace::Error::PermissionDenied(_)) => {
            Status::permission_denied("history unavailable")
        }
        coral_task::Error::InvalidInput(_) => Status::invalid_argument("invalid history query"),
        coral_task::Error::Run(error) => execution::run_error(error),
        coral_task::Error::TaskClosed => Status::failed_precondition("history unavailable"),
        // Name every remaining variant so a new domain error needs an explicit wire decision.
        coral_task::Error::SnapshotTooLarge
        | coral_task::Error::Search(_)
        | coral_task::Error::Functions(_)
        | coral_task::Error::Clock
        | coral_task::Error::IdentifierIssuance(_)
        | coral_task::Error::Workspace(_)
        | coral_task::Error::CorruptData(_)
        | coral_task::Error::Database(_) => Status::unavailable("history unavailable"),
    }
}
fn task_identity(name: &str) -> Result<TaskIdentity, Status> {
    let (workspace, id) = resource(name, "tasks")?;
    let task = coral_task::TaskId::parse(id)
        .map_err(|_invalid| Status::invalid_argument("invalid Task name"))?;
    Ok(TaskIdentity { workspace, task })
}
fn interaction_identity(name: &str) -> Result<(TaskIdentity, InteractionId), Status> {
    let (task, id) = name
        .rsplit_once("/interactions/")
        .ok_or_else(|| Status::invalid_argument("invalid interaction name"))?;
    Ok((
        task_identity(task)?,
        InteractionId::parse(id)
            .map_err(|_invalid| Status::invalid_argument("invalid interaction name"))?,
    ))
}
/// Parses an optional `workspaces/{id}` filter; empty means every authorized Workspace.
fn workspace_filter(name: &str) -> Result<Option<WorkspaceId>, Status> {
    if name.is_empty() {
        return Ok(None);
    }
    WorkspaceId::parse(name.strip_prefix("workspaces/").unwrap_or_default())
        .map(Some)
        .map_err(|_invalid| Status::invalid_argument("invalid Workspace name"))
}

fn scope(scope: &wire::ActivityScope) -> Result<HistoryScope, Status> {
    let workspace = workspace_filter(&scope.workspace)?;
    if scope.agent.is_empty() {
        return Ok(HistoryScope::Personal(workspace));
    }
    let (agent_workspace, agent) = resource(&scope.agent, "agents")?;
    if workspace.as_ref() != Some(&agent_workspace) {
        return Err(Status::invalid_argument("Agent Workspace mismatch"));
    }
    Ok(HistoryScope::Agent {
        workspace: agent_workspace,
        agent: PrincipalId::parse(agent)
            .map_err(|_invalid| Status::invalid_argument("invalid Agent name"))?,
    })
}
fn limit(value: u32) -> Result<u16, Status> {
    match value {
        0 => Ok(50),
        1..=100 => {
            u16::try_from(value).map_err(|_overflow| Status::invalid_argument("invalid page size"))
        }
        _ => Err(Status::invalid_argument("invalid page size")),
    }
}
fn task(task: &coral_task::Task) -> wire::ActivityTask {
    wire::ActivityTask {
        context_impact_bytes: task.context_impact_bytes(),
        context_impact_partial: task.context_impact_partial(),
        name: format!("workspaces/{}/tasks/{}", task.workspace_id(), task.id()),
        workspace: format!("workspaces/{}", task.workspace_id()),
        intent: task.intent().as_str().to_owned(),
        outcome: match task.outcome() {
            None => "open",
            Some(TaskOutcome::Success) => "success",
            Some(TaskOutcome::Failure) => "failure",
        }
        .into(),
        started_at_unix_nanos: task.started_at_unix_nanos(),
        ended_at_unix_nanos: task.ended_at_unix_nanos(),
        actor: task.principal().id().to_string(),
        agent: execution::agent(task.workspace_id(), task.principal()),
    }
}
fn interaction(parent: &str, value: &coral_task::InteractionSummary) -> wire::ActivityInteraction {
    wire::ActivityInteraction {
        position: None,
        context_impact_bytes: value.context_impact_bytes,
        name: format!("{parent}/interactions/{}", value.id),
        agent: execution::agent(&value.workspace, &value.principal),
        kind: match value.kind {
            AttemptKind::Search => "search",
            AttemptKind::Describe => "describe",
            AttemptKind::Save => "save",
            AttemptKind::Delete => "delete",
            AttemptKind::Submit => "submit",
        }
        .into(),
        intent: value.intent.as_str().into(),
        outcome: match value.outcome {
            AttemptOutcome::Unknown => "unknown",
            AttemptOutcome::Succeeded => "succeeded",
            AttemptOutcome::Rejected => "rejected",
        }
        .into(),
        started_at_unix_nanos: value.started_at_unix_nanos,
        completed_at_unix_nanos: value.completed_at_unix_nanos,
        run: None,
        accepted_run: value
            .run
            .as_ref()
            .map(|id| {
                format!(
                    "{}/runs/{id}",
                    parent.split("/tasks/").next().unwrap_or_default()
                )
            })
            .unwrap_or_default(),
    }
}

fn interaction_window(
    input: &wire::InspectTaskRequest,
    identity: &TaskIdentity,
) -> Result<coral_task::InteractionWindow, Status> {
    use coral_task::InteractionWindow as W;
    if [
        !input.after.is_empty(),
        !input.anchor.is_empty(),
        input.viewport.is_some(),
    ]
    .into_iter()
    .filter(|set| *set)
    .count()
        > 1
    {
        return Err(Status::invalid_argument("ambiguous interaction window"));
    }
    let child = |name: &str| -> Result<InteractionId, Status> {
        let (parent, id) = interaction_identity(name)?;
        if parent.workspace != identity.workspace || parent.task != identity.task {
            return Err(Status::invalid_argument("interaction Task mismatch"));
        }
        Ok(id)
    };
    if let Some(viewport) = &input.viewport {
        return Ok(W::Viewport {
            offset: viewport.offset,
            anchor: if viewport.anchor.is_empty() {
                None
            } else {
                Some(child(&viewport.anchor)?)
            },
            preceding: u16::try_from(viewport.preceding).map_err(|_overflow| {
                Status::invalid_argument("invalid viewport predecessor budget")
            })?,
        });
    }
    if !input.after.is_empty() {
        return Ok(W::After(child(&input.after)?));
    }
    if input.anchor.is_empty() {
        return Ok(W::Start);
    }
    if input.anchor.contains("/interactions/") {
        return Ok(W::At(child(&input.anchor)?));
    }
    let (workspace, id) = resource(&input.anchor, "runs")?;
    if workspace != identity.workspace {
        return Err(Status::invalid_argument("Run Workspace mismatch"));
    }
    Ok(W::Run(
        coral_run::RunId::parse(id).map_err(execution::run_error)?,
    ))
}

#[tonic::async_trait]
impl wire::activity_service_server::ActivityService for GrpcServices {
    type WatchActivityStream =
        tokio_stream::wrappers::ReceiverStream<Result<wire::WatchActivityResponse, Status>>;

    async fn watch_activity(
        &self,
        request: Request<wire::WatchActivityRequest>,
    ) -> Result<Response<Self::WatchActivityStream>, Status> {
        let actor = self
            .principal(&request, None, PrivateAudience::Reef)
            .await?;
        let mut observer = self
            .runs
            .observe_activity(&actor)
            .await
            .map_err(execution::run_error)?;
        let services = self.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        // The subscription predates the caller's first read, so that read is current.
        sender
            .try_send(Ok(wire::WatchActivityResponse { changed: true }))
            .map_err(|_full| Status::internal("activity stream unavailable"))?;
        tokio::spawn(async move {
            loop {
                let result = tokio::select! {
                    () = sender.closed() => break,
                    result = async {
                        let wake = observer.next().await.map_err(execution::run_error)?;
                        services.principal(&request, None, PrivateAudience::Reef).await?;
                        Ok(wire::WatchActivityResponse {
                            changed: wake == coral_run::ActivityWake::Changed,
                        })
                    } => result,
                };
                let failed = result.is_err();
                if sender.send(result).await.is_err() || failed {
                    break;
                }
            }
        });
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            receiver,
        )))
    }
    async fn browse_activity_roots(
        &self,
        request: Request<wire::BrowseActivityRootsRequest>,
    ) -> Result<Response<wire::BrowseActivityRootsResponse>, Status> {
        tree::roots(self, request).await
    }
    async fn browse_run_entries(
        &self,
        request: Request<wire::BrowseRunEntriesRequest>,
    ) -> Result<Response<wire::BrowseRunEntriesResponse>, Status> {
        tree::entries(self, request).await
    }
    async fn inspect_run_event(
        &self,
        request: Request<wire::InspectRunEventRequest>,
    ) -> Result<Response<wire::InspectRunEventResponse>, Status> {
        tree::event(self, request).await
    }
    async fn inspect_agent_history(
        &self,
        request: Request<wire::InspectAgentHistoryRequest>,
    ) -> Result<Response<wire::InspectAgentHistoryResponse>, Status> {
        let user = self.management_user(&request).await?;
        let input = request.into_inner();
        let (workspace, id) = resource(&input.name, "agents")?;
        let id = PrincipalId::parse(id).map_err(super::workspaces::error)?;
        let history = self
            .workspaces
            .inspect_agent_history(&user, &workspace, &id)
            .await
            .map_err(super::workspaces::error)?;
        Ok(Response::new(wire::InspectAgentHistoryResponse {
            name: format!("workspaces/{}/agents/{}", history.workspace, history.id),
            display_name: history.display_name.as_str().into(),
            deleted: history.deleted,
        }))
    }

    async fn count_required_actions(
        &self,
        request: Request<wire::CountRequiredActionsRequest>,
    ) -> Result<Response<wire::CountRequiredActionsResponse>, Status> {
        let actor = self
            .principal(&request, None, PrivateAudience::Reef)
            .await?;
        let workspace = workspace_filter(&request.into_inner().workspace)?;
        let action_count = self
            .runs
            .required_action_count(&actor, workspace.as_ref())
            .await
            .map_err(execution::run_error)?;
        Ok(Response::new(wire::CountRequiredActionsResponse {
            action_count,
        }))
    }

    async fn summarize_activity(
        &self,
        request: Request<wire::SummarizeActivityRequest>,
    ) -> Result<Response<wire::SummarizeActivityResponse>, Status> {
        let actor = self
            .principal(&request, None, PrivateAudience::Reef)
            .await?;
        let input = request.into_inner();
        let workspace = workspace_filter(&input.workspace)?
            .ok_or_else(|| Status::invalid_argument("Workspace is required"))?;
        let summary = self
            .tasks
            .activity_summary(&self.runs, &actor, &workspace, input.days)
            .await
            .map_err(error)?;
        Ok(Response::new(execution::summary(&workspace, summary)))
    }

    async fn browse_required_actions(
        &self,
        request: Request<wire::BrowseRequiredActionsRequest>,
    ) -> Result<Response<wire::BrowseRequiredActionsResponse>, Status> {
        let actor = self
            .principal(&request, None, PrivateAudience::Reef)
            .await?;
        let input = request.into_inner();
        let workspace = workspace_filter(&input.workspace)?;
        let after = if input.after.is_empty() {
            None
        } else {
            Some(coral_run::QueueCursor::parse(&input.after).map_err(execution::run_error)?)
        };
        let page = self
            .tasks
            .required_actions(
                &self.runs,
                &actor,
                workspace.as_ref(),
                after.as_ref(),
                limit(input.page_size)?,
            )
            .await
            .map_err(error)?;
        Ok(Response::new(wire::BrowseRequiredActionsResponse {
            action_count: page.action_count,
            actions: page
                .actions
                .iter()
                .map(|queued| {
                    let context = &queued.run;
                    let mut run = execution::run(&context.run);
                    run.intent = context
                        .intent
                        .as_ref()
                        .map(|intent| intent.as_str().to_owned())
                        .unwrap_or_default();
                    run.task = context
                        .task
                        .as_ref()
                        .map(|task| format!("workspaces/{}/tasks/{}", task.workspace, task.task))
                        .unwrap_or_default();
                    wire::ActivityRequiredAction {
                        action: Some(execution::action_summary(
                            &context.run.workspace,
                            &queued.action,
                        )),
                        run: Some(run),
                    }
                })
                .collect(),
            next_after: page
                .next
                .map(|cursor| cursor.to_string())
                .unwrap_or_default(),
        }))
    }
    async fn decide_approval(
        &self,
        request: Request<wire::DecideApprovalRequest>,
    ) -> Result<Response<wire::DecideApprovalResponse>, Status> {
        let actor = self
            .principal(&request, None, PrivateAudience::Reef)
            .await?;
        let input = request.into_inner();
        let (workspace, id) = resource(&input.name, "approvals")?;
        let id = coral_run::approval::ApprovalId::parse(id).map_err(execution::run_error)?;
        let decision = match input.decision.as_str() {
            "approve" => coral_run::approval::ApprovalDecision::Approve,
            "decline" => coral_run::approval::ApprovalDecision::Decline,
            _ => return Err(Status::invalid_argument("invalid approval decision")),
        };
        let result = self
            .runs
            .decide_approval(
                &actor,
                &workspace,
                &id,
                decision,
                input.comment.as_deref(),
                &coral_workspace::TraceId::current_or_new(),
            )
            .await
            .map_err(execution::run_error)?;
        Ok(Response::new(wire::DecideApprovalResponse {
            state: approval_state(result.state).into(),
        }))
    }
    async fn cancel_activity_run(
        &self,
        request: Request<wire::CancelActivityRunRequest>,
    ) -> Result<Response<wire::CancelActivityRunResponse>, Status> {
        let actor = self
            .principal(&request, None, PrivateAudience::Reef)
            .await?;
        if actor.authenticated_user().is_none() {
            return Err(Status::not_found("history unavailable"));
        }
        let input = request.into_inner();
        let (workspace, id) = resource(&input.name, "runs")?;
        let id = coral_run::RunId::parse(id).map_err(execution::run_error)?;
        let result = self
            .runs
            .cancel(
                &actor,
                &workspace,
                &id,
                &coral_run::RunObservation::immediate(None, 1).map_err(execution::run_error)?,
            )
            .await
            .map_err(execution::run_error)?;
        Ok(Response::new(wire::CancelActivityRunResponse {
            state: execution::state(result.state()),
        }))
    }
    async fn inspect_approval(
        &self,
        request: Request<wire::InspectApprovalRequest>,
    ) -> Result<Response<wire::InspectApprovalResponse>, Status> {
        let actor = self
            .principal(&request, None, PrivateAudience::Reef)
            .await?;
        let input = request.into_inner();
        let (workspace, id) = resource(&input.name, "approvals")?;
        let id = coral_run::approval::ApprovalId::parse(id).map_err(execution::run_error)?;
        let value = self
            .runs
            .inspect_approval(&actor, &workspace, &id)
            .await
            .map_err(execution::run_error)?;
        let approver_label = if let Some(approver) = &value.approver {
            if approver == actor.principal() {
                "You".into()
            } else {
                value
                    .approver_name
                    .unwrap_or_else(|| execution::UNAVAILABLE_USER.into())
            }
        } else {
            String::new()
        };
        Ok(Response::new(wire::InspectApprovalResponse {
            provider_label: value
                .provider_label
                .unwrap_or_else(|| execution::UNAVAILABLE_PROVIDER.into()),
            actor_label: execution::actor_label(
                &value.invoking_principal,
                value.agent_label.as_deref(),
            ),
            approver_label,
            name: input.name,
            run: format!("workspaces/{workspace}/runs/{}", value.run_id),
            invocation: format!("workspaces/{workspace}/invocations/{}", value.invocation_id),
            provider: format!("workspaces/{workspace}/providers/{}", value.provider_id),
            call_path: value.call_path,
            arguments_json: value.arguments.to_string(),
            state: approval_state(value.state).into(),
            actionable: value.actionable,
            deadline_at_unix_nanos: value.deadline_at_unix_nanos,
            approver: value
                .approver
                .map(|p| p.id().to_string())
                .unwrap_or_default(),
            decided_at_unix_nanos: value.decided_at_unix_nanos,
            comment: value.comment,
        }))
    }
    async fn inspect_run(
        &self,
        request: Request<wire::InspectRunRequest>,
    ) -> Result<Response<wire::InspectRunResponse>, Status> {
        execution::inspect_run(self, request).await
    }
    async fn inspect_invocation(
        &self,
        request: Request<wire::InspectInvocationRequest>,
    ) -> Result<Response<wire::InspectInvocationResponse>, Status> {
        execution::inspect_invocation(self, request).await
    }
    async fn inspect_task(
        &self,
        request: Request<wire::InspectTaskRequest>,
    ) -> Result<Response<wire::InspectTaskResponse>, Status> {
        let actor = self
            .principal(&request, None, PrivateAudience::Reef)
            .await?;
        let input = request.into_inner();
        let identity = task_identity(&input.name)?;
        let window = interaction_window(&input, &identity)?;
        let size = limit(input.page_size)?;
        let selection = input.scope.as_ref().map(scope).transpose()?;
        let page = self
            .tasks
            .activity_interactions(
                &self.runs,
                &actor,
                &identity,
                &window,
                size,
                selection.as_ref(),
            )
            .await
            .map_err(error)?;
        let next_after = page
            .next_after
            .map(|id| format!("{}/interactions/{id}", input.name))
            .unwrap_or_default();
        Ok(Response::new(wire::InspectTaskResponse {
            feedback: page.task.feedback().map(str::to_owned),
            offset: page.offset,
            anchor_unavailable: page.anchor_unavailable,
            total: page.total,
            task: Some(task(&page.task)),
            interactions: page
                .rows
                .into_iter()
                .enumerate()
                .map(|(index, row)| {
                    let mut summary = interaction(&input.name, &row.interaction);
                    summary.position = Some(page.offset + index as u64);
                    summary.run = row.run.as_ref().map(|run| {
                        let mut value = execution::run(run);
                        value.task.clone_from(&input.name);
                        value
                    });
                    summary
                })
                .collect(),
            next_after,
        }))
    }
    async fn inspect_task_interaction(
        &self,
        request: Request<wire::InspectTaskInteractionRequest>,
    ) -> Result<Response<wire::InspectTaskInteractionResponse>, Status> {
        let actor = self
            .principal(&request, None, PrivateAudience::Reef)
            .await?;
        let input = request.into_inner();
        let (identity, id) = interaction_identity(&input.name)?;
        let detail = self
            .tasks
            .interaction(&actor, &identity.workspace, &identity.task, &id)
            .await
            .map_err(error)?;
        let evidence = detail.evidence.map(|value| match value {
            AttemptEvidence::Discovery { context, response } => {
                Evidence::Discovery(wire::ActivityDiscovery {
                    context: Some(match context {
                        coral_task::DiscoveryContext::Search {
                            query,
                            provider,
                            limit,
                            cursor,
                        } => {
                            wire::activity_discovery::Context::Search(wire::ActivitySearchContext {
                                query,
                                provider,
                                limit: u32::from(limit),
                                cursor,
                            })
                        }
                        coral_task::DiscoveryContext::Describe { call_path, types } => {
                            wire::activity_discovery::Context::Describe(
                                wire::ActivityDescribeContext { call_path, types },
                            )
                        }
                    }),
                    response_json: response.to_string(),
                })
            }
            AttemptEvidence::Run(id) => {
                Evidence::Run(format!("workspaces/{}/runs/{id}", identity.workspace))
            }
            AttemptEvidence::SavedFunction(id) => Evidence::SavedFunction(format!(
                "workspaces/{}/savedFunctions/{id}",
                identity.workspace
            )),
            AttemptEvidence::Rejected(failure) => Evidence::Rejection(wire::ActivityRejection {
                code: failure.code,
                message: failure.message,
            }),
        });
        Ok(Response::new(wire::InspectTaskInteractionResponse {
            interaction: Some(interaction(
                &format!("workspaces/{}/tasks/{}", identity.workspace, identity.task),
                &detail.summary,
            )),
            evidence,
        }))
    }
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    /// History reads conceal absence and refusal alike and never report a
    /// denial, bad query, or closed Task as a retryable failure.
    #[test]
    fn history_errors_are_retryable_only_for_dependencies() {
        let cases = [
            (coral_task::Error::TaskNotFound, Code::NotFound),
            (coral_task::Error::InteractionNotFound, Code::NotFound),
            (coral_task::Error::WorkspaceNotFound, Code::NotFound),
            (
                coral_task::Error::Workspace(coral_workspace::Error::AgentNotFound),
                Code::NotFound,
            ),
            (
                coral_task::Error::PermissionDenied("private detail"),
                Code::PermissionDenied,
            ),
            (
                coral_task::Error::Workspace(coral_workspace::Error::PermissionDenied(
                    "private detail",
                )),
                Code::PermissionDenied,
            ),
            (
                coral_task::Error::InvalidInput("private detail".into()),
                Code::InvalidArgument,
            ),
            (coral_task::Error::TaskClosed, Code::FailedPrecondition),
            (
                coral_task::Error::Run(coral_run::Error::RunNotFound),
                Code::NotFound,
            ),
            (
                coral_task::Error::Run(coral_run::Error::ApprovalNotFound),
                Code::NotFound,
            ),
            (
                coral_task::Error::Run(coral_run::Error::PermissionDenied("private detail")),
                Code::PermissionDenied,
            ),
            (
                coral_task::Error::Run(coral_run::Error::InvalidInput("private detail".into())),
                Code::InvalidArgument,
            ),
            (
                coral_task::Error::Run(coral_run::Error::ProgramWorkerStopped),
                Code::Unavailable,
            ),
            // The Run Owner stopped before answering: the outcome is unknown
            // until the Run is read again.
            (
                coral_task::Error::Run(coral_run::Error::OwnerLost),
                Code::Aborted,
            ),
            (coral_task::Error::Clock, Code::Unavailable),
            (
                coral_task::Error::CorruptData("private detail".into()),
                Code::Unavailable,
            ),
        ];
        for (failure, code) in cases {
            let label = format!("{failure:?}");
            let status = super::error(failure);
            assert_eq!(status.code(), code, "{label}");
            assert!(!status.message().contains("private"), "{label}");
        }
        // A request that waits in the Run Inbox is retryable, with its reason.
        let waiting = super::error(coral_task::Error::Run(
            coral_run::Error::RunRequestNotYetRecorded,
        ));
        assert_eq!(
            (waiting.code(), waiting.message()),
            (Code::Unavailable, "RUN_REQUEST_NOT_YET_RECORDED")
        );
        assert!(
            !waiting.details().is_empty(),
            "an ErrorInfo names the reason"
        );
    }
}
