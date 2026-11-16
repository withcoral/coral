use std::borrow::Cow;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use coral_run::{RUN_EVENT_PAGE_MAX, RunCursor, RunId, RunObservation};
use coral_task::{TaskId, TaskIntent, TaskOutcome};
use coral_telemetry::safe;
use coral_workspace::WorkspaceId;
use rmcp::model::{
    CacheScope, CallToolRequestParams, CallToolResponse, CallToolResult, Implementation,
    ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, PaginatedRequestParams,
    ProgressNotificationParam, ProgressToken, ProtocolVersion, ReadResourceRequestParams,
    ReadResourceResponse, ServerCapabilities, ServerConfig, Tool,
};
use rmcp::service::{MaybeSendFuture, Peer, RequestContext, RoleServer};
use rmcp::{ErrorData, ServerHandler};

use crate::McpRuntime;
use crate::arguments::{
    self, CancelArguments, EndTaskArguments, ExecArguments, TaskOutcomeArgument, ToolArguments,
    WaitArguments,
};
use crate::projection::{self, Reporter, ToolFailure};

#[cfg(test)]
mod tests;
mod tool_call;

use tool_call::ToolCall;

/// MCP protocol versions this Adapter serves, oldest first.
///
/// This is the tested window, not everything RMCP knows. `server/discover`
/// advertises exactly this list. An `initialize` handshake exists only before
/// 2026-07-28, so a client asking over it for a newer version, or for a legacy
/// version outside the window, lands on the oldest entry, which is also the
/// version with the fewest wire obligations. Add a version here only together
/// with its raw-wire test coverage; drop the oldest once no supported client
/// sends it.
pub const SUPPORTED_PROTOCOL_VERSIONS: &[ProtocolVersion] =
    &[ProtocolVersion::V_2025_11_25, ProtocolVersion::V_2026_07_28];

/// The version an `initialize` handshake lands on when the requested version
/// is outside the window or, from 2026-07-28, has no such handshake at all.
fn fallback_protocol_version() -> ProtocolVersion {
    SUPPORTED_PROTOCOL_VERSIONS
        .first()
        .expect("the protocol window is never empty")
        .clone()
}

#[derive(Clone)]
pub(crate) struct McpServer {
    runtime: McpRuntime,
    workspace_id: WorkspaceId,
    title: Option<String>,
}

impl McpServer {
    pub(crate) const fn new(runtime: McpRuntime, workspace_id: WorkspaceId) -> Self {
        Self {
            runtime,
            workspace_id,
            title: None,
        }
    }

    /// Names the Workspace in `serverInfo.title`, for a client that connected
    /// at the Organization MCP Endpoint and cannot read it from the URL. The
    /// name is display text only, and long names are cut short.
    pub(crate) fn naming_workspace(mut self, display_name: &str) -> Self {
        const NAME_CHARS: usize = 120;
        let name = display_name.chars().take(NAME_CHARS).collect::<String>();
        self.title = Some(format!("Coral · {name}"));
        self
    }

    /// The tools this caller is served. `exec` names the namespaces the
    /// caller can search, so an agent passes `provider` from its first
    /// Program on. A caller that lost the Workspace meets that on every call,
    /// so the list fails too. Any other failure only costs the namespaces:
    /// `exec`'s description says so and tells the agent to leave `provider`
    /// out.
    async fn listed_tools(&self) -> Result<Vec<Tool>, ErrorData> {
        let searchable = self
            .runtime
            .modules
            .search
            .searchable_providers(&self.runtime.principal, &self.workspace_id)
            .await;
        let namespaces = match &searchable {
            Ok(providers) => arguments::Namespaces::Listed(providers),
            Err(error) if error.lost_workspace_authority() => {
                return Err(ErrorData::invalid_request("Permission was denied", None));
            }
            Err(error) => {
                tracing::error!(
                    coral.workspace.id = safe::id(&self.workspace_id),
                    error.type = safe::kind(lookup_error_kind(error)),
                    "failed to list the namespaces for exec's description"
                );
                arguments::Namespaces::Unavailable
            }
        };
        Ok(arguments::tools(arguments::exec_description(&namespaces)))
    }

    async fn dispatch(
        &self,
        request: CallToolRequestParams,
        progress: Option<Progress>,
    ) -> Result<CallToolResponse, DispatchError> {
        // Coral asks a client no in-band question, so no call is a retry that
        // answers one: a Run that waits on a person reports it in the Run
        // Update, and the person decides on the page its link opens.
        if request.request_state.is_some() || request.input_responses.is_some() {
            return Err(invalid_input_message(
                "tool does not accept requestState or inputResponses".to_owned(),
            ));
        }
        let name = request.name.as_ref();
        let complete = match name {
            ExecArguments::NAME => {
                let arguments = arguments::parse::<ExecArguments>(request.arguments)?;
                return self.exec(arguments).await;
            }
            WaitArguments::NAME => {
                let arguments = arguments::parse::<WaitArguments>(request.arguments)?;
                return self.wait(arguments, progress).await;
            }
            CancelArguments::NAME => {
                let arguments = arguments::parse::<CancelArguments>(request.arguments)?;
                self.cancel(arguments).await
            }
            EndTaskArguments::NAME => {
                let arguments = arguments::parse::<EndTaskArguments>(request.arguments)?;
                self.end_task(arguments).await
            }
            _ => Err(DispatchError::Protocol(ErrorData::invalid_params(
                "tool not found",
                None,
            ))),
        };
        complete.map(Into::into)
    }

    async fn exec(&self, arguments: ExecArguments) -> Result<CallToolResponse, DispatchError> {
        let target = match (&arguments.task_id, &arguments.goal) {
            (Some(task_id), None) => {
                ExecTarget::Task(self.attributed_command(task_id, &arguments.intent)?)
            }
            (None, Some(goal)) => ExecTarget::Goal(coral_task::NewTaskCommand::new(
                self.runtime.principal.clone(),
                self.workspace_id.clone(),
                // The call's own `intent` fails with the same text, so name
                // which of the two this is.
                TaskIntent::parse(goal)
                    .map_err(|error| invalid_input(format!("`goal`: {error}")))?,
                TaskIntent::parse(&arguments.intent).map_err(invalid_input)?,
            )),
            _ => {
                return Err(invalid_input_message(
                    "exec takes exactly one of `task_id` or `goal`: pass `task_id` to continue a Task, or `goal` to start a new one".to_owned(),
                ));
            }
        };
        let timeout = arguments
            .run_timeout_ms
            .unwrap_or(arguments::DEFAULT_RUN_TIMEOUT_MS);
        let wait_ms = validated_wait(arguments.wait_ms.unwrap_or(arguments::DEFAULT_EXEC_WAIT_MS))?;
        // Built before acceptance, so a refused wait accepts no Run. It links
        // no trace: this request accepts the Run, and acceptance links it to
        // the Run Trace.
        let observation = (wait_ms > 0)
            .then(|| RunObservation::initial(Duration::from_millis(wait_ms)))
            .transpose()
            .map_err(invalid_input)?
            .map(RunObservation::without_trace_link);
        let (started, accepted) = match target {
            ExecTarget::Task(call) => {
                let accepted = self
                    .runtime
                    .modules
                    .commands
                    .submit(&call, &arguments.program, timeout)
                    .await
                    .map_err(task_failure)?;
                (None, accepted)
            }
            ExecTarget::Goal(call) => {
                let (task, launched) = self
                    .runtime
                    .modules
                    .commands
                    .submit_new_task(&call, &arguments.program, timeout)
                    .await
                    .map_err(task_failure)?;
                let accepted = launched
                    .map_err(|error| started_task_failure(task_failure(error), task.id()))?;
                (Some(task), accepted)
            }
        };
        // The Run is accepted, so every answer from here names it. A caller
        // without its `run_id` can neither read nor cancel the Run, and
        // another `exec` runs the Program again.
        let observed = match &observation {
            Some(observation) => match self.observe(accepted.run_id(), observation).await {
                Ok(update) => Some(update),
                Err(error) => {
                    // The Run stays accepted, so report it as accepted; a
                    // later `wait` reads it again.
                    let run_id = accepted.telemetry_id();
                    let task_id = started.as_ref().map(coral_task::Task::telemetry_id);
                    tracing::warn!(
                        coral.workspace.id = safe::id(&self.workspace_id),
                        coral.run.id = safe::id(&run_id),
                        coral.task.id = task_id.as_ref().map(safe::id),
                        error.type = safe::kind(error.code()),
                        "failed to observe the Run that exec accepted"
                    );
                    None
                }
            },
            None => None,
        };
        let started = started.as_ref().map(coral_task::Task::id);
        self.run_update(
            observed.as_ref().unwrap_or(&accepted),
            Reporter::Exec(started),
        )
        .map(Into::into)
        .map_err(|failure| accepted_run_failure(failure, accepted.run_id(), started))
    }

    async fn wait(
        &self,
        arguments: WaitArguments,
        progress: Option<Progress>,
    ) -> Result<CallToolResponse, DispatchError> {
        let run_id = RunId::parse(&arguments.run_id).map_err(invalid_input)?;
        let cursor = arguments
            .cursor
            .as_deref()
            .map(RunCursor::parse)
            .transpose()
            .map_err(invalid_input)?;
        let wait_ms = validated_wait(arguments.wait_ms.unwrap_or(arguments::DEFAULT_WAIT_MS))?;
        // A client that asked for progress shows the person what the Run
        // waits for while this call holds, so the link reaches them even
        // when the assistant writes nothing between its calls.
        if let Some(progress) = progress.filter(|_| wait_ms > 0) {
            // The observation below links this request to the Run Trace.
            let peek = RunObservation::immediate(cursor.clone(), RUN_EVENT_PAGE_MAX)
                .map_err(invalid_input)?
                .without_trace_link();
            let current = self.observe(&run_id, &peek).await.map_err(run_failure)?;
            if let Some(message) =
                projection::handoff_progress(&current, &self.runtime.modules.human_interface)
            {
                progress.report(message).await;
            }
        }
        let observation = RunObservation::immediate(cursor, RUN_EVENT_PAGE_MAX)
            .map_err(invalid_input)?
            .batching_output()
            .with_wait(Duration::from_millis(wait_ms))
            .map_err(invalid_input)?;
        self.observe_update(&run_id, &observation, Reporter::Observer)
            .await
    }

    /// Observes a Run and projects what it found. A Run that waits on a
    /// person reports it in the update's required actions; nothing is asked
    /// of the client.
    async fn observe_update(
        &self,
        run_id: &RunId,
        observation: &RunObservation,
        reporter: Reporter<'_>,
    ) -> Result<CallToolResponse, DispatchError> {
        let update = self
            .observe(run_id, observation)
            .await
            .map_err(run_failure)?;
        self.run_update(&update, reporter)
            .map(Into::into)
            .map_err(DispatchError::Tool)
    }

    async fn observe(
        &self,
        run_id: &RunId,
        observation: &RunObservation,
    ) -> Result<coral_run::RunUpdate, coral_run::Error> {
        self.runtime
            .modules
            .runs
            .observe(
                &self.runtime.principal,
                &self.workspace_id,
                run_id,
                observation,
            )
            .await
    }

    async fn cancel(&self, arguments: CancelArguments) -> Result<CallToolResult, DispatchError> {
        let run_id = RunId::parse(&arguments.run_id).map_err(invalid_input)?;
        let cursor = arguments
            .cursor
            .as_deref()
            .map(RunCursor::parse)
            .transpose()
            .map_err(invalid_input)?;
        let observation =
            RunObservation::immediate(cursor, RUN_EVENT_PAGE_MAX).map_err(invalid_input)?;
        let update = self
            .runtime
            .modules
            .runs
            .cancel(
                &self.runtime.principal,
                &self.workspace_id,
                &run_id,
                &observation,
            )
            .await
            .map_err(run_failure)?;
        self.run_update(&update, Reporter::Observer)
            .map_err(DispatchError::Tool)
    }

    async fn end_task(&self, arguments: EndTaskArguments) -> Result<CallToolResult, DispatchError> {
        let task_id = TaskId::parse(&arguments.task_id).map_err(invalid_input)?;
        let outcome = match arguments.outcome {
            TaskOutcomeArgument::Success => TaskOutcome::Success,
            TaskOutcomeArgument::Failure => TaskOutcome::Failure,
        };
        let task = self
            .runtime
            .modules
            .tasks
            .end(
                &self.runtime.principal,
                &self.workspace_id,
                &task_id,
                outcome,
                arguments.feedback.as_deref(),
                now_unix_nanos()?,
            )
            .await
            .map_err(task_failure)?;
        tool_call::record_task(task.telemetry_id());
        let durable_outcome = task
            .outcome()
            .ok_or_else(|| DispatchError::Tool(ToolFailure::internal()))?;
        projection::success(projection::EndedTask {
            task_id: task.id().to_string(),
            outcome: durable_outcome.into(),
        })
        .map_err(DispatchError::Tool)
    }

    fn run_update(
        &self,
        update: &coral_run::RunUpdate,
        reporter: Reporter<'_>,
    ) -> Result<CallToolResult, ToolFailure> {
        let modules = &self.runtime.modules;
        projection::run_update(
            update,
            reporter,
            &modules.human_interface,
            modules.run_update_page_bytes.get(),
        )
    }

    fn attributed_command(
        &self,
        task_id: &str,
        intent: &str,
    ) -> Result<coral_task::AttributedCommand, DispatchError> {
        let task_id = TaskId::parse(task_id).map_err(invalid_input)?;
        Ok(coral_task::AttributedCommand::new(
            self.runtime.principal.clone(),
            self.workspace_id.clone(),
            task_id,
            TaskIntent::parse(intent).map_err(invalid_input)?,
        ))
    }
}

impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerConfig {
        // Embedded output blocks are `resource` content, and the spec asks servers
        // that send them to declare resources. Nothing is listed or readable.
        ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_extensions_with(crate::app::extensions())
                .build(),
        )
        .with_protocol_version(fallback_protocol_version())
        .with_server_info({
            let implementation = Implementation::new("Coral", env!("CARGO_PKG_VERSION"));
            match &self.title {
                Some(title) => implementation.with_title(title),
                None => implementation,
            }
        })
        .with_instructions(crate::instructions::INSTRUCTIONS)
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(SUPPORTED_PROTOCOL_VERSIONS)
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let parts = context.extensions.get::<axum::http::request::Parts>();
        if let Some(parts) = parts {
            // Each tool call records its request's duration under its tool.
            coral_telemetry::name_request_operation(
                &parts.extensions,
                "tools/call",
                crate::arguments::served_name(request.name.as_ref()),
            );
        }
        let carrier = parts.and_then(|parts| coral_telemetry::request_span(&parts.extensions));
        let call = ToolCall::open(
            request.name.as_ref(),
            carrier,
            &self.workspace_id,
            &self.runtime.principal,
        );
        let progress = context.meta.get_progress_token().map(|token| Progress {
            token,
            peer: context.peer.clone(),
        });
        let dispatched = Box::pin(self.dispatch(request, progress));
        call.run(dispatched).await
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let tools = self.listed_tools().await?;
        // Protocol 2026-07-28 (SEP-2549) requires cache hints on list results
        // and Claude Code rejects a tools/list result that omits them. Older
        // versions ignore the extra fields, so the hints are not version-gated.
        // The list is this caller's, never shared.
        Ok(ListToolsResult::with_all_items(tools)
            .with_ttl_ms(0)
            .with_cache_scope(CacheScope::Private))
    }

    // Embedded output URIs label one result's blocks; they are never listed or read.
    fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListResourcesResult, ErrorData>> + MaybeSendFuture + '_ {
        std::future::ready(Ok(ListResourcesResult::with_all_items(Vec::new())
            .with_ttl_ms(0)
            .with_cache_scope(CacheScope::Private)))
    }

    fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ReadResourceResponse, ErrorData>> + MaybeSendFuture + '_ {
        // The app that shows a Run's Required Action is the one readable
        // resource; an embedded output block's URI stays unreadable.
        std::future::ready(if request.uri == crate::app::RESOURCE_URI {
            Ok(crate::app::resource().into())
        } else {
            Err(ErrorData::resource_not_found("Resource not found", None))
        })
    }

    fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListResourceTemplatesResult, ErrorData>> + MaybeSendFuture + '_
    {
        std::future::ready(Ok(ListResourceTemplatesResult::with_all_items(Vec::new())
            .with_ttl_ms(0)
            .with_cache_scope(CacheScope::Private)))
    }
}

fn validated_wait(wait_ms: u64) -> Result<u64, DispatchError> {
    if wait_ms > arguments::MAX_WAIT_MS {
        return Err(invalid_input_message(format!(
            "wait_ms must be at most {}",
            arguments::MAX_WAIT_MS
        )));
    }
    Ok(wait_ms)
}

fn now_unix_nanos() -> Result<i64, DispatchError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| DispatchError::Tool(ToolFailure::internal()))?;
    i64::try_from(duration.as_nanos())
        .map_err(|_error| DispatchError::Tool(ToolFailure::internal()))
}

fn invalid_input(error: impl std::fmt::Display) -> DispatchError {
    invalid_input_message(error.to_string())
}

fn invalid_input_message(message: String) -> DispatchError {
    DispatchError::Protocol(ErrorData::invalid_params(message, None))
}

fn task_failure(error: coral_task::Error) -> DispatchError {
    let failure = match error {
        coral_task::Error::InvalidInput(message) => ToolFailure::safe("invalid_input", message),
        coral_task::Error::TaskNotFound => {
            ToolFailure::stable("task_not_found", "Task was not found")
        }
        coral_task::Error::WorkspaceNotFound => {
            ToolFailure::stable("workspace_not_found", "Workspace was not found")
        }
        coral_task::Error::PermissionDenied(_) => {
            ToolFailure::stable("permission_denied", "Permission was denied")
        }
        coral_task::Error::Workspace(error) => workspace_tool_failure(error),
        coral_task::Error::Search(error) => return search_failure(error),
        coral_task::Error::Tools(error) => return tools_failure(error),
        coral_task::Error::Run(error) => return run_failure(error),
        // With no start_task, the refusal is the agent's only hint that new
        // work starts a new Task.
        coral_task::Error::TaskClosed => ToolFailure::stable(
            "task_closed",
            "Task is closed to new attempts. For new work, call `exec` with `goal` instead of `task_id` to start a new Task.",
        ),
        coral_task::Error::InteractionNotFound => {
            ToolFailure::stable("interaction_not_found", "Task Interaction was not found")
        }
        coral_task::Error::SnapshotTooLarge => ToolFailure::stable(
            "snapshot_too_large",
            "Complete discovery snapshot exceeds its limit",
        ),
        coral_task::Error::Clock
        | coral_task::Error::IdentifierIssuance(_)
        | coral_task::Error::CorruptData(_)
        | coral_task::Error::Database(_) => ToolFailure::internal(),
    };
    DispatchError::Tool(failure)
}

/// A failure after an `exec` with `goal` started its Task. It names the Task,
/// so the agent continues it with `task_id` instead of starting another with
/// `goal`.
fn started_task_failure(error: DispatchError, task: &TaskId) -> DispatchError {
    match error {
        DispatchError::Tool(failure) => DispatchError::Tool(failure.naming_started_task(task)),
        protocol @ DispatchError::Protocol(_) => protocol,
    }
}

/// A failure after `exec` accepted its Run. It names the Run, so the agent
/// reads it with `wait` instead of running the Program again, and the Task
/// when this call started it.
fn accepted_run_failure(
    failure: ToolFailure,
    run: &RunId,
    started: Option<&TaskId>,
) -> DispatchError {
    let failure = failure.naming_accepted_run(run);
    DispatchError::Tool(match started {
        Some(task) => failure.naming_started_task(task),
        None => failure,
    })
}

/// The kind of a failed namespace lookup, as a log names it.
const fn lookup_error_kind(error: &coral_search::Error) -> &'static str {
    match error {
        coral_search::Error::Database(_) => "database",
        coral_search::Error::Provider(_) => "provider",
        coral_search::Error::Workspace(_) => "workspace",
        _ => "search",
    }
}

fn search_failure(error: coral_search::Error) -> DispatchError {
    if let Some((code, message)) = error.safe_rejection() {
        return DispatchError::Tool(ToolFailure::safe(code, message));
    }
    let failure = match error {
        coral_search::Error::PermissionDenied(_) => {
            ToolFailure::stable("permission_denied", "Permission was denied")
        }
        coral_search::Error::Workspace(error) => workspace_tool_failure(error),
        coral_search::Error::Tools(error) => return tools_failure(error),
        _ => ToolFailure::internal(),
    };
    DispatchError::Tool(failure)
}

fn tools_failure(error: coral_tools::Error) -> DispatchError {
    use coral_tools::Error;
    if let Some((code, message)) = error.safe_rejection() {
        return DispatchError::Tool(ToolFailure::safe(code, message));
    }
    let failure = match error {
        Error::Workspace(error) => workspace_tool_failure(error),
        _ => ToolFailure::internal(),
    };
    DispatchError::Tool(failure)
}

fn run_failure(error: coral_run::Error) -> DispatchError {
    if let Some((code, message)) = error.safe_rejection() {
        return DispatchError::Tool(ToolFailure::safe(code, message));
    }
    let failure = match error {
        coral_run::Error::RunNotFound => {
            ToolFailure::stable("run_not_found", "Program Run was not found")
        }
        coral_run::Error::OwnerLost => ToolFailure::stable(
            "run_owner_lost",
            "Program Run outcome is unknown; read the Run again",
        ),
        coral_run::Error::RunRequestNotYetRecorded => ToolFailure::stable(
            "run_request_not_yet_recorded",
            "Program Run request is not yet recorded; retry",
        ),
        coral_run::Error::Tools(error) => return tools_failure(error),
        coral_run::Error::ProgramValueTooLarge => ToolFailure::stable(
            "program_value_too_large",
            "Program Final Value is too large",
        ),
        coral_run::Error::InvocationRejected(failure) => {
            ToolFailure::safe("invocation_rejected", failure.message())
        }
        coral_run::Error::Workspace(error) => workspace_tool_failure(error),
        _ => ToolFailure::internal(),
    };
    DispatchError::Tool(failure)
}

fn workspace_tool_failure(error: coral_workspace::Error) -> ToolFailure {
    match error {
        coral_workspace::Error::InvalidInput(message) => {
            ToolFailure::safe("invalid_input", message)
        }
        coral_workspace::Error::WorkspaceNotFound => {
            ToolFailure::stable("workspace_not_found", "Workspace was not found")
        }
        coral_workspace::Error::WorkspaceArchived => {
            ToolFailure::stable("workspace_archived", "Workspace is archived")
        }
        coral_workspace::Error::PermissionDenied(_)
        | coral_workspace::Error::AgentNotFound
        | coral_workspace::Error::InvalidAgentCredential => {
            ToolFailure::stable("permission_denied", "Permission was denied")
        }
        coral_workspace::Error::AgentDisabled => {
            ToolFailure::stable("agent_disabled", "Agent is disabled")
        }
        coral_workspace::Error::AgentRegistrationExpired => ToolFailure::stable(
            "agent_registration_expired",
            "Agent Registration has expired",
        ),
        coral_workspace::Error::OrganizationAuthorityConflict
        | coral_workspace::Error::EtagMismatch
        | coral_workspace::Error::RequestAlreadyApplied
        | coral_workspace::Error::CredentialGeneration
        | coral_workspace::Error::WorkspaceAlreadyActive
        | coral_workspace::Error::WorkspaceAlreadyArchived
        | coral_workspace::Error::MembershipAlreadyExists
        | coral_workspace::Error::MembershipNotFound
        | coral_workspace::Error::LastWorkspaceOwner
        | coral_workspace::Error::OwnerTransferRequired
        | coral_workspace::Error::OwnerTargetNotEnabled
        | coral_workspace::Error::CorruptData(_)
        | coral_workspace::Error::Database(_)
        | coral_workspace::Error::EventFields(_)
        | coral_workspace::Error::AmbiguousEmail => ToolFailure::internal(),
    }
}

/// The Task an `exec` continues, or the goal of the Task it starts.
enum ExecTarget {
    Task(coral_task::AttributedCommand),
    Goal(coral_task::NewTaskCommand),
}

/// The progress the client asked for with this request, if it sent a token.
struct Progress {
    token: ProgressToken,
    peer: Peer<RoleServer>,
}

impl Progress {
    /// Sends one progress message on this call's own response stream. A
    /// client that cannot take it loses nothing the Run Update does not carry.
    async fn report(self, message: String) {
        let sent = self
            .peer
            .notify_progress(ProgressNotificationParam::new(self.token, 1.0).with_message(message))
            .await;
        if sent.is_err() {
            tracing::debug!("progress notification not delivered");
        }
    }
}

enum DispatchError {
    Protocol(ErrorData),
    Tool(ToolFailure),
}

impl From<ErrorData> for DispatchError {
    fn from(error: ErrorData) -> Self {
        Self::Protocol(error)
    }
}
