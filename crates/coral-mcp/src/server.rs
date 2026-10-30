use std::borrow::Cow;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use coral_run::{RUN_EVENT_PAGE_MAX, RunCursor, RunId, RunObservation};
use coral_search::SearchInput;
use coral_task::{TaskId, TaskIntent, TaskOutcome};
use coral_workspace::{TraceId, WorkspaceId};
use rmcp::model::{
    CacheScope, CallToolRequestParams, CallToolResponse, CallToolResult, Implementation,
    ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, PaginatedRequestParams,
    ProtocolVersion, ReadResourceRequestParams, ReadResourceResponse, ServerCapabilities,
    ServerConfig,
};
use rmcp::service::{MaybeSendFuture, RequestContext, RoleServer};
use rmcp::{ErrorData, ServerHandler};

use crate::McpRuntime;
use crate::arguments::{
    self, CancelArguments, DeleteFunctionArguments, DescribeArguments, EndTaskArguments,
    ExecArguments, SaveFunctionArguments, SearchArguments, StartTaskArguments, TaskOutcomeArgument,
    ToolArguments, WaitArguments,
};
use crate::projection::{self, ToolFailure};
use crate::prompts::{Asked, Channel, Prompting};

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

    async fn dispatch(
        &self,
        mut request: CallToolRequestParams,
        channel: Channel,
    ) -> Result<CallToolResponse, DispatchError> {
        let prompting = Prompting::from_request(
            channel,
            request.request_state.take(),
            request.input_responses.take(),
        )
        .map_err(invalid_input_message)?;
        let name = request.name.as_ref();
        if prompting.continued_run().is_some()
            && !matches!(name, ExecArguments::NAME | WaitArguments::NAME)
        {
            return Err(invalid_input_message(format!(
                "tool '{name}' does not accept requestState"
            )));
        }
        let complete = match name {
            StartTaskArguments::NAME => {
                let arguments = arguments::parse::<StartTaskArguments>(request.arguments)?;
                self.start_task(arguments).await
            }
            SearchArguments::NAME => {
                let arguments = arguments::parse::<SearchArguments>(request.arguments)?;
                self.search(arguments).await
            }
            DescribeArguments::NAME => {
                let arguments = arguments::parse::<DescribeArguments>(request.arguments)?;
                self.describe(arguments).await
            }
            SaveFunctionArguments::NAME => {
                let arguments = arguments::parse::<SaveFunctionArguments>(request.arguments)?;
                self.save_function(arguments).await
            }
            DeleteFunctionArguments::NAME => {
                let arguments = arguments::parse::<DeleteFunctionArguments>(request.arguments)?;
                self.delete_function(arguments).await
            }
            ExecArguments::NAME => {
                let arguments = arguments::parse::<ExecArguments>(request.arguments)?;
                return self.exec(arguments, prompting).await;
            }
            WaitArguments::NAME => {
                let arguments = arguments::parse::<WaitArguments>(request.arguments)?;
                return self.wait(arguments, prompting).await;
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
                format!("tool '{name}' not found"),
                None,
            ))),
        };
        complete.map(Into::into)
    }

    async fn start_task(
        &self,
        arguments: StartTaskArguments,
    ) -> Result<CallToolResult, DispatchError> {
        let intent = TaskIntent::parse(&arguments.intent).map_err(invalid_input)?;
        // Read before the Task starts, so a failed read leaves no Task behind.
        let providers = self
            .runtime
            .modules
            .commands
            .searchable_providers(&self.runtime.principal, &self.workspace_id)
            .await
            .map_err(task_failure)?;
        let task = self
            .runtime
            .modules
            .tasks
            .start(
                &self.runtime.principal,
                &self.workspace_id,
                intent,
                now_unix_nanos()?,
            )
            .await
            .map_err(task_failure)?;
        tool_call::record_task(task.telemetry_id());
        projection::success(projection::StartedTask {
            task_id: task.id().to_string(),
            providers: providers
                .iter()
                .map(|call_root| coral_search::provider_filter_value(call_root).to_string())
                .collect(),
        })
        .map_err(DispatchError::Tool)
    }

    async fn search(&self, arguments: SearchArguments) -> Result<CallToolResult, DispatchError> {
        let call = self.attributed_command(&arguments.task_id, &arguments.intent)?;
        let request = SearchInput {
            query: arguments.query,
            provider: arguments.provider,
            limit: arguments.limit,
            cursor: arguments.cursor,
        };
        let results = self
            .runtime
            .modules
            .commands
            .search(&call, request)
            .await
            .map_err(task_failure)?;
        projection::search(&results)
            .and_then(projection::success)
            .map_err(DispatchError::Tool)
    }

    async fn describe(
        &self,
        arguments: DescribeArguments,
    ) -> Result<CallToolResult, DispatchError> {
        let call = self.attributed_command(&arguments.task_id, &arguments.intent)?;
        let result = self
            .runtime
            .modules
            .commands
            .describe(
                &call,
                &arguments.path,
                arguments.types.as_deref().unwrap_or_default(),
            )
            .await
            .map_err(task_failure)?;
        projection::exact_description(&result)
            .and_then(projection::success)
            .map_err(DispatchError::Tool)
    }

    async fn save_function(
        &self,
        arguments: SaveFunctionArguments,
    ) -> Result<CallToolResult, DispatchError> {
        let call = self.attributed_command(&arguments.task_id, &arguments.intent)?;
        let function = self
            .runtime
            .modules
            .commands
            .save_function(
                &call,
                &arguments.path,
                &arguments.description,
                &arguments.source,
            )
            .await
            .map_err(task_failure)?;
        projection::saved_function(&function)
            .and_then(projection::success)
            .map_err(DispatchError::Tool)
    }

    async fn delete_function(
        &self,
        arguments: DeleteFunctionArguments,
    ) -> Result<CallToolResult, DispatchError> {
        let call = self.attributed_command(&arguments.task_id, &arguments.intent)?;
        self.runtime
            .modules
            .commands
            .delete_function(&call, &arguments.path)
            .await
            .map_err(task_failure)?;
        projection::success(projection::DeletedFunction {
            call_path: format!("coral.functions.{}", arguments.path),
        })
        .map_err(DispatchError::Tool)
    }

    async fn exec(
        &self,
        arguments: ExecArguments,
        prompting: Prompting,
    ) -> Result<CallToolResponse, DispatchError> {
        let call = self.attributed_command(&arguments.task_id, &arguments.intent)?;
        let timeout = arguments
            .run_timeout_ms
            .unwrap_or(arguments::DEFAULT_RUN_TIMEOUT_MS);
        let wait_ms = validated_wait(arguments.wait_ms.unwrap_or(arguments::DEFAULT_EXEC_WAIT_MS))?;
        let reread = RunObservation::initial(Duration::ZERO).map_err(invalid_input)?;
        // A retry answers prompts about the Run this call already started.
        if let Some(run_id) = prompting.continued_run().cloned() {
            return self
                .observe_prompting(&run_id, &reread, &reread, prompting)
                .await;
        }
        let accepted = self
            .runtime
            .modules
            .commands
            .submit(&call, &arguments.program, timeout)
            .await
            .map_err(task_failure)?;
        if wait_ms == 0 {
            return self
                .run_update(&accepted)
                .map(Into::into)
                .map_err(DispatchError::Tool);
        }
        // This request accepted the Run, so acceptance already linked it to
        // the Run Trace.
        let observation = RunObservation::initial(Duration::from_millis(wait_ms))
            .map_err(invalid_input)?
            .by_acceptor();
        self.observe_prompting(
            accepted.run_id(),
            &observation,
            &reread.by_acceptor(),
            prompting,
        )
        .await
    }

    async fn wait(
        &self,
        arguments: WaitArguments,
        prompting: Prompting,
    ) -> Result<CallToolResponse, DispatchError> {
        let run_id = RunId::parse(&arguments.run_id).map_err(invalid_input)?;
        let cursor = arguments
            .cursor
            .as_deref()
            .map(RunCursor::parse)
            .transpose()
            .map_err(invalid_input)?;
        let wait_ms = validated_wait(arguments.wait_ms.unwrap_or(arguments::DEFAULT_WAIT_MS))?;
        let reread =
            RunObservation::immediate(cursor, RUN_EVENT_PAGE_MAX).map_err(invalid_input)?;
        // A retry has already waited; it answers and observes immediately.
        let observation = match prompting.continued_run() {
            Some(continued) if continued != &run_id => {
                return Err(invalid_input_message(
                    "requestState belongs to another Run".into(),
                ));
            }
            Some(_) => reread.clone(),
            None => reread
                .clone()
                .with_wait(Duration::from_millis(wait_ms))
                .map_err(invalid_input)?,
        };
        self.observe_prompting(&run_id, &observation, &reread, prompting)
            .await
    }

    /// Observes a Run and asks the client about each current action it can answer.
    /// Answers go to the Run domain, which decides whether they change anything;
    /// the returned update is reread after them.
    async fn observe_prompting(
        &self,
        run_id: &RunId,
        observation: &RunObservation,
        reread: &RunObservation,
        mut prompting: Prompting,
    ) -> Result<CallToolResponse, DispatchError> {
        let trace = &TraceId::current_or_new();
        self.answer_prompts(run_id, prompting.take_answers(), trace)
            .await?;
        let mut update = self.observe(run_id, observation).await?;
        while prompting.available() && !update.required_actions().is_empty() {
            let prompts = self
                .runtime
                .modules
                .runs
                .action_prompts(&self.runtime.principal, &self.workspace_id, run_id)
                .await
                .map_err(run_failure)?;
            let intent = if prompts.is_empty() {
                None
            } else {
                self.runtime
                    .modules
                    .tasks
                    .run_intent(&self.runtime.principal, &self.workspace_id, run_id)
                    .await
                    .map_err(task_failure)?
            };
            match prompting
                .ask(
                    prompts,
                    run_id,
                    &self.runtime.modules.human_interface,
                    intent.as_ref().map(coral_task::TaskIntent::as_str),
                )
                .map_err(|_error| DispatchError::Tool(ToolFailure::internal()))?
            {
                Asked::Nothing => break,
                Asked::InputRequired(result) => return Ok(result.into()),
                Asked::InBand(prompts) => {
                    for prompt in prompts {
                        let request = prompt.request().clone();
                        // Boxed: the watcher holds a whole Run observation.
                        let withdrawn = Box::pin(self.until_withdrawn(
                            run_id,
                            &request,
                            update.next_cursor().clone(),
                        ));
                        // Boxed: it reads the connection through the Account domain.
                        let connected = Box::pin(self.runtime.modules.runs.connection_connected(
                            &self.runtime.principal,
                            &self.workspace_id,
                            &request,
                            trace,
                        ));
                        let answer = prompt.ask(withdrawn, connected).await;
                        // Apply each answer at once: a later prompt may stay open.
                        self.answer_prompts(run_id, vec![answer], trace).await?;
                    }
                    update = self.observe(run_id, reread).await?;
                }
            }
        }
        self.run_update(&update)
            .map(Into::into)
            .map_err(DispatchError::Tool)
    }

    /// Completes once `request` is no longer a current action of the Run.
    /// An unreadable Run never withdraws a prompt; its answer is checked anyway.
    async fn until_withdrawn(
        &self,
        run_id: &RunId,
        request: &coral_run::RequiredActionRequest,
        mut cursor: RunCursor,
    ) {
        loop {
            let observation = RunObservation::immediate(Some(cursor), RUN_EVENT_PAGE_MAX).and_then(
                |observation| observation.with_wait(Duration::from_millis(arguments::MAX_WAIT_MS)),
            );
            let Ok(observation) = observation else {
                return std::future::pending().await;
            };
            let Ok(update) = self.observe(run_id, &observation).await else {
                return std::future::pending().await;
            };
            if !update
                .required_actions()
                .iter()
                .any(|action| &action.request == request)
            {
                return;
            }
            cursor = update.next_cursor().clone();
        }
    }

    async fn observe(
        &self,
        run_id: &RunId,
        observation: &RunObservation,
    ) -> Result<coral_run::RunUpdate, DispatchError> {
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
            .map_err(run_failure)
    }

    async fn answer_prompts(
        &self,
        run_id: &RunId,
        answers: Vec<(coral_run::RequiredActionRequest, coral_run::PromptAnswer)>,
        trace: &TraceId,
    ) -> Result<(), DispatchError> {
        for (request, answer) in answers {
            match self
                .runtime
                .modules
                .runs
                .answer_action_prompt(
                    &self.runtime.principal,
                    &self.workspace_id,
                    run_id,
                    &request,
                    answer,
                    trace,
                )
                .await
            {
                // The action ended or was resolved elsewhere while the User
                // considered it, or the answer still waits in the Run Inbox
                // for its Run Owner: the reread update shows what is recorded
                // so far, which may still be the open action.
                Ok(())
                | Err(
                    coral_run::Error::ApprovalNotFound | coral_run::Error::RunRequestNotYetRecorded,
                ) => {}
                Err(error) => return Err(run_failure(error)),
            }
        }
        Ok(())
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
        self.run_update(&update).map_err(DispatchError::Tool)
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

    fn run_update(&self, update: &coral_run::RunUpdate) -> Result<CallToolResult, ToolFailure> {
        let modules = &self.runtime.modules;
        projection::run_update(
            update,
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
            TraceId::current_or_new(),
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
        let dispatched = Box::pin(self.dispatch(request, Channel::negotiate(&context)));
        call.run(dispatched).await
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, ErrorData>> + MaybeSendFuture + '_ {
        // Protocol 2026-07-28 (SEP-2549) requires cache hints on list results
        // and Claude Code rejects a tools/list result that omits them. Older
        // versions ignore the extra fields, so the hints are not version-gated.
        std::future::ready(Ok(ListToolsResult::with_all_items(arguments::tools())
            .with_ttl_ms(0)
            .with_cache_scope(CacheScope::Private)))
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
        _request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ReadResourceResponse, ErrorData>> + MaybeSendFuture + '_ {
        std::future::ready(Err(ErrorData::resource_not_found(
            "Resource not found",
            None,
        )))
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
        coral_task::Error::Functions(error) => return functions_failure(error),
        coral_task::Error::Run(error) => return run_failure(error),
        coral_task::Error::TaskClosed => {
            ToolFailure::stable("task_closed", "Task is closed to new attempts")
        }
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

fn search_failure(error: coral_search::Error) -> DispatchError {
    if let Some((code, message)) = error.safe_rejection() {
        return DispatchError::Tool(ToolFailure::safe(code, message));
    }
    let failure = match error {
        coral_search::Error::PermissionDenied(_) => {
            ToolFailure::stable("permission_denied", "Permission was denied")
        }
        coral_search::Error::Workspace(error) => workspace_tool_failure(error),
        coral_search::Error::Functions(error) => return functions_failure(error),
        _ => ToolFailure::internal(),
    };
    DispatchError::Tool(failure)
}

fn functions_failure(error: coral_functions::Error) -> DispatchError {
    use coral_functions::Error;
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
        coral_run::Error::Functions(error) => return functions_failure(error),
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

enum DispatchError {
    Protocol(ErrorData),
    Tool(ToolFailure),
}

impl From<ErrorData> for DispatchError {
    fn from(error: ErrorData) -> Self {
        Self::Protocol(error)
    }
}
