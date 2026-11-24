use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use coral_run::{RUN_EVENT_PAGE_MAX, RunId, RunObservation};
use coral_task::{TaskId, TaskIntent, TaskOutcome};
use coral_telemetry::safe;
use coral_workspace::WorkspaceId;
use rmcp::model::{
    CacheScope, CallToolRequestParams, CallToolResponse, CallToolResult, ClientCapabilities,
    DiscoverResult, Implementation, InitializeRequestParams, InitializeResult,
    ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, PaginatedRequestParams,
    ProtocolVersion, ReadResourceRequestParams, ReadResourceResponse, ServerCapabilities,
    ServerConfig, Tool,
};
use rmcp::service::{MaybeSendFuture, RequestContext, RoleServer};
use rmcp::{ErrorData, ServerHandler};

use crate::arguments::{
    self, CancelArguments, EndTaskArguments, ExecArguments, ShowRequiredActionsArguments,
    TaskOutcomeArgument, ToolArguments, WaitArguments,
};
use coral_run::approval::ApprovalDecision;
use rmcp::model::InputResponses;

use crate::client_prompt::{self, Shown, WaitCursor};
use crate::interface::{McpCaller, McpInterface, Progress, ToolCallError};
use crate::projection::{self, Reporter, ToolFailure};
use crate::{McpModules, McpRuntime};

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

/// The MCP server of one request: the protocol, and the span for each tool
/// call. It asks `interface` for the instructions, the tools and each call.
#[derive(Clone)]
pub(crate) struct McpServer {
    interface: Arc<dyn McpInterface>,
    caller: McpCaller,
    title: Option<String>,
}

impl McpServer {
    pub(crate) const fn new(interface: Arc<dyn McpInterface>, caller: McpCaller) -> Self {
        Self {
            interface,
            caller,
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
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(SUPPORTED_PROTOCOL_VERSIONS)
    }

    // Both handshakes carry the instructions of this request's caller.
    async fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, ErrorData> {
        context.peer.set_peer_info(request.clone());
        let instructions = self.interface.instructions(&self.caller).await;
        Ok(self
            .negotiate_initialize(&request)?
            .with_instructions(instructions))
    }

    async fn discover(
        &self,
        _context: RequestContext<RoleServer>,
    ) -> Result<DiscoverResult, ErrorData> {
        let instructions = self.interface.instructions(&self.caller).await;
        Ok(DiscoverResult::from_server_info(
            SUPPORTED_PROTOCOL_VERSIONS.to_vec(),
            self.get_info().with_instructions(instructions),
        ))
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
            self.caller.workspace_id(),
            self.caller.principal(),
        );
        let progress = context.meta.get_progress_token().map(|token| Progress {
            token,
            peer: context.peer.clone(),
        });
        let capabilities = context.meta.client_capabilities();
        let called = self
            .interface
            .call(&self.caller, request, progress, capabilities);
        call.run(called).await
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let tools = self.interface.tools(&self.caller).await?;
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

/// Core's tool surface: `exec`, `wait`, `show_required_actions`, `cancel` and
/// `end_task`.
pub(crate) struct CoreInterface {
    modules: McpModules,
}

#[async_trait::async_trait]
impl McpInterface for CoreInterface {
    async fn instructions(&self, caller: &McpCaller) -> &str {
        if self.for_caller(caller).delegation().await.is_some() {
            crate::instructions::DELEGATING_INSTRUCTIONS
        } else {
            crate::instructions::INSTRUCTIONS
        }
    }

    async fn tools(&self, caller: &McpCaller) -> Result<Vec<Tool>, ErrorData> {
        self.for_caller(caller).listed_tools().await
    }

    async fn call(
        &self,
        caller: &McpCaller,
        request: CallToolRequestParams,
        progress: Option<Progress>,
        capabilities: Option<ClientCapabilities>,
    ) -> Result<CallToolResponse, ToolCallError> {
        self.for_caller(caller)
            .dispatch(request, progress, capabilities)
            .await
    }
}

impl CoreInterface {
    pub(crate) const fn new(modules: McpModules) -> Self {
        Self { modules }
    }

    /// Core's tools as `caller` is served them.
    fn for_caller(&self, caller: &McpCaller) -> CoreTools {
        CoreTools::new(
            McpRuntime {
                modules: self.modules.clone(),
                principal: caller.principal().clone(),
                now: caller.now(),
            },
            caller.workspace_id().clone(),
        )
    }
}

/// What a client sends back when it repeats a call that asked a Client
/// Prompt: the state Coral gave it, and the person's answers.
struct Repeat {
    state: Option<String>,
    answers: Option<InputResponses>,
}

/// Core's tools for one caller.
struct CoreTools {
    runtime: McpRuntime,
    workspace_id: WorkspaceId,
}

impl CoreTools {
    const fn new(runtime: McpRuntime, workspace_id: WorkspaceId) -> Self {
        Self {
            runtime,
            workspace_id,
        }
    }

    /// How to hand work to a Sub-agent, in `coral.agent`'s own words, where
    /// policy enables that Tool for this caller. A read that fails costs only
    /// this text: the next call says why.
    async fn delegation(&self) -> Option<String> {
        let agent = [coral_search::CallPath::parse("coral.agent").ok()?];
        let search = &self.runtime.modules.search;
        let described = search.describe_tools(&self.runtime.principal, &self.workspace_id, &agent);
        let agent = described.await.ok()?.into_iter().next()?;
        let enabled = coral_search::ApprovalRequirement::NotRequired;
        (agent.approval_requirement() == enabled)
            .then(|| agent.guidance().map(str::to_owned))
            .flatten()
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
        let delegation = self.delegation().await;
        let description = arguments::exec_description(&namespaces, delegation.as_deref());
        Ok(arguments::tools(description))
    }

    async fn dispatch(
        &self,
        request: CallToolRequestParams,
        progress: Option<Progress>,
        capabilities: Option<ClientCapabilities>,
    ) -> Result<CallToolResponse, ToolCallError> {
        let name = request.name.as_ref();
        // Only `wait` asks a Client Prompt, so only `wait` is repeated with
        // its answer.
        if name != WaitArguments::NAME
            && (request.request_state.is_some() || request.input_responses.is_some())
        {
            return Err(invalid_input_message(
                "tool does not accept requestState or inputResponses".to_owned(),
            ));
        }
        let complete = match name {
            ExecArguments::NAME => {
                let arguments = arguments::parse::<ExecArguments>(request.arguments)?;
                return self.exec(arguments).await;
            }
            WaitArguments::NAME => {
                let repeat = Repeat {
                    state: request.request_state,
                    answers: request.input_responses,
                };
                let arguments = arguments::parse::<WaitArguments>(request.arguments)?;
                let ask =
                    client_prompt::Ask::of(self.runtime.modules.elicitation, capabilities.as_ref());
                return self.wait(arguments, repeat, progress, ask).await;
            }
            ShowRequiredActionsArguments::NAME => {
                let arguments =
                    arguments::parse::<ShowRequiredActionsArguments>(request.arguments)?;
                self.show_required_actions(arguments).await
            }
            CancelArguments::NAME => {
                let arguments = arguments::parse::<CancelArguments>(request.arguments)?;
                self.cancel(arguments).await
            }
            EndTaskArguments::NAME => {
                let arguments = arguments::parse::<EndTaskArguments>(request.arguments)?;
                self.end_task(arguments).await
            }
            _ => Err(ToolCallError::Protocol(ErrorData::invalid_params(
                "tool not found",
                None,
            ))),
        };
        complete.map(Into::into)
    }

    async fn exec(&self, arguments: ExecArguments) -> Result<CallToolResponse, ToolCallError> {
        // A later call that still carries its Task's `goal` continues the Task:
        // refusing it would only cost the agent a turn to send it again.
        let target = match (&arguments.task_id, &arguments.goal) {
            (Some(task_id), _) => {
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
            (None, None) => {
                return Err(invalid_input_message(
                    "exec takes `task_id` or `goal`: pass `task_id` to continue a Task, or `goal` to start a new one".to_owned(),
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
            &Shown::default(),
        )
        .map(Into::into)
        .map_err(|failure| accepted_run_failure(failure, accepted.run_id(), started))
    }

    /// Observes a Run, holding up to `wait_ms` for a change. When `ask`
    /// allows a Client Prompt and a Required Action names the invoking User
    /// that no prompt asked about yet, the call asks for it instead of
    /// returning the Run Update. The client repeats the call with the state
    /// in `repeat`, which this call then counts as asked. A form's Approve or
    /// Decline decides its approval before the call reads the Run; any other
    /// answer decides nothing: a link prompt's page decides, and a dismissed
    /// prompt leaves the assistant's link, as before. A prompt after the hold
    /// leaves the cursor where it was, so the repeated call reads the same
    /// Events again.
    async fn wait(
        &self,
        arguments: WaitArguments,
        repeat: Repeat,
        progress: Option<Progress>,
        ask: Option<client_prompt::Ask>,
    ) -> Result<CallToolResponse, ToolCallError> {
        let run_id = RunId::parse(&arguments.run_id).map_err(invalid_input)?;
        let (cursor, shown) = match arguments.cursor.as_deref() {
            Some(cursor) => {
                let cursor = WaitCursor::parse(cursor).map_err(invalid_input)?;
                (Some(cursor.run), cursor.shown)
            }
            None => (None, Shown::default()),
        };
        let prompted = repeat
            .state
            .as_deref()
            .and_then(|state| client_prompt::prompted_in_state(state, &run_id))
            .unwrap_or_default();
        let wait_ms = validated_wait(arguments.wait_ms.unwrap_or(arguments::DEFAULT_WAIT_MS))?;
        // The call's own input is checked first, so a call it refuses
        // decides nothing.
        if let (Some(ask), Some(answers)) = (ask, &repeat.answers)
            && let Some((approval, decision)) = ask.decision(answers, &prompted)
        {
            self.decide(&approval, decision)
                .await
                .map_err(run_failure)?;
        }
        let shown = shown.union(prompted.shown);
        let human_interface = &self.runtime.modules.human_interface;
        // A Required Action already waiting would not end the hold, so it is
        // asked before. Otherwise, a client that asked for progress shows the
        // person what the Run waits for while this call holds, so the link
        // reaches them even when the assistant writes nothing between its
        // calls.
        let progress = progress.filter(|_| wait_ms > 0);
        if ask.is_some() || progress.is_some() {
            // The observation below links this request to the Run Trace.
            let peek = RunObservation::immediate(cursor.clone(), RUN_EVENT_PAGE_MAX)
                .map_err(invalid_input)?
                .without_trace_link();
            let current = self.observe(&run_id, &peek).await.map_err(run_failure)?;
            if let Some(ask) = ask
                && let Some(prompt) = self.prompt(&current, &shown, ask).await
            {
                return Ok(prompt.into());
            }
            if let Some(progress) = progress
                && let Some(message) = projection::handoff_progress(&current, human_interface)
            {
                progress.report(message).await;
            }
        }
        let observation = RunObservation::immediate(cursor, RUN_EVENT_PAGE_MAX)
            .map_err(invalid_input)?
            .batching_output()
            .with_wait(Duration::from_millis(wait_ms))
            .map_err(invalid_input)?;
        let update = self
            .observe(&run_id, &observation)
            .await
            .map_err(run_failure)?;
        if let Some(ask) = ask
            && let Some(prompt) = self.prompt(&update, &shown, ask).await
        {
            return Ok(prompt.into());
        }
        self.run_update(&update, Reporter::Observer, &shown)
            .map(Into::into)
            .map_err(ToolCallError::Tool)
    }

    /// The Client Prompt `wait` asks for `update`, if any. A form shows the
    /// approval's exact arguments; one whose arguments it cannot show whole,
    /// or cannot read, is asked as a link where `ask` allows one.
    async fn prompt(
        &self,
        update: &coral_run::RunUpdate,
        shown: &Shown,
        ask: client_prompt::Ask,
    ) -> Option<rmcp::model::InputRequiredResult> {
        let (action, mode) = client_prompt::next(update, shown, ask)?;
        let arguments = match (mode, &action.request) {
            (
                client_prompt::Mode::Form,
                coral_run::RequiredActionRequest::InvocationApproval(id),
            ) => self
                .runtime
                .modules
                .runs
                .approvals()
                .get(&self.runtime.principal, &self.workspace_id, id)
                .await
                .ok()
                .and_then(|request| client_prompt::form_arguments(&request.arguments)),
            _ => None,
        };
        let mode = match (mode, &arguments) {
            (client_prompt::Mode::Form, None) => ask.instead_of_form()?,
            (mode, _) => mode,
        };
        client_prompt::prompt(
            update.run_id(),
            action,
            mode,
            arguments.as_deref(),
            shown,
            &self.runtime.modules.human_interface,
        )
    }

    /// Records the invoking User's decision from a form, through the same
    /// decision path as Reef's, which authorizes the person. A decision that
    /// fails ends the call with its error. The approval then stays pending,
    /// and since only the request state of the failed call counted it as
    /// asked, the next `wait` asks again.
    async fn decide(
        &self,
        approval: &coral_run::approval::ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<(), coral_run::Error> {
        self.runtime
            .modules
            .runs
            .approvals()
            .decide(
                &self.runtime.principal,
                &self.workspace_id,
                approval,
                decision,
                Some(client_prompt::FORM_COMMENT),
                &coral_workspace::TraceId::current_or_new(),
            )
            .await
            .map(drop)
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

    /// Reads what the Run waits for now, without waiting. It reports none of
    /// the Run's output, which stays with `wait`.
    async fn show_required_actions(
        &self,
        arguments: ShowRequiredActionsArguments,
    ) -> Result<CallToolResult, ToolCallError> {
        // This tool reports no Events, and an observation takes no limit
        // below one.
        const EVENT_LIMIT: u16 = 1;
        let run_id = RunId::parse(&arguments.run_id).map_err(invalid_input)?;
        let observation = RunObservation::immediate(None, EVENT_LIMIT).map_err(invalid_input)?;
        let update = self
            .observe(&run_id, &observation)
            .await
            .map_err(run_failure)?;
        projection::shown_required_actions(&update, &self.runtime.modules.human_interface)
            .map_err(ToolCallError::Tool)
    }

    async fn cancel(&self, arguments: CancelArguments) -> Result<CallToolResult, ToolCallError> {
        let run_id = RunId::parse(&arguments.run_id).map_err(invalid_input)?;
        let cursor = arguments
            .cursor
            .as_deref()
            .map(WaitCursor::parse)
            .transpose()
            .map_err(invalid_input)?;
        let (cursor, shown) = cursor.map_or((None, Shown::default()), |cursor| {
            (Some(cursor.run), cursor.shown)
        });
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
        self.run_update(&update, Reporter::Observer, &shown)
            .map_err(ToolCallError::Tool)
    }

    async fn end_task(&self, arguments: EndTaskArguments) -> Result<CallToolResult, ToolCallError> {
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
                self.runtime.now,
            )
            .await
            .map_err(task_failure)?;
        tool_call::record_task(task.telemetry_id());
        let durable_outcome = task
            .outcome()
            .ok_or_else(|| ToolCallError::Tool(ToolFailure::internal()))?;
        projection::success(projection::EndedTask {
            task_id: task.id().to_string(),
            outcome: durable_outcome.into(),
        })
        .map_err(ToolCallError::Tool)
    }

    /// Projects `update`. Its cursor keeps whichever of `shown` the Run still
    /// waits on.
    fn run_update(
        &self,
        update: &coral_run::RunUpdate,
        reporter: Reporter<'_>,
        shown: &Shown,
    ) -> Result<CallToolResult, ToolFailure> {
        let modules = &self.runtime.modules;
        projection::run_update(
            update,
            reporter,
            &modules.human_interface,
            modules.run_update_page_bytes.get(),
            shown,
        )
    }

    fn attributed_command(
        &self,
        task_id: &str,
        intent: &str,
    ) -> Result<coral_task::AttributedCommand, ToolCallError> {
        let task_id = TaskId::parse(task_id).map_err(invalid_input)?;
        Ok(coral_task::AttributedCommand::new(
            self.runtime.principal.clone(),
            self.workspace_id.clone(),
            task_id,
            TaskIntent::parse(intent).map_err(invalid_input)?,
        ))
    }
}

fn validated_wait(wait_ms: u64) -> Result<u64, ToolCallError> {
    if wait_ms > arguments::MAX_WAIT_MS {
        return Err(invalid_input_message(format!(
            "wait_ms must be at most {}",
            arguments::MAX_WAIT_MS
        )));
    }
    Ok(wait_ms)
}

fn invalid_input(error: impl std::fmt::Display) -> ToolCallError {
    invalid_input_message(error.to_string())
}

fn invalid_input_message(message: String) -> ToolCallError {
    ToolCallError::Protocol(ErrorData::invalid_params(message, None))
}

fn task_failure(error: coral_task::Error) -> ToolCallError {
    let failure = match error {
        coral_task::Error::InvalidInput(message) => ToolFailure::safe("invalid_input", message),
        coral_task::Error::TaskNotFound => {
            ToolFailure::stable("task_not_found", "Task was not found")
        }
        coral_task::Error::WorkspaceNotFound => {
            refusal_failure(coral_workspace::AuthorityRefusal::WorkspaceNotFound)
        }
        coral_task::Error::PermissionDenied(_) => {
            refusal_failure(coral_workspace::AuthorityRefusal::PermissionDenied)
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
        coral_task::Error::IdentifierIssuance(_)
        | coral_task::Error::CorruptData(_)
        | coral_task::Error::Database(_) => ToolFailure::internal(),
    };
    ToolCallError::Tool(failure)
}

/// A failure after an `exec` with `goal` started its Task. It names the Task,
/// so the agent continues it with `task_id` instead of starting another with
/// `goal`.
fn started_task_failure(error: ToolCallError, task: &TaskId) -> ToolCallError {
    match error {
        ToolCallError::Tool(failure) => ToolCallError::Tool(failure.naming_started_task(task)),
        protocol @ ToolCallError::Protocol(_) => protocol,
    }
}

/// A failure after `exec` accepted its Run. It names the Run, so the agent
/// reads it with `wait` instead of running the Program again, and the Task
/// when this call started it.
fn accepted_run_failure(
    failure: ToolFailure,
    run: &RunId,
    started: Option<&TaskId>,
) -> ToolCallError {
    let failure = failure.naming_accepted_run(run);
    ToolCallError::Tool(match started {
        Some(task) => failure.naming_started_task(task),
        None => failure,
    })
}

/// The kind of a failed namespace lookup, as a log names it.
const fn lookup_error_kind(error: &coral_search::Error) -> &'static str {
    match error {
        coral_search::Error::Database(_) => "database",
        coral_search::Error::Provider(_) => "provider",
        coral_search::Error::Policy(_) => "policy",
        coral_search::Error::Tools(_) => "tools",
        coral_search::Error::Workspace(_) => "workspace",
        // Name every remaining variant so a new one needs a kind here.
        coral_search::Error::InvalidInput(_)
        | coral_search::Error::UnknownProviderCallRoot { .. }
        | coral_search::Error::ServiceUnavailable
        | coral_search::Error::AuthoringDescriptionNotFound
        | coral_search::Error::TypesLimit
        | coral_search::Error::UndeclaredTypes { .. }
        | coral_search::Error::PermissionDenied(_) => "search",
    }
}

fn search_failure(error: coral_search::Error) -> ToolCallError {
    if let Some((code, message)) = error.safe_rejection() {
        return ToolCallError::Tool(ToolFailure::safe(code, message));
    }
    let failure = match error {
        coral_search::Error::PermissionDenied(_) => {
            refusal_failure(coral_workspace::AuthorityRefusal::PermissionDenied)
        }
        coral_search::Error::Workspace(error) => workspace_tool_failure(error),
        coral_search::Error::Tools(error) => return tools_failure(error),
        // Name every remaining variant so a new one needs a decision here.
        // `safe_rejection` answers these before the match.
        coral_search::Error::InvalidInput(_)
        | coral_search::Error::UnknownProviderCallRoot { .. }
        | coral_search::Error::ServiceUnavailable
        | coral_search::Error::AuthoringDescriptionNotFound
        | coral_search::Error::TypesLimit
        | coral_search::Error::UndeclaredTypes { .. }
        // `safe_rejection` has no answer for the rest.
        | coral_search::Error::Provider(_)
        | coral_search::Error::Policy(_)
        | coral_search::Error::Database(_) => ToolFailure::internal(),
    };
    ToolCallError::Tool(failure)
}

fn tools_failure(error: coral_tools::Error) -> ToolCallError {
    use coral_tools::Error;
    if let Some((code, message)) = error.safe_rejection() {
        return ToolCallError::Tool(ToolFailure::safe(code, message));
    }
    let failure = match error {
        Error::Workspace(error) => workspace_tool_failure(error),
        // Name every remaining variant so a new one needs a decision here.
        // `safe_rejection` answers these before the match.
        Error::InvalidInput(_)
        | Error::NotFound(_)
        | Error::PrivateToolExists
        | Error::RevisionConflict { .. }
        | Error::Admission(..)
        | Error::SharedToolSourceUnreadable
        | Error::SharedToolRestoreDiffers
        | Error::SharedToolDeclarationChanged
        | Error::SharedToolHasDependents { .. }
        | Error::PathConflict(_)
        | Error::NotData(..)
        | Error::CompilationTimedOut(_)
        | Error::CompilerBusy
        | Error::NoCompiler
        | Error::PermissionDenied(_)
        // `safe_rejection` has no answer for the rest.
        | Error::CompilerUnavailable(_)
        | Error::CompilerWorkerStopped
        | Error::Json(_)
        | Error::CorruptData(_)
        | Error::Database(_) => ToolFailure::internal(),
    };
    ToolCallError::Tool(failure)
}

fn run_failure(error: coral_run::Error) -> ToolCallError {
    if let Some((code, message)) = error.safe_rejection() {
        return ToolCallError::Tool(ToolFailure::safe(code, message));
    }
    let failure = match error {
        coral_run::Error::RunNotFound => {
            ToolFailure::stable("run_not_found", "Program Run was not found")
        }
        coral_run::Error::ApprovalNotFound => {
            ToolFailure::stable("approval_not_found", "Approval Request was not found")
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
        // Name every remaining variant so a new one needs a decision here.
        // `safe_rejection` answers these before the match.
        coral_run::Error::InvalidInput(_)
        | coral_run::Error::ProgramAdmission(_)
        | coral_run::Error::PermissionDenied(_)
        | coral_run::Error::CompilationTimedOut
        | coral_run::Error::CompilerBusy
        | coral_run::Error::CompilerUnavailable
        | coral_run::Error::PackageTooLarge { .. }
        | coral_run::Error::ShuttingDown
        // `safe_rejection` has no answer for the rest.
        | coral_run::Error::ProgramRuntimeUnavailable(_)
        | coral_run::Error::ProgramWorkerStopped
        | coral_run::Error::Clock(_)
        | coral_run::Error::IdentifierIssuance(_)
        | coral_run::Error::InvalidProgramValue
        | coral_run::Error::ProgramValueJson(_)
        | coral_run::Error::InvocationPersistence(_)
        | coral_run::Error::RunEventPersistence(_)
        | coral_run::Error::ClaimMismatch
        | coral_run::Error::HeadMismatch
        | coral_run::Error::RunEnded
        | coral_run::Error::ProgramHostTerminated
        | coral_run::Error::Provider(_)
        | coral_run::Error::Policy(_)
        | coral_run::Error::Upstream(_)
        | coral_run::Error::CorruptData(_)
        | coral_run::Error::Database(_) => ToolFailure::internal(),
    };
    ToolCallError::Tool(failure)
}

/// A refusal reports the same code and message whichever module's error
/// wrapped it.
fn refusal_failure(refusal: coral_workspace::AuthorityRefusal) -> ToolFailure {
    ToolFailure::stable(refusal.code(), refusal.message())
}

fn workspace_tool_failure(error: coral_workspace::Error) -> ToolFailure {
    if let Some(refusal) = error.refusal() {
        return refusal_failure(refusal);
    }
    match error {
        coral_workspace::Error::InvalidInput(message) => {
            ToolFailure::safe("invalid_input", message)
        }
        // Name every remaining variant so a new one needs a decision here.
        // `refusal` answers these before the match.
        coral_workspace::Error::WorkspaceNotFound
        | coral_workspace::Error::WorkspaceArchived
        | coral_workspace::Error::PermissionDenied(_)
        | coral_workspace::Error::AgentNotFound
        | coral_workspace::Error::InvalidAgentCredential
        | coral_workspace::Error::AgentDisabled
        | coral_workspace::Error::AgentRegistrationExpired
        // `refusal` has no answer for the rest.
        | coral_workspace::Error::OrganizationAuthorityConflict
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
        | coral_workspace::Error::OrganizationMembershipNotFound
        | coral_workspace::Error::LastOrganizationOwner
        | coral_workspace::Error::MemberTargetNotEnabled
        | coral_workspace::Error::AccessRemovalMismatch
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
