//! What a Turn records in traces and metrics, through the names that core's
//! `coral-telemetry` declares: counts, token numbers and durations, never text.

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use coral_task::FoundTaskId;
use coral_telemetry::metric::{
    self, CompactionOutcome, ModelCallOutcome, SubAgentTurnOutcome, TokenType,
};
use coral_telemetry::safe;
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};
use reqwest::StatusCode;
use tracing::Instrument as _;
use tracing::field::Empty;
use url::Url;

use crate::model::{ModelError, Reply};
use crate::prompt;
use crate::sub_agents::TurnFailure;

/// The trace and metrics of one Turn. Its span is the root of a trace of its
/// own, linked to the span of the `agent` call that started the Turn.
pub(crate) struct TurnTrace {
    span: tracing::Span,
    started: Instant,
    model_calls: AtomicU64,
    tool_calls: AtomicU64,
    compactions: AtomicU64,
    _running: metric::LevelGuard,
}

impl TurnTrace {
    /// Begins the trace of a Turn of `sub_agent_id`, an ID Coral minted, and
    /// links it to the current span.
    pub(crate) fn begin(
        sub_agent_id: &str,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        task: FoundTaskId<'_>,
    ) -> Self {
        let span = tracing::info_span!(
            parent: None,
            "coral.sub_agent.turn",
            coral.workspace.id = safe::id(workspace_id),
            coral.principal.id = safe::id(&actor.telemetry_id()),
            coral.principal.kind = safe::kind(actor.principal().kind().as_storage_str()),
            coral.task.id = safe::id(&task),
            coral.sub_agent.id = safe::id(&Minted(sub_agent_id)),
            coral.sub_agent.turn.outcome = Empty,
            coral.sub_agent.turn.model_calls = Empty,
            coral.sub_agent.turn.tool_calls = Empty,
            coral.sub_agent.turn.compactions = Empty,
            error.type = Empty,
        );
        coral_telemetry::link_to_current(&span);
        Self {
            span,
            started: Instant::now(),
            model_calls: AtomicU64::new(0),
            tool_calls: AtomicU64::new(0),
            compactions: AtomicU64::new(0),
            _running: metric::sub_agent_turn_active(),
        }
    }

    /// The Turn's span, for the task that runs the Turn.
    pub(crate) fn span(&self) -> tracing::Span {
        self.span.clone()
    }

    pub(crate) fn count_model_call(&self) {
        self.model_calls.fetch_add(1, Ordering::Relaxed);
    }

    /// Runs `call`, the tool call `name`, in a span of its own, so that a Run
    /// it starts links to that span.
    pub(crate) async fn tool_call<F: Future>(&self, name: &str, call: F) -> F::Output {
        self.tool_calls.fetch_add(1, Ordering::Relaxed);
        // A name the model made up is its text, so it stays off the span.
        let tool = prompt::TOOLS
            .iter()
            .map(|tool| tool.name)
            .find(|served| *served == name);
        let span = tracing::info_span!(
            "coral.sub_agent.tool_call",
            gen_ai.tool.name = tool.map(safe::kind),
            coral.run.id = Empty,
            error.type = Empty,
        );
        call.instrument(span).await
    }

    /// Opens the span of a compaction of a thread of `before` tokens.
    pub(crate) fn compaction(&self, before: usize) -> CompactionTrace<'_> {
        let span = tracing::info_span!(
            "coral.sub_agent.compaction",
            coral.sub_agent.compaction.tokens_before = safe::number(before),
            coral.sub_agent.compaction.tokens_after = Empty,
        );
        CompactionTrace { turn: self, span }
    }

    /// Records how the Turn ended.
    pub(crate) fn ended(&self, end: TurnEnd) {
        let (outcome, code) = match end {
            TurnEnd::Completed => (SubAgentTurnOutcome::Completed, None),
            TurnEnd::Failed(code) => (SubAgentTurnOutcome::Failed, Some(code)),
            TurnEnd::Canceled => (SubAgentTurnOutcome::Canceled, None),
            TurnEnd::Interrupted => (SubAgentTurnOutcome::Interrupted, None),
        };
        let model_calls = self.model_calls.load(Ordering::Relaxed);
        let tool_calls = self.tool_calls.load(Ordering::Relaxed);
        let span = &self.span;
        span.record("coral.sub_agent.turn.outcome", safe::kind(outcome.as_str()));
        span.record(
            "coral.sub_agent.turn.model_calls",
            safe::number(model_calls),
        );
        span.record("coral.sub_agent.turn.tool_calls", safe::number(tool_calls));
        let compactions = self.compactions.load(Ordering::Relaxed);
        span.record(
            "coral.sub_agent.turn.compactions",
            safe::number(compactions),
        );
        if let Some(code) = code {
            coral_telemetry::record_failure(span, code);
        }
        metric::sub_agent_turn_ended(outcome, code, self.started.elapsed(), model_calls);
    }
}

/// How a Turn ended, with a failure's code.
#[derive(Debug, Clone, Copy)]
pub(crate) enum TurnEnd {
    Completed,
    Failed(&'static str),
    Canceled,
    Interrupted,
}

impl TurnEnd {
    /// How a Turn whose work ended on `answered` ended.
    pub(crate) const fn of(answered: &Result<String, TurnFailure>) -> Self {
        match answered {
            Ok(_) => Self::Completed,
            Err(TurnFailure::Model(error)) => Self::Failed(model_failure(error)),
            Err(TurnFailure::TooManyModelCalls) => Self::Failed("too_many_model_calls"),
            Err(TurnFailure::NoAnswer) => Self::Failed("no_answer"),
            Err(TurnFailure::TimeLimit) => Self::Failed("time_limit"),
            Err(TurnFailure::Internal) => Self::Failed("internal"),
            Err(TurnFailure::Canceled) => Self::Canceled,
        }
    }
}

/// The span of one compaction of a Turn's thread.
pub(crate) struct CompactionTrace<'a> {
    turn: &'a TurnTrace,
    span: tracing::Span,
}

impl CompactionTrace<'_> {
    /// The compaction's span, for the work it covers.
    pub(crate) fn span(&self) -> tracing::Span {
        self.span.clone()
    }

    /// Records a compaction that left the thread at `after` tokens: done if
    /// it `summarized`, else skipped.
    pub(crate) fn ended(self, summarized: bool, after: usize) {
        let span = &self.span;
        span.record(
            "coral.sub_agent.compaction.tokens_after",
            safe::number(after),
        );
        let outcome = if summarized {
            self.turn.compactions.fetch_add(1, Ordering::Relaxed);
            CompactionOutcome::Done
        } else {
            CompactionOutcome::Skipped
        };
        metric::sub_agent_compacted(outcome);
    }
}

/// The span and metrics of one model call, its retries included.
pub(crate) struct ModelCallTrace {
    span: tracing::Span,
    started: Instant,
    retries: u32,
}

impl ModelCallTrace {
    /// Opens the span of a call to `model` at `url`, as the config file names
    /// them.
    pub(crate) fn open(model: &str, url: &str) -> Self {
        let url = Url::parse(url).ok();
        let host = url.as_ref().and_then(Url::host_str).map(Configured);
        let port = url.as_ref().and_then(Url::port_or_known_default);
        let span = tracing::info_span!(
            "coral.sub_agent.model_call",
            otel.kind = "client",
            gen_ai.request.model = safe::id(&Configured(model)),
            server.address = host.as_ref().map(safe::id),
            server.port = port.map(safe::number),
            gen_ai.response.finish_reasons = Empty,
            gen_ai.usage.input_tokens = Empty,
            gen_ai.usage.cache_read.input_tokens = Empty,
            gen_ai.usage.output_tokens = Empty,
            coral.sub_agent.model_call.retries = Empty,
            http.response.status_code = Empty,
            error.type = Empty,
        );
        Self {
            span,
            started: Instant::now(),
            retries: 0,
        }
    }

    pub(crate) const fn retried(&mut self) {
        self.retries += 1;
    }

    /// Records how the call ended, and closes its span.
    pub(crate) fn ended(self, result: &Result<Reply, ModelError>) {
        let span = &self.span;
        span.record(
            "coral.sub_agent.model_call.retries",
            safe::number(self.retries),
        );
        let (outcome, tokens) = match result {
            Ok(reply) => (ModelCallOutcome::Ok, answered(span, reply)),
            Err(error) => (failed(span, error), Vec::new()),
        };
        metric::sub_agent_model_called(outcome, self.started.elapsed(), &tokens);
    }
}

/// Records a reply's finish reason and tokens on its span, and returns its
/// tokens by type. The span's input counts cached input, as the conventions
/// ask; the metric's does not.
fn answered(span: &tracing::Span, reply: &Reply) -> Vec<(TokenType, u64)> {
    if let Some(reason) = &reply.finish_reason {
        let reason = finish_reason(reason);
        span.record("gen_ai.response.finish_reasons", safe::kind(reason));
    }
    let Some(usage) = reply.call.usage else {
        return Vec::new();
    };
    let count = |tokens: i64| u64::try_from(tokens).unwrap_or_default();
    let input = count(usage.input_tokens);
    let cached = count(usage.cache_read_input_tokens);
    let output = count(usage.output_tokens);
    let all_input = input.saturating_add(cached);
    span.record("gen_ai.usage.input_tokens", safe::number(all_input));
    span.record("gen_ai.usage.cache_read.input_tokens", safe::number(cached));
    span.record("gen_ai.usage.output_tokens", safe::number(output));
    vec![
        (TokenType::Input, input),
        (TokenType::CachedInput, cached),
        (TokenType::Output, output),
    ]
}

/// Records why a model call failed on its span, without the server's text,
/// and returns its outcome.
fn failed(span: &tracing::Span, error: &ModelError) -> ModelCallOutcome {
    let status = match error {
        ModelError::Unavailable(status) => *status,
        ModelError::Refused(status) | ModelError::ContextTooLong(status) => Some(*status),
        ModelError::Misconfigured => None,
    };
    if let Some(status) = status {
        span.record("http.response.status_code", safe::number(status.as_u16()));
    }
    coral_telemetry::record_failure(span, model_failure(error));
    match (error, status) {
        (ModelError::Unavailable(None), _) => ModelCallOutcome::Unreachable,
        (_, Some(StatusCode::TOO_MANY_REQUESTS)) => ModelCallOutcome::TooManyRequests,
        (_, Some(status)) if status.is_server_error() => ModelCallOutcome::ServerError,
        _ => ModelCallOutcome::Other,
    }
}

const fn model_failure(error: &ModelError) -> &'static str {
    match error {
        ModelError::Unavailable(_) => "model_unavailable",
        ModelError::Refused(_) => "model_refused",
        ModelError::ContextTooLong(_) => "model_context_too_long",
        ModelError::Misconfigured => "model_misconfigured",
    }
}

/// The finish reason the server wrote, as one of a fixed few.
fn finish_reason(reason: &str) -> &'static str {
    match reason {
        "stop" => "stop",
        "length" => "length",
        "tool_calls" => "tool_calls",
        "content_filter" => "content_filter",
        _ => "other",
    }
}

/// A Sub-agent's ID, which Coral minted.
struct Minted<'a>(&'a str);

/// Coral minted it, so it holds no caller text.
impl safe::Identifier for Minted<'_> {}

impl fmt::Display for Minted<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

/// The model's name or its server's host, from the `[sub_agent]` section.
struct Configured<'a>(&'a str);

/// The operator wrote it in the config file; no caller chooses it.
impl safe::Identifier for Configured<'_> {}

impl fmt::Display for Configured<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}
