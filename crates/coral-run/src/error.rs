// One table defines each failure, its doc and its durable code, so the enum, `code()`
// and decoding cannot drift apart. Where two failures share a code, decoding returns
// the first one listed.
macro_rules! coral_failures {
    ($($(#[doc = $doc:literal])* $variant:ident => $code:literal,)*) => {
        /// Known safe Invocation failures. Only this vocabulary, and a [`Rejection`]
        /// reason Coral writes, can cross into a Program; private Adapter errors and
        /// host defects never supply their own message.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum CoralFailure {
            $($(#[doc = $doc])* $variant,)*
        }

        impl CoralFailure {
            pub(crate) const ALL: &[Self] = &[$(Self::$variant),*];

            /// Stable, transport-neutral code, with no retry or effect claim.
            #[must_use]
            pub const fn code(self) -> &'static str {
                match self {
                    $(Self::$variant => $code,)*
                }
            }
        }
    };
}

coral_failures! {
    /// The destination is outside the deployment outbound policy.
    OutboundDestinationRejected => "outbound_destination_rejected",
    /// No current Tool matches the requested Call Path.
    ToolNotFound => "tool_not_found",
    /// Logical arguments cannot satisfy the Tool contract.
    InvalidInput => "invalid_input",
    /// Encoded Tool input exceeds its accepted bound.
    InputTooLarge => "tool_input_too_large",
    /// The cataloged Tool cannot be invoked faithfully.
    ToolUnavailable => "tool_unavailable",
    /// An authorized human decision is still required.
    ApprovalRequired => "approval_required",
    /// An authorized User declined this Invocation.
    ApprovalDeclined => "approval_declined",
    /// The exact Approval Request deadline elapsed.
    ApprovalExpired => "approval_expired",
    /// The Provider is no longer available in the Workspace.
    ProviderUnavailable => "provider_unavailable",
    /// Another Provider in the Workspace already uses the requested namespace.
    ProviderNamespaceConflict => "provider_namespace_conflict",
    /// No Provider in the Workspace holds the requested namespace.
    ProviderNotFound => "provider_not_found",
    /// Coral attempted the Provider's Publication, and it failed.
    ProviderPublicationFailed => "provider_publication_failed",
    /// Exact Tool policy disables this Invocation.
    ToolDisabled => "tool_disabled",
    /// The retained Credential Route changed.
    CredentialRouteChanged => "credential_route_changed",
    /// The exact retained route has no compatible usable Account.
    AccountRequired => "account_required",
    /// Credential refresh could not complete; Account state is unchanged.
    CredentialRefreshFailed => "credential_refresh_failed",
    /// The Provider owner must repair the shared OAuth application; account grants remain intact.
    OAuthClientConfiguration => "oauth_client_configuration",
    /// The exact Account authorization changed before dispatch.
    AccountAuthorizationChanged => "account_authorization_changed",
    /// The User declined its shared connection request.
    AccountConnectionDeclined => "account_connection_declined",
    /// The shared connection request expired.
    AccountConnectionExpired => "account_connection_expired",
    /// Current authority does not permit the Tool.
    AuthorityLost => "dispatch_authority_lost",
    /// No complete bounded upstream result was received.
    UpstreamIncomplete => "upstream_incomplete",
    /// The upstream peer received the request and answered with a protocol error.
    UpstreamRejected => "upstream_rejected",
    // Per-result and cumulative output limits share one durable code; this one decodes.
    /// The upstream result exceeds its accepted bound.
    ResultTooLarge => "upstream_result_too_large",
    /// The upstream result would exceed the Run's remaining output budget.
    RunOutputTooLarge => "upstream_result_too_large",
}

impl CoralFailure {
    /// An invalid-params rejection is the Tool contract refusing the
    /// arguments, a tool the peer no longer lists leaves the cataloged Tool
    /// nothing to invoke, and every other protocol error is the peer refusing
    /// the request.
    #[must_use]
    pub(crate) const fn from_mcp_rejection(rejection: coral_upstream::McpRejection) -> Self {
        match rejection {
            coral_upstream::McpRejection::InvalidParams => Self::InvalidInput,
            coral_upstream::McpRejection::UnknownTool => Self::ToolUnavailable,
            coral_upstream::McpRejection::Other(_) => Self::UpstreamRejected,
        }
    }

    /// Inverse of [`Self::code`] for durable evidence.
    pub(crate) fn from_code(code: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|failure| failure.code() == code)
    }

    /// Safe message without upstream content or authority configuration.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::ToolNotFound => "Tool Call Path was not found",
            Self::InvalidInput => "Tool arguments are invalid",
            Self::InputTooLarge => "Tool input exceeds its limit",
            Self::ToolUnavailable => "Tool is unavailable",
            Self::ApprovalRequired => "Tool requires approval",
            Self::ApprovalDeclined => "Invocation approval was declined",
            Self::ApprovalExpired => "Invocation approval expired",
            Self::ToolDisabled => "Tool is disabled",
            Self::ProviderUnavailable => "Provider is unavailable",
            Self::ProviderNamespaceConflict => "Provider namespace is already in use",
            Self::ProviderNotFound => "Provider was not found",
            Self::ProviderPublicationFailed => "Coral could not publish the Provider",
            Self::CredentialRouteChanged => "Tool credential route changed",
            Self::AccountRequired => "Tool requires a Provider Account",
            Self::CredentialRefreshFailed => "Tool credential refresh failed",
            Self::OAuthClientConfiguration => {
                "Ask the Provider owner to repair the OAuth application"
            }
            Self::AccountAuthorizationChanged => "Tool account authorization changed",
            Self::AccountConnectionDeclined => "Account connection was declined",
            Self::AccountConnectionExpired => "Account connection expired",
            Self::AuthorityLost => "Tool is not permitted",
            Self::OutboundDestinationRejected => "Provider destination is not permitted",
            Self::UpstreamIncomplete => "Upstream result is incomplete",
            Self::UpstreamRejected => "Upstream rejected the request",
            Self::ResultTooLarge => "Upstream result exceeds its limit",
            Self::RunOutputTooLarge => "Run output exceeds its limit",
        }
    }
}

/// Why a complete upstream outcome rejects its Tool call. The upstream
/// answered, so no code here is a [`CoralFailure`] code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpstreamRejection {
    /// A non-2xx HTTP status, or an MCP tool result with `isError: true`.
    Error,
    /// A 2xx status or media type that the Tool does not declare as a success.
    Undeclared,
    /// A body Coral could not decode.
    Undecodable,
}

impl UpstreamRejection {
    /// The rejection for an outcome of `class`, or `None` for a declared success.
    pub(crate) const fn of(class: coral_upstream::OutcomeClass) -> Option<Self> {
        match class {
            coral_upstream::OutcomeClass::Success => None,
            coral_upstream::OutcomeClass::ErrorResponse => Some(Self::Error),
            coral_upstream::OutcomeClass::UndeclaredResponse => Some(Self::Undeclared),
            coral_upstream::OutcomeClass::UndecodableResponse => Some(Self::Undecodable),
        }
    }

    /// Stable, transport-neutral code.
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::Error => "error_response",
            Self::Undeclared => "undeclared_response",
            Self::Undecodable => "undecodable_response",
        }
    }

    /// Safe message without upstream content.
    pub(crate) const fn message(self) -> &'static str {
        match self {
            Self::Error => "The upstream service returned an error",
            Self::Undeclared => "The upstream response is not a success this Tool declares",
            Self::Undecodable => "The upstream response body could not be decoded",
        }
    }
}

impl std::fmt::Display for CoralFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message())
    }
}

/// A Coral Failure and each argument the Program should correct, when Coral
/// knows it. Only the code is durable evidence that later work decodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    failure: CoralFailure,
    problems: Vec<InputProblem>,
}

/// One argument to correct. Coral writes each reason; it never holds a
/// submitted value, upstream content or a host diagnostic. A field or key name
/// the Program wrote can appear, since it says what to correct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InputProblem {
    field: String,
    reason: String,
}

impl InputProblem {
    /// The argument at `field`, a dotted path from the arguments object. An
    /// empty path names the arguments object itself.
    pub(crate) fn new(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            reason: reason.into(),
        }
    }
}

impl Rejection {
    /// `invalid_input` for the one argument at `field`.
    pub(crate) fn invalid_input(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::invalid_inputs(vec![InputProblem::new(field, reason)])
    }

    /// `invalid_input` for each of `problems`, in the order Coral found them.
    pub(crate) fn invalid_inputs(problems: Vec<InputProblem>) -> Self {
        Self::naming(CoralFailure::InvalidInput, problems)
    }

    /// `failure` naming each argument that caused it, such as each Call Path
    /// that names no Tool.
    pub(crate) const fn naming(failure: CoralFailure, problems: Vec<InputProblem>) -> Self {
        Self { failure, problems }
    }

    /// The durable failure.
    #[must_use]
    pub const fn failure(&self) -> CoralFailure {
        self.failure
    }

    /// Stable, transport-neutral code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.failure.code()
    }

    /// Safe, bounded message, naming each argument to correct when Coral knows it.
    #[must_use]
    pub fn message(&self) -> std::borrow::Cow<'static, str> {
        let message = match self.problems.as_slice() {
            [] => return self.failure.message().into(),
            [InputProblem { field, reason }] if !field.is_empty() => {
                format!("{} at `{field}`: {reason}", self.failure.message())
            }
            problems => {
                let problems = problems
                    .iter()
                    .map(|InputProblem { field, reason }| {
                        if field.is_empty() {
                            reason.clone()
                        } else {
                            format!("`{field}`: {reason}")
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("; ");
                format!("{}: {problems}", self.failure.message())
            }
        };
        coral_v8::bounded_diagnostic_message(&message).into()
    }
}

impl From<CoralFailure> for Rejection {
    fn from(failure: CoralFailure) -> Self {
        Self {
            failure,
            problems: Vec::new(),
        }
    }
}

impl std::fmt::Display for Rejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message())
    }
}

/// Program Run Module failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Caller-supplied domain input was invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The Run is absent or concealed from this Principal.
    #[error("Program Run was not found")]
    RunNotFound,
    /// The Approval Request is absent or concealed from this Principal.
    #[error("Approval Request was not found")]
    ApprovalNotFound,
    /// Program source could not be lowered into an accepted package.
    #[error("Program admission failed: {0}")]
    ProgramAdmission(String),
    /// Current Saved Function selection or cleanup failed.
    #[error(transparent)]
    Functions(#[from] coral_functions::Error),
    /// Current Principal authority does not permit this staged operation.
    #[error("permission denied: {0}")]
    PermissionDenied(&'static str),
    /// Compilation exceeded the fixed authoring deadline.
    #[error("Program compilation exceeded the two-second deadline")]
    CompilationTimedOut,
    /// The compiler queue was full, so the Program was not judged. Retryable.
    #[error("Program compiler is busy; retry shortly")]
    CompilerBusy,
    /// The emitted package exceeded a fixed authoring limit.
    #[error("Program package exceeds the {component} limit of {maximum_bytes} bytes")]
    PackageTooLarge {
        /// Package component that crossed its bound.
        component: &'static str,
        /// Fixed accepted maximum.
        maximum_bytes: usize,
    },
    /// The Program runtime could not initialize.
    #[error("Program runtime is unavailable: {0}")]
    ProgramRuntimeUnavailable(String),
    /// A worker thread stopped before returning its result.
    #[error("Program worker stopped unexpectedly")]
    ProgramWorkerStopped,
    /// The Run clock could not provide safe durable time evidence.
    #[error("Program Run clock failed: {0}")]
    Clock(String),
    /// Coral could not issue an unused Run identifier.
    #[error("Program Run identifier could not be issued: {0}")]
    IdentifierIssuance(String),
    /// A Program Value or cumulative output crossed its accepted limit.
    #[error("Program output exceeds its accepted limit")]
    ProgramValueTooLarge,
    /// A binary extension violated the Program Value contract.
    #[error("invalid Program Value")]
    InvalidProgramValue,
    /// Native Program Value JSON could not be parsed or encoded.
    #[error("Program Value JSON was invalid: {0}")]
    ProgramValueJson(#[source] serde_json::Error),
    /// A resolved Tool was rejected with safe durable evidence.
    #[error("Tool Invocation failed: {0}")]
    InvocationRejected(Rejection),
    /// Invocation persistence lost an atomic lifecycle transition.
    #[error("Tool Invocation persistence failed: {0}")]
    InvocationPersistence(String),
    /// Run Event persistence lost an atomic sequence transition.
    #[error("Program Run Event persistence failed: {0}")]
    RunEventPersistence(String),
    /// A Run Ledger chunk was built on an execution claim the Run row does not
    /// hold. Definitive: the writer has lost the Run.
    #[error("Program Run Ledger chunk was built on a stale execution claim")]
    ClaimMismatch,
    /// A Run Ledger chunk was built on a head the Run row has moved past under
    /// the same claim.
    #[error("Program Run Ledger chunk was built on a stale head")]
    HeadMismatch,
    /// A Run Ledger chunk was built on a Run that has ended. No Event follows
    /// `run.ended`.
    #[error("Program Run Ledger chunk was built on an ended Run")]
    RunEnded,
    /// The Run Owner stopped before it recorded the request. The outcome is
    /// unknown: read the Run again.
    #[error("Program Run Owner stopped before recording the request")]
    OwnerLost,
    /// An outside Run Request waits in the Run's Run Inbox and its Run Owner
    /// has not recorded it within the wait. Retry: the retry finds it still
    /// waiting, recorded, or the Run ended without it, when the record
    /// answers.
    #[error("Program Run request is not yet recorded; retry")]
    RunRequestNotYetRecorded,
    /// The process is shutting down and accepts no new Run: its Shutdown
    /// Drain lets the live ones end. No Run was written (a Task records the
    /// refusal as its attempt's rejection); the caller may retry, and during
    /// a rolling update the retry may reach the process that replaces this
    /// one.
    #[error("the server is shutting down and accepts no new Program Run; retry")]
    ShuttingDown,
    /// A fatal Program host failure stopped execution for recovery.
    #[error("Program host terminated execution")]
    ProgramHostTerminated,
    /// Provider or retained Catalog evidence rejected Invocation work.
    #[error(transparent)]
    Provider(#[from] coral_provider::Error),
    /// Exact Tool policy rejected Invocation work.
    #[error(transparent)]
    Policy(#[from] coral_policy::Error),
    /// The private upstream Adapter could not plan, send, or project a result.
    #[error(transparent)]
    Upstream(#[from] coral_upstream::Error),
    /// Workspace authority or lifecycle rejected the operation.
    #[error(transparent)]
    Workspace(#[from] coral_workspace::Error),
    /// A persisted row does not decode to the closed domain model.
    #[error("database contains corrupt Program Run data: {0}")]
    CorruptData(String),
    /// The persistence Adapter rejected the operation.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
}

impl Error {
    /// The error's stable kind, for an event's `error.type`: a bounded
    /// code that never carries the error's text.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidInput { .. } => "invalid_input",
            Self::RunNotFound { .. } => "run_not_found",
            Self::ApprovalNotFound { .. } => "approval_not_found",
            Self::ProgramAdmission { .. } => "program_admission",
            Self::Functions { .. } => "functions",
            Self::PermissionDenied { .. } => "permission_denied",
            Self::CompilationTimedOut { .. } => "compilation_timed_out",
            Self::CompilerBusy { .. } => "compiler_busy",
            Self::PackageTooLarge { .. } => "package_too_large",
            Self::ProgramRuntimeUnavailable { .. } => "program_runtime_unavailable",
            Self::ProgramWorkerStopped { .. } => "program_worker_stopped",
            Self::Clock { .. } => "clock",
            Self::IdentifierIssuance { .. } => "identifier_issuance",
            Self::ProgramValueTooLarge { .. } => "program_value_too_large",
            Self::InvalidProgramValue { .. } => "invalid_program_value",
            Self::ProgramValueJson { .. } => "program_value_json",
            Self::InvocationRejected { .. } => "invocation_rejected",
            Self::InvocationPersistence { .. } => "invocation_persistence",
            Self::RunEventPersistence { .. } => "run_event_persistence",
            Self::ClaimMismatch { .. } => "claim_mismatch",
            Self::HeadMismatch { .. } => "head_mismatch",
            Self::RunEnded { .. } => "run_ended",
            Self::OwnerLost { .. } => "owner_lost",
            Self::RunRequestNotYetRecorded { .. } => "run_request_not_yet_recorded",
            Self::ShuttingDown { .. } => "shutting_down",
            Self::ProgramHostTerminated { .. } => "program_host_terminated",
            Self::Provider { .. } => "provider",
            Self::Policy { .. } => "policy",
            Self::Upstream { .. } => "upstream",
            Self::Workspace { .. } => "workspace",
            Self::CorruptData { .. } => "corrupt_data",
            Self::Database { .. } => "database",
        }
    }

    /// Whether the database gave no answer to a write (a lost connection, a
    /// pool timeout, a lock or serialization race): the write may or may not
    /// have taken effect, and a reread decides. Every other error, a
    /// constraint violation included, is the database's or the domain's
    /// definite answer.
    #[must_use]
    pub(crate) fn is_operational(&self) -> bool {
        self.database_error()
            .is_some_and(coral_db::Error::is_unanswered)
    }

    /// The database error behind this one, through the module errors that
    /// wrap it (a Provider's Catalog or Workspace error, a Policy's Provider
    /// or Workspace error, a Saved Function's Workspace error).
    #[must_use]
    pub(crate) fn database_error(&self) -> Option<&coral_db::Error> {
        fn workspace(error: &coral_workspace::Error) -> Option<&coral_db::Error> {
            match error {
                coral_workspace::Error::Database(error) => Some(error),
                _ => None,
            }
        }
        fn catalog(error: &coral_catalog::Error) -> Option<&coral_db::Error> {
            match error {
                coral_catalog::Error::Database(error) => Some(error),
                _ => None,
            }
        }
        fn provider(error: &coral_provider::Error) -> Option<&coral_db::Error> {
            match error {
                coral_provider::Error::Database(error) => Some(error),
                coral_provider::Error::Catalog(error) => catalog(error),
                coral_provider::Error::Workspace(error) => workspace(error),
                _ => None,
            }
        }
        match self {
            Self::Database(error) => Some(error),
            Self::Workspace(error) => workspace(error),
            Self::Provider(error) => provider(error),
            Self::Policy(error) => match error {
                coral_policy::Error::Database(error) => Some(error),
                coral_policy::Error::Provider(error) => provider(error),
                coral_policy::Error::Workspace(error) => workspace(error),
                _ => None,
            },
            Self::Functions(error) => match error {
                coral_functions::Error::Database(error) => Some(error),
                coral_functions::Error::Workspace(error) => workspace(error),
                _ => None,
            },
            _ => None,
        }
    }

    /// Returns a definite safe rejection; operational uncertainty has no invented outcome.
    #[must_use]
    pub fn safe_rejection(&self) -> Option<(&'static str, String)> {
        if let Self::Workspace(error) = self {
            return error
                .safe_rejection()
                .map(|(code, message)| (code, message.to_owned()));
        }
        let code = match self {
            Self::InvalidInput(_) => "invalid_input",
            Self::ProgramAdmission(_) => "program_admission_failed",
            Self::CompilationTimedOut => "program_compilation_timed_out",
            Self::CompilerBusy => "program_compiler_busy",
            Self::PackageTooLarge { .. } => "program_package_too_large",
            Self::Functions(error) => return error.safe_rejection(),
            Self::PermissionDenied(_) => "permission_denied",
            Self::ShuttingDown => "shutting_down",
            _ => return None,
        };
        Some((
            code,
            coral_v8::bounded_diagnostic_message(&self.to_string()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{CoralFailure, Error};

    #[test]
    fn an_unanswered_database_error_is_operational_through_every_module_wrapper() {
        type Wrap = fn(coral_db::Error) -> Error;
        let unanswered = || coral_db::Error::WriterBusy;
        let definite = || coral_db::Error::Sqlx(sqlx::Error::RowNotFound);
        let wrapped: [(Wrap, &str); 5] = [
            (|error| Error::Database(error), "run"),
            (
                |error| Error::Workspace(coral_workspace::Error::Database(error)),
                "workspace",
            ),
            (
                |error| {
                    Error::Provider(coral_provider::Error::Catalog(
                        coral_catalog::Error::Database(error),
                    ))
                },
                "provider catalog",
            ),
            (
                |error| {
                    Error::Policy(coral_policy::Error::Provider(
                        coral_provider::Error::Workspace(coral_workspace::Error::Database(error)),
                    ))
                },
                "policy provider workspace",
            ),
            (
                |error| {
                    Error::Functions(coral_functions::Error::Workspace(
                        coral_workspace::Error::Database(error),
                    ))
                },
                "functions workspace",
            ),
        ];
        for (wrap, label) in wrapped {
            assert!(wrap(unanswered()).is_operational(), "{label}");
            assert!(!wrap(definite()).is_operational(), "{label}");
        }
        assert!(!Error::RunNotFound.is_operational());
        assert!(!Error::ClaimMismatch.is_operational());
        assert!(!Error::RunRequestNotYetRecorded.is_operational());
    }

    #[test]
    fn durable_failure_codes_are_distinct_except_the_output_limit_alias() {
        let mut shared = std::collections::BTreeMap::<_, Vec<_>>::new();
        for &failure in CoralFailure::ALL {
            shared.entry(failure.code()).or_default().push(failure);
        }
        shared.retain(|_, failures| failures.len() > 1);
        assert_eq!(
            shared,
            std::collections::BTreeMap::from([(
                "upstream_result_too_large",
                vec![
                    CoralFailure::ResultTooLarge,
                    CoralFailure::RunOutputTooLarge
                ]
            )])
        );
        assert_eq!(
            CoralFailure::from_code("upstream_result_too_large"),
            Some(CoralFailure::ResultTooLarge)
        );
        assert_eq!(CoralFailure::from_code("unknown"), None);
    }

    #[test]
    fn upstream_rejection_codes_never_name_a_coral_failure() {
        use coral_upstream::OutcomeClass;
        assert_eq!(super::UpstreamRejection::of(OutcomeClass::Success), None);
        for class in [
            OutcomeClass::ErrorResponse,
            OutcomeClass::UndeclaredResponse,
            OutcomeClass::UndecodableResponse,
        ] {
            let code = super::UpstreamRejection::of(class)
                .expect("rejection")
                .code();
            assert_eq!(CoralFailure::from_code(code), None, "{code}");
        }
    }
}
