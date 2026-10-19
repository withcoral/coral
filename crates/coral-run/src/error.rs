// One table defines each failure, its doc and its durable code, so the enum, `code()`
// and decoding cannot drift apart. Where two failures share a code, decoding returns
// the first one listed.
macro_rules! coral_failures {
    ($($(#[doc = $doc:literal])* $variant:ident => $code:literal,)*) => {
        /// Known safe Invocation failures. Only this vocabulary can cross into a Program;
        /// private Adapter errors and host defects never supply their own message.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum CoralFailure {
            $($(#[doc = $doc])* $variant,)*
        }

        impl CoralFailure {
            const ALL: &[Self] = &[$(Self::$variant),*];

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
    /// No current Operation matches the requested Call Path.
    OperationNotFound => "operation_not_found",
    /// Logical arguments cannot satisfy the Operation contract.
    InvalidInput => "invalid_input",
    /// Encoded Operation input exceeds its accepted bound.
    InputTooLarge => "operation_input_too_large",
    /// The cataloged Operation cannot be invoked faithfully.
    OperationUnavailable => "operation_unavailable",
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
    /// Coral attempted the Provider's Publication, and it failed.
    ProviderPublicationFailed => "provider_publication_failed",
    /// Exact Operation policy disables this Invocation.
    OperationDisabled => "operation_disabled",
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
    /// Current authority does not permit the Operation.
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
    /// An invalid-params rejection is the Operation contract refusing the
    /// arguments, a tool the peer no longer lists leaves the cataloged Operation
    /// nothing to invoke, and every other protocol error is the peer refusing
    /// the request.
    #[must_use]
    pub(crate) const fn from_mcp_rejection(rejection: coral_upstream::McpRejection) -> Self {
        match rejection {
            coral_upstream::McpRejection::InvalidParams => Self::InvalidInput,
            coral_upstream::McpRejection::UnknownTool => Self::OperationUnavailable,
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
            Self::OperationNotFound => "Operation Call Path was not found",
            Self::InvalidInput => "Operation arguments are invalid",
            Self::InputTooLarge => "Operation input exceeds its limit",
            Self::OperationUnavailable => "Operation is unavailable",
            Self::ApprovalRequired => "Operation requires approval",
            Self::ApprovalDeclined => "Invocation approval was declined",
            Self::ApprovalExpired => "Invocation approval expired",
            Self::OperationDisabled => "Operation is disabled",
            Self::ProviderUnavailable => "Provider is unavailable",
            Self::ProviderNamespaceConflict => "Provider namespace is already in use",
            Self::ProviderPublicationFailed => "Coral could not publish the Provider",
            Self::CredentialRouteChanged => "Operation credential route changed",
            Self::AccountRequired => "Operation requires a Provider Account",
            Self::CredentialRefreshFailed => "Operation credential refresh failed",
            Self::OAuthClientConfiguration => {
                "Ask the Provider owner to repair the OAuth application"
            }
            Self::AccountAuthorizationChanged => "Operation account authorization changed",
            Self::AccountConnectionDeclined => "Account connection was declined",
            Self::AccountConnectionExpired => "Account connection expired",
            Self::AuthorityLost => "Operation is not permitted",
            Self::OutboundDestinationRejected => "Provider destination is not permitted",
            Self::UpstreamIncomplete => "Upstream result is incomplete",
            Self::UpstreamRejected => "Upstream rejected the request",
            Self::ResultTooLarge => "Upstream result exceeds its limit",
            Self::RunOutputTooLarge => "Run output exceeds its limit",
        }
    }
}

/// Why a complete upstream outcome rejects its Operation call. The upstream
/// answered, so no code here is a [`CoralFailure`] code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpstreamRejection {
    /// A non-2xx HTTP status, or an MCP tool result with `isError: true`.
    Error,
    /// A 2xx status or media type that the Operation does not declare as a success.
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
            Self::Undeclared => "The upstream response is not a success this Operation declares",
            Self::Undecodable => "The upstream response body could not be decoded",
        }
    }
}

impl std::fmt::Display for CoralFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message())
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
    /// A Program Value or cumulative output crossed its accepted limit.
    #[error("Program output exceeds its accepted limit")]
    ProgramValueTooLarge,
    /// A binary extension violated the Program Value contract.
    #[error("invalid Program Value")]
    InvalidProgramValue,
    /// Native Program Value JSON could not be parsed or encoded.
    #[error("Program Value JSON was invalid: {0}")]
    ProgramValueJson(#[source] serde_json::Error),
    /// A resolved Operation was rejected with safe durable evidence.
    #[error("Operation Invocation failed: {0}")]
    InvocationRejected(CoralFailure),
    /// Invocation persistence lost an atomic lifecycle transition.
    #[error("Operation Invocation persistence failed: {0}")]
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
    /// A fatal Program host failure stopped execution for recovery.
    #[error("Program host terminated execution")]
    ProgramHostTerminated,
    /// Provider or retained Catalog evidence rejected Invocation work.
    #[error(transparent)]
    Provider(#[from] coral_provider::Error),
    /// Exact Operation policy rejected Invocation work.
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
            Self::PackageTooLarge { .. } => "program_package_too_large",
            Self::Functions(error) => return error.safe_rejection(),
            Self::PermissionDenied(_) => "permission_denied",
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
    use super::CoralFailure;

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
