//! The bounded values of the catalog's attributes. A release can add a
//! value, but never rename or remove one.

/// Declares a bounded attribute: an enum whose variants each name one value.
macro_rules! attribute {
    ($(#[$meta:meta])* $name:ident { $($(#[$variant_meta:meta])* $variant:ident => $value:literal,)* }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name {
            $(
                #[doc = concat!("`", $value, "`")]
                $(#[$variant_meta])*
                $variant,
            )*
        }

        impl $name {
            /// The attribute's value.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value,)*
                }
            }
        }
    };
}

attribute! {
    /// The listener a request reached, for `interface`.
    Interface {
        /// The MCP endpoint and its metadata routes.
        Mcp => "mcp",
        /// The private gRPC interface.
        Grpc => "grpc",
        /// The authorization server and Provider callbacks.
        Auth => "auth",
    }
}

attribute! {
    /// How a request ended, for `outcome`.
    RequestOutcome {
        Ok => "ok",
        /// The caller's fault, such as a 4xx status.
        ClientError => "client_error",
        /// The server's fault, such as a 5xx status or a broken body.
        ServerError => "server_error",
    }
}

attribute! {
    /// How a Program compile ended, for `outcome`.
    CompileOutcome {
        Ok => "ok",
        /// The Program did not compile or failed admission.
        Rejected => "rejected",
        TimedOut => "timed_out",
        /// The compiler was busy or stopped.
        Unavailable => "unavailable",
    }
}

attribute! {
    /// Why Coral stopped a Program before it returned, for `outcome`.
    TerminationReason {
        Canceled => "canceled",
        TimedOut => "timed_out",
        Interrupted => "interrupted",
        MemoryLimitExceeded => "memory_limit_exceeded",
    }
}

attribute! {
    /// Whether a Principal is a User or an Agent, for `principal.kind`.
    PrincipalKind {
        User => "user",
        Agent => "agent",
    }
}

attribute! {
    /// A Program Run's terminal state, for `run.status`.
    RunStatus {
        Succeeded => "succeeded",
        Failed => "failed",
        Canceled => "canceled",
        TimedOut => "timed_out",
        Interrupted => "interrupted",
    }
}

attribute! {
    /// One phase of a Program Run under its Run Owner, for `run.phase`.
    RunPhase {
        Start => "start",
        Execute => "execute",
        Complete => "complete",
        Terminate => "terminate",
        Recover => "recover",
    }
}

attribute! {
    /// How a recovered Run Owner's one chunk ended, for `outcome`.
    RecoveryOutcome {
        /// The chunk ended the Run.
        Ended => "ended",
        /// The Run had already ended.
        AlreadyEnded => "already_ended",
        /// Another Coral Instance owns the Run.
        Lost => "lost",
        /// The chunk failed.
        Failed => "failed",
    }
}

attribute! {
    /// A claim to own a Program Run, for `operation`.
    ClaimKind {
        /// The claim written with the accepted Run.
        Accept => "accept",
        /// The Run Owner's start mark, which renews the acceptance claim.
        Start => "start",
        /// An expired-lease sweep's takeover.
        Recover => "recover",
        /// A panicked Run Owner's supervisor taking over to end its Run.
        Takeover => "takeover",
    }
}

attribute! {
    /// Whether a claim won the Run, for `outcome`.
    ClaimOutcome {
        Won => "won",
        Lost => "lost",
    }
}

attribute! {
    /// The protocol of a Provider Tool, for `provider.protocol`.
    ProviderProtocol {
        Http => "http",
        Mcp => "mcp",
        /// A Tool Coral serves itself.
        Builtin => "builtin",
    }
}

attribute! {
    /// Whether a Tool reads or changes provider state, for `tool.effect`.
    ToolEffect {
        /// The Tool declares itself read-only.
        Read => "read",
        Write => "write",
    }
}

attribute! {
    /// How a Tool Invocation ended, for `invocation.result_kind`.
    InvocationEnd {
        UpstreamOutcome => "upstream_outcome",
        CoralFailure => "coral_failure",
        RunTerminated => "run_terminated",
        Indeterminate => "indeterminate",
        /// The Invocation ended without a result.
        HostDefect => "host_defect",
    }
}

attribute! {
    /// A gate a Tool Invocation passes before its Dispatch Fence, for
    /// `gate.kind`.
    GateKind {
        Authority => "authority",
        Account => "account",
        Approval => "approval",
    }
}

attribute! {
    /// How a request to an Upstream Service ended, for `outcome`.
    UpstreamOutcome {
        /// A complete response, whatever its status.
        Response => "response",
        /// The response was larger than Coral keeps.
        TooLarge => "too_large",
        /// No complete response.
        Failed => "failed",
    }
}

attribute! {
    /// The database, for `db.system`.
    DbSystem {
        Sqlite => "sqlite",
        Postgresql => "postgresql",
    }
}

attribute! {
    /// How a database transaction ended, for `outcome`.
    TransactionOutcome {
        Committed => "committed",
        RolledBack => "rolled_back",
        /// The commit failed.
        Failed => "failed",
    }
}

attribute! {
    /// What a Program Run kept, for `output.kind`.
    OutputKind {
        FinalValue => "final_value",
        Emission => "emission",
        Log => "log",
        /// A Tool Invocation's result.
        Result => "result",
    }
}

attribute! {
    /// A kind of telemetry signal, for `signal.kind`.
    SignalKind {
        Span => "span",
        Log => "log",
        Metric => "metric",
    }
}

attribute! {
    /// Why Coral dropped a signal, for `drop.reason`.
    DropReason {
        /// The export queue was full.
        QueueFull => "queue_full",
        /// The exporter failed to send it.
        ExportFailed => "export_failed",
    }
}
