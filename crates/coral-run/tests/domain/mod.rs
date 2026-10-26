mod action_prompts;
mod admission;
mod agents;
mod approvals;
mod binary;
mod cancellation;
mod connections;
mod credentials;
mod emissions;
mod fan_out;
mod fence;
mod history;
mod invocations;
mod ledger;
mod locations;
mod mcp;
mod memory;
mod observation;
mod output;
mod owner;
mod provider_setup;
mod provider_update;
mod refresh;
mod required_actions;
mod resilience;
mod scoping;
mod summary;
mod support;
mod updates;

use coral_outbound::DestinationPolicy;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use async_trait::async_trait;
use coral_catalog::{OpenApiDocument, OperationKey};
use coral_policy::{OperationAccessState, OperationPolicyModule, OperationPolicyTarget};
use coral_provider::access::{CredentialRoute, ProviderNamespace};
use coral_provider::{
    ManagementRequestId, ProviderId, ProviderModule, ProviderRegenerationRequest,
    WorkspaceProviderPublicationRequest,
};
use coral_test_support::{TestDatabase, provision_user as organization_user};
use coral_upstream::{HttpOutcome, ReadyDispatch, UpstreamOutcome};
use coral_workspace::AuthenticatedPrincipal;
use coral_workspace::{
    AuthenticatedUser, ChangeContext, OrganizationRole, TraceId, UserId, WorkspaceDisplayName,
    WorkspaceModule, WorkspaceRole,
};
use sea_query::{Alias, Expr, ExprTrait as _, Query};
use tokio::sync::{oneshot, watch};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::engine::{
    Clock, ExecutionCompletion, ProgramEngine, ProgramPackage, SavedFunctionReference,
};
use crate::execution::{ExecutionObserver, ExecutionStop, NoopExecutionObserver};
use crate::invocation::{InvocationObserver, NoopInvocationObserver, UpstreamSender};
use crate::{
    Error, FinalValue, ProgramLogLevel, ProgramSource, ProgramValue, RunEvent, RunEventKind,
    RunFailureCode, RunModule, RunObservation, RunState, RunSubmission, RunTerminalOutcome,
    RunTimeout,
};
use coral_functions::{
    Error as FunctionsError, SavedFunctionChangeContext, SavedFunctionDescription,
    SavedFunctionModule, SavedFunctionPath, SavedFunctionSave, SavedFunctionSelection,
    SavedFunctionSource,
};
use support::*;

coral_test_support::provider_setup_fixtures!(coral_provider);
