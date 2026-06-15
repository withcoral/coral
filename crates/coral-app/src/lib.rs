//! Internal local server composition for the Coral app.
//!
//! `coral-app` is the local server composition root for Coral. It wires
//! together the generated transport API from `coral-api` with the
//! management-plane stores and the data plane in `coral-engine`.
//!
//! This crate is primarily an internal workspace boundary. Sibling crates such
//! as `coral-client` use its bootstrap seam, but end-user code should normally
//! enter through `coral-client`, not through `coral-app` directly.
//!
//! # Main Internal Areas
//!
//! - [`ServerBuilder`] starts the local application server with filesystem
//!   config, managed source resources, and plaintext credential material storage.
//! - [`RunningServer`] owns the running local gRPC server task.
//! - [`AppError`] is the transport-neutral application error type used during
//!   bootstrap and management operations.
//! - `sources/` owns managed-source lifecycle and the reviewable installed
//!   source contract.
//! - `state/` owns persisted config-dir layout and config storage.
//! - `credentials/` owns credential-set identity and credential material
//!   persistence.
//! - `query/` owns query-time source loading and `coral-engine`
//!   orchestration.
//! - `catalog/` owns workspace-scoped discovery semantics over query-visible
//!   table metadata.
//!
//! # Crate Relationships
//!
//! - `coral-api` defines the generated gRPC surface.
//! - `coral-spec` owns declarative source-spec parsing, validation, and input
//!   discovery.
//! - `coral-engine` owns the data plane: backend registration, `DataFusion`
//!   runtime assembly, and `SQL` execution over validated specs.
//!
mod authorization;
/// Bootstrap entrypoints and local server assembly.
pub mod bootstrap;
mod catalog;
mod credentials;
mod episode;
pub mod features;
mod feedback;
mod identities;
mod identity;
mod identity_specs;
mod query;
mod sources;
mod state;
mod storage;
pub mod telemetry;
mod transport;
mod workspaces;

pub use authorization::{
    AllowAllManagementAuthorizer, AuthorizationError, ManagementAuthorizer, SourceMutationKind,
};
pub use bootstrap::{
    AppError, RunningServer, ServerBuilder, ServerMode, StaticAsset, StaticAssetsProvider,
};
pub use coral_engine::{EngineExtensions, QuerySource};
pub use credentials::oauth::{OAuthProgressEvent, OAuthProgressEventSender};
pub use identities::{
    CreateFixedTokenIdentityCommand, CreateOAuthIdentityCommand, IdentityCredentialInput,
    IdentityManagementHandle, IdentityOwnerKey, UserOwnedIdentityMaterialGuard,
    UserOwnedIdentityRecord, UserOwnedIdentityStore,
};
pub use identity::SingleUserPrincipalProvider;
pub use identity::{
    RuntimeSourceIdentity, SourceIdentityBinding, SourceIdentityOwner, SourceIdentityProvider,
    SourceIdentityResolutionRequest, SourceIdentitySelection, SourceIdentitySelectionRequest,
    SourceIdentitySubject, UserPrincipal, UserPrincipalError, UserPrincipalProvider,
};
pub use identity_specs::{
    IdentitySpecManifestMetadata, IdentitySpecRegistry, IdentitySpecRegistryRecord,
    IdentitySpecUsageProvider, identity_spec_input_material_from_manifest,
    identity_spec_input_material_from_manifest_with_existing, identity_spec_manifest_metadata,
};
pub use query::extensions::{
    AwsEngineExtensionsProvider, EngineExtensionsProvider, NoopEngineExtensionsProvider,
};
pub use telemetry::{RunContext, RunErrorTelemetry, run_with_context, shutdown_tracing};
pub use transport::{OAuthProgressProto, oauth_operation_response_stream};
pub use workspaces::DEFAULT_WORKSPACE_ID;
