//! Private Provider Generation and Materialized Operation Catalog semantics.
//!
//! This Module owns OpenAPI and credential-free normalized MCP materialization,
//! immutable Provider Generation packages, Operation availability, typed HTTP
//! and MCP Execution Binding facts, exact Operation Key and relative-path
//! resolution, and Provider-scoped catalog persistence independent of any
//! installation Workspace. OpenAPI and MCP are private materialization profiles
//! behind [`materialize`], and callers inspect their recipes through one
//! [`ExecutionBinding`] enum.
//!
//! Provider Publication materializes through this Interface outside a write
//! transaction, then joins the narrow transaction-compatible publication facet
//! to its own terminal transaction.

mod binding;
mod builtin;
pub use builtin::{
    ArgumentProblem, BUILTIN_PROVIDER_DISPLAY_NAME, BuiltinArguments, BuiltinCatalog,
    BuiltinCatalogIdentity, BuiltinCredentialRoute, BuiltinExecutionBinding, BuiltinOperation,
    ConfigurationChoices, CreateProviderArguments, CreateProviderResult, ListProvidersResult,
    ListedProvider, ListedProviderState, ListedSetup, ListedSetupState, ProviderFacts,
    ProviderSourceArgument,
};
mod configuration;
pub use configuration::{
    ConfigurationFieldError, CredentialField, DISPLAY_NAME_MAX_BYTES, ProviderConfiguration,
    ProviderConfigurationReview, ServerGroup, ServerOption, ServerSelection, ServerVariable,
    normalize_display_name, normalize_request_headers,
};
mod declaration_path;
mod error;
mod inspection;
mod mcp;
mod openapi;
mod paths;
/// Reads one retained Generation's normalized, secret-free credential contract.
/// # Errors
/// Returns a persistence or corrupt-evidence error.
pub async fn credential_contract_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<ProviderCredentialContract, Error> {
    repository::load_credential_contract(transaction, generation_id).await
}

/// Reads how one retained Generation presents its Credential Fields; empty when
/// it has no confirmed configuration.
/// # Errors
/// Returns a persistence or corrupt-evidence error.
pub async fn credential_fields_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<std::collections::BTreeMap<String, CredentialField>, Error> {
    Ok(repository::load_configuration(transaction, generation_id)
        .await?
        .map(|configuration| configuration.credential_fields)
        .unwrap_or_default())
}

/// Returns the exact pinned input for an owner-authorized settings-only Update.
/// # Errors
/// Returns missing, corrupt, or persistence errors; authority belongs to Provider.
pub async fn setup_input_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<ProviderSetupInput, Error> {
    repository::load_setup_input(transaction, generation_id)
        .await?
        .ok_or_else(no_setup_input)
}

/// Reads only the source kind for an owner-authorized Provider inspection.
/// # Errors
/// Returns missing, corrupt, or persistence errors; authority belongs to Provider.
pub async fn setup_input_kind_in_transaction(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<ProviderSetupInputKind, Error> {
    repository::load_setup_input_kind(transaction, generation_id)
        .await?
        .ok_or_else(no_setup_input)
}

/// The Built-in Provider's Generation pins Coral's own catalog, not a Setup input.
fn no_setup_input() -> Error {
    Error::InvalidInput("the Built-in Provider has no Setup input".to_string())
}

mod credentials;
pub use credentials::{
    AuthenticationConfiguration, AuthenticationKind, CredentialSource, HeaderBinding,
    OAuthClientAuthenticationMethod, OAuthClientBootstrap, OAuthClientPolicy, OAuthPkcePolicy,
    OAuthScopeFormat, ProviderCredentialContract, oauth_endpoint_is_valid,
    oauth_parameter_is_reserved, validate_oauth_client_id,
};
pub mod history_repository;
mod repository;
mod schema;
mod search;
mod types;

use coral_db::Transaction;

#[cfg(test)]
use coral_v8 as _;
// The dev-dependency on this crate exists for the integration tests' feature.
#[cfg(test)]
use coral_catalog as _;

pub use binding::ExecutionBinding;
pub use declaration_path::{DECLARATION_PATH_MAX_BYTES, DeclarationPath};
pub use error::Error;
pub use inspection::OperationSummary;
pub use mcp::{
    MCP_DISCOVERY_SNAPSHOT_MAX_BYTES, McpDiscoverySnapshot, McpExecutionBinding,
    McpInvocationMethod, McpTaskSupport, McpToolAnnotations, McpTransport,
};
pub use openapi::{
    HTTP_RESULT, HttpContentBinding, HttpCredentialSlot, HttpEncodingBinding, HttpExecutionBinding,
    HttpHeaderBinding, HttpMethod, HttpParameterBinding, HttpParameterLocation, HttpParameterStyle,
    HttpRedirectPolicy, HttpRequestBodyBinding, HttpResponseBinding, HttpResponseStatus,
    OPENAPI_DOCUMENT_MAX_BYTES, OpenApiDocument, SuccessSelection, validate_openapi_configuration,
};
pub use paths::{
    OperationPathAssignment, call_path_segment_is_reserved, provider_namespace_is_valid,
};
pub use search::{DeclarationIndexEntry, PreparedDeclarationFields, StoredDeclarationFields};
pub use types::{
    CatalogDiagnostic, CatalogDiagnosticSeverity, CatalogSearchTarget, MaterializationOutcome,
    MaterializedOperation, OPERATION_AUTHORING_DESCRIPTION_MAX_BYTES, OperationAvailability,
    OperationHints, OperationKey, OperationPresentation, PreparedProviderCatalog,
    ProviderGenerationFingerprint, ProviderGenerationId, ProviderSetupInput,
    ProviderSetupInputDigest, ProviderSetupInputKind, SchemaGraph, SchemaId, SchemaSemantics,
    SearchableOperation,
};

/// Materializes one pinned Provider Setup input with the deployment-selected policy.
///
/// Active Operation-relative paths are carried forward only for Operation Keys
/// that remain present in the replacement catalog.
#[must_use]
pub fn materialize(
    policy: coral_outbound::DestinationPolicy,
    setup_input: &ProviderSetupInput,
    previous_paths: &[OperationPathAssignment],
) -> MaterializationOutcome {
    let outcome = match setup_input {
        ProviderSetupInput::OpenApi(document) => openapi::materialize(document, previous_paths),
        ProviderSetupInput::Mcp(snapshot) => mcp::materialize(snapshot, previous_paths),
    };
    if let MaterializationOutcome::Ready(catalog) = &outcome {
        let destinations = catalog.operations().iter().filter_map(|operation| {
            match operation.execution_binding() {
                ExecutionBinding::Http(binding) => binding.server_url(),
                ExecutionBinding::Mcp(binding) => Some(binding.server_url()),
                ExecutionBinding::Builtin(_) => None,
            }
        });
        if let Err(diagnostic) =
            validate_provider_destinations(policy, destinations, &catalog.credential_contract())
        {
            return MaterializationOutcome::Rejected(diagnostic);
        }
    }
    outcome
}

/// Checks Coral-requested Provider destinations under the current server-egress policy.
/// Cached previews and final materialization share this rule and its safe diagnostic.
/// DNS validation belongs to outbound dispatch. Browser consent is screened separately
/// by Provider authorization at issuance, not by this materialization check.
/// # Errors
/// Returns a structural diagnostic without echoing a rejected address.
pub fn validate_provider_destinations<'a>(
    policy: coral_outbound::DestinationPolicy,
    destinations: impl IntoIterator<Item = &'a str>,
    contract: &'a ProviderCredentialContract,
) -> Result<(), CatalogDiagnostic> {
    let oauth = match contract.authentication() {
        // Authorization URLs belong to the browser, not server egress. The
        // Provider Module screens each initial consent navigation when issued.
        AuthenticationConfiguration::OAuthAuthorizationCode {
            token_url, issuer, ..
        } => [Some(token_url.as_str()), issuer.as_deref()],
        AuthenticationConfiguration::OAuthDeviceCode {
            device_authorization_url,
            token_url,
            ..
        } => [
            Some(device_authorization_url.as_str()),
            Some(token_url.as_str()),
        ],
        _ => [None, None],
    };
    for address in destinations.into_iter().chain(oauth.into_iter().flatten()) {
        if !url::Url::parse(address).is_ok_and(|url| policy.validate(&url).is_ok()) {
            return Err(CatalogDiagnostic::stable(
                "outbound_destination_rejected",
                "Provider destination is not permitted",
            ));
        }
    }
    Ok(())
}

/// Identifies the algorithm used to derive catalog and Setup preview facts.
#[must_use]
pub const fn materializer_version(kind: ProviderSetupInputKind) -> &'static str {
    match kind {
        ProviderSetupInputKind::OpenApi => openapi::MATERIALIZER_VERSION,
        ProviderSetupInputKind::Mcp => mcp::MATERIALIZER_VERSION,
    }
}

/// Publishes one completely prepared immutable Generation package.
///
/// The caller owns the transaction and must commit the Provider's active
/// Generation selection atomically with this facet.
///
/// # Errors
///
/// Returns a persistence failure; dropping the transaction removes every
/// Generation and Operation row written by this call.
pub async fn publish_generation_in_transaction(
    transaction: &mut Transaction<'_>,
    setup_input: &ProviderSetupInput,
    input_digest: &ProviderSetupInputDigest,
    prepared: PreparedProviderCatalog,
    created_at_unix_nanos: i64,
) -> Result<ProviderGenerationId, Error> {
    repository::publish_generation(
        transaction,
        repository::GenerationInput::Setup(setup_input),
        input_digest,
        prepared,
        materializer_version(setup_input.kind()),
        created_at_unix_nanos,
    )
    .await
}

/// Publishes one Workspace's own Generation of `catalog`. The caller owns
/// the transaction and commits the Provider that selects it atomically with
/// this facet.
///
/// # Errors
///
/// Returns a persistence failure.
pub async fn publish_builtin_generation_in_transaction(
    transaction: &mut Transaction<'_>,
    catalog: &BuiltinCatalog,
    created_at_unix_nanos: i64,
) -> Result<ProviderGenerationId, Error> {
    let identity = catalog.identity();
    repository::publish_generation(
        transaction,
        repository::GenerationInput::Builtin(identity),
        &ProviderSetupInputDigest::of_bytes(identity.as_str().as_bytes()),
        catalog.prepared(),
        builtin::MATERIALIZER_VERSION,
        created_at_unix_nanos,
    )
    .await
}

/// Reports whether a Workspace's Built-in Generation pins `catalog`, by the
/// catalog identity it pinned when Coral published it.
///
/// # Errors
///
/// Returns a persistence failure, or corrupt data for a Generation that pins
/// no well-formed catalog identity.
pub async fn builtin_generation_pins_in_transaction(
    transaction: &mut Transaction<'_>,
    catalog: &BuiltinCatalog,
    generation_id: &ProviderGenerationId,
) -> Result<bool, Error> {
    Ok(&repository::load_builtin_identity(transaction, generation_id).await? == catalog.identity())
}

/// Reads confirmed secret-free OpenAPI configuration for Update review.
/// # Errors
/// Returns persistence or corrupt-data errors. The caller owns Provider authority.
pub async fn configuration_in_transaction(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<Option<ProviderConfiguration>, Error> {
    repository::load_configuration(transaction, generation_id).await
}

/// Computes opaque canonical equality evidence for one persisted Generation.
///
/// Pinned Setup input and materializer provenance are intentionally excluded. The
/// Provider Module reads this before replacement materialization, so its final
/// activation transaction can remain short.
///
/// # Errors
///
/// Returns a persistence or corrupt-package failure.
pub async fn generation_fingerprint_in_transaction(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<ProviderGenerationFingerprint, Error> {
    repository::generation_fingerprint(transaction, generation_id).await
}

/// Loads active Operation-relative path claims for regeneration carry-forward.
///
/// # Errors
///
/// Returns a persistence or corrupt-package failure.
pub async fn operation_paths_in_transaction(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<Vec<OperationPathAssignment>, Error> {
    let operations = repository::load_catalog_operations(transaction, generation_id).await?;
    Ok(OperationPathAssignment::for_operations(&operations))
}

/// Verifies one active private Generation and complete Catalog package.
///
/// # Errors
///
/// Returns a persistence or corrupt-package failure.
pub async fn verify_generation_in_transaction(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<(), Error> {
    repository::verify_generation(transaction, generation_id).await
}

/// Loads the authoritative schema graph for one retained private Generation.
///
/// # Errors
///
/// Returns a persistence or corrupt-package failure.
pub async fn schema_graph_in_transaction(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<SchemaGraph, Error> {
    repository::load_schema_graph(transaction, generation_id).await
}

/// Loads only structurally available Operation identities needed before ranking.
///
/// This facet deliberately omits presentation metadata, TypeScript declarations,
/// schemas, and Execution Bindings so complete descriptions remain lazy.
///
/// # Errors
///
/// Returns a persistence or minimal-index corruption failure.
pub async fn available_search_targets_in_transaction(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<Vec<CatalogSearchTarget>, Error> {
    repository::available_search_targets(transaction, generation_id).await
}

/// Loads what the search index reads of each structurally available
/// Operation, in Operation Key order: presentation, binding and input schema,
/// without separately materialized schemas, declarations or diagnostics.
///
/// # Errors
///
/// Returns a persistence or corrupt-package failure.
pub async fn searchable_operations_in_transaction(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<Vec<SearchableOperation>, Error> {
    repository::searchable_operations(transaction, generation_id).await
}

/// Whether the exact private Generation still holds the Operation, by one
/// existence read of its row: a Generation's package is immutable, so a row
/// that exists is the one admission retained.
///
/// # Errors
///
/// Returns a persistence failure.
pub async fn operation_exists_in_transaction(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
    operation_key: &OperationKey,
) -> Result<bool, Error> {
    repository::operation_exists(transaction, generation_id, operation_key).await
}

/// Loads one Operation from an exact private Generation package.
///
/// # Errors
///
/// Returns a persistence or corrupt-package failure. `Ok(None)` means the
/// Generation has no matching Operation Key.
pub async fn operation_in_transaction(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
    operation_key: &OperationKey,
) -> Result<Option<MaterializedOperation>, Error> {
    repository::load_operation(transaction, generation_id, operation_key).await
}

/// Loads an Operation's complete self-contained declaration: its own
/// declaration and every shared type of the Generation that it reaches.
///
/// # Errors
///
/// Returns a persistence or corrupt-package failure.
pub async fn authoring_declaration_in_transaction(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
    operation: &MaterializedOperation,
) -> Result<String, Error> {
    let mut closure = schema::TypeClosure::new(operation.type_references());
    loop {
        let names = closure.pending();
        if names.is_empty() {
            break;
        }
        let found = repository::load_shared_types(transaction, generation_id, &names).await?;
        closure.reach(&names, found)?;
    }
    Ok(closure.compose(operation.typescript_declaration()))
}

/// Reads hints and the HTTP method for 1–100 exact keys without loading
/// executable definitions or schemas. The method is `None` for other protocols.
/// Absent keys are omitted; structurally unavailable Operations remain included.
/// # Errors
/// Returns invalid-size, persistence or corrupt-generation failures.
pub async fn operation_hints_and_methods_in_transaction(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
    keys: &[OperationKey],
) -> Result<std::collections::BTreeMap<OperationKey, (OperationHints, Option<HttpMethod>)>, Error> {
    if !(1..=100).contains(&keys.len()) {
        return Err(Error::InvalidInput(
            "invalid Operation hints batch size".into(),
        ));
    }
    repository::load_operation_hints_and_methods(transaction, generation_id, keys).await
}

/// Reads one summary page in Operation Key order, plus at most one lookahead row.
///
/// The caller owns authority and continuation encoding. `after` is exclusive;
/// `page_size` must be 1..=100. Definitions, schemas, and declarations stay lazy.
///
/// # Errors
/// Returns an invalid size, persistence, or summary corruption failure.
pub async fn operation_summaries_in_transaction(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
    after: Option<&OperationKey>,
    page_size: u32,
) -> Result<Vec<OperationSummary>, Error> {
    if !(1..=100).contains(&page_size) {
        return Err(Error::InvalidInput("invalid catalog page size".into()));
    }
    repository::load_operation_summaries(transaction, generation_id, after, page_size).await
}

/// Loads the bounded current catalog for authorized administrative inspection.
///
/// Unavailable Operations remain visible with their structural diagnostics.
///
/// # Errors
/// Returns persistence or corrupt-package failures.
pub async fn operations_in_transaction(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
) -> Result<Vec<MaterializedOperation>, Error> {
    repository::load_catalog_operations(transaction, generation_id).await
}

/// Loads one Operation by its exact Generation-relative Call Path projection.
///
/// This transaction-compatible facet supports atomic Invocation admission;
/// callers still retain the returned Operation Key as the callable address.
///
/// # Errors
///
/// Returns a persistence or corrupt-package failure. `Ok(None)` means the
/// Generation has no matching active projection.
pub async fn operation_by_relative_path_in_transaction(
    transaction: &mut Transaction<'_>,
    generation_id: &ProviderGenerationId,
    relative_path: &str,
) -> Result<Option<MaterializedOperation>, Error> {
    repository::load_operation_by_relative_path(transaction, generation_id, relative_path).await
}
