//! Provider-owned input acquisition and atomic Setup/Update preparation.
//! Network inspection runs between authority checks, never inside a write transaction.
use coral_catalog::{McpDiscoverySnapshot, ProviderCredentialContract, ProviderSetupInput};
use coral_workspace::ChangeContext;
use coral_workspace::WorkspaceAction;
use coral_workspace::WorkspaceId;

use crate::setup_credentials::{InspectedAuthorization, StoredRequirement};
use crate::{
    Error, OpenApiDocument, ProviderId, ProviderModule, ProviderSetup, SetupRequest, repository,
};

const SOURCE_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// One explicit input source. Acquisition never accepts account credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderSetupSource {
    /// The Update target's exact current input; no catalog discovery or network call.
    Current,
    /// An exact uploaded OpenAPI document.
    OpenApiDocument(OpenApiDocument),
    /// An exact normalized MCP discovery snapshot.
    McpSnapshot(McpDiscoverySnapshot),
    /// One OpenAPI document fetched from this URL.
    OpenApiUrl(String),
    /// One Streamable HTTP MCP server inspected without credentials.
    McpUrl {
        /// Server endpoint.
        url: String,
        /// Non-secret headers needed for initial catalog discovery.
        request_headers: std::collections::BTreeMap<String, String>,
    },
}

/// What a new Setup is for.
#[derive(Debug, Clone, Copy)]
pub(crate) enum SetupPurpose<'a> {
    /// A new Provider, with what a Program asked for, if a Program prepared it.
    Create(Option<&'a SetupRequest>),
    /// A new Generation of this Provider.
    Update(&'a ProviderId),
}

impl<'a> SetupPurpose<'a> {
    pub(crate) const fn update_target(self) -> Option<&'a ProviderId> {
        match self {
            Self::Create(_) => None,
            Self::Update(target) => Some(target),
        }
    }

    pub(crate) const fn request(self) -> Option<&'a SetupRequest> {
        match self {
            Self::Create(request) => request,
            Self::Update(_) => None,
        }
    }
}

impl ProviderModule {
    /// Acquires an input and prepares a Setup for the Provider a Program asked
    /// for, recording `request` on it so that the person who finishes it, and
    /// the Program, can read it back.
    /// # Errors
    /// As [`Self::prepare_setup`] for a new Provider.
    pub(crate) async fn prepare_requested_setup(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        source: ProviderSetupSource,
        request: SetupRequest,
    ) -> Result<ProviderSetup, Error> {
        self.prepare(context, owner, source, SetupPurpose::Create(Some(&request)))
            .await
    }

    /// Acquires an input and prepares its Setup, optionally for an exact Update target.
    /// Target identity and authority are checked before acquisition and again when
    /// the Setup, saved configuration and lifecycle evidence are committed together.
    /// # Errors
    /// Rejects missing/out-of-scope targets, unsafe or unavailable input and authority
    /// lost during acquisition. Failure creates no Setup or Setup lifecycle event.
    pub async fn prepare_setup(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        source: ProviderSetupSource,
        update_target: Option<&ProviderId>,
    ) -> Result<ProviderSetup, Error> {
        let purpose = match update_target {
            Some(target) => SetupPurpose::Update(target),
            None => SetupPurpose::Create(None),
        };
        self.prepare(context, owner, source, purpose).await
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one acquisition sequence keeps preflight, protocol inspection and atomic persistence together"
    )]
    async fn prepare(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        source: ProviderSetupSource,
        purpose: SetupPurpose<'_>,
    ) -> Result<ProviderSetup, Error> {
        let update_target = purpose.update_target();
        let mut tx = self.database.begin_read_snapshot().await?;
        self.workspaces
            .check_in_transaction(&mut tx, context.actor(), owner, WorkspaceAction::Configure)
            .await?;
        if let Some(target) = update_target {
            crate::change::load_for_change(
                &mut tx,
                owner,
                target,
                crate::change::ProviderChange::Reconfigure,
            )
            .await?;
        }
        let (current_input, current_url) = if source == ProviderSetupSource::Current {
            let target = update_target.ok_or(Error::ProviderSetupNotApplicable)?;
            let current = repository::provider_in_transaction(&mut tx, owner, target)
                .await?
                .ok_or(Error::ProviderNotFound)?;
            (
                Some(
                    coral_catalog::setup_input_in_transaction(&mut tx, &current.generation_id)
                        .await?,
                ),
                current.source_url,
            )
        } else {
            (None, None)
        };
        tx.rollback().await?;
        let mut initial_headers = match &source {
            ProviderSetupSource::McpUrl {
                request_headers, ..
            } => request_headers.clone(),
            _ => std::collections::BTreeMap::new(),
        };
        coral_catalog::normalize_request_headers(&mut initial_headers, None, true)
            .map_err(|_invalid| Error::AccountInput)?;
        match source {
            ProviderSetupSource::Current => {
                // Authentication edits can discard a protected MCP snapshot. Keep
                // its source and discovered requirement independently so the same
                // Setup can authorize and rediscover without another input fetch.
                let (input_url, metadata) = match &current_input {
                    Some(ProviderSetupInput::Mcp(snapshot)) => {
                        let (url, contract) = snapshot
                            .connection()
                            .map_err(|_invalid| Error::ProviderSetupNotApplicable)?;
                        (
                            current_url.or(Some(url)),
                            Some(InspectedAuthorization {
                                requirement: StoredRequirement::from_contract(&contract),
                                failure_code: None,
                            }),
                        )
                    }
                    _ => (current_url, None),
                };
                self.create_setup(
                    context,
                    owner,
                    current_input,
                    &initial_headers,
                    metadata,
                    input_url,
                    purpose,
                )
                .await
            }
            ProviderSetupSource::OpenApiDocument(document) => {
                self.create_setup(
                    context,
                    owner,
                    Some(document.into()),
                    &initial_headers,
                    None,
                    None,
                    purpose,
                )
                .await
            }
            ProviderSetupSource::McpSnapshot(snapshot) => {
                self.create_setup(
                    context,
                    owner,
                    Some(snapshot.into()),
                    &initial_headers,
                    None,
                    None,
                    purpose,
                )
                .await
            }
            ProviderSetupSource::OpenApiUrl(url) => {
                let document =
                    fetch_openapi(self.outbound_policy, validate_input_url(&url)?).await?;
                self.create_setup(
                    context,
                    owner,
                    Some(document.into()),
                    &initial_headers,
                    None,
                    Some(url),
                    purpose,
                )
                .await
            }
            ProviderSetupSource::McpUrl {
                url,
                request_headers: _,
            } => {
                let validated = validate_input_url(&url)?;
                let snapshot = match coral_upstream::discover_mcp(
                    self.outbound_policy,
                    validated.as_str(),
                    None,
                    &ProviderCredentialContract::NONE,
                    &initial_headers,
                )
                .await
                {
                    Ok(snapshot) => snapshot,
                    Err(coral_upstream::Error::McpUnauthorized(challenge)) => {
                        let challenge = challenge.unwrap_or_else(|| {
                            coral_upstream::BearerChallenge::from_parts(None, Vec::new())
                        });
                        let requirement = match coral_upstream::discover_authorization(self.outbound_policy, validated.as_str(), &challenge, &initial_headers).await {
                            Ok(requirement) => Some(StoredRequirement::from_discovery(&requirement)),
                            Err(coral_upstream::AuthorizationDiscoveryError::ResourceMetadataMissing) => None,
                            Err(error) => return Err(Error::AuthorizationDiscovery(error.code())),
                        };
                        return self
                            .create_setup(
                                context,
                                owner,
                                None,
                                &initial_headers,
                                Some(InspectedAuthorization {
                                    requirement,
                                    failure_code: None,
                                }),
                                Some(url),
                                purpose,
                            )
                            .await;
                    }
                    Err(coral_upstream::Error::McpAuthenticationUnsupported) => {
                        return Err(Error::AuthorizationDiscovery("challenge_unsupported"));
                    }
                    Err(coral_upstream::Error::DestinationRejected) => {
                        return Err(Error::OutboundDestinationRejected);
                    }
                    Err(_discovery) => return Err(Error::InputUnavailable),
                };
                // Anonymous catalog access does not imply absent OAuth metadata for calls.
                let hint = coral_upstream::BearerChallenge::from_parts(None, Vec::new());
                let metadata = match coral_upstream::discover_authorization(
                    self.outbound_policy,
                    validated.as_str(),
                    &hint,
                    &initial_headers,
                )
                .await
                {
                    Ok(requirement) => Some(InspectedAuthorization {
                        requirement: Some(StoredRequirement::from_discovery(&requirement)),
                        failure_code: None,
                    }),
                    Err(coral_upstream::AuthorizationDiscoveryError::ResourceMetadataMissing) => {
                        None
                    }
                    Err(error) => Some(InspectedAuthorization {
                        requirement: None,
                        failure_code: Some(error.code()),
                    }),
                };
                self.create_setup(
                    context,
                    owner,
                    Some(ProviderSetupInput::Mcp(snapshot)),
                    &initial_headers,
                    metadata,
                    Some(url),
                    purpose,
                )
                .await
            }
        }
    }
}

/// Snapshot inputs already carry their source. Invalid snapshots still receive
/// their structural diagnostic through the normal preparation/publication path.
pub(super) fn snapshot_source_url(input: Option<&ProviderSetupInput>) -> Option<String> {
    match input {
        Some(ProviderSetupInput::Mcp(snapshot)) => {
            snapshot.connection().ok().map(|(url, _contract)| url)
        }
        _ => None,
    }
}

/// Checks input shape. The shared policy owns transport and destination access.
/// A query can select the document or server, as Tailscale's
/// `?outputOpenapiSchema=true` does, so it is kept.
fn validate_input_url(value: &str) -> Result<url::Url, Error> {
    let parsed = url::Url::parse(value).map_err(|_invalid| Error::AccountInput)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        return Err(Error::AccountInput);
    }
    Ok(parsed)
}

async fn fetch_openapi(
    policy: coral_outbound::DestinationPolicy,
    url: url::Url,
) -> Result<OpenApiDocument, Error> {
    let client = coral_outbound::Client::new(policy, SOURCE_FETCH_TIMEOUT)
        .map_err(|_client| Error::InputUnavailable)?;
    let mut response = client
        .get(url.as_str())
        .map_err(|_error| Error::OutboundDestinationRejected)?
        .send()
        .await
        .map_err(Error::source_fetch)?;
    if !response.status().is_success() {
        return Err(Error::InputUnavailable);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_body| Error::InputUnavailable)?
    {
        if bytes.len().saturating_add(chunk.len()) > crate::OPENAPI_DOCUMENT_MAX_BYTES {
            return Err(coral_catalog::Error::OpenApiDocumentTooLarge.into());
        }
        bytes.extend_from_slice(&chunk);
    }
    OpenApiDocument::from_bytes(bytes).map_err(Error::from)
}
