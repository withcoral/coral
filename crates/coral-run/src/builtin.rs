//! Built-in Provider Operations, which Coral performs itself with no upstream
//! dispatch. Their input is validated before any other work, and their results
//! never carry a secret or an OAuth URL.

use std::collections::BTreeMap;

use async_trait::async_trait;
use coral_catalog::{
    AuthenticationConfiguration, BuiltinOperation, CredentialField, ExecutionBinding,
    ProviderConfiguration, ServerSelection,
};
use coral_provider::access::{CredentialRoute, ProviderCallRoot, ProviderNamespace};
use coral_provider::{
    ManagementRequestId, OpenApiDocument, ProviderModule, ProviderSetupId, ProviderSetupSource,
    ProviderState, PublishabilityRejection, PublishedProvider, ResolvedInvocationOperation,
    SetupPublishability, WorkspaceProviderPublicationRequest, WorkspaceProviderPublisher,
};
use coral_workspace::{ChangeContext, Principal, WorkspaceId};
use serde::Deserialize;

use crate::error::InputProblem;
use crate::{CoralFailure, Error, HumanInterfaceUrl, Rejection};

mod input;

/// One admitted Built-in Operation with its validated input.
#[derive(Clone)]
pub(crate) enum BuiltinCall {
    CreateProvider(Box<CreateProvider>),
    ListProviders,
}

impl BuiltinCall {
    /// The Built-in Operation an admitted target names, with its input checked
    /// before any other work; `None` for an Operation sent upstream.
    pub(crate) fn parse(
        target: &ResolvedInvocationOperation,
        arguments: &serde_json::Value,
    ) -> Option<Result<Self, Rejection>> {
        match target.operation().execution_binding() {
            ExecutionBinding::Builtin(binding) => Some(match binding.operation() {
                Some(BuiltinOperation::CreateProvider) => CreateProvider::parse(arguments)
                    .map(|input| Self::CreateProvider(Box::new(input))),
                Some(BuiltinOperation::ListProviders) => {
                    no_arguments(arguments).map(|()| Self::ListProviders)
                }
                // A newer Coral published an Operation this version does not perform.
                None => Err(CoralFailure::OperationUnavailable.into()),
            }),
            ExecutionBinding::Http(_) | ExecutionBinding::Mcp(_) => None,
        }
    }

    /// Performs the Operation and returns its Operation Result.
    pub(crate) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        match self {
            Self::CreateProvider(input) => input.perform(host).await,
            Self::ListProviders => list_providers(host).await,
        }
    }
}

/// The Run that invoked a Built-in Operation: whether it still accepts the
/// result, when it is asked to stop, and its clock.
#[async_trait]
pub(crate) trait InvokingRun: Send + Sync {
    async fn still_current(&self) -> Result<bool, Error>;

    /// Completes when the Run is asked to stop.
    async fn stopped(&self);

    /// The current time on the Run's clock.
    fn now(&self) -> Result<i64, Error>;

    /// Lets a test end the Run once the call's Setup exists.
    #[cfg(test)]
    fn setup_prepared(&self) {}
}

/// What a Built-in Operation acts through: the invoking User in the Run's
/// Workspace, the workflow Reef publishes with, and the Reef origin its
/// results link to.
pub(crate) struct BuiltinHost<'a> {
    pub(crate) providers: &'a ProviderModule,
    pub(crate) publisher: Option<&'a dyn WorkspaceProviderPublisher>,
    pub(crate) context: &'a ChangeContext,
    pub(crate) principal: &'a Principal,
    pub(crate) workspace_id: &'a WorkspaceId,
    pub(crate) human_interface: &'a HumanInterfaceUrl,
    /// The Invocation's identity, which makes a repeated Publication an exact replay.
    pub(crate) request_id: &'a ManagementRequestId,
    pub(crate) run: &'a dyn InvokingRun,
}

/// Validated `createProvider` input: the Provider's whole configuration, with
/// each omitted field left to the source's suggestion.
#[derive(Clone)]
pub(crate) struct CreateProvider {
    namespace: ProviderNamespace,
    source: ProviderSetupSource,
    route: CredentialRoute,
    choices: ConfigurationChoices,
}

/// The configuration fields the Program chose, converted from its input.
#[derive(Clone)]
struct ConfigurationChoices {
    display_name: Option<String>,
    servers: Option<BTreeMap<String, ServerSelection>>,
    authentication: Option<AuthenticationConfiguration>,
    request_headers: Option<BTreeMap<String, String>>,
    credential_fields: Option<BTreeMap<String, CredentialField>>,
}

/// Who publishes a configured Setup.
enum Publisher {
    /// Nobody needs to sign in, so Coral publishes it at once.
    Coral,
    /// A person signs in or enters a credential, and Reef publishes it.
    Person,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CreateProviderArguments {
    namespace: String,
    source: SourceArguments,
    credential_route: Option<RouteArgument>,
    display_name: Option<String>,
    servers: Option<BTreeMap<String, input::ServerInput>>,
    authentication: Option<input::AuthenticationInput>,
    request_headers: Option<BTreeMap<String, String>>,
    credential_fields: Option<BTreeMap<String, input::CredentialFieldInput>>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum SourceArguments {
    OpenapiDocument { document: String },
    OpenapiUrl { url: String },
    McpUrl { url: String },
}

impl SourceArguments {
    /// The source to prepare. An MCP server is discovered with the same
    /// non-secret headers it is configured with.
    fn into_source(
        self,
        request_headers: Option<&BTreeMap<String, String>>,
    ) -> Result<ProviderSetupSource, InputProblem> {
        match self {
            Self::OpenapiDocument { document } => OpenApiDocument::parse(&document)
                .map(ProviderSetupSource::OpenApiDocument)
                .map_err(|_error| {
                    InputProblem::new(
                        "source.document",
                        "Coral cannot read this as an OpenAPI document",
                    )
                }),
            Self::OpenapiUrl { url } => web_url(url).map(ProviderSetupSource::OpenApiUrl),
            Self::McpUrl { url } => web_url(url).map(|url| ProviderSetupSource::McpUrl {
                url,
                request_headers: request_headers.cloned().unwrap_or_default(),
            }),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum RouteArgument {
    None,
    Fixed,
    Caller,
}

impl CreateProvider {
    /// Validates the namespace syntax first, then the route and the source,
    /// without any network call. A rejection names every argument to correct.
    fn parse(arguments: &serde_json::Value) -> Result<Self, Rejection> {
        let arguments: CreateProviderArguments = serde_path_to_error::deserialize(arguments)
            .map_err(|error| {
                let field = error.path().to_string();
                let field = if field == "." { String::new() } else { field };
                Rejection::invalid_input(field, shape_reason(&error.into_inner().to_string()))
            })?;
        // Each check below stands alone, so the Program learns every problem at once.
        let mut problems = Vec::new();
        let namespace = ProviderNamespace::parse(&arguments.namespace)
            .map_err(|_error| {
                problems.push(InputProblem::new(
                    "namespace",
                    "use a JavaScript identifier of at most 128 bytes, such as `weather`",
                ));
            })
            .ok();
        // A credential-named header would carry a credential value through the
        // Program; a person enters every value in Reef. The authentication input
        // takes no literal value but an empty one.
        for name in arguments
            .request_headers
            .iter()
            .flat_map(BTreeMap::keys)
            .filter(|name| names_a_credential(name))
        {
            problems.push(InputProblem::new(
                format!("requestHeaders.{name}"),
                "this header names a credential; send it through `authentication` \
                 so a person enters its value when they sign in",
            ));
        }
        let authentication = arguments
            .authentication
            .map(AuthenticationConfiguration::try_from)
            .transpose()
            .map_err(|problem| problems.push(problem))
            .ok()
            .flatten();
        let route = match arguments.credential_route {
            Some(RouteArgument::None) => Some(CredentialRoute::None),
            Some(RouteArgument::Fixed) => Some(CredentialRoute::Fixed),
            Some(RouteArgument::Caller) => Some(CredentialRoute::Caller),
            None => {
                problems.push(InputProblem::new(
                    "credentialRoute",
                    "choose `none`, `fixed` or `caller`",
                ));
                None
            }
        };
        let source = arguments
            .source
            .into_source(arguments.request_headers.as_ref())
            .map_err(|problem| problems.push(problem))
            .ok();
        let (Some(namespace), Some(route), Some(source), true) =
            (namespace, route, source, problems.is_empty())
        else {
            return Err(Rejection::invalid_inputs(problems));
        };
        Ok(Self {
            namespace,
            source,
            route,
            choices: ConfigurationChoices {
                display_name: arguments.display_name,
                servers: arguments.servers.map(|servers| {
                    servers
                        .into_iter()
                        .map(|(declaration, server)| (declaration, server.into()))
                        .collect()
                }),
                authentication,
                request_headers: arguments.request_headers,
                credential_fields: arguments.credential_fields.map(|fields| {
                    fields
                        .into_iter()
                        .map(|(name, field)| (name, field.into()))
                        .collect()
                }),
            },
        })
    }

    /// Prepares and configures a Setup as the invoking User. Coral publishes it
    /// when nobody needs to sign in; otherwise the result is the Reef page where
    /// a person finishes it. The call never waits for that person.
    async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        // A taken namespace would fail the Publication; fail before any fetch or Setup.
        if host
            .providers
            .namespace_in_use(host.context.actor(), host.workspace_id, &self.namespace)
            .await
            .map_err(setup_error)?
        {
            return Err(Error::InvocationRejected(
                CoralFailure::ProviderNamespaceConflict.into(),
            ));
        }
        // Only the fetch waits on the network, so only it stops with the Run.
        // Once a Setup exists, the call runs to an end that publishes it,
        // links a person to it, or ends it.
        let setup = tokio::select! {
            biased;
            () = host.run.stopped() => return Err(Error::ProgramHostTerminated),
            setup = host.providers.prepare_setup(
                host.context,
                host.workspace_id,
                self.source.clone(),
                None,
            ) => setup.map_err(setup_error)?,
        };
        #[cfg(test)]
        host.run.setup_prepared();
        let publisher = match self.configure(host, setup.id()).await {
            Ok(publisher) => publisher,
            Err(error) => {
                // Nobody will finish a Setup whose configuration failed.
                end_setup(host, setup.id()).await;
                return Err(error);
            }
        };
        match publisher {
            Publisher::Person => {
                // A Run that ended could no longer hand anyone the link.
                require_current_run(host, setup.id()).await?;
                Ok(self.needs_sign_in(host, setup.id()))
            }
            Publisher::Coral => self.publish(host, setup.id()).await,
        }
    }

    /// Confirms the Program's configuration over the source's suggestions and
    /// decides who publishes the Setup. The route must suit the source.
    async fn configure(
        &self,
        host: &BuiltinHost<'_>,
        setup: &ProviderSetupId,
    ) -> Result<Publisher, Error> {
        let invalid = |field: &str, reason: &str| {
            Error::InvocationRejected(Rejection::invalid_input(field, reason))
        };
        let review = host
            .providers
            .review_setup_configuration(host.context.actor(), host.workspace_id, setup)
            .await
            .map_err(setup_error)?;
        let Some(document) = review.document else {
            return Err(Error::InvocationRejected(
                configuration_rejection(&review).unwrap_or_else(|| {
                    Rejection::invalid_input(
                        "source",
                        "Coral could not read a configuration from the source",
                    )
                }),
            ));
        };
        let suggested = document.suggested;
        let confirmed = host
            .providers
            .confirm_setup_configuration(
                host.context,
                host.workspace_id,
                setup,
                review.revision,
                self.configuration(suggested),
            )
            .await
            .map_err(setup_error)?;
        if let Some(rejection) = configuration_rejection(&confirmed) {
            return Err(Error::InvocationRejected(rejection));
        }
        match host
            .providers
            .setup_publishability(host.context.actor(), host.workspace_id, setup, self.route)
            .await
            .map_err(setup_error)?
        {
            SetupPublishability::Coral => Ok(Publisher::Coral),
            SetupPublishability::Person => Ok(Publisher::Person),
            SetupPublishability::Rejected(PublishabilityRejection::NotConfigured) => Err(invalid(
                "source",
                "Coral could not tell whether the source needs a credential",
            )),
            // The route must suit the source, as Reef offers it; no route falls back to another.
            SetupPublishability::Rejected(PublishabilityRejection::SourceNeedsCredential) => {
                Err(invalid(
                    "credentialRoute",
                    "the source needs a credential; use `fixed` or `caller`",
                ))
            }
            SetupPublishability::Rejected(PublishabilityRejection::SourceNeedsNoCredential) => {
                Err(invalid(
                    "credentialRoute",
                    "the source declares no credential; use `none`, or give `authentication` \
                     to say how a person signs in",
                ))
            }
        }
    }

    /// The configuration to confirm: each chosen field over the source's suggestion.
    fn configuration(&self, suggested: ProviderConfiguration) -> ProviderConfiguration {
        let choices = &self.choices;
        ProviderConfiguration {
            display_name: choices
                .display_name
                .clone()
                .unwrap_or(suggested.display_name),
            servers: choices.servers.clone().unwrap_or(suggested.servers),
            authentication: choices.authentication.clone().or(suggested.authentication),
            request_headers: choices
                .request_headers
                .clone()
                .unwrap_or(suggested.request_headers),
            credential_fields: choices
                .credential_fields
                .clone()
                .unwrap_or(suggested.credential_fields),
        }
    }

    /// The Reef page where a person finishes the Setup.
    fn needs_sign_in(&self, host: &BuiltinHost<'_>, setup: &ProviderSetupId) -> serde_json::Value {
        let url = host.human_interface.page_url(&setup_path(
            host.workspace_id,
            setup,
            &self.namespace,
            self.route,
        ));
        serde_json::json!({
            "data": {
                "status": "needs_sign_in",
                "url": url,
                "namespace": self.namespace.as_str(),
            },
            "metadata": { "kind": "builtin" },
        })
    }

    /// Publishes the Setup as the invoking User, through the workflow Reef
    /// publishes with, and returns what the Program needs to call it.
    async fn publish(
        &self,
        host: &BuiltinHost<'_>,
        setup: &ProviderSetupId,
    ) -> Result<serde_json::Value, Error> {
        let Some(publisher) = host.publisher else {
            end_setup(host, setup).await;
            return Err(Error::InvocationRejected(
                CoralFailure::OperationUnavailable.into(),
            ));
        };
        // A Run that ended could no longer return the Provider: publish nothing.
        // The Run can still end between this check and the Publication; the
        // Provider then exists and search finds it, though the call ended.
        require_current_run(host, setup).await?;
        let publication = match publisher
            .publish_workspace_provider(
                host.context,
                WorkspaceProviderPublicationRequest::new(
                    host.request_id.clone(),
                    host.workspace_id.clone(),
                    setup.clone(),
                    self.namespace.clone(),
                    self.route,
                ),
            )
            .await
        {
            Ok(publication) => publication,
            Err(error) => {
                // The Publication left the Setup reusable, but nobody will reuse it.
                end_setup(host, setup).await;
                return Err(publication_error(error));
            }
        };
        let Some(provider) = publication.published_provider() else {
            return Err(Error::InvocationRejected(
                if publication.failed_on_namespace_conflict() {
                    CoralFailure::ProviderNamespaceConflict
                } else {
                    CoralFailure::ProviderPublicationFailed
                }
                .into(),
            ));
        };
        let details = host
            .providers
            .published_provider(host.workspace_id, provider, host.principal)
            .await?;
        Ok(published_result(&details))
    }
}

/// Accepts only an empty arguments object. A Program that calls with no
/// argument sends one.
fn no_arguments(arguments: &serde_json::Value) -> Result<(), Rejection> {
    let Some(fields) = arguments.as_object() else {
        return Err(Rejection::invalid_input("", "pass an empty object"));
    };
    if fields.is_empty() {
        return Ok(());
    }
    Err(Rejection::invalid_inputs(
        fields
            .keys()
            .map(|field| InputProblem::new(field.clone(), "unknown field; pass an empty object"))
            .collect(),
    ))
}

/// Lists every Provider in the Workspace, with what each offers the invoking User.
async fn list_providers(host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
    let providers = host
        .providers
        .published_providers(host.context.actor(), host.workspace_id)
        .await
        .map_err(authority_error)?;
    Ok(serde_json::json!({
        "data": {
            "providers": providers.iter().map(listed_provider).collect::<Vec<_>>(),
        },
        "metadata": { "kind": "builtin" },
    }))
}

/// One `listProviders` entry. The Built-in Provider has no namespace: its
/// Operations are under `coral.admin`.
fn listed_provider(published: &PublishedProvider) -> serde_json::Value {
    let provider = &published.provider;
    let namespace = match provider.call_root() {
        ProviderCallRoot::Owner(namespace) => Some(namespace.as_str()),
        ProviderCallRoot::Builtin => None,
    };
    let state = match provider.state() {
        ProviderState::Active => "active",
        ProviderState::Disabled => "disabled",
    };
    let mut entry = provider_facts(published);
    entry.insert("namespace".to_owned(), serde_json::json!(namespace));
    entry.insert("state".to_owned(), state.into());
    serde_json::Value::Object(entry)
}

/// Whether a recorded Call Path is `coral.admin.createProvider`.
pub(crate) fn names_create_provider(call_path: &str) -> bool {
    call_path
        .strip_prefix(coral_provider::access::ProviderCallRoot::Builtin.as_str())
        .and_then(|relative| relative.strip_prefix('.'))
        == Some(BuiltinOperation::CreateProvider.key())
}

/// Settles a `createProvider` whose worker was lost after the dispatch fence,
/// from the Publication Coral attempted under the Invocation's identity. A
/// published Provider becomes the Operation Result; otherwise the Run ended
/// first. A Setup that was never published expires at its deadline.
pub(crate) async fn recover_create_provider(
    transaction: &mut coral_db::Transaction<'_>,
    run: &crate::lifecycle::RunRecord,
    invocation: &crate::InvocationId,
) -> Result<(crate::InvocationResultKind, Option<String>), Error> {
    let request = ManagementRequestId::parse(&invocation.to_string())?;
    let published = match coral_provider::publication_for_request_in_transaction(
        transaction,
        &run.workspace_id,
        &request,
    )
    .await?
    {
        Some(publication) => publication.published_provider().cloned(),
        None => None,
    };
    let Some(provider) = published else {
        return Ok((crate::InvocationResultKind::RunTerminated, None));
    };
    let published = match coral_provider::published_provider_in_transaction(
        transaction,
        &run.workspace_id,
        &provider,
        &run.principal,
    )
    .await
    {
        Ok(published) => published,
        // An Owner deleted the Provider since; no result can describe it now.
        Err(coral_provider::Error::ProviderNotFound) => {
            return Ok((crate::InvocationResultKind::RunTerminated, None));
        }
        Err(error) => return Err(error.into()),
    };
    let result =
        serde_json::to_string(&published_result(&published)).map_err(Error::ProgramValueJson)?;
    Ok((crate::InvocationResultKind::UpstreamOutcome, Some(result)))
}

/// Fails unless the Run still accepts this call's result. A Run that ended,
/// or whose state cannot be read, ends the Setup this call prepared.
async fn require_current_run(host: &BuiltinHost<'_>, setup: &ProviderSetupId) -> Result<(), Error> {
    let current = host.run.still_current().await;
    if matches!(current, Ok(true)) {
        return Ok(());
    }
    end_setup(host, setup).await;
    current.and(Err(Error::ProgramHostTerminated))
}

/// Ends, as Coral, a Setup that this call prepared but nobody will finish.
/// Best effort: the Setup still expires at its deadline.
async fn end_setup(host: &BuiltinHost<'_>, setup: &ProviderSetupId) {
    let Ok(now) = host.run.now() else {
        return;
    };
    let _ended = host
        .providers
        .abandon_setup(host.workspace_id, setup, now)
        .await;
}

/// Header names whose value is a credential. Request headers are non-secret by
/// contract, but only a name can show what a value is.
const CREDENTIAL_HEADERS: [&str; 5] = [
    "authorization",
    "proxy-authorization",
    "cookie",
    "x-api-key",
    "api-key",
];

fn names_a_credential(header: &str) -> bool {
    let header = header.to_ascii_lowercase();
    CREDENTIAL_HEADERS.contains(&header.as_str())
        || header.ends_with("-token")
        || header.ends_with("-secret")
}

/// The Operation Result for a Provider Coral published, with Coral as the
/// Upstream Service. It says what Coral resolved, so the Program need not guess
/// whether its next call needs a person.
fn published_result(published: &PublishedProvider) -> serde_json::Value {
    let mut data = provider_facts(published);
    data.insert("status".to_owned(), "published".into());
    data.insert(
        "namespace".to_owned(),
        published.provider.call_root().name().into(),
    );
    serde_json::json!({
        "data": data,
        "metadata": { "kind": "builtin" },
    })
}

/// What both `createProvider` and `listProviders` report about a Provider:
/// how to call it and whether the Program needs a person first.
fn provider_facts(published: &PublishedProvider) -> serde_json::Map<String, serde_json::Value> {
    let provider = &published.provider;
    [
        ("displayName", serde_json::json!(provider.display_name())),
        (
            "credentialRoute",
            route_name(provider.access().credential_route()).into(),
        ),
        (
            "authentication",
            authentication_name(published.authentication).into(),
        ),
        ("operations", published.available_operations.into()),
        ("accountRequired", published.account_required.into()),
    ]
    .into_iter()
    .map(|(field, value)| (field.to_owned(), value))
    .collect()
}

const fn route_name(route: CredentialRoute) -> &'static str {
    match route {
        CredentialRoute::None => "none",
        CredentialRoute::Fixed => "fixed",
        CredentialRoute::Caller => "caller",
    }
}

/// The Program's name for each method, as the `createProvider` input spells it.
const fn authentication_name(kind: coral_catalog::AuthenticationKind) -> &'static str {
    use coral_catalog::AuthenticationKind as K;
    match kind {
        K::None => "none",
        K::Bearer => "bearer",
        K::HeaderCredentials => "header_credentials",
        K::BasicAuth => "basic_auth",
        K::QueryCredentials => "query_credentials",
        K::OAuthDeviceCode => "oauth_device_code",
        K::OAuthAuthorizationCode => "oauth_authorization_code",
    }
}

/// A source URL must be an http or https URL with a host, and no credentials
/// or fragment, as the Setup requires. Whether Coral may fetch it is the
/// destination policy's to decide, when the Setup is prepared.
fn web_url(url: String) -> Result<String, InputProblem> {
    let problem = || {
        InputProblem::new(
            "source.url",
            "use an absolute `http` or `https` URL without credentials or a fragment",
        )
    };
    let parsed = url::Url::parse(&url).map_err(|_error| problem())?;
    if matches!(parsed.scheme(), "http" | "https")
        && parsed.host_str().is_some()
        && parsed.username().is_empty()
        && parsed.password().is_none()
        && parsed.fragment().is_none()
    {
        Ok(url)
    } else {
        Err(problem())
    }
}

/// Reef's new-provider stepper for one Setup. The namespace and the Credential
/// Route are already chosen. The link opens on the Operations, so the person
/// sees what the Program's Provider can do before signing in and publishing.
/// A valid namespace, a Setup id and a route need no URL encoding.
fn setup_path(
    workspace: &WorkspaceId,
    setup: &ProviderSetupId,
    namespace: &ProviderNamespace,
    route: CredentialRoute,
) -> String {
    let path = format!(
        "/workspaces/{workspace}/providers/new?setup={setup}&namespace={}",
        namespace.as_str()
    );
    // Reef asks the account question only for a route that has an Account.
    // `accountReview` would also let a sign-in in hand add the Provider without
    // a click, so the link carries it only where the question needs it.
    match route {
        CredentialRoute::None => path,
        CredentialRoute::Fixed => format!("{path}&account=fixed"),
        // It opens the question on Connect now. A confidential application
        // takes its secret only at that sign-in, so Connect later could not
        // publish.
        CredentialRoute::Caller => format!("{path}&account=caller&accountReview=1"),
    }
}

/// A Publication rejected because its Setup ended or changed, or its request
/// is stale, is a failure the Program can catch. Persistence and corrupt
/// evidence still stop the Run for recovery.
fn publication_error(error: coral_provider::Error) -> Error {
    match error {
        coral_provider::Error::ProviderSetupNotFound
        | coral_provider::Error::ProviderSetupNotReady
        | coral_provider::Error::ProviderSetupConsumed
        | coral_provider::Error::EtagMismatch
        | coral_provider::Error::RequestIdConflict
        | coral_provider::Error::ProviderChangedDuringPublication => {
            Error::InvocationRejected(CoralFailure::ProviderPublicationFailed.into())
        }
        other => setup_error(other),
    }
}

/// Maps a Setup preparation failure to the Program-safe vocabulary. Only
/// input problems are the Program's to catch; anything else is a host defect.
fn setup_error(error: coral_provider::Error) -> Error {
    let source =
        |reason: &str| Error::InvocationRejected(Rejection::invalid_input("source", reason));
    match error {
        coral_provider::Error::OutboundDestinationRejected => {
            Error::InvocationRejected(CoralFailure::OutboundDestinationRejected.into())
        }
        // A source Coral fetched but cannot use is the Program's to correct.
        // Each reason is Coral's own: a fetched body or error never reaches it.
        coral_provider::Error::InputUnavailable => {
            source("Coral could not fetch a usable response from the source URL")
        }
        coral_provider::Error::InvalidInput(_)
        | coral_provider::Error::Catalog(coral_catalog::Error::InvalidInput(_)) => {
            source("Coral cannot read an OpenAPI document or an MCP server from the source")
        }
        coral_provider::Error::Catalog(coral_catalog::Error::OpenApiDocumentTooLarge) => {
            source("the OpenAPI document exceeds Coral's size limit")
        }
        coral_provider::Error::AuthorizationDiscovery(code) => source(&format!(
            "Coral could not discover how to sign in to the MCP server ({code})"
        )),
        coral_provider::Error::AccountInput => Error::InvocationRejected(Rejection::invalid_input(
            "authentication",
            "Coral cannot use this authentication method for the source",
        )),
        coral_provider::Error::ProviderCallRootConflict => {
            Error::InvocationRejected(CoralFailure::ProviderNamespaceConflict.into())
        }
        other => authority_error(other),
    }
}

/// A User who lost Workspace authority since admission is a failure the
/// Program can catch; anything else is a host defect.
fn authority_error(error: coral_provider::Error) -> Error {
    match error {
        coral_provider::Error::Workspace(
            coral_workspace::Error::WorkspaceNotFound
            | coral_workspace::Error::PermissionDenied(_)
            | coral_workspace::Error::WorkspaceArchived,
        ) => Error::InvocationRejected(CoralFailure::AuthorityLost.into()),
        other => other.into(),
    }
}

/// Every problem Coral found when it read or confirmed the configuration, as
/// the arguments to correct. Coral writes these field errors and diagnostic
/// codes without any submitted value.
fn configuration_rejection(
    confirmed: &coral_provider::SetupConfigurationReview,
) -> Option<Rejection> {
    let problems: Vec<_> = confirmed
        .errors
        .iter()
        .map(|error| InputProblem::new(program_field(&error.field), error.message.clone()))
        .chain(confirmed.diagnostic.iter().map(|diagnostic| {
            InputProblem::new(
                "source",
                format!("Coral cannot build a Provider from the source ({diagnostic})"),
            )
        }))
        .collect();
    (!problems.is_empty()).then(|| Rejection::invalid_inputs(problems))
}

/// The `createProvider` argument for a stored configuration field address.
fn program_field(field: &str) -> String {
    // A server declaration is a JSON pointer that keys `servers`.
    if field.starts_with("#/") {
        return format!("servers.{field}");
    }
    for (stored, argument) in [
        ("display_name", "displayName"),
        ("request_headers", "requestHeaders"),
        ("credential_fields", "credentialFields"),
        ("authentication.fields", "credentialFields"),
        ("application", "authentication.client"),
        ("server", "servers"),
    ] {
        if field == stored {
            return argument.to_owned();
        }
        if let Some(rest) = field
            .strip_prefix(stored)
            .and_then(|rest| rest.strip_prefix('.'))
        {
            return format!("{argument}.{rest}");
        }
    }
    field.to_owned()
}

/// What is wrong with the shape of one argument, from serde's message without
/// the submitted value: a misplaced credential must not reach a failure message.
/// Field and variant names the contract declares stay, since they say what to write.
fn shape_reason(message: &str) -> String {
    if message.starts_with("missing field") || message.starts_with("unknown field") {
        return message.to_owned();
    }
    // serde writes the submitted value first, so only the last clause is its own.
    match message.rsplit_once("expected ") {
        Some((_found, expected)) => format!("expected {expected}"),
        None => "has a value Coral does not accept".to_owned(),
    }
}
