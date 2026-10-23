//! `coral.workspace.addProvider`: the schemas a Program is typed from, the
//! arguments Coral reads against the input schema, and the result it writes
//! against the output schema.

use std::collections::BTreeMap;

use coral_workspace::WorkspaceAction;
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{
    ArgumentProblem, BuiltinCredentialRoute, BuiltinOperation, Definition, ProviderFacts,
    derived_schema, operation_result, present,
};
use crate::{
    AuthenticationConfiguration, CredentialField, OpenApiDocument, OperationHints,
    OperationPresentation, ProviderConfiguration, ServerSelection,
};

mod input;
#[cfg(test)]
pub(super) mod tests;

/// Adding a Provider changes the Workspace's configuration, which only an
/// Owner does.
pub(super) const NEEDS: WorkspaceAction = WorkspaceAction::Configure;

pub(super) fn definition() -> Definition {
    Definition {
        key: BuiltinOperation::AddProvider.key(),
        summary: "Add a Provider from an OpenAPI document or URL, or an MCP server URL",
        presentation: OperationPresentation::new(
            Some("Add provider".to_string()),
            "Add a Provider to this Workspace from an OpenAPI document, an OpenAPI \
             document URL, or an MCP server URL. Coral publishes it when nobody needs to \
             sign in. Otherwise it returns a Coral UI link where a person finishes \
             and publishes it."
                .to_string(),
            false,
            vec!["providers".to_string()],
            Some(
                "Choose credentialRoute to match the source: none when it needs no \
                 credential, fixed for one shared account, caller for each person's own \
                 account. A route that does not match the source fails with invalid_input. \
                 Omit displayName, servers, authentication, credentialFields and \
                 requestHeaders to use what the source suggests, or give them to override \
                 it. When the source declares no credential but the service needs one, \
                 give authentication, such as { method: \"bearer\" }, with fixed or \
                 caller. Never pass a secret or a literal credential value: Coral stores \
                 your arguments, and Workspace Owners can see them. When Coral cannot \
                 fetch a source URL, the call fails with upstream_incomplete: check that \
                 the URL is reachable, then call again. When status is \
                 published, call the Provider's Operations under \
                 coral.providers.<namespace>. When status is needs_sign_in, show the \
                 person the returned url. To check later whether the person published it, \
                 call coral.workspace.listProviders."
                    .to_string(),
            ),
        ),
        // No read-only hint: the default Operation Access State is approval_required.
        hints: OperationHints::default(),
        input: derived_schema::<Arguments>(Contract::Deserialize),
        output: derived_schema::<AddProviderResult>(Contract::Serialize),
    }
}

/// `addProvider`'s arguments, checked by every rule that needs no
/// Workspace state. The Provider's configuration leaves each omitted field to
/// the source's suggestion.
#[derive(Debug, Clone)]
pub struct AddProviderArguments {
    /// A valid Provider Namespace.
    pub namespace: String,
    /// Where Coral reads the Provider from.
    pub source: ProviderSourceArgument,
    /// Whose Account Invocations use.
    pub credential_route: BuiltinCredentialRoute,
    /// The configuration fields the Program chose.
    pub choices: ConfigurationChoices,
}

/// Where Coral reads a Provider from.
#[derive(Debug, Clone)]
pub enum ProviderSourceArgument {
    /// A complete OpenAPI document.
    OpenApiDocument(OpenApiDocument),
    /// An OpenAPI document's web URL.
    OpenApiUrl(String),
    /// A Streamable HTTP MCP server's web URL.
    McpUrl(String),
}

/// The configuration fields the Program chose, each over the source's
/// suggestion.
#[derive(Debug, Clone)]
pub struct ConfigurationChoices {
    /// The Provider display name.
    pub display_name: Option<String>,
    /// A server per server declaration.
    pub servers: Option<BTreeMap<String, ServerSelection>>,
    /// The authentication method and its non-secret settings.
    pub authentication: Option<AuthenticationConfiguration>,
    /// Non-secret headers sent with every request.
    pub request_headers: Option<BTreeMap<String, String>>,
    /// The values a person enters when they sign in.
    pub credential_fields: Option<BTreeMap<String, CredentialField>>,
}

impl ConfigurationChoices {
    /// The configuration to confirm: each chosen field over `suggested`.
    #[must_use]
    pub fn over(&self, suggested: ProviderConfiguration) -> ProviderConfiguration {
        ProviderConfiguration {
            display_name: self.display_name.clone().unwrap_or(suggested.display_name),
            servers: self.servers.clone().unwrap_or(suggested.servers),
            authentication: self.authentication.clone().or(suggested.authentication),
            request_headers: self
                .request_headers
                .clone()
                .unwrap_or(suggested.request_headers),
            credential_fields: self
                .credential_fields
                .clone()
                .unwrap_or(suggested.credential_fields),
        }
    }
}

/// `addProvider`'s arguments as a Program writes them. The input schema
/// is derived from these types, so the two cannot disagree on a shape.
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(description = "", transform = require_credential_route)]
struct Arguments {
    #[schemars(
        description = "Provider Namespace: a short JavaScript identifier, such as github. The \
                       Provider's Operations are called under coral.providers.<namespace>."
    )]
    namespace: String,
    source: SourceInput,
    // Optional to serde, so a missing route is reported beside every other
    // problem; required to a Program.
    #[serde(default, deserialize_with = "present")]
    #[schemars(
        with = "BuiltinCredentialRoute",
        description = "Whose account Invocations use: none for a source that needs no \
                       credential, fixed for one shared account that a person signs in to, or \
                       caller for each person's own account."
    )]
    credential_route: Option<BuiltinCredentialRoute>,
    #[serde(default, deserialize_with = "present")]
    #[schemars(
        with = "String",
        description = "The Provider's name. Omit it to use the source's title."
    )]
    display_name: Option<String>,
    #[serde(default, deserialize_with = "present")]
    #[schemars(
        with = "BTreeMap<String, input::ServerInput>",
        description = "Server choice per server declaration, keyed by declaration pointer such \
                       as #/servers. Omit it to use the source's server. If the source declares \
                       none, or a server variable has no default, the call fails with \
                       invalid_input: find the value in the source's documentation or ask the \
                       user, then pass it here."
    )]
    servers: Option<BTreeMap<String, input::ServerInput>>,
    #[serde(default, deserialize_with = "present")]
    #[schemars(with = "input::AuthenticationInput")]
    authentication: Option<input::AuthenticationInput>,
    #[serde(default, deserialize_with = "present")]
    #[schemars(
        with = "BTreeMap<String, String>",
        description = "Non-secret headers sent with every request. Omit it to use the source's \
                       suggestion. Send a credential through authentication, so a person \
                       enters it when they sign in."
    )]
    request_headers: Option<BTreeMap<String, String>>,
    #[serde(default, deserialize_with = "present")]
    #[schemars(
        with = "BTreeMap<String, input::CredentialFieldInput>",
        description = "The values a person enters when they sign in, keyed by field name, such \
                       as token. Omit it to use the source's fields. Give it when authentication \
                       names a field the source does not declare. Never include a value."
    )]
    credential_fields: Option<BTreeMap<String, input::CredentialFieldInput>>,
}

/// A Program must give a route, though serde reads a missing one, so that
/// Coral reports it beside every other problem.
fn require_credential_route(schema: &mut schemars::Schema) {
    if let Some(required) = schema.get_mut("required").and_then(Value::as_array_mut) {
        required.push(json!("credentialRoute"));
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[schemars(rename = "ProviderSource")]
enum SourceInput {
    OpenapiDocument {
        #[schemars(description = "A complete OpenAPI document, as JSON or YAML text.")]
        document: String,
    },
    OpenapiUrl {
        #[schemars(description = "The URL of an OpenAPI document.")]
        url: String,
    },
    McpUrl {
        #[schemars(description = "The URL of a Streamable HTTP MCP server.")]
        url: String,
    },
}

impl SourceInput {
    fn into_source(self) -> Result<ProviderSourceArgument, ArgumentProblem> {
        match self {
            Self::OpenapiDocument { document } => OpenApiDocument::parse(&document)
                .map(ProviderSourceArgument::OpenApiDocument)
                .map_err(|_error| {
                    ArgumentProblem::new(
                        "source.document",
                        "Coral cannot read this as an OpenAPI document",
                    )
                }),
            Self::OpenapiUrl { url } => web_url(url).map(ProviderSourceArgument::OpenApiUrl),
            Self::McpUrl { url } => web_url(url).map(ProviderSourceArgument::McpUrl),
        }
    }
}

impl AddProviderArguments {
    /// Reads the arguments: the shape first, then the namespace syntax, the
    /// route and the source, without any network call.
    ///
    /// # Errors
    ///
    /// Returns every argument to correct.
    pub fn parse(arguments: &Value) -> Result<Self, Vec<ArgumentProblem>> {
        let arguments: Arguments = super::read(arguments).map_err(|problem| vec![problem])?;
        // Each check below stands alone, so the Program learns every problem at once.
        let mut problems = Vec::new();
        if !crate::provider_namespace_is_valid(&arguments.namespace) {
            problems.push(ArgumentProblem::new(
                "namespace",
                "use a JavaScript identifier of at most 128 bytes, such as `weather`",
            ));
        }
        let authentication = arguments
            .authentication
            .map(AuthenticationConfiguration::try_from)
            .transpose()
            .map_err(|problem| problems.push(problem))
            .ok()
            .flatten();
        if arguments.credential_route.is_none() {
            problems.push(ArgumentProblem::new(
                "credentialRoute",
                "choose `none`, `fixed` or `caller`",
            ));
        }
        let source = arguments
            .source
            .into_source()
            .map_err(|problem| problems.push(problem))
            .ok();
        let (Some(credential_route), Some(source), true) =
            (arguments.credential_route, source, problems.is_empty())
        else {
            return Err(problems);
        };
        Ok(Self {
            namespace: arguments.namespace,
            source,
            credential_route,
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
}

impl AddProviderArguments {
    /// The argument a Program corrects for a configuration field that Coral
    /// reports when it reads or confirms the configuration, such as
    /// `requestHeaders.Host` for `request_headers.Host`.
    #[must_use]
    pub fn argument_for(configuration_field: &str) -> String {
        // A server declaration is a JSON pointer that keys `servers`.
        if configuration_field.starts_with("#/") {
            return format!("servers.{configuration_field}");
        }
        for (stored, argument) in [
            ("display_name", "displayName"),
            ("request_headers", "requestHeaders"),
            ("credential_fields", "credentialFields"),
            ("authentication.fields", "credentialFields"),
            ("application", "authentication.client"),
            ("server", "servers"),
        ] {
            if configuration_field == stored {
                return argument.to_owned();
            }
            if let Some(rest) = configuration_field
                .strip_prefix(stored)
                .and_then(|rest| rest.strip_prefix('.'))
            {
                return format!("{argument}.{rest}");
            }
        }
        configuration_field.to_owned()
    }
}

/// What `addProvider` returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
#[schemars(deny_unknown_fields, description = "")]
pub enum AddProviderResult {
    /// Coral published the Provider, and the Program can call it now.
    #[schemars(description = "Coral published the Provider. You can call it now.")]
    Published {
        /// The published Provider Namespace.
        #[schemars(description = "The published Provider Namespace.")]
        namespace: String,
        /// What the Program needs to call it.
        #[serde(flatten)]
        facts: ProviderFacts,
    },
    /// A person finishes the Provider at `url`.
    #[schemars(description = "A person must finish the Provider in the Coral UI.")]
    NeedsSignIn {
        /// The Coral UI page where a person finishes and publishes it.
        #[schemars(
            description = "The Coral UI page where a person finishes and publishes the Provider."
        )]
        url: String,
        /// The requested Provider Namespace.
        #[schemars(description = "The requested Provider Namespace. The person can change it.")]
        namespace: String,
    },
}

impl AddProviderResult {
    /// The Operation Result.
    #[must_use]
    pub fn into_operation_result(self) -> Value {
        operation_result(&self)
    }
}

/// A source URL must be an http or https URL with a host, and no credentials
/// or fragment, as the Setup requires. Whether Coral may fetch it is the
/// destination policy's to decide, when the Setup is prepared.
fn web_url(url: String) -> Result<String, ArgumentProblem> {
    let problem = || {
        ArgumentProblem::new(
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
