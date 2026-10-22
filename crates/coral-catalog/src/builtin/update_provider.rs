//! `coral.workspace.updateProvider`: the schemas a Program is typed from, the
//! arguments Coral reads against the input schema, and the result it writes
//! against the output schema. A call sends the namespace and only the fields
//! it changes. Each sent field replaces its saved value whole, maps included;
//! each omitted field keeps the value saved when the call runs.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};

use super::provider_input::{
    self, AuthenticationInput, ConfigurationChoices, CredentialFieldInput, ProviderSourceArgument,
    ProviderSourceInput, ServerInput,
};
use super::{
    ArgumentProblem, BuiltinCredentialRoute, BuiltinOperation, Definition, NAMESPACE_REASON,
    ProviderFacts, derived_schema, operation_result, present,
};
use crate::{
    AuthenticationConfiguration, OperationHints, OperationPresentation, ProviderConfiguration,
};

mod changes;
#[cfg(test)]
pub(super) mod tests;

pub use changes::{
    EntryChange, EntryStatus, FieldChange, ProviderChanges, SavedProvider, SourceRead,
};

pub(super) fn definition() -> Definition {
    Definition {
        key: BuiltinOperation::UpdateProvider.key(),
        summary: "Change a Provider's configuration or source",
        presentation: OperationPresentation::new(
            Some("Update provider".to_string()),
            "Change the configuration or the source of one Provider in this Workspace. Coral \
             publishes the change when nobody needs to act. Otherwise it returns a Coral UI \
             link where a person reviews and publishes it."
                .to_string(),
            false,
            vec!["providers".to_string()],
            Some(
                "Read the saved values with coral.workspace.getProvider first. Send namespace \
                 and only the fields to change. An omitted field keeps its saved value. A sent \
                 field replaces its saved value whole, maps included: to drop a request \
                 header, send requestHeaders without it. Remove authentication.issuer, \
                 authentication.resource and authentication.client.registration from \
                 getProvider's result before you send authentication. An OAuth method you send \
                 without client keeps the saved application. Send source to read the \
                 Provider from a new source, or refresh: true to read the saved source URL \
                 again, never both. Without either, Coral keeps the saved document. For an \
                 MCP source, Coral takes the server and the OAuth endpoints from the MCP \
                 server unless you send them. Never pass a secret or a literal credential \
                 value. A namespace that no Provider holds fails with provider_not_found. \
                 When Coral cannot fetch the source, the call fails with \
                 upstream_incomplete. When status is published, the change is live. When \
                 status is needs_sign_in, show the person the returned url: a sign-in, a new \
                 credential route, a new way to sign in, or a source that drops Operations \
                 needs a person. To check later whether the person published it, call \
                 coral.workspace.getProvider."
                    .to_string(),
            ),
        ),
        // No read-only hint: the default Operation Access State is approval_required.
        hints: OperationHints::default(),
        input: derived_schema::<Arguments>(Contract::Deserialize),
        output: derived_schema::<UpdateProviderResult>(Contract::Serialize),
    }
}

/// `updateProvider`'s arguments as a Program writes them. The input schema
/// is derived from these types, so the two cannot disagree on a shape.
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(description = "", transform = require_a_change)]
struct Arguments {
    #[schemars(
        description = "The Provider Namespace, such as github. It names the Provider to change, \
                       and it cannot change."
    )]
    namespace: String,
    // A description beside a reference would widen the type, so the guidance
    // says what `source` does.
    #[serde(default, deserialize_with = "present")]
    #[schemars(with = "ProviderSourceInput")]
    source: Option<ProviderSourceInput>,
    #[serde(default, deserialize_with = "present")]
    #[schemars(
        with = "Refresh",
        description = "Reads the saved source URL again, to pick up a newer document or tool \
                       list. Do not send it with source."
    )]
    refresh: Option<Refresh>,
    #[serde(default, deserialize_with = "present")]
    #[schemars(
        with = "BuiltinCredentialRoute",
        description = "Whose account Invocations use. A new route needs a person to confirm it \
                       in the Coral UI."
    )]
    credential_route: Option<BuiltinCredentialRoute>,
    #[serde(default, deserialize_with = "present")]
    #[schemars(with = "String", description = "The Provider's name.")]
    display_name: Option<String>,
    #[serde(default, deserialize_with = "present")]
    #[schemars(
        with = "BTreeMap<String, ServerInput>",
        description = "Server choice per server declaration, keyed by declaration pointer such \
                       as #/servers. It replaces every saved choice."
    )]
    servers: Option<BTreeMap<String, ServerInput>>,
    #[serde(default, deserialize_with = "present")]
    #[schemars(with = "AuthenticationInput")]
    authentication: Option<AuthenticationInput>,
    #[serde(default, deserialize_with = "present")]
    #[schemars(
        with = "BTreeMap<String, String>",
        description = "Non-secret headers sent with every request. It replaces every saved \
                       header: to drop one, send the others without it."
    )]
    request_headers: Option<BTreeMap<String, String>>,
    #[serde(default, deserialize_with = "present")]
    #[schemars(
        with = "BTreeMap<String, CredentialFieldInput>",
        description = "The values a person enters when they sign in, keyed by field name, such \
                       as token. It replaces every saved field. Never include a value."
    )]
    credential_fields: Option<BTreeMap<String, CredentialFieldInput>>,
}

/// A call must change something: the namespace alone changes nothing. The
/// schema says so with the one count it can state; the parser also names it.
fn require_a_change(schema: &mut schemars::Schema) {
    schema.insert("minProperties".to_owned(), json!(2));
}

/// `refresh` is only ever `true`: `false` would be a field that changes nothing.
#[derive(Debug, Clone, Copy)]
struct Refresh;

impl<'de> Deserialize<'de> for Refresh {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if bool::deserialize(deserializer)? {
            Ok(Self)
        } else {
            Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Bool(false),
                &"`true`",
            ))
        }
    }
}

impl JsonSchema for Refresh {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Refresh".into()
    }

    fn inline_schema() -> bool {
        true
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "type": "boolean", "const": true })
    }
}

/// `updateProvider`'s arguments, checked by every rule that needs no
/// Workspace state.
#[derive(Debug, Clone)]
pub struct UpdateProviderArguments {
    /// A valid Provider Namespace.
    pub namespace: String,
    /// Where Coral reads the Provider from.
    pub source: SourceChange,
    /// The Credential Route the Program asked for, if any.
    pub credential_route: Option<BuiltinCredentialRoute>,
    /// The configuration fields the Program sent, each over its saved value.
    pub choices: ConfigurationChoices,
}

/// Where an update reads the Provider from.
#[derive(Debug, Clone)]
pub enum SourceChange {
    /// The saved document or snapshot, with no network call.
    Keep,
    /// The saved source URL, read again when the call runs.
    Refresh,
    /// A new source.
    Replace(ProviderSourceArgument),
}

impl UpdateProviderArguments {
    /// Reads the arguments: the shape first, then the namespace syntax, the
    /// source, and that the call changes something, without any network call.
    ///
    /// # Errors
    ///
    /// Returns every argument to correct.
    pub fn parse(arguments: &Value) -> Result<Self, Vec<ArgumentProblem>> {
        let arguments: Arguments = super::read(arguments).map_err(|problem| vec![problem])?;
        // Each check below stands alone, so the Program learns every problem at once.
        let mut problems = Vec::new();
        if !crate::provider_namespace_is_valid(&arguments.namespace) {
            problems.push(ArgumentProblem::new("namespace", NAMESPACE_REASON));
        }
        let choices = provider_input::choices(
            arguments.display_name,
            arguments.servers,
            arguments.authentication,
            arguments.request_headers,
            arguments.credential_fields,
            &mut problems,
        );
        let refresh = arguments.refresh.is_some();
        let replaces_source = arguments.source.is_some();
        let source = arguments
            .source
            .map(ProviderSourceInput::into_source)
            .transpose()
            .map_err(|problem| problems.push(problem))
            .ok()
            .flatten();
        if refresh && replaces_source {
            problems.push(ArgumentProblem::new(
                "refresh",
                "send `refresh` or `source`, not both: `source` replaces the saved source",
            ));
        }
        if !refresh
            && !replaces_source
            && arguments.credential_route.is_none()
            && choices.is_empty()
            && problems.is_empty()
        {
            problems.push(ArgumentProblem::new(
                "",
                "send at least one field to change beside `namespace`",
            ));
        }
        if !problems.is_empty() {
            return Err(problems);
        }
        Ok(Self {
            namespace: arguments.namespace,
            source: match (source, refresh) {
                (Some(source), _) => SourceChange::Replace(source),
                (None, true) => SourceChange::Refresh,
                (None, false) => SourceChange::Keep,
            },
            credential_route: arguments.credential_route,
            choices,
        })
    }
}

impl UpdateProviderArguments {
    /// The configuration to confirm: each sent field over the configuration
    /// saved when the call runs. An MCP server reports its own server and
    /// OAuth endpoints, which Reef never keeps from the saved values either,
    /// so `discovered`, the MCP server's suggestion, sets them unless sent.
    /// An OAuth method sent without an application keeps the saved one, as
    /// confirmation does.
    #[must_use]
    pub fn configuration(
        &self,
        saved: ProviderConfiguration,
        discovered: Option<&ProviderConfiguration>,
    ) -> ProviderConfiguration {
        let application = saved
            .authentication
            .as_ref()
            .and_then(AuthenticationConfiguration::oauth_client)
            .cloned();
        let mut configured = self.choices.over(match discovered {
            Some(discovered) => with_mcp_server_settings(saved, discovered),
            None => saved,
        });
        if let Some(application) = &application {
            configured.authentication = configured
                .authentication
                .map(|method| method.with_oauth_client(application));
        }
        configured
    }
}

/// `saved` with an MCP server's own server and OAuth endpoints.
fn with_mcp_server_settings(
    saved: ProviderConfiguration,
    discovered: &ProviderConfiguration,
) -> ProviderConfiguration {
    let authentication = match (saved.authentication, &discovered.authentication) {
        (Some(saved), Some(found)) if saved.kind() == found.kind() => {
            Some(match saved.oauth_scopes() {
                // The endpoints are the server's; the scopes and the
                // application stay the saved choices.
                Some((scopes, format)) => found
                    .clone()
                    .with_oauth_scopes(scopes.to_vec(), format)
                    .with_client_policy(saved.oauth_client().cloned()),
                None => saved,
            })
        }
        (saved, _) => saved,
    };
    ProviderConfiguration {
        servers: discovered.servers.clone(),
        authentication,
        ..saved
    }
}

/// What `updateProvider` returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
#[schemars(deny_unknown_fields, description = "")]
pub enum UpdateProviderResult {
    /// Coral published the change, and the Program can call the Provider now.
    #[schemars(description = "Coral published the change. Calls use it now.")]
    Published {
        /// The Provider Namespace.
        #[schemars(description = "The Provider Namespace.")]
        namespace: String,
        /// What the Program needs to call it.
        #[serde(flatten)]
        facts: ProviderFacts,
    },
    /// A person finishes the change at `url`.
    #[schemars(description = "A person must finish the change in the Coral UI.")]
    NeedsSignIn {
        /// The Coral UI page where a person reviews and publishes the change.
        #[schemars(
            description = "The Coral UI page where a person reviews and publishes the change."
        )]
        url: String,
        /// The Provider Namespace.
        #[schemars(description = "The Provider Namespace.")]
        namespace: String,
    },
}

impl UpdateProviderResult {
    /// The Operation Result.
    #[must_use]
    pub fn into_operation_result(self) -> Value {
        operation_result(&self)
    }
}
