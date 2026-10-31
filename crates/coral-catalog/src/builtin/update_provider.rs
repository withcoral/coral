//! `coral.workspace.updateProvider`: the schemas a Program is typed from, the
//! arguments Coral reads against the input schema, and the result it writes
//! against the output schema. A call sends the namespace and only the fields
//! it changes. A sent value replaces its saved value. A sent map changes only
//! the keys it names, and a key sent as `null` removes its entry, so the
//! arguments show everything a call removes. Each omitted field keeps the
//! value saved when the call runs.

use std::collections::BTreeMap;

use coral_workspace::WorkspaceAction;
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};

use super::provider_input::{
    AuthenticationInput, CredentialFieldInput, ProviderSourceArgument, ProviderSourceInput,
    ServerInput,
};
use super::{
    ArgumentProblem, BuiltinCredentialRoute, BuiltinTool, Definition, NAMESPACE_REASON,
    ProviderFacts, derived_schema, present, tool_result,
};
use crate::{
    AuthenticationConfiguration, CredentialField, ProviderConfiguration, ServerSelection,
    ToolHints, ToolPresentation,
};

#[cfg(test)]
pub(super) mod tests;

/// Changing a Provider changes the Workspace's configuration, which only an
/// Owner does.
pub(super) const NEEDS: WorkspaceAction = WorkspaceAction::Configure;

pub(super) fn definition() -> Definition {
    Definition {
        key: BuiltinTool::UpdateProvider.key(),
        summary: "Change a Provider's configuration or source",
        presentation: ToolPresentation::new(
            Some("Update provider".to_string()),
            "Change the configuration or the source of one Provider in this Workspace. It \
             returns a Coral UI link where a person reviews and publishes the change. A \
             change to displayName alone applies at once."
                .to_string(),
            false,
            vec!["providers".to_string()],
            Some(
                "Read the saved values with coral.workspace.getProvider first. Send namespace \
                 and only the fields to change. An omitted field keeps its saved value. A sent \
                 value replaces its saved value. A sent map changes only the keys it names: \
                 to drop a request header, send requestHeaders with that header set to null. \
                 Remove authentication.issuer, \
                 authentication.resource and authentication.client.registration from \
                 getProvider's result before you send authentication. An OAuth method you send \
                 without client keeps the saved application. Send source to read the \
                 Provider from a new source, or refresh: true to read the saved source URL \
                 again, never both. Without either, Coral keeps the saved document. For an \
                 MCP source, Coral takes the server and the OAuth endpoints from the MCP \
                 server unless you send them. Never pass a secret or a literal credential \
                 value. A namespace that no Provider holds fails with provider_not_found. \
                 When Coral cannot fetch the source, the call fails with \
                 upstream_incomplete. When status is published, the Provider has its new \
                 displayName. When status is needs_person, show the person the returned \
                 url. Calls keep the saved configuration until the person publishes the \
                 change. To check later whether the person published it, call \
                 coral.workspace.getProvider."
                    .to_string(),
            ),
        ),
        // No read-only hint: the default Tool Access State is approval_required.
        hints: ToolHints::default(),
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
        description = "Whose account Invocations use."
    )]
    credential_route: Option<BuiltinCredentialRoute>,
    #[serde(default, deserialize_with = "present")]
    #[schemars(with = "String", description = "The Provider's name.")]
    display_name: Option<String>,
    #[serde(default, deserialize_with = "present")]
    #[schemars(
        with = "BTreeMap<String, Option<ServerInput>>",
        description = "Server choices to change, keyed by declaration pointer such as \
                       #/servers. null removes the saved choice. An unnamed declaration keeps \
                       its saved choice."
    )]
    servers: Option<BTreeMap<String, Option<ServerInput>>>,
    #[serde(default, deserialize_with = "present")]
    #[schemars(with = "AuthenticationInput")]
    authentication: Option<AuthenticationInput>,
    #[serde(default, deserialize_with = "present")]
    #[schemars(
        with = "BTreeMap<String, Option<String>>",
        description = "Non-secret headers to change, sent with every request. null removes the \
                       saved header. An unnamed header keeps its saved value."
    )]
    request_headers: Option<BTreeMap<String, Option<String>>>,
    #[serde(default, deserialize_with = "present")]
    #[schemars(
        with = "BTreeMap<String, Option<CredentialFieldInput>>",
        description = "Values a person enters when they sign in, keyed by field name, such as \
                       token. null removes the saved field. An unnamed field stays. Never \
                       include a value."
    )]
    credential_fields: Option<BTreeMap<String, Option<CredentialFieldInput>>>,
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
    pub choices: ConfigurationPatch,
}

/// The configuration fields an update sends. A sent value replaces its saved
/// value. A sent map changes only the keys it names: a key with a value sets
/// that entry, and a key with `None` removes it.
#[derive(Debug, Clone, Default)]
pub struct ConfigurationPatch {
    /// The Provider display name.
    pub display_name: Option<String>,
    /// The authentication method and its non-secret settings.
    pub authentication: Option<AuthenticationConfiguration>,
    /// Server choices to set or remove, by server declaration.
    pub servers: BTreeMap<String, Option<ServerSelection>>,
    /// Non-secret headers to set or remove.
    pub request_headers: BTreeMap<String, Option<String>>,
    /// Credential fields to set or remove.
    pub credential_fields: BTreeMap<String, Option<CredentialField>>,
}

impl ConfigurationPatch {
    /// Whether the call sends no configuration change.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.display_name.is_none()
            && self.authentication.is_none()
            && self.servers.is_empty()
            && self.request_headers.is_empty()
            && self.credential_fields.is_empty()
    }

    /// `saved` with each sent change applied.
    #[must_use]
    pub fn over(&self, saved: ProviderConfiguration) -> ProviderConfiguration {
        ProviderConfiguration {
            display_name: self.display_name.clone().unwrap_or(saved.display_name),
            authentication: self.authentication.clone().or(saved.authentication),
            servers: patched(saved.servers, &self.servers),
            request_headers: patched(saved.request_headers, &self.request_headers),
            credential_fields: patched(saved.credential_fields, &self.credential_fields),
        }
    }

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
        let mut configured = self.over(match discovered {
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

/// `saved` with each key of `changes` set, or removed for `None`.
fn patched<T: Clone>(
    mut saved: BTreeMap<String, T>,
    changes: &BTreeMap<String, Option<T>>,
) -> BTreeMap<String, T> {
    for (key, change) in changes {
        match change {
            Some(value) => saved.insert(key.clone(), value.clone()),
            None => saved.remove(key),
        };
    }
    saved
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
        let choices = ConfigurationPatch {
            display_name: arguments.display_name,
            authentication: arguments
                .authentication
                .map(AuthenticationConfiguration::try_from)
                .transpose()
                .map_err(|problem| problems.push(problem))
                .ok()
                .flatten(),
            servers: entries(arguments.servers),
            request_headers: entries(arguments.request_headers),
            credential_fields: entries(arguments.credential_fields),
        };
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

/// A sent map's changes in Coral's types; an unsent map changes nothing.
fn entries<I, T: From<I>>(
    sent: Option<BTreeMap<String, Option<I>>>,
) -> BTreeMap<String, Option<T>> {
    sent.into_iter()
        .flatten()
        .map(|(key, value)| (key, value.map(T::from)))
        .collect()
}

impl UpdateProviderArguments {
    /// The new display name, when the call changes nothing else. A rename
    /// keeps the Provider's Generation, as Reef's settings page does.
    #[must_use]
    pub fn rename(&self) -> Option<&str> {
        let ConfigurationPatch {
            display_name,
            authentication,
            servers,
            request_headers,
            credential_fields,
        } = &self.choices;
        let keeps_the_rest = matches!(self.source, SourceChange::Keep)
            && self.credential_route.is_none()
            && authentication.is_none()
            && servers.is_empty()
            && request_headers.is_empty()
            && credential_fields.is_empty();
        display_name.as_deref().filter(|_name| keeps_the_rest)
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
    /// Coral renamed the Provider, and the Program can call it now.
    #[schemars(description = "Coral renamed the Provider. Calls use the new name now.")]
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
    NeedsPerson {
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
    /// The Tool Result.
    #[must_use]
    pub fn into_tool_result(self) -> Value {
        tool_result(&self)
    }
}
