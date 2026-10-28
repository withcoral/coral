//! The Built-in Provider's Coral-defined Tool catalog.
//!
//! Coral defines each Tool here in Rust: its Tool Key, input and
//! output schemas, presentation, and Tool Hints. No OpenAPI document or
//! MCP snapshot exists. The Generation pins this catalog's identity instead,
//! so each Workspace's Generation records which definition it materialized.
//!
//! Each Tool's module owns its whole contract: the schemas a Program is
//! typed from, the arguments Coral reads against the input schema, and the
//! result it writes against the output schema. Each schema is derived from
//! the types Coral reads or writes, and a test holds each parser and result to
//! its schema where serde and the derivation could still differ.

use std::fmt;
use std::sync::Arc;

use coral_workspace::WorkspaceAction;
use schemars::generate::{Contract, SchemaSettings};
use schemars::transform::{RecursiveTransform, RestrictFormats};
use schemars::{JsonSchema, Schema};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Value, json};

use crate::schema::{
    Direction, ProjectionOptions, SchemaGraphAccumulator, SharedTypes, TypeScriptNames,
    TypeScriptProjector, canonicalize, render_callable_declaration,
};
use crate::{
    AuthenticationKind, CatalogDiagnostic, Error, ExecutionBinding, MaterializedTool,
    PreparedProviderCatalog, SchemaGraph, SchemaId, SchemaSemantics, ToolHints, ToolKey,
    ToolPresentation,
};

mod add_provider;
mod get_policy;
mod get_provider;
mod list_providers;
mod provider_input;
mod update_policy;
mod update_provider;

pub use add_provider::{AddProviderArguments, AddProviderResult};
pub use get_policy::{GetPolicyArguments, GetPolicyResult};
pub use get_provider::{
    ConfigurationOutput, GetProviderArguments, GetProviderResult, ProviderSourceOutput,
};
pub use list_providers::{
    ListProvidersResult, ListedProvider, ListedProviderState, ListedSetup, ListedSetupState,
};
pub use provider_input::{ConfigurationChoices, ProviderSourceArgument};
pub use update_policy::{UpdatePolicyArguments, UpdatePolicyResult};
pub use update_provider::{
    ConfigurationPatch, SourceChange, UpdateProviderArguments, UpdateProviderResult,
};

/// Safe display name of the Built-in Provider.
pub const BUILTIN_PROVIDER_DISPLAY_NAME: &str = "Coral";
/// Starts the catalog identity a built-in Generation pins as its input.
const CATALOG_IDENTITY_PREFIX: &str = "coral-admin-catalog:";
pub(crate) const MATERIALIZER_VERSION: &str = "coral-admin-catalog-v1";

/// The identity a built-in Generation pins: the prefix and the catalog's
/// fingerprint. A Coral version that changes the catalog pins a different
/// identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltinCatalogIdentity(String);

impl BuiltinCatalogIdentity {
    fn of(catalog: &PreparedProviderCatalog) -> Self {
        Self(format!(
            "{CATALOG_IDENTITY_PREFIX}{}",
            catalog.fingerprint().as_str()
        ))
    }

    /// Reads a pinned identity; `None` for a malformed one. It accepts any
    /// fingerprint, because another Coral version pins its own catalog's identity.
    pub(crate) fn parse(pinned: &str) -> Option<Self> {
        pinned
            .strip_prefix(CATALOG_IDENTITY_PREFIX)
            .is_some_and(crate::types::is_lowercase_sha256)
            .then(|| Self(pinned.to_owned()))
    }

    /// Returns the identity as a Generation stores it.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BuiltinCatalogIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// One Coral-owned Tool that this Coral version performs itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinTool {
    /// Adds a Provider from an OpenAPI document or URL, or an MCP server URL.
    /// Coral publishes it when nobody needs to sign in; otherwise a person
    /// finishes it in Reef.
    AddProvider,
    /// Lists every Provider in the Workspace, so a Program can see which
    /// namespaces are taken before it adds one.
    ListProviders,
    /// Reads one Provider's Provider Configuration, so a Program can change some
    /// of it and send back only those fields.
    GetProvider,
    /// Reads the exact rules a Workspace Owner set for one Provider's
    /// Tools, so a Program can find the ones discovery hides.
    GetPolicy,
    /// Sets the exact state of 1–100 Tools of one Provider. It never
    /// reaches a Built-in Tool.
    UpdatePolicy,
    /// Changes one Owner-created Provider's configuration or source. Coral
    /// publishes the change when nobody needs to act; otherwise a person
    /// finishes it in Reef.
    UpdateProvider,
}

impl BuiltinTool {
    const ALL: [Self; 6] = [
        Self::AddProvider,
        Self::ListProviders,
        Self::GetProvider,
        Self::GetPolicy,
        Self::UpdatePolicy,
        Self::UpdateProvider,
    ];

    /// Returns the Tool Key, which is also its relative path.
    #[must_use]
    pub const fn key(self) -> &'static str {
        match self {
            Self::AddProvider => "addProvider",
            Self::ListProviders => "listProviders",
            Self::GetProvider => "getProvider",
            Self::GetPolicy => "getPolicy",
            Self::UpdatePolicy => "updatePolicy",
            Self::UpdateProvider => "updateProvider",
        }
    }

    /// Returns the Built-in Tool under `key`, or `None` for one that
    /// another Coral version defines.
    #[must_use]
    pub fn from_key(key: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|tool| tool.key() == key)
    }

    /// Returns the Workspace action a Principal's authority must permit for
    /// it to discover or call this Tool.
    #[must_use]
    pub const fn needs(self) -> WorkspaceAction {
        match self {
            Self::AddProvider => add_provider::NEEDS,
            Self::ListProviders => list_providers::NEEDS,
            Self::GetProvider => get_provider::NEEDS,
            Self::GetPolicy => get_policy::NEEDS,
            Self::UpdatePolicy => update_policy::NEEDS,
            Self::UpdateProvider => update_provider::NEEDS,
        }
    }
}

/// The Built-in Provider's catalog that a Coral process serves, and the
/// identity each Generation of it pins. Bootstrap prepares it once. Workspace
/// creation and the update at boot publish it.
#[derive(Clone)]
pub struct BuiltinCatalog(Arc<PreparedBuiltinCatalog>);

struct PreparedBuiltinCatalog {
    prepared: PreparedProviderCatalog,
    identity: BuiltinCatalogIdentity,
}

impl BuiltinCatalog {
    /// Prepares the catalog this Coral version defines.
    ///
    /// # Errors
    ///
    /// Returns an invalid built-in definition.
    pub fn running() -> Result<Self, Error> {
        Self::from_definitions(BuiltinTool::ALL.into_iter().map(definition).collect())
    }

    /// Prepares this Coral version's catalog plus Tools it does not
    /// perform, as another Coral version could publish them. Each extra
    /// Tool takes an object and returns one.
    ///
    /// # Errors
    ///
    /// Returns an invalid built-in definition.
    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn with_unknown_tools(
        extra: impl IntoIterator<Item = (&'static str, ToolHints)>,
    ) -> Result<Self, Error> {
        let mut definitions: Vec<Definition> =
            BuiltinTool::ALL.into_iter().map(definition).collect();
        definitions.extend(extra.into_iter().map(|(key, hints)| Definition {
            key,
            summary: "A Tool another Coral version defines",
            presentation: ToolPresentation::new(
                None,
                "A Tool another Coral version defines.".to_string(),
                false,
                Vec::new(),
                None,
            ),
            hints,
            input: empty_object_schema(),
            output: empty_object_schema(),
        }));
        Self::from_definitions(definitions)
    }

    fn from_definitions(definitions: Vec<Definition>) -> Result<Self, Error> {
        let prepared = prepare(definitions)
            .map_err(|diagnostic| Error::InvalidInput(diagnostic.message().to_string()))?;
        let identity = BuiltinCatalogIdentity::of(&prepared);
        Ok(Self(Arc::new(PreparedBuiltinCatalog {
            prepared,
            identity,
        })))
    }

    /// Returns the identity a Generation of this catalog pins.
    #[must_use]
    pub fn identity(&self) -> &BuiltinCatalogIdentity {
        &self.0.identity
    }

    pub(crate) fn prepared(&self) -> PreparedProviderCatalog {
        self.0.prepared.clone()
    }
}

/// A Built-in Provider Tool's Execution Binding. It names the
/// Coral-owned Tool and carries no destination, credential, or routing data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuiltinExecutionBinding {
    /// The Tool Key. Another Coral version may name a Tool that
    /// this version does not perform.
    #[serde(rename = "tool")]
    tool: ToolKey,
    input_schema: SchemaId,
    output_schema: SchemaId,
}

impl BuiltinExecutionBinding {
    /// Returns the Coral-owned Tool this binding names, or `None` when
    /// another Coral version published a Tool this version does not
    /// perform.
    #[must_use]
    pub fn tool(&self) -> Option<BuiltinTool> {
        BuiltinTool::from_key(self.tool.as_str())
    }

    pub(crate) const fn tool_key(&self) -> &ToolKey {
        &self.tool
    }

    /// Returns the authoritative input-schema root.
    #[must_use]
    pub const fn input_schema(&self) -> &SchemaId {
        &self.input_schema
    }

    /// Returns the authoritative output-schema root.
    #[must_use]
    pub const fn output_schema(&self) -> &SchemaId {
        &self.output_schema
    }

    pub(crate) fn to_storage_json(&self) -> Result<String, Error> {
        serde_json::to_string(self).map_err(|_error| {
            Error::CorruptData("built-in Execution Binding cannot be encoded".to_string())
        })
    }

    pub(crate) fn from_storage_json(value: &str) -> Result<Self, Error> {
        serde_json::from_str(value).map_err(|_error| {
            Error::CorruptData("built-in Execution Binding is not valid canonical JSON".to_string())
        })
    }

    pub(crate) fn schema_ids(&self) -> impl Iterator<Item = &SchemaId> {
        [&self.input_schema, &self.output_schema].into_iter()
    }
}

/// One Built-in Tool's arguments, read against its input schema and
/// checked by every rule that needs no Workspace state.
#[derive(Debug, Clone)]
pub enum BuiltinArguments {
    /// `addProvider`'s arguments.
    AddProvider(Box<AddProviderArguments>),
    /// `listProviders` takes none.
    ListProviders,
    /// `getProvider`'s arguments.
    GetProvider(GetProviderArguments),
    /// `getPolicy`'s arguments.
    GetPolicy(GetPolicyArguments),
    /// `updatePolicy`'s arguments.
    UpdatePolicy(UpdatePolicyArguments),
    /// `updateProvider`'s arguments.
    UpdateProvider(Box<UpdateProviderArguments>),
}

impl BuiltinArguments {
    /// Reads `tool`'s arguments.
    ///
    /// # Errors
    ///
    /// Returns every argument to correct. A shape the schema forbids stops
    /// the reading at its first problem; the other checks each report theirs.
    pub fn parse(tool: BuiltinTool, arguments: &Value) -> Result<Self, Vec<ArgumentProblem>> {
        match tool {
            BuiltinTool::AddProvider => AddProviderArguments::parse(arguments)
                .map(|arguments| Self::AddProvider(Box::new(arguments))),
            BuiltinTool::ListProviders => {
                list_providers::parse(arguments).map(|()| Self::ListProviders)
            }
            BuiltinTool::GetProvider => {
                GetProviderArguments::parse(arguments).map(Self::GetProvider)
            }
            BuiltinTool::GetPolicy => GetPolicyArguments::parse(arguments).map(Self::GetPolicy),
            BuiltinTool::UpdatePolicy => {
                UpdatePolicyArguments::parse(arguments).map(Self::UpdatePolicy)
            }
            BuiltinTool::UpdateProvider => UpdateProviderArguments::parse(arguments)
                .map(|arguments| Self::UpdateProvider(Box::new(arguments))),
        }
    }
}

/// One argument a Program must correct. Coral writes each reason, and it never
/// holds a submitted value. A field or key name the Program wrote can appear
/// in the field and the reason, since it says what to correct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArgumentProblem {
    field: String,
    reason: String,
}

impl ArgumentProblem {
    fn new(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            reason: reason.into(),
        }
    }

    /// The argument's dotted path from the arguments object; empty for the
    /// object itself.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }

    /// What to correct.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// What to correct in a Provider Namespace that no Provider can hold.
const NAMESPACE_REASON: &str =
    "use a JavaScript identifier of at most 128 bytes, such as `weather`";

/// Reads `arguments` as `T`, or names the first argument whose shape is wrong.
fn read<T: DeserializeOwned>(arguments: &Value) -> Result<T, ArgumentProblem> {
    serde_path_to_error::deserialize(arguments).map_err(|error| {
        let field = error.path().to_string();
        let field = if field == "." { String::new() } else { field };
        ArgumentProblem::new(field, shape_reason(&error.into_inner().to_string()))
    })
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

/// Reads an optional argument that, when present, must have its type: the
/// schema has no `null` for an argument to omit.
fn present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

/// An object with no fields.
fn empty_object_schema() -> Value {
    json!({ "type": "object", "properties": {}, "additionalProperties": false })
}

/// The schema a Program is typed from, derived from `T` as Coral reads it
/// (`Contract::Deserialize`) or writes it (`Contract::Serialize`). It states
/// no defaults or titles: a description says what an omitted argument means.
///
/// schemars turns rustdoc into a `description`, and no transform can tell it
/// from text written for Programs. So a type or field whose rustdoc is not for
/// Programs sets `#[schemars(description = "")]`, and Program text goes in
/// `#[schemars(description = "...")]`, as in coral-mcp.
fn derived_schema<T: JsonSchema>(contract: Contract) -> Value {
    SchemaSettings::draft2020_12()
        .with(|settings| {
            settings.meta_schema = None;
            settings.contract = contract;
        })
        .with_transform(RecursiveTransform(|schema: &mut Schema| {
            schema.remove("default");
            schema.remove("title");
        }))
        .with_transform(RestrictFormats::default())
        .into_generator()
        .into_root_schema_for::<T>()
        .to_value()
}

/// The Tool Result a Built-in Tool returns, with Coral as the
/// Upstream Service.
fn tool_result(data: &impl Serialize) -> Value {
    json!({ "data": data, "metadata": { "kind": "builtin" } })
}

/// A Credential Route, as Built-in Tools spell it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BuiltinCredentialRoute {
    /// No Account.
    None,
    /// One shared Account.
    Fixed,
    /// Each caller's own Account.
    Caller,
}

impl JsonSchema for BuiltinCredentialRoute {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "CredentialRoute".into()
    }

    fn inline_schema() -> bool {
        true
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> Schema {
        words_schema(&Self::names())
    }
}

impl BuiltinCredentialRoute {
    const ALL: [Self; 3] = [Self::None, Self::Fixed, Self::Caller];

    /// The route's name, as a Program spells it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Fixed => "fixed",
            Self::Caller => "caller",
        }
    }

    /// Each route's name, for a schema's `enum`.
    fn names() -> [&'static str; 3] {
        Self::ALL.map(Self::name)
    }
}

/// Each way Coral sends a Provider's credentials, in the order a Program reads them.
const AUTHENTICATION_KINDS: [AuthenticationKind; 7] = [
    AuthenticationKind::None,
    AuthenticationKind::Bearer,
    AuthenticationKind::HeaderCredentials,
    AuthenticationKind::BasicAuth,
    AuthenticationKind::QueryCredentials,
    AuthenticationKind::OAuthDeviceCode,
    AuthenticationKind::OAuthAuthorizationCode,
];

/// The Program's name for each method, as the `addProvider` input spells
/// it. Stored configuration has its own encoding, which never becomes the
/// Program contract.
const fn authentication_name(kind: AuthenticationKind) -> &'static str {
    match kind {
        AuthenticationKind::None => "none",
        AuthenticationKind::Bearer => "bearer",
        AuthenticationKind::HeaderCredentials => "header_credentials",
        AuthenticationKind::BasicAuth => "basic_auth",
        AuthenticationKind::QueryCredentials => "query_credentials",
        AuthenticationKind::OAuthDeviceCode => "oauth_device_code",
        AuthenticationKind::OAuthAuthorizationCode => "oauth_authorization_code",
    }
}

/// A Tool Access State, as Built-in Tools spell it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BuiltinAccessState {
    /// A call runs without approval.
    Enabled,
    /// A call waits for a person to approve it.
    ApprovalRequired,
    /// A call is refused.
    Disabled,
}

impl JsonSchema for BuiltinAccessState {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ToolAccessState".into()
    }

    fn inline_schema() -> bool {
        true
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> Schema {
        words_schema(&["enabled", "approval_required", "disabled"])
    }
}

/// A string that is one of `words`.
fn words_schema(words: &[&str]) -> Schema {
    schemars::json_schema!({ "type": "string", "enum": words })
}

/// Each method, by the name a Program reads.
fn authentication_schema(_generator: &mut schemars::SchemaGenerator) -> Schema {
    words_schema(&AUTHENTICATION_KINDS.map(authentication_name))
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde's serialize_with passes a reference"
)]
fn serialize_authentication<S: Serializer>(
    kind: &AuthenticationKind,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(authentication_name(*kind))
}

/// What `addProvider`, `updateProvider` and `listProviders` report about a Provider: how to
/// call it and whether the Program needs a person first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(deny_unknown_fields, description = "")]
pub struct ProviderFacts {
    /// The Provider display name.
    #[schemars(description = "The Provider display name.")]
    pub display_name: String,
    /// Whose Account Invocations use.
    #[schemars(description = "Whose account Invocations use.")]
    pub credential_route: BuiltinCredentialRoute,
    /// How Coral sends the Provider's credentials.
    #[serde(serialize_with = "serialize_authentication")]
    #[schemars(
        schema_with = "authentication_schema",
        description = "How Coral sends the Provider's credentials."
    )]
    pub authentication: AuthenticationKind,
    /// How many of its Tools the caller can discover and call.
    #[schemars(description = "How many of the Provider's Tools you can discover and call.")]
    #[serde(rename = "tools")]
    pub tools: usize,
    /// Whether the caller must connect an Account before calling it.
    #[schemars(
        description = "True when you must connect your own Account before your calls can use the \
                       Provider. Your first call then asks you to connect it."
    )]
    pub account_required: bool,
}

struct Definition {
    key: &'static str,
    summary: &'static str,
    presentation: ToolPresentation,
    hints: ToolHints,
    input: Value,
    output: Value,
}

fn definition(tool: BuiltinTool) -> Definition {
    match tool {
        BuiltinTool::AddProvider => add_provider::definition(),
        BuiltinTool::ListProviders => list_providers::definition(),
        BuiltinTool::GetProvider => get_provider::definition(),
        BuiltinTool::GetPolicy => get_policy::definition(),
        BuiltinTool::UpdatePolicy => update_policy::definition(),
        BuiltinTool::UpdateProvider => update_provider::definition(),
    }
}

/// Prepares the complete Built-in Provider catalog package of `definitions`.
fn prepare(definitions: Vec<Definition>) -> Result<PreparedProviderCatalog, CatalogDiagnostic> {
    let mut schemas = SchemaGraphAccumulator::new(SchemaSemantics::JsonSchema2020_12);
    let mut drafts = Vec::new();
    for definition in definitions {
        let input_schema = schemas
            .add_embedded_schema(&schema_source(definition.key, "input"), &definition.input)?;
        let output_schema = schemas
            .add_embedded_schema(&schema_source(definition.key, "output"), &definition.output)?;
        let binding = BuiltinExecutionBinding {
            tool: ToolKey::parse(definition.key)
                .map_err(|_error| invalid("a built-in Tool Key is invalid"))?,
            input_schema,
            output_schema,
        };
        drafts.push((definition, binding));
    }
    let schema_graph = schemas.finish()?;
    let schema_graph_json = schema_graph
        .to_storage_json()
        .map_err(|_error| invalid("the built-in schema graph cannot be encoded"))?;
    let type_names = typescript_names(&schema_graph);
    let mut shared_types = SharedTypes::default();
    let mut tools = Vec::with_capacity(drafts.len());
    for (definition, binding) in drafts {
        let key = binding.tool_key().clone();
        let relative_path = definition.key.to_string();
        let (declaration, type_references) =
            project(&binding, &schema_graph, &type_names, &mut shared_types)?;
        if crate::types::tool_description_too_large(
            &relative_path,
            BUILTIN_PROVIDER_DISPLAY_NAME,
            &declaration,
            &definition.presentation,
        )
        .is_some()
        {
            return Err(invalid(
                "a built-in Tool description exceeds the discovery limit",
            ));
        }
        let execution_binding = ExecutionBinding::Builtin(binding.clone());
        tools.push(MaterializedTool::new(
            key,
            relative_path,
            definition.summary.to_string(),
            definition.presentation,
            definition.hints,
            execution_binding,
            binding
                .to_storage_json()
                .map_err(|_error| invalid("a built-in Execution Binding cannot be encoded"))?,
            binding.input_schema().clone(),
            encode(&definition.input)?,
            encode(&definition.output)?,
            declaration,
            type_references,
            Vec::new(),
            "[]".to_string(),
        ));
    }
    Ok(PreparedProviderCatalog::new(
        BUILTIN_PROVIDER_DISPLAY_NAME.to_string(),
        schema_graph,
        schema_graph_json,
        tools,
        shared_types,
    ))
}

/// Names every schema of the built-in graph for projection.
pub(crate) fn typescript_names(graph: &SchemaGraph) -> TypeScriptNames {
    TypeScriptNames::new(graph, ProjectionOptions::builtin())
}

fn project(
    binding: &BuiltinExecutionBinding,
    graph: &SchemaGraph,
    type_names: &TypeScriptNames,
    shared: &mut SharedTypes,
) -> Result<(String, Vec<String>), CatalogDiagnostic> {
    let mut projector = TypeScriptProjector::new(graph, type_names, ProjectionOptions::builtin());
    let input = projector.project(binding.input_schema(), Direction::Input)?;
    let output = projector.project(binding.output_schema(), Direction::Output)?;
    let type_references = projector.share_named_declarations(shared)?;
    // A closed object is wider in structural TypeScript; only an error rejects.
    if projector
        .into_diagnostics()
        .iter()
        .any(|diagnostic| diagnostic.severity() == crate::CatalogDiagnosticSeverity::Error)
    {
        return Err(invalid("a built-in schema cannot be projected faithfully"));
    }
    let declaration = render_callable_declaration(
        &format!("input: {input}"),
        &format!("Promise<{{ data: {output}; metadata: {{ kind: \"builtin\" }} }}>"),
    );
    Ok((declaration, type_references))
}

fn schema_source(key: &str, direction: &str) -> String {
    format!("builtin:{key}:{direction}")
}

fn encode(schema: &Value) -> Result<String, CatalogDiagnostic> {
    serde_json::to_string(&canonicalize(schema))
        .map_err(|_error| invalid("a built-in schema cannot be encoded"))
}

fn invalid(message: &'static str) -> CatalogDiagnostic {
    CatalogDiagnostic::stable("invalid_builtin_catalog", message)
}

#[cfg(test)]
mod tests;
