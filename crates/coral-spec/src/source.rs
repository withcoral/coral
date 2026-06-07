//! `SourceSpec` parser for `spec_version: 1`, `kind: source`.

#![allow(
    missing_docs,
    reason = "SourceSpec contract fields are documented in the generated schema and source-spec reference."
)]
#![allow(
    clippy::module_name_repetitions,
    reason = "SourceSpec model names intentionally carry interface context."
)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    ManifestCredentialMethod, ManifestCredentialMethodKind, ManifestCredentialSpec, ManifestError,
    ManifestInputKind, ManifestInputSpec, ManifestOAuthClientIdSpec, ManifestOAuthClientSecretSpec,
    ManifestOAuthClientSecretTransport, ManifestOAuthClientSpec, ManifestOAuthCredentialSpec,
    ManifestOAuthFlowKind, ManifestOAuthFlowSpec, ManifestOAuthPkceMode,
    ManifestOAuthRedirectUriPortMode, ManifestOAuthScopeDelimiter, ManifestOAuthScopeSpec,
    ManifestOAuthScopesSpec, ParsedTemplate, Result, TemplateNamespace,
};

const RESERVED_INPUT_KEY_PREFIXES: &[&str] = &["__coral"];

#[derive(Debug, Clone, PartialEq)]
pub struct SourceSpec {
    pub spec_version: u32,
    pub kind: SourceSpecKind,
    pub name: String,
    pub description: String,
    pub test_queries: Vec<String>,
    pub inputs: Vec<ManifestInputSpec>,
    pub interfaces: Vec<SourceInterface>,
    pub declared_inputs: Vec<ManifestInputSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceSpecKind {
    Source,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SourceInterface {
    OpenApi(OpenApiInterface),
    Mcp(McpInterface),
    Graphql(GraphqlInterface),
    File(FileInterface),
}

impl SourceInterface {
    #[must_use]
    pub fn id(&self) -> &str {
        match self {
            Self::OpenApi(interface) => &interface.id,
            Self::Mcp(interface) => &interface.id,
            Self::Graphql(interface) => &interface.id,
            Self::File(interface) => &interface.id,
        }
    }

    #[must_use]
    pub fn interface_type(&self) -> &'static str {
        match self {
            Self::OpenApi(_) => "openapi",
            Self::Mcp(_) => "mcp",
            Self::Graphql(_) => "graphql",
            Self::File(_) => "file",
        }
    }

    fn inputs(&self) -> &[ManifestInputSpec] {
        match self {
            Self::OpenApi(interface) => &interface.inputs,
            Self::Mcp(interface) => &interface.inputs,
            Self::Graphql(interface) => &interface.inputs,
            Self::File(interface) => &interface.inputs,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpenApiInterface {
    pub id: String,
    pub descriptor: OpenApiDescriptor,
    pub base_url: Option<ParsedTemplate>,
    pub auth: Option<AuthDescriptor>,
    pub inputs: Vec<ManifestInputSpec>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OpenApiDescriptor {
    Url { url: ParsedTemplate },
    File { file: PathBuf },
}

#[derive(Debug, Clone, PartialEq)]
pub struct McpInterface {
    pub id: String,
    pub server: McpServerDescriptor,
    pub inputs: Vec<ManifestInputSpec>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct McpServerDescriptor {
    pub transport: McpTransportDescriptor,
    pub auth: Option<AuthDescriptor>,
    pub env: Vec<McpEnvBinding>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AuthDescriptor {
    BearerInput { key: String },
    HeaderInput { name: String, key: String },
    Headers { headers: Vec<AuthHeaderDescriptor> },
    None,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuthHeaderDescriptor {
    pub name: String,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpEnvBinding {
    pub name: String,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum McpTransportDescriptor {
    Stdio { command: String, args: Vec<String> },
    StreamableHttp { url: ParsedTemplate },
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphqlInterface {
    pub id: String,
    pub endpoint: ParsedTemplate,
    pub schema: GraphqlSchemaDescriptor,
    pub auth: Option<AuthDescriptor>,
    pub inputs: Vec<ManifestInputSpec>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GraphqlSchemaDescriptor {
    SdlUrl { url: String },
    SdlFile { file: PathBuf },
    IntrospectionJsonUrl { url: String },
    IntrospectionJsonFile { file: PathBuf },
    IntrospectionQuery { endpoint: Option<ParsedTemplate> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileInterface {
    pub id: String,
    pub files: Vec<PathBuf>,
    pub format: FileFormatDescriptor,
    pub inputs: Vec<ManifestInputSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormatDescriptor {
    Json,
    Jsonl,
    Parquet,
    Csv,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawSourceSpec {
    spec_version: u32,
    kind: RawSourceSpecKind,
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    test_queries: Vec<String>,
    #[serde(default)]
    inputs: Vec<RawInput>,
    interfaces: Vec<RawInterface>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum RawSourceSpecKind {
    Source,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawInput {
    key: String,
    kind: RawInputKind,
    #[serde(default = "default_true")]
    required: bool,
    #[serde(default)]
    default: String,
    #[serde(default)]
    allowed_values: Vec<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    hint: Option<String>,
    #[serde(default)]
    credential: Option<RawCredential>,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum RawInputKind {
    Variable,
    Secret,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawCredential {
    methods: Vec<RawCredentialMethod>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawCredentialMethod {
    #[serde(rename = "type")]
    kind: RawCredentialMethodKind,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    hint: Option<String>,
    #[serde(default)]
    oauth: Option<RawOAuthCredential>,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum RawCredentialMethodKind {
    SourceConfig,
    Oauth,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawOAuthCredential {
    flow: RawOAuthFlow,
    #[serde(default)]
    redirect_uri: Option<String>,
    #[serde(default = "default_oauth_redirect_uri_port_mode")]
    redirect_uri_port_mode: RawOAuthRedirectUriPortMode,
    endpoints: RawOAuthEndpoints,
    client: RawOAuthClient,
    #[serde(default)]
    scopes: Option<RawOAuthScopes>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawOAuthFlow {
    #[serde(rename = "type")]
    kind: RawOAuthFlowKind,
    pkce: RawOAuthPkceMode,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum RawOAuthFlowKind {
    AuthorizationCode,
    DeviceCode,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum RawOAuthPkceMode {
    Required,
    Disabled,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum RawOAuthRedirectUriPortMode {
    Fixed,
    Random,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[expect(
    clippy::struct_field_names,
    reason = "Raw field names intentionally mirror SourceSpec endpoint keys."
)]
struct RawOAuthEndpoints {
    #[serde(default)]
    authorization_url: Option<String>,
    #[serde(default)]
    device_authorization_url: Option<String>,
    token_url: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawOAuthClient {
    id: RawOAuthClientId,
    #[serde(default)]
    secret: Option<RawOAuthClientSecret>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawOAuthClientId {
    #[serde(default)]
    default: Option<String>,
    #[serde(default)]
    input: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawOAuthClientSecret {
    input: String,
    transport: RawOAuthClientSecretTransport,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum RawOAuthClientSecretTransport {
    BasicAuth,
    RequestBody,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawOAuthScopes {
    scope: RawOAuthScope,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawOAuthScope {
    delimiter: RawOAuthScopeDelimiter,
    values: Vec<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum RawOAuthScopeDelimiter {
    Space,
    Comma,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum RawInterface {
    #[serde(rename = "openapi")]
    OpenApi {
        id: String,
        #[serde(default)]
        url: Option<ParsedTemplate>,
        #[serde(default)]
        file: Option<PathBuf>,
        #[serde(default)]
        base_url: Option<ParsedTemplate>,
        #[serde(default)]
        auth: Option<RawAuth>,
        #[serde(default)]
        inputs: Vec<RawInput>,
    },
    Mcp {
        id: String,
        server: RawMcpServer,
        #[serde(default)]
        inputs: Vec<RawInput>,
    },
    Graphql {
        id: String,
        endpoint: ParsedTemplate,
        schema: RawGraphqlSchema,
        #[serde(default)]
        auth: Option<RawAuth>,
        #[serde(default)]
        inputs: Vec<RawInput>,
    },
    File {
        id: String,
        files: Vec<PathBuf>,
        format: RawFileFormat,
        #[serde(default)]
        inputs: Vec<RawInput>,
    },
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawMcpServer {
    transport: RawMcpTransport,
    #[serde(default)]
    auth: Option<RawAuth>,
    #[serde(default)]
    env: Vec<RawMcpEnvBinding>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum RawMcpTransport {
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
    },
    StreamableHttp {
        url: ParsedTemplate,
    },
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawMcpEnvBinding {
    name: String,
    key: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum RawAuth {
    BearerInput { key: String },
    HeaderInput { name: String, key: String },
    Headers { headers: Vec<RawAuthHeader> },
    None,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawAuthHeader {
    name: String,
    key: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum RawGraphqlSchema {
    SdlUrl {
        url: String,
    },
    SdlFile {
        file: PathBuf,
    },
    IntrospectionJsonUrl {
        url: String,
    },
    IntrospectionJsonFile {
        file: PathBuf,
    },
    IntrospectionQuery {
        #[serde(default)]
        endpoint: Option<ParsedTemplate>,
    },
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum RawFileFormat {
    Json,
    Jsonl,
    Parquet,
    Csv,
}

/// Generate the JSON Schema for replacement `SourceSpec` manifests.
///
/// # Panics
///
/// Panics only if the schema produced by `schemars` cannot be serialized to
/// JSON, which would indicate an invalid schema type definition in this crate.
pub fn generated_source_spec_schema() -> Value {
    let mut schema = serde_json::to_value(schemars::schema_for!(RawSourceSpec))
        .expect("generated SourceSpec schema must serialize");
    post_process_generated_schema(&mut schema);
    schema
}

fn post_process_generated_schema(schema: &mut Value) {
    let Some(root) = schema.as_object_mut() else {
        return;
    };
    root.insert(
        "$id".to_string(),
        Value::String("https://coral.local/source_spec.schema.json".to_string()),
    );
    root.insert(
        "title".to_string(),
        Value::String("Coral SourceSpec".to_string()),
    );
    root.entry("$schema".to_string()).or_insert_with(|| {
        Value::String("https://json-schema.org/draft/2020-12/schema".to_string())
    });

    if let Some(properties) = root.get_mut("properties").and_then(Value::as_object_mut) {
        if let Some(spec_version) = properties
            .get_mut("spec_version")
            .and_then(Value::as_object_mut)
        {
            spec_version.insert("const".to_string(), json!(1));
        }
        if let Some(kind) = properties.get_mut("kind").and_then(Value::as_object_mut) {
            kind.insert("const".to_string(), json!("source"));
        }
        if let Some(name) = properties.get_mut("name").and_then(Value::as_object_mut) {
            name.insert("minLength".to_string(), json!(1));
        }
        if let Some(test_queries) = properties
            .get_mut("test_queries")
            .and_then(Value::as_object_mut)
            && let Some(items) = test_queries.get_mut("items").and_then(Value::as_object_mut)
        {
            items.insert("minLength".to_string(), json!(1));
        }
        if let Some(interfaces) = properties
            .get_mut("interfaces")
            .and_then(Value::as_object_mut)
        {
            interfaces.insert("minItems".to_string(), json!(1));
        }
    }
}

impl SourceSpec {
    pub(crate) fn parse_value(value: Value) -> Result<Self> {
        let raw: RawSourceSpec =
            serde_json::from_value(value).map_err(ManifestError::deserialize)?;
        let RawSourceSpec {
            spec_version,
            kind,
            name,
            description,
            test_queries,
            inputs,
            interfaces,
        } = raw;
        match kind {
            RawSourceSpecKind::Source => {}
        }
        if spec_version != 1 {
            return Err(ManifestError::validation(format!(
                "source '{name}' declares spec_version {spec_version}; expected 1"
            )));
        }
        if interfaces.is_empty() {
            return Err(ManifestError::validation(format!(
                "source '{name}' must declare at least one interface"
            )));
        }
        validate_test_queries(&name, &test_queries)?;
        let top_inputs = parse_inputs(&name, "source", &inputs)?;
        let mut input_by_key = top_inputs
            .iter()
            .map(|input| (input.key.clone(), input.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut parsed_interfaces = Vec::with_capacity(interfaces.len());
        let mut seen_interface_ids = BTreeSet::new();
        for interface in interfaces {
            let parsed = parse_interface(&name, interface)?;
            validate_interface_id(&name, parsed.id())?;
            if !seen_interface_ids.insert(parsed.id().to_string()) {
                return Err(ManifestError::validation(format!(
                    "source '{name}' declares duplicate interface id '{}'",
                    parsed.id()
                )));
            }
            merge_inputs(&name, parsed.id(), parsed.inputs(), &mut input_by_key)?;
            parsed_interfaces.push(parsed);
        }
        let declared_inputs = input_by_key.into_values().collect::<Vec<_>>();
        for interface in &parsed_interfaces {
            validate_interface_references(&name, interface, &declared_inputs)?;
        }
        Ok(Self {
            spec_version,
            kind: SourceSpecKind::Source,
            name,
            description,
            test_queries,
            inputs: top_inputs,
            interfaces: parsed_interfaces,
            declared_inputs,
        })
    }
}

fn validate_test_queries(source_name: &str, test_queries: &[String]) -> Result<()> {
    for (index, query) in test_queries.iter().enumerate() {
        if query.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' test_queries[{index}] must not be empty"
            )));
        }
    }
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "Interface parsing keeps provider variant validation in one discriminated match."
)]
fn parse_interface(source_name: &str, raw: RawInterface) -> Result<SourceInterface> {
    match raw {
        RawInterface::OpenApi {
            id,
            url,
            file,
            base_url,
            auth,
            inputs,
        } => {
            let inputs = parse_inputs(source_name, &format!("interface '{id}'"), &inputs)?;
            let descriptor = match (url, file) {
                (Some(url), None) => OpenApiDescriptor::Url { url },
                (None, Some(file)) => OpenApiDescriptor::File { file },
                (Some(_), Some(_)) | (None, None) => {
                    return Err(ManifestError::validation(format!(
                        "source '{source_name}' OpenAPI interface '{id}' must declare exactly one of url or file"
                    )));
                }
            };
            Ok(SourceInterface::OpenApi(OpenApiInterface {
                id,
                descriptor,
                base_url,
                auth: auth.map(AuthDescriptor::from),
                inputs,
            }))
        }
        RawInterface::Mcp { id, server, inputs } => {
            let inputs = parse_inputs(source_name, &format!("interface '{id}'"), &inputs)?;
            let transport = match server.transport {
                RawMcpTransport::Stdio { command, args } => {
                    if command.trim().is_empty() {
                        return Err(ManifestError::validation(format!(
                            "source '{source_name}' MCP stdio command must not be empty"
                        )));
                    }
                    McpTransportDescriptor::Stdio { command, args }
                }
                RawMcpTransport::StreamableHttp { url } => {
                    McpTransportDescriptor::StreamableHttp { url }
                }
            };
            Ok(SourceInterface::Mcp(McpInterface {
                id,
                server: McpServerDescriptor {
                    transport,
                    auth: server.auth.map(AuthDescriptor::from),
                    env: server
                        .env
                        .into_iter()
                        .map(|env| McpEnvBinding {
                            name: env.name,
                            key: env.key,
                        })
                        .collect(),
                },
                inputs,
            }))
        }
        RawInterface::Graphql {
            id,
            endpoint,
            schema,
            auth,
            inputs,
        } => {
            let inputs = parse_inputs(source_name, &format!("interface '{id}'"), &inputs)?;
            let schema = match schema {
                RawGraphqlSchema::SdlUrl { url } => GraphqlSchemaDescriptor::SdlUrl { url },
                RawGraphqlSchema::SdlFile { file } => GraphqlSchemaDescriptor::SdlFile { file },
                RawGraphqlSchema::IntrospectionJsonUrl { url } => {
                    GraphqlSchemaDescriptor::IntrospectionJsonUrl { url }
                }
                RawGraphqlSchema::IntrospectionJsonFile { file } => {
                    GraphqlSchemaDescriptor::IntrospectionJsonFile { file }
                }
                RawGraphqlSchema::IntrospectionQuery { endpoint } => {
                    GraphqlSchemaDescriptor::IntrospectionQuery { endpoint }
                }
            };
            Ok(SourceInterface::Graphql(GraphqlInterface {
                id,
                endpoint,
                schema,
                auth: auth.map(AuthDescriptor::from),
                inputs,
            }))
        }
        RawInterface::File {
            id,
            files,
            format,
            inputs,
        } => {
            if files.is_empty() {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' file interface '{id}' must declare at least one file"
                )));
            }
            let inputs = parse_inputs(source_name, &format!("interface '{id}'"), &inputs)?;
            Ok(SourceInterface::File(FileInterface {
                id,
                files,
                format: match format {
                    RawFileFormat::Json => FileFormatDescriptor::Json,
                    RawFileFormat::Jsonl => FileFormatDescriptor::Jsonl,
                    RawFileFormat::Parquet => FileFormatDescriptor::Parquet,
                    RawFileFormat::Csv => FileFormatDescriptor::Csv,
                },
                inputs,
            }))
        }
    }
}

impl From<RawAuth> for AuthDescriptor {
    fn from(value: RawAuth) -> Self {
        match value {
            RawAuth::BearerInput { key } => Self::BearerInput { key },
            RawAuth::HeaderInput { name, key } => Self::HeaderInput { name, key },
            RawAuth::Headers { headers } => Self::Headers {
                headers: headers
                    .into_iter()
                    .map(|header| AuthHeaderDescriptor {
                        name: header.name,
                        key: header.key,
                    })
                    .collect(),
            },
            RawAuth::None => Self::None,
        }
    }
}

fn parse_inputs(
    source_name: &str,
    owner: &str,
    inputs: &[RawInput],
) -> Result<Vec<ManifestInputSpec>> {
    let mut seen = BTreeSet::new();
    inputs
        .iter()
        .map(|input| {
            validate_input_key(source_name, owner, &input.key)?;
            if !seen.insert(input.key.as_str()) {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' {owner} declares duplicate input '{}'",
                    input.key
                )));
            }
            let kind = match input.kind {
                RawInputKind::Variable => ManifestInputKind::Variable,
                RawInputKind::Secret => ManifestInputKind::Secret,
            };
            if kind == ManifestInputKind::Secret && !input.default.is_empty() {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' {owner} secret input '{}' must not declare a default",
                    input.key
                )));
            }
            validate_input_allowed_values(source_name, owner, input, kind)?;
            if kind == ManifestInputKind::Variable && credential_like_input_key(&input.key) {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' {owner} input '{}' looks credential-like and must use kind: secret",
                    input.key
                )));
            }
            let credential =
                parse_credential(source_name, owner, &input.key, kind, input.credential.as_ref())?;
            Ok(ManifestInputSpec {
                key: input.key.clone(),
                kind,
                required: input.required,
                default_value: input.default.clone(),
                allowed_values: input.allowed_values.clone(),
                hint: input.hint.clone().or_else(|| input.label.clone()),
                credential,
            })
        })
        .collect()
}

fn validate_input_allowed_values(
    source_name: &str,
    owner: &str,
    input: &RawInput,
    kind: ManifestInputKind,
) -> Result<()> {
    if input.allowed_values.is_empty() {
        return Ok(());
    }
    if kind == ManifestInputKind::Secret {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {owner} secret input '{}' must not declare allowed_values",
            input.key
        )));
    }
    let mut seen = BTreeSet::new();
    for value in &input.allowed_values {
        if value.is_empty() {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' {owner} input '{}' allowed_values must not contain an empty value",
                input.key
            )));
        }
        if !seen.insert(value.as_str()) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' {owner} input '{}' declares duplicate allowed value '{value}'",
                input.key
            )));
        }
    }
    if !input.default.is_empty()
        && !input
            .allowed_values
            .iter()
            .any(|allowed| allowed == &input.default)
    {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {owner} input '{}' default must be one of allowed_values",
            input.key
        )));
    }
    Ok(())
}

fn parse_credential(
    source_name: &str,
    owner: &str,
    input_key: &str,
    input_kind: ManifestInputKind,
    raw: Option<&RawCredential>,
) -> Result<Option<ManifestCredentialSpec>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    if input_kind != ManifestInputKind::Secret {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {owner} input '{input_key}' credential methods must use kind: secret"
        )));
    }
    if raw.methods.is_empty() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {owner} input '{input_key}' credential must declare at least one method"
        )));
    }
    let methods = raw
        .methods
        .iter()
        .enumerate()
        .map(|(index, method)| {
            parse_credential_method(source_name, owner, input_key, index, method)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(ManifestCredentialSpec { methods }))
}

fn parse_credential_method(
    source_name: &str,
    owner: &str,
    input_key: &str,
    index: usize,
    method: &RawCredentialMethod,
) -> Result<ManifestCredentialMethod> {
    let (kind, oauth) = match method.kind {
        RawCredentialMethodKind::SourceConfig => {
            if method.oauth.is_some() {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' {owner} input '{input_key}' credential method {index} has type source_config and must not declare oauth"
                )));
            }
            (ManifestCredentialMethodKind::SourceConfig, None)
        }
        RawCredentialMethodKind::Oauth => {
            let oauth = method.oauth.as_ref().ok_or_else(|| {
                ManifestError::validation(format!(
                    "source '{source_name}' {owner} input '{input_key}' credential method {index} has type oauth and must declare oauth"
                ))
            })?;
            (
                ManifestCredentialMethodKind::OAuth,
                Some(parse_oauth_credential(
                    source_name,
                    owner,
                    input_key,
                    index,
                    oauth,
                )?),
            )
        }
    };
    Ok(ManifestCredentialMethod {
        kind,
        label: method.label.clone(),
        description: method.description.clone(),
        hint: method.hint.clone(),
        oauth,
    })
}

fn parse_oauth_credential(
    source_name: &str,
    owner: &str,
    input_key: &str,
    method_index: usize,
    raw: &RawOAuthCredential,
) -> Result<ManifestOAuthCredentialSpec> {
    let client_id =
        parse_oauth_client_id(source_name, owner, input_key, method_index, &raw.client.id)?;
    let client_secret = raw
        .client
        .secret
        .as_ref()
        .map(|secret| {
            parse_oauth_client_secret(source_name, owner, input_key, method_index, secret)
        })
        .transpose()?;
    let scopes = raw
        .scopes
        .as_ref()
        .map(|scopes| parse_oauth_scopes(source_name, owner, input_key, method_index, scopes))
        .transpose()?;
    let oauth = ManifestOAuthCredentialSpec {
        flow: ManifestOAuthFlowSpec {
            kind: match raw.flow.kind {
                RawOAuthFlowKind::AuthorizationCode => ManifestOAuthFlowKind::AuthorizationCode,
                RawOAuthFlowKind::DeviceCode => ManifestOAuthFlowKind::DeviceCode,
            },
            pkce: match raw.flow.pkce {
                RawOAuthPkceMode::Required => ManifestOAuthPkceMode::Required,
                RawOAuthPkceMode::Disabled => ManifestOAuthPkceMode::Disabled,
            },
        },
        redirect_uri: raw.redirect_uri.clone(),
        redirect_uri_port_mode: match raw.redirect_uri_port_mode {
            RawOAuthRedirectUriPortMode::Fixed => ManifestOAuthRedirectUriPortMode::Fixed,
            RawOAuthRedirectUriPortMode::Random => ManifestOAuthRedirectUriPortMode::Random,
        },
        authorization_url: raw.endpoints.authorization_url.clone(),
        device_authorization_url: raw.endpoints.device_authorization_url.clone(),
        token_url: raw.endpoints.token_url.clone(),
        client: ManifestOAuthClientSpec {
            id: client_id,
            secret: client_secret,
        },
        scopes,
    };
    validate_oauth_credential(source_name, owner, input_key, method_index, &oauth)?;
    Ok(oauth)
}

fn parse_oauth_client_id(
    source_name: &str,
    owner: &str,
    input_key: &str,
    method_index: usize,
    raw: &RawOAuthClientId,
) -> Result<ManifestOAuthClientIdSpec> {
    validate_optional_non_empty_oauth_field(
        source_name,
        owner,
        input_key,
        method_index,
        "client id default",
        raw.default.as_deref(),
    )?;
    if let Some(input) = raw.input.as_deref() {
        validate_credential_input_key(source_name, owner, input_key, method_index, input)?;
    }
    if raw.default.as_deref().unwrap_or_default().is_empty()
        && raw.input.as_deref().unwrap_or_default().is_empty()
    {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {owner} input '{input_key}' oauth credential method {method_index} must declare client.id.default or client.id.input"
        )));
    }
    Ok(ManifestOAuthClientIdSpec {
        default: raw.default.clone(),
        input: raw.input.clone(),
    })
}

fn parse_oauth_client_secret(
    source_name: &str,
    owner: &str,
    input_key: &str,
    method_index: usize,
    raw: &RawOAuthClientSecret,
) -> Result<ManifestOAuthClientSecretSpec> {
    validate_credential_input_key(source_name, owner, input_key, method_index, &raw.input)?;
    Ok(ManifestOAuthClientSecretSpec {
        input: raw.input.clone(),
        transport: match raw.transport {
            RawOAuthClientSecretTransport::BasicAuth => {
                ManifestOAuthClientSecretTransport::BasicAuth
            }
            RawOAuthClientSecretTransport::RequestBody => {
                ManifestOAuthClientSecretTransport::RequestBody
            }
        },
    })
}

fn parse_oauth_scopes(
    source_name: &str,
    owner: &str,
    input_key: &str,
    method_index: usize,
    raw: &RawOAuthScopes,
) -> Result<ManifestOAuthScopesSpec> {
    if raw.scope.values.is_empty() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {owner} input '{input_key}' oauth credential method {method_index} scopes.scope.values must not be empty"
        )));
    }
    for scope in &raw.scope.values {
        if scope.trim().is_empty() || scope.trim() != scope {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' {owner} input '{input_key}' oauth credential method {method_index} scope value '{scope}' must not be empty or contain leading/trailing whitespace"
            )));
        }
    }
    Ok(ManifestOAuthScopesSpec {
        scope: ManifestOAuthScopeSpec {
            delimiter: match raw.scope.delimiter {
                RawOAuthScopeDelimiter::Space => ManifestOAuthScopeDelimiter::Space,
                RawOAuthScopeDelimiter::Comma => ManifestOAuthScopeDelimiter::Comma,
            },
            values: raw.scope.values.clone(),
        },
    })
}

fn validate_oauth_credential(
    source_name: &str,
    owner: &str,
    input_key: &str,
    method_index: usize,
    oauth: &ManifestOAuthCredentialSpec,
) -> Result<()> {
    validate_required_oauth_url(
        source_name,
        owner,
        input_key,
        method_index,
        "token_url",
        &oauth.token_url,
    )?;
    match oauth.flow.kind {
        ManifestOAuthFlowKind::AuthorizationCode => {
            validate_required_oauth_url(
                source_name,
                owner,
                input_key,
                method_index,
                "authorization_url",
                oauth.authorization_url.as_deref().unwrap_or_default(),
            )?;
            oauth.redirect_bind_port().map_err(|error| {
                ManifestError::validation(format!(
                    "source '{source_name}' {owner} input '{input_key}' oauth credential method {method_index}: {error}"
                ))
            })?;
        }
        ManifestOAuthFlowKind::DeviceCode => {
            validate_required_oauth_url(
                source_name,
                owner,
                input_key,
                method_index,
                "device_authorization_url",
                oauth
                    .device_authorization_url
                    .as_deref()
                    .unwrap_or_default(),
            )?;
            if oauth.client.secret.is_some() {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' {owner} input '{input_key}' oauth credential method {method_index} device_code flow must not declare client.secret"
                )));
            }
        }
    }
    Ok(())
}

fn validate_required_oauth_url(
    source_name: &str,
    owner: &str,
    input_key: &str,
    method_index: usize,
    field: &str,
    value: &str,
) -> Result<()> {
    if value.trim().is_empty() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {owner} input '{input_key}' oauth credential method {method_index} is missing {field}"
        )));
    }
    Ok(())
}

fn validate_optional_non_empty_oauth_field(
    source_name: &str,
    owner: &str,
    input_key: &str,
    method_index: usize,
    field: &str,
    value: Option<&str>,
) -> Result<()> {
    if let Some(value) = value
        && (value.trim().is_empty() || value.trim() != value)
    {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {owner} input '{input_key}' oauth credential method {method_index} {field} must not be empty or contain leading/trailing whitespace"
        )));
    }
    Ok(())
}

fn validate_credential_input_key(
    source_name: &str,
    owner: &str,
    input_key: &str,
    method_index: usize,
    value: &str,
) -> Result<()> {
    validate_input_key(
        source_name,
        &format!("{owner} input '{input_key}' oauth credential method {method_index}"),
        value,
    )
}

fn validate_input_key(source_name: &str, owner: &str, value: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {owner} input key must not be empty"
        )));
    }
    if trimmed != value {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {owner} input key '{value}' must not contain leading or trailing whitespace"
        )));
    }
    if trimmed.contains('/') || trimmed.contains('\\') {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {owner} input key '{value}' must not contain '/' or '\\\\'"
        )));
    }
    if trimmed.contains('=') || trimmed.contains('\n') || trimmed.contains('\r') {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {owner} input key '{value}' must not contain '=', '\\n', or '\\r'"
        )));
    }
    if trimmed.starts_with('#') {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {owner} input key '{value}' must not start with '#'"
        )));
    }
    if let Some(prefix) = RESERVED_INPUT_KEY_PREFIXES
        .iter()
        .find(|prefix| trimmed.starts_with(**prefix))
    {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {owner} input key '{value}' must not start with reserved prefix '{prefix}'"
        )));
    }
    Ok(())
}

fn credential_like_input_key(key: &str) -> bool {
    const MARKERS: &[&str] = &[
        "API_KEY",
        "APPLICATION_KEY",
        "ACCESS_KEY",
        "ACCESS_KEY_ID",
        "ACCESS_TOKEN",
        "ADMIN_KEY",
        "AUTHORIZATION",
        "BEARER_TOKEN",
        "CLIENT_SECRET",
        "PASSWORD",
        "PRIVATE_KEY",
        "READ_KEY",
        "SECRET",
        "TOKEN",
    ];

    let key = key.to_ascii_uppercase();
    MARKERS.iter().any(|marker| {
        key == *marker
            || key.contains(&format!("_{marker}_"))
            || key.ends_with(&format!("_{marker}"))
            || key.starts_with(&format!("{marker}_"))
    })
}

fn merge_inputs(
    source_name: &str,
    interface_id: &str,
    inputs: &[ManifestInputSpec],
    input_by_key: &mut BTreeMap<String, ManifestInputSpec>,
) -> Result<()> {
    for input in inputs {
        if let Some(existing) = input_by_key.get(&input.key) {
            if existing != input {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' interface '{interface_id}' declares incompatible input '{}'",
                    input.key
                )));
            }
        } else {
            input_by_key.insert(input.key.clone(), input.clone());
        }
    }
    Ok(())
}

fn validate_interface_references(
    source_name: &str,
    interface: &SourceInterface,
    inputs: &[ManifestInputSpec],
) -> Result<()> {
    let input_kind_by_key = inputs
        .iter()
        .map(|input| (input.key.as_str(), input.kind))
        .collect::<BTreeMap<_, _>>();
    match interface {
        SourceInterface::OpenApi(interface) => {
            validate_openapi_interface_references(source_name, interface, &input_kind_by_key)
        }
        SourceInterface::Mcp(interface) => {
            validate_mcp_interface_references(source_name, interface, &input_kind_by_key)
        }
        SourceInterface::Graphql(interface) => {
            validate_graphql_interface_references(source_name, interface, &input_kind_by_key)
        }
        SourceInterface::File(_) => Ok(()),
    }
}

fn validate_declared_input_ref(
    source_name: &str,
    interface_id: &str,
    input_kind_by_key: &BTreeMap<&str, ManifestInputKind>,
    key: &str,
    context: &str,
) -> Result<()> {
    if input_kind_by_key.contains_key(key) {
        Ok(())
    } else {
        Err(ManifestError::validation(format!(
            "source '{source_name}' interface '{interface_id}' {context} references undeclared input '{key}'"
        )))
    }
}

fn validate_secret_input_ref(
    source_name: &str,
    interface_id: &str,
    input_kind_by_key: &BTreeMap<&str, ManifestInputKind>,
    key: &str,
    context: &str,
) -> Result<()> {
    match input_kind_by_key.get(key) {
        Some(ManifestInputKind::Secret) => Ok(()),
        Some(ManifestInputKind::Variable) => Err(ManifestError::validation(format!(
            "source '{source_name}' interface '{interface_id}' {context} must reference secret input '{key}'"
        ))),
        None => Err(ManifestError::validation(format!(
            "source '{source_name}' interface '{interface_id}' {context} references undeclared input '{key}'"
        ))),
    }
}

fn validate_openapi_interface_references(
    source_name: &str,
    interface: &OpenApiInterface,
    input_kind_by_key: &BTreeMap<&str, ManifestInputKind>,
) -> Result<()> {
    if let OpenApiDescriptor::Url { url } = &interface.descriptor {
        validate_provider_url_template(
            source_name,
            &interface.id,
            "OpenAPI url",
            url,
            input_kind_by_key,
        )?;
    }
    if let Some(base_url) = &interface.base_url {
        validate_provider_url_template(
            source_name,
            &interface.id,
            "OpenAPI base_url",
            base_url,
            input_kind_by_key,
        )?;
    }
    let mut check_secret_key = |key: &str, context: &str| {
        validate_secret_input_ref(source_name, &interface.id, input_kind_by_key, key, context)
    };
    validate_auth_ref(interface.auth.as_ref(), &mut check_secret_key)
}

fn validate_mcp_interface_references(
    source_name: &str,
    interface: &McpInterface,
    input_kind_by_key: &BTreeMap<&str, ManifestInputKind>,
) -> Result<()> {
    if let McpTransportDescriptor::StreamableHttp { url } = &interface.server.transport {
        validate_provider_url_template(
            source_name,
            &interface.id,
            "MCP Streamable HTTP url",
            url,
            input_kind_by_key,
        )?;
    }
    let mut check_secret_key = |key: &str, context: &str| {
        validate_secret_input_ref(source_name, &interface.id, input_kind_by_key, key, context)
    };
    validate_auth_ref(interface.server.auth.as_ref(), &mut check_secret_key)?;
    for env in &interface.server.env {
        validate_declared_input_ref(
            source_name,
            &interface.id,
            input_kind_by_key,
            &env.key,
            "env",
        )?;
    }
    Ok(())
}

fn validate_graphql_interface_references(
    source_name: &str,
    interface: &GraphqlInterface,
    input_kind_by_key: &BTreeMap<&str, ManifestInputKind>,
) -> Result<()> {
    validate_provider_url_template(
        source_name,
        &interface.id,
        "GraphQL endpoint",
        &interface.endpoint,
        input_kind_by_key,
    )?;
    match &interface.schema {
        GraphqlSchemaDescriptor::SdlUrl { url } => {
            validate_https_url_literal(source_name, &interface.id, "GraphQL SDL url", url)?;
        }
        GraphqlSchemaDescriptor::IntrospectionJsonUrl { url } => {
            validate_https_url_literal(
                source_name,
                &interface.id,
                "GraphQL introspection JSON url",
                url,
            )?;
        }
        GraphqlSchemaDescriptor::IntrospectionQuery {
            endpoint: Some(endpoint),
        } => validate_provider_url_template(
            source_name,
            &interface.id,
            "GraphQL introspection endpoint",
            endpoint,
            input_kind_by_key,
        )?,
        GraphqlSchemaDescriptor::SdlFile { .. }
        | GraphqlSchemaDescriptor::IntrospectionJsonFile { .. }
        | GraphqlSchemaDescriptor::IntrospectionQuery { endpoint: None } => {}
    }
    let mut check_secret_key = |key: &str, context: &str| {
        validate_secret_input_ref(source_name, &interface.id, input_kind_by_key, key, context)
    };
    validate_auth_ref(interface.auth.as_ref(), &mut check_secret_key)
}

fn validate_auth_ref(
    auth: Option<&AuthDescriptor>,
    check_key: &mut impl FnMut(&str, &str) -> Result<()>,
) -> Result<()> {
    match auth {
        Some(AuthDescriptor::BearerInput { key } | AuthDescriptor::HeaderInput { key, .. }) => {
            check_key(key, "auth")
        }
        Some(AuthDescriptor::Headers { headers }) => {
            for header in headers {
                check_key(&header.key, "auth")?;
            }
            Ok(())
        }
        Some(AuthDescriptor::None) | None => Ok(()),
    }
}

fn validate_interface_id(source_name: &str, id: &str) -> Result<()> {
    let mut chars = id.chars();
    let valid = matches!(chars.next(), Some(ch) if ch.is_ascii_lowercase())
        && chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_');
    if valid {
        Ok(())
    } else {
        Err(ManifestError::validation(format!(
            "source '{source_name}' interface id '{id}' must match [a-z][a-z0-9_]*"
        )))
    }
}

fn validate_provider_url_template(
    source_name: &str,
    interface_id: &str,
    label: &str,
    template: &ParsedTemplate,
    input_kind_by_key: &BTreeMap<&str, ManifestInputKind>,
) -> Result<()> {
    if template.tokens().next().is_none() {
        return validate_https_url_literal(source_name, interface_id, label, template.raw());
    }
    if !template.raw().starts_with("https://") {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' interface '{interface_id}' {label} URL templates must start with https://"
        )));
    }
    for token in template.tokens() {
        if token.namespace() != &TemplateNamespace::Input {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' interface '{interface_id}' {label} URL template token '{}' must use the input namespace",
                token.raw()
            )));
        }
        if token.default_value().is_some() {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' interface '{interface_id}' {label} URL template token '{}' must declare defaults under top-level inputs",
                token.raw()
            )));
        }
        match input_kind_by_key.get(token.key()) {
            Some(ManifestInputKind::Variable) => {}
            Some(ManifestInputKind::Secret) => {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' interface '{interface_id}' {label} URL template must reference variable input '{}', not a secret",
                    token.key()
                )));
            }
            None => {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' interface '{interface_id}' {label} URL template references undeclared input '{}'",
                    token.key()
                )));
            }
        }
    }
    Ok(())
}

fn validate_https_url_literal(
    source_name: &str,
    interface_id: &str,
    label: &str,
    raw: &str,
) -> Result<()> {
    if raw.contains("{{") || raw.contains("}}") {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' interface '{interface_id}' {label} must be a literal URL; template tokens are not supported for provider URLs"
        )));
    }
    let parsed = url::Url::parse(raw).map_err(|error| {
        ManifestError::validation(format!(
            "source '{source_name}' interface '{interface_id}' {label} URL is invalid: {error}"
        ))
    })?;
    if descriptor_url_is_allowed(&parsed) {
        return Ok(());
    }
    Err(ManifestError::validation(format!(
        "source '{source_name}' interface '{interface_id}' {label} must use https, except localhost development URLs"
    )))
}

fn descriptor_url_is_allowed(url: &url::Url) -> bool {
    match url.scheme() {
        "https" => true,
        "http" => match url.host() {
            Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
            Some(url::Host::Ipv4(address)) => address.is_loopback(),
            Some(url::Host::Ipv6(address)) => address.is_loopback(),
            None => false,
        },
        _ => false,
    }
}

fn default_true() -> bool {
    true
}

fn default_oauth_redirect_uri_port_mode() -> RawOAuthRedirectUriPortMode {
    RawOAuthRedirectUriPortMode::Fixed
}

#[cfg(test)]
mod tests {
    use crate::{
        AuthDescriptor, ManifestCredentialMethodKind, ManifestOAuthFlowKind, ManifestOAuthPkceMode,
        ManifestOAuthRedirectUriPortMode, ManifestOAuthScopeDelimiter, SourceInterface,
        parse_source_manifest_yaml,
    };

    #[test]
    fn parses_multi_interface_source_spec() {
        let manifest = parse_source_manifest_yaml(
            r#"
spec_version: 1
kind: source
name: github
inputs:
  - key: github_token
    kind: secret
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
    auth:
      kind: bearer_input
      key: github_token
  - id: mcp
    type: mcp
    server:
      transport:
        type: streamable_http
        url: https://mcp.example.com/mcp
  - id: graph
    type: graphql
    endpoint: https://api.example.com/graphql
    schema:
      kind: introspection_query
  - id: files
    type: file
    files: ["./issues.jsonl"]
    format:
      kind: jsonl
"#,
        )
        .expect("parse source spec");
        assert_eq!(manifest.spec_version, 1);
        assert_eq!(manifest.interfaces.len(), 4);
        assert!(matches!(
            manifest.interfaces.first().expect("first interface"),
            SourceInterface::OpenApi(_)
        ));
        assert_eq!(manifest.declared_inputs.len(), 1);
    }

    #[test]
    fn parses_secret_oauth_credential_methods() {
        let manifest = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: slack
inputs:
  - key: SLACK_USER_TOKEN
    kind: secret
    label: Slack user token
    credential:
      methods:
        - type: oauth
          label: Connect with Slack
          description: Authorize Slack with OAuth and store a user token.
          hint: Authorize in your browser.
          oauth:
            flow:
              type: authorization_code
              pkce: required
            redirect_uri: http://localhost:53682/oauth/callback
            redirect_uri_port_mode: fixed
            endpoints:
              authorization_url: https://slack.com/oauth/v2_user/authorize
              token_url: https://slack.com/api/oauth.v2.user.access
            client:
              id:
                default: '6057250636981.7381814187793'
                input: SLACK_OAUTH_CLIENT_ID
            scopes:
              scope:
                delimiter: comma
                values:
                  - search:read.public
                  - search:read.private
        - type: source_config
          label: Paste user token
interfaces:
  - id: mcp
    type: mcp
    server:
      transport:
        type: streamable_http
        url: https://mcp.slack.com/mcp
      auth:
        kind: bearer_input
        key: SLACK_USER_TOKEN
",
        )
        .expect("parse source spec");

        let input = manifest
            .declared_inputs
            .iter()
            .find(|input| input.key == "SLACK_USER_TOKEN")
            .expect("slack token input");
        let credential = input.credential.as_ref().expect("credential methods");
        assert_eq!(credential.methods.len(), 2);
        let oauth_method = credential.methods.first().expect("oauth method");
        let source_config_method = credential.methods.get(1).expect("source config method");
        assert_eq!(oauth_method.kind, ManifestCredentialMethodKind::OAuth);
        assert_eq!(
            source_config_method.kind,
            ManifestCredentialMethodKind::SourceConfig
        );
        let oauth = oauth_method.oauth.as_ref().expect("oauth config");
        assert_eq!(oauth.flow.kind, ManifestOAuthFlowKind::AuthorizationCode);
        assert_eq!(oauth.flow.pkce, ManifestOAuthPkceMode::Required);
        assert_eq!(
            oauth.redirect_uri.as_deref(),
            Some("http://localhost:53682/oauth/callback")
        );
        assert_eq!(
            oauth.redirect_uri_port_mode,
            ManifestOAuthRedirectUriPortMode::Fixed
        );
        assert_eq!(
            oauth.client.id.default.as_deref(),
            Some("6057250636981.7381814187793")
        );
        assert_eq!(
            oauth.client.id.input.as_deref(),
            Some("SLACK_OAUTH_CLIENT_ID")
        );
        let scopes = oauth.scopes.as_ref().expect("scopes");
        assert_eq!(scopes.scope.delimiter, ManifestOAuthScopeDelimiter::Comma);
        assert_eq!(
            scopes.scope.values.first().map(String::as_str),
            Some("search:read.public")
        );
    }

    #[test]
    fn rejects_unknown_contract_keys_in_source_spec() {
        let raw = "\
spec_version: 1
kind: source
name: bad
unused_key: 3
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
";
        let error = parse_source_manifest_yaml(raw).expect_err("unknown key rejected");
        assert!(error.to_string().contains("unused_key"));
    }

    #[test]
    fn auth_must_reference_declared_input() {
        let error = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: bad
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
    auth:
      kind: bearer_input
      key: missing
",
        )
        .expect_err("missing input rejected");
        assert!(error.to_string().contains("undeclared input"));
    }

    #[test]
    fn auth_must_reference_secret_input() {
        let error = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: bad
inputs:
  - key: account_id
    kind: variable
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
    auth:
      kind: bearer_input
      key: account_id
",
        )
        .expect_err("variable auth input rejected");
        assert!(error.to_string().contains("must reference secret input"));
    }

    #[test]
    fn parses_multi_header_auth() {
        let manifest = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: datadog
inputs:
  - key: DD_API_KEY
    kind: secret
  - key: DD_APPLICATION_KEY
    kind: secret
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
    auth:
      kind: headers
      headers:
        - name: DD-API-KEY
          key: DD_API_KEY
        - name: DD-APPLICATION-KEY
          key: DD_APPLICATION_KEY
",
        )
        .expect("parse source spec");
        let SourceInterface::OpenApi(interface) =
            manifest.interfaces.first().expect("first interface")
        else {
            panic!("expected openapi interface");
        };
        assert!(matches!(
            interface.auth.as_ref(),
            Some(AuthDescriptor::Headers { headers }) if headers.len() == 2
        ));
    }

    #[test]
    fn multi_header_auth_must_reference_secret_inputs() {
        let error = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: bad
inputs:
  - key: DD_API_KEY
    kind: secret
  - key: account_id
    kind: variable
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
    auth:
      kind: headers
      headers:
        - name: DD-API-KEY
          key: DD_API_KEY
        - name: Account-ID
          key: account_id
",
        )
        .expect_err("variable auth input rejected");
        assert!(error.to_string().contains("must reference secret input"));
    }

    #[test]
    fn secret_inputs_must_not_declare_defaults() {
        let error = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: bad
inputs:
  - key: api_token
    kind: secret
    default: abc123
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
",
        )
        .expect_err("secret default rejected");
        assert!(error.to_string().contains("must not declare a default"));
    }

    #[test]
    fn parses_variable_allowed_values() {
        let manifest = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: datadog
inputs:
  - key: DD_SITE
    kind: variable
    default: datadoghq.com
    allowed_values:
      - datadoghq.com
      - datadoghq.eu
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
    base_url: https://api.{{input.DD_SITE}}
",
        )
        .expect("allowed values should parse");
        let input = manifest
            .declared_inputs
            .iter()
            .find(|input| input.key == "DD_SITE")
            .expect("DD_SITE input");
        assert_eq!(
            input.allowed_values,
            vec!["datadoghq.com".to_string(), "datadoghq.eu".to_string()]
        );
    }

    #[test]
    fn variable_default_must_be_in_allowed_values() {
        let error = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: bad
inputs:
  - key: DD_SITE
    kind: variable
    default: datadoghq.com
    allowed_values:
      - datadoghq.eu
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
    base_url: https://api.{{input.DD_SITE}}
",
        )
        .expect_err("default outside allowed_values should fail");
        assert!(error.to_string().contains("default must be one of"));
    }

    #[test]
    fn credential_like_variables_must_be_secret_inputs() {
        let error = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: bad
inputs:
  - key: service_api_key
    kind: variable
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
",
        )
        .expect_err("credential-like variable rejected");
        assert!(error.to_string().contains("looks credential-like"));
    }

    #[test]
    fn credential_methods_must_be_declared_on_secret_inputs() {
        let error = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: bad
inputs:
  - key: workspace
    kind: variable
    credential:
      methods:
        - type: source_config
interfaces:
  - id: mcp
    type: mcp
    server:
      transport:
        type: streamable_http
        url: https://mcp.example.com/mcp
",
        )
        .expect_err("variable credential methods rejected");
        assert!(
            error
                .to_string()
                .contains("credential methods must use kind: secret")
        );
    }

    #[test]
    fn oauth_credential_method_requires_oauth_config() {
        let error = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: bad
inputs:
  - key: API_TOKEN
    kind: secret
    credential:
      methods:
        - type: oauth
interfaces:
  - id: mcp
    type: mcp
    server:
      transport:
        type: streamable_http
        url: https://mcp.example.com/mcp
      auth:
        kind: bearer_input
        key: API_TOKEN
",
        )
        .expect_err("oauth method without config rejected");
        assert!(error.to_string().contains("must declare oauth"));
    }

    #[test]
    fn input_keys_must_not_use_reserved_prefixes() {
        let error = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: bad
inputs:
  - key: __coral_api_token
    kind: secret
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
",
        )
        .expect_err("reserved input rejected");
        assert!(error.to_string().contains("reserved prefix"));
    }

    #[test]
    fn openapi_base_url_rejects_remote_http() {
        let error = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: bad
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
    base_url: http://api.example.com
",
        )
        .expect_err("remote HTTP base_url rejected");
        let message = error.to_string();
        assert!(message.contains("OpenAPI base_url"));
        assert!(message.contains("must use https"));
    }

    #[test]
    fn provider_url_templates_may_reference_variables() {
        parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: datadog
inputs:
  - key: DD_SITE
    kind: variable
    default: datadoghq.com
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
    base_url: https://api.{{input.DD_SITE}}
  - id: graph
    type: graphql
    endpoint: https://api.{{input.DD_SITE}}/graphql
    schema:
      kind: introspection_query
  - id: mcp
    type: mcp
    server:
      transport:
        type: streamable_http
        url: https://mcp.{{input.DD_SITE}}/api/unstable/mcp-server/mcp
",
        )
        .expect("provider URL templates parse");
    }

    #[test]
    fn provider_url_templates_reject_secret_inputs() {
        let error = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: bad
inputs:
  - key: API_TOKEN
    kind: secret
interfaces:
  - id: mcp
    type: mcp
    server:
      transport:
        type: streamable_http
        url: https://{{input.API_TOKEN}}.example.com/mcp
",
        )
        .expect_err("secret URL input rejected");
        assert!(error.to_string().contains("not a secret"));
    }

    #[test]
    fn provider_url_templates_reject_remote_http() {
        let error = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: bad
inputs:
  - key: HOST
    kind: variable
interfaces:
  - id: mcp
    type: mcp
    server:
      transport:
        type: streamable_http
        url: http://{{input.HOST}}/mcp
",
        )
        .expect_err("remote HTTP URL template rejected");
        assert!(error.to_string().contains("must start with https://"));
    }

    #[test]
    fn localhost_lookalike_urls_are_rejected() {
        let error = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: bad
interfaces:
  - id: mcp
    type: mcp
    server:
      transport:
        type: streamable_http
        url: http://localhost.evil.example/mcp
",
        )
        .expect_err("localhost lookalike should fail");
        assert!(error.to_string().contains("must use https"));
    }
}
