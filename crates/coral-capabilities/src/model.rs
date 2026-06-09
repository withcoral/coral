use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Generator version for source capability artifacts produced by this binary.
pub const SOURCE_CAPABILITY_GENERATOR_VERSION: &str = "derive-capabilities-v13";

/// Result type for capability validation and construction.
pub type Result<T> = std::result::Result<T, CapabilityError>;

/// Capability model errors.
#[derive(Debug, thiserror::Error)]
pub enum CapabilityError {
    /// A capability artifact violates a semantic invariant.
    #[error("{0}")]
    Validation(String),
}

impl CapabilityError {
    /// Build a validation error.
    #[must_use]
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }
}

/// Immutable app-assigned installed source id.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourceId(pub String);

impl SourceId {
    /// Returns the source id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Stable capability id.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CapabilityId(pub String);

impl CapabilityId {
    /// Creates the canonical id for one source/interface/operation tuple.
    #[must_use]
    pub fn new(source_id: &SourceId, interface_id: &str, operation_id: &str) -> Self {
        Self(format!(
            "source/{}/interface/{interface_id}/operation/{operation_id}",
            source_id.as_str()
        ))
    }

    /// Returns the capability id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CapabilityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Source capability set artifact.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceCapabilitySet {
    pub artifact_schema_version: u32,
    pub source_id: SourceId,
    pub generator_version: String,
    pub capabilities: Vec<Capability>,
    pub diagnostics: Vec<Diagnostic>,
}

impl SourceCapabilitySet {
    /// Creates a schema-version-1 capability set.
    #[must_use]
    pub fn new(source_id: SourceId, capabilities: Vec<Capability>) -> Self {
        Self {
            artifact_schema_version: 1,
            source_id,
            generator_version: SOURCE_CAPABILITY_GENERATOR_VERSION.to_string(),
            capabilities,
            diagnostics: Vec::new(),
        }
    }

    /// Validate stable identity and projection separation invariants.
    ///
    /// # Errors
    ///
    /// Returns [`CapabilityError`] when the set references another source,
    /// repeats a capability id, or leaks downstream projection references.
    pub fn validate(&self) -> Result<()> {
        if self.artifact_schema_version != 1 {
            return Err(CapabilityError::validation(
                "SourceCapabilitySet artifact_schema_version must be 1",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for capability in &self.capabilities {
            if capability.source_id != self.source_id {
                return Err(CapabilityError::validation(format!(
                    "capability '{}' source_id '{}' does not match set source_id '{}'",
                    capability.capability_id, capability.source_id, self.source_id
                )));
            }
            if !seen.insert(capability.capability_id.clone()) {
                return Err(CapabilityError::validation(format!(
                    "duplicate capability id '{}'",
                    capability.capability_id
                )));
            }
            capability.validate_projection_separation()?;
        }
        Ok(())
    }
}

/// One provider-neutral capability.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Capability {
    pub capability_id: CapabilityId,
    pub source_id: SourceId,
    pub interface_id: String,
    pub operation_id: String,
    pub provider_origin: ProviderOrigin,
    pub display: CapabilityDisplay,
    pub effect_profile: EffectProfile,
    pub input_schema: InvocationSchema,
    pub output_contract: OutputContract,
    pub shape_hints: ShapeHints,
    pub credential_requirements: CredentialRequirementSet,
    pub upstream_binding: UpstreamBinding,
    pub diagnostics: Vec<Diagnostic>,
}

impl Capability {
    /// Build a capability with the canonical capability id format.
    #[must_use]
    pub fn new(
        source_id: SourceId,
        interface_id: impl Into<String>,
        operation_id: impl Into<String>,
        provider_origin: ProviderOrigin,
        upstream_binding: UpstreamBinding,
    ) -> Self {
        let interface_id = interface_id.into();
        let operation_id = operation_id.into();
        let capability_id = CapabilityId::new(&source_id, &interface_id, &operation_id);
        Self {
            capability_id,
            source_id,
            interface_id,
            operation_id,
            provider_origin,
            display: CapabilityDisplay::default(),
            effect_profile: EffectProfile::unknown_action(),
            input_schema: InvocationSchema::object(Map::new(), Vec::new(), false),
            output_contract: OutputContract::Unknown,
            shape_hints: ShapeHints::unknown(),
            credential_requirements: CredentialRequirementSet::anonymous(),
            upstream_binding,
            diagnostics: Vec::new(),
        }
    }

    fn validate_projection_separation(&self) -> Result<()> {
        let haystack = serde_json::to_string(self)
            .map_err(|error| CapabilityError::validation(error.to_string()))?;
        for forbidden in ["sql_table:", "sql_function:", "typescript:", "coral.sql"] {
            if haystack.contains(forbidden) {
                return Err(CapabilityError::validation(format!(
                    "capability '{}' leaks projection or MCP serving reference '{forbidden}'",
                    self.capability_id
                )));
            }
        }
        Ok(())
    }
}

/// Provider origin pointer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderOrigin {
    pub kind: ProviderOriginKind,
    pub snapshot_ref: String,
    pub provider_name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

/// Provider origin kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderOriginKind {
    RestOperation,
    McpTool,
    GraphqlRootField,
    FileRelation,
}

/// Display metadata copied into exports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityDisplay {
    pub title: String,
    pub description: String,
    #[serde(default)]
    pub deprecated: bool,
    #[serde(default)]
    pub support_status: SupportStatus,
}

impl Default for CapabilityDisplay {
    fn default() -> Self {
        Self {
            title: String::new(),
            description: String::new(),
            deprecated: false,
            support_status: SupportStatus::Generated,
        }
    }
}

/// Capability support status.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportStatus {
    #[default]
    Generated,
    GeneratedPartial,
    PartiallySupported,
    Unsupported,
    Deprecated,
}

/// Effect metadata used for discovery, planning, and mutation gating.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectProfile {
    pub capability_kind: CapabilityKind,
    pub effects: Vec<EffectKind>,
    pub idempotency: IdempotencyKind,
    pub entity: Option<String>,
    pub response_trust: ResponseTrust,
}

impl EffectProfile {
    /// Default read profile.
    #[must_use]
    pub fn read() -> Self {
        Self {
            capability_kind: CapabilityKind::Query,
            effects: vec![EffectKind::Read],
            idempotency: IdempotencyKind::Idempotent,
            entity: None,
            response_trust: ResponseTrust::UntrustedProviderData,
        }
    }

    /// Default write profile.
    #[must_use]
    pub fn write() -> Self {
        Self {
            capability_kind: CapabilityKind::Mutation,
            effects: vec![EffectKind::Write],
            idempotency: IdempotencyKind::Unknown,
            entity: None,
            response_trust: ResponseTrust::UntrustedProviderData,
        }
    }

    /// Default delete profile.
    #[must_use]
    pub fn delete() -> Self {
        Self {
            capability_kind: CapabilityKind::Mutation,
            effects: vec![EffectKind::Delete],
            idempotency: IdempotencyKind::NonIdempotent,
            entity: None,
            response_trust: ResponseTrust::UntrustedProviderData,
        }
    }

    /// Default action profile.
    #[must_use]
    pub fn unknown_action() -> Self {
        Self {
            capability_kind: CapabilityKind::Action,
            effects: vec![EffectKind::Unknown],
            idempotency: IdempotencyKind::Unknown,
            entity: None,
            response_trust: ResponseTrust::UntrustedProviderData,
        }
    }
}

/// Capability class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityKind {
    Query,
    Mutation,
    Action,
}

/// Side-effect class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectKind {
    Read,
    Write,
    Delete,
    Unknown,
}

/// Idempotency class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyKind {
    Idempotent,
    NonIdempotent,
    Unknown,
}

/// Provider-output trust class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseTrust {
    UntrustedProviderData,
    TrustedCoralMetadata,
    Empty,
    Unknown,
}

/// Coral invocation schema dialect.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InvocationSchema {
    pub schema_version: u32,
    pub schema: Value,
    pub diagnostics: Vec<Diagnostic>,
}

impl InvocationSchema {
    /// Creates an invocation schema from a JSON Schema value.
    #[must_use]
    pub fn new(schema: Value) -> Self {
        Self {
            schema_version: 1,
            schema,
            diagnostics: Vec::new(),
        }
    }

    /// Creates a strict or open object schema.
    #[must_use]
    pub fn object(properties: Map<String, Value>, required: Vec<String>, open: bool) -> Self {
        Self::new(Value::Object(Map::from_iter([
            ("type".to_string(), Value::String("object".to_string())),
            ("properties".to_string(), Value::Object(properties)),
            (
                "required".to_string(),
                Value::Array(required.into_iter().map(Value::String).collect()),
            ),
            ("additionalProperties".to_string(), Value::Bool(open)),
        ])))
    }
}

/// Output contract for callable invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutputContract {
    Single { schema: InvocationSchema },
    RestResponseVariants { variants: Vec<RestOutputVariant> },
    McpStructuredContent { schema: Option<InvocationSchema> },
    GraphqlData { schema: InvocationSchema },
    Unknown,
}

/// REST output variant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RestOutputVariant {
    pub status: StatusRange,
    pub media_type: String,
    pub schema: InvocationSchema,
    pub provider_origin: String,
}

/// HTTP status or status range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StatusRange {
    Code { code: u16 },
    Range { start: u16, end: u16 },
    Default,
}

/// Shape hints used by projection derivation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShapeHints {
    pub result_shape: ResultShapeHint,
    pub stable_output_shape: bool,
    pub row_path_candidates: Vec<Vec<String>>,
    pub pagination_hint: Option<PaginationHint>,
}

impl ShapeHints {
    /// Unknown shape.
    #[must_use]
    pub fn unknown() -> Self {
        Self {
            result_shape: ResultShapeHint::Unknown,
            stable_output_shape: false,
            row_path_candidates: Vec::new(),
            pagination_hint: None,
        }
    }

    /// List-shaped rows at the provider root.
    #[must_use]
    pub fn root_list() -> Self {
        Self {
            result_shape: ResultShapeHint::List,
            stable_output_shape: true,
            row_path_candidates: vec![Vec::new()],
            pagination_hint: None,
        }
    }

    /// List-shaped rows at a nested provider response path.
    #[must_use]
    pub fn list_at_path(path: Vec<String>) -> Self {
        Self {
            result_shape: ResultShapeHint::List,
            stable_output_shape: true,
            row_path_candidates: vec![path],
            pagination_hint: None,
        }
    }

    /// Singleton-shaped row at the provider root.
    #[must_use]
    pub fn root_singleton() -> Self {
        Self {
            result_shape: ResultShapeHint::Singleton,
            stable_output_shape: true,
            row_path_candidates: vec![Vec::new()],
            pagination_hint: None,
        }
    }

    /// Singleton-shaped row at a nested provider response path.
    #[must_use]
    pub fn singleton_at_path(path: Vec<String>) -> Self {
        Self {
            result_shape: ResultShapeHint::Singleton,
            stable_output_shape: true,
            row_path_candidates: vec![path],
            pagination_hint: None,
        }
    }
}

/// Result shape hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultShapeHint {
    List,
    Singleton,
    MutationResult,
    Empty,
    Unknown,
}

/// Pagination hint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaginationHint {
    pub kind: PaginationKind,
    pub cursor_arg: Option<String>,
    pub cursor_path: Option<Vec<String>>,
    pub page_size_arg: Option<String>,
}

/// Pagination family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaginationKind {
    Cursor,
    OffsetLimit,
    RelayConnection,
    SinglePage,
    Unknown,
}

/// Provider execution binding facts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UpstreamBinding {
    Rest(RestUpstreamBinding),
    McpTool(McpToolUpstreamBinding),
    Graphql(GraphqlOperationBinding),
    FileRead(FileScanBinding),
}

/// REST execution binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RestUpstreamBinding {
    pub operation_ref: String,
    pub method: HttpMethod,
    pub path_template: String,
    pub parameter_bindings: Vec<RestParameterBinding>,
    pub request_bodies: Vec<RestRequestBody>,
    pub responses: Vec<RestResponseVariant>,
    pub pagination: Option<PaginationHint>,
}

/// HTTP method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    Get,
    Head,
    Options,
    Post,
    Put,
    Patch,
    Delete,
}

impl HttpMethod {
    /// Default effect profile for the HTTP method.
    #[must_use]
    pub fn default_effect_profile(self) -> EffectProfile {
        match self {
            Self::Get | Self::Head | Self::Options => EffectProfile::read(),
            Self::Post | Self::Put | Self::Patch => EffectProfile::write(),
            Self::Delete => EffectProfile::delete(),
        }
    }

    /// Lowercase method token (`get`, `post`, ...).
    #[must_use]
    pub const fn as_lowercase_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Head => "head",
            Self::Options => "options",
            Self::Post => "post",
            Self::Put => "put",
            Self::Patch => "patch",
            Self::Delete => "delete",
        }
    }

    /// Parses a method from its lowercase token.
    #[must_use]
    pub fn from_lowercase(value: &str) -> Option<Self> {
        match value {
            "get" => Some(Self::Get),
            "head" => Some(Self::Head),
            "options" => Some(Self::Options),
            "post" => Some(Self::Post),
            "put" => Some(Self::Put),
            "patch" => Some(Self::Patch),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }
}

/// REST parameter location.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestParameterLocation {
    Path,
    Query,
    Header,
    Cookie,
}

/// REST parameter binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestParameterBinding {
    pub name: String,
    pub location: RestParameterLocation,
    pub required: bool,
    pub style: String,
    pub explode: bool,
    pub allow_reserved: bool,
}

/// REST request body media type binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RestRequestBody {
    pub media_type: String,
    pub required: bool,
    pub schema: InvocationSchema,
}

/// REST response variant binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RestResponseVariant {
    pub status: StatusRange,
    pub media_type: String,
    pub schema: InvocationSchema,
}

/// MCP tool execution binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpToolUpstreamBinding {
    pub server_ref: String,
    pub tool_name: String,
    pub task_support: McpTaskSupport,
}

/// MCP task support imported as a fact only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpTaskSupport {
    Forbidden,
    Advertised,
    Unknown,
}

/// GraphQL operation binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphqlOperationBinding {
    pub endpoint_ref: String,
    pub operation_name: String,
    pub graphql_operation_kind: GraphqlOperationKind,
    pub document_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection_set: Option<String>,
    pub variable_bindings: Vec<GraphqlVariableBinding>,
    pub response_path: Vec<String>,
}

/// GraphQL operation kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphqlOperationKind {
    Query,
    Mutation,
    Subscription,
}

impl GraphqlOperationKind {
    /// Lowercase GraphQL operation keyword (`query`, `mutation`, `subscription`).
    #[must_use]
    pub const fn as_keyword(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Mutation => "mutation",
            Self::Subscription => "subscription",
        }
    }
}

/// GraphQL variable binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphqlVariableBinding {
    pub variable_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graphql_type: Option<String>,
    pub argument_path: Vec<String>,
    pub required: bool,
}

/// File-backed read binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileScanBinding {
    pub file_refs: Vec<FileArtifactRef>,
    pub format: FileFormatDescriptor,
    pub schema_ref: Option<String>,
}

/// Source-local file artifact reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileArtifactRef {
    pub id: String,
    pub source_local_path: String,
    pub display_name: Option<String>,
}

/// File format descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FileFormatDescriptor {
    Json,
    Jsonl,
    Parquet,
    Csv,
}

/// Credential requirements for a capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialRequirementSet {
    pub alternatives: Vec<CredentialRequirementAlternative>,
}

impl CredentialRequirementSet {
    /// Anonymous access.
    #[must_use]
    pub fn anonymous() -> Self {
        Self {
            alternatives: vec![CredentialRequirementAlternative {
                requirements: Vec::new(),
                anonymous: true,
            }],
        }
    }
}

/// One `OR` alternative. Every requirement inside the alternative is `ANDed`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialRequirementAlternative {
    pub requirements: Vec<CredentialRequirement>,
    pub anonymous: bool,
}

/// One credential slot requirement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialRequirement {
    pub scheme_id: String,
    pub scopes: Vec<String>,
    pub source_input_key: Option<String>,
}

/// Layer/stage diagnostic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: String,
    pub severity: DiagnosticSeverity,
    pub stage: DiagnosticStage,
    pub source_id: Option<SourceId>,
    pub interface_id: Option<String>,
    pub capability_id: Option<CapabilityId>,
    pub message: String,
    pub source_ref: Option<String>,
    #[serde(default)]
    pub details: Value,
}

impl Diagnostic {
    /// Creates a diagnostic with empty details.
    #[must_use]
    pub fn new(
        code: impl Into<String>,
        severity: DiagnosticSeverity,
        stage: DiagnosticStage,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            severity,
            stage,
            source_id: None,
            interface_id: None,
            capability_id: None,
            message: message.into(),
            source_ref: None,
            details: Value::Null,
        }
    }
}

/// Diagnostic severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Info,
    Warning,
    Error,
}

/// Diagnostic stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticStage {
    SourceSpec,
    ProviderImport,
    CapabilityGeneration,
    ExportGeneration,
    SqlProjection,
    Materialization,
    Runtime,
}
