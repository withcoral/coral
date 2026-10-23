use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::schema::SharedTypes;
use crate::{
    Error, ExecutionBinding, McpDiscoverySnapshot, OpenApiDocument, OperationPathAssignment,
};

macro_rules! uuid_id {
    ($name:ident, $label:literal) => {
        #[doc = $label]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(Uuid);

        impl $name {
            pub(crate) fn generate() -> Self {
                Self(Uuid::new_v4())
            }

            /// Parses a canonical UUID.
            ///
            /// # Errors
            ///
            /// Returns [`Error::InvalidInput`] when the value is not canonical.
            pub fn parse(value: &str) -> Result<Self, Error> {
                let id = Uuid::parse_str(value).map_err(|_error| {
                    Error::InvalidInput(concat!($label, " must be a UUID").to_string())
                })?;
                if id.hyphenated().to_string() != value {
                    return Err(Error::InvalidInput(
                        concat!($label, " must use canonical lowercase UUID form").to_string(),
                    ));
                }
                Ok(Self(id))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{}", self.0.hyphenated())
            }
        }
    };
}

uuid_id!(ProviderGenerationId, "Provider Generation identity");

/// Kind of exact input pinned by one Provider Setup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderSetupInputKind {
    /// OpenAPI document input.
    OpenApi,
    /// Normalized MCP discovery snapshot input.
    Mcp,
}

impl ProviderSetupInputKind {
    /// Returns the stable persistence spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OpenApi => "openapi",
            Self::Mcp => "mcp",
        }
    }

    pub(crate) fn from_storage(value: &str) -> Result<Self, Error> {
        match value {
            "openapi" => Ok(Self::OpenApi),
            "mcp" => Ok(Self::Mcp),
            _ => Err(Error::CorruptData(format!(
                "Provider Setup input has unrecognized kind '{value}'"
            ))),
        }
    }
}

/// Exact secret-free OpenAPI document or MCP snapshot pinned by Provider Setup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderSetupInput {
    /// One pinned OpenAPI document.
    OpenApi(OpenApiDocument),
    /// One pinned normalized MCP discovery snapshot.
    Mcp(McpDiscoverySnapshot),
}

impl ProviderSetupInput {
    /// Returns the exact input kind.
    #[must_use]
    pub const fn kind(&self) -> ProviderSetupInputKind {
        match self {
            Self::OpenApi(_) => ProviderSetupInputKind::OpenApi,
            Self::Mcp(_) => ProviderSetupInputKind::Mcp,
        }
    }

    /// Returns the exact accepted UTF-8 input.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::OpenApi(document) => document.as_str(),
            Self::Mcp(snapshot) => snapshot.as_str(),
        }
    }

    /// Returns the confirmed secret-free configuration retained beside the exact input.
    #[must_use]
    pub fn configuration(&self) -> Option<&crate::ProviderConfiguration> {
        match self {
            Self::OpenApi(document) => document.configuration(),
            Self::Mcp(snapshot) => snapshot.configuration(),
        }
    }

    pub(crate) fn from_storage(kind: ProviderSetupInputKind, value: &str) -> Result<Self, Error> {
        match kind {
            ProviderSetupInputKind::OpenApi => OpenApiDocument::parse(value).map(Self::OpenApi),
            ProviderSetupInputKind::Mcp => McpDiscoverySnapshot::parse(value).map(Self::Mcp),
        }
    }
}

impl From<OpenApiDocument> for ProviderSetupInput {
    fn from(document: OpenApiDocument) -> Self {
        Self::OpenApi(document)
    }
}

impl From<McpDiscoverySnapshot> for ProviderSetupInput {
    fn from(snapshot: McpDiscoverySnapshot) -> Self {
        Self::Mcp(snapshot)
    }
}

/// SHA-256 evidence for exact pinned Provider input bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderSetupInputDigest(String);

impl ProviderSetupInputDigest {
    /// Computes digest evidence for one exact Provider Setup input.
    #[must_use]
    pub fn of_setup_input(input: &ProviderSetupInput) -> Self {
        Self::of_bytes(input.as_str().as_bytes())
    }

    pub(crate) fn of_bytes(bytes: &[u8]) -> Self {
        Self(encode_sha256(Sha256::digest(bytes)))
    }

    /// Parses persisted lowercase hexadecimal digest evidence.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CorruptData`] when the value is not SHA-256 shaped.
    pub fn parse(value: String) -> Result<Self, Error> {
        if !is_lowercase_sha256(&value) {
            return Err(Error::CorruptData(
                "Provider Setup input digest is not lowercase SHA-256".to_string(),
            ));
        }
        Ok(Self(value))
    }

    /// Returns lowercase hexadecimal digest evidence.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
struct DiagnosticCode(String);

impl DiagnosticCode {
    fn parse(value: String) -> Result<Self, Error> {
        let mut bytes = value.bytes();
        let valid_first = bytes.next().is_some_and(|byte| byte.is_ascii_lowercase());
        let valid_rest =
            bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
        if !valid_first || !valid_rest || value.len() > 64 {
            return Err(Error::CorruptData(
                "diagnostic code is not bounded snake_case".to_string(),
            ));
        }
        Ok(Self(value))
    }
}

/// Whether a safe Catalog diagnostic blocks one Operation or records widening.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogDiagnosticSeverity {
    /// The Operation cannot be invoked faithfully.
    Error,
    /// The Operation remains available with a sound wider authoring projection.
    Warning,
}

/// Safe structural or projection diagnostic attached to a publication package.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogDiagnostic {
    code: DiagnosticCode,
    message: String,
    severity: CatalogDiagnosticSeverity,
}

impl CatalogDiagnostic {
    /// Builds one stable blocking code and safe static message.
    #[must_use]
    pub fn stable(code: &'static str, message: &'static str) -> Self {
        Self {
            code: DiagnosticCode(code.to_string()),
            message: message.to_string(),
            severity: CatalogDiagnosticSeverity::Error,
        }
    }

    /// Builds one stable non-blocking projection warning.
    #[must_use]
    pub fn warning(code: &'static str, message: &'static str) -> Self {
        Self {
            code: DiagnosticCode(code.to_string()),
            message: message.to_string(),
            severity: CatalogDiagnosticSeverity::Warning,
        }
    }

    /// Names the document location, such as a JSON pointer, that caused this diagnostic.
    ///
    /// A location names document structure (component and property names), never a value.
    #[must_use]
    pub(crate) fn at_location(mut self, location: &str) -> Self {
        const MAX_LOCATION_CHARS: usize = 256;
        let truncated = location.chars().nth(MAX_LOCATION_CHARS).is_some();
        let kept = if truncated {
            MAX_LOCATION_CHARS - 1
        } else {
            MAX_LOCATION_CHARS
        };
        let mut shown = location
            .chars()
            .take(kept)
            .map(|character| {
                if character.is_control() {
                    char::REPLACEMENT_CHARACTER
                } else {
                    character
                }
            })
            .collect::<String>();
        if truncated {
            shown.push('…');
        }
        self.message = format!("{} at {shown}", self.message);
        self
    }

    /// Decodes persisted safe diagnostic evidence.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CorruptData`] for an invalid code.
    pub fn from_storage(code: String, message: String) -> Result<Self, Error> {
        Ok(Self {
            code: DiagnosticCode::parse(code)?,
            message,
            severity: CatalogDiagnosticSeverity::Error,
        })
    }

    /// Returns the bounded snake-case code.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code.0
    }

    /// Returns the safe message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns whether this diagnostic blocks the Operation.
    #[must_use]
    pub const fn severity(&self) -> CatalogDiagnosticSeverity {
        self.severity
    }

    pub(crate) fn list_from_storage(value: &str) -> Result<Vec<Self>, Error> {
        let diagnostics: Vec<Self> = serde_json::from_str(value).map_err(|_error| {
            Error::CorruptData("Materialized Operation diagnostics are not valid JSON".to_string())
        })?;
        for diagnostic in &diagnostics {
            DiagnosticCode::parse(diagnostic.code.0.clone())?;
            if diagnostic.message.is_empty() {
                return Err(Error::CorruptData(
                    "Materialized Operation diagnostic message is empty".to_string(),
                ));
            }
        }
        Ok(diagnostics)
    }
}

/// Provider-scoped stable key for one materialized Operation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct OperationKey(String);

impl OperationKey {
    /// Parses one Provider-scoped Operation Key.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for an empty or control-bearing key.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err(Error::InvalidInput(
                "Operation Key must be non-empty and contain no control characters".to_string(),
            ));
        }
        Ok(Self(value.to_string()))
    }

    /// Returns the provider-authored or deterministic key.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for OperationKey {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl From<OperationKey> for String {
    fn from(key: OperationKey) -> Self {
        key.0
    }
}

/// Package-local identity for one authoritative JSON Schema root.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaId(String);

impl SchemaId {
    pub(crate) fn for_source(source: &str) -> Self {
        let digest = Sha256::digest(source.as_bytes());
        let suffix = encode_sha256(digest).chars().take(24).collect::<String>();
        Self(format!("schema_{suffix}"))
    }

    pub(crate) fn from_storage(value: String) -> Result<Self, Error> {
        let suffix = value.strip_prefix("schema_");
        if value.len() != 31
            || !suffix.is_some_and(|suffix| {
                suffix
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
        {
            return Err(Error::CorruptData(
                "Materialized Operation has an invalid schema identity".to_string(),
            ));
        }
        Ok(Self(value))
    }

    /// Returns the deterministic package-local identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// JSON Schema semantics selected by the pinned Provider Definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaSemantics {
    /// The OpenAPI 3.0 Schema Object dialect.
    OpenApiV3_0,
    /// JSON Schema Draft 2020-12, used by OpenAPI 3.1 and MCP tool schemas.
    JsonSchema2020_12,
}

impl SchemaSemantics {
    pub(crate) const fn ref_siblings_apply(self) -> bool {
        matches!(self, Self::JsonSchema2020_12)
    }

    /// Only the OpenAPI 3.0 dialect defines `nullable`; elsewhere it has no effect.
    pub(crate) const fn nullable_applies(self) -> bool {
        matches!(self, Self::OpenApiV3_0)
    }
}

/// Complete authoritative package-local schema graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaGraph {
    semantics: SchemaSemantics,
    schemas: BTreeMap<SchemaId, Value>,
    source_index: BTreeMap<String, SchemaId>,
}

impl SchemaGraph {
    pub(crate) fn new(
        semantics: SchemaSemantics,
        schemas: BTreeMap<SchemaId, Value>,
        source_index: BTreeMap<String, SchemaId>,
    ) -> Result<Self, Error> {
        let graph = Self {
            semantics,
            schemas,
            source_index,
        };
        graph.validate()?;
        Ok(graph)
    }

    /// Returns the JSON Schema semantics used by this graph.
    #[must_use]
    pub const fn semantics(&self) -> SchemaSemantics {
        self.semantics
    }

    /// Resolves a package-local schema root.
    #[must_use]
    pub fn schema(&self, id: &SchemaId) -> Option<&Value> {
        self.schemas.get(id)
    }

    /// Returns whether the schema or its referenced root declares raw binary bytes.
    #[must_use]
    pub fn is_binary(&self, id: &SchemaId) -> bool {
        crate::schema::is_binary_schema(id, |id| self.schema(id))
    }

    /// Returns whether a value conforms to one authoritative schema root.
    ///
    /// Invalid package-local validator material returns `false`; Provider
    /// publication and persistence verification own reporting that corruption.
    #[must_use]
    pub fn accepts(&self, id: &SchemaId, value: &Value) -> bool {
        accepts_bundled(self, id, value, SchemaValidationDirection::Neutral)
    }

    /// Returns whether a response value conforms under OpenAPI output semantics.
    ///
    /// A required `writeOnly` property applies only to requests and is not
    /// required in an upstream response.
    #[must_use]
    pub fn accepts_output(&self, id: &SchemaId, value: &Value) -> bool {
        accepts_bundled(self, id, value, SchemaValidationDirection::Output)
    }

    /// Resolves one retained source locator to its package-local identity.
    #[must_use]
    pub fn schema_id_for_source(&self, source: &str) -> Option<&SchemaId> {
        self.source_index.get(source)
    }

    /// Returns the number of authoritative schema roots.
    #[must_use]
    pub fn len(&self) -> usize {
        self.schemas.len()
    }

    /// Returns whether the graph contains no schema roots.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }

    pub(crate) fn to_storage_json(&self) -> Result<String, Error> {
        serde_json::to_string(self).map_err(|_error| {
            Error::CorruptData("Provider schema graph cannot be encoded".to_string())
        })
    }

    pub(crate) fn from_storage_json(value: &str) -> Result<Self, Error> {
        let graph: Self = serde_json::from_str(value).map_err(|_error| {
            Error::CorruptData("Provider schema graph is not valid canonical JSON".to_string())
        })?;
        graph.validate()?;
        Ok(graph)
    }

    fn validate(&self) -> Result<(), Error> {
        let indexed_ids = self.source_index.values().collect::<HashSet<_>>();
        if self.is_empty()
            || self.source_index.iter().any(|(source, schema_id)| {
                SchemaId::for_source(source) != *schema_id || !self.schemas.contains_key(schema_id)
            })
            || indexed_ids.len() != self.schemas.len()
            || self
                .schemas
                .keys()
                .any(|schema_id| !indexed_ids.contains(schema_id))
            || !crate::schema::graph_references_are_package_local(self.semantics, &self.schemas)
        {
            return Err(Error::CorruptData(
                "Provider schema graph has incoherent roots or references".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn contains(&self, schema_id: &SchemaId) -> bool {
        self.schemas.contains_key(schema_id)
    }

    pub(crate) fn sources(&self) -> impl Iterator<Item = (&str, &SchemaId)> {
        self.source_index
            .iter()
            .map(|(source, schema_id)| (source.as_str(), schema_id))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SchemaValidationDirection {
    Neutral,
    Output,
}

fn accepts_bundled(
    graph: &SchemaGraph,
    root: &SchemaId,
    value: &Value,
    direction: SchemaValidationDirection,
) -> bool {
    bundled_schema(graph, root, direction).is_some_and(|schema| {
        jsonschema::validator_for(&schema).is_ok_and(|validator| validator.is_valid(value))
    })
}

fn bundled_schema(
    graph: &SchemaGraph,
    root: &SchemaId,
    direction: SchemaValidationDirection,
) -> Option<Value> {
    let mut pending = vec![root.as_str().to_string()];
    let mut seen = BTreeSet::new();
    let mut definitions = serde_json::Map::new();
    while let Some(id) = pending.pop() {
        if !seen.insert(id.clone()) {
            continue;
        }
        let schema = graph
            .schemas
            .iter()
            .find_map(|(candidate, schema)| (candidate.as_str() == id).then_some(schema))?;
        let mut schema = schema.clone();
        rewrite_schema_references(&mut schema, &mut pending, graph, direction);
        definitions.insert(id, schema);
    }
    Some(serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$ref": format!("#/$defs/{}", root.as_str()),
        "$defs": definitions,
    }))
}

fn rewrite_schema_references(
    schema: &mut Value,
    pending: &mut Vec<String>,
    graph: &SchemaGraph,
    direction: SchemaValidationDirection,
) {
    match schema {
        Value::Object(object) => {
            if direction == SchemaValidationDirection::Output {
                remove_write_only_output_requirements(object, graph);
            }
            let mut has_reference = false;
            if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
                let reference = reference.to_string();
                if reference.starts_with("schema_") {
                    has_reference = true;
                    pending.push(reference.clone());
                    object.insert(
                        "$ref".to_string(),
                        Value::String(format!("#/$defs/{reference}")),
                    );
                }
            }
            if has_reference && !graph.semantics.ref_siblings_apply() {
                return;
            }
            for keyword in crate::schema::SCHEMA_MAP_KEYWORDS {
                if let Some(children) = object.get_mut(keyword).and_then(Value::as_object_mut) {
                    for child in children.values_mut() {
                        rewrite_schema_references(child, pending, graph, direction);
                    }
                }
            }
            for keyword in crate::schema::SCHEMA_ARRAY_KEYWORDS {
                if let Some(children) = object.get_mut(keyword).and_then(Value::as_array_mut) {
                    for child in children {
                        rewrite_schema_references(child, pending, graph, direction);
                    }
                }
            }
            for keyword in crate::schema::SCHEMA_VALUE_KEYWORDS {
                if let Some(child) = object.get_mut(keyword) {
                    rewrite_schema_references(child, pending, graph, direction);
                }
            }
            if graph.semantics == SchemaSemantics::OpenApiV3_0 {
                normalize_openapi_30_numeric_bounds(object);
            }
            if graph.semantics.nullable_applies()
                && object.remove("nullable") == Some(Value::Bool(true))
            {
                let non_null = Value::Object(std::mem::take(object));
                *schema = serde_json::json!({"anyOf": [non_null, {"type": "null"}]});
            }
        }
        Value::Array(values) => {
            for value in values {
                rewrite_schema_references(value, pending, graph, direction);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn remove_write_only_output_requirements(
    schema: &mut serde_json::Map<String, Value>,
    graph: &SchemaGraph,
) {
    let mut write_only = HashSet::new();
    collect_write_only_property_names(schema, graph, &mut HashSet::new(), &mut write_only);
    if !write_only.is_empty() {
        remove_required_names(schema, &write_only);
    }
}

fn collect_write_only_property_names(
    schema: &serde_json::Map<String, Value>,
    graph: &SchemaGraph,
    resolving: &mut HashSet<String>,
    write_only: &mut HashSet<String>,
) {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        if resolving.insert(reference.to_string())
            && let Ok(schema_id) = SchemaId::from_storage(reference.to_string())
            && let Some(referenced) = graph.schemas.get(&schema_id).and_then(Value::as_object)
        {
            collect_write_only_property_names(referenced, graph, resolving, write_only);
        }
        if !graph.semantics.ref_siblings_apply() {
            return;
        }
    }
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        write_only.extend(
            properties
                .iter()
                .filter(|(_name, property)| {
                    schema_is_write_only(property, graph, &mut HashSet::new())
                })
                .map(|(name, _property)| name.clone()),
        );
    }
    if let Some(all_of) = schema.get("allOf").and_then(Value::as_array) {
        for branch in all_of.iter().filter_map(Value::as_object) {
            collect_write_only_property_names(branch, graph, resolving, write_only);
        }
    }
}

fn remove_required_names(
    schema: &mut serde_json::Map<String, Value>,
    write_only: &HashSet<String>,
) {
    if let Some(required) = schema.get_mut("required").and_then(Value::as_array_mut) {
        required.retain(|name| name.as_str().is_none_or(|name| !write_only.contains(name)));
    }
    if let Some(all_of) = schema.get_mut("allOf").and_then(Value::as_array_mut) {
        for branch in all_of.iter_mut().filter_map(Value::as_object_mut) {
            remove_required_names(branch, write_only);
        }
    }
}

fn schema_is_write_only(
    schema: &Value,
    graph: &SchemaGraph,
    resolving: &mut HashSet<String>,
) -> bool {
    let Some(object) = schema.as_object() else {
        return false;
    };
    let own_annotation = object.get("writeOnly").and_then(Value::as_bool) == Some(true);
    let composed_annotation =
        object
            .get("allOf")
            .and_then(Value::as_array)
            .is_some_and(|schemas| {
                schemas
                    .iter()
                    .any(|schema| schema_is_write_only(schema, graph, resolving))
            });
    let Some(reference) = object.get("$ref").and_then(Value::as_str) else {
        return own_annotation || composed_annotation;
    };
    if !resolving.insert(reference.to_string()) {
        return own_annotation;
    }
    let referenced_annotation = SchemaId::from_storage(reference.to_string())
        .ok()
        .and_then(|schema_id| graph.schemas.get(&schema_id))
        .is_some_and(|schema| schema_is_write_only(schema, graph, resolving));
    if graph.semantics.ref_siblings_apply() {
        own_annotation || composed_annotation || referenced_annotation
    } else {
        referenced_annotation
    }
}

fn normalize_openapi_30_numeric_bounds(object: &mut serde_json::Map<String, Value>) {
    for (exclusive, inclusive) in [
        ("exclusiveMinimum", "minimum"),
        ("exclusiveMaximum", "maximum"),
    ] {
        match object.get(exclusive) {
            Some(Value::Bool(true)) => {
                if let Some(bound) = object.remove(inclusive) {
                    object.insert(exclusive.to_string(), bound);
                } else {
                    object.remove(exclusive);
                }
            }
            Some(Value::Bool(false)) => {
                object.remove(exclusive);
            }
            _ => {}
        }
    }
}

/// Structural availability of one cataloged Operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationAvailability {
    /// Coral can faithfully invoke this Operation in the current profile.
    Available,
    /// Reef can inspect it, but Principal-facing discovery must omit it.
    Unavailable,
}

impl OperationAvailability {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Unavailable => "unavailable",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "available" => Ok(Self::Available),
            "unavailable" => Ok(Self::Unavailable),
            _ => Err(Error::CorruptData(format!(
                "Materialized Operation has unrecognized availability '{value}'"
            ))),
        }
    }
}

/// Effect hints the Provider declared for one Operation.
///
/// Each hint is `None` unless the Provider declared it. Coral never assumes a
/// protocol default, so an undeclared hint says nothing about the Operation.
/// OpenAPI/HTTP-sourced Operations declare none in v1.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OperationHints {
    read_only: Option<bool>,
    destructive: Option<bool>,
    idempotent: Option<bool>,
    open_world: Option<bool>,
}

impl OperationHints {
    /// Creates hints from exact declared values.
    #[must_use]
    pub const fn new(
        read_only: Option<bool>,
        destructive: Option<bool>,
        idempotent: Option<bool>,
        open_world: Option<bool>,
    ) -> Self {
        Self {
            read_only,
            destructive,
            idempotent,
            open_world,
        }
    }

    /// Returns whether the Operation declared that it changes nothing.
    #[must_use]
    pub const fn read_only(&self) -> Option<bool> {
        self.read_only
    }

    /// Returns whether the Operation declared that it may destroy data.
    #[must_use]
    pub const fn destructive(&self) -> Option<bool> {
        self.destructive
    }

    /// Returns whether the Operation declared that repeating a call has no further effect.
    #[must_use]
    pub const fn idempotent(&self) -> Option<bool> {
        self.idempotent
    }

    /// Returns whether the Operation declared that it reaches entities outside the Provider.
    #[must_use]
    pub const fn open_world(&self) -> Option<bool> {
        self.open_world
    }

    /// Returns whether the Operation declared that it changes nothing.
    #[must_use]
    pub const fn declares_read_only(&self) -> bool {
        matches!(self.read_only, Some(true))
    }
}

/// Maximum raw content bytes in one complete Operation Authoring Description.
pub const OPERATION_AUTHORING_DESCRIPTION_MAX_BYTES: usize = 256 * 1024;

/// Safe Provider-authored presentation metadata for one Operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationPresentation {
    title: Option<String>,
    description: String,
    deprecated: bool,
    tags: Vec<String>,
    guidance: Option<String>,
}

impl OperationPresentation {
    pub(crate) const fn new(
        title: Option<String>,
        description: String,
        deprecated: bool,
        tags: Vec<String>,
        guidance: Option<String>,
    ) -> Self {
        Self {
            title,
            description,
            deprecated,
            tags,
            guidance,
        }
    }

    /// Returns the optional Provider-authored title.
    #[must_use]
    pub fn title(&self) -> Option<&str> {
        self.title.as_deref()
    }

    /// Returns the safe complete Provider-authored description.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns Provider-authored deprecation evidence.
    #[must_use]
    pub const fn deprecated(&self) -> bool {
        self.deprecated
    }

    /// Returns safe Provider-authored tags in source order.
    #[must_use]
    pub fn tags(&self) -> &[String] {
        &self.tags
    }

    /// Returns optional safe Provider-authored usage guidance.
    #[must_use]
    pub fn guidance(&self) -> Option<&str> {
        self.guidance.as_deref()
    }

    pub(crate) fn is_valid(&self) -> bool {
        valid_presentation_text(&self.description)
            && self
                .title
                .as_ref()
                .is_none_or(|value| valid_presentation_text(value))
            && self.tags.len() <= 64
            && self.tags.iter().all(|value| valid_presentation_text(value))
            && self
                .guidance
                .as_ref()
                .is_none_or(|value| valid_presentation_text(value))
    }
}

/// Returns true when one Provider-authored presentation string is safe to retain.
///
/// Ordinary prose whitespace, including the newlines of a multiline description, is allowed.
/// Every other control character is not.
pub(crate) fn valid_presentation_text(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 16 * 1024 && !value.chars().any(unsafe_control)
}

/// Returns true for one control character that is not ordinary whitespace.
fn unsafe_control(character: char) -> bool {
    character.is_control() && !character.is_whitespace()
}

/// The diagnostic that makes an Operation unavailable when its Authoring
/// Description is over [`OPERATION_AUTHORING_DESCRIPTION_MAX_BYTES`].
///
/// The declaration counted is the Operation's own: the Provider's shared
/// types it names are declared once for the Provider and not counted here.
pub(crate) fn operation_description_too_large(
    relative_path: &str,
    provider_display_name: &str,
    typescript_declaration: &str,
    presentation: &OperationPresentation,
) -> Option<CatalogDiagnostic> {
    let raw_bytes = [
        relative_path.len(),
        provider_display_name.len(),
        typescript_declaration.len(),
        presentation.description.len(),
        presentation.title.as_ref().map_or(0, String::len),
        presentation.guidance.as_ref().map_or(0, String::len),
        presentation.tags.iter().map(String::len).sum(),
    ]
    .into_iter()
    .fold(8 * 1024usize, usize::saturating_add);
    (raw_bytes > OPERATION_AUTHORING_DESCRIPTION_MAX_BYTES).then(|| CatalogDiagnostic {
        code: DiagnosticCode("operation_authoring_description_too_large".to_string()),
        message: format!(
            "the Operation Authoring Description is {raw_bytes} bytes, over the discovery limit of {OPERATION_AUTHORING_DESCRIPTION_MAX_BYTES} bytes"
        ),
        severity: CatalogDiagnosticSeverity::Error,
    })
}

/// Minimal structurally available Operation identity used before search hydration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSearchTarget {
    key: OperationKey,
    relative_path: String,
    hints: OperationHints,
    http_method: Option<crate::HttpMethod>,
}

impl CatalogSearchTarget {
    pub(crate) const fn new(
        key: OperationKey,
        relative_path: String,
        hints: OperationHints,
        http_method: Option<crate::HttpMethod>,
    ) -> Self {
        Self {
            key,
            relative_path,
            hints,
            http_method,
        }
    }

    /// Returns the private Provider-catalog policy address.
    #[must_use]
    pub const fn key(&self) -> &OperationKey {
        &self.key
    }

    /// Returns the collision-free Operation-relative Call Path projection.
    #[must_use]
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    /// Returns the declared effect hints used by exact policy.
    #[must_use]
    pub const fn hints(&self) -> OperationHints {
        self.hints
    }

    /// Returns the method of an HTTP Operation; `None` for other protocols.
    #[must_use]
    pub const fn http_method(&self) -> Option<crate::HttpMethod> {
        self.http_method
    }
}

/// What the search index reads of one structurally available Operation: its
/// presentation, and the binding and input schema its declaration fields come
/// from. It omits schemas, declarations and diagnostics, which a large
/// Provider's indexing cannot afford to hold for every Operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchableOperation {
    key: OperationKey,
    relative_path: String,
    presentation: OperationPresentation,
    execution_binding: ExecutionBinding,
    input_schema: SchemaId,
}

impl SearchableOperation {
    pub(crate) const fn new(
        key: OperationKey,
        relative_path: String,
        presentation: OperationPresentation,
        execution_binding: ExecutionBinding,
        input_schema: SchemaId,
    ) -> Self {
        Self {
            key,
            relative_path,
            presentation,
            execution_binding,
            input_schema,
        }
    }

    /// Returns the private Provider-catalog policy address.
    #[must_use]
    pub const fn key(&self) -> &OperationKey {
        &self.key
    }

    /// Returns the collision-free Operation-relative Call Path projection.
    #[must_use]
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    /// Returns the title, description, tags, guidance and deprecation.
    #[must_use]
    pub const fn presentation(&self) -> &OperationPresentation {
        &self.presentation
    }

    pub(crate) const fn execution_binding(&self) -> &ExecutionBinding {
        &self.execution_binding
    }

    pub(crate) const fn input_schema(&self) -> &SchemaId {
        &self.input_schema
    }
}

/// Secret-free Operation definition materialized by this profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedOperation {
    key: OperationKey,
    relative_path: String,
    summary: String,
    presentation: OperationPresentation,
    hints: OperationHints,
    execution_binding: ExecutionBinding,
    binding_json: String,
    input_schema: SchemaId,
    input_schema_json: String,
    output_schema_json: String,
    typescript_declaration: String,
    type_references: Vec<String>,
    availability: OperationAvailability,
    diagnostics: Vec<CatalogDiagnostic>,
    diagnostics_json: String,
}

impl MaterializedOperation {
    #[expect(
        clippy::too_many_arguments,
        reason = "one immutable Operation definition is assembled at materialization"
    )]
    pub(crate) fn new(
        key: OperationKey,
        relative_path: String,
        summary: String,
        presentation: OperationPresentation,
        hints: OperationHints,
        execution_binding: ExecutionBinding,
        binding_json: String,
        input_schema: SchemaId,
        input_schema_json: String,
        output_schema_json: String,
        typescript_declaration: String,
        type_references: Vec<String>,
        diagnostics: Vec<CatalogDiagnostic>,
        diagnostics_json: String,
    ) -> Self {
        let availability = if diagnostics
            .iter()
            .any(|diagnostic| diagnostic.severity == CatalogDiagnosticSeverity::Error)
        {
            OperationAvailability::Unavailable
        } else {
            OperationAvailability::Available
        };
        let hints = match execution_binding {
            ExecutionBinding::Http(_) => OperationHints::default(),
            ExecutionBinding::Mcp(_) | ExecutionBinding::Builtin(_) => hints,
        };
        Self {
            key,
            relative_path,
            summary,
            presentation,
            hints,
            execution_binding,
            binding_json,
            input_schema,
            input_schema_json,
            output_schema_json,
            typescript_declaration,
            type_references,
            availability,
            diagnostics,
            diagnostics_json,
        }
    }

    /// Returns the Operation Key.
    #[must_use]
    pub const fn key(&self) -> &OperationKey {
        &self.key
    }

    /// Returns the collision-free Operation-relative JavaScript path.
    #[must_use]
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    /// Returns the complete typed upstream invocation recipe.
    #[must_use]
    pub const fn execution_binding(&self) -> &ExecutionBinding {
        &self.execution_binding
    }

    /// Returns safe authoring summary text.
    #[must_use]
    pub fn summary(&self) -> &str {
        &self.summary
    }

    /// Returns complete safe Provider-authored presentation metadata.
    #[must_use]
    pub const fn presentation(&self) -> &OperationPresentation {
        &self.presentation
    }

    /// Returns the declared effect hints.
    #[must_use]
    pub const fn hints(&self) -> OperationHints {
        self.hints
    }

    /// Returns the method of an HTTP Operation; `None` for other protocols.
    #[must_use]
    pub const fn http_method(&self) -> Option<crate::HttpMethod> {
        match &self.execution_binding {
            ExecutionBinding::Http(binding) => Some(binding.method()),
            ExecutionBinding::Mcp(_) | ExecutionBinding::Builtin(_) => None,
        }
    }

    pub(crate) fn binding_json(&self) -> &str {
        &self.binding_json
    }

    /// Stamps client ownership into every OAuth credential contract this
    /// Operation's recipe carries, re-encoding the retained recipe.
    pub(crate) fn with_oauth_client(
        mut self,
        policy: &crate::OAuthClientPolicy,
    ) -> Result<Self, Error> {
        match &mut self.execution_binding {
            ExecutionBinding::Http(binding) => {
                let slots = binding
                    .credential_slots()
                    .iter()
                    .map(|slot| {
                        crate::HttpCredentialSlot::new(
                            slot.scheme_name().to_string(),
                            slot.contract().clone().with_oauth_client(policy),
                        )
                    })
                    .collect();
                binding.set_credential_slots(slots);
                self.binding_json = binding.to_storage_json()?;
            }
            ExecutionBinding::Mcp(binding) => {
                let contract = binding
                    .credential_contract()
                    .clone()
                    .with_oauth_client(policy);
                binding.set_credential_contract(contract);
                self.binding_json = binding.to_storage_json()?;
            }
            ExecutionBinding::Builtin(_) => {}
        }
        Ok(self)
    }

    pub(crate) fn binding_kind(&self) -> &'static str {
        self.execution_binding.as_storage_str()
    }

    pub(crate) fn http_index(&self) -> (Option<&str>, Option<&str>, Option<&str>) {
        self.execution_binding.http_index()
    }

    pub(crate) fn schema_ids(&self) -> Box<dyn Iterator<Item = &SchemaId> + '_> {
        self.execution_binding.schema_ids()
    }

    /// Returns the authoritative flat logical input schema root.
    #[must_use]
    pub const fn input_schema(&self) -> &SchemaId {
        &self.input_schema
    }

    pub(crate) fn input_schema_json(&self) -> &str {
        &self.input_schema_json
    }

    pub(crate) fn output_schema_json(&self) -> &str {
        &self.output_schema_json
    }

    /// Returns the Operation's own TypeScript declaration. It names the
    /// Provider's shared types without declaring them; the complete
    /// declaration comes from [`crate::PreparedProviderCatalog::authoring_declaration`]
    /// or [`crate::authoring_declaration_in_transaction`].
    #[must_use]
    pub fn typescript_declaration(&self) -> &str {
        &self.typescript_declaration
    }

    /// Returns the shared types the Operation's own declaration names.
    pub(crate) fn type_references(&self) -> &[String] {
        &self.type_references
    }

    pub(crate) fn diagnostics_json(&self) -> &str {
        &self.diagnostics_json
    }

    /// Returns whether this Operation can be invoked faithfully.
    #[must_use]
    pub const fn availability(&self) -> OperationAvailability {
        self.availability
    }

    /// Returns every safe structural and projection diagnostic.
    #[must_use]
    pub fn diagnostics(&self) -> &[CatalogDiagnostic] {
        &self.diagnostics
    }

    /// Returns the first blocking structural reason for unavailability.
    #[must_use]
    pub fn diagnostic(&self) -> Option<&CatalogDiagnostic> {
        self.diagnostics
            .iter()
            .find(|diagnostic| diagnostic.severity == CatalogDiagnosticSeverity::Error)
    }
}

/// Opaque equality evidence for one canonical Provider Generation package.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderGenerationFingerprint(String);

impl ProviderGenerationFingerprint {
    pub(crate) fn from_material(
        schema_graph_json: &str,
        typescript_view: &str,
        operations: &[MaterializedOperation],
        configuration_json: Option<&str>,
    ) -> Self {
        let mut hasher = Sha256::new();
        fingerprint_text(&mut hasher, configuration_json.unwrap_or_default());
        fingerprint_text(&mut hasher, schema_graph_json);
        fingerprint_text(&mut hasher, typescript_view);
        hasher.update(operations.len().to_be_bytes());
        for operation in operations {
            for tag in operation.presentation.tags() {
                fingerprint_text(&mut hasher, tag);
            }
            for value in [
                operation.key.as_str(),
                operation.relative_path.as_str(),
                operation.binding_kind(),
                operation.summary.as_str(),
                operation.presentation.title().unwrap_or_default(),
                operation.presentation.description(),
                if operation.presentation.deprecated() {
                    "deprecated"
                } else {
                    "current"
                },
                operation.presentation.guidance().unwrap_or_default(),
                hint_fingerprint(operation.hints.read_only),
                hint_fingerprint(operation.hints.destructive),
                hint_fingerprint(operation.hints.idempotent),
                hint_fingerprint(operation.hints.open_world),
                operation.binding_json(),
                operation.input_schema_json.as_str(),
                operation.output_schema_json.as_str(),
                operation.typescript_declaration.as_str(),
                &operation.type_references.join("\n"),
                operation.availability.as_storage_str(),
                operation.diagnostics_json.as_str(),
            ] {
                fingerprint_text(&mut hasher, value);
            }
        }
        Self(encode_sha256(hasher.finalize()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

const fn hint_fingerprint(hint: Option<bool>) -> &'static str {
    match hint {
        Some(true) => "true",
        Some(false) => "false",
        None => "undeclared",
    }
}

fn fingerprint_text(hasher: &mut Sha256, value: &str) {
    hasher.update(value.len().to_be_bytes());
    hasher.update(value.as_bytes());
}

/// Reports whether `value` is SHA-256 evidence as [`encode_sha256`] writes it.
pub(crate) fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn encode_sha256(digest: impl IntoIterator<Item = u8>) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        if let Some(character) = char::from_digit(u32::from(byte >> 4), 16) {
            encoded.push(character);
        }
        if let Some(character) = char::from_digit(u32::from(byte & 0x0f), 16) {
            encoded.push(character);
        }
    }
    encoded
}

/// Fully prepared catalog package produced outside a write transaction.
#[derive(Clone)]
pub struct PreparedProviderCatalog {
    display_name: String,
    schema_graph: SchemaGraph,
    schema_graph_json: String,
    typescript_view: String,
    operations: Vec<MaterializedOperation>,
    shared_types: SharedTypes,
    configuration_json: Option<String>,
}

pub(crate) struct PreparedCatalogParts {
    pub(crate) schema_graph_json: String,
    pub(crate) typescript_view: String,
    pub(crate) operations: Vec<MaterializedOperation>,
    pub(crate) shared_types: SharedTypes,
}

impl PreparedProviderCatalog {
    /// Holds `operations` in Operation Key order, the order storage returns
    /// them in, so a stored Generation's fingerprint matches the prepared one.
    pub(crate) fn new(
        display_name: String,
        schema_graph: SchemaGraph,
        schema_graph_json: String,
        mut operations: Vec<MaterializedOperation>,
        shared_types: SharedTypes,
    ) -> Self {
        operations.sort_by(|left, right| left.key().cmp(right.key()));
        let typescript_view = operations
            .iter()
            .flat_map(|operation| operation.typescript_declaration().lines())
            .chain(
                shared_types
                    .iter()
                    .map(|(_name, shared)| shared.declaration.as_str()),
            )
            .map(str::to_string)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join("\n");
        Self {
            display_name,
            schema_graph,
            schema_graph_json,
            typescript_view,
            operations,
            shared_types,
            configuration_json: None,
        }
    }

    /// Binds credentials only after structural and authoring availability is final.
    /// Every recipe retains the same contract, even an unavailable first Operation.
    pub(crate) fn bind_http_credential_destinations(mut self) -> Result<Self, Error> {
        // Most Operations share a server. Every Operation gets a copy of this
        // list, so collapse it once rather than once per Operation.
        let destinations = self
            .operations
            .iter()
            .filter(|operation| operation.availability() == OperationAvailability::Available)
            .filter_map(|operation| match operation.execution_binding() {
                ExecutionBinding::Http(binding) if !binding.credential_slots().is_empty() => {
                    binding.server_url().map(str::to_owned)
                }
                _ => None,
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        for operation in &mut self.operations {
            if let ExecutionBinding::Http(binding) = &mut operation.execution_binding {
                let slots = binding
                    .credential_slots()
                    .iter()
                    .map(|slot| {
                        crate::HttpCredentialSlot::new(
                            slot.scheme_name().into(),
                            slot.contract()
                                .clone()
                                .at_destinations(destinations.clone()),
                        )
                    })
                    .collect();
                binding.set_credential_slots(slots);
                operation.binding_json = binding.to_storage_json()?;
            }
        }
        Ok(self)
    }

    /// Attaches confirmed configuration provenance without rewriting the pinned input.
    pub(crate) fn with_configuration(
        mut self,
        configuration: &crate::ProviderConfiguration,
    ) -> Result<Self, Error> {
        self.configuration_json = Some(
            serde_json::to_string(configuration)
                .map_err(|_invalid| Error::CorruptData("invalid provider configuration".into()))?,
        );
        Ok(self)
    }

    /// Returns provider-authored safe display metadata.
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    /// Returns the complete authoritative private schema graph.
    #[must_use]
    pub const fn schema_graph(&self) -> &SchemaGraph {
        &self.schema_graph
    }

    /// Promotes client ownership into a package whose OAuth contract has none,
    /// so the published Generation's contract names the client the sealed
    /// credential was granted through.
    ///
    /// # Errors
    /// Returns a catalog error if a retained recipe cannot be re-encoded.
    pub fn with_oauth_client(mut self, policy: &crate::OAuthClientPolicy) -> Result<Self, Error> {
        self.operations = self
            .operations
            .into_iter()
            .map(|operation| operation.with_oauth_client(policy))
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(self)
    }

    /// Returns the exact credential contract retained with the executable recipes.
    #[must_use]
    pub fn credential_contract(&self) -> crate::ProviderCredentialContract {
        self.operations
            .first()
            .and_then(|operation| match operation.execution_binding() {
                ExecutionBinding::Http(binding) => binding
                    .credential_slots()
                    .first()
                    .map(|slot| slot.contract().clone()),
                ExecutionBinding::Mcp(binding) => Some(binding.credential_contract().clone()),
                ExecutionBinding::Builtin(_) => None,
            })
            .unwrap_or(crate::ProviderCredentialContract::NONE)
    }

    /// Returns every Operation in deterministic Operation-Key order.
    #[must_use]
    pub fn operations(&self) -> &[MaterializedOperation] {
        &self.operations
    }

    /// Returns an Operation's complete self-contained declaration: its own
    /// declaration and every shared type it reaches.
    #[must_use]
    pub fn authoring_declaration(&self, operation: &MaterializedOperation) -> String {
        self.shared_types.compose(
            &operation.typescript_declaration,
            &operation.type_references,
        )
    }

    /// Returns current path assignments for regeneration carry-forward.
    #[must_use]
    pub fn operation_paths(&self) -> Vec<OperationPathAssignment> {
        OperationPathAssignment::for_operations(&self.operations)
    }

    /// Returns opaque equality evidence for canonical package reuse.
    #[must_use]
    pub fn fingerprint(&self) -> ProviderGenerationFingerprint {
        ProviderGenerationFingerprint::from_material(
            &self.schema_graph_json,
            &self.typescript_view,
            &self.operations,
            self.configuration_json.as_deref(),
        )
    }

    pub(crate) fn into_parts(self) -> PreparedCatalogParts {
        PreparedCatalogParts {
            schema_graph_json: self.schema_graph_json,
            typescript_view: self.typescript_view,
            operations: self.operations,
            shared_types: self.shared_types,
        }
    }
}

/// Synchronous catalog materialization result.
pub enum MaterializationOutcome {
    /// The complete package is ready for a short publication transaction.
    Ready(PreparedProviderCatalog),
    /// A root, credential-contract, or index failure rejects publication.
    Rejected(CatalogDiagnostic),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parts of a one-root graph, before `SchemaGraph::new` validates them.
    fn one_root(
        schema: Value,
    ) -> (
        SchemaSemantics,
        BTreeMap<SchemaId, Value>,
        BTreeMap<String, SchemaId>,
    ) {
        let id = SchemaId::for_source("generated:test");
        (
            SchemaSemantics::JsonSchema2020_12,
            BTreeMap::from([(id.clone(), schema)]),
            BTreeMap::from([("generated:test".to_string(), id)]),
        )
    }

    #[test]
    fn diagnostic_location_is_capped_at_256_characters() {
        let shown_location = |location: &str| {
            CatalogDiagnostic::stable("code", "message")
                .at_location(location)
                .message()
                .strip_prefix("message at ")
                .expect("location suffix")
                .to_string()
        };

        let fits = "x".repeat(256);
        assert_eq!(shown_location(&fits), fits);

        let shown = shown_location(&"x".repeat(257));
        assert_eq!(shown.chars().count(), 256);
        assert_eq!(shown, format!("{}…", "x".repeat(255)));
    }

    #[test]
    fn schema_graph_construction_rejects_nonlocal_semantic_references() {
        let (semantics, schemas, source_index) =
            one_root(serde_json::json!({"$ref": "#/provider/source"}));
        SchemaGraph::new(semantics, schemas, source_index)
            .expect_err("raw semantic reference must be rejected");
    }

    #[test]
    fn stored_schema_graph_rejects_missing_targets_but_ignores_literal_references() {
        let (semantics, schemas, source_index) = one_root(serde_json::json!({
            "type": "object",
            "example": {"$ref": "#/literal/provider/data"}
        }));
        let literal = SchemaGraph::new(semantics, schemas, source_index)
            .expect("literal reference must not be interpreted as schema authority");
        let literal_json = serde_json::to_string(&literal).expect("literal graph can be encoded");
        SchemaGraph::from_storage_json(&literal_json)
            .expect("literal reference must not be interpreted as schema authority");

        // Stored data can still be corrupt: its root names a target the package lacks.
        let mut missing: Value = serde_json::from_str(&literal_json).expect("stored graph is JSON");
        *missing
            .pointer_mut(&format!(
                "/schemas/{}",
                SchemaId::for_source("generated:test").as_str()
            ))
            .expect("stored root") = serde_json::json!({"$ref": "schema_000000000000000000000000"});
        SchemaGraph::from_storage_json(&missing.to_string())
            .expect_err("missing package target must be rejected");
    }

    #[test]
    fn presentation_metadata_retains_prose_whitespace_and_rejects_other_control_characters() {
        let presentation = |description: &str| {
            OperationPresentation::new(
                Some("Update split DNS".to_string()),
                description.to_string(),
                false,
                Vec::new(),
                None,
            )
        };

        // Read-back must accept what materialization now retains, or a stored
        // multiline description would come back as corrupt data.
        assert!(
            presentation("Partial update.\n\nOther mappings are preserved.\tAlways.").is_valid()
        );
        assert!(!presentation("Partial\u{0}update.").is_valid());
        assert!(!presentation("Partial\u{7f}update.").is_valid());
        assert!(!presentation("   \n  ").is_valid());
    }
}
