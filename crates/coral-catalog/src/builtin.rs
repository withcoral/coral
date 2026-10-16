//! The Built-in Provider's Coral-defined Operation catalog.
//!
//! Coral defines each Operation here in Rust: its Operation Key, input and
//! output schemas, presentation, and Operation Hints. No OpenAPI document or
//! MCP snapshot exists. The Generation pins this catalog's identity instead,
//! so each Workspace's Generation records which definition it materialized.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::schema::{
    Direction, ProjectionOptions, SchemaGraphAccumulator, TypeScriptNames, TypeScriptProjector,
    canonicalize, render_callable_declaration,
};
use crate::{
    CatalogDiagnostic, Error, ExecutionBinding, MaterializedOperation, OperationHints,
    OperationKey, OperationPresentation, PreparedProviderCatalog, SchemaGraph, SchemaId,
    SchemaSemantics,
};

/// Safe display name of the Built-in Provider.
pub const BUILTIN_PROVIDER_DISPLAY_NAME: &str = "Coral";
/// Starts the catalog identity a built-in Generation pins as its input.
const CATALOG_IDENTITY_PREFIX: &str = "coral-admin-catalog:";
pub(crate) const MATERIALIZER_VERSION: &str = "coral-admin-catalog-v1";

/// The Built-in Provider's catalog package and the identity its Generation
/// pins, prepared outside a write transaction.
pub struct PreparedBuiltinCatalog {
    pub(crate) catalog: PreparedProviderCatalog,
    pub(crate) identity: String,
}

/// Returns the identity a Generation of `catalog` pins: the prefix and the
/// catalog's fingerprint. A Coral version that changes the catalog pins a
/// different identity.
fn catalog_identity(catalog: &PreparedProviderCatalog) -> String {
    format!(
        "{CATALOG_IDENTITY_PREFIX}{}",
        catalog.fingerprint().as_str()
    )
}

/// Reports whether `pinned` is a well-formed catalog identity. It accepts any
/// fingerprint, because another Coral version pins its own catalog's identity.
pub(crate) fn is_catalog_identity(pinned: &str) -> bool {
    pinned
        .strip_prefix(CATALOG_IDENTITY_PREFIX)
        .is_some_and(crate::types::is_lowercase_sha256)
}

/// One Coral-owned Operation that Coral performs itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuiltinOperation {
    /// Creates a Provider from an OpenAPI document or URL, or an MCP server URL.
    /// Coral publishes it when nobody needs to sign in; otherwise a person
    /// finishes it in Reef.
    #[serde(rename = "createProvider")]
    CreateProvider,
}

impl BuiltinOperation {
    const ALL: [Self; 1] = [Self::CreateProvider];

    /// Returns the Operation Key, which is also its relative path.
    #[must_use]
    pub const fn key(self) -> &'static str {
        match self {
            Self::CreateProvider => "createProvider",
        }
    }
}

/// A Built-in Provider Operation's Execution Binding. It names the
/// Coral-owned Operation and carries no destination, credential, or routing data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuiltinExecutionBinding {
    operation: BuiltinOperation,
    input_schema: SchemaId,
    output_schema: SchemaId,
}

impl BuiltinExecutionBinding {
    /// Returns the Coral-owned Operation this binding names.
    #[must_use]
    pub const fn operation(&self) -> BuiltinOperation {
        self.operation
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

struct Definition {
    operation: BuiltinOperation,
    summary: &'static str,
    presentation: OperationPresentation,
    hints: OperationHints,
    input: Value,
    output: Value,
}

#[expect(
    clippy::too_many_lines,
    reason = "each Built-in Operation's whole contract reads as one declaration"
)]
fn definition(operation: BuiltinOperation) -> Definition {
    match operation {
        BuiltinOperation::CreateProvider => Definition {
            operation,
            summary: "Create a Provider from an OpenAPI document or URL, or an MCP server URL",
            presentation: OperationPresentation::new(
                Some("Create provider".to_string()),
                "Create a Provider in this Workspace from an OpenAPI document, an OpenAPI \
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
                     Give displayName, servers, authentication or requestHeaders only to \
                     override what the source suggests. Never pass a secret or a literal \
                     credential value: Coral stores your arguments, and Workspace Owners can \
                     see them. When status is published, call the Provider's Operations under \
                     coral.providers.<namespace>. When status is needs_sign_in, show the person \
                     the returned url. To check later whether it is published, search with its \
                     namespace as provider."
                        .to_string(),
                ),
            ),
            // No read-only hint: the default Operation Access State is approval_required.
            hints: OperationHints::default(),
            input: json!({
                "type": "object",
                "required": ["namespace", "source", "credentialRoute"],
                "properties": {
                    "namespace": {
                        "type": "string",
                        "description": "Provider Namespace: a short JavaScript identifier, such as github. The Provider's Operations are called under coral.providers.<namespace>."
                    },
                    "source": { "$ref": "#/$defs/ProviderSource" },
                    "credentialRoute": {
                        "enum": ["none", "fixed", "caller"],
                        "description": "Whose account Invocations use: none for a source that needs no credential, fixed for one shared account that a person signs in to, or caller for each person's own account."
                    },
                    "displayName": {
                        "type": "string",
                        "description": "The Provider's name. Omit it to use the source's title."
                    },
                    "servers": {
                        "type": "object",
                        "description": "Server choice per server declaration, keyed by declaration pointer such as #/servers. Omit it to use the source's server. If the source declares none, or a server variable has no default, the call fails with invalid_input: find the value in the source's documentation or ask the user, then pass it here.",
                        "additionalProperties": { "$ref": "#/$defs/ServerSelection" }
                    },
                    "authentication": {
                        "type": "object",
                        "description": "The authentication method and its non-secret settings, such as header names, OAuth endpoints and scopes. Omit it to use the method the source declares. Never include a secret or a literal value.",
                        "required": ["method"],
                        "properties": {
                            "method": {
                                "enum": ["none", "bearer", "header_credentials", "basic_auth", "query_credentials", "oauth_device_code", "oauth_authorization_code"]
                            }
                        }
                    },
                    "requestHeaders": {
                        "type": "object",
                        "description": "Non-secret headers sent with every request. Omit it to use the source's suggestion. Authorization, Proxy-Authorization, Cookie, X-API-Key, Api-Key, and any header that ends in -Token or -Secret fail with invalid_input: a person enters credentials when they sign in.",
                        "additionalProperties": { "type": "string" }
                    }
                },
                "additionalProperties": false,
                "$defs": {
                    "ServerSelection": {
                        "oneOf": [
                            {
                                "type": "object",
                                "required": ["kind", "url", "variables"],
                                "properties": {
                                    "kind": { "const": "declared" },
                                    "url": { "type": "string", "description": "A server URL template the source declares." },
                                    "variables": { "type": "object", "additionalProperties": { "type": "string" } }
                                },
                                "additionalProperties": false
                            },
                            {
                                "type": "object",
                                "required": ["kind", "url"],
                                "properties": {
                                    "kind": { "const": "custom" },
                                    "url": { "type": "string", "description": "An absolute HTTP or HTTPS base URL, without credentials, a query, or a fragment." }
                                },
                                "additionalProperties": false
                            }
                        ]
                    },
                    "ProviderSource": {
                        "oneOf": [
                            {
                                "type": "object",
                                "required": ["kind", "document"],
                                "properties": {
                                    "kind": { "const": "openapi_document" },
                                    "document": { "type": "string", "description": "A complete OpenAPI document, as JSON or YAML text." }
                                },
                                "additionalProperties": false
                            },
                            {
                                "type": "object",
                                "required": ["kind", "url"],
                                "properties": {
                                    "kind": { "const": "openapi_url" },
                                    "url": { "type": "string", "description": "The URL of an OpenAPI document." }
                                },
                                "additionalProperties": false
                            },
                            {
                                "type": "object",
                                "required": ["kind", "url"],
                                "properties": {
                                    "kind": { "const": "mcp_url" },
                                    "url": { "type": "string", "description": "The URL of a Streamable HTTP MCP server." }
                                },
                                "additionalProperties": false
                            }
                        ]
                    }
                }
            }),
            output: json!({
                "oneOf": [
                    {
                        "type": "object",
                        "required": [
                            "status", "namespace", "displayName", "credentialRoute",
                            "authentication", "operations", "accountRequired"
                        ],
                        "properties": {
                            "status": { "const": "published", "description": "Coral published the Provider. You can call it now." },
                            "namespace": { "type": "string", "description": "The published Provider Namespace." },
                            "displayName": { "type": "string", "description": "The published Provider display name." },
                            "credentialRoute": {
                                "enum": ["none", "fixed", "caller"],
                                "description": "The published Credential Route."
                            },
                            "authentication": {
                                "enum": [
                                    "none", "bearer", "header_credentials", "basic_auth",
                                    "query_credentials", "oauth_device_code", "oauth_authorization_code"
                                ],
                                "description": "How Coral sends the Provider's credentials, as resolved from your input and the source."
                            },
                            "operations": {
                                "type": "integer",
                                "minimum": 0,
                                "description": "How many of the Provider's Operations you can discover and call."
                            },
                            "accountRequired": {
                                "type": "boolean",
                                "description": "True when you must connect your own Account before your calls can use the Provider. Your first call then asks you to connect it."
                            }
                        },
                        "additionalProperties": false
                    },
                    {
                        "type": "object",
                        "required": ["status", "url", "namespace"],
                        "properties": {
                            "status": { "const": "needs_sign_in", "description": "A person must finish the Provider in the Coral UI." },
                            "url": { "type": "string", "description": "The Coral UI page where a person finishes and publishes the Provider." },
                            "namespace": { "type": "string", "description": "The requested Provider Namespace. The person can change it." }
                        },
                        "additionalProperties": false
                    }
                ]
            }),
        },
    }
}

/// Prepares the complete Built-in Provider catalog package and its identity.
pub(crate) fn prepare() -> Result<PreparedBuiltinCatalog, CatalogDiagnostic> {
    let mut schemas = SchemaGraphAccumulator::new(SchemaSemantics::JsonSchema2020_12);
    let mut drafts = Vec::new();
    for operation in BuiltinOperation::ALL {
        let definition = definition(operation);
        let input_schema =
            schemas.add_embedded_schema(&schema_source(operation, "input"), &definition.input)?;
        let output_schema =
            schemas.add_embedded_schema(&schema_source(operation, "output"), &definition.output)?;
        let binding = BuiltinExecutionBinding {
            operation,
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
    let mut operations = Vec::with_capacity(drafts.len());
    for (definition, binding) in drafts {
        let key = OperationKey::parse(definition.operation.key())
            .map_err(|_error| invalid("a built-in Operation Key is invalid"))?;
        let relative_path = definition.operation.key().to_string();
        let declaration = project(&relative_path, &binding, &schema_graph, &type_names)?;
        if !crate::types::operation_description_fits(
            &relative_path,
            BUILTIN_PROVIDER_DISPLAY_NAME,
            &declaration,
            &definition.presentation,
        ) {
            return Err(invalid(
                "a built-in Operation description exceeds the discovery limit",
            ));
        }
        let execution_binding = ExecutionBinding::Builtin(binding.clone());
        operations.push(MaterializedOperation::new(
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
            Vec::new(),
            "[]".to_string(),
        ));
    }
    let catalog = PreparedProviderCatalog::new(
        BUILTIN_PROVIDER_DISPLAY_NAME.to_string(),
        schema_graph,
        schema_graph_json,
        operations,
    );
    Ok(PreparedBuiltinCatalog {
        identity: catalog_identity(&catalog),
        catalog,
    })
}

/// Names every schema of the built-in graph for projection.
pub(crate) fn typescript_names(graph: &SchemaGraph) -> TypeScriptNames {
    TypeScriptNames::new(graph, ProjectionOptions::builtin())
}

fn project(
    relative_path: &str,
    binding: &BuiltinExecutionBinding,
    graph: &SchemaGraph,
    type_names: &TypeScriptNames,
) -> Result<String, CatalogDiagnostic> {
    let mut projector = TypeScriptProjector::new(graph, type_names, ProjectionOptions::builtin());
    let input = projector.project(binding.input_schema(), Direction::Input)?;
    let output = projector.project(binding.output_schema(), Direction::Output)?;
    let mut declarations = projector.render_named_declarations()?;
    // A closed object is wider in structural TypeScript; only an error rejects.
    if projector
        .into_diagnostics()
        .iter()
        .any(|diagnostic| diagnostic.severity() == crate::CatalogDiagnosticSeverity::Error)
    {
        return Err(invalid("a built-in schema cannot be projected faithfully"));
    }
    declarations.push(render_callable_declaration(
        relative_path,
        &format!("input: {input}"),
        &format!("Promise<{{ data: {output}; metadata: {{ kind: \"builtin\" }} }}>"),
    )?);
    Ok(declarations.join("\n"))
}

fn schema_source(operation: BuiltinOperation, direction: &str) -> String {
    format!("builtin:{}:{direction}", operation.key())
}

fn encode(schema: &Value) -> Result<String, CatalogDiagnostic> {
    serde_json::to_string(&canonicalize(schema))
        .map_err(|_error| invalid("a built-in schema cannot be encoded"))
}

fn invalid(message: &'static str) -> CatalogDiagnostic {
    CatalogDiagnostic::stable("invalid_builtin_catalog", message)
}
