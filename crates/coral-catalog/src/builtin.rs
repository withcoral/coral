//! The Built-in Provider's Coral-defined Operation catalog.
//!
//! Coral defines each Operation here in Rust: its Operation Key, input and
//! output schemas, presentation, and Operation Hints. No OpenAPI document or
//! MCP snapshot exists. The Generation pins this catalog's identity instead,
//! so each Workspace's Generation records which definition it materialized.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::schema::{
    Direction, ProjectionOptions, SchemaGraphAccumulator, TypeScriptNames, TypeScriptProjector,
    canonicalize, render_callable_declaration,
};
use crate::search::prepare_operation_declaration_fields;
use crate::{
    CatalogDiagnostic, Error, ExecutionBinding, MaterializedOperation, OperationHints,
    OperationKey, OperationPresentation, OperationSummary, PreparedProviderCatalog,
    ProviderGenerationFingerprint, SchemaGraph, SchemaId, SchemaSemantics,
};

/// Safe display name of the Built-in Provider.
pub const BUILTIN_PROVIDER_DISPLAY_NAME: &str = "Coral";
/// Starts the catalog identity a built-in Generation pins as its input.
const CATALOG_IDENTITY_PREFIX: &str = "coral-admin-catalog:";
pub(crate) const MATERIALIZER_VERSION: &str = "coral-admin-catalog-v1";

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

/// One Coral-owned Operation that this Coral version performs itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinOperation {
    /// Creates a Provider from an OpenAPI document or URL, or an MCP server URL.
    /// Coral publishes it when nobody needs to sign in; otherwise a person
    /// finishes it in Reef.
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

    fn from_key(key: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|operation| operation.key() == key)
    }
}

/// The Built-in Provider's catalog that a Coral process serves, and the
/// identity each Generation of it pins. Bootstrap prepares it once. Workspace
/// creation and the boot update publish it.
#[derive(Clone)]
pub struct BuiltinCatalog(Arc<PreparedBuiltinCatalog>);

struct PreparedBuiltinCatalog {
    prepared: PreparedProviderCatalog,
    fingerprint: ProviderGenerationFingerprint,
    identity: String,
}

impl BuiltinCatalog {
    /// Prepares the catalog this Coral version defines.
    ///
    /// # Errors
    ///
    /// Returns an invalid built-in definition.
    pub fn running() -> Result<Self, Error> {
        Self::from_definitions(BuiltinOperation::ALL.into_iter().map(definition).collect())
    }

    /// Prepares this Coral version's catalog plus Operations it does not
    /// perform, as another Coral version could publish them. Each extra
    /// Operation takes an object and returns one.
    ///
    /// # Errors
    ///
    /// Returns an invalid built-in definition.
    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn with_unknown_operations(
        extra: impl IntoIterator<Item = (&'static str, OperationHints)>,
    ) -> Result<Self, Error> {
        let mut definitions: Vec<Definition> =
            BuiltinOperation::ALL.into_iter().map(definition).collect();
        definitions.extend(extra.into_iter().map(|(key, hints)| Definition {
            key,
            summary: "An Operation another Coral version defines",
            presentation: OperationPresentation::new(
                None,
                "An Operation another Coral version defines.".to_string(),
                false,
                Vec::new(),
                None,
            ),
            hints,
            input: json!({ "type": "object", "properties": {}, "additionalProperties": false }),
            output: json!({ "type": "object", "properties": {}, "additionalProperties": false }),
        }));
        Self::from_definitions(definitions)
    }

    fn from_definitions(definitions: Vec<Definition>) -> Result<Self, Error> {
        let prepared = prepare(definitions)
            .map_err(|diagnostic| Error::InvalidInput(diagnostic.message().to_string()))?;
        let identity = catalog_identity(&prepared);
        Ok(Self(Arc::new(PreparedBuiltinCatalog {
            fingerprint: prepared.fingerprint(),
            prepared,
            identity,
        })))
    }

    /// Returns the identity a Generation of this catalog pins.
    #[must_use]
    pub fn identity(&self) -> &str {
        &self.0.identity
    }

    pub(crate) fn fingerprint(&self) -> &ProviderGenerationFingerprint {
        &self.0.fingerprint
    }

    pub(crate) fn operations(&self) -> &[MaterializedOperation] {
        self.0.prepared.operations()
    }

    pub(crate) fn prepared(&self) -> PreparedProviderCatalog {
        self.0.prepared.clone()
    }
}

/// How the catalog a Coral process serves differs from a Workspace's
/// Built-in Generation, in Operation Key order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltinCatalogChanges {
    /// Operations that only the served catalog has.
    pub added: Vec<OperationSummary>,
    /// Operations that only the Workspace's Generation has.
    pub removed: Vec<OperationSummary>,
    /// Operations both have, with a different definition. Each is the served one.
    pub changed: Vec<OperationSummary>,
}

/// Compares the served catalog's Operations with a Generation's, by key.
pub(crate) fn changes(
    served: &[MaterializedOperation],
    stored: &[MaterializedOperation],
) -> BuiltinCatalogChanges {
    let served: BTreeMap<_, _> = served
        .iter()
        .map(|operation| (operation.key(), operation))
        .collect();
    let stored: BTreeMap<_, _> = stored
        .iter()
        .map(|operation| (operation.key(), operation))
        .collect();
    BuiltinCatalogChanges {
        added: served
            .iter()
            .filter(|(key, _)| !stored.contains_key(*key))
            .map(|(_, operation)| OperationSummary::of(operation))
            .collect(),
        removed: stored
            .iter()
            .filter(|(key, _)| !served.contains_key(*key))
            .map(|(_, operation)| OperationSummary::of(operation))
            .collect(),
        changed: served
            .iter()
            .filter(|(key, operation)| stored.get(*key).is_some_and(|stored| stored != *operation))
            .map(|(_, operation)| OperationSummary::of(operation))
            .collect(),
    }
}

/// A Built-in Provider Operation's Execution Binding. It names the
/// Coral-owned Operation and carries no destination, credential, or routing data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuiltinExecutionBinding {
    /// The Operation Key. Another Coral version may name an Operation that
    /// this version does not perform.
    operation: String,
    input_schema: SchemaId,
    output_schema: SchemaId,
}

impl BuiltinExecutionBinding {
    /// Returns the Coral-owned Operation this binding names, or `None` when
    /// another Coral version published an Operation this version does not
    /// perform.
    #[must_use]
    pub fn operation(&self) -> Option<BuiltinOperation> {
        BuiltinOperation::from_key(&self.operation)
    }

    pub(crate) fn operation_key(&self) -> &str {
        &self.operation
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
    key: &'static str,
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
            key: operation.key(),
            summary: "Create a Provider from an OpenAPI document or URL, or an MCP server URL",
            presentation: OperationPresentation::new(
                Some("Create provider".to_string()),
                "Create a Provider in this Workspace from an OpenAPI document, an OpenAPI \
                 document URL, or an MCP server URL. Coral publishes it when nobody needs to \
                 sign in. Otherwise it returns a Reef link where a person finishes and \
                 publishes it."
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
                     your arguments, and Workspace Owners can see them. When status is \
                     published, call the Provider's Operations under \
                     coral.providers.<namespace>. When status is needs_sign_in, show the \
                     person the returned url. To check later whether it is published, search \
                     with its namespace as provider."
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
                    "authentication": { "$ref": "#/$defs/Authentication" },
                    "credentialFields": {
                        "type": "object",
                        "description": "The values a person enters when they sign in, keyed by field name, such as token. Omit it to use the source's fields. Give it when authentication names a field the source does not declare. Never include a value.",
                        "additionalProperties": { "$ref": "#/$defs/CredentialField" }
                    },
                    "requestHeaders": {
                        "type": "object",
                        "description": "Non-secret headers sent with every request. Omit it to use the source's suggestion. Authorization, Proxy-Authorization, Cookie, X-API-Key, Api-Key, and any header that ends in -Token or -Secret fail with invalid_input: a person enters credentials when they sign in.",
                        "additionalProperties": { "type": "string" }
                    }
                },
                "additionalProperties": false,
                "$defs": {
                    "CredentialField": {
                        "type": "object",
                        "required": ["label", "sensitive"],
                        "properties": {
                            "label": { "type": "string", "description": "The name of the box the person fills in." },
                            "description": { "type": "string", "description": "Help text." },
                            "sensitive": { "type": "boolean", "description": "Masks the value while it is typed." }
                        },
                        "additionalProperties": false
                    },
                    "Authentication": {
                        "description": "The authentication method and its non-secret settings. Omit it to use the method the source declares. Never include a secret or a literal value: a header, query parameter or Basic part names a field from credentialFields. The one literal is empty, for a Basic or query part the service leaves blank.",
                        "oneOf": [
                            {
                                "type": "object",
                                "required": ["method"],
                                "properties": { "method": { "enum": ["none", "bearer"] } },
                                "additionalProperties": false
                            },
                            {
                                "type": "object",
                                "required": ["method", "headers"],
                                "properties": {
                                    "method": { "const": "header_credentials" },
                                    "headers": {
                                        "type": "object",
                                        "description": "Header name to the field it sends.",
                                        "additionalProperties": { "$ref": "#/$defs/HeaderField" }
                                    }
                                },
                                "additionalProperties": false
                            },
                            {
                                "type": "object",
                                "required": ["method", "username", "password"],
                                "properties": {
                                    "method": { "const": "basic_auth" },
                                    "username": { "$ref": "#/$defs/CredentialPart" },
                                    "password": { "$ref": "#/$defs/CredentialPart" }
                                },
                                "additionalProperties": false
                            },
                            {
                                "type": "object",
                                "required": ["method", "parameters"],
                                "properties": {
                                    "method": { "const": "query_credentials" },
                                    "parameters": {
                                        "type": "object",
                                        "description": "Query parameter name to the field it sends.",
                                        "additionalProperties": { "$ref": "#/$defs/CredentialPart" }
                                    }
                                },
                                "additionalProperties": false
                            },
                            {
                                "type": "object",
                                "required": ["method", "device_authorization_url", "token_url", "scopes"],
                                "properties": {
                                    "method": { "const": "oauth_device_code" },
                                    "device_authorization_url": { "type": "string" },
                                    "token_url": { "type": "string" },
                                    "scopes": { "type": "array", "items": { "type": "string" } },
                                    "scope_format": { "$ref": "#/$defs/ScopeFormat" },
                                    "client": { "$ref": "#/$defs/DeviceOAuthClient" }
                                },
                                "additionalProperties": false
                            },
                            {
                                "type": "object",
                                "required": ["method", "authorization_url", "token_url", "scopes"],
                                "properties": {
                                    "method": { "const": "oauth_authorization_code" },
                                    "authorization_url": { "type": "string" },
                                    "token_url": { "type": "string" },
                                    "scopes": { "type": "array", "items": { "type": "string" } },
                                    "scope_format": { "$ref": "#/$defs/ScopeFormat" },
                                    "client": { "$ref": "#/$defs/OAuthClient" }
                                },
                                "additionalProperties": false
                            }
                        ]
                    },
                    "HeaderField": {
                        "type": "object",
                        "required": ["source", "field"],
                        "properties": {
                            "source": { "const": "field" },
                            "field": { "type": "string", "description": "A field from credentialFields." },
                            "prefix": { "type": "string", "description": "Non-secret text sent before the value, such as Bot . Never a credential." }
                        },
                        "additionalProperties": false
                    },
                    "CredentialPart": {
                        "oneOf": [
                            {
                                "type": "object",
                                "required": ["source", "field"],
                                "properties": {
                                    "source": { "const": "field" },
                                    "field": { "type": "string", "description": "A field from credentialFields." }
                                },
                                "additionalProperties": false
                            },
                            {
                                "type": "object",
                                "required": ["source", "value"],
                                "description": "A part the service leaves blank, such as a Basic password.",
                                "properties": {
                                    "source": { "const": "literal" },
                                    "value": { "const": "" }
                                },
                                "additionalProperties": false
                            }
                        ]
                    },
                    "ScopeFormat": {
                        "enum": ["space", "comma"],
                        "description": "How scopes are joined in the request. Omit it for space."
                    },
                    "OAuthClient": {
                        "type": "object",
                        "description": "An OAuth application registered with the service. Omit it to let Coral register one, when the service allows it. A confidential application never takes its secret here: the person enters it when they sign in.",
                        "required": ["client_id"],
                        "properties": {
                            "client_id": { "type": "string" },
                            "authentication": {
                                "enum": ["none", "client_secret_basic", "client_secret_post"],
                                "description": "How the application authenticates at the token endpoint. Omit it, or give none, for a public application."
                            },
                            "pkce": {
                                "enum": ["s256", "disabled"],
                                "description": "Authorization-code protection. Omit it for s256."
                            },
                            "authorization_parameters": {
                                "type": "object",
                                "description": "Non-secret authorization options, such as access_type.",
                                "additionalProperties": { "type": "string" }
                            }
                        },
                        "additionalProperties": false
                    },
                    "DeviceOAuthClient": {
                        "type": "object",
                        "description": "An OAuth application registered with the service. A confidential application never takes its secret here: the person enters it when they sign in. Device authorization takes no authorization parameters.",
                        "required": ["client_id"],
                        "properties": {
                            "client_id": { "type": "string" },
                            "authentication": {
                                "enum": ["none", "client_secret_basic", "client_secret_post"],
                                "description": "How the application authenticates at the token endpoint. Omit it, or give none, for a public application."
                            },
                            "pkce": {
                                "enum": ["s256", "disabled"],
                                "description": "Device code never uses it."
                            }
                        },
                        "additionalProperties": false
                    },
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
                            "status": { "const": "needs_sign_in", "description": "A person must finish the Provider in Reef." },
                            "url": { "type": "string", "description": "The Reef page where a person finishes and publishes the Provider." },
                            "namespace": { "type": "string", "description": "The requested Provider Namespace. The person can change it." }
                        },
                        "additionalProperties": false
                    }
                ]
            }),
        },
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
            operation: definition.key.to_string(),
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
    let mut declaration_fields = BTreeMap::new();
    for (definition, binding) in drafts {
        let key = OperationKey::parse(definition.key)
            .map_err(|_error| invalid("a built-in Operation Key is invalid"))?;
        let relative_path = definition.key.to_string();
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
        let prepared = prepare_operation_declaration_fields(
            &execution_binding,
            binding.input_schema(),
            true,
            &schema_graph,
        )?;
        declaration_fields.insert(key.clone(), prepared.entries);
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
    Ok(PreparedProviderCatalog::new(
        BUILTIN_PROVIDER_DISPLAY_NAME.to_string(),
        schema_graph,
        schema_graph_json,
        operations,
        declaration_fields,
    ))
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
