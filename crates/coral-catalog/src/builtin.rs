//! The Built-in Provider's Coral-defined Operation catalog.
//!
//! Coral defines each Operation here in Rust: its Operation Key, input and
//! output schemas, presentation, and Operation Hints. No OpenAPI document or
//! MCP snapshot exists. The Generation pins this catalog's identity instead,
//! so each Workspace's Generation records which definition it materialized.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::schema::{
    Direction, ProjectionOptions, SchemaGraphAccumulator, SharedTypes, TypeScriptNames,
    TypeScriptProjector, canonicalize, render_callable_declaration,
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

/// One Coral-owned Operation that this Coral version performs itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinOperation {
    /// Creates a Provider from an OpenAPI document or URL, or an MCP server URL.
    /// Coral publishes it when nobody needs to sign in; otherwise a person
    /// finishes it in Reef.
    CreateProvider,
    /// Lists every Provider in the Workspace, so a Program can see which
    /// namespaces are taken before it creates one.
    ListProviders,
}

impl BuiltinOperation {
    const ALL: [Self; 2] = [Self::CreateProvider, Self::ListProviders];

    /// Returns the Operation Key, which is also its relative path.
    #[must_use]
    pub const fn key(self) -> &'static str {
        match self {
            Self::CreateProvider => "createProvider",
            Self::ListProviders => "listProviders",
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

/// A Built-in Provider Operation's Execution Binding. It names the
/// Coral-owned Operation and carries no destination, credential, or routing data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuiltinExecutionBinding {
    /// The Operation Key. Another Coral version may name an Operation that
    /// this version does not perform.
    operation: OperationKey,
    input_schema: SchemaId,
    output_schema: SchemaId,
}

impl BuiltinExecutionBinding {
    /// Returns the Coral-owned Operation this binding names, or `None` when
    /// another Coral version published an Operation this version does not
    /// perform.
    #[must_use]
    pub fn operation(&self) -> Option<BuiltinOperation> {
        BuiltinOperation::from_key(self.operation.as_str())
    }

    pub(crate) const fn operation_key(&self) -> &OperationKey {
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

/// Each Credential Route, as a Program writes it.
const CREDENTIAL_ROUTES: [&str; 3] = ["none", "fixed", "caller"];
/// Each way Coral sends a Provider's credentials, as a Program reads it.
const AUTHENTICATION_KINDS: [&str; 7] = [
    "none",
    "bearer",
    "header_credentials",
    "basic_auth",
    "query_credentials",
    "oauth_device_code",
    "oauth_authorization_code",
];

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
                        "enum": CREDENTIAL_ROUTES,
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
                                "enum": CREDENTIAL_ROUTES,
                                "description": "The published Credential Route."
                            },
                            "authentication": {
                                "enum": AUTHENTICATION_KINDS,
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
        BuiltinOperation::ListProviders => Definition {
            key: operation.key(),
            summary: "List the Providers in this Workspace",
            presentation: OperationPresentation::new(
                Some("List providers".to_string()),
                "List every Provider in this Workspace, including disabled ones and Coral's \
                 own, in the order they were created."
                    .to_string(),
                false,
                vec!["providers".to_string()],
                Some(
                    "Pass an empty object. A namespace in the list is taken, even when its \
                     Provider is disabled. Call a Provider's Operations under \
                     coral.providers.<namespace>."
                        .to_string(),
                ),
            ),
            // Read-only: the default Operation Access State is enabled.
            hints: OperationHints::new(Some(true), None, Some(true), None),
            input: json!({ "type": "object", "properties": {}, "additionalProperties": false }),
            output: json!({
                "type": "object",
                "required": ["providers"],
                "properties": {
                    "providers": {
                        "type": "array",
                        "description": "Every Provider in this Workspace, in the order they were created. Coral's own comes first.",
                        "items": { "$ref": "#/$defs/Provider" }
                    }
                },
                "additionalProperties": false,
                "$defs": {
                    "Provider": {
                        "type": "object",
                        "required": [
                            "namespace", "displayName", "state", "credentialRoute",
                            "authentication", "operations", "accountRequired"
                        ],
                        "properties": {
                            "namespace": {
                                "type": ["string", "null"],
                                "description": "The Provider Namespace. Its Operations are called under coral.providers.<namespace>. Null for Coral's own Provider, whose Operations are called under coral.workspace."
                            },
                            "displayName": { "type": "string", "description": "The Provider display name." },
                            "state": {
                                "enum": ["active", "disabled"],
                                "description": "Whether the Provider accepts calls. A disabled Provider still holds its namespace."
                            },
                            "credentialRoute": {
                                "enum": CREDENTIAL_ROUTES,
                                "description": "Whose account Invocations use."
                            },
                            "authentication": {
                                "enum": AUTHENTICATION_KINDS,
                                "description": "How Coral sends the Provider's credentials."
                            },
                            "operations": {
                                "type": "integer",
                                "minimum": 0,
                                "description": "How many of the Provider's Operations you can discover and call."
                            },
                            "accountRequired": {
                                "type": "boolean",
                                "description": "True when you must connect your own Account before your calls can use the Provider."
                            }
                        },
                        "additionalProperties": false
                    }
                }
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
            operation: OperationKey::parse(definition.key)
                .map_err(|_error| invalid("a built-in Operation Key is invalid"))?,
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
    let mut operations = Vec::with_capacity(drafts.len());
    for (definition, binding) in drafts {
        let key = binding.operation_key().clone();
        let relative_path = definition.key.to_string();
        let (declaration, type_references) =
            project(&binding, &schema_graph, &type_names, &mut shared_types)?;
        if crate::types::operation_description_too_large(
            &relative_path,
            BUILTIN_PROVIDER_DISPLAY_NAME,
            &declaration,
            &definition.presentation,
        )
        .is_some()
        {
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
            type_references,
            Vec::new(),
            "[]".to_string(),
        ));
    }
    Ok(PreparedProviderCatalog::new(
        BUILTIN_PROVIDER_DISPLAY_NAME.to_string(),
        schema_graph,
        schema_graph_json,
        operations,
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
