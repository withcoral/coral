//! `coral.workspace.createProvider`: the schemas a Program is typed from, the
//! arguments Coral reads against the input schema, and the result it writes
//! against the output schema.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{
    ArgumentProblem, BuiltinCredentialRoute, BuiltinOperation, Definition, ProviderFacts,
    authentication_names, operation_result, present,
};
use crate::{
    AuthenticationConfiguration, CredentialField, OpenApiDocument, OperationHints,
    OperationPresentation, ProviderConfiguration, ServerSelection,
};

mod input;
#[cfg(test)]
pub(super) mod tests;

#[expect(
    clippy::too_many_lines,
    reason = "the Operation's whole contract reads as one declaration"
)]
pub(super) fn definition() -> Definition {
    Definition {
        key: BuiltinOperation::CreateProvider.key(),
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
                 your arguments, and Workspace Owners can see them. When Coral cannot \
                 fetch a source URL, the call fails with upstream_incomplete: check that \
                 the URL is reachable, then call again. When status is \
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
                    "enum": BuiltinCredentialRoute::names(),
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
                            "enum": BuiltinCredentialRoute::names(),
                            "description": "The published Credential Route."
                        },
                        "authentication": {
                            "enum": authentication_names(),
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
    }
}

/// `createProvider`'s arguments, checked by every rule that needs no
/// Workspace state. The Provider's configuration leaves each omitted field to
/// the source's suggestion.
#[derive(Debug, Clone)]
pub struct CreateProviderArguments {
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Arguments {
    namespace: String,
    source: SourceInput,
    #[serde(default, deserialize_with = "present")]
    credential_route: Option<BuiltinCredentialRoute>,
    #[serde(default, deserialize_with = "present")]
    display_name: Option<String>,
    #[serde(default, deserialize_with = "present")]
    servers: Option<BTreeMap<String, input::ServerInput>>,
    #[serde(default, deserialize_with = "present")]
    authentication: Option<input::AuthenticationInput>,
    #[serde(default, deserialize_with = "present")]
    request_headers: Option<BTreeMap<String, String>>,
    #[serde(default, deserialize_with = "present")]
    credential_fields: Option<BTreeMap<String, input::CredentialFieldInput>>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum SourceInput {
    OpenapiDocument { document: String },
    OpenapiUrl { url: String },
    McpUrl { url: String },
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

impl CreateProviderArguments {
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
        // A credential-named header would carry a credential value through the
        // Program; a person enters every value in Reef. The authentication input
        // takes no literal value but an empty one.
        for name in arguments
            .request_headers
            .iter()
            .flat_map(BTreeMap::keys)
            .filter(|name| names_a_credential(name))
        {
            problems.push(ArgumentProblem::new(
                format!("requestHeaders.{name}"),
                "this header names a credential; send it through `authentication` \
                 so a person enters its value when they sign in",
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

/// What `createProvider` returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CreateProviderResult {
    /// Coral published the Provider, and the Program can call it now.
    Published {
        /// The published Provider Namespace.
        namespace: String,
        /// What the Program needs to call it.
        #[serde(flatten)]
        facts: ProviderFacts,
    },
    /// A person finishes the Provider at `url`.
    NeedsSignIn {
        /// The Coral UI page where a person finishes and publishes it.
        url: String,
        /// The requested Provider Namespace.
        namespace: String,
    },
}

impl CreateProviderResult {
    /// The Operation Result.
    #[must_use]
    pub fn into_operation_result(self) -> Value {
        operation_result(&self)
    }
}

/// Header names whose value is a credential. Request headers are non-secret by
/// contract, but only a name can show what a value is.
const CREDENTIAL_HEADERS: [&str; 5] = [
    "authorization",
    "proxy-authorization",
    "cookie",
    "x-api-key",
    "api-key",
];

fn names_a_credential(header: &str) -> bool {
    let header = header.to_ascii_lowercase();
    CREDENTIAL_HEADERS.contains(&header.as_str())
        || header.ends_with("-token")
        || header.ends_with("-secret")
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
