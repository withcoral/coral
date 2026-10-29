use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

use crate::{Error, SchemaId};

/// MCP invocation method selected by a materialized Execution Binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpInvocationMethod {
    /// Invoke one discovered tool with an ordinary `tools/call` request.
    CallTool,
    /// Read one opaque Provider-bound resource reference with `resources/read`.
    ReadResource,
}

/// Provider-declared Upstream MCP Task support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum McpTaskSupport {
    /// The tool must be called directly and cannot use an Upstream MCP Task.
    Forbidden,
    /// The tool permits either direct invocation or an Upstream MCP Task.
    Optional,
    /// The tool requires the Upstream MCP Task lifecycle.
    Required,
}

impl McpTaskSupport {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "forbidden" => Some(Self::Forbidden),
            "optional" => Some(Self::Optional),
            "required" => Some(Self::Required),
            _ => None,
        }
    }
}

/// MCP server transport retained by an Execution Binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpTransport {
    /// Streamable HTTP MCP transport.
    StreamableHttp,
}

/// Exact standard MCP annotation evidence, including provider-declared absence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpToolAnnotations {
    title: Option<String>,
    read_only_hint: Option<bool>,
    destructive_hint: Option<bool>,
    idempotent_hint: Option<bool>,
    open_world_hint: Option<bool>,
    extensions: BTreeMap<String, Value>,
}

impl McpToolAnnotations {
    pub(crate) const fn new(
        title: Option<String>,
        read_only_hint: Option<bool>,
        destructive_hint: Option<bool>,
        idempotent_hint: Option<bool>,
        open_world_hint: Option<bool>,
        extensions: BTreeMap<String, Value>,
    ) -> Self {
        Self {
            title,
            read_only_hint,
            destructive_hint,
            idempotent_hint,
            open_world_hint,
            extensions,
        }
    }

    /// Returns the annotation title when the provider declared it.
    #[must_use]
    pub fn title(&self) -> Option<&str> {
        self.title.as_deref()
    }

    /// Returns exact `readOnlyHint` presence and value.
    #[must_use]
    pub const fn read_only_hint(&self) -> Option<bool> {
        self.read_only_hint
    }

    /// Returns exact `destructiveHint` presence and value.
    #[must_use]
    pub const fn destructive_hint(&self) -> Option<bool> {
        self.destructive_hint
    }

    /// Returns exact `idempotentHint` presence and value.
    #[must_use]
    pub const fn idempotent_hint(&self) -> Option<bool> {
        self.idempotent_hint
    }

    /// Returns exact `openWorldHint` presence and value.
    #[must_use]
    pub const fn open_world_hint(&self) -> Option<bool> {
        self.open_world_hint
    }

    /// Returns the declared effect hints, without assuming protocol defaults.
    #[must_use]
    pub const fn hints(&self) -> crate::ToolHints {
        crate::ToolHints::new(
            self.read_only_hint,
            self.destructive_hint,
            self.idempotent_hint,
            self.open_world_hint,
        )
    }

    /// Returns provider annotation extensions without interpreting them as authority.
    #[must_use]
    pub const fn extensions(&self) -> &BTreeMap<String, Value> {
        &self.extensions
    }
}

/// Complete secret-free MCP invocation recipe retained without live discovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpExecutionBinding {
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    request_headers: std::collections::BTreeMap<String, String>,
    // Boxed and optional so the enum of bindings stays small and older retained
    // recipes decode as credential-free.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    credential_contract: Option<Box<crate::ProviderCredentialContract>>,
    protocol_version: String,
    transport: McpTransport,
    server_url: String,
    invocation_method: McpInvocationMethod,
    tool_name: Option<String>,
    tool_definition: Option<Value>,
    input_schema: SchemaId,
    output_schema: Option<SchemaId>,
    annotations: Option<McpToolAnnotations>,
    server_task_capability: Option<Value>,
    declared_task_support: Option<McpTaskSupport>,
}

impl McpExecutionBinding {
    /// Returns confirmed non-secret headers, independent of account credentials.
    #[must_use]
    pub const fn request_headers(&self) -> &std::collections::BTreeMap<String, String> {
        &self.request_headers
    }

    pub(crate) fn set_request_headers(
        &mut self,
        headers: std::collections::BTreeMap<String, String>,
    ) {
        self.request_headers = headers;
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one immutable MCP recipe is assembled once"
    )]
    pub(crate) fn new(
        protocol_version: String,
        transport: McpTransport,
        server_url: String,
        invocation_method: McpInvocationMethod,
        tool_name: Option<String>,
        tool_definition: Option<Value>,
        input_schema: SchemaId,
        output_schema: Option<SchemaId>,
        annotations: Option<McpToolAnnotations>,
        server_task_capability: Option<Value>,
        declared_task_support: Option<McpTaskSupport>,
    ) -> Self {
        Self {
            request_headers: std::collections::BTreeMap::new(),
            credential_contract: None,
            protocol_version,
            transport,
            server_url,
            invocation_method,
            tool_name,
            tool_definition,
            input_schema,
            output_schema,
            annotations,
            server_task_capability,
            declared_task_support,
        }
    }

    /// Returns the exact secret-free credential contract the snapshot was discovered under.
    #[must_use]
    pub fn credential_contract(&self) -> &crate::ProviderCredentialContract {
        self.credential_contract
            .as_deref()
            .unwrap_or(&CREDENTIAL_FREE)
    }

    pub(crate) fn set_credential_contract(&mut self, contract: crate::ProviderCredentialContract) {
        self.credential_contract =
            (contract != crate::ProviderCredentialContract::NONE).then(|| Box::new(contract));
    }

    /// Returns the exact negotiated MCP protocol version.
    #[must_use]
    pub fn protocol_version(&self) -> &str {
        &self.protocol_version
    }

    /// Returns the retained upstream MCP transport.
    #[must_use]
    pub const fn transport(&self) -> McpTransport {
        self.transport
    }

    /// Returns the exact secret-free upstream MCP server URL.
    #[must_use]
    pub fn server_url(&self) -> &str {
        &self.server_url
    }

    /// Returns whether this recipe calls a tool or reads an opaque resource.
    #[must_use]
    pub const fn invocation_method(&self) -> McpInvocationMethod {
        self.invocation_method
    }

    /// Returns the exact provider tool name for `tools/call` recipes.
    #[must_use]
    pub fn tool_name(&self) -> Option<&str> {
        self.tool_name.as_deref()
    }

    /// Returns the complete canonical discovered tool definition.
    #[must_use]
    pub const fn tool_definition(&self) -> Option<&Value> {
        self.tool_definition.as_ref()
    }

    /// Returns the exact authoritative input-schema root.
    #[must_use]
    pub const fn input_schema(&self) -> &SchemaId {
        &self.input_schema
    }

    /// Returns exact `outputSchema` presence and its authoritative root.
    #[must_use]
    pub const fn output_schema(&self) -> Option<&SchemaId> {
        self.output_schema.as_ref()
    }

    /// Returns exact annotation-object presence and standard field evidence.
    #[must_use]
    pub const fn annotations(&self) -> Option<&McpToolAnnotations> {
        self.annotations.as_ref()
    }

    /// Returns exact server Task-capability presence and value.
    #[must_use]
    pub const fn server_task_capability(&self) -> Option<&Value> {
        self.server_task_capability.as_ref()
    }

    /// Returns exact declared task-support presence and value.
    #[must_use]
    pub const fn declared_task_support(&self) -> Option<McpTaskSupport> {
        self.declared_task_support
    }

    /// Returns the effective protocol task support when the field was absent.
    #[must_use]
    pub const fn task_support(&self) -> McpTaskSupport {
        match self.declared_task_support {
            Some(support) => support,
            None => McpTaskSupport::Forbidden,
        }
    }

    pub(crate) fn to_storage_json(&self) -> Result<String, Error> {
        serde_json::to_string(self).map_err(|_error| {
            Error::CorruptData("MCP Execution Binding cannot be encoded".to_string())
        })
    }

    pub(crate) fn from_storage_json(value: &str) -> Result<Self, Error> {
        let binding: Self = serde_json::from_str(value).map_err(|_error| {
            Error::CorruptData("MCP Execution Binding is not valid canonical JSON".to_string())
        })?;
        let method_is_valid = match binding.invocation_method {
            McpInvocationMethod::CallTool => {
                binding
                    .tool_name
                    .as_ref()
                    .is_some_and(|name| !name.is_empty())
                    && binding.tool_definition.is_some()
            }
            McpInvocationMethod::ReadResource => {
                binding.tool_name.is_none()
                    && binding.tool_definition.is_none()
                    && binding.output_schema.is_none()
                    && binding.annotations.is_none()
                    && binding.declared_task_support.is_none()
            }
        };
        let server_url_is_valid = Url::parse(&binding.server_url).is_ok_and(|url| {
            matches!(url.scheme(), "http" | "https")
                && url.username().is_empty()
                && url.password().is_none()
                && url.fragment().is_none()
        });
        if binding.protocol_version.is_empty() || !server_url_is_valid || !method_is_valid {
            return Err(Error::CorruptData(
                "MCP Execution Binding has incoherent protocol evidence".to_string(),
            ));
        }
        Ok(binding)
    }

    pub(crate) fn schema_ids(&self) -> impl Iterator<Item = &SchemaId> {
        std::iter::once(&self.input_schema).chain(self.output_schema.iter())
    }
}

static CREDENTIAL_FREE: crate::ProviderCredentialContract = crate::ProviderCredentialContract::NONE;
