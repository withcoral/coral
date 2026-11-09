use crate::{
    BuiltinExecutionBinding, CodeExecutionBinding, Error, HttpExecutionBinding,
    McpExecutionBinding, SchemaId,
};

/// Complete upstream-specific invocation recipe for one materialized Tool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionBinding {
    /// HTTP request/response execution.
    Http(HttpExecutionBinding),
    /// MCP tool or resource execution.
    Mcp(McpExecutionBinding),
    /// A Built-in Provider Tool that Coral performs itself.
    Builtin(BuiltinExecutionBinding),
    /// A Private Tool that Coral performs by running its definition in the
    /// Run. No catalog stores one: the Private Provider's catalog holds no
    /// Tool, and each Invocation retains the definition its Run linked.
    Code(CodeExecutionBinding),
}

impl ExecutionBinding {
    pub(crate) const fn as_storage_str(&self) -> &'static str {
        match self {
            Self::Http(_) => "http",
            Self::Mcp(_) => "mcp",
            Self::Builtin(_) => "builtin",
            Self::Code(_) => "code",
        }
    }

    /// Decodes a stored binding. An HTTP binding gets back the credential
    /// contract that storage keeps once, on the Provider Generation.
    pub(crate) fn from_storage_json(
        protocol: &str,
        value: &str,
        credential_contract: &std::sync::Arc<crate::ProviderCredentialContract>,
    ) -> Result<Self, Error> {
        match protocol {
            "http" => {
                let mut binding = HttpExecutionBinding::from_storage_json(value)?;
                binding.restore_credential_contract(credential_contract)?;
                Ok(Self::Http(binding))
            }
            "mcp" => McpExecutionBinding::from_storage_json(value).map(Self::Mcp),
            "builtin" => BuiltinExecutionBinding::from_storage_json(value).map(Self::Builtin),
            _ => Err(Error::CorruptData(format!(
                "Materialized Tool has unrecognized protocol '{protocol}'"
            ))),
        }
    }

    pub(crate) fn http_index(&self) -> (Option<&str>, Option<&str>, Option<&str>) {
        match self {
            Self::Http(binding) => (
                Some(binding.method().as_str()),
                Some(binding.path_template()),
                binding.server_url(),
            ),
            Self::Mcp(_) | Self::Builtin(_) | Self::Code(_) => (None, None, None),
        }
    }

    pub(crate) fn schema_ids(&self) -> Box<dyn Iterator<Item = &SchemaId> + '_> {
        match self {
            Self::Http(binding) => Box::new(binding.schema_ids()),
            Self::Mcp(binding) => Box::new(binding.schema_ids()),
            Self::Builtin(binding) => Box::new(binding.schema_ids()),
            Self::Code(_) => Box::new(std::iter::empty()),
        }
    }
}
