use crate::Error;

/// Maximum normalized MCP discovery snapshot size accepted by the synchronous profile.
pub const MCP_DISCOVERY_SNAPSHOT_MAX_BYTES: usize = 4 * 1024 * 1024;

/// Exact UTF-8 normalized MCP discovery snapshot accepted for durable Provider Setup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpDiscoverySnapshot {
    source: String,
    configuration: Option<Box<crate::ProviderConfiguration>>,
    configuration_revision: i64,
}

impl McpDiscoverySnapshot {
    /// Accepts exact bounded nonempty UTF-8 input for later materialization.
    ///
    /// Structural snapshot validation occurs during Publication.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for empty, oversized, or null-bearing input.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.is_empty()
            || value.len() > MCP_DISCOVERY_SNAPSHOT_MAX_BYTES
            || value.contains('\0')
        {
            return Err(Error::InvalidInput(format!(
                "MCP discovery snapshot must be non-empty, contain no null character, and be at most {MCP_DISCOVERY_SNAPSHOT_MAX_BYTES} bytes"
            )));
        }
        Ok(Self {
            source: value.to_owned(),
            configuration: None,
            configuration_revision: 0,
        })
    }

    /// Restores independently confirmed settings without modifying the exact snapshot.
    /// # Errors
    /// Rejects malformed stored configuration or revision evidence.
    pub fn restore_configuration(mut self, value: &str, revision: i64) -> Result<Self, Error> {
        if revision < 1 {
            return Err(Error::CorruptData("invalid configuration revision".into()));
        }
        self.configuration = Some(
            serde_json::from_str(value)
                .map_err(|_invalid| Error::CorruptData("invalid MCP configuration".into()))?,
        );
        self.configuration_revision = revision;
        Ok(self)
    }

    /// Confirmed authentication and destination, separate from discovered Tools.
    #[must_use]
    pub fn configuration(&self) -> Option<&crate::ProviderConfiguration> {
        self.configuration.as_deref()
    }

    /// Inspects source connection facts without editing the snapshot or listing a new catalog.
    /// # Errors
    /// Returns a safe structural diagnostic for an invalid snapshot.
    pub fn connection(
        &self,
    ) -> Result<(String, crate::ProviderCredentialContract), crate::CatalogDiagnostic> {
        let value: serde_json::Value = serde_json::from_str(&self.source).map_err(|_invalid| {
            crate::CatalogDiagnostic::stable(
                "invalid_mcp_discovery_snapshot",
                "review the MCP snapshot",
            )
        })?;
        let root = value.as_object().ok_or_else(|| {
            crate::CatalogDiagnostic::stable(
                "invalid_mcp_discovery_snapshot",
                "review the MCP snapshot",
            )
        })?;
        let (_, url) = super::materialize::server_binding(root)?;
        Ok((url, super::materialize::credential_contract(root)?))
    }

    /// The server's own name, as the suggested Provider display name.
    #[must_use]
    pub fn server_name(&self) -> Option<String> {
        let value: serde_json::Value = serde_json::from_str(&self.source).ok()?;
        super::materialize::display_name(value.as_object()?).ok()
    }

    /// Returns the exact accepted normalized snapshot.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.source
    }
}
