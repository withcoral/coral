//! Trusted, bootstrap-selected human navigation, never an incoming HTTP identity.

use coral_outbound::{EndpointUrl, ResourceIdentifier};

/// Validated HTTPS or explicit loopback HTTP origin for authenticated human review.
/// The MCP listener requires one; built-in Tools link to Reef pages under it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HumanInterfaceUrl(String);

impl HumanInterfaceUrl {
    /// Validates an operator/launcher destination using the shared URL policy.
    ///
    /// # Errors
    /// Rejects non-origins, credentials, queries, fragments and unsafe transports.
    pub fn parse(value: &str) -> Result<Self, HumanInterfaceUrlError> {
        let endpoint = EndpointUrl::<ResourceIdentifier>::parse(value)
            .map_err(|_error| HumanInterfaceUrlError)?;
        let url = endpoint.as_url();
        if url.path() != "/"
            || url.query().is_some()
            || url.port() == Some(0)
            || matches!(url.host_str(), Some("0.0.0.0" | "[::]"))
        {
            return Err(HumanInterfaceUrlError);
        }
        Ok(Self(url.as_str().trim_end_matches('/').to_owned()))
    }

    /// Returns the canonical configured origin, without a trailing slash.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Joins a root-relative Reef path that the Run domain owns to this origin.
    /// Concatenation, unlike URL resolution, cannot turn even a network-path
    /// reference into another origin.
    #[must_use]
    pub fn page_url(&self, path: &str) -> String {
        format!("{}{path}", self.0)
    }
}

/// Invalid human-interface bootstrap destination. Never echoes configuration.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error(
    "must be a concrete HTTPS or explicit loopback HTTP origin with a nonzero port when specified, and no credentials, path, query or fragment"
)]
pub struct HumanInterfaceUrlError;
