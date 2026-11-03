//! The names an upstream client span carries.

use std::fmt;

use coral_catalog::{HttpExecutionBinding, McpExecutionBinding};
use coral_telemetry::safe::Identifier;

/// A name a Provider's declaration gives, as Coral's catalog imported it: an
/// MCP tool's upstream name, or an HTTP operation's route template.
#[derive(Clone, Copy)]
pub(crate) struct Declared<'a>(&'a str);

impl<'a> Declared<'a> {
    /// The upstream name of the MCP tool `binding` calls, if it calls one.
    pub(crate) fn tool_name(binding: &'a McpExecutionBinding) -> Option<Self> {
        binding.tool_name().map(Self)
    }

    /// The route template of the HTTP operation `binding` sends.
    pub(crate) fn path_template(binding: &'a HttpExecutionBinding) -> Self {
        Self(binding.path_template())
    }
}

/// Coral imported it with the Provider's declaration, so a Program cannot
/// choose it.
impl Identifier for Declared<'_> {}

impl fmt::Display for Declared<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

/// The host of a server Coral sends a request to.
pub(crate) struct Host<'a>(&'a str);

/// The declared server's host, or a redirect's that the declaration lets
/// Coral follow. A host holds no path, query or credential.
impl Identifier for Host<'_> {}

impl fmt::Display for Host<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

/// The host of `url`.
pub(crate) fn host(url: &url::Url) -> Option<Host<'_>> {
    url.host_str().map(Host)
}

/// The scheme of `url` as a span field: a web scheme's name, else `_OTHER`.
pub(crate) fn scheme(url: &url::Url) -> &'static str {
    match url.scheme() {
        "http" => "http",
        "https" => "https",
        _ => "_OTHER",
    }
}
