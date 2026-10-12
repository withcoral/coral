/// Private transport reachability, never a Principal role or resource permission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrivateAudience {
    /// Internal MCP support.
    Mcp,
    /// Human management through Reef.
    Reef,
}

impl PrivateAudience {
    /// Parses an exact private audience; unknown values grant no reachability.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "mcp" => Some(Self::Mcp),
            "reef" => Some(Self::Reef),
            _ => None,
        }
    }

    /// Returns the token and metadata spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mcp => "mcp",
            Self::Reef => "reef",
        }
    }
}
