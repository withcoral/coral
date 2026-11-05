//! Lazy administrative catalog projection. This projection deliberately has no
//! executable recipes, authoritative schemas, or TypeScript declarations.
use crate::{CatalogDiagnostic, ToolAvailability, ToolHints, ToolKey, ToolPolicyFacts};

/// Safe catalog facts needed by an administrative tree and its summary pane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolSummary {
    /// Provider-local Tool identity.
    pub key: ToolKey,
    /// Tool-relative Call Path projection.
    pub relative_path: String,
    /// Provider-authored summary.
    pub summary: String,
    /// Safe Provider-authored prose, without schemas or executable material.
    pub description: String,
    /// Provider-authored grouping tags, in source order.
    pub tags: Vec<String>,
    /// Effect hints the Provider declared.
    pub hints: ToolHints,
    /// Structural availability, independent of caller readiness.
    pub availability: ToolAvailability,
    /// Safe structural diagnostics, including unavailable Tools.
    pub diagnostics: Vec<CatalogDiagnostic>,
}

/// One Tool's row in an administrative table: what the row shows and what its
/// policy default needs. Prose stays with [`ToolSummary`], so a whole catalog
/// can list without reading it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolListing {
    /// Provider-local Tool identity.
    pub key: ToolKey,
    /// Tool-relative Call Path projection.
    pub relative_path: String,
    /// Provider-authored grouping tags, in source order.
    pub tags: Vec<String>,
    /// The declared effect hints, the method of an HTTP Tool and any default
    /// the catalog declares: what the row shows and its policy default.
    pub policy: ToolPolicyFacts,
    /// Structural availability, independent of caller readiness.
    pub availability: ToolAvailability,
    /// Safe structural diagnostics, including unavailable Tools.
    pub diagnostics: Vec<CatalogDiagnostic>,
}
