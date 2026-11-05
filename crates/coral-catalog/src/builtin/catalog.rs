//! PROTOTYPE (regex-search): `coral.workspace.catalog`. Hands a Program every
//! Tool and Saved Function the caller can call, as compact entries it filters
//! itself, with a regex or any other JavaScript. Not ranked, not paged.
//!
//! Throwaway: it answers whether a whole catalog in Program memory beats
//! ranked search for discovery. See the regex-search branch.

use coral_workspace::WorkspaceAction;
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ArgumentProblem, BuiltinTool, Declared, Definition, derived_schema, tool_result};
use crate::{ToolHints, ToolPresentation};

/// Any Principal that can inspect the Workspace reads what it can call.
pub(super) const NEEDS: WorkspaceAction = WorkspaceAction::Inspect;

pub(super) fn definition() -> Definition {
    Definition {
        key: BuiltinTool::Catalog.key(),
        summary: "Read every provider tool and Saved Function you can call, to filter in code",
        presentation: ToolPresentation::new(
            Some("Catalog".to_string()),
            "Return every provider tool and Saved Function you can call, in Call Path order, \
             as compact entries, with a summary of each Call Root: its tool count and groups. \
             Filter the entries in your Program with a regex or any other code, and return \
             only the entries you need."
                .to_string(),
            false,
            vec!["discovery".to_string()],
            Some(
                "Return providers to see what is there. Filter tools in the Program, for \
                 example tools.filter((t) => /pull/i.test(t.callPath + ' ' + t.description)). \
                 Never return tools or a slice of it unfiltered. Call an entry by its complete \
                 literal callPath. When complete is false, read its declaration with \
                 coral.workspace.search({ paths }) first."
                    .to_string(),
            ),
        ),
        // Read-only: the default Tool Access State is enabled.
        hints: ToolHints::new(Some(true), None, Some(true), None),
        input: derived_schema::<Arguments>(Contract::Deserialize),
        output: derived_schema::<CatalogResult>(Contract::Serialize),
        declared: Declared::Once,
    }
}

/// `catalog`'s arguments.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields, description = "")]
pub struct CatalogArguments {
    /// The Provider filter.
    #[serde(default, deserialize_with = "super::present")]
    #[schemars(
        with = "String",
        description = "A namespace from coral.providers.<namespace>, or coral.workspace for \
                       Coral's own tools. It narrows provider tools to one Provider; Saved \
                       Functions stay. Omit it to read every Provider."
    )]
    pub provider: Option<String>,
}

type Arguments = CatalogArguments;

impl CatalogArguments {
    /// Reads the arguments.
    ///
    /// # Errors
    ///
    /// Returns the argument to correct.
    pub fn parse(arguments: &Value) -> Result<Self, Vec<ArgumentProblem>> {
        if !arguments.is_object() {
            return Err(vec![ArgumentProblem::new("", "pass an object")]);
        }
        super::read(arguments).map_err(|problem| vec![problem])
    }
}

/// What `catalog` returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields, description = "")]
pub struct CatalogResult {
    /// What each Call Root holds, so a Program sees what is there without
    /// returning `tools`.
    #[schemars(
        description = "One summary per Call Root, in Call Path order: what is there, so you \
                       can choose a regex without returning tools."
    )]
    pub providers: Vec<CatalogProvider>,
    /// Every entry, in Call Path order.
    #[schemars(
        description = "Every provider tool and Saved Function you can call, in Call \
                              Path order."
    )]
    pub tools: Vec<CatalogEntry>,
}

impl CatalogResult {
    /// The Tool Result.
    #[must_use]
    pub fn into_tool_result(self) -> Value {
        tool_result(&self)
    }
}

/// What one Call Root holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "CatalogProvider", deny_unknown_fields, description = "")]
pub struct CatalogProvider {
    /// The Call Root.
    #[schemars(description = "coral.providers.<namespace>, coral.workspace, or \
                              coral.functions.")]
    pub call_root: String,
    /// How many entries it holds.
    #[schemars(description = "How many entries of tools are under it.")]
    pub tools: u32,
    /// The entry count under each next segment that has children.
    #[schemars(
        description = "The entry count under each next path segment that holds \
                              more than one level, such as { pulls: 27 } for \
                              coral.providers.github.pulls.*. Entries directly under the Call \
                              Root count only in tools."
    )]
    pub groups: std::collections::BTreeMap<String, u32>,
}

/// One compact catalog entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "CatalogEntry", deny_unknown_fields, description = "")]
pub struct CatalogEntry {
    /// The complete Call Path.
    #[schemars(description = "`coral.providers.<namespace>.…` for a provider tool, \
                       `coral.workspace.…` for one of Coral's own, `coral.functions.…` for a \
                       Saved Function.")]
    pub call_path: String,
    /// The provider-authored title, when it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        with = "String",
        description = "The tool's title; absent when it has none."
    )]
    pub title: Option<String>,
    /// The signature line.
    #[schemars(
        description = "The callable's input members one level deep, then its result. \
                              `…` marks what the line cut."
    )]
    pub signature: String,
    /// Whether the signature line is complete enough to call from.
    #[schemars(
        description = "Whether you can call from the signature alone. When false, read \
                              the declaration with coral.workspace.search({ paths }) first."
    )]
    pub complete: bool,
    /// The start of the description.
    #[schemars(
        description = "The first sentence of the description, at most 200 UTF-8 \
                              bytes."
    )]
    pub description: String,
}
