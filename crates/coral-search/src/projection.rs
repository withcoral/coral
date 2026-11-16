//! Safe discovery responses: what discovery found, as the result types
//! coral-catalog declares for `coral.search`. The `search` and
//! `describe` records Task history retains hold the same types as JSON, so
//! no surface can show a field another does not.
use crate::{
    ApprovalRequirement, AuthoringDescription, Discovery, ExactAuthoringDescription, ExactLookup,
    RetrieverState, RetrieverStatus, SearchHit, SearchPage,
};
use coral_catalog::{
    DeclaredHints, Description, DescriptionAuthoring, MissingPath, PathsResult, SavedDetail,
    SavedDiagnostic, SavedDiagnosticCategory, SearchOutput, SearchResult, SearchRetriever,
    SearchRetrieverCoverage, SearchRetrieverState, SearchRetrieverStatus, ToolHints,
};
use coral_tools::{AuthoredToolDiagnostic, AuthoredToolDiagnosticCategory};
use serde_json::{Value, json};

#[cfg(test)]
mod tests;

/// What one discovery request found.
#[must_use]
pub fn discovery(found: &Discovery) -> SearchOutput {
    match found {
        Discovery::Page(page) => SearchOutput::Page(search_result(page)),
        Discovery::Exact(lookup) => SearchOutput::Paths(paths_result(lookup)),
    }
}

/// Projects every ordered hit, and only what the caller acts on besides: the
/// next offset while more remain, and the status of each retriever whose
/// results may be missing.
#[must_use]
pub fn search_result(page: &SearchPage) -> SearchResult {
    SearchResult {
        hits: page.hits().iter().map(hit).collect(),
        next_offset: page.next_offset(),
        retriever_statuses: page
            .retriever_statuses()
            .iter()
            .filter_map(retriever_status)
            .collect(),
    }
}

/// [`search_result`] as the JSON Task history retains.
#[must_use]
pub fn search(page: &SearchPage) -> Value {
    json!(search_result(page))
}

fn paths_result(lookup: &ExactLookup) -> PathsResult {
    PathsResult {
        descriptions: lookup.descriptions().iter().map(description).collect(),
        missing: lookup
            .missing()
            .iter()
            .map(|missing| MissingPath {
                call_path: missing.call_path().to_owned(),
                suggestions: missing.suggestions().to_vec(),
            })
            .collect(),
    }
}

/// One Search Hit, cut to what the caller needs to choose and call it. The
/// Call Path already names the kind and the Provider, and an exact lookup
/// gives the rest. A flag appears only when set, a hint only when declared.
/// Nothing says why the hit matched.
fn hit(hit: &SearchHit) -> coral_catalog::SearchHit {
    let tool = hit.tool();
    coral_catalog::SearchHit {
        call_path: hit.call_path().as_str().to_owned(),
        signature_line: hit.signature_line().to_owned(),
        complete: hit.complete(),
        description_snippet: hit.description_snippet().to_owned(),
        hints: tool
            .map(crate::ToolHit::hints)
            .filter(|hints| *hints != ToolHints::default())
            .map(DeclaredHints::from),
        approval_required: tool
            .is_some_and(|tool| tool.approval_requirement() == ApprovalRequirement::Required),
        deprecated: tool.is_some_and(crate::ToolHit::deprecated),
    }
}

/// The status of a retriever whose results may be missing; `None` for one
/// that searched everything.
fn retriever_status(status: &RetrieverStatus) -> Option<SearchRetrieverStatus> {
    let state = match status.state() {
        RetrieverState::Partial => SearchRetrieverState::Partial,
        RetrieverState::Error => SearchRetrieverState::Error,
        RetrieverState::ResultsFound | RetrieverState::Empty | RetrieverState::Skipped => {
            return None;
        }
    };
    let coverage = status.coverage();
    Some(SearchRetrieverStatus {
        retriever: match status.retriever() {
            crate::EntityKind::ProviderTool => SearchRetriever::ProviderTool,
            crate::EntityKind::PrivateTool => SearchRetriever::PrivateTool,
            crate::EntityKind::SharedTool => SearchRetriever::SharedTool,
        },
        state,
        coverage: SearchRetrieverCoverage {
            eligible_units: coverage.eligible_units(),
            searched_units: coverage.searched_units(),
            failed_units: coverage.failed_units(),
            returned_count: coverage.returned_count(),
            has_more: coverage.has_more(),
        },
    })
}

/// Projects the complete exact response, including an Authored Tool's source
/// and stored diagnostics.
#[must_use]
pub fn description(exact: &ExactAuthoringDescription) -> Description {
    match exact.authoring() {
        AuthoringDescription::Tool(tool) => Description::ProviderTool {
            authoring: authoring(exact.authoring()),
            provider_access: crate::provider_filter_value(tool.provider_call_root()).to_owned(),
            provider_id: tool.provider_id().to_string(),
            provider_display_name: tool.provider_display_name().to_owned(),
            title: tool.title().map(str::to_owned),
            deprecated: tool.deprecated(),
            tags: tool.tags().to_vec(),
            guidance: tool.guidance().map(str::to_owned),
            hints: DeclaredHints::from(tool.hints()),
            approval_required: tool.approval_requirement() == ApprovalRequirement::Required,
        },
        AuthoringDescription::AuthoredTool(tool) => {
            let authoring = authoring(exact.authoring());
            let provider_id = tool.provider_id().to_string();
            let provider_display_name = tool.provider_display_name().to_owned();
            // An Authored Tool declares no hints.
            let hints = DeclaredHints::from(coral_catalog::ToolHints::default());
            let approval_required = tool.approval_requirement() == ApprovalRequirement::Required;
            let revision = tool.revision();
            let detail = exact.authored_tool_detail().map(|detail| SavedDetail {
                source: detail.source().as_str().to_owned(),
                diagnostics: detail.diagnostics().iter().map(diagnostic).collect(),
                diagnostics_truncated: detail.diagnostics_truncated(),
            });
            match tool.kind() {
                // The kind keeps the name retained Task history uses.
                coral_catalog::AuthoredToolKind::Private => Description::PrivateTool {
                    authoring,
                    provider_id,
                    provider_display_name,
                    hints,
                    approval_required,
                    revision,
                    detail,
                },
                coral_catalog::AuthoredToolKind::Shared => Description::SharedTool {
                    authoring,
                    provider_id,
                    provider_display_name,
                    hints,
                    approval_required,
                    revision,
                    detail,
                },
            }
        }
    }
}

/// [`description`] as the JSON Task history retains.
#[must_use]
pub fn exact_description(exact: &ExactAuthoringDescription) -> Value {
    json!(description(exact))
}

fn authoring(description: &AuthoringDescription) -> DescriptionAuthoring {
    DescriptionAuthoring {
        call_path: description.call_path().as_str().to_owned(),
        description: description.description().to_owned(),
        typescript_declaration: description.typescript_declaration().to_owned(),
    }
}

/// One saved diagnostic as a Program reads it.
#[must_use]
pub fn diagnostic(diagnostic: &AuthoredToolDiagnostic) -> SavedDiagnostic {
    SavedDiagnostic {
        category: match diagnostic.category() {
            AuthoredToolDiagnosticCategory::Message => SavedDiagnosticCategory::Message,
            AuthoredToolDiagnosticCategory::Warning => SavedDiagnosticCategory::Warning,
            AuthoredToolDiagnosticCategory::Error => SavedDiagnosticCategory::Error,
            AuthoredToolDiagnosticCategory::Suggestion => SavedDiagnosticCategory::Suggestion,
        },
        code: diagnostic.code(),
        message: diagnostic.message().to_owned(),
        line: diagnostic.line(),
        column: diagnostic.column(),
    }
}
