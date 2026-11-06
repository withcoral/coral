//! Safe discovery responses: what discovery found, as the result types
//! coral-catalog declares for `coral.workspace.search`. The `search` and
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
use coral_functions::{SavedFunctionDiagnostic, SavedFunctionDiagnosticCategory};
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

/// PROTOTYPE (regex-search): the compact entries `coral.workspace.catalog`
/// returns.
#[must_use]
pub fn catalog(hits: &[SearchHit]) -> coral_catalog::CatalogResult {
    coral_catalog::CatalogResult {
        providers: catalog_summary(hits.iter().map(|hit| hit.call_path().as_str())),
        tools: hits
            .iter()
            .map(|hit| coral_catalog::CatalogEntry {
                call_path: hit.call_path().as_str().to_owned(),
                title: hit
                    .tool()
                    .and_then(crate::ToolHit::title)
                    .map(str::to_owned),
                signature: hit.signature_line().to_owned(),
                complete: hit.complete(),
                description: first_sentence(hit.description_snippet(), 200),
            })
            .collect(),
    }
}

/// PROTOTYPE (regex-search): one summary per Call Root, in Call Path order:
/// its entry count, and the entry count under each next segment that holds
/// more than one level.
#[must_use]
pub fn catalog_summary<'a>(
    call_paths: impl IntoIterator<Item = &'a str>,
) -> Vec<coral_catalog::CatalogProvider> {
    let mut providers = std::collections::BTreeMap::<String, coral_catalog::CatalogProvider>::new();
    for call_path in call_paths {
        let segments: Vec<&str> = call_path.split('.').collect();
        // coral.providers.<namespace>, or coral.workspace / coral.functions.
        let root_len = if segments.get(1) == Some(&"providers") {
            3
        } else {
            2
        };
        let Some(root) = segments.get(..root_len) else {
            continue;
        };
        let call_root = root.join(".");
        let provider =
            providers
                .entry(call_root.clone())
                .or_insert_with(|| coral_catalog::CatalogProvider {
                    call_root,
                    tools: 0,
                    groups: std::collections::BTreeMap::new(),
                });
        provider.tools += 1;
        if let Some(rest) = segments.get(root_len..)
            && rest.len() >= 2
            && let Some(group) = rest.first()
        {
            *provider.groups.entry((*group).to_owned()).or_insert(0) += 1;
        }
    }
    providers.into_values().collect()
}

/// The first sentence of `text`, cut at a character boundary to at most
/// `max` bytes.
fn first_sentence(text: &str, max: usize) -> String {
    let text = text.trim();
    let sentence = text
        .split_once(". ")
        .map_or(text, |(first, _rest)| first)
        .trim_end();
    if sentence.len() <= max {
        return sentence.to_owned();
    }
    let cut: String = sentence
        .char_indices()
        .take_while(|(index, character)| index + character.len_utf8() <= max)
        .map(|(_index, character)| character)
        .collect();
    format!("{cut}…")
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
/// Call Path already names the kind and the Provider, the rank already
/// reflects the evidence, and an exact lookup gives the rest. A flag appears
/// only when set, a hint only when declared, and a match only where a query
/// term matched.
fn hit(hit: &SearchHit) -> coral_catalog::SearchHit {
    let tool = hit.tool();
    coral_catalog::SearchHit {
        call_path: hit.call_path().as_str().to_owned(),
        signature_line: hit.signature_line().to_owned(),
        complete: hit.complete(),
        description_snippet: hit.description_snippet().to_owned(),
        guidance_snippet: hit.guidance_snippet().map(str::to_owned),
        matched_tags: hit.matched_tags().to_vec(),
        matched_inputs: matched_inputs(hit).into_iter().map(str::to_owned).collect(),
        hints: tool
            .map(crate::ToolHit::hints)
            .filter(|hints| *hints != ToolHints::default())
            .map(DeclaredHints::from),
        approval_required: tool
            .is_some_and(|tool| tool.approval_requirement() == ApprovalRequirement::Required),
        deprecated: tool.is_some_and(crate::ToolHit::deprecated),
    }
}

/// The top-level arguments a query term matched that the cut signature line
/// does not show, in name order: a long argument can push the one the agent
/// needs past the cut. Only the input counts, since the result can show a
/// member of the same name.
fn matched_inputs(hit: &SearchHit) -> Vec<&str> {
    let line = hit.signature_input();
    let mut names = hit
        .evidence()
        .paths()
        .iter()
        .filter_map(|path| path.strip_prefix("input."))
        .filter_map(|rest| rest.split(['.', '[']).next())
        .filter(|name| {
            !["{ ", "; "].iter().any(|before| {
                line.contains(&format!("{before}{name}:"))
                    || line.contains(&format!("{before}{name}?:"))
            })
        })
        .collect::<Vec<_>>();
    names.sort_unstable();
    names.dedup();
    names
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
            crate::EntityKind::SavedFunction => SearchRetriever::SavedFunction,
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

/// Projects the complete exact response, including owned source and saved
/// diagnostics.
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
        AuthoringDescription::SavedFunction(_function) => Description::SavedFunction {
            authoring: authoring(exact.authoring()),
            detail: exact.saved_function_detail().map(|detail| SavedDetail {
                source: detail.source().as_str().to_owned(),
                diagnostics: detail.diagnostics().iter().map(diagnostic).collect(),
                diagnostics_truncated: detail.diagnostics_truncated(),
            }),
        },
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

fn diagnostic(diagnostic: &SavedFunctionDiagnostic) -> SavedDiagnostic {
    SavedDiagnostic {
        category: match diagnostic.category() {
            SavedFunctionDiagnosticCategory::Message => SavedDiagnosticCategory::Message,
            SavedFunctionDiagnosticCategory::Warning => SavedDiagnosticCategory::Warning,
            SavedFunctionDiagnosticCategory::Error => SavedDiagnosticCategory::Error,
            SavedFunctionDiagnosticCategory::Suggestion => SavedDiagnosticCategory::Suggestion,
        },
        code: diagnostic.code(),
        message: diagnostic.message().to_owned(),
        line: diagnostic.line(),
        column: diagnostic.column(),
    }
}
