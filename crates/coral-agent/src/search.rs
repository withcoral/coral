//! The model's `search` tool.

use coral_catalog::{
    Description, PathsResult, SearchArguments, SearchHit, SearchOutput, SearchResult,
    SearchRetriever, SearchRetrieverState,
};
use coral_run::RunId;
use coral_search::{
    DiscoveryRequest, ExactPaths, SearchInput, SearchRequest, UniversalSearchModule,
};
use serde::Serialize;
use serde_json::json;

use crate::run_tools::{Caller, RunTools, shortened};

/// Runs one tool call of the model's, `search` here and the rest in
/// `run_tools`, and shortens its result.
pub(crate) async fn call(
    module: &UniversalSearchModule,
    run_tools: &RunTools,
    caller: &Caller,
    name: &str,
    arguments: &str,
) -> (String, Option<RunId>) {
    let (text, run_id) = match name {
        "search" => (search(module, caller, arguments).await, None),
        _ => run_tools.call(caller, name, arguments).await,
    };
    (shortened(text), run_id)
}

/// Core's discovery as the lead's caller, written as the model reads it.
async fn search(module: &UniversalSearchModule, caller: &Caller, arguments: &str) -> String {
    let request = match request(arguments) {
        Ok(request) => request,
        Err((code, message)) => return error(code, &message),
    };
    let found = module
        .discover(&caller.principal, &caller.workspace_id, &request)
        .await;
    match found.as_ref().map(coral_search::projection::discovery) {
        Ok(SearchOutput::Page(page)) => json!(Page::from(&page)).to_string(),
        Ok(SearchOutput::Paths(paths)) => json!(Paths::from(&paths)).to_string(),
        Err(failure) => {
            let (code, message) = reason(failure);
            error(code, &message)
        }
    }
}

/// A page of hits.
#[derive(Serialize)]
struct Page<'a> {
    hits: Vec<Hit<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_offset: Option<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    retriever_statuses: Vec<RetrieverStatus>,
}

#[derive(Serialize)]
struct Hit<'a> {
    call_path: &'a str,
    signature_line: &'a str,
    complete: bool,
    description_snippet: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    hints: Option<Hints>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    approval_required: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    deprecated: bool,
}

#[derive(Serialize)]
struct Hints {
    #[serde(skip_serializing_if = "Option::is_none")]
    read_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    destructive: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    idempotent: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    open_world: Option<bool>,
}

/// A retriever whose results may be missing from the page.
#[derive(Serialize)]
struct RetrieverStatus {
    retriever: &'static str,
    state: &'static str,
}

/// The declarations of the named paths, as core writes them, and the paths
/// that name nothing.
#[derive(Serialize)]
struct Paths<'a> {
    descriptions: &'a [Description],
    missing: Vec<Missing<'a>>,
}

#[derive(Serialize)]
struct Missing<'a> {
    call_path: &'a str,
    suggestions: &'a [String],
}

impl<'a> From<&'a SearchResult> for Page<'a> {
    fn from(page: &'a SearchResult) -> Self {
        let hits = page.hits.iter();
        let statuses = page.retriever_statuses.iter();
        Self {
            hits: hits.map(Hit::from).collect(),
            next_offset: page.next_offset,
            retriever_statuses: statuses
                .map(|status| RetrieverStatus {
                    retriever: match status.retriever {
                        SearchRetriever::ProviderTool => "provider_tool",
                        SearchRetriever::PrivateTool => "private_tool",
                        SearchRetriever::SharedTool => "shared_tool",
                    },
                    state: match status.state {
                        SearchRetrieverState::Partial => "partial",
                        SearchRetrieverState::Error => "error",
                    },
                })
                .collect(),
        }
    }
}

impl<'a> From<&'a SearchHit> for Hit<'a> {
    fn from(hit: &'a SearchHit) -> Self {
        Self {
            call_path: &hit.call_path,
            signature_line: &hit.signature_line,
            complete: hit.complete,
            description_snippet: &hit.description_snippet,
            hints: hit.hints.map(|hints| Hints {
                read_only: hints.read_only,
                destructive: hints.destructive,
                idempotent: hints.idempotent,
                open_world: hints.open_world,
            }),
            approval_required: hit.approval_required,
            deprecated: hit.deprecated,
        }
    }
}

impl<'a> From<&'a PathsResult> for Paths<'a> {
    fn from(found: &'a PathsResult) -> Self {
        let missing = found.missing.iter().map(|missing| Missing {
            call_path: &missing.call_path,
            suggestions: &missing.suggestions,
        });
        Self {
            descriptions: &found.descriptions,
            missing: missing.collect(),
        }
    }
}

/// The model's arguments as a discovery request, or what to correct.
fn request(arguments: &str) -> Result<DiscoveryRequest, (&'static str, String)> {
    let invalid = |message: String| ("invalid_arguments", message);
    let arguments = serde_json::from_str(arguments)
        .map_err(|_not_json| invalid("Pass the arguments as a JSON object.".to_owned()))?;
    let arguments = SearchArguments::parse(&arguments).map_err(|problems| {
        let problems = problems
            .iter()
            .map(|problem| format!("{}: {}", problem.field(), problem.reason()));
        invalid(problems.collect::<Vec<_>>().join("; "))
    })?;
    let request = match arguments {
        SearchArguments::Query {
            query,
            provider,
            limit,
            offset,
        } => {
            let provider = provider.map(|provider| provider.values());
            let input = SearchInput {
                query,
                provider: provider.unwrap_or_default(),
                limit,
                offset,
            };
            SearchRequest::try_from(input).map(DiscoveryRequest::Query)
        }
        SearchArguments::Paths { paths, types } => {
            ExactPaths::new(paths, types).map(DiscoveryRequest::Paths)
        }
    };
    request.map_err(|failure| reason(&failure))
}

/// The code and text that core marks safe to show, or a general message.
fn reason(failure: &coral_search::Error) -> (&'static str, String) {
    failure
        .safe_rejection()
        .unwrap_or_else(|| ("internal_error", "Search failed. Try again.".to_owned()))
}

/// A failure in the shape that `exec`, `wait` and `cancel` write one, recorded on the call's span.
fn error(code: &'static str, message: &str) -> String {
    coral_telemetry::record_failure(&tracing::Span::current(), code);
    json!({"error": {"code": code, "message": message}}).to_string()
}
