//! Performs `coral.search`: Universal Search's one discovery
//! operation as the invoking Principal, and the only way an agent discovers.
//! The search is this Invocation of this Run, recorded like any other, and
//! nothing else.

use coral_catalog::{ProviderArgument, SearchArguments};
use coral_search::{DiscoveryRequest, ExactPaths, SearchInput, SearchRequest};

use super::BuiltinHost;
use crate::error::InputProblem;
use crate::{CoralFailure, Error, Rejection};

/// `search`'s arguments in Universal Search's types.
#[derive(Clone)]
pub(crate) struct Search {
    request: DiscoveryRequest,
    /// The `provider` argument as written, which names the values a refusal
    /// points at.
    provider: Option<ProviderArgument>,
}

impl Search {
    /// The call for arguments the contract accepted, with each value
    /// Universal Search checks itself, so a Program learns which to correct
    /// before any other work. The namespaces are checked against the
    /// Workspace when the search runs.
    pub(super) fn new(arguments: SearchArguments) -> Result<Self, Rejection> {
        let mut written = None;
        let request = match arguments {
            SearchArguments::Query {
                query,
                provider,
                limit,
                offset,
            } => {
                let input = SearchInput {
                    query,
                    provider: provider
                        .as_ref()
                        .map(ProviderArgument::values)
                        .unwrap_or_default(),
                    limit,
                    offset,
                };
                written = provider;
                // Each value alone, so a refusal names the one to correct.
                // Any offset reads: one past the end is an empty last page.
                let alone = [
                    (
                        "query",
                        SearchInput {
                            query: input.query.clone(),
                            ..SearchInput::default()
                        },
                    ),
                    (
                        "limit",
                        SearchInput {
                            limit: input.limit,
                            ..SearchInput::default()
                        },
                    ),
                ];
                let problems = alone
                    .into_iter()
                    .filter(|(_field, alone)| SearchRequest::try_from(alone.clone()).is_err())
                    .map(|(field, _alone)| InputProblem::new(field, value_reason(field)))
                    .collect::<Vec<_>>();
                if !problems.is_empty() {
                    return Err(Rejection::invalid_inputs(problems));
                }
                DiscoveryRequest::Query(
                    SearchRequest::try_from(input)
                        .map_err(|_error| Rejection::from(CoralFailure::InvalidInput))?,
                )
            }
            SearchArguments::Paths { paths, types } => DiscoveryRequest::Paths(
                ExactPaths::new(paths, types).map_err(|error| match error {
                    coral_search::Error::TypesLimit => rejection("types", search_reason(&error)),
                    // The contract bounds the paths with the same limits.
                    _ => Rejection::from(CoralFailure::InvalidInput),
                })?,
            ),
        };
        Ok(Self {
            request,
            provider: written,
        })
    }

    /// Discovers as the invoking Principal. A path it cannot discover is in
    /// the result's `missing`, not a failure.
    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        let found = host
            .search
            .discover(host.actor, host.workspace_id, &self.request)
            .await
            .map_err(|error| failure(error, self.provider.as_ref()))?;
        let output = coral_search::projection::discovery(&found);
        // As MCP `describe` does: fail rather than hand the Program a number
        // V8 rounds. Only corrupt stored data holds one.
        if !output.is_exact_in_json() {
            return Err(Error::CorruptData(
                "a discovery result holds an integer past the exact JSON range".to_owned(),
            ));
        }
        Ok(output.into_tool_result())
    }
}

/// What to correct in a value Universal Search refused. It never repeats
/// the value.
fn value_reason(field: &str) -> String {
    match field {
        "query" => format!(
            "pass keywords that are not blank, with no null character, in at most {} UTF-8 bytes",
            coral_search::SEARCH_QUERY_MAX_BYTES
        ),
        "provider" => "pass a namespace from coral.providers.<namespace>, or coral".to_owned(),
        _ => format!("pass 1 to {}", coral_search::SEARCH_RESULT_LIMIT_MAX),
    }
}

/// The reason a refused discovery gives, which names only what the caller
/// can use: the Providers it can search, or the types a path declares.
fn search_reason(error: &coral_search::Error) -> String {
    error.safe_rejection().map_or_else(
        || "has a value Coral does not accept".to_owned(),
        |(_code, reason)| reason,
    )
}

fn rejection(field: &str, reason: String) -> Rejection {
    Rejection::invalid_inputs(vec![InputProblem::new(field, reason)])
}

/// A failure the Program can read and correct, or a host error. A refused
/// namespace is named by its place in the `provider` argument as written,
/// and the namespaces to use come once.
fn failure(error: coral_search::Error, provider: Option<&ProviderArgument>) -> Error {
    match error {
        coral_search::Error::UnknownProviderCallRoot {
            ref searchable,
            ref unmatched,
        } => {
            let field = |position: &usize| {
                provider.map_or_else(
                    || "provider".to_owned(),
                    |provider| provider.field(*position),
                )
            };
            let problems = match unmatched.as_slice() {
                [position] => vec![InputProblem::new(field(position), search_reason(&error))],
                positions => positions
                    .iter()
                    .map(|position| {
                        InputProblem::new(field(position), coral_search::UNKNOWN_NAMESPACE_REASON)
                    })
                    .chain([InputProblem::new(
                        "",
                        coral_search::searchable_namespaces(searchable),
                    )])
                    .collect(),
            };
            Error::InvocationRejected(Rejection::invalid_inputs(problems))
        }
        coral_search::Error::Workspace(error) if crate::enforcement::loses_authority(&error) => {
            Error::InvocationRejected(CoralFailure::AuthorityLost.into())
        }
        coral_search::Error::PermissionDenied(_) => {
            Error::InvocationRejected(CoralFailure::AuthorityLost.into())
        }
        coral_search::Error::UndeclaredTypes { .. } | coral_search::Error::TypesLimit => {
            Error::InvocationRejected(rejection("types", search_reason(&error)))
        }
        coral_search::Error::ServiceUnavailable => {
            Error::InvocationRejected(CoralFailure::ToolUnavailable.into())
        }
        coral_search::Error::Workspace(error) => error.into(),
        coral_search::Error::Provider(error) => error.into(),
        coral_search::Error::Policy(error) => error.into(),
        coral_search::Error::Tools(error) => error.into(),
        coral_search::Error::Database(error) => error.into(),
        // Neither reaches here: the call checked its input, and a lookup
        // reports an absent path in `missing`.
        coral_search::Error::InvalidInput(_)
        | coral_search::Error::AuthoringDescriptionNotFound => {
            Error::InvocationRejected(CoralFailure::InvalidInput.into())
        }
    }
}
