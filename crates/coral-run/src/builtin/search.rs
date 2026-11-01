//! Performs `coral.workspace.search`: Universal Search as the invoking
//! Principal, with the page and descriptions the MCP `search` and `describe`
//! tools returned.

use coral_catalog::{SearchArguments, search_tool_result};
use coral_search::{CallPath, SearchInput, SearchRequest, UniversalSearchModule, projection};
use serde_json::{Value, json};

use super::BuiltinHost;
use crate::{CoralFailure, Error, Rejection};

/// How many bytes of descriptions a query with `declarations` adds to a page.
/// The top hit always gets one, so one call can always read the best match.
const DECLARATIONS_MAX_BYTES: usize = 32 * 1024;

pub(super) async fn perform(
    arguments: &SearchArguments,
    host: &BuiltinHost<'_>,
) -> Result<Value, Error> {
    let Some(search) = host.search else {
        return Err(Error::InvocationRejected(
            CoralFailure::ToolUnavailable.into(),
        ));
    };
    let data = match arguments {
        SearchArguments::Ranked {
            query,
            provider,
            limit,
            cursor,
            declarations,
        } => {
            let input = SearchInput {
                query: query.clone(),
                provider: provider.clone(),
                limit: *limit,
                cursor: cursor.clone(),
            };
            let request = SearchRequest::try_from(input).map_err(failure)?;
            let page = search
                .search(host.actor, host.workspace_id, &request)
                .await
                .map_err(failure)?;
            let mut data = projection::search(&page);
            if *declarations {
                add_descriptions(search, host, &mut data).await?;
            }
            data
        }
        SearchArguments::Exact { paths, types } => exact(search, host, paths, types).await?,
    };
    Ok(search_tool_result(&data))
}

/// Adds each hit's description, best first, until the next one would pass
/// [`DECLARATIONS_MAX_BYTES`]. A hit that left discovery since the page was read
/// stays a summary.
async fn add_descriptions(
    search: &UniversalSearchModule,
    host: &BuiltinHost<'_>,
    data: &mut Value,
) -> Result<(), Error> {
    let Some(hits) = data.get_mut("hits").and_then(Value::as_array_mut) else {
        return Ok(());
    };
    let mut used = 0;
    for hit in hits {
        let Some(path) = hit
            .get("call_path")
            .and_then(Value::as_str)
            .and_then(|path| CallPath::parse(path).ok())
        else {
            continue;
        };
        let Some(description) = describe(search, host, &path, &[]).await? else {
            continue;
        };
        let size = description.to_string().len();
        if used > 0 && used + size > DECLARATIONS_MAX_BYTES {
            break;
        }
        used += size;
        hit["description"] = description;
    }
    Ok(())
}

/// One hit for each path Coral can describe, and the rest in `missing`.
async fn exact(
    search: &UniversalSearchModule,
    host: &BuiltinHost<'_>,
    paths: &[String],
    types: &[String],
) -> Result<Value, Error> {
    let mut hits = Vec::with_capacity(paths.len());
    let mut missing = Vec::new();
    for raw in paths {
        let description = match CallPath::parse(raw) {
            Ok(path) => describe(search, host, &path, types).await?,
            Err(_error) => None,
        };
        match description {
            Some(description) => {
                hits.push(json!({ "call_path": raw, "description": description }));
            }
            None => missing.push(raw),
        }
    }
    Ok(if missing.is_empty() {
        json!({ "hits": hits, "has_more": false })
    } else {
        json!({ "hits": hits, "has_more": false, "missing": missing })
    })
}

/// The description at `path`, or `None` when the Principal cannot reach it.
async fn describe(
    search: &UniversalSearchModule,
    host: &BuiltinHost<'_>,
    path: &CallPath,
    types: &[String],
) -> Result<Option<Value>, Error> {
    match search
        .describe(host.actor, host.workspace_id, path, types)
        .await
    {
        Ok(description) => Ok(Some(projection::exact_description(&description))),
        Err(coral_search::Error::AuthoringDescriptionNotFound) => Ok(None),
        Err(error) => Err(failure(error)),
    }
}

/// A failure the Program can read and correct, or a host error. An unknown
/// Provider's message lists what the caller can search, and an undeclared type's
/// lists what the declaration declares.
fn failure(error: coral_search::Error) -> Error {
    match error {
        coral_search::Error::Workspace(error) if crate::enforcement::loses_authority(&error) => {
            Error::InvocationRejected(CoralFailure::AuthorityLost.into())
        }
        coral_search::Error::Workspace(error) => error.into(),
        coral_search::Error::Provider(error) => error.into(),
        coral_search::Error::Policy(error) => error.into(),
        coral_search::Error::Functions(error) => error.into(),
        coral_search::Error::Database(error) => error.into(),
        coral_search::Error::ServiceUnavailable | coral_search::Error::PermissionDenied(_) => {
            Error::InvocationRejected(CoralFailure::ToolUnavailable.into())
        }
        error => {
            let reason = error.safe_rejection().map_or_else(
                || "Discovery input is invalid".to_owned(),
                |(_code, reason)| reason,
            );
            Error::InvocationRejected(Rejection::invalid_input("", reason))
        }
    }
}
