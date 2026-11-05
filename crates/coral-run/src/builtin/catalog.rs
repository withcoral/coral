//! PROTOTYPE (regex-search): performs `coral.workspace.catalog`, which hands
//! the Program every Tool and Saved Function the invoking Principal can call,
//! for the Program to filter itself.

use coral_catalog::CatalogArguments;
use coral_search::{SearchInput, SearchRequest};

use super::BuiltinHost;
use super::search::failure;
use crate::error::InputProblem;
use crate::{Error, Rejection};

/// `catalog`'s arguments in Universal Search's types.
#[derive(Clone)]
pub(crate) struct Catalog {
    request: SearchRequest,
}

impl Catalog {
    pub(super) fn new(arguments: CatalogArguments) -> Result<Self, Rejection> {
        let request = SearchRequest::try_from(SearchInput {
            provider: arguments.provider,
            ..SearchInput::default()
        })
        .map_err(|_error| {
            Rejection::invalid_inputs(vec![InputProblem::new(
                "provider",
                "pass a namespace from coral.providers.<namespace>, or coral.workspace".to_owned(),
            )])
        })?;
        Ok(Self { request })
    }

    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        let hits = host
            .search
            .catalog(host.actor, host.workspace_id, &self.request)
            .await
            .map_err(failure)?;
        Ok(coral_search::projection::catalog(&hits).into_tool_result())
    }
}
