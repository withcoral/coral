//! One bounded page over the fused result universe.

use std::sync::Arc;

use coral_db::Transaction;

use crate::cursor::encode_cursor;
use crate::retriever::{EntityRef, EntityRetriever, Hydrated, RequestContext};
use crate::{
    Error, ProviderCallRoot, RetrieverStatus, SEARCH_CURSOR_MAX_BYTES, SEARCH_PAGE_MAX_BYTES,
    SearchHit, SearchLimit, SearchPage,
};

const SEARCH_PAGE_BASE_BYTES: usize = SEARCH_CURSOR_MAX_BYTES + 8 * 1024;

/// One position of the result universe: the Entity and the retriever that
/// ranked or listed it, which alone can hydrate it.
pub(crate) struct Ranked {
    pub(crate) retriever_index: usize,
    pub(crate) entity: EntityRef,
}

/// Hydrates the page that starts at `start`, within the result limit and the
/// byte budget. An Entity that is dropped or unreadable is left out and the
/// page moves on; a page always advances by at least one position. A page
/// whose every remaining Entity was unreadable is unavailable rather than an
/// empty success; stale or vanished Entities give an empty page.
#[expect(
    clippy::too_many_arguments,
    reason = "page hydration binds one cursor to its complete request context"
)]
pub(crate) async fn hydrate_page_in_transaction(
    transaction: &mut Transaction<'_>,
    context: RequestContext<'_>,
    retrievers: &[Arc<dyn EntityRetriever>],
    ranked: &[Ranked],
    start: usize,
    limit: SearchLimit,
    normalized_query: Option<&str>,
    provider_filter: Option<&ProviderCallRoot>,
    retriever_statuses: Vec<RetrieverStatus>,
) -> Result<SearchPage, Error> {
    let mut hits = Vec::<SearchHit>::new();
    let mut encoded_bytes = SEARCH_PAGE_BASE_BYTES;
    let mut position = start;
    let mut every_candidate_unreadable = true;
    while hits.len() < limit.get() {
        let Some(next) = ranked.get(position) else {
            break;
        };
        let retriever = retrievers
            .get(next.retriever_index)
            .ok_or(Error::ServiceUnavailable)?;
        let item = match retriever
            .hydrate(transaction, context, &next.entity)
            .await?
        {
            Hydrated::Hit(item) => *item,
            Hydrated::Dropped => {
                every_candidate_unreadable = false;
                position += 1;
                continue;
            }
            Hydrated::Unreadable => {
                position += 1;
                continue;
            }
        };
        let item_bytes = item.encoded_size_upper_bound();
        if encoded_bytes.saturating_add(item_bytes) > SEARCH_PAGE_MAX_BYTES {
            if hits.is_empty() {
                return Err(Error::ServiceUnavailable);
            }
            break;
        }
        encoded_bytes = encoded_bytes.saturating_add(item_bytes);
        hits.push(item);
        position += 1;
    }
    if hits.is_empty()
        && every_candidate_unreadable
        && start < ranked.len()
        && position == ranked.len()
    {
        return Err(Error::ServiceUnavailable);
    }
    let has_more = position < ranked.len();
    let next_cursor = has_more
        .then(|| {
            encode_cursor(
                position,
                context.actor,
                context.workspace_id,
                normalized_query,
                provider_filter,
            )
        })
        .transpose()?;
    Ok(SearchPage::new(
        hits,
        next_cursor,
        has_more,
        retriever_statuses,
    ))
}
