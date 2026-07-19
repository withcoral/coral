//! Provider-facing Universal Search contracts.

use crate::search::result::{ProviderStatus, SearchCandidate};

#[derive(Debug, Clone)]
pub(crate) struct ProviderSearchOutcome {
    pub(crate) candidates: Vec<SearchCandidate>,
    pub(crate) status: ProviderStatus,
}
