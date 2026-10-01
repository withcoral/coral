//! App-private catalog resolution models.

use std::collections::BTreeSet;

use coral_engine::CatalogInfo;

/// Query-visible catalog metadata and the sources omitted while resolving it.
#[derive(Debug)]
pub(crate) struct CatalogResolution {
    pub(crate) catalog: CatalogInfo,
    pub(crate) failed_source_names: BTreeSet<String>,
}
