//! App-private catalog resolution models.

use std::collections::{BTreeMap, BTreeSet};

use coral_engine::CatalogInfo;

/// Query-visible catalog metadata and the sources omitted while resolving it.
#[derive(Debug)]
pub(crate) struct CatalogResolution {
    pub(crate) catalog: CatalogInfo,
    pub(crate) failed_source_names: BTreeSet<String>,
    /// Top-level SQL schema or catalog name to canonical installed source owner.
    pub(crate) runtime_namespace_owners: BTreeMap<String, String>,
}
