//! App-private runtime catalog resolution models.

use std::collections::{BTreeMap, BTreeSet};

use coral_engine::CatalogInfo;

/// Query-visible catalog metadata and the sources omitted while resolving it.
#[derive(Debug)]
pub(crate) struct CatalogResolution {
    pub(crate) catalog: CatalogInfo,
    /// Runtime schema name to canonical installed source owner.
    pub(crate) runtime_schema_owners: BTreeMap<String, String>,
    /// Canonical installed source names skipped before or during runtime setup.
    pub(crate) failed_source_names: BTreeSet<String>,
    /// Digest of installed function artifacts. This invalidates Search's
    /// projection after a function body changes without changing its catalog
    /// metadata.
    pub(crate) udf_artifact_fingerprint: String,
}
