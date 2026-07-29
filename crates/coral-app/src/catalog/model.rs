//! App-private catalog resolution models.

use std::collections::{BTreeMap, BTreeSet};

use coral_engine::CatalogInfo;

use crate::sources::universal_search::UniversalSearchResolution;

/// Query-visible catalog metadata and the sources omitted while resolving it.
#[derive(Debug)]
pub(crate) struct CatalogResolution {
    pub(crate) catalog: CatalogInfo,
    pub(crate) failed_source_names: BTreeSet<String>,
    /// Runtime schema name to canonical installed source owner.
    pub(crate) runtime_schema_owners: BTreeMap<String, String>,
    /// Passive route decisions derived from the exact runtime source snapshot.
    #[expect(
        dead_code,
        reason = "native fanout consumes passive route decisions later in the child stack"
    )]
    pub(crate) universal_search_resolutions: Vec<UniversalSearchResolution>,
}
