//! App-private catalog resolution models.

use std::collections::{BTreeMap, BTreeSet};

use coral_engine::CatalogInfo;

pub(crate) type RuntimeRelationOwners = BTreeMap<(Option<String>, String), String>;

/// Query-visible catalog metadata and the sources omitted while resolving it.
#[derive(Debug)]
pub(crate) struct CatalogResolution {
    pub(crate) catalog: CatalogInfo,
    pub(crate) failed_source_names: BTreeSet<String>,
    /// Runtime catalog/schema identity to canonical installed source owner.
    pub(crate) runtime_relation_owners: RuntimeRelationOwners,
}
