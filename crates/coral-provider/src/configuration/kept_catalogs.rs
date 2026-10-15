//! Catalogs of the latest Setup previews, kept in memory so Publication of the
//! same input need not materialize it again. Another process materializes.
use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard, PoisonError};

use coral_catalog::{
    MaterializationOutcome, OperationPathAssignment, PreparedProviderCatalog,
    ProviderConfiguration, ProviderSetupInput, ProviderSetupInputDigest,
};
use coral_outbound::DestinationPolicy;

use crate::ProviderSetup;

// A large catalog can take tens of megabytes. Keep only the latest few: an
// evicted Setup materializes again at Publication.
const MAX_KEPT_CATALOGS: usize = 4;

#[derive(Default)]
pub(super) struct KeptCatalogs(Mutex<VecDeque<KeptCatalog>>);

struct KeptCatalog {
    setup: ProviderSetup,
    source: Source,
    catalog: PreparedProviderCatalog,
}

/// Everything materialization reads. Equal sources give equal catalogs.
#[derive(PartialEq)]
struct Source {
    input_digest: ProviderSetupInputDigest,
    configuration: Option<ProviderConfiguration>,
    materializer_version: &'static str,
}

impl Source {
    /// `input_digest` covers the input's bytes, not its configuration.
    fn of(input_digest: ProviderSetupInputDigest, input: &ProviderSetupInput) -> Self {
        Self {
            input_digest,
            configuration: input.configuration().cloned(),
            materializer_version: coral_catalog::materializer_version(input.kind()),
        }
    }
}

impl KeptCatalog {
    fn is_for(&self, setup: &ProviderSetup) -> bool {
        self.setup.owner() == setup.owner() && self.setup.id() == setup.id()
    }

    fn expired(&self, now_unix_nanos: i64) -> bool {
        self.setup
            .expires_at_unix_nanos()
            .is_some_and(|deadline| now_unix_nanos >= deadline)
    }
}

impl KeptCatalogs {
    fn entries(&self) -> MutexGuard<'_, VecDeque<KeptCatalog>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Drops `setup`'s catalog, so a failed or rejected preview leaves none behind.
    pub(super) fn forget(&self, setup: &ProviderSetup) {
        self.entries().retain(|kept| !kept.is_for(setup));
    }

    /// Keeps the catalog materialized from `input` without previous paths.
    pub(super) fn keep(
        &self,
        setup: &ProviderSetup,
        input_digest: ProviderSetupInputDigest,
        input: &ProviderSetupInput,
        catalog: PreparedProviderCatalog,
    ) {
        let mut entries = self.entries();
        entries.retain(|kept| !kept.is_for(setup));
        if entries.len() >= MAX_KEPT_CATALOGS {
            entries.pop_front();
        }
        entries.push_back(KeptCatalog {
            setup: setup.clone(),
            source: Source::of(input_digest, input),
            catalog,
        });
    }

    /// Takes `setup`'s catalog when it was materialized from exactly what
    /// Publication would materialize, and screens it under the host `policy`
    /// as [`coral_catalog::materialize`] would.
    pub(super) fn take(
        &self,
        setup: &ProviderSetup,
        input: &ProviderSetupInput,
        previous_paths: &[OperationPathAssignment],
        policy: DestinationPolicy,
        now_unix_nanos: i64,
    ) -> Option<MaterializationOutcome> {
        // Previews materialize without previous paths.
        if !previous_paths.is_empty() {
            return None;
        }
        let kept = {
            let mut entries = self.entries();
            entries.retain(|kept| !kept.expired(now_unix_nanos));
            let position = entries.iter().position(|kept| kept.is_for(setup))?;
            entries.remove(position)?
        };
        let source = Source::of(setup.input_digest()?.clone(), input);
        (kept.source == source).then(|| {
            coral_catalog::screen_destinations(policy, MaterializationOutcome::Ready(kept.catalog))
        })
    }
}
