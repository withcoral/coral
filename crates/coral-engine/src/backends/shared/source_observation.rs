//! Shared source-scan observation helpers.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;

use crate::{SourceObservationPublisher, SourceObservationSurfaceKind, SourceScanObservation};

pub(crate) type SourceObservationPublishers = Arc<[Arc<dyn SourceObservationPublisher>]>;

#[derive(Debug, Clone)]
#[expect(
    clippy::struct_field_names,
    reason = "explicit name suffixes distinguish each SQL and ownership coordinate"
)]
pub(crate) struct SourceObservationIdentity {
    pub(crate) source_name: String,
    pub(crate) catalog_name: Option<String>,
    pub(crate) schema_name: String,
    pub(crate) surface_name: String,
}

impl SourceObservationIdentity {
    pub(crate) fn new(
        source_name: impl Into<String>,
        catalog_name: Option<String>,
        schema_name: impl Into<String>,
        surface_name: impl Into<String>,
    ) -> Self {
        Self {
            source_name: source_name.into(),
            catalog_name,
            schema_name: schema_name.into(),
            surface_name: surface_name.into(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct SourceObservationConfig {
    pub(crate) identity: SourceObservationIdentity,
    pub(crate) surface_kind: SourceObservationSurfaceKind,
    pub(crate) publishers: SourceObservationPublishers,
}

impl SourceObservationConfig {
    pub(crate) fn new(
        identity: SourceObservationIdentity,
        surface_kind: SourceObservationSurfaceKind,
        publishers: SourceObservationPublishers,
    ) -> Option<Self> {
        (!publishers.is_empty()).then_some(Self {
            identity,
            surface_kind,
            publishers,
        })
    }
}

pub(crate) fn source_observation_publishers(
    publishers: &[Arc<dyn SourceObservationPublisher>],
) -> SourceObservationPublishers {
    Arc::from(publishers.to_vec())
}

/// Publishes through the engine's non-blocking observation contract.
///
/// This helper intentionally does not spawn detached tasks. Queueing,
/// backpressure, dropping, and shutdown drainage belong in the app-side
/// publisher implementation that owns the corresponding lifecycle.
pub(crate) fn publish_source_scan_batch(
    observation: &SourceObservationConfig,
    batch: &RecordBatch,
) {
    let identity = &observation.identity;
    let event = SourceScanObservation {
        source_name: &identity.source_name,
        catalog_name: identity.catalog_name.as_deref(),
        schema_name: &identity.schema_name,
        surface_kind: observation.surface_kind,
        surface_name: &identity.surface_name,
        batch,
    };
    for publisher in observation.publishers.iter() {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            publisher.publish_source_scan(event);
        }))
        .is_err()
        {
            tracing::warn!(
                source = identity.source_name,
                catalog = identity.catalog_name,
                schema = identity.schema_name,
                surface = identity.surface_name,
                "source observation publisher panicked; dropping source-scan observation"
            );
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Mutex;

    use datafusion::arrow::array::RecordBatch;

    use super::*;

    #[derive(Default)]
    pub(crate) struct RecordingSourceObservationPublisher {
        observations: Mutex<Vec<RecordedSourceObservation>>,
    }

    pub(crate) struct RecordedSourceObservation {
        pub(crate) source_name: String,
        pub(crate) catalog_name: Option<String>,
        pub(crate) schema_name: String,
        pub(crate) surface_kind: SourceObservationSurfaceKind,
        pub(crate) surface_name: String,
        pub(crate) column_names: Vec<String>,
        pub(crate) row_count: usize,
        pub(crate) batch: RecordBatch,
    }

    impl RecordingSourceObservationPublisher {
        pub(crate) fn observations(&self) -> Vec<RecordedSourceObservation> {
            self.observations.lock().expect("observations lock").clone()
        }
    }

    impl Clone for RecordedSourceObservation {
        fn clone(&self) -> Self {
            Self {
                source_name: self.source_name.clone(),
                catalog_name: self.catalog_name.clone(),
                schema_name: self.schema_name.clone(),
                surface_kind: self.surface_kind,
                surface_name: self.surface_name.clone(),
                column_names: self.column_names.clone(),
                row_count: self.row_count,
                batch: self.batch.clone(),
            }
        }
    }

    impl SourceObservationPublisher for RecordingSourceObservationPublisher {
        fn publish_source_scan(&self, observation: SourceScanObservation<'_>) {
            self.observations
                .lock()
                .expect("observations lock")
                .push(RecordedSourceObservation {
                    source_name: observation.source_name.to_string(),
                    catalog_name: observation.catalog_name.map(ToString::to_string),
                    schema_name: observation.schema_name.to_string(),
                    surface_kind: observation.surface_kind,
                    surface_name: observation.surface_name.to_string(),
                    column_names: observation
                        .batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| field.name().clone())
                        .collect(),
                    row_count: observation.batch.num_rows(),
                    batch: observation.batch.clone(),
                });
        }
    }
}
