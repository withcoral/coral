//! Source-surface routing and opaque scope derivation for observed values.

use coral_engine::{QuerySource, RuntimeRelationKind};
use serde::Serialize;
use uuid::Uuid;

use crate::hash::sha256_hex;
use crate::search::observed::ObservedValuesLiveScope;
use crate::search::observed::sqlite_queue::ObservedValuesSurfaceKind;

const SOURCE_SCOPE_FORMAT_VERSION: u8 = 1;
#[cfg(test)]
const PRE_ACTIVATION_RUNTIME_CONTRACT_FINGERPRINT: &str = "observed-values/pre-activation/v0";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct SurfaceKey {
    pub(super) source_name: String,
    pub(super) surface_kind: ObservedValuesSurfaceKind,
    pub(super) surface_name: String,
}

/// Opaque identity supplied by the app-owned runtime-package boundary.
///
/// The queue substrate does not interpret either value. The app-wiring PR
/// replaces the pre-activation seed with a complete runtime-contract
/// fingerprint and an app-owned credential revision.
#[derive(Debug, Clone, Copy)]
pub(super) struct SourceScopeSeed<'a> {
    runtime_contract_fingerprint: &'a str,
    credential_revision: Uuid,
}

#[cfg(test)]
impl SourceScopeSeed<'static> {
    pub(super) const PRE_ACTIVATION: Self =
        Self::new(PRE_ACTIVATION_RUNTIME_CONTRACT_FINGERPRINT, Uuid::nil());
}

impl<'a> SourceScopeSeed<'a> {
    pub(super) const fn new(
        runtime_contract_fingerprint: &'a str,
        credential_revision: Uuid,
    ) -> Self {
        Self {
            runtime_contract_fingerprint,
            credential_revision,
        }
    }
}

/// A runtime SQL namespace whose name diverges from its package's source name.
///
/// Since #1791 one source publishes exactly one surface and one SQL namespace,
/// Default-catalog relations use their schema as the namespace. Catalog-backed
/// relations use their catalog, leaving provider or projection schemas free to
/// differ. Search refuses a source whose top-level namespace does not match its
/// installed owner rather than writing rows under an incoherent identity.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "source '{source_name}' exposes runtime namespace '{component_source_name}'; one installed source must publish exactly one top-level SQL namespace"
)]
pub(crate) struct ObservedSourceIdentityMismatch {
    source_name: String,
    component_source_name: String,
}

#[derive(Debug, Clone)]
pub(super) struct ObservedSourceSurfaceScope {
    /// Canonical installed source that owns lifecycle clears and invalidation epochs.
    pub(super) owner_source_name: String,
    /// Runtime component schema used in SQL and search results.
    pub(super) source_name: String,
    surface_key: SurfaceKey,
    pub(super) source_scope_id: String,
}

impl ObservedSourceSurfaceScope {
    pub(super) fn key(&self) -> SurfaceKey {
        self.surface_key.clone()
    }

    pub(super) fn live_scope(&self) -> ObservedValuesLiveScope {
        ObservedValuesLiveScope {
            owner_source_name: self.owner_source_name.clone(),
            source_name: self.source_name.clone(),
            source_scope_id: self.source_scope_id.clone(),
            surface_kind: self.surface_key.surface_kind,
            surface_name: self.surface_key.surface_name.clone(),
        }
    }
}

pub(super) fn source_surface_scopes(
    source: &QuerySource,
    seed: SourceScopeSeed<'_>,
) -> Result<Vec<ObservedSourceSurfaceScope>, ObservedSourceIdentityMismatch> {
    let source_name = source.source_name();
    let mut scopes = Vec::new();
    for relation in source.declared_relations() {
        let sql_name = relation.sql_name();
        let component_source_name = if sql_name.catalog_name() == "datafusion" {
            sql_name.schema_name()
        } else {
            sql_name.catalog_name()
        };
        if component_source_name != source_name {
            return Err(ObservedSourceIdentityMismatch {
                source_name: source_name.to_string(),
                component_source_name: component_source_name.to_string(),
            });
        }
        let surface_kind = match relation.kind() {
            RuntimeRelationKind::Table => ObservedValuesSurfaceKind::Table,
            RuntimeRelationKind::TableFunction => ObservedValuesSurfaceKind::Function,
        };
        scopes.push(surface_scope(
            source,
            component_source_name,
            surface_kind,
            sql_name.name(),
            seed,
        ));
    }
    Ok(scopes)
}

fn surface_scope(
    source: &QuerySource,
    component_source_name: &str,
    surface_kind: ObservedValuesSurfaceKind,
    surface_name: &str,
    seed: SourceScopeSeed<'_>,
) -> ObservedSourceSurfaceScope {
    let scope_bytes = serde_json::to_vec(&ScopeFingerprint {
        format_version: SOURCE_SCOPE_FORMAT_VERSION,
        runtime_contract_fingerprint: seed.runtime_contract_fingerprint,
        credential_revision: seed.credential_revision,
        component_source_name,
        surface_kind: surface_kind.as_str(),
        surface_name,
    })
    .expect("observed-values source scope must serialize");
    ObservedSourceSurfaceScope {
        owner_source_name: source.source_name().to_string(),
        source_name: component_source_name.to_string(),
        surface_key: SurfaceKey {
            source_name: component_source_name.to_string(),
            surface_kind,
            surface_name: surface_name.to_string(),
        },
        source_scope_id: sha256_hex(&scope_bytes),
    }
}

#[derive(Serialize)]
struct ScopeFingerprint<'a> {
    format_version: u8,
    runtime_contract_fingerprint: &'a str,
    credential_revision: Uuid,
    component_source_name: &'a str,
    surface_kind: &'static str,
    surface_name: &'a str,
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{SOURCE_SCOPE_FORMAT_VERSION, ScopeFingerprint};
    use crate::hash::sha256_hex;

    /// Pins the exact bytes hashed into `source_scope_id`.
    ///
    /// Every stored observed row is keyed by that id and retrieval is
    /// fail-closed, so a change to this serialization silently hides the entire
    /// observed corpus rather than failing anything. The struct is private and
    /// its field names are part of the on-disk format, not an implementation
    /// detail -- if a refactor needs to rename one, it owes a `#[serde(rename)]`
    /// and this test must keep passing untouched.
    #[test]
    fn scope_fingerprint_hash_is_stable() {
        let scope_bytes = serde_json::to_vec(&ScopeFingerprint {
            format_version: SOURCE_SCOPE_FORMAT_VERSION,
            runtime_contract_fingerprint: "v1:test-runtime-contract",
            credential_revision: Uuid::nil(),
            component_source_name: "github_v4",
            surface_kind: "table",
            surface_name: "issues",
        })
        .expect("scope fingerprint serializes");

        assert_eq!(
            String::from_utf8(scope_bytes.clone()).expect("scope fingerprint is utf-8"),
            concat!(
                r#"{"format_version":1,"#,
                r#""runtime_contract_fingerprint":"v1:test-runtime-contract","#,
                r#""credential_revision":"00000000-0000-0000-0000-000000000000","#,
                r#""component_source_name":"github_v4","#,
                r#""surface_kind":"table","surface_name":"issues"}"#,
            )
        );
        assert_eq!(
            sha256_hex(&scope_bytes),
            "82fca0aa8c55fc4b8a22cf7a8f57a63d5acab8d7d2d84853d3f12b3b7a24886b"
        );
    }
}
