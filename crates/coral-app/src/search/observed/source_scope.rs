//! Source-surface routing and opaque scope derivation for observed values.

use coral_engine::{QuerySource, RuntimeSourceComponent};
use serde::Serialize;
use uuid::Uuid;

use crate::hash::sha256_hex;
use crate::search::observed::sqlite_queue::ObservedValuesSurfaceKind;

const SOURCE_SCOPE_FORMAT_VERSION: u8 = 1;
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
}

pub(super) fn source_surface_scopes(
    source: &QuerySource,
    seed: SourceScopeSeed<'_>,
) -> Vec<ObservedSourceSurfaceScope> {
    let mut scopes = Vec::new();
    for component in source.components() {
        match component {
            RuntimeSourceComponent::Http(manifest) => {
                scopes.extend(manifest.tables.iter().map(|table| {
                    surface_scope(
                        source,
                        manifest.common.name.as_str(),
                        ObservedValuesSurfaceKind::Table,
                        table.name(),
                        seed,
                    )
                }));
                scopes.extend(manifest.functions.iter().map(|function| {
                    surface_scope(
                        source,
                        manifest.common.name.as_str(),
                        ObservedValuesSurfaceKind::Function,
                        function.name.as_str(),
                        seed,
                    )
                }));
            }
            RuntimeSourceComponent::File(manifest) => {
                scopes.extend(manifest.tables.iter().map(|table| {
                    surface_scope(
                        source,
                        manifest.common.name.as_str(),
                        ObservedValuesSurfaceKind::Table,
                        table.name(),
                        seed,
                    )
                }));
            }
            RuntimeSourceComponent::Mcp(manifest) => {
                scopes.extend(manifest.tables.iter().map(|table| {
                    surface_scope(
                        source,
                        manifest.common.name.as_str(),
                        ObservedValuesSurfaceKind::Table,
                        table.name(),
                        seed,
                    )
                }));
                scopes.extend(manifest.functions.iter().map(|function| {
                    surface_scope(
                        source,
                        manifest.common.name.as_str(),
                        ObservedValuesSurfaceKind::Function,
                        function.name(),
                        seed,
                    )
                }));
            }
        }
    }
    scopes
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
