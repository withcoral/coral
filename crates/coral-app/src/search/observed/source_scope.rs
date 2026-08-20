//! Source-surface routing and opaque scope derivation for observed values.

use coral_engine::{QuerySource, RuntimeSourceComponent};
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

/// A runtime component whose name diverges from its package's source name.
///
/// Since #1791 one source publishes exactly one surface and one SQL namespace,
/// and the sources domain copies both names from the same `manifest.common.name`
/// field, so this is unreachable on every production path. Search still refuses
/// the source here rather than writing rows under an identity that would select
/// nothing back — a tripwire at the single seam that derives identity from a
/// runtime package, not an enforcement boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "source '{source_name}' exposes runtime component '{component_source_name}'; one installed source must publish exactly one runtime schema"
)]
pub(crate) struct ObservedSourceIdentityMismatch {
    source_name: String,
    component_source_name: String,
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

#[derive(Debug, Clone)]
pub(super) struct ObservedSourceSurfaceScope {
    /// Installed source: lifecycle clears, invalidation epochs, and the SQL
    /// namespace used in search results — one name, because they are one thing.
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
    for component in source.components() {
        let component_source_name = match component {
            // Database components declare no HTTP/MCP observation surfaces, so
            // they never contribute a stored identity for the tripwire to
            // protect; the arm below enumerates nothing for them.
            RuntimeSourceComponent::Database(_) => source_name,
            RuntimeSourceComponent::Http(manifest) => manifest.common.name.as_str(),
            RuntimeSourceComponent::File(manifest) => manifest.common.name.as_str(),
            RuntimeSourceComponent::Mcp(manifest) => manifest.common.name.as_str(),
        };
        if component_source_name != source_name {
            return Err(ObservedSourceIdentityMismatch {
                source_name: source_name.to_string(),
                component_source_name: component_source_name.to_string(),
            });
        }

        match component {
            RuntimeSourceComponent::Database(_) => {
                // Database tables do not declare HTTP/MCP observation surfaces.
            }
            RuntimeSourceComponent::Http(manifest) => {
                scopes.extend(manifest.tables.iter().map(|table| {
                    surface_scope(
                        source_name,
                        ObservedValuesSurfaceKind::Table,
                        table.name(),
                        seed,
                    )
                }));
                scopes.extend(manifest.functions.iter().map(|function| {
                    surface_scope(
                        source_name,
                        ObservedValuesSurfaceKind::Function,
                        function.name.as_str(),
                        seed,
                    )
                }));
            }
            RuntimeSourceComponent::File(manifest) => {
                scopes.extend(manifest.tables.iter().map(|table| {
                    surface_scope(
                        source_name,
                        ObservedValuesSurfaceKind::Table,
                        table.name(),
                        seed,
                    )
                }));
            }
            RuntimeSourceComponent::Mcp(manifest) => {
                scopes.extend(manifest.tables.iter().map(|table| {
                    surface_scope(
                        source_name,
                        ObservedValuesSurfaceKind::Table,
                        table.name(),
                        seed,
                    )
                }));
                scopes.extend(manifest.functions.iter().map(|function| {
                    surface_scope(
                        source_name,
                        ObservedValuesSurfaceKind::Function,
                        function.name(),
                        seed,
                    )
                }));
            }
        }
    }
    Ok(scopes)
}

fn surface_scope(
    source_name: &str,
    surface_kind: ObservedValuesSurfaceKind,
    surface_name: &str,
    seed: SourceScopeSeed<'_>,
) -> ObservedSourceSurfaceScope {
    let scope_bytes = serde_json::to_vec(&ScopeFingerprint {
        format_version: SOURCE_SCOPE_FORMAT_VERSION,
        runtime_contract_fingerprint: seed.runtime_contract_fingerprint,
        credential_revision: seed.credential_revision,
        source_name,
        surface_kind: surface_kind.as_str(),
        surface_name,
    })
    .expect("observed-values source scope must serialize");
    ObservedSourceSurfaceScope {
        source_name: source_name.to_string(),
        surface_key: SurfaceKey {
            source_name: source_name.to_string(),
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
    // The serialized key is a stable on-disk format: these bytes are hashed
    // into `source_scope_id`, which every stored observed row is keyed by.
    // Renaming the Rust field to match the singular identity model must not
    // rotate scope ids, or migrated rows would be fail-closed invisible.
    #[serde(rename = "component_source_name")]
    source_name: &'a str,
    surface_kind: &'static str,
    surface_name: &'a str,
}

#[cfg(test)]
mod tests {
    use super::{SOURCE_SCOPE_FORMAT_VERSION, ScopeFingerprint};
    use crate::hash::sha256_hex;
    use uuid::Uuid;

    /// Pins the hashed bytes across the `component_source_name` -> `source_name`
    /// Rust rename. A change here invalidates every stored observed row.
    #[test]
    fn scope_fingerprint_hash_is_stable_across_the_field_rename() {
        let scope_bytes = serde_json::to_vec(&ScopeFingerprint {
            format_version: SOURCE_SCOPE_FORMAT_VERSION,
            runtime_contract_fingerprint: "v1:test-runtime-contract",
            credential_revision: Uuid::nil(),
            source_name: "github_v4",
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
