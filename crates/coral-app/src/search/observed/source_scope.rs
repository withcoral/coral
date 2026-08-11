//! Source-surface routing and opaque scope derivation for observed values.

use coral_engine::{QuerySource, RuntimeCatalogTarget, RuntimeSourceComponent};
use serde::Serialize;
use uuid::Uuid;

use crate::hash::sha256_hex;
use crate::search::observed::ObservedValuesLiveScope;
use crate::search::observed::sqlite_queue::ObservedValuesSurfaceKind;

const LEGACY_SOURCE_SCOPE_FORMAT_VERSION: u8 = 1;
const CATALOG_SOURCE_SCOPE_FORMAT_VERSION: u8 = 2;
#[cfg(test)]
const PRE_ACTIVATION_RUNTIME_CONTRACT_FINGERPRINT: &str = "observed-values/pre-activation/v0";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct SurfaceKey {
    pub(super) source_name: String,
    pub(super) catalog_name: Option<String>,
    pub(super) schema_name: String,
    pub(super) surface_kind: ObservedValuesSurfaceKind,
    pub(super) surface_name: String,
}

/// Opaque identity supplied by the app-owned runtime-package boundary.
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
    /// Installed source that owns lifecycle clears and invalidation epochs.
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
            catalog_name: self.surface_key.catalog_name.clone(),
            schema_name: self.surface_key.schema_name.clone(),
            source_scope_id: self.source_scope_id.clone(),
            surface_kind: self.surface_key.surface_kind,
            surface_name: self.surface_key.surface_name.clone(),
        }
    }
}

pub(super) fn source_surface_scopes(
    source: &QuerySource,
    seed: SourceScopeSeed<'_>,
) -> Vec<ObservedSourceSurfaceScope> {
    let catalog_name = matches!(source.catalog_target(), RuntimeCatalogTarget::Source)
        .then(|| source.source_name());
    let mut scopes = Vec::new();

    for component in source.components() {
        match component {
            RuntimeSourceComponent::Database(_) => {
                // Database tables are discovered and do not declare observed surfaces.
            }
            RuntimeSourceComponent::Http(manifest) => {
                extend_surface_scopes(
                    source,
                    catalog_name,
                    &manifest.common.name,
                    manifest
                        .tables
                        .iter()
                        .map(|table| (ObservedValuesSurfaceKind::Table, table.name())),
                    seed,
                    &mut scopes,
                );
                extend_surface_scopes(
                    source,
                    catalog_name,
                    &manifest.common.name,
                    manifest.functions.iter().map(|function| {
                        (ObservedValuesSurfaceKind::Function, function.name.as_str())
                    }),
                    seed,
                    &mut scopes,
                );
            }
            RuntimeSourceComponent::File(manifest) => {
                extend_surface_scopes(
                    source,
                    catalog_name,
                    &manifest.common.name,
                    manifest
                        .tables
                        .iter()
                        .map(|table| (ObservedValuesSurfaceKind::Table, table.name())),
                    seed,
                    &mut scopes,
                );
            }
            RuntimeSourceComponent::Mcp(manifest) => {
                extend_surface_scopes(
                    source,
                    catalog_name,
                    &manifest.common.name,
                    manifest
                        .tables
                        .iter()
                        .map(|table| (ObservedValuesSurfaceKind::Table, table.name())),
                    seed,
                    &mut scopes,
                );
                extend_surface_scopes(
                    source,
                    catalog_name,
                    &manifest.common.name,
                    manifest
                        .functions
                        .iter()
                        .map(|function| (ObservedValuesSurfaceKind::Function, function.name())),
                    seed,
                    &mut scopes,
                );
            }
        }
    }
    scopes
}

fn extend_surface_scopes<'a>(
    source: &QuerySource,
    catalog_name: Option<&str>,
    schema_name: &str,
    surfaces: impl IntoIterator<Item = (ObservedValuesSurfaceKind, &'a str)>,
    seed: SourceScopeSeed<'_>,
    scopes: &mut Vec<ObservedSourceSurfaceScope>,
) {
    scopes.extend(
        surfaces
            .into_iter()
            .map(|(kind, name)| surface_scope(source, catalog_name, schema_name, kind, name, seed)),
    );
}

fn surface_scope(
    source: &QuerySource,
    catalog_name: Option<&str>,
    schema_name: &str,
    surface_kind: ObservedValuesSurfaceKind,
    surface_name: &str,
    seed: SourceScopeSeed<'_>,
) -> ObservedSourceSurfaceScope {
    let scope_bytes =
        scope_fingerprint_bytes(catalog_name, schema_name, surface_kind, surface_name, seed);
    ObservedSourceSurfaceScope {
        source_name: source.source_name().to_string(),
        surface_key: SurfaceKey {
            source_name: source.source_name().to_string(),
            catalog_name: catalog_name.map(ToString::to_string),
            schema_name: schema_name.to_string(),
            surface_kind,
            surface_name: surface_name.to_string(),
        },
        source_scope_id: sha256_hex(&scope_bytes),
    }
}

fn scope_fingerprint_bytes(
    catalog_name: Option<&str>,
    schema_name: &str,
    surface_kind: ObservedValuesSurfaceKind,
    surface_name: &str,
    seed: SourceScopeSeed<'_>,
) -> Vec<u8> {
    match catalog_name {
        None => serde_json::to_vec(&LegacyScopeFingerprint {
            format_version: LEGACY_SOURCE_SCOPE_FORMAT_VERSION,
            runtime_contract_fingerprint: seed.runtime_contract_fingerprint,
            credential_revision: seed.credential_revision,
            component_source_name: schema_name,
            surface_kind: surface_kind.as_str(),
            surface_name,
        }),
        Some(catalog_name) => serde_json::to_vec(&CatalogScopeFingerprint {
            format_version: CATALOG_SOURCE_SCOPE_FORMAT_VERSION,
            runtime_contract_fingerprint: seed.runtime_contract_fingerprint,
            credential_revision: seed.credential_revision,
            catalog_name,
            schema_name,
            surface_kind: surface_kind.as_str(),
            surface_name,
        }),
    }
    .expect("observed-values source scope must serialize")
}

#[derive(Serialize)]
struct LegacyScopeFingerprint<'a> {
    format_version: u8,
    runtime_contract_fingerprint: &'a str,
    credential_revision: Uuid,
    component_source_name: &'a str,
    surface_kind: &'static str,
    surface_name: &'a str,
}

#[derive(Serialize)]
struct CatalogScopeFingerprint<'a> {
    format_version: u8,
    runtime_contract_fingerprint: &'a str,
    credential_revision: Uuid,
    catalog_name: &'a str,
    schema_name: &'a str,
    surface_kind: &'static str,
    surface_name: &'a str,
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{SourceScopeSeed, scope_fingerprint_bytes};
    use crate::hash::sha256_hex;
    use crate::search::observed::sqlite_queue::ObservedValuesSurfaceKind;

    #[test]
    fn legacy_scope_fingerprint_hash_is_stable() {
        let scope_bytes = scope_fingerprint_bytes(
            None,
            "github_v4",
            ObservedValuesSurfaceKind::Table,
            "issues",
            SourceScopeSeed::new("v1:test-runtime-contract", Uuid::nil()),
        );

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

    #[test]
    fn catalog_scope_fingerprint_includes_three_part_identity() {
        let scope_bytes = scope_fingerprint_bytes(
            Some("github_v4"),
            "issues",
            ObservedValuesSurfaceKind::Table,
            "list",
            SourceScopeSeed::new("v5:test-runtime-contract", Uuid::nil()),
        );

        assert_eq!(
            String::from_utf8(scope_bytes).expect("scope fingerprint is utf-8"),
            concat!(
                r#"{"format_version":2,"#,
                r#""runtime_contract_fingerprint":"v5:test-runtime-contract","#,
                r#""credential_revision":"00000000-0000-0000-0000-000000000000","#,
                r#""catalog_name":"github_v4","schema_name":"issues","#,
                r#""surface_kind":"table","surface_name":"list"}"#,
            )
        );
    }
}
