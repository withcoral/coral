//! Transport-neutral Universal Search capability models.

use crate::sources::universal_search::UniversalSearchResolution;

pub(crate) const MAX_SEARCH_CAPABILITY_ROUTES: usize = 16;

/// Effective Search behavior and the bounded set of provider routes that may
/// be called in the current workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SearchCapabilities {
    pub(crate) provider_fanout_enabled: bool,
    pub(crate) eligible_routes: Vec<SearchRouteIdentity>,
    pub(crate) truncated: bool,
    pub(crate) omitted_route_count: u32,
}

impl SearchCapabilities {
    pub(crate) fn disabled() -> Self {
        Self {
            provider_fanout_enabled: false,
            eligible_routes: Vec::new(),
            truncated: false,
            omitted_route_count: 0,
        }
    }

    pub(crate) fn enabled(resolutions: Vec<UniversalSearchResolution>) -> Self {
        let mut eligible_routes = resolutions
            .into_iter()
            .flat_map(|resolution| {
                resolution
                    .eligible_routes
                    .into_iter()
                    .map(SearchRouteIdentity::from)
            })
            .collect::<Vec<_>>();
        eligible_routes.sort();
        eligible_routes.dedup();

        let omitted_route_count = eligible_routes
            .len()
            .saturating_sub(MAX_SEARCH_CAPABILITY_ROUTES);
        eligible_routes.truncate(MAX_SEARCH_CAPABILITY_ROUTES);

        Self {
            provider_fanout_enabled: true,
            eligible_routes,
            truncated: omitted_route_count != 0,
            omitted_route_count: u32::try_from(omitted_route_count).unwrap_or(u32::MAX),
        }
    }
}

/// Safe identity of one resolved provider route. No request arguments,
/// credentials, URLs, or provider-authored errors are exposed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SearchRouteIdentity {
    pub(crate) installed_source_name: String,
    pub(crate) schema_name: String,
    pub(crate) function_name: String,
    pub(crate) authored_route_id: Option<String>,
}

impl From<crate::sources::universal_search::ResolvedUniversalSearchRoute> for SearchRouteIdentity {
    fn from(route: crate::sources::universal_search::ResolvedUniversalSearchRoute) -> Self {
        Self {
            installed_source_name: route.owner_source_name,
            schema_name: route.locator.schema_name,
            function_name: route.locator.function_name,
            authored_route_id: route.authored_route_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use coral_spec::{ManifestDataType, SearchLimitsSpec};
    use uuid::Uuid;

    use super::*;
    use crate::sources::runtime_package::RuntimeContractFingerprint;
    use crate::sources::universal_search::{
        ResolvedUniversalSearchArgument, ResolvedUniversalSearchResultMapping,
        ResolvedUniversalSearchRoute, ResolvedUniversalSearchTarget,
        UniversalSearchFunctionLocator, UniversalSearchResolutionOrigin,
    };

    fn resolution(source: &str, route_count: usize) -> UniversalSearchResolution {
        let mut resolution = UniversalSearchResolution::empty(source);
        resolution.eligible_routes = (0..route_count)
            .map(|index| ResolvedUniversalSearchRoute {
                owner_source_name: source.to_string(),
                installation_revision: Uuid::nil(),
                authored_route_id: Some(format!("route-{index:02}")),
                target: ResolvedUniversalSearchTarget {
                    operation_id: format!("search_{index:02}"),
                },
                locator: UniversalSearchFunctionLocator {
                    schema_name: format!("schema_{source}"),
                    function_name: format!("search_{index:02}"),
                },
                query_argument: ResolvedUniversalSearchArgument {
                    name: "query".to_string(),
                    data_type: ManifestDataType::Utf8,
                },
                default_arguments: Vec::new(),
                search_limits: SearchLimitsSpec {
                    default_top_k: 5,
                    max_top_k: 5,
                    max_calls_per_query: 1,
                },
                result: ResolvedUniversalSearchResultMapping::default(),
                origin: UniversalSearchResolutionOrigin::Explicit,
                runtime_contract_fingerprint: RuntimeContractFingerprint::for_test(&format!(
                    "fingerprint-{source}-{index}"
                )),
            })
            .collect();
        resolution
    }

    #[test]
    fn disabled_capabilities_never_report_routes() {
        assert_eq!(
            SearchCapabilities::disabled(),
            SearchCapabilities {
                provider_fanout_enabled: false,
                eligible_routes: Vec::new(),
                truncated: false,
                omitted_route_count: 0,
            }
        );
    }

    #[test]
    fn enabled_capabilities_are_sorted_and_bounded() {
        let capabilities =
            SearchCapabilities::enabled(vec![resolution("zulu", 9), resolution("alpha", 9)]);

        assert!(capabilities.provider_fanout_enabled);
        assert_eq!(capabilities.eligible_routes.len(), 16);
        assert!(capabilities.truncated);
        assert_eq!(capabilities.omitted_route_count, 2);
        assert_eq!(
            capabilities
                .eligible_routes
                .first()
                .map(|route| route.installed_source_name.as_str()),
            Some("alpha")
        );
        assert_eq!(
            capabilities
                .eligible_routes
                .last()
                .map(|route| route.installed_source_name.as_str()),
            Some("zulu")
        );
    }
}
