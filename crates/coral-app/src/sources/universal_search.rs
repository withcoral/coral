//! Passive, app-owned resolution of Universal Search source routes.
//!
//! Resolution never starts a backend or enables provider execution. It joins
//! authored policy to the exact locally materialized runtime contract and
//! records bounded diagnostics when that join cannot be proven safe.

use std::collections::BTreeSet;

use chrono::DateTime;
use coral_engine::{
    RuntimeTableFunctionAuthorizationInfo, UniversalSearchAuthorizationDecision,
    UniversalSearchAuthorizationInfo, UniversalSearchAuthorizationOrigin,
};
use coral_spec::v4::{
    HttpMethod as V4HttpMethod, IrExecutionAttachment, IrInputLocation, IrOperation, Projection,
    ProjectionInput, ProjectionKind, ProjectionVisibility, SqlInputExposure, V4MaterializedSource,
    V4SourceManifest, V4UniversalSearchInputLocation, V4UniversalSearchRouteSpec,
    ValidatedSurfacePlan,
};
use coral_spec::{
    DeclaredDefaultValue, ManifestDataType, SearchLimitsSpec, SourceTableFunctionKind,
    UniversalSearchResultMappingSpec, ValidatedSourceManifest,
};
use serde_json::Value;
use uuid::Uuid;

use super::runtime_package::RuntimeContractFingerprint;

const MAX_RESOLUTION_DIAGNOSTICS: usize = 32;

/// A source-level, passive route-resolution report.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UniversalSearchResolution {
    pub(crate) source_name: String,
    pub(crate) eligible_routes: Vec<ResolvedUniversalSearchRoute>,
    pub(crate) explicit_denials: Vec<ResolvedUniversalSearchDenial>,
    pub(crate) diagnostics: Vec<UniversalSearchResolutionDiagnostic>,
    pub(crate) diagnostics_truncated: bool,
    pub(crate) omitted_diagnostic_count: usize,
}

impl UniversalSearchResolution {
    pub(crate) fn empty(source_name: &str) -> Self {
        Self {
            source_name: source_name.to_string(),
            eligible_routes: Vec::new(),
            explicit_denials: Vec::new(),
            diagnostics: Vec::new(),
            diagnostics_truncated: false,
            omitted_diagnostic_count: 0,
        }
    }

    /// Converts attachable decisions into the engine's deliberately passive
    /// catalog transport shape. The engine does not interpret this metadata.
    pub(crate) fn engine_authorizations(&self) -> Vec<RuntimeTableFunctionAuthorizationInfo> {
        self.eligible_routes
            .iter()
            .filter(|route| {
                route.source_name == self.source_name
                    && route.locator.schema_name == self.source_name
            })
            .map(|route| RuntimeTableFunctionAuthorizationInfo {
                schema_name: self.source_name.clone(),
                function_name: route.locator.function_name.clone(),
                authorization: UniversalSearchAuthorizationInfo {
                    source_name: self.source_name.clone(),
                    route_id: route.authored_route_id.clone(),
                    origin: match route.origin {
                        UniversalSearchResolutionOrigin::Explicit => {
                            UniversalSearchAuthorizationOrigin::Explicit
                        }
                        UniversalSearchResolutionOrigin::Inferred => {
                            UniversalSearchAuthorizationOrigin::Inferred
                        }
                    },
                    decision: UniversalSearchAuthorizationDecision::Eligible,
                    query_argument: Some(route.query_argument.name.clone()),
                    operation_id: route.target.operation_id.clone(),
                },
            })
            .chain(self.explicit_denials.iter().filter_map(|denial| {
                let locator = denial.locator.as_ref()?;
                if denial.source_name != self.source_name || locator.schema_name != self.source_name
                {
                    return None;
                }
                Some(RuntimeTableFunctionAuthorizationInfo {
                    schema_name: self.source_name.clone(),
                    function_name: locator.function_name.clone(),
                    authorization: UniversalSearchAuthorizationInfo {
                        source_name: self.source_name.clone(),
                        route_id: Some(denial.authored_route_id.clone()),
                        origin: UniversalSearchAuthorizationOrigin::Explicit,
                        decision: UniversalSearchAuthorizationDecision::Denied,
                        query_argument: None,
                        operation_id: denial.target.operation_id.clone(),
                    },
                })
            }))
            .collect()
    }

    fn push_diagnostic(&mut self, diagnostic: UniversalSearchResolutionDiagnostic) {
        if self.diagnostics.len() < MAX_RESOLUTION_DIAGNOSTICS {
            self.diagnostics.push(diagnostic);
        } else {
            self.diagnostics_truncated = true;
            self.omitted_diagnostic_count = self.omitted_diagnostic_count.saturating_add(1);
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedUniversalSearchRoute {
    pub(crate) source_name: String,
    pub(crate) installation_revision: Uuid,
    pub(crate) authored_route_id: Option<String>,
    pub(crate) target: ResolvedUniversalSearchTarget,
    pub(crate) locator: UniversalSearchFunctionLocator,
    pub(crate) query_argument: ResolvedUniversalSearchArgument,
    pub(crate) default_arguments: Vec<ResolvedUniversalSearchDefaultArgument>,
    pub(crate) search_limits: SearchLimitsSpec,
    pub(crate) result: ResolvedUniversalSearchResultMapping,
    pub(crate) origin: UniversalSearchResolutionOrigin,
    pub(crate) runtime_contract_fingerprint: RuntimeContractFingerprint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedUniversalSearchDenial {
    pub(crate) source_name: String,
    pub(crate) authored_route_id: String,
    pub(crate) target: ResolvedUniversalSearchTarget,
    pub(crate) locator: Option<UniversalSearchFunctionLocator>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedUniversalSearchTarget {
    pub(crate) operation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UniversalSearchFunctionLocator {
    pub(crate) schema_name: String,
    pub(crate) function_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedUniversalSearchArgument {
    pub(crate) name: String,
    pub(crate) data_type: ManifestDataType,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedUniversalSearchDefaultArgument {
    pub(crate) name: String,
    pub(crate) data_type: ManifestDataType,
    pub(crate) value: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UniversalSearchResolutionOrigin {
    Explicit,
    Inferred,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct ResolvedUniversalSearchResultMapping {
    pub(crate) authored_mapping: bool,
    pub(crate) entity_type: Option<String>,
    pub(crate) identity_fields: Vec<ResolvedUniversalSearchResultField>,
    pub(crate) provider_id: Option<ResolvedUniversalSearchResultField>,
    pub(crate) title: Option<ResolvedUniversalSearchResultField>,
    pub(crate) url: Option<ResolvedUniversalSearchResultField>,
    pub(crate) snippet: Option<ResolvedUniversalSearchResultField>,
    pub(crate) attributes: Vec<ResolvedUniversalSearchResultField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedUniversalSearchResultField {
    pub(crate) column_name: String,
    pub(crate) data_type: ManifestDataType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UniversalSearchResolutionDiagnostic {
    pub(crate) source_name: String,
    pub(crate) authored_route_id: Option<String>,
    pub(crate) locator: Option<UniversalSearchFunctionLocator>,
    pub(crate) reason: UniversalSearchResolutionReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UniversalSearchResolutionReason {
    AmbiguousRoute,
    InvalidSearchLimits,
    MissingArgumentDefault,
    QueryInputUnmappable,
    RouteStale,
    UnsafeOperation,
}

#[derive(Debug)]
struct RouteResolutionFailure {
    locator: Option<UniversalSearchFunctionLocator>,
    reason: UniversalSearchResolutionReason,
}

/// Resolves one installed source without performing provider I/O.
pub(crate) fn resolve_universal_search(
    source_name: &str,
    installation_revision: Uuid,
    manifest: &ValidatedSourceManifest,
    materialized: Option<&V4MaterializedSource>,
    runtime_contract_fingerprint: &RuntimeContractFingerprint,
) -> UniversalSearchResolution {
    let mut resolution = resolve_universal_search_without_installation_revision(
        source_name,
        manifest,
        materialized,
        runtime_contract_fingerprint,
    );
    for route in &mut resolution.eligible_routes {
        route.installation_revision = installation_revision;
    }
    resolution
}

fn resolve_universal_search_without_installation_revision(
    source_name: &str,
    manifest: &ValidatedSourceManifest,
    materialized: Option<&V4MaterializedSource>,
    runtime_contract_fingerprint: &RuntimeContractFingerprint,
) -> UniversalSearchResolution {
    if source_name != manifest.schema_name() {
        let mut resolution = UniversalSearchResolution::empty(source_name);
        resolution.push_diagnostic(diagnostic(
            source_name,
            None,
            None,
            UniversalSearchResolutionReason::RouteStale,
        ));
        return resolution;
    }

    // DSL v3 sources remain ordinary SQL/catalog sources but never yield
    // provider-fanout routes.
    let Some(v4) = manifest.as_v4() else {
        return UniversalSearchResolution::empty(source_name);
    };
    let Some(materialized) = materialized else {
        let mut resolution = UniversalSearchResolution::empty(source_name);
        resolution.push_diagnostic(diagnostic(
            source_name,
            None,
            None,
            UniversalSearchResolutionReason::RouteStale,
        ));
        return resolution;
    };
    resolve_v4(source_name, v4, materialized, runtime_contract_fingerprint)
}

#[expect(
    clippy::too_many_lines,
    reason = "explicit and inferred v4 policy share one auditable source-level decision flow"
)]
fn resolve_v4(
    source_name: &str,
    manifest: &V4SourceManifest,
    materialized: &V4MaterializedSource,
    fingerprint: &RuntimeContractFingerprint,
) -> UniversalSearchResolution {
    let mut resolution = UniversalSearchResolution::empty(source_name);
    if let Some(policy) = manifest.universal_search.as_ref() {
        let mut seen_targets = BTreeSet::new();
        let duplicate_target = policy
            .routes
            .values()
            .any(|route| !seen_targets.insert(route.target.operation_id.as_str()));
        if duplicate_target {
            resolution.push_diagnostic(diagnostic(
                source_name,
                None,
                None,
                UniversalSearchResolutionReason::AmbiguousRoute,
            ));
            return resolution;
        }
        let mut source_invalid = false;
        for (route_id, route) in &policy.routes {
            if !route.execute {
                let locator = match v4_target_parts(source_name, materialized, route) {
                    Ok(parts) => Some(parts.locator),
                    Err(failure) => {
                        source_invalid = true;
                        resolution.push_diagnostic(diagnostic(
                            source_name,
                            Some(route_id.clone()),
                            failure.locator,
                            failure.reason,
                        ));
                        None
                    }
                };
                resolution
                    .explicit_denials
                    .push(ResolvedUniversalSearchDenial {
                        source_name: source_name.to_string(),
                        authored_route_id: route_id.clone(),
                        target: v4_target(route),
                        locator,
                    });
                continue;
            }
            match resolve_v4_route(
                source_name,
                route,
                Some(route_id.clone()),
                UniversalSearchResolutionOrigin::Explicit,
                materialized,
                fingerprint,
            ) {
                Ok(route) => resolution.eligible_routes.push(route),
                Err(failure) => {
                    source_invalid = true;
                    resolution.push_diagnostic(diagnostic(
                        source_name,
                        Some(route_id.clone()),
                        failure.locator,
                        failure.reason,
                    ));
                }
            }
        }
        if source_invalid {
            resolution.eligible_routes.clear();
        }
        return resolution;
    }

    let candidates = materialized
        .projections
        .projections
        .iter()
        .filter(|projection| {
            projection.visibility == ProjectionVisibility::Published
                && matches!(
                    projection.kind,
                    ProjectionKind::TableFunction {
                        function_kind: SourceTableFunctionKind::Search
                    }
                )
        })
        .collect::<Vec<_>>();
    let named_search = candidates
        .iter()
        .copied()
        .filter(|projection| projection.name == "search")
        .collect::<Vec<_>>();
    let candidate = if named_search.len() == 1 {
        named_search.first().copied()
    } else if named_search.is_empty() && candidates.len() == 1 {
        candidates.first().copied()
    } else {
        None
    };
    let Some(candidate) = candidate else {
        if !candidates.is_empty() {
            resolution.push_diagnostic(diagnostic(
                source_name,
                None,
                None,
                UniversalSearchResolutionReason::AmbiguousRoute,
            ));
        }
        return resolution;
    };
    let inferred = V4UniversalSearchRouteSpec {
        execute: true,
        target: coral_spec::v4::V4UniversalSearchTargetSpec {
            operation_id: candidate.operation_id.clone(),
        },
        query_input: infer_v4_query_input(candidate),
        result: None,
    };
    let Some(_) = inferred.query_input else {
        resolution.push_diagnostic(diagnostic(
            source_name,
            None,
            Some(projection_locator(source_name, candidate)),
            UniversalSearchResolutionReason::QueryInputUnmappable,
        ));
        return resolution;
    };
    match resolve_v4_route(
        source_name,
        &inferred,
        None,
        UniversalSearchResolutionOrigin::Inferred,
        materialized,
        fingerprint,
    ) {
        Ok(route) => resolution.eligible_routes.push(route),
        Err(failure) => {
            resolution.push_diagnostic(diagnostic(
                source_name,
                None,
                failure.locator,
                failure.reason,
            ));
        }
    }
    resolution
}

fn infer_v4_query_input(
    projection: &Projection,
) -> Option<coral_spec::v4::V4UniversalSearchQueryInputSpec> {
    let candidates = projection
        .inputs
        .iter()
        .filter(|input| {
            input.sql_exposure == SqlInputExposure::FunctionArg
                && input.data_type == ManifestDataType::Utf8
                && input.default_value.is_none()
        })
        .collect::<Vec<_>>();
    let input = exactly_one(&candidates)?;
    let location = match input.source_location {
        IrInputLocation::Path => V4UniversalSearchInputLocation::Path,
        IrInputLocation::Query => V4UniversalSearchInputLocation::Query,
        IrInputLocation::ToolArg => V4UniversalSearchInputLocation::ToolArg,
        IrInputLocation::Header | IrInputLocation::Cookie | IrInputLocation::Body => return None,
    };
    Some(coral_spec::v4::V4UniversalSearchQueryInputSpec {
        location,
        name: input.wire_name.clone(),
    })
}

struct V4TargetParts<'a> {
    operation: &'a IrOperation,
    projection: &'a Projection,
    locator: UniversalSearchFunctionLocator,
}

fn v4_target_parts<'a>(
    source_name: &str,
    materialized: &'a V4MaterializedSource,
    route: &V4UniversalSearchRouteSpec,
) -> Result<V4TargetParts<'a>, RouteResolutionFailure> {
    let operations = materialized
        .surface
        .plan
        .semantic_ir()
        .operations
        .iter()
        .filter(|operation| operation.id == route.target.operation_id)
        .collect::<Vec<_>>();
    let operation = exactly_one(&operations).ok_or(RouteResolutionFailure {
        locator: None,
        reason: if operations.len() > 1 {
            UniversalSearchResolutionReason::AmbiguousRoute
        } else {
            UniversalSearchResolutionReason::RouteStale
        },
    })?;
    let projections = materialized
        .projections
        .projections
        .iter()
        .filter(|projection| projection.operation_id == route.target.operation_id)
        .collect::<Vec<_>>();
    let projection = exactly_one(&projections).ok_or(RouteResolutionFailure {
        locator: None,
        reason: if projections.len() > 1 {
            UniversalSearchResolutionReason::AmbiguousRoute
        } else {
            UniversalSearchResolutionReason::RouteStale
        },
    })?;
    let locator = projection_locator(source_name, projection);
    if projection.visibility != ProjectionVisibility::Published
        || !matches!(
            projection.kind,
            ProjectionKind::TableFunction {
                function_kind: SourceTableFunctionKind::Search
            }
        )
    {
        return Err(RouteResolutionFailure {
            locator: Some(locator),
            reason: UniversalSearchResolutionReason::RouteStale,
        });
    }
    Ok(V4TargetParts {
        operation,
        projection,
        locator,
    })
}

fn resolve_v4_route(
    source_name: &str,
    route: &V4UniversalSearchRouteSpec,
    route_id: Option<String>,
    origin: UniversalSearchResolutionOrigin,
    materialized: &V4MaterializedSource,
    fingerprint: &RuntimeContractFingerprint,
) -> Result<ResolvedUniversalSearchRoute, RouteResolutionFailure> {
    let parts = v4_target_parts(source_name, materialized, route)?;
    if !v4_projection_inputs_match_operation(
        &materialized.surface.plan,
        parts.operation,
        parts.projection,
    ) {
        return Err(RouteResolutionFailure {
            locator: Some(parts.locator),
            reason: UniversalSearchResolutionReason::RouteStale,
        });
    }
    // Universal Search fanout has no guide-acknowledgement channel. Exposing a
    // guide-gated function here would bypass the ordinary SQL execution gate,
    // so it is not safe for provider execution.
    if parts.projection.require_guide_read {
        return Err(RouteResolutionFailure {
            locator: Some(parts.locator),
            reason: UniversalSearchResolutionReason::UnsafeOperation,
        });
    }
    if origin == UniversalSearchResolutionOrigin::Inferred && !v4_operation_is_safe(parts.operation)
    {
        return Err(RouteResolutionFailure {
            locator: Some(parts.locator),
            reason: UniversalSearchResolutionReason::UnsafeOperation,
        });
    }
    let query = route
        .query_input
        .as_ref()
        .and_then(|query| resolve_v4_query_input(parts.operation, parts.projection, query))
        .filter(|input| input.data_type == ManifestDataType::Utf8 && input.default_value.is_none())
        .ok_or_else(|| RouteResolutionFailure {
            locator: Some(parts.locator.clone()),
            reason: UniversalSearchResolutionReason::QueryInputUnmappable,
        })?;
    let defaults = parts
        .projection
        .inputs
        .iter()
        .filter(|input| input.sql_exposure == SqlInputExposure::FunctionArg)
        .filter(|input| input.name != query.name)
        .map(|input| {
            let default = input.default_value.as_ref()?;
            default_matches_type(default.value(), input.data_type).then(|| {
                ResolvedUniversalSearchDefaultArgument {
                    name: input.name.clone(),
                    data_type: input.data_type,
                    value: default.value().clone(),
                }
            })
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| RouteResolutionFailure {
            locator: Some(parts.locator.clone()),
            reason: UniversalSearchResolutionReason::MissingArgumentDefault,
        })?;
    let search_limits =
        parts
            .projection
            .search_limits
            .clone()
            .ok_or_else(|| RouteResolutionFailure {
                locator: Some(parts.locator.clone()),
                reason: UniversalSearchResolutionReason::InvalidSearchLimits,
            })?;
    if search_limits
        .validate("universal_search.resolved_route.search_limits")
        .is_err()
    {
        return Err(RouteResolutionFailure {
            locator: Some(parts.locator),
            reason: UniversalSearchResolutionReason::InvalidSearchLimits,
        });
    }
    let result = resolve_v4_result(parts.operation, parts.projection, route.result.as_ref())
        .ok_or_else(|| RouteResolutionFailure {
            locator: Some(parts.locator.clone()),
            reason: UniversalSearchResolutionReason::RouteStale,
        })?;
    Ok(ResolvedUniversalSearchRoute {
        source_name: source_name.to_string(),
        installation_revision: Uuid::nil(),
        authored_route_id: route_id,
        target: v4_target(route),
        locator: parts.locator,
        query_argument: ResolvedUniversalSearchArgument {
            name: query.name.clone(),
            data_type: query.data_type,
        },
        default_arguments: defaults,
        search_limits,
        result,
        origin,
        runtime_contract_fingerprint: fingerprint.clone(),
    })
}

fn v4_projection_inputs_match_operation(
    plan: &ValidatedSurfacePlan,
    operation: &IrOperation,
    projection: &Projection,
) -> bool {
    if projection.inputs.len() != operation.inputs.len() {
        return false;
    }

    projection.inputs.iter().all(|projection_input| {
        if projection
            .inputs
            .iter()
            .filter(|candidate| {
                candidate.source_location == projection_input.source_location
                    && candidate.wire_name == projection_input.wire_name
            })
            .count()
            != 1
            || projection
                .inputs
                .iter()
                .filter(|candidate| candidate.name == projection_input.name)
                .count()
                != 1
        {
            return false;
        }

        let operation_inputs = operation
            .inputs
            .iter()
            .filter(|operation_input| {
                operation_input.location == projection_input.source_location
                    && operation_input.name == projection_input.wire_name
            })
            .collect::<Vec<_>>();
        let Some(operation_input) = exactly_one(&operation_inputs) else {
            return false;
        };

        let lowered_required = operation_input.required
            && (operation_input.default_value.is_none()
                || operation_input.location == IrInputLocation::ToolArg);
        projection_input.sql_exposure
            == expected_v4_search_input_exposure(plan, operation, operation_input)
            && projection_input.required == lowered_required
            && projection_input.data_type == operation_input.data_type.lower()
            && projection_input
                .default_value
                .as_ref()
                .map(DeclaredDefaultValue::value)
                == operation_input
                    .default_value
                    .as_ref()
                    .map(DeclaredDefaultValue::value)
    })
}

fn expected_v4_search_input_exposure(
    plan: &ValidatedSurfacePlan,
    operation: &IrOperation,
    input: &coral_spec::v4::IrOperationInput,
) -> SqlInputExposure {
    if plan.pagination_owns_input(operation, &input.name, input.location) {
        return SqlInputExposure::Internal;
    }

    match &operation.execution {
        IrExecutionAttachment::Rest(_)
            if matches!(
                input.location,
                IrInputLocation::Path | IrInputLocation::Query | IrInputLocation::ToolArg
            ) =>
        {
            SqlInputExposure::FunctionArg
        }
        IrExecutionAttachment::Rest(_) => SqlInputExposure::Internal,
        IrExecutionAttachment::Mcp(_) => SqlInputExposure::FunctionArg,
    }
}

fn v4_operation_is_safe(operation: &IrOperation) -> bool {
    match &operation.execution {
        IrExecutionAttachment::Rest(rest) => rest.method == V4HttpMethod::Get,
        IrExecutionAttachment::Mcp(_) => operation.read_only && operation.idempotent,
    }
}

fn resolve_v4_query_input<'a>(
    operation: &IrOperation,
    projection: &'a Projection,
    query: &coral_spec::v4::V4UniversalSearchQueryInputSpec,
) -> Option<&'a ProjectionInput> {
    let location = match query.location {
        V4UniversalSearchInputLocation::Path => IrInputLocation::Path,
        V4UniversalSearchInputLocation::Query => IrInputLocation::Query,
        V4UniversalSearchInputLocation::ToolArg => IrInputLocation::ToolArg,
    };
    let operation_matches = operation
        .inputs
        .iter()
        .filter(|input| input.location == location && input.name == query.name)
        .count();
    if operation_matches != 1 {
        return None;
    }
    let projection_matches = projection
        .inputs
        .iter()
        .filter(|input| {
            input.source_location == location
                && input.wire_name == query.name
                && input.sql_exposure == SqlInputExposure::FunctionArg
        })
        .collect::<Vec<_>>();
    exactly_one(&projection_matches)
}

fn resolve_v4_result(
    operation: &IrOperation,
    projection: &Projection,
    mapping: Option<&UniversalSearchResultMappingSpec>,
) -> Option<ResolvedUniversalSearchResultMapping> {
    let mut resolved = match mapping {
        Some(mapping) => resolve_result_mapping(mapping, |pointer| {
            let path = decode_json_pointer(pointer)?;
            let columns = projection
                .columns
                .iter()
                .filter(|column| column.source_path == path)
                .collect::<Vec<_>>();
            let column = exactly_one(&columns)?;
            Some(ResolvedUniversalSearchResultField {
                column_name: column.name.clone(),
                data_type: column.data_type,
            })
        })?,
        None => ResolvedUniversalSearchResultMapping::default(),
    };
    if resolved.identity_fields.is_empty()
        && let Some(entity) = operation.entity.as_ref()
    {
        let fields = entity
            .identity_fields
            .iter()
            .map(|field| {
                let path = if field.starts_with('/') {
                    decode_json_pointer(field)?
                } else {
                    vec![field.clone()]
                };
                let matches = projection
                    .columns
                    .iter()
                    .filter(|column| column.source_path == path)
                    .collect::<Vec<_>>();
                let column = exactly_one(&matches)?;
                Some(ResolvedUniversalSearchResultField {
                    column_name: column.name.clone(),
                    data_type: column.data_type,
                })
            })
            .collect::<Option<Vec<_>>>();
        if let Some(fields) = fields.filter(|fields| {
            !fields.is_empty()
                && fields
                    .iter()
                    .all(|field| field.data_type != ManifestDataType::Json)
        }) {
            resolved
                .entity_type
                .get_or_insert_with(|| entity.name.clone());
            resolved.identity_fields = fields;
        }
    }
    Some(resolved)
}

fn resolve_result_mapping(
    mapping: &UniversalSearchResultMappingSpec,
    mut resolve: impl FnMut(&str) -> Option<ResolvedUniversalSearchResultField>,
) -> Option<ResolvedUniversalSearchResultMapping> {
    let identity_fields = mapping
        .identity_fields
        .iter()
        .map(|field| resolve(field))
        .collect::<Option<Vec<_>>>()?;
    let provider_id = resolve_optional_field(mapping.provider_id.as_deref(), &mut resolve).ok()?;
    let title = resolve_optional_field(mapping.title.as_deref(), &mut resolve).ok()?;
    let url = resolve_optional_field(mapping.url.as_deref(), &mut resolve).ok()?;
    let snippet = resolve_optional_field(mapping.snippet.as_deref(), &mut resolve).ok()?;
    let attributes = mapping
        .attributes
        .iter()
        .map(|field| resolve(field))
        .collect::<Option<Vec<_>>>()?;
    if identity_fields
        .iter()
        .chain([&provider_id, &title, &url, &snippet].into_iter().flatten())
        .any(|field| field.data_type == ManifestDataType::Json)
    {
        return None;
    }
    Some(ResolvedUniversalSearchResultMapping {
        authored_mapping: true,
        entity_type: mapping.entity_type.clone(),
        identity_fields,
        provider_id,
        title,
        url,
        snippet,
        attributes,
    })
}

fn resolve_optional_field<T>(
    authored: Option<&str>,
    resolve: &mut impl FnMut(&str) -> Option<T>,
) -> Result<Option<T>, ()> {
    match authored {
        Some(field) => resolve(field).map(Some).ok_or(()),
        None => Ok(None),
    }
}

fn decode_json_pointer(pointer: &str) -> Option<Vec<String>> {
    if pointer.is_empty() {
        return Some(Vec::new());
    }
    let tail = pointer.strip_prefix('/')?;
    tail.split('/')
        .map(|segment| {
            let mut decoded = String::new();
            let mut chars = segment.chars();
            while let Some(ch) = chars.next() {
                if ch != '~' {
                    decoded.push(ch);
                    continue;
                }
                match chars.next()? {
                    '0' => decoded.push('~'),
                    '1' => decoded.push('/'),
                    _ => return None,
                }
            }
            Some(decoded)
        })
        .collect()
}

fn default_matches_type(value: &Value, data_type: ManifestDataType) -> bool {
    match data_type {
        ManifestDataType::Utf8 => value.is_string(),
        ManifestDataType::Timestamp => value
            .as_str()
            .is_some_and(|value| DateTime::parse_from_rfc3339(value).is_ok()),
        ManifestDataType::Int64 => value.as_i64().is_some(),
        ManifestDataType::Boolean => value.is_boolean(),
        ManifestDataType::Float64 => value.is_number(),
        ManifestDataType::Json => true,
    }
}

fn exactly_one<'a, T>(values: &[&'a T]) -> Option<&'a T> {
    if values.len() == 1 {
        values.first().copied()
    } else {
        None
    }
}

fn v4_target(route: &V4UniversalSearchRouteSpec) -> ResolvedUniversalSearchTarget {
    ResolvedUniversalSearchTarget {
        operation_id: route.target.operation_id.clone(),
    }
}

fn projection_locator(
    schema_name: &str,
    projection: &Projection,
) -> UniversalSearchFunctionLocator {
    UniversalSearchFunctionLocator {
        schema_name: schema_name.to_string(),
        function_name: projection.name.clone(),
    }
}

fn diagnostic(
    source_name: &str,
    route_id: Option<String>,
    locator: Option<UniversalSearchFunctionLocator>,
    reason: UniversalSearchResolutionReason,
) -> UniversalSearchResolutionDiagnostic {
    UniversalSearchResolutionDiagnostic {
        source_name: source_name.to_string(),
        authored_route_id: route_id,
        locator,
        reason,
    }
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::indexing_slicing,
        reason = "single-element resolver fixtures should fail loudly when their shape changes"
    )]

    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use coral_engine::{
        CatalogInfo, CoralQuery, QueryRuntimeConfig, QuerySource, RuntimeSourceComponent,
        RuntimeSourcePackage,
    };
    use coral_spec::v4::{
        Fingerprint, FingerprintSurface, HttpMethod as V4HttpMethod, IrEntityCandidate,
        IrExecutionAttachment, IrInputLocation, IrOperation, IrOperationInput, IrOperationOutput,
        IrScalarType, IrType, IrTypeShape, MaterializedSurface, McpExecutionAttachment,
        McpOperationPagination, McpToolCatalog, McpToolDescriptor,
        OPERATION_METADATA_GENERATOR_VERSION, OperationMetadata, OperationMetadataCatalog,
        OutputCardinality, PROJECTION_GENERATOR_VERSION, Projection, ProjectionCatalog,
        ProjectionColumn, ProjectionInput, ProjectionKind, ProjectionVisibility,
        RestExecutionAttachment, RestResponseAttachment, SURFACE_IMPORTER_VERSION, SemanticIr,
        SqlInputExposure, SurfaceType, V4_ARTIFACT_SCHEMA_VERSION, V4MaterializedSource,
        V4SourceManifest, ValidatedSurfacePlan, generate_projection_catalog, import_mcp_surface,
    };
    use coral_spec::{
        DeclaredDefaultValue, ManifestDataType, PaginationSpec, ResponseSpec, SearchLimitsSpec,
        SourceTableFunctionKind, ValidatedSourceManifest, parse_source_manifest_yaml,
    };
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        ResolvedUniversalSearchTarget, UniversalSearchResolution, UniversalSearchResolutionOrigin,
        UniversalSearchResolutionReason, decode_json_pointer, resolve_universal_search, resolve_v4,
    };
    use crate::sources::runtime_package::{
        RuntimeContractFingerprint, runtime_component_for_v4_source,
    };

    #[test]
    fn json_pointer_decodes_rfc6901_escapes() {
        assert_eq!(
            decode_json_pointer("/repository/owner~1name/~0meta"),
            Some(vec![
                "repository".to_string(),
                "owner/name".to_string(),
                "~meta".to_string()
            ])
        );
        assert_eq!(decode_json_pointer("/bad~2escape"), None);
    }

    #[test]
    fn dsl_v3_search_functions_never_resolve_fanout_routes() {
        let manifest = parse_source_manifest_yaml(
            r"
name: legacy_search
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
functions:
  - name: search_items
    kind: search
    search_limits:
      default_top_k: 10
      max_top_k: 100
      max_calls_per_query: 1
    args:
      - name: query
        bind:
          arg: query
    request:
      method: GET
      path: /search
      query:
        - name: q
          from: arg
          key: query
    columns:
      - name: id
        type: Utf8
",
        )
        .expect("ordinary DSL v3 search function");
        let resolution = resolve_universal_search(
            "legacy_search",
            Uuid::nil(),
            &manifest,
            None,
            &RuntimeContractFingerprint::for_test("v1:dsl-v3"),
        );

        assert!(resolution.eligible_routes.is_empty());
        assert!(resolution.explicit_denials.is_empty());
        assert!(resolution.diagnostics.is_empty());
    }

    #[test]
    fn installed_and_manifest_source_name_mismatch_fails_closed() {
        let manifest = validated_v4_rest_manifest();
        let resolution = resolve_universal_search(
            "different_installed_name",
            &manifest,
            None,
            &RuntimeContractFingerprint::for_test("v1:identity-mismatch"),
        );

        assert_eq!(resolution.source_name, "different_installed_name");
        assert!(resolution.eligible_routes.is_empty());
        assert!(resolution.explicit_denials.is_empty());
        assert_eq!(resolution.diagnostics.len(), 1);
        assert_eq!(
            resolution.diagnostics[0].source_name,
            "different_installed_name"
        );
        assert!(resolution.diagnostics[0].locator.is_none());
        assert_eq!(
            resolution.diagnostics[0].reason,
            UniversalSearchResolutionReason::RouteStale
        );
        assert!(resolution.engine_authorizations().is_empty());
    }

    #[test]
    fn stale_materialized_source_headers_are_advisory() {
        let manifest = validated_v4_rest_manifest();
        let (_, mut materialized) = v4_rest_fixture(true, ProjectionVisibility::Published);
        materialized.projections.source_name = "legacy_runtime_component".to_string();
        materialized
            .fingerprint
            .as_mut()
            .expect("fixture fingerprint")
            .source_name = "legacy_fingerprint".to_string();
        mutate_surface_plan(&mut materialized, |semantic_ir, operation_metadata| {
            semantic_ir.source_name = "legacy_semantic_ir".to_string();
            operation_metadata.source_name = "legacy_operation_metadata".to_string();
        });

        let resolution = resolve_universal_search(
            "authored_owner",
            &manifest,
            Some(&materialized),
            &RuntimeContractFingerprint::for_test("v1:advisory-headers"),
        );

        assert!(resolution.diagnostics.is_empty());
        let route = resolution.eligible_routes.first().expect("resolved route");
        assert_eq!(route.source_name, "authored_owner");
        assert_eq!(route.locator.schema_name, "authored_owner");
        let authorization = resolution
            .engine_authorizations()
            .into_iter()
            .next()
            .expect("engine authorization");
        assert_eq!(authorization.schema_name, "authored_owner");
        assert_eq!(authorization.authorization.source_name, "authored_owner");
    }

    #[test]
    fn v4_explicit_allow_and_deny_remain_distinct() {
        let (mut manifest, mut materialized) =
            v4_rest_fixture(true, ProjectionVisibility::Published);
        clone_v4_operation(&mut materialized, "search/private", "search_private_items");
        let mut denial = manifest
            .universal_search
            .as_ref()
            .expect("policy")
            .routes
            .get("primary")
            .expect("primary route")
            .clone();
        denial.execute = false;
        denial.target.operation_id = "search/private".to_string();
        denial.query_input = None;
        denial.result = None;
        manifest
            .universal_search
            .as_mut()
            .expect("policy")
            .routes
            .insert("private".to_string(), denial);

        let resolution = resolve_v4(
            "authored_owner",
            &manifest,
            &materialized,
            &RuntimeContractFingerprint::for_test("v1:test"),
        );

        assert!(resolution.diagnostics.is_empty());
        assert_eq!(resolution.eligible_routes.len(), 1);
        assert_eq!(resolution.explicit_denials.len(), 1);
        assert_eq!(
            resolution.eligible_routes[0].origin,
            UniversalSearchResolutionOrigin::Explicit
        );
        assert_eq!(
            resolution.eligible_routes[0].target.operation_id,
            "search/items"
        );
        assert_eq!(
            resolution.explicit_denials[0].target.operation_id,
            "search/private"
        );
        for route in &resolution.eligible_routes {
            assert_eq!(route.source_name, "authored_owner");
            assert_eq!(route.locator.schema_name, route.source_name);
        }
        for denial in &resolution.explicit_denials {
            assert_eq!(denial.source_name, "authored_owner");
            assert_eq!(
                denial
                    .locator
                    .as_ref()
                    .map(|locator| locator.schema_name.as_str()),
                Some(denial.source_name.as_str())
            );
        }
        for authorization in resolution.engine_authorizations() {
            assert_eq!(authorization.schema_name, "authored_owner");
            assert_eq!(
                authorization.schema_name,
                authorization.authorization.source_name
            );
        }
    }

    #[test]
    fn one_invalid_v4_explicit_route_clears_source_eligibility() {
        let (mut manifest, mut materialized) =
            v4_rest_fixture(true, ProjectionVisibility::Published);
        clone_v4_operation(&mut materialized, "search/invalid", "search_invalid_items");
        materialized
            .projections
            .projections
            .last_mut()
            .expect("cloned projection")
            .search_limits = None;
        let mut invalid = manifest
            .universal_search
            .as_ref()
            .expect("policy")
            .routes
            .get("primary")
            .expect("primary route")
            .clone();
        invalid.target.operation_id = "search/invalid".to_string();
        manifest
            .universal_search
            .as_mut()
            .expect("policy")
            .routes
            .insert("invalid".to_string(), invalid);

        let resolution = resolve_v4(
            "authored_owner",
            &manifest,
            &materialized,
            &RuntimeContractFingerprint::for_test("v1:test"),
        );

        assert!(resolution.eligible_routes.is_empty());
        assert_eq!(resolution.diagnostics.len(), 1);
        assert_eq!(
            resolution.diagnostics[0].authored_route_id.as_deref(),
            Some("invalid")
        );
        assert_eq!(
            resolution.diagnostics[0].reason,
            UniversalSearchResolutionReason::InvalidSearchLimits
        );
    }

    #[test]
    fn v4_timestamp_defaults_require_rfc3339_values() {
        let (manifest, mut materialized) = v4_rest_fixture(true, ProjectionVisibility::Published);
        let valid_default = DeclaredDefaultValue::new(json!("2026-07-16T09:10:11+02:00"));
        mutate_surface_plan(&mut materialized, |semantic_ir, _| {
            semantic_ir.operations[0].inputs[1].data_type = IrScalarType::Timestamp;
            semantic_ir.operations[0].inputs[1].default_value = Some(valid_default.clone());
        });
        materialized.projections.projections[0].inputs[1].data_type = ManifestDataType::Timestamp;
        materialized.projections.projections[0].inputs[1].default_value =
            Some(valid_default.clone());
        let fingerprint = RuntimeContractFingerprint::for_test("v1:timestamp-default");

        let valid = resolve_v4("authored_owner", &manifest, &materialized, &fingerprint);
        assert!(valid.diagnostics.is_empty());
        assert_eq!(valid.eligible_routes.len(), 1);
        assert_eq!(
            valid.eligible_routes[0].default_arguments[0].value,
            json!("2026-07-16T09:10:11+02:00")
        );

        let invalid_default = DeclaredDefaultValue::new(json!("16 July 2026"));
        mutate_surface_plan(&mut materialized, |semantic_ir, _| {
            semantic_ir.operations[0].inputs[1].default_value = Some(invalid_default.clone());
        });
        materialized.projections.projections[0].inputs[1].default_value = Some(invalid_default);
        let invalid = resolve_v4("authored_owner", &manifest, &materialized, &fingerprint);
        assert!(invalid.eligible_routes.is_empty());
        assert_eq!(invalid.diagnostics.len(), 1);
        assert_eq!(
            invalid.diagnostics[0].reason,
            UniversalSearchResolutionReason::MissingArgumentDefault
        );
    }

    #[test]
    fn v4_explicit_route_uses_canonical_source_and_generated_function_locator() {
        let (manifest, materialized) = v4_rest_fixture(true, ProjectionVisibility::Published);
        let resolution = resolve_v4(
            "authored_owner",
            &manifest,
            &materialized,
            &RuntimeContractFingerprint::for_test("v1:fixture"),
        );

        assert!(resolution.diagnostics.is_empty());
        let route = resolution.eligible_routes.first().expect("resolved route");
        assert!(route.result.authored_mapping);
        assert_eq!(resolution.source_name, "authored_owner");
        assert_eq!(route.source_name, "authored_owner");
        assert_eq!(route.authored_route_id.as_deref(), Some("primary"));
        assert_eq!(route.locator.schema_name, "authored_owner");
        assert_eq!(route.locator.schema_name, route.source_name);
        assert_eq!(route.locator.function_name, "search_items");
        assert_eq!(
            route.target,
            ResolvedUniversalSearchTarget {
                operation_id: "search/items".to_string(),
            }
        );
        assert_eq!(route.default_arguments.len(), 1);
        assert!(route.default_arguments[0].value.is_null());
        assert_eq!(
            route
                .result
                .title
                .as_ref()
                .map(|field| field.column_name.as_str()),
            Some("repository_owner")
        );
        let authorizations = resolution.engine_authorizations();
        assert_eq!(authorizations.len(), 1);
        assert_eq!(authorizations[0].schema_name, route.source_name);
        assert_eq!(
            authorizations[0].authorization.source_name,
            route.source_name
        );
    }

    #[test]
    fn v4_guide_gated_projection_is_not_eligible_for_fanout() {
        let (mut manifest, mut materialized) =
            v4_rest_fixture(true, ProjectionVisibility::Published);
        materialized.projections.projections[0].guide =
            "Read this source-specific guidance before querying.".to_string();
        materialized.projections.projections[0].require_guide_read = true;
        let fingerprint = RuntimeContractFingerprint::for_test("v1:guide-gated");

        let explicit = resolve_v4("authored_owner", &manifest, &materialized, &fingerprint);

        assert!(explicit.eligible_routes.is_empty());
        assert_eq!(explicit.diagnostics.len(), 1);
        assert_eq!(
            explicit.diagnostics[0].reason,
            UniversalSearchResolutionReason::UnsafeOperation
        );
        assert!(explicit.engine_authorizations().is_empty());

        manifest.universal_search = None;
        let inferred = resolve_v4("authored_owner", &manifest, &materialized, &fingerprint);
        assert!(inferred.eligible_routes.is_empty());
        assert_eq!(inferred.diagnostics.len(), 1);
        assert_eq!(
            inferred.diagnostics[0].reason,
            UniversalSearchResolutionReason::UnsafeOperation
        );
        assert!(inferred.engine_authorizations().is_empty());
    }

    #[test]
    fn malformed_locator_identity_is_not_attached_to_the_engine() {
        let (manifest, materialized) = v4_rest_fixture(true, ProjectionVisibility::Published);
        let mut resolution = resolve_v4(
            "authored_owner",
            &manifest,
            &materialized,
            &RuntimeContractFingerprint::for_test("v1:malformed-locator"),
        );
        resolution.eligible_routes[0].locator.schema_name = "stale_component_name".to_string();

        assert!(resolution.engine_authorizations().is_empty());

        resolution.eligible_routes[0].locator.schema_name = "authored_owner".to_string();
        resolution.eligible_routes[0].source_name = "stale_component_name".to_string();
        assert!(resolution.engine_authorizations().is_empty());
    }

    #[test]
    fn v4_upstream_operation_rename_fails_closed_without_retargeting() {
        let (manifest, mut materialized) = v4_rest_fixture(true, ProjectionVisibility::Published);
        mutate_surface_plan(&mut materialized, |semantic_ir, metadata| {
            semantic_ir.operations[0].id = "search/items_v2".to_string();
            let operation_metadata = metadata
                .operations
                .remove("search/items")
                .expect("fixture operation metadata");
            metadata
                .operations
                .insert("search/items_v2".to_string(), operation_metadata);
        });
        materialized.projections.projections[0].operation_id = "search/items_v2".to_string();

        let resolution = resolve_v4(
            "authored_owner",
            &manifest,
            &materialized,
            &RuntimeContractFingerprint::for_test("v1:renamed-operation"),
        );

        assert!(resolution.eligible_routes.is_empty());
        assert_eq!(resolution.diagnostics.len(), 1);
        assert_eq!(
            resolution.diagnostics[0].reason,
            UniversalSearchResolutionReason::RouteStale
        );
        assert_eq!(
            resolution.diagnostics[0].authored_route_id.as_deref(),
            Some("primary")
        );
    }

    #[test]
    fn v4_projection_collision_rename_changes_only_runtime_locator() {
        let (manifest, mut materialized) = v4_rest_fixture(true, ProjectionVisibility::Published);
        let fingerprint = RuntimeContractFingerprint::for_test("v1:projection-collision");
        let before = resolve_v4("authored_owner", &manifest, &materialized, &fingerprint);
        let before = before.eligible_routes.first().expect("original route");

        materialized.projections.projections[0].name = "search_items__collision_suffix".to_string();
        let after = resolve_v4("authored_owner", &manifest, &materialized, &fingerprint);
        let after = after
            .eligible_routes
            .first()
            .expect("collision-renamed route");

        assert_eq!(after.authored_route_id, before.authored_route_id);
        assert_eq!(after.target, before.target);
        assert_eq!(after.source_name, before.source_name);
        assert_eq!(before.locator.schema_name, before.source_name);
        assert_eq!(after.locator.schema_name, after.source_name);
        assert_eq!(
            after.runtime_contract_fingerprint,
            before.runtime_contract_fingerprint
        );
        assert_eq!(before.locator.function_name, "search_items");
        assert_eq!(
            after.locator.function_name,
            "search_items__collision_suffix"
        );
    }

    #[test]
    fn v4_structured_identity_fields_fail_closed() {
        let (mut manifest, mut materialized) =
            v4_rest_fixture(true, ProjectionVisibility::Published);
        manifest
            .universal_search
            .as_mut()
            .expect("policy")
            .routes
            .get_mut("primary")
            .expect("route")
            .result
            .as_mut()
            .expect("result mapping")
            .identity_fields = vec!["/payload".to_string()];
        materialized.projections.projections[0]
            .columns
            .push(ProjectionColumn {
                name: "payload".to_string(),
                data_type: ManifestDataType::Json,
                source_path: vec!["payload".to_string()],
                nullable: true,
                description: String::new(),
                do_not_index: false,
            });
        let v4_resolution = resolve_v4(
            "authored_owner",
            &manifest,
            &materialized,
            &RuntimeContractFingerprint::for_test("v1:v4-structured-identity"),
        );
        assert!(v4_resolution.eligible_routes.is_empty());
        assert_eq!(
            v4_resolution.diagnostics[0].reason,
            UniversalSearchResolutionReason::RouteStale
        );
    }

    #[test]
    fn v4_projection_inputs_must_match_exact_ir_inputs() {
        let (manifest, mut materialized) = v4_rest_fixture(true, ProjectionVisibility::Published);
        materialized.projections.projections[0].inputs[1].default_value = Some(
            DeclaredDefaultValue::new(serde_json::json!({"stale": true})),
        );

        let resolution = resolve_v4(
            "authored_owner",
            &manifest,
            &materialized,
            &RuntimeContractFingerprint::for_test("v1:stale-projection-input"),
        );
        assert!(resolution.eligible_routes.is_empty());
        assert_eq!(
            resolution.diagnostics[0].reason,
            UniversalSearchResolutionReason::RouteStale
        );
    }

    #[test]
    fn v4_projection_requiredness_matches_default_lowering() {
        let (manifest, mut materialized) = v4_rest_fixture(true, ProjectionVisibility::Published);
        mutate_surface_plan(&mut materialized, |semantic_ir, _| {
            semantic_ir.operations[0].inputs[1].required = true;
        });
        assert!(!materialized.projections.projections[0].inputs[1].required);

        let resolution = resolve_v4(
            "authored_owner",
            &manifest,
            &materialized,
            &RuntimeContractFingerprint::for_test("v1:required-default"),
        );
        assert_eq!(resolution.eligible_routes.len(), 1);
        assert!(resolution.diagnostics.is_empty());
    }

    #[test]
    fn v4_projection_inputs_reject_duplicate_query_visible_names() {
        let (manifest, mut materialized) = v4_rest_fixture(true, ProjectionVisibility::Published);
        materialized.projections.projections[0].inputs[1].name = "query".to_string();

        let resolution = resolve_v4(
            "authored_owner",
            &manifest,
            &materialized,
            &RuntimeContractFingerprint::for_test("v1:duplicate-input-name"),
        );
        assert!(resolution.eligible_routes.is_empty());
        assert_eq!(
            resolution.diagnostics[0].reason,
            UniversalSearchResolutionReason::RouteStale
        );
    }

    #[test]
    fn v4_projection_inputs_require_a_complete_operation_bijection() {
        let (manifest, mut missing_input) = v4_rest_fixture(true, ProjectionVisibility::Published);
        missing_input.projections.projections[0].inputs.pop();
        let missing = resolve_v4(
            "authored_owner",
            &manifest,
            &missing_input,
            &RuntimeContractFingerprint::for_test("v1:missing-projection-input"),
        );
        assert!(missing.eligible_routes.is_empty());
        assert_eq!(
            missing.diagnostics[0].reason,
            UniversalSearchResolutionReason::RouteStale
        );

        let (manifest, mut hidden_input) = v4_rest_fixture(true, ProjectionVisibility::Published);
        hidden_input.projections.projections[0].inputs[1].sql_exposure = SqlInputExposure::Internal;
        let hidden = resolve_v4(
            "authored_owner",
            &manifest,
            &hidden_input,
            &RuntimeContractFingerprint::for_test("v1:hidden-projection-input"),
        );
        assert!(hidden.eligible_routes.is_empty());
        assert_eq!(
            hidden.diagnostics[0].reason,
            UniversalSearchResolutionReason::RouteStale
        );
    }

    #[test]
    fn v4_projection_search_limits_must_remain_bounded() {
        for limits in [
            SearchLimitsSpec {
                default_top_k: 5,
                max_top_k: 5,
                max_calls_per_query: 0,
            },
            SearchLimitsSpec {
                default_top_k: 5,
                max_top_k: 5,
                max_calls_per_query: 101,
            },
        ] {
            let (manifest, mut materialized) =
                v4_rest_fixture(true, ProjectionVisibility::Published);
            materialized.projections.projections[0].search_limits = Some(limits);

            let resolution = resolve_v4(
                "authored_owner",
                &manifest,
                &materialized,
                &RuntimeContractFingerprint::for_test("v1:invalid-search-limits"),
            );
            assert!(resolution.eligible_routes.is_empty());
            assert_eq!(
                resolution.diagnostics[0].reason,
                UniversalSearchResolutionReason::InvalidSearchLimits
            );
        }

        let (manifest, mut materialized) = v4_rest_fixture(true, ProjectionVisibility::Published);
        materialized.projections.projections[0].search_limits = None;
        let resolution = resolve_v4(
            "authored_owner",
            &manifest,
            &materialized,
            &RuntimeContractFingerprint::for_test("v1:missing-search-limits"),
        );
        assert!(resolution.eligible_routes.is_empty());
        assert_eq!(
            resolution.diagnostics[0].reason,
            UniversalSearchResolutionReason::InvalidSearchLimits
        );
    }

    #[test]
    fn v4_ir_identity_fields_fill_an_authored_entity_label() {
        let (mut manifest, mut materialized) =
            v4_rest_fixture(true, ProjectionVisibility::Published);
        manifest
            .universal_search
            .as_mut()
            .expect("policy")
            .routes
            .get_mut("primary")
            .expect("route")
            .result
            .as_mut()
            .expect("result mapping")
            .entity_type = Some("authored_issue".to_string());
        mutate_surface_plan(&mut materialized, |semantic_ir, _| {
            semantic_ir.operations[0].entity = Some(IrEntityCandidate {
                name: "ir_issue".to_string(),
                type_ref: "row".to_string(),
                identity_fields: vec!["/repository/owner~1name".to_string()],
            });
        });

        let resolution = resolve_v4(
            "authored_owner",
            &manifest,
            &materialized,
            &RuntimeContractFingerprint::for_test("v1:entity-fallback"),
        );
        let result = &resolution.eligible_routes[0].result;
        assert!(result.authored_mapping);
        assert_eq!(result.entity_type.as_deref(), Some("authored_issue"));
        assert_eq!(result.identity_fields.len(), 1);
        assert_eq!(result.identity_fields[0].column_name, "repository_owner");
    }

    #[test]
    fn v4_hidden_or_unsafe_inferred_routes_fail_closed() {
        let (manifest, hidden) = v4_rest_fixture(true, ProjectionVisibility::Hidden);
        let hidden_resolution = resolve_v4(
            "authored_owner",
            &manifest,
            &hidden,
            &RuntimeContractFingerprint::for_test("v1:hidden"),
        );
        assert!(hidden_resolution.eligible_routes.is_empty());
        assert_eq!(
            hidden_resolution.diagnostics[0].reason,
            UniversalSearchResolutionReason::RouteStale
        );

        let (mut inferred_manifest, unsafe_materialized) =
            v4_rest_fixture(false, ProjectionVisibility::Published);
        inferred_manifest.universal_search = None;
        let inferred = resolve_v4(
            "authored_owner",
            &inferred_manifest,
            &unsafe_materialized,
            &RuntimeContractFingerprint::for_test("v1:unsafe"),
        );
        assert!(inferred.eligible_routes.is_empty());
        assert_eq!(
            inferred.diagnostics[0].reason,
            UniversalSearchResolutionReason::UnsafeOperation
        );
    }

    #[test]
    fn v4_unresolvable_ir_identity_is_an_optional_fallback() {
        let (mut manifest, mut materialized) =
            v4_rest_fixture(true, ProjectionVisibility::Published);
        manifest
            .universal_search
            .as_mut()
            .expect("policy")
            .routes
            .get_mut("primary")
            .expect("route")
            .result = None;
        mutate_surface_plan(&mut materialized, |semantic_ir, _| {
            semantic_ir.operations[0].entity = Some(IrEntityCandidate {
                name: "response".to_string(),
                type_ref: "row".to_string(),
                identity_fields: vec!["id".to_string()],
            });
        });

        let resolution = resolve_v4(
            "authored_owner",
            &manifest,
            &materialized,
            &RuntimeContractFingerprint::for_test("v1:missing-ir-identity"),
        );
        assert_eq!(resolution.eligible_routes.len(), 1);
        assert!(
            resolution.eligible_routes[0]
                .result
                .identity_fields
                .is_empty()
        );
        assert!(resolution.diagnostics.is_empty());
    }

    #[test]
    fn v4_mcp_inference_requires_both_read_only_and_idempotent_evidence() {
        let (mut manifest, mut materialized) =
            v4_rest_fixture(true, ProjectionVisibility::Published);
        manifest.universal_search = None;
        mutate_surface_plan(&mut materialized, |semantic_ir, metadata| {
            semantic_ir.surface_type = SurfaceType::Mcp;
            let operation = &mut semantic_ir.operations[0];
            for input in &mut operation.inputs {
                input.location = IrInputLocation::ToolArg;
            }
            operation.read_only = true;
            operation.idempotent = false;
            operation.execution = IrExecutionAttachment::Mcp(McpExecutionAttachment {
                tool_name: "search_items".to_string(),
            });
            metadata.operations.insert(
                operation.id.clone(),
                OperationMetadata::Mcp {
                    row_path: Vec::new(),
                    pagination: McpOperationPagination::default(),
                },
            );
        });
        for input in &mut materialized.projections.projections[0].inputs {
            input.source_location = IrInputLocation::ToolArg;
        }

        let unsafe_resolution = resolve_v4(
            "authored_owner",
            &manifest,
            &materialized,
            &RuntimeContractFingerprint::for_test("v1:mcp-unsafe"),
        );
        assert!(unsafe_resolution.eligible_routes.is_empty());
        assert_eq!(
            unsafe_resolution.diagnostics[0].reason,
            UniversalSearchResolutionReason::UnsafeOperation
        );

        mutate_surface_plan(&mut materialized, |semantic_ir, _| {
            semantic_ir.operations[0].idempotent = true;
        });
        let safe_resolution = resolve_v4(
            "authored_owner",
            &manifest,
            &materialized,
            &RuntimeContractFingerprint::for_test("v1:mcp-safe"),
        );
        assert_eq!(safe_resolution.eligible_routes.len(), 1);
        assert_eq!(
            safe_resolution.eligible_routes[0].origin,
            UniversalSearchResolutionOrigin::Inferred
        );
        assert!(!safe_resolution.eligible_routes[0].result.authored_mapping);
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one end-to-end fixture keeps the complete typed-default matrix auditable"
    )]
    async fn explicit_v4_mcp_route_preserves_all_typed_defaults_through_catalog() {
        let manifest = parse_source_manifest_yaml(
            r"
name: installed_mcp
dsl_version: 4
universal_search:
  routes:
    record_search:
      execute: true
      target:
        operation_id: search_records
      query_input:
        location: tool_arg
        name: query
      result:
        entity_type: record
        identity_fields: [/id]
        title: /title
surface:
    type: mcp
    server:
      transport: stdio
      command: demo-mcp-server
",
        )
        .expect("v4 MCP manifest");
        let tool_catalog = McpToolCatalog {
            tools: vec![McpToolDescriptor {
                name: "search_records".to_string(),
                title: None,
                description: Some("Search records".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "scope": {"type": "string", "default": "all"},
                        "threshold": {"type": "number", "default": 0.75},
                        "exact": {"type": "boolean", "default": false},
                        "tags": {
                            "type": "array",
                            "items": {"type": "string"},
                            "default": ["bug", "docs"]
                        },
                        "options": {
                            "type": "object",
                            "default": {"sort": "recent"}
                        },
                        "nullable_options": {
                            "type": ["object", "null"],
                            "default": null
                        }
                    },
                    "required": ["query"]
                }),
                output_schema: Some(json!({
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "title": {"type": "string"}
                        },
                        "required": ["id", "title"]
                    }
                })),
                read_only_hint: Some(true),
                idempotent_hint: Some(true),
            }],
        };
        let materialized = materialize_mcp_fixture(&manifest, &tool_catalog);
        let fingerprint = RuntimeContractFingerprint::for_test("v1:mcp-defaults");
        let resolution = resolve_universal_search(
            "installed_mcp",
            Uuid::nil(),
            &manifest,
            Some(&materialized),
            &fingerprint,
        );

        assert!(resolution.diagnostics.is_empty());
        let route = resolution
            .eligible_routes
            .first()
            .expect("resolved MCP route");
        assert_eq!(route.authored_route_id.as_deref(), Some("record_search"));
        assert_eq!(route.source_name, "installed_mcp");
        assert_eq!(route.locator.schema_name, route.source_name);
        assert_eq!(route.target.operation_id, "search_records");
        assert_eq!(route.query_argument.name, "query");
        assert_eq!(route.default_arguments.len(), 6);

        let v4 = manifest.as_v4().expect("v4 manifest");
        let components = runtime_component_for_v4_source(v4, &materialized)
            .expect("runtime MCP component")
            .into_iter()
            .collect::<Vec<_>>();
        let catalog =
            runtime_catalog_from_components("installed_mcp", components, &resolution).await;
        let function = catalog
            .table_functions
            .iter()
            .find(|function| function.function_name == "search_records")
            .expect("search_records catalog function");
        assert_eq!(function.schema_name, "installed_mcp");
        let authorization = function
            .universal_search
            .as_ref()
            .expect("passive MCP authorization");
        assert_eq!(authorization.source_name, "installed_mcp");
        assert_eq!(authorization.route_id.as_deref(), Some("record_search"));

        let defaults = function
            .arguments
            .iter()
            .map(|argument| {
                (
                    argument.name.as_str(),
                    (
                        argument.data_type.as_str(),
                        argument.default_json.as_deref(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(defaults.get("query"), Some(&("Utf8", None)));
        assert_eq!(defaults.get("scope"), Some(&("Utf8", Some("\"all\""))));
        assert_eq!(defaults.get("threshold"), Some(&("Float64", Some("0.75"))));
        assert_eq!(defaults.get("exact"), Some(&("Boolean", Some("false"))));
        assert_eq!(
            defaults.get("tags"),
            Some(&("Json", Some("[\"bug\",\"docs\"]")))
        );
        assert_eq!(
            defaults.get("options"),
            Some(&("Json", Some("{\"sort\":\"recent\"}")))
        );
        assert_eq!(
            defaults.get("nullable_options"),
            Some(&("Json", Some("null")))
        );
    }

    #[tokio::test]
    async fn source_diagnostic_survives_without_attachable_function_and_catalog_stays_usable() {
        let manifest = parse_source_manifest_yaml(
            r"
name: installed_stale
dsl_version: 4
universal_search:
  routes:
    missing_search:
      execute: true
      target:
        operation_id: deleted_search_tool
      query_input:
        location: tool_arg
        name: query
surface:
    type: mcp
    server:
      transport: stdio
      command: demo-mcp-server
",
        )
        .expect("stale-route manifest");
        let tool_catalog = McpToolCatalog {
            tools: vec![McpToolDescriptor {
                name: "lookup_records".to_string(),
                title: None,
                description: Some("Ordinary catalog function".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {"record_id": {"type": "string"}},
                    "required": ["record_id"]
                }),
                output_schema: Some(json!({
                    "type": "object",
                    "properties": {"id": {"type": "string"}}
                })),
                read_only_hint: Some(true),
                idempotent_hint: Some(true),
            }],
        };
        let materialized = materialize_mcp_fixture(&manifest, &tool_catalog);
        let fingerprint = RuntimeContractFingerprint::for_test("v1:stale-route");
        let resolution = resolve_universal_search(
            "installed_stale",
            Uuid::nil(),
            &manifest,
            Some(&materialized),
            &fingerprint,
        );

        assert!(resolution.eligible_routes.is_empty());
        assert_eq!(resolution.diagnostics.len(), 1);
        assert_eq!(
            resolution.diagnostics[0].reason,
            UniversalSearchResolutionReason::RouteStale
        );
        assert!(resolution.diagnostics[0].locator.is_none());

        let components =
            runtime_component_for_v4_source(manifest.as_v4().expect("v4 manifest"), &materialized)
                .expect("ordinary runtime component")
                .into_iter()
                .collect::<Vec<_>>();
        let catalog =
            runtime_catalog_from_components("installed_stale", components, &resolution).await;
        let ordinary_function = catalog
            .table_functions
            .iter()
            .find(|function| function.function_name == "lookup_records")
            .expect("ordinary function remains discoverable");
        assert!(ordinary_function.universal_search.is_none());
        assert_eq!(resolution.diagnostics.len(), 1);
    }

    async fn runtime_catalog_from_components(
        source_name: &str,
        components: Vec<RuntimeSourceComponent>,
        resolution: &UniversalSearchResolution,
    ) -> CatalogInfo {
        let source = QuerySource::from_runtime_components(
            RuntimeSourcePackage {
                source_name: source_name.to_string(),
                authored_version: None,
                description: String::new(),
                declared_inputs: Vec::new(),
                test_queries: Vec::new(),
                identity_requirements: None,
                components,
                universal_search_authorizations: resolution.engine_authorizations(),
            },
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("runtime source package");
        CoralQuery::list_catalog(
            std::slice::from_ref(&source),
            QueryRuntimeConfig::default(),
            None,
            None,
        )
        .await
        .expect("runtime catalog")
    }

    fn materialize_mcp_fixture(
        manifest: &ValidatedSourceManifest,
        tool_catalog: &McpToolCatalog,
    ) -> V4MaterializedSource {
        let v4 = manifest.as_v4().expect("v4 manifest");
        let surface = &v4.surface;
        let imported = import_mcp_surface(v4, surface, tool_catalog).expect("MCP semantic IR");
        let plan = imported.validated_plan().expect("validated MCP plan");
        let projections = generate_projection_catalog(v4, &plan).expect("MCP projection catalog");
        V4MaterializedSource {
            fingerprint: Some(Fingerprint {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: v4.common.name.clone(),
                manifest_sha256: String::new(),
                surface: FingerprintSurface {
                    surface_type: surface.surface_type,
                    descriptor_kind: surface.descriptor.kind().to_string(),
                    descriptor_location: surface.descriptor.location(),
                    descriptor_sha256: String::new(),
                    input_declarations_sha256: String::new(),
                },
                importer_version: SURFACE_IMPORTER_VERSION.to_string(),
                operation_metadata_generator_version: OPERATION_METADATA_GENERATOR_VERSION
                    .to_string(),
                projection_generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
            }),
            surface: MaterializedSurface {
                plan,
                source_document_sha256: None,
                normalized_source_document_path: PathBuf::new(),
                raw_source_document_path: PathBuf::new(),
            },
            projections,
            diagnostics: Vec::new(),
        }
    }

    fn validated_v4_rest_manifest() -> ValidatedSourceManifest {
        parse_source_manifest_yaml(
            r"
name: authored_owner
dsl_version: 4
universal_search:
  routes:
    primary:
      execute: true
      target:
        operation_id: search/items
      query_input:
        location: query
        name: q
      result:
        title: /repository/owner~1name
surface:
  type: openapi
  file: /tmp/openapi.yaml
",
        )
        .expect("v4 REST manifest")
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the fixture keeps one complete operation and projection contract together"
    )]
    fn v4_rest_fixture(
        safe_method: bool,
        visibility: ProjectionVisibility,
    ) -> (V4SourceManifest, V4MaterializedSource) {
        let manifest = validated_v4_rest_manifest()
            .as_v4()
            .expect("v4 manifest")
            .clone();
        let operation = IrOperation {
            id: "search/items".to_string(),
            method_name: "search_items".to_string(),
            description: String::new(),
            deprecated: false,
            read_only: safe_method,
            idempotent: safe_method,
            naming: None,
            inputs: vec![
                IrOperationInput {
                    name: "q".to_string(),
                    location: IrInputLocation::Query,
                    required: true,
                    data_type: IrScalarType::String,
                    default_value: None,
                    description: String::new(),
                },
                IrOperationInput {
                    name: "options".to_string(),
                    location: IrInputLocation::Query,
                    required: false,
                    data_type: IrScalarType::Json,
                    default_value: Some(DeclaredDefaultValue::new(serde_json::Value::Null)),
                    description: String::new(),
                },
            ],
            output: IrOperationOutput {
                cardinality: OutputCardinality::List,
                type_ref: "row".to_string(),
            },
            entity: None,
            execution: IrExecutionAttachment::Rest(Box::new(RestExecutionAttachment {
                method: if safe_method {
                    V4HttpMethod::Get
                } else {
                    V4HttpMethod::Post
                },
                path_template: "/search".to_string(),
                parameters: Vec::new(),
                request_body: None,
                response: RestResponseAttachment {
                    status_code: 200,
                    media_type: "application/json".to_string(),
                    response: ResponseSpec::default(),
                },
            })),
            diagnostics: Vec::new(),
        };
        let projection = Projection {
            name: "search_items".to_string(),
            kind: ProjectionKind::TableFunction {
                function_kind: SourceTableFunctionKind::Search,
            },
            description: String::new(),
            guide: String::new(),
            require_guide_read: false,
            operation_id: "search/items".to_string(),
            visibility,
            inputs: vec![
                ProjectionInput {
                    name: "query".to_string(),
                    sql_exposure: SqlInputExposure::FunctionArg,
                    source_location: IrInputLocation::Query,
                    wire_name: "q".to_string(),
                    required: true,
                    data_type: ManifestDataType::Utf8,
                    default_value: None,
                    description: String::new(),
                    lookup_key: false,
                },
                ProjectionInput {
                    name: "options".to_string(),
                    sql_exposure: SqlInputExposure::FunctionArg,
                    source_location: IrInputLocation::Query,
                    wire_name: "options".to_string(),
                    required: false,
                    data_type: ManifestDataType::Json,
                    default_value: Some(DeclaredDefaultValue::new(serde_json::Value::Null)),
                    description: String::new(),
                    lookup_key: false,
                },
            ],
            columns: vec![ProjectionColumn {
                name: "repository_owner".to_string(),
                data_type: ManifestDataType::Utf8,
                source_path: vec!["repository".to_string(), "owner/name".to_string()],
                nullable: true,
                description: String::new(),
                do_not_index: false,
            }],
            search_limits: Some(SearchLimitsSpec {
                default_top_k: 5,
                max_top_k: 5,
                max_calls_per_query: 1,
            }),
            detail_hints: Vec::new(),
            diagnostics: Vec::new(),
        };
        let plan = ValidatedSurfacePlan::new(
            SemanticIr {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "authored_owner".to_string(),
                surface_type: SurfaceType::OpenApi,
                importer_version: String::new(),
                operations: vec![operation],
                types: vec![IrType {
                    id: "row".to_string(),
                    shape: IrTypeShape::Json,
                    nullable: true,
                    description: String::new(),
                }],
                diagnostics: Vec::new(),
            },
            OperationMetadataCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "authored_owner".to_string(),
                generator_version: Some(OPERATION_METADATA_GENERATOR_VERSION.to_string()),
                operations: BTreeMap::from([(
                    "search/items".to_string(),
                    OperationMetadata::Rest {
                        row_path: Vec::new(),
                        pagination: PaginationSpec::default(),
                        lookup_keys: Vec::new(),
                    },
                )]),
            },
        )
        .expect("valid REST fixture plan");
        (
            manifest,
            V4MaterializedSource {
                fingerprint: Some(Fingerprint {
                    artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                    source_name: "authored_owner".to_string(),
                    manifest_sha256: String::new(),
                    surface: FingerprintSurface {
                        surface_type: SurfaceType::OpenApi,
                        descriptor_kind: "file".to_string(),
                        descriptor_location: "/tmp/openapi.yaml".to_string(),
                        descriptor_sha256: String::new(),
                        input_declarations_sha256: String::new(),
                    },
                    importer_version: SURFACE_IMPORTER_VERSION.to_string(),
                    operation_metadata_generator_version: OPERATION_METADATA_GENERATOR_VERSION
                        .to_string(),
                    projection_generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
                }),
                surface: MaterializedSurface {
                    plan,
                    source_document_sha256: None,
                    normalized_source_document_path: PathBuf::new(),
                    raw_source_document_path: PathBuf::new(),
                },
                projections: ProjectionCatalog {
                    artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                    source_name: "authored_owner".to_string(),
                    generator_version: Some(PROJECTION_GENERATOR_VERSION.to_string()),
                    projections: vec![projection],
                    diagnostics: Vec::new(),
                },
                diagnostics: Vec::new(),
            },
        )
    }

    fn mutate_surface_plan(
        materialized: &mut V4MaterializedSource,
        mutate: impl FnOnce(&mut SemanticIr, &mut OperationMetadataCatalog),
    ) {
        let mut semantic_ir = materialized.surface.plan.semantic_ir().clone();
        let mut operation_metadata = materialized.surface.plan.operation_metadata().clone();
        mutate(&mut semantic_ir, &mut operation_metadata);
        materialized.surface.plan =
            ValidatedSurfacePlan::new(semantic_ir, operation_metadata).expect("valid mutated plan");
    }

    fn clone_v4_operation(
        materialized: &mut V4MaterializedSource,
        operation_id: &str,
        projection_name: &str,
    ) {
        mutate_surface_plan(materialized, |semantic_ir, operation_metadata| {
            let original_id = semantic_ir
                .operations
                .first()
                .expect("fixture operation")
                .id
                .clone();
            let mut operation = semantic_ir
                .operations
                .first()
                .expect("fixture operation")
                .clone();
            operation.id = operation_id.to_string();
            operation.method_name = projection_name.to_string();
            semantic_ir.operations.push(operation);

            let metadata = operation_metadata
                .operations
                .get(&original_id)
                .expect("fixture operation metadata")
                .clone();
            operation_metadata
                .operations
                .insert(operation_id.to_string(), metadata);
        });

        let mut projection = materialized
            .projections
            .projections
            .first()
            .expect("fixture projection")
            .clone();
        projection.operation_id = operation_id.to_string();
        projection.name = projection_name.to_string();
        materialized.projections.projections.push(projection);
    }
}
