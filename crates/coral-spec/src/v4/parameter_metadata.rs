use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::Deserialize;

use crate::v4::ir::{HttpMethod, IrExecutionAttachment, IrInputLocation, IrOperation, SemanticIr};
use crate::v4::projections::pagination_query_param_names;
use crate::v4::projections::{
    ProjectionCatalog, ProjectionInput, ProjectionKind, SqlInputExposure,
};
use crate::{ManifestError, PaginationSpec, Result};

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ParameterMetadataOverrides {
    #[serde(default)]
    pub pagination: Vec<NamedPaginationOverride>,
    #[serde(default)]
    pub operation_overrides: BTreeMap<String, OperationParameterMetadataOverride>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedPaginationOverride {
    pub name: String,
    #[serde(rename = "match")]
    pub match_rules: PaginationStrategyMatch,
    #[serde(flatten)]
    pagination: PaginationSpec,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PaginationStrategyMatch {
    #[serde(default)]
    pub operation_ids: Vec<String>,
    #[serde(default)]
    pub methods: Vec<String>,
    #[serde(default)]
    pub path_patterns: Vec<String>,
    #[serde(default)]
    pub required_query_params: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationParameterMetadataOverride {
    pub pagination: PaginationSpec,
}

pub fn parse_parameter_metadata_overrides_yaml(raw: &str) -> Result<ParameterMetadataOverrides> {
    let overrides: ParameterMetadataOverrides =
        serde_yaml::from_str(raw).map_err(ManifestError::parse_yaml)?;
    overrides.validate_shape()?;
    Ok(overrides)
}

pub fn apply_parameter_metadata_overrides(
    ir: &mut SemanticIr,
    overrides: &ParameterMetadataOverrides,
) -> Result<()> {
    overrides.validate_for_surface(ir)?;

    for operation in &mut ir.operations {
        let mut matched = None;
        for strategy in &overrides.pagination {
            if strategy.matches_operation(operation) {
                matched = Some(strategy.pagination.clone());
                break;
            }
        }

        if let Some(pagination) = matched
            && let IrExecutionAttachment::Rest(rest) = &mut operation.execution
        {
            rest.pagination = pagination;
        }
    }

    for (operation_id, operation_override) in &overrides.operation_overrides {
        let operation = ir
            .operations
            .iter_mut()
            .find(|operation| operation_matches_identifier(operation, operation_id))
            .ok_or_else(|| {
                ManifestError::validation(format!(
                    "parameter metadata operation override '{operation_id}' references unknown operation '{operation_id}'"
                ))
            })?;
        if let IrExecutionAttachment::Rest(rest) = &mut operation.execution {
            rest.pagination = operation_override.pagination.clone();
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionPaginationInputSyncMode {
    RecomputeRestInputExposure,
    PreserveExistingExposure,
}

pub fn sync_projection_pagination_inputs<'a>(
    surfaces: impl IntoIterator<Item = &'a SemanticIr>,
    projections: &mut ProjectionCatalog,
    mode: ProjectionPaginationInputSyncMode,
) {
    let pagination_by_operation = surfaces
        .into_iter()
        .flat_map(|surface| {
            surface.operations.iter().filter_map(|operation| {
                let IrExecutionAttachment::Rest(rest) = &operation.execution else {
                    return None;
                };
                Some((
                    (surface.surface_id.as_str(), operation.id.as_str()),
                    pagination_query_param_names(&rest.pagination),
                ))
            })
        })
        .collect::<BTreeMap<_, _>>();

    for projection in &mut projections.projections {
        let Some(pagination_query_params) = pagination_by_operation.get(&(
            projection.surface_id.as_str(),
            projection.operation_id.as_str(),
        )) else {
            continue;
        };
        let default_exposure = match projection.kind {
            ProjectionKind::Table => SqlInputExposure::Filter,
            ProjectionKind::TableFunction { .. } => SqlInputExposure::FunctionArg,
        };
        for input in &mut projection.inputs {
            match mode {
                ProjectionPaginationInputSyncMode::RecomputeRestInputExposure => {
                    input.sql_exposure = projection_input_sql_exposure(
                        input,
                        default_exposure,
                        pagination_query_params,
                    );
                }
                ProjectionPaginationInputSyncMode::PreserveExistingExposure => {
                    if input.source_location == IrInputLocation::Query
                        && pagination_query_params.contains(input.wire_name.as_str())
                    {
                        input.sql_exposure = SqlInputExposure::Internal;
                    }
                }
            }
        }
    }
}

impl ParameterMetadataOverrides {
    fn validate_shape(&self) -> Result<()> {
        let mut names = BTreeSet::new();
        for strategy in &self.pagination {
            if strategy.name.trim().is_empty() {
                return Err(ManifestError::validation(
                    "parameter metadata pagination strategy name must not be empty",
                ));
            }
            if !names.insert(strategy.name.as_str()) {
                return Err(ManifestError::validation(format!(
                    "parameter metadata pagination strategy '{}' is repeated",
                    strategy.name
                )));
            }
            strategy.match_rules.validate(&strategy.name)?;
        }
        Ok(())
    }

    fn validate_for_surface(&self, ir: &SemanticIr) -> Result<()> {
        for strategy in &self.pagination {
            strategy.pagination.validated(
                &ir.source_name,
                &format!(
                    "{} parameter_metadata pagination '{}'",
                    ir.surface_id, strategy.name
                ),
            )?;
            for operation_id in &strategy.match_rules.operation_ids {
                validate_rest_operation_target(
                    &ir.operations,
                    operation_id,
                    "pagination strategy",
                    &strategy.name,
                )?;
            }
        }

        for (operation_id, operation_override) in &self.operation_overrides {
            validate_rest_operation_target(
                &ir.operations,
                operation_id,
                "operation override",
                operation_id,
            )?;
            operation_override.pagination.validated(
                &ir.source_name,
                &format!(
                    "{} parameter_metadata operation override '{}'",
                    ir.surface_id, operation_id
                ),
            )?;
        }

        Ok(())
    }
}

impl NamedPaginationOverride {
    fn matches_operation(&self, operation: &IrOperation) -> bool {
        let IrExecutionAttachment::Rest(rest) = &operation.execution else {
            return false;
        };

        if !self.match_rules.operation_ids.is_empty()
            && !self
                .match_rules
                .operation_ids
                .iter()
                .any(|operation_id| operation_matches_identifier(operation, operation_id))
        {
            return false;
        }

        if !self.match_rules.methods.is_empty()
            && !self.match_rules.methods.iter().any(|method| {
                parse_override_http_method(method).is_some_and(|method| method == rest.method)
            })
        {
            return false;
        }

        if !self.match_rules.path_patterns.is_empty()
            && !self
                .match_rules
                .path_patterns
                .iter()
                .any(|pattern| path_pattern_matches(pattern, &rest.path_template))
        {
            return false;
        }

        if !self.match_rules.required_query_params.is_empty() {
            let query_params = operation
                .inputs
                .iter()
                .filter(|input| input.location == IrInputLocation::Query)
                .map(|input| input.name.as_str())
                .collect::<BTreeSet<_>>();
            if !self
                .match_rules
                .required_query_params
                .iter()
                .all(|param| query_params.contains(param.as_str()))
            {
                return false;
            }
        }

        true
    }
}

impl PaginationStrategyMatch {
    fn validate(&self, strategy_name: &str) -> Result<()> {
        if self.operation_ids.is_empty()
            && self.methods.is_empty()
            && self.path_patterns.is_empty()
            && self.required_query_params.is_empty()
        {
            return Err(ManifestError::validation(format!(
                "parameter metadata pagination strategy '{strategy_name}' must define at least one match criterion"
            )));
        }

        validate_non_empty_values(
            &self.operation_ids,
            "operation_ids",
            "pagination strategy",
            strategy_name,
        )?;
        validate_non_empty_values(
            &self.path_patterns,
            "path_patterns",
            "pagination strategy",
            strategy_name,
        )?;
        validate_non_empty_values(
            &self.required_query_params,
            "required_query_params",
            "pagination strategy",
            strategy_name,
        )?;
        for method in &self.methods {
            if method.trim().is_empty() {
                return Err(ManifestError::validation(format!(
                    "parameter metadata pagination strategy '{strategy_name}' has an empty HTTP method"
                )));
            }
            if parse_override_http_method(method).is_none() {
                return Err(ManifestError::validation(format!(
                    "parameter metadata pagination strategy '{strategy_name}' has unsupported HTTP method '{method}'"
                )));
            }
        }

        Ok(())
    }
}

fn projection_input_sql_exposure(
    input: &ProjectionInput,
    default_exposure: SqlInputExposure,
    pagination_query_params: &HashSet<&str>,
) -> SqlInputExposure {
    let pagination_owned_query_input = input.source_location == IrInputLocation::Query
        && pagination_query_params.contains(input.wire_name.as_str());
    match input.source_location {
        IrInputLocation::Query if pagination_owned_query_input => SqlInputExposure::Internal,
        IrInputLocation::Path | IrInputLocation::Query | IrInputLocation::ToolArg => {
            default_exposure
        }
        IrInputLocation::Header | IrInputLocation::Cookie | IrInputLocation::Body => {
            SqlInputExposure::Internal
        }
    }
}

fn validate_rest_operation_target(
    operations: &[IrOperation],
    operation_id: &str,
    owner_kind: &str,
    owner_name: &str,
) -> Result<()> {
    let matches = operations
        .iter()
        .filter(|operation| operation_matches_identifier(operation, operation_id))
        .collect::<Vec<_>>();
    if matches.is_empty() {
        return Err(ManifestError::validation(format!(
            "parameter metadata {owner_kind} '{owner_name}' references unknown operation '{operation_id}'"
        )));
    }
    if matches.len() > 1 {
        return Err(ManifestError::validation(format!(
            "parameter metadata {owner_kind} '{owner_name}' references ambiguous operation '{operation_id}'"
        )));
    }
    let Some(operation) = matches.first() else {
        return Err(ManifestError::validation(format!(
            "parameter metadata {owner_kind} '{owner_name}' references unknown operation '{operation_id}'"
        )));
    };
    if !matches!(operation.execution, IrExecutionAttachment::Rest(_)) {
        return Err(ManifestError::validation(format!(
            "parameter metadata {owner_kind} '{owner_name}' references non-REST operation '{operation_id}'"
        )));
    }
    Ok(())
}

fn operation_matches_identifier(operation: &IrOperation, identifier: &str) -> bool {
    operation.id == identifier || operation.method_name == identifier
}

fn validate_non_empty_values(
    values: &[String],
    field: &str,
    owner_kind: &str,
    owner_name: &str,
) -> Result<()> {
    for value in values {
        if value.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "parameter metadata {owner_kind} '{owner_name}' has an empty {field} value"
            )));
        }
    }
    Ok(())
}

fn parse_override_http_method(method: &str) -> Option<HttpMethod> {
    match method.trim().to_ascii_lowercase().as_str() {
        "get" => Some(HttpMethod::Get),
        "head" => Some(HttpMethod::Head),
        "options" => Some(HttpMethod::Options),
        "post" => Some(HttpMethod::Post),
        "put" => Some(HttpMethod::Put),
        "patch" => Some(HttpMethod::Patch),
        "delete" => Some(HttpMethod::Delete),
        "trace" => Some(HttpMethod::Trace),
        _ => None,
    }
}

fn path_pattern_matches(pattern: &str, path: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if !pattern.contains('*') {
        return pattern == path;
    }

    let parts = pattern.split('*').collect::<Vec<_>>();
    let mut remainder = path;
    let mut middle_start = 0usize;

    if !pattern.starts_with('*') {
        let Some(first) = parts.first() else {
            return false;
        };
        let Some(stripped) = remainder.strip_prefix(first) else {
            return false;
        };
        remainder = stripped;
        middle_start = 1;
    }

    let last_index = parts.len().saturating_sub(1);
    for part in parts.iter().take(last_index).skip(middle_start) {
        if !part.is_empty() {
            let Some(found) = remainder.find(part) else {
                return false;
            };
            let Some(stripped) = remainder.get(found + part.len()..) else {
                return false;
            };
            remainder = stripped;
        }
    }

    if pattern.ends_with('*') {
        true
    } else {
        parts.last().is_some_and(|last| remainder.ends_with(last))
    }
}

#[cfg(test)]
mod tests {
    use crate::v4::ir::{
        HttpMethod, IrExecutionAttachment, IrInputLocation, IrOperation, IrOperationInput,
        IrOperationOutput, IrScalarType, OutputCardinality, RestExecutionAttachment,
        RestParameterBinding, RestResponseAttachment, SemanticIr,
    };
    use crate::v4::manifest::SurfaceType;
    use crate::v4::projections::{
        Projection, ProjectionCatalog, ProjectionInput, ProjectionKind, ProjectionVisibility,
        SqlInputExposure,
    };
    use crate::v4::{OPENAPI_IMPORTER_VERSION, V4_ARTIFACT_SCHEMA_VERSION};
    use crate::{ManifestDataType, PaginationMode, PaginationSpec, ResponseSpec};

    use super::{
        ProjectionPaginationInputSyncMode, apply_parameter_metadata_overrides,
        parse_parameter_metadata_overrides_yaml, path_pattern_matches,
        sync_projection_pagination_inputs,
    };

    #[test]
    fn surface_strategy_matches_required_query_params() {
        let mut ir = semantic_ir(vec![rest_operation(
            "widgets/list",
            "/widgets",
            vec![query_input("page_number"), query_input("per_page")],
            PaginationSpec::default(),
        )]);
        let overrides = parse_parameter_metadata_overrides_yaml(
            r"
pagination:
  - name: page_number
    match:
      required_query_params: [page_number, per_page]
    mode: page
    page_param: page_number
    page_size:
      default: 50
      max: 100
      query_param: per_page
",
        )
        .expect("parse overrides");

        apply_parameter_metadata_overrides(&mut ir, &overrides).expect("apply overrides");

        let pagination = rest_pagination(first_operation(&ir));
        assert_eq!(pagination.mode, PaginationMode::Page);
        assert_eq!(pagination.page_param.as_deref(), Some("page_number"));
        assert_eq!(
            pagination
                .page_size
                .as_ref()
                .and_then(|page_size| page_size.query_param.as_deref()),
            Some("per_page")
        );
    }

    #[test]
    fn explicit_non_param_match_supports_link_header_strategy() {
        let mut ir = semantic_ir(vec![rest_operation(
            "issues/list",
            "/repos/{owner}/{repo}/issues",
            vec![query_input("per_page")],
            PaginationSpec::default(),
        )]);
        let overrides = parse_parameter_metadata_overrides_yaml(
            r"
pagination:
  - name: github_link_header
    match:
      methods: [GET]
      path_patterns: ['/repos/*/issues']
    mode: link_header
    page_size:
      default: 100
      max: 100
      query_param: per_page
",
        )
        .expect("parse overrides");

        apply_parameter_metadata_overrides(&mut ir, &overrides).expect("apply overrides");

        let pagination = rest_pagination(first_operation(&ir));
        assert_eq!(pagination.mode, PaginationMode::LinkHeader);
    }

    #[test]
    fn first_matching_strategy_wins() {
        let mut ir = semantic_ir(vec![rest_operation(
            "widgets/list",
            "/widgets",
            vec![query_input("page"), query_input("per_page")],
            PaginationSpec::default(),
        )]);
        let overrides = parse_parameter_metadata_overrides_yaml(
            r"
pagination:
  - name: first
    match:
      methods: [GET]
    mode: page
    page_param: page
    page_size:
      default: 25
      max: 25
      query_param: per_page
  - name: second
    match:
      required_query_params: [page, per_page]
    mode: page
    page_param: page
    page_size:
      default: 100
      max: 100
      query_param: per_page
",
        )
        .expect("parse overrides");

        apply_parameter_metadata_overrides(&mut ir, &overrides).expect("apply overrides");

        let page_size = rest_pagination(first_operation(&ir))
            .page_size
            .as_ref()
            .expect("page size");
        assert_eq!(page_size.default, 25);
    }

    #[test]
    fn operation_override_wins_over_surface_strategy() {
        let mut ir = semantic_ir(vec![rest_operation(
            "widgets/list",
            "/widgets",
            vec![
                query_input("page"),
                query_input("page_number"),
                query_input("per_page"),
            ],
            PaginationSpec::default(),
        )]);
        let overrides = parse_parameter_metadata_overrides_yaml(
            r"
pagination:
  - name: generic
    match:
      methods: [GET]
    mode: page
    page_param: page
    page_size:
      default: 50
      max: 100
      query_param: per_page
operation_overrides:
  widgets/list:
    pagination:
      mode: page
      page_param: page_number
      page_size:
        default: 50
        max: 100
        query_param: per_page
",
        )
        .expect("parse overrides");

        apply_parameter_metadata_overrides(&mut ir, &overrides).expect("apply overrides");

        assert_eq!(
            rest_pagination(first_operation(&ir)).page_param.as_deref(),
            Some("page_number")
        );
    }

    #[test]
    fn sync_projection_pagination_inputs_keeps_projection_query_inputs_in_step() {
        let mut ir = semantic_ir(vec![rest_operation(
            "widgets_list",
            "/widgets",
            vec![
                query_input("page_number"),
                query_input("per_page"),
                query_input("state"),
            ],
            PaginationSpec::default(),
        )]);
        let overrides = parse_parameter_metadata_overrides_yaml(
            r"
pagination:
  - name: page_number
    match:
      operation_ids: [widgets_list]
    mode: page
    page_param: page_number
    page_size:
      default: 50
      max: 100
      query_param: per_page
",
        )
        .expect("parse overrides");
        let mut projections = projection_catalog(vec![
            projection_input("page_number", "page_number", SqlInputExposure::Filter),
            projection_input("per_page", "per_page", SqlInputExposure::Filter),
            projection_input("state", "state", SqlInputExposure::Filter),
            projection_input("debug", "debug", SqlInputExposure::Internal),
        ]);

        apply_parameter_metadata_overrides(&mut ir, &overrides).expect("apply overrides");
        sync_projection_pagination_inputs(
            std::slice::from_ref(&ir),
            &mut projections,
            ProjectionPaginationInputSyncMode::RecomputeRestInputExposure,
        );

        assert_eq!(
            projection_input_exposure(&projections, "page_number"),
            SqlInputExposure::Internal
        );
        assert_eq!(
            projection_input_exposure(&projections, "per_page"),
            SqlInputExposure::Internal
        );
        assert_eq!(
            projection_input_exposure(&projections, "state"),
            SqlInputExposure::Filter
        );
        assert_eq!(
            projection_input_exposure(&projections, "debug"),
            SqlInputExposure::Filter
        );

        let mut overridden_projections = projection_catalog(vec![
            projection_input("page_number", "page_number", SqlInputExposure::Filter),
            projection_input("debug", "debug", SqlInputExposure::Internal),
        ]);
        sync_projection_pagination_inputs(
            std::slice::from_ref(&ir),
            &mut overridden_projections,
            ProjectionPaginationInputSyncMode::PreserveExistingExposure,
        );

        assert_eq!(
            projection_input_exposure(&overridden_projections, "page_number"),
            SqlInputExposure::Internal
        );
        assert_eq!(
            projection_input_exposure(&overridden_projections, "debug"),
            SqlInputExposure::Internal
        );
    }

    #[test]
    fn non_matching_strategy_leaves_operation_unchanged() {
        let mut ir = semantic_ir(vec![rest_operation(
            "widgets/list",
            "/widgets",
            vec![query_input("limit")],
            PaginationSpec::default(),
        )]);
        let overrides = parse_parameter_metadata_overrides_yaml(
            r"
pagination:
  - name: page_number
    match:
      required_query_params: [page_number, per_page]
    mode: page
    page_param: page_number
    page_size:
      default: 50
      max: 100
      query_param: per_page
",
        )
        .expect("parse overrides");

        apply_parameter_metadata_overrides(&mut ir, &overrides).expect("apply overrides");

        assert_eq!(
            rest_pagination(first_operation(&ir)).mode,
            PaginationMode::None
        );
    }

    #[test]
    fn rejects_invalid_override_shapes() {
        let duplicate_name = parse_parameter_metadata_overrides_yaml(
            r"
pagination:
  - name: repeated
    match:
      methods: [GET]
  - name: repeated
    match:
      methods: [GET]
",
        )
        .expect_err("duplicate names should fail");
        assert!(duplicate_name.to_string().contains("repeated"));

        let empty_match = parse_parameter_metadata_overrides_yaml(
            r"
pagination:
  - name: no_match
    match: {}
",
        )
        .expect_err("empty match should fail");
        assert!(
            empty_match
                .to_string()
                .contains("at least one match criterion")
        );

        let unknown_field = parse_parameter_metadata_overrides_yaml(
            r"
pagination:
  - name: bad
    match:
      methods: [GET]
    not_a_pagination_field: true
",
        )
        .expect_err("unknown field should fail");
        assert!(unknown_field.to_string().contains("unknown field"));
    }

    #[test]
    fn rejects_unknown_operation_override_target() {
        let mut ir = semantic_ir(vec![rest_operation(
            "widgets/list",
            "/widgets",
            vec![query_input("page"), query_input("per_page")],
            PaginationSpec::default(),
        )]);
        let overrides = parse_parameter_metadata_overrides_yaml(
            r"
operation_overrides:
  widgets/missing:
    pagination:
      mode: page
      page_param: page
",
        )
        .expect("parse overrides");

        let error = apply_parameter_metadata_overrides(&mut ir, &overrides)
            .expect_err("missing operation should fail");

        assert!(error.to_string().contains("unknown operation"));
    }

    #[test]
    fn path_patterns_use_simple_star_globs() {
        assert!(path_pattern_matches(
            "/repos/*/issues",
            "/repos/{owner}/{repo}/issues"
        ));
        assert!(path_pattern_matches("/repos/*", "/repos/{owner}/{repo}"));
        assert!(path_pattern_matches("*issues", "/repos/{owner}/issues"));
        assert!(!path_pattern_matches("/users/*", "/repos/{owner}/issues"));
    }

    fn semantic_ir(operations: Vec<IrOperation>) -> SemanticIr {
        SemanticIr {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: "demo".to_string(),
            surface_id: "rest".to_string(),
            surface_type: SurfaceType::OpenApi,
            importer_version: OPENAPI_IMPORTER_VERSION.to_string(),
            operations,
            types: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    fn rest_operation(
        id: &str,
        path_template: &str,
        inputs: Vec<IrOperationInput>,
        pagination: PaginationSpec,
    ) -> IrOperation {
        let parameters = inputs
            .iter()
            .map(|input| RestParameterBinding {
                input_name: input.name.clone(),
                location: input.location,
                wire_name: input.name.clone(),
                required: input.required,
                data_type: input.data_type,
            })
            .collect();
        IrOperation {
            id: id.to_string(),
            method_name: "GET".to_string(),
            description: String::new(),
            deprecated: false,
            read_only: true,
            inputs,
            output: IrOperationOutput {
                cardinality: OutputCardinality::List,
                type_ref: "item".to_string(),
                row_path: Vec::new(),
            },
            entity: None,
            naming: None,
            execution: IrExecutionAttachment::Rest(Box::new(RestExecutionAttachment {
                method: HttpMethod::Get,
                path_template: path_template.to_string(),
                parameters,
                request_body: None,
                response: RestResponseAttachment {
                    status_code: 200,
                    media_type: "application/json".to_string(),
                    response: ResponseSpec::default(),
                },
                pagination,
            })),
            diagnostics: Vec::new(),
        }
    }

    fn query_input(name: &str) -> IrOperationInput {
        IrOperationInput {
            name: name.to_string(),
            location: IrInputLocation::Query,
            required: false,
            data_type: IrScalarType::String,
            default_value: None,
            description: String::new(),
        }
    }

    fn rest_pagination(operation: &IrOperation) -> &PaginationSpec {
        let IrExecutionAttachment::Rest(rest) = &operation.execution else {
            panic!("expected REST operation");
        };
        &rest.pagination
    }

    fn first_operation(ir: &SemanticIr) -> &IrOperation {
        ir.operations.first().expect("operation")
    }

    fn projection_catalog(inputs: Vec<ProjectionInput>) -> ProjectionCatalog {
        ProjectionCatalog {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: "demo".to_string(),
            generator_version: "test".to_string(),
            projections: vec![Projection {
                name: "widgets".to_string(),
                namespace: "demo".to_string(),
                kind: ProjectionKind::Table,
                description: String::new(),
                guide: String::new(),
                surface_id: "rest".to_string(),
                operation_id: "widgets_list".to_string(),
                visibility: ProjectionVisibility::Published,
                inputs,
                columns: Vec::new(),
                search_limits: None,
                detail_hints: Vec::new(),
                diagnostics: Vec::new(),
            }],
            diagnostics: Vec::new(),
        }
    }

    fn projection_input(
        name: &str,
        wire_name: &str,
        sql_exposure: SqlInputExposure,
    ) -> ProjectionInput {
        ProjectionInput {
            name: name.to_string(),
            sql_exposure,
            source_location: IrInputLocation::Query,
            wire_name: wire_name.to_string(),
            required: false,
            data_type: ManifestDataType::Utf8,
            default_value: None,
            description: String::new(),
        }
    }

    fn projection_input_exposure(
        catalog: &ProjectionCatalog,
        input_name: &str,
    ) -> SqlInputExposure {
        catalog
            .projections
            .first()
            .expect("projection")
            .inputs
            .iter()
            .find(|input| input.name == input_name)
            .map(|input| input.sql_exposure)
            .expect("projection input")
    }
}
