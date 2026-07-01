use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::v4::ir::{IrExecutionAttachment, IrInputLocation, IrOperation, SemanticIr};
use crate::v4::manifest::SurfaceType;
use crate::{ManifestError, PaginationMode, PaginationSpec, Result};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParameterMetadata {
    #[serde(default)]
    pub pagination: Vec<NamedPaginationSpec>,
    #[serde(default)]
    pub operation_overrides: BTreeMap<String, OperationParameterMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedPaginationSpec {
    pub name: String,
    #[serde(default, rename = "match")]
    pub match_rule: PaginationMatch,
    #[serde(flatten)]
    pub pagination: PaginationSpec,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationParameterMetadata {
    #[serde(default)]
    pub pagination: Option<PaginationSpec>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PaginationMatch {
    #[serde(default)]
    pub query_params: Vec<String>,
    #[serde(default)]
    pub body_paths: Vec<Vec<String>>,
    #[serde(default)]
    pub response_headers: Vec<String>,
}

impl ParameterMetadata {
    pub fn validate_for_surfaces<'a>(
        &self,
        surfaces: impl IntoIterator<Item = &'a SemanticIr>,
    ) -> Result<()> {
        let mut names = BTreeSet::new();
        for strategy in &self.pagination {
            if strategy.name.trim().is_empty() {
                return Err(ManifestError::validation(
                    "parameter metadata pagination strategy name must not be empty",
                ));
            }
            if !names.insert(strategy.name.as_str()) {
                return Err(ManifestError::validation(format!(
                    "parameter metadata pagination strategy '{}' is duplicated",
                    strategy.name
                )));
            }
            strategy
                .pagination
                .validated("parameter_metadata.pagination", &strategy.name)?;
            if strategy.pagination.mode == PaginationMode::LinkHeader
                && strategy.match_rule.response_headers.is_empty()
            {
                return Err(ManifestError::validation(format!(
                    "parameter metadata pagination strategy '{}' uses mode=link_header and must define match.response_headers",
                    strategy.name
                )));
            }
            if strategy.effective_match().is_empty() {
                return Err(ManifestError::validation(format!(
                    "parameter metadata pagination strategy '{}' must not match every operation",
                    strategy.name
                )));
            }
        }

        let operation_ids = openapi_operation_ids(surfaces);
        for (operation_id, override_metadata) in &self.operation_overrides {
            match operation_ids.get(operation_id).copied().unwrap_or_default() {
                0 => {
                    return Err(ManifestError::validation(format!(
                        "parameter metadata operation override '{operation_id}' does not match any OpenAPI operation"
                    )));
                }
                1 => {}
                count => {
                    return Err(ManifestError::validation(format!(
                        "parameter metadata operation override '{operation_id}' is ambiguous across {count} OpenAPI operations"
                    )));
                }
            }
            if let Some(pagination) = &override_metadata.pagination {
                pagination.validated("parameter_metadata.operation_overrides", operation_id)?;
            }
        }

        Ok(())
    }

    pub fn effective_pagination_for_operation(&self, operation: &IrOperation) -> PaginationSpec {
        if let Some(pagination) = self
            .operation_overrides
            .get(&operation.id)
            .and_then(|override_metadata| override_metadata.pagination.clone())
        {
            return pagination;
        }

        self.pagination
            .iter()
            .find(|strategy| strategy.matches_operation(operation))
            .map_or_else(
                || inferred_pagination(operation),
                |strategy| strategy.pagination.clone(),
            )
    }
}

impl NamedPaginationSpec {
    fn effective_match(&self) -> PaginationMatch {
        if !self.match_rule.is_empty() {
            return self.match_rule.clone();
        }

        let mut match_rule = PaginationMatch::default();
        if let Some(param) = &self.pagination.page_param {
            match_rule.query_params.push(param.clone());
        }
        if let Some(param) = &self.pagination.offset_param {
            match_rule.query_params.push(param.clone());
        }
        if let Some(param) = &self.pagination.cursor_param {
            match_rule.query_params.push(param.clone());
        }
        if !self.pagination.cursor_body_path.is_empty() {
            match_rule
                .body_paths
                .push(self.pagination.cursor_body_path.clone());
        }
        if let Some(page_size) = &self.pagination.page_size {
            if let Some(param) = &page_size.query_param {
                match_rule.query_params.push(param.clone());
            }
            if !page_size.body_path.is_empty() {
                match_rule.body_paths.push(page_size.body_path.clone());
            }
        }
        match_rule
    }

    fn matches_operation(&self, operation: &IrOperation) -> bool {
        self.effective_match().matches_operation(operation)
    }
}

impl PaginationMatch {
    fn is_empty(&self) -> bool {
        self.query_params.is_empty()
            && self.body_paths.is_empty()
            && self.response_headers.is_empty()
    }

    fn matches_operation(&self, operation: &IrOperation) -> bool {
        if self.is_empty() {
            return false;
        }

        let query_params = operation
            .inputs
            .iter()
            .filter(|input| input.location == IrInputLocation::Query)
            .map(|input| input.name.as_str())
            .collect::<BTreeSet<_>>();
        let body_paths = operation
            .inputs
            .iter()
            .filter(|input| input.location == IrInputLocation::Body)
            .map(|input| vec![input.name.as_str()])
            .collect::<BTreeSet<_>>();
        let response_headers = match &operation.execution {
            IrExecutionAttachment::Rest(rest) => rest
                .response
                .headers
                .iter()
                .map(|header| header.to_ascii_lowercase())
                .collect::<BTreeSet<_>>(),
            IrExecutionAttachment::Mcp(_) => BTreeSet::new(),
        };

        self.query_params
            .iter()
            .all(|param| query_params.contains(param.as_str()))
            && self.body_paths.iter().all(|path| {
                let candidate = path.iter().map(String::as_str).collect::<Vec<_>>();
                body_paths.contains(&candidate)
            })
            && self
                .response_headers
                .iter()
                .all(|header| response_headers.contains(header.to_ascii_lowercase().as_str()))
    }
}

fn openapi_operation_ids<'a>(
    surfaces: impl IntoIterator<Item = &'a SemanticIr>,
) -> BTreeMap<String, usize> {
    let mut operation_ids = BTreeMap::new();
    for surface in surfaces
        .into_iter()
        .filter(|surface| surface.surface_type == SurfaceType::OpenApi)
    {
        for operation in &surface.operations {
            *operation_ids.entry(operation.id.clone()).or_default() += 1;
        }
    }
    operation_ids
}

fn inferred_pagination(operation: &IrOperation) -> PaginationSpec {
    match &operation.execution {
        IrExecutionAttachment::Rest(rest) => rest.pagination.clone(),
        IrExecutionAttachment::Mcp(_) => PaginationSpec::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::ParameterMetadata;
    use crate::v4::ir::{
        HttpMethod, IrExecutionAttachment, IrInputLocation, IrOperation, IrOperationInput,
        IrOperationOutput, IrScalarType, OutputCardinality, RestExecutionAttachment,
        RestResponseAttachment, SemanticIr,
    };
    use crate::v4::manifest::SurfaceType;
    use crate::{PageSizeSpec, PaginationMode, PaginationSpec, ResponseSpec};

    #[test]
    fn per_operation_pagination_override_wins_over_global_strategy() {
        let metadata: ParameterMetadata = serde_yaml::from_str(
            r"
pagination:
  - name: global_page
    mode: page
    page_param: page
    page_size:
      default: 50
      max: 100
      query_param: per_page
operation_overrides:
  list_items:
    pagination:
      mode: offset
      offset_param: offset
      offset_step: 50
",
        )
        .expect("metadata");
        let operation = operation_with_query_params("list_items", &["page", "per_page"]);
        metadata
            .validate_for_surfaces(&[surface_with_operation(operation.clone())])
            .expect("valid metadata");

        let pagination = metadata.effective_pagination_for_operation(&operation);

        assert_eq!(pagination.mode, PaginationMode::Offset);
        assert_eq!(pagination.offset_param.as_deref(), Some("offset"));
    }

    #[test]
    fn global_strategies_match_in_file_order() {
        let metadata: ParameterMetadata = serde_yaml::from_str(
            r"
pagination:
  - name: first
    mode: page
    page_param: page
    page_size:
      default: 25
      max: 100
      query_param: per_page
  - name: second
    mode: page
    page_param: page
    page_size:
      default: 50
      max: 100
      query_param: per_page
",
        )
        .expect("metadata");
        let operation = operation_with_query_params("list_items", &["page", "per_page"]);
        metadata
            .validate_for_surfaces(&[surface_with_operation(operation.clone())])
            .expect("valid metadata");

        let pagination = metadata.effective_pagination_for_operation(&operation);

        assert_eq!(
            pagination
                .page_size
                .as_ref()
                .map(|page_size| page_size.default),
            Some(25)
        );
    }

    #[test]
    fn link_header_strategy_matches_response_headers() {
        let metadata: ParameterMetadata = serde_yaml::from_str(
            r"
pagination:
  - name: link_header
    mode: link_header
    match:
      response_headers: [Link]
",
        )
        .expect("metadata");
        let mut operation = operation_with_query_params("list_items", &[]);
        if let IrExecutionAttachment::Rest(rest) = &mut operation.execution {
            rest.response.headers.push("link".to_string());
        }
        metadata
            .validate_for_surfaces(&[surface_with_operation(operation.clone())])
            .expect("valid metadata");

        let pagination = metadata.effective_pagination_for_operation(&operation);

        assert_eq!(pagination.mode, PaginationMode::LinkHeader);
    }

    #[test]
    fn body_path_strategy_matches_body_inputs() {
        let metadata: ParameterMetadata = serde_yaml::from_str(
            r"
pagination:
  - name: body_cursor
    mode: cursor_body
    cursor_body_path: [page_token]
    response_cursor_path: [next_page_token]
",
        )
        .expect("metadata");
        let mut operation = operation_with_query_params("list_items", &[]);
        operation.inputs.push(IrOperationInput {
            name: "page_token".to_string(),
            location: IrInputLocation::Body,
            required: false,
            data_type: IrScalarType::String,
            default_value: None,
            description: String::new(),
        });
        metadata
            .validate_for_surfaces(&[surface_with_operation(operation.clone())])
            .expect("valid metadata");

        let pagination = metadata.effective_pagination_for_operation(&operation);

        assert_eq!(pagination.mode, PaginationMode::CursorBody);
    }

    #[test]
    fn unmatched_strategy_falls_back_to_inferred_pagination() {
        let metadata: ParameterMetadata = serde_yaml::from_str(
            r"
pagination:
  - name: custom_page
    mode: page
    page_param: page_number
",
        )
        .expect("metadata");
        let operation = operation_with_inferred_pagination();
        metadata
            .validate_for_surfaces(&[surface_with_operation(operation.clone())])
            .expect("valid metadata");

        let pagination = metadata.effective_pagination_for_operation(&operation);

        assert_eq!(pagination.mode, PaginationMode::Page);
        assert_eq!(pagination.page_param.as_deref(), Some("page"));
    }

    #[test]
    fn rejects_duplicate_strategy_names() {
        let metadata: ParameterMetadata = serde_yaml::from_str(
            r"
pagination:
  - name: page
    mode: page
    page_param: page
  - name: page
    mode: page
    page_param: page_number
",
        )
        .expect("metadata");

        let error = metadata
            .validate_for_surfaces(&[surface_with_operation(operation_with_query_params(
                "list_items",
                &[],
            ))])
            .expect_err("duplicate names should be rejected");

        assert!(error.to_string().contains("duplicated"));
    }

    #[test]
    fn rejects_global_match_all_strategy() {
        let metadata: ParameterMetadata = serde_yaml::from_str(
            r"
pagination:
  - name: none
    mode: none
",
        )
        .expect("metadata");

        let error = metadata
            .validate_for_surfaces(&[surface_with_operation(operation_with_query_params(
                "list_items",
                &[],
            ))])
            .expect_err("match-all strategy should be rejected");

        assert!(error.to_string().contains("must not match every operation"));
    }

    #[test]
    fn rejects_unknown_operation_override() {
        let metadata: ParameterMetadata = serde_yaml::from_str(
            r"
operation_overrides:
  missing:
    pagination:
      mode: none
",
        )
        .expect("metadata");

        let error = metadata
            .validate_for_surfaces(&[surface_with_operation(operation_with_query_params(
                "list_items",
                &[],
            ))])
            .expect_err("unknown operation should be rejected");

        assert!(
            error
                .to_string()
                .contains("does not match any OpenAPI operation")
        );
    }

    #[test]
    fn rejects_ambiguous_operation_override() {
        let metadata: ParameterMetadata = serde_yaml::from_str(
            r"
operation_overrides:
  list_items:
    pagination:
      mode: none
",
        )
        .expect("metadata");

        let error = metadata
            .validate_for_surfaces(&[
                surface_with_operation(operation_with_query_params("list_items", &[])),
                surface_with_operation(operation_with_query_params("list_items", &[])),
            ])
            .expect_err("ambiguous operation should be rejected");

        assert!(error.to_string().contains("is ambiguous"));
    }

    #[test]
    fn rejects_invalid_pagination_spec() {
        let metadata: ParameterMetadata = serde_yaml::from_str(
            r"
pagination:
  - name: page
    mode: page
",
        )
        .expect("metadata");

        let error = metadata
            .validate_for_surfaces(&[surface_with_operation(operation_with_query_params(
                "list_items",
                &[],
            ))])
            .expect_err("invalid pagination should be rejected");

        assert!(error.to_string().contains("requires page_param"));
    }

    fn operation_with_inferred_pagination() -> IrOperation {
        let mut operation = operation_with_query_params("list_items", &["page", "per_page"]);
        if let IrExecutionAttachment::Rest(rest) = &mut operation.execution {
            rest.pagination = PaginationSpec {
                mode: PaginationMode::Page,
                page_param: Some("page".to_string()),
                page_size: Some(PageSizeSpec {
                    default: 30,
                    max: 100,
                    query_param: Some("per_page".to_string()),
                    body_path: Vec::new(),
                }),
                ..PaginationSpec::default()
            };
        }
        operation
    }

    fn operation_with_query_params(operation_id: &str, query_params: &[&str]) -> IrOperation {
        IrOperation {
            id: operation_id.to_string(),
            method_name: "GET".to_string(),
            description: String::new(),
            deprecated: false,
            read_only: true,
            inputs: query_params
                .iter()
                .map(|name| IrOperationInput {
                    name: (*name).to_string(),
                    location: IrInputLocation::Query,
                    required: false,
                    data_type: IrScalarType::String,
                    default_value: None,
                    description: String::new(),
                })
                .collect(),
            output: IrOperationOutput {
                cardinality: OutputCardinality::List,
                type_ref: "item".to_string(),
                row_path: Vec::new(),
            },
            entity: None,
            execution: IrExecutionAttachment::Rest(Box::new(RestExecutionAttachment {
                method: HttpMethod::Get,
                path_template: "/items".to_string(),
                parameters: Vec::new(),
                request_body: None,
                response: RestResponseAttachment {
                    status_code: 200,
                    media_type: "application/json".to_string(),
                    response: ResponseSpec::default(),
                    headers: Vec::new(),
                },
                pagination: PaginationSpec::default(),
            })),
            diagnostics: Vec::new(),
        }
    }

    fn surface_with_operation(operation: IrOperation) -> SemanticIr {
        SemanticIr {
            artifact_schema_version: crate::v4::V4_ARTIFACT_SCHEMA_VERSION,
            source_name: "demo".to_string(),
            surface_id: "rest".to_string(),
            surface_type: SurfaceType::OpenApi,
            importer_version: crate::v4::OPENAPI_IMPORTER_VERSION.to_string(),
            operations: vec![operation],
            types: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
}
