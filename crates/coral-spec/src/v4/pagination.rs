use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::Value;

use crate::backends::mcp::{McpOffsetPaginationSpec, McpPaginationSpec};
use crate::v4::ir::HttpMethod;
use crate::{ManifestError, PageSizeSpec, PaginationMode, PaginationSpec, Result};

#[derive(Debug, Clone, Default)]
pub struct V4SurfacePagination {
    pub profiles: Vec<V4PaginationProfile>,
    pub operations: Vec<V4OperationPaginationOverlay>,
}

#[derive(Debug, Clone)]
pub struct V4PaginationProfile {
    pub name: String,
    pub matcher: V4PaginationMatcher,
    pub outcome: V4PaginationOutcome,
}

#[derive(Debug, Clone)]
pub struct V4OperationPaginationOverlay {
    pub target: V4OperationTarget,
    pub outcome: V4PaginationOutcome,
}

#[derive(Debug, Clone)]
pub enum V4PaginationMatcher {
    OpenApi(OpenApiPaginationMatcher),
    Mcp(McpPaginationMatcher),
}

#[derive(Debug, Clone)]
pub enum V4OperationTarget {
    OpenApi(OpenApiOperationTarget),
    Mcp(McpOperationTarget),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenApiOperationTarget {
    OperationId(String),
    MethodPath { method: HttpMethod, path: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOperationTarget {
    pub tool: String,
}

#[derive(Debug, Clone)]
pub enum V4PaginationOutcome {
    Http(Box<PaginationSpec>),
    McpCursor(McpPaginationSpec),
    McpOffset(McpOffsetPaginationSpec),
    Unsupported { reason: String },
}

#[derive(Debug, Clone, Copy, Deserialize, serde::Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum PaginationProvenance {
    Authored,
    ProfileGenerated,
    Unsupported,
    #[default]
    None,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenApiPaginationMatcher {
    #[serde(default)]
    pub methods: Vec<HttpMethod>,
    #[serde(default)]
    pub paths: Vec<String>,
    #[serde(default)]
    pub query_params: Vec<String>,
    #[serde(default)]
    pub response_cursor_path: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpPaginationMatcher {
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub tool_args: Vec<String>,
    #[serde(default)]
    pub response_cursor_path: Vec<String>,
    #[serde(default)]
    pub offset_args: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawSurfacePagination {
    #[serde(default)]
    profiles: Vec<RawPaginationProfile>,
    #[serde(default)]
    operations: Vec<RawOperationPaginationOverlay>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPaginationProfile {
    name: String,
    #[serde(rename = "match")]
    matcher: Value,
    #[serde(default)]
    pagination: Option<Value>,
    #[serde(default)]
    unsupported: Option<RawUnsupportedPagination>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOperationPaginationOverlay {
    target: Value,
    #[serde(default)]
    pagination: Option<Value>,
    #[serde(default)]
    unsupported: Option<RawUnsupportedPagination>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawUnsupportedPagination {
    reason: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHttpPaginationOverlay {
    #[serde(default)]
    mode: PaginationMode,
    #[serde(default)]
    page_size: Option<PageSizeSpec>,
    #[serde(default)]
    cursor_param: Option<String>,
    #[serde(default)]
    cursor_body_path: Vec<String>,
    #[serde(default)]
    response_cursor_path: Vec<String>,
    #[serde(default)]
    cursor_from_last_row_path: Vec<String>,
    #[serde(default)]
    has_more_path: Vec<String>,
    #[serde(default)]
    response_next_url_path: Vec<String>,
    #[serde(default)]
    suppressed_query_params: Vec<String>,
    #[serde(default)]
    page_param: Option<String>,
    #[serde(default)]
    page_start: i64,
    #[serde(default = "default_v4_page_step")]
    page_step: i64,
    #[serde(default)]
    offset_param: Option<String>,
    #[serde(default)]
    offset_start: i64,
    #[serde(default)]
    offset_step: Option<i64>,
    #[serde(default)]
    link_header_require_results: bool,
    #[serde(default)]
    max_pages: Option<usize>,
}

impl From<RawHttpPaginationOverlay> for PaginationSpec {
    fn from(raw: RawHttpPaginationOverlay) -> Self {
        Self {
            mode: raw.mode,
            page_size: raw.page_size,
            cursor_param: raw.cursor_param,
            cursor_body_path: raw.cursor_body_path,
            response_cursor_path: raw.response_cursor_path,
            cursor_from_last_row_path: raw.cursor_from_last_row_path,
            has_more_path: raw.has_more_path,
            response_next_url_path: raw.response_next_url_path,
            suppressed_query_params: raw.suppressed_query_params,
            page_param: raw.page_param,
            page_start: raw.page_start,
            page_step: raw.page_step,
            offset_param: raw.offset_param,
            offset_start: raw.offset_start,
            offset_step: raw.offset_step,
            link_header_require_results: raw.link_header_require_results,
            max_pages: raw.max_pages,
        }
    }
}

fn default_v4_page_step() -> i64 {
    1
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOpenApiOperationTarget {
    #[serde(default)]
    operation_id: Option<String>,
    #[serde(default)]
    method: Option<HttpMethod>,
    #[serde(default)]
    path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMcpOperationTarget {
    tool: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum RawMcpPaginationOverlay {
    Cursor {
        cursor_arg: String,
        response_cursor_path: Vec<String>,
        #[serde(default)]
        max_pages: Option<usize>,
    },
    Offset {
        limit_arg: String,
        default_limit: usize,
        max_limit: usize,
        offset_arg: String,
        #[serde(default)]
        offset_start: usize,
        #[serde(default)]
        max_pages: Option<usize>,
    },
}

impl RawSurfacePagination {
    pub(crate) fn parse_openapi(
        self,
        source_name: &str,
        surface_id: &str,
    ) -> Result<V4SurfacePagination> {
        self.parse(source_name, surface_id, SurfacePaginationKind::OpenApi)
    }

    pub(crate) fn parse_mcp(
        self,
        source_name: &str,
        surface_id: &str,
    ) -> Result<V4SurfacePagination> {
        self.parse(source_name, surface_id, SurfacePaginationKind::Mcp)
    }

    fn parse(
        self,
        source_name: &str,
        surface_id: &str,
        kind: SurfacePaginationKind,
    ) -> Result<V4SurfacePagination> {
        let mut profile_names = BTreeSet::new();
        let profiles = self
            .profiles
            .into_iter()
            .map(|profile| {
                if profile.name.trim().is_empty() {
                    return Err(ManifestError::validation(format!(
                        "source '{source_name}' surface '{surface_id}' pagination profile name must not be empty"
                    )));
                }
                if !profile_names.insert(profile.name.clone()) {
                    return Err(ManifestError::validation(format!(
                        "source '{source_name}' surface '{surface_id}' repeats pagination profile '{}'",
                        profile.name
                    )));
                }
                let matcher = match kind {
                    SurfacePaginationKind::OpenApi => {
                        let matcher =
                            parse_json_value(profile.matcher, "OpenAPI pagination profile match")?;
                        validate_openapi_matcher(
                            &matcher,
                            source_name,
                            surface_id,
                            &profile.name,
                        )?;
                        V4PaginationMatcher::OpenApi(matcher)
                    }
                    SurfacePaginationKind::Mcp => {
                        let matcher =
                            parse_json_value(profile.matcher, "MCP pagination profile match")?;
                        validate_mcp_matcher(&matcher, source_name, surface_id, &profile.name)?;
                        V4PaginationMatcher::Mcp(matcher)
                    }
                };
                let outcome = parse_outcome(
                    profile.pagination,
                    profile.unsupported,
                    kind,
                    source_name,
                    surface_id,
                    &format!("pagination profile '{}'", profile.name),
                )?;
                Ok(V4PaginationProfile {
                    name: profile.name,
                    matcher,
                    outcome,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let operations = self
            .operations
            .into_iter()
            .enumerate()
            .map(|(index, operation)| {
                let target = match kind {
                    SurfacePaginationKind::OpenApi => V4OperationTarget::OpenApi(
                        parse_openapi_operation_target(
                            operation.target,
                            source_name,
                            surface_id,
                            index,
                        )?,
                    ),
                    SurfacePaginationKind::Mcp => {
                        let target: RawMcpOperationTarget =
                            parse_json_value(operation.target, "MCP pagination operation target")?;
                        if target.tool.trim().is_empty() {
                            return Err(ManifestError::validation(format!(
                                "source '{source_name}' surface '{surface_id}' pagination operation[{index}] target.tool must not be empty"
                            )));
                        }
                        V4OperationTarget::Mcp(McpOperationTarget { tool: target.tool })
                    }
                };
                let outcome = parse_outcome(
                    operation.pagination,
                    operation.unsupported,
                    kind,
                    source_name,
                    surface_id,
                    &format!("pagination operation[{index}]"),
                )?;
                Ok(V4OperationPaginationOverlay { target, outcome })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(V4SurfacePagination {
            profiles,
            operations,
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum SurfacePaginationKind {
    OpenApi,
    Mcp,
}

fn validate_openapi_matcher(
    matcher: &OpenApiPaginationMatcher,
    source_name: &str,
    surface_id: &str,
    profile_name: &str,
) -> Result<()> {
    let prefix = format!(
        "source '{source_name}' surface '{surface_id}' pagination profile '{profile_name}' match"
    );
    if matcher.methods.is_empty()
        && matcher.paths.is_empty()
        && matcher.query_params.is_empty()
        && matcher.response_cursor_path.is_empty()
    {
        return Err(ManifestError::validation(format!(
            "{prefix} must declare at least one criterion"
        )));
    }
    validate_non_empty_strings(&matcher.paths, &prefix, "paths")?;
    validate_non_empty_strings(&matcher.query_params, &prefix, "query_params")?;
    validate_non_empty_strings(
        &matcher.response_cursor_path,
        &prefix,
        "response_cursor_path",
    )
}

fn validate_mcp_matcher(
    matcher: &McpPaginationMatcher,
    source_name: &str,
    surface_id: &str,
    profile_name: &str,
) -> Result<()> {
    let prefix = format!(
        "source '{source_name}' surface '{surface_id}' pagination profile '{profile_name}' match"
    );
    if matcher.tools.is_empty()
        && matcher.tool_args.is_empty()
        && matcher.response_cursor_path.is_empty()
        && !matcher.offset_args
    {
        return Err(ManifestError::validation(format!(
            "{prefix} must declare at least one criterion"
        )));
    }
    validate_non_empty_strings(&matcher.tools, &prefix, "tools")?;
    validate_non_empty_strings(&matcher.tool_args, &prefix, "tool_args")?;
    validate_non_empty_strings(
        &matcher.response_cursor_path,
        &prefix,
        "response_cursor_path",
    )
}

fn validate_non_empty_strings(values: &[String], prefix: &str, field: &str) -> Result<()> {
    if values.iter().any(|value| value.trim().is_empty()) {
        return Err(ManifestError::validation(format!(
            "{prefix}.{field} must not contain empty values"
        )));
    }
    Ok(())
}

fn parse_outcome(
    pagination: Option<Value>,
    unsupported: Option<RawUnsupportedPagination>,
    kind: SurfacePaginationKind,
    source_name: &str,
    surface_id: &str,
    context: &str,
) -> Result<V4PaginationOutcome> {
    match (pagination, unsupported) {
        (Some(_), Some(_)) => Err(ManifestError::validation(format!(
            "source '{source_name}' surface '{surface_id}' {context} must declare exactly one of pagination or unsupported"
        ))),
        (None, None) => Err(ManifestError::validation(format!(
            "source '{source_name}' surface '{surface_id}' {context} must declare pagination or unsupported"
        ))),
        (None, Some(unsupported)) => {
            if unsupported.reason.trim().is_empty() {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' surface '{surface_id}' {context} unsupported.reason must not be empty"
                )));
            }
            Ok(V4PaginationOutcome::Unsupported {
                reason: unsupported.reason,
            })
        }
        (Some(value), None) => match kind {
            SurfacePaginationKind::OpenApi => {
                let pagination: RawHttpPaginationOverlay =
                    parse_json_value(value, "OpenAPI pagination overlay")?;
                let pagination = PaginationSpec::from(pagination);
                validate_http_pagination(&pagination, source_name, surface_id, context)?;
                Ok(V4PaginationOutcome::Http(Box::new(pagination)))
            }
            SurfacePaginationKind::Mcp => {
                match parse_json_value(value, "MCP pagination overlay")? {
                    RawMcpPaginationOverlay::Cursor {
                        cursor_arg,
                        response_cursor_path,
                        max_pages,
                    } => {
                        let pagination = McpPaginationSpec {
                            cursor_arg,
                            response_cursor_path,
                            max_pages,
                        };
                        validate_mcp_cursor_pagination(
                            &pagination,
                            source_name,
                            surface_id,
                            context,
                        )?;
                        Ok(V4PaginationOutcome::McpCursor(pagination))
                    }
                    RawMcpPaginationOverlay::Offset {
                        limit_arg,
                        default_limit,
                        max_limit,
                        offset_arg,
                        offset_start,
                        max_pages,
                    } => {
                        let pagination = McpOffsetPaginationSpec {
                            limit_arg,
                            default_limit,
                            max_limit,
                            offset_arg,
                            offset_start,
                            max_pages,
                        };
                        validate_mcp_offset_pagination(
                            &pagination,
                            source_name,
                            surface_id,
                            context,
                        )?;
                        Ok(V4PaginationOutcome::McpOffset(pagination))
                    }
                }
            }
        },
    }
}

fn validate_http_pagination(
    pagination: &PaginationSpec,
    source_name: &str,
    surface_id: &str,
    context: &str,
) -> Result<()> {
    let table = format!("surface '{surface_id}' {context}");
    pagination.validate(source_name, &table)
}

fn validate_mcp_cursor_pagination(
    pagination: &McpPaginationSpec,
    source_name: &str,
    surface_id: &str,
    context: &str,
) -> Result<()> {
    let prefix = format!("source '{source_name}' surface '{surface_id}' {context}");
    if pagination.cursor_arg.trim().is_empty() {
        return Err(ManifestError::validation(format!(
            "{prefix} pagination.cursor_arg must not be empty"
        )));
    }
    if pagination.response_cursor_path.is_empty()
        || pagination
            .response_cursor_path
            .iter()
            .any(|segment| segment.trim().is_empty())
    {
        return Err(ManifestError::validation(format!(
            "{prefix} pagination.response_cursor_path must not be empty"
        )));
    }
    if matches!(pagination.max_pages, Some(0)) {
        return Err(ManifestError::validation(format!(
            "{prefix} pagination.max_pages must be greater than 0"
        )));
    }
    Ok(())
}

fn validate_mcp_offset_pagination(
    pagination: &McpOffsetPaginationSpec,
    source_name: &str,
    surface_id: &str,
    context: &str,
) -> Result<()> {
    let prefix = format!("source '{source_name}' surface '{surface_id}' {context}");
    let limit_arg = pagination.limit_arg.trim();
    let offset_arg = pagination.offset_arg.trim();
    if limit_arg.is_empty() {
        return Err(ManifestError::validation(format!(
            "{prefix} pagination.limit_arg must not be empty"
        )));
    }
    if offset_arg.is_empty() {
        return Err(ManifestError::validation(format!(
            "{prefix} pagination.offset_arg must not be empty"
        )));
    }
    if limit_arg == offset_arg {
        return Err(ManifestError::validation(format!(
            "{prefix} pagination.limit_arg and pagination.offset_arg must differ"
        )));
    }
    if pagination.default_limit == 0 {
        return Err(ManifestError::validation(format!(
            "{prefix} pagination.default_limit must be greater than 0"
        )));
    }
    if pagination.max_limit == 0 {
        return Err(ManifestError::validation(format!(
            "{prefix} pagination.max_limit must be greater than 0"
        )));
    }
    if pagination.default_limit > pagination.max_limit {
        return Err(ManifestError::validation(format!(
            "{prefix} pagination.default_limit must not exceed pagination.max_limit"
        )));
    }
    if matches!(pagination.max_pages, Some(0)) {
        return Err(ManifestError::validation(format!(
            "{prefix} pagination.max_pages must be greater than 0"
        )));
    }
    Ok(())
}

fn parse_openapi_operation_target(
    value: Value,
    source_name: &str,
    surface_id: &str,
    index: usize,
) -> Result<OpenApiOperationTarget> {
    let target: RawOpenApiOperationTarget =
        parse_json_value(value, "OpenAPI pagination operation target")?;
    let has_operation_id = target.operation_id.is_some();
    let has_method_path = target.method.is_some() || target.path.is_some();
    match (has_operation_id, has_method_path) {
        (true, false) => {
            let operation_id = target.operation_id.expect("checked");
            if operation_id.trim().is_empty() {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' surface '{surface_id}' pagination operation[{index}] target.operation_id must not be empty"
                )));
            }
            Ok(OpenApiOperationTarget::OperationId(operation_id))
        }
        (false, true) => {
            let Some(method) = target.method else {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' surface '{surface_id}' pagination operation[{index}] target.method is required when target.path is used"
                )));
            };
            let Some(path) = target.path else {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' surface '{surface_id}' pagination operation[{index}] target.path is required when target.method is used"
                )));
            };
            if path.trim().is_empty() {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' surface '{surface_id}' pagination operation[{index}] target.path must not be empty"
                )));
            }
            Ok(OpenApiOperationTarget::MethodPath { method, path })
        }
        (true, true) => Err(ManifestError::validation(format!(
            "source '{source_name}' surface '{surface_id}' pagination operation[{index}] target must use exactly one of operation_id or method+path"
        ))),
        (false, false) => Err(ManifestError::validation(format!(
            "source '{source_name}' surface '{surface_id}' pagination operation[{index}] target must declare operation_id or method+path"
        ))),
    }
}

fn parse_json_value<T>(value: Value, context: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(value)
        .map_err(|error| ManifestError::validation(format!("{context} is invalid: {error}")))
}

pub(crate) fn duplicate_operation_overlay_error(
    source_name: &str,
    surface_id: &str,
    operation: &str,
) -> ManifestError {
    ManifestError::validation(format!(
        "source '{source_name}' surface '{surface_id}' has multiple pagination operation overlays for {operation}"
    ))
}

pub(crate) fn unmatched_operation_overlay_error(
    source_name: &str,
    surface_id: &str,
    target: &str,
) -> ManifestError {
    ManifestError::validation(format!(
        "source '{source_name}' surface '{surface_id}' pagination operation target {target} did not match any imported operation"
    ))
}

pub(crate) fn multiple_profile_match_error(
    source_name: &str,
    surface_id: &str,
    operation: &str,
    profile_names: &[String],
) -> ManifestError {
    ManifestError::validation(format!(
        "source '{source_name}' surface '{surface_id}' operation {operation} matched multiple pagination profiles: {}",
        profile_names.join(", ")
    ))
}

pub(crate) fn validate_no_duplicate_targets(
    source_name: &str,
    surface_id: &str,
    overlays: &V4SurfacePagination,
) -> Result<()> {
    let mut targets: BTreeMap<String, usize> = BTreeMap::new();
    for (index, overlay) in overlays.operations.iter().enumerate() {
        let key = match &overlay.target {
            V4OperationTarget::OpenApi(OpenApiOperationTarget::OperationId(operation_id)) => {
                format!("operation_id:{operation_id}")
            }
            V4OperationTarget::OpenApi(OpenApiOperationTarget::MethodPath { method, path }) => {
                format!("method_path:{method:?}:{path}")
            }
            V4OperationTarget::Mcp(target) => format!("tool:{}", target.tool),
        };
        if let Some(existing) = targets.insert(key.clone(), index) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' surface '{surface_id}' pagination operations[{existing}] and operations[{index}] target the same operation ({key})"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::RawSurfacePagination;

    #[test]
    fn openapi_overlay_parser_rejects_unknown_http_pagination_fields() {
        let raw: RawSurfacePagination = serde_json::from_value(json!({
            "profiles": [{
                "name": "mcp_shape",
                "match": {
                    "methods": ["get"]
                },
                "pagination": {
                    "type": "cursor",
                    "cursor_arg": "cursor",
                    "response_cursor_path": ["meta", "nextCursor"]
                }
            }]
        }))
        .expect("raw pagination shape");

        let error = raw
            .parse_openapi("demo", "rest")
            .expect_err("unknown HTTP pagination overlay fields");

        assert!(
            error
                .to_string()
                .contains("OpenAPI pagination overlay is invalid: unknown field `type`"),
            "{error}"
        );
    }
}
