use rmcp::ErrorData;
use schemars::JsonSchema;
use serde_json::{Map, Value};

pub(crate) const MIN_PAGINATION_LIMIT: u32 = 1;
pub(crate) const DEFAULT_PAGINATION_LIMIT: u32 = 50;
pub(crate) const MAX_PAGINATION_LIMIT: u32 = 200;
pub(crate) const DEFAULT_SEARCH_PAGINATION_LIMIT: u32 = 20;
pub(crate) const MAX_SEARCH_PAGINATION_LIMIT: u32 = 100;
pub(crate) const DEFAULT_PAGINATION_OFFSET: u32 = 0;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Pagination {
    pub(crate) limit: u32,
    pub(crate) offset: u32,
}

#[derive(JsonSchema)]
#[expect(
    dead_code,
    reason = "schema-only struct for flattened default pagination inputs"
)]
pub(crate) struct DefaultPaginationInput {
    #[serde(default = "default_pagination_limit")]
    #[schemars(
        range(min = MIN_PAGINATION_LIMIT, max = MAX_PAGINATION_LIMIT),
        description = "Maximum items to return, from 1 to 200. Defaults to 50."
    )]
    limit: u32,
    #[serde(default = "default_pagination_offset")]
    #[schemars(
        range(min = DEFAULT_PAGINATION_OFFSET, max = u32::MAX),
        description = "Number of matching items to skip. Defaults to 0."
    )]
    offset: u32,
}

#[derive(JsonSchema)]
#[expect(
    dead_code,
    reason = "schema-only struct for flattened search pagination inputs"
)]
pub(crate) struct SearchPaginationInput {
    #[serde(default = "default_search_pagination_limit")]
    #[schemars(
        range(min = MIN_PAGINATION_LIMIT, max = MAX_SEARCH_PAGINATION_LIMIT),
        description = "Maximum catalog items to return, from 1 to 100. Defaults to 20."
    )]
    limit: u32,
    #[serde(default = "default_pagination_offset")]
    #[schemars(
        range(min = DEFAULT_PAGINATION_OFFSET, max = u32::MAX),
        description = "Number of matching catalog items to skip. Defaults to 0."
    )]
    offset: u32,
}

pub(crate) fn parse_pagination(
    arguments: Option<&Map<String, Value>>,
) -> Result<Pagination, ErrorData> {
    parse_pagination_with_limits(arguments, DEFAULT_PAGINATION_LIMIT, MAX_PAGINATION_LIMIT)
}

pub(crate) fn parse_search_pagination(
    arguments: Option<&Map<String, Value>>,
) -> Result<Pagination, ErrorData> {
    parse_pagination_with_limits(
        arguments,
        DEFAULT_SEARCH_PAGINATION_LIMIT,
        MAX_SEARCH_PAGINATION_LIMIT,
    )
}

fn parse_pagination_with_limits(
    arguments: Option<&Map<String, Value>>,
    default_limit: u32,
    max_limit: u32,
) -> Result<Pagination, ErrorData> {
    Ok(Pagination {
        limit: optional_u32_argument(
            arguments,
            "limit",
            default_limit,
            MIN_PAGINATION_LIMIT,
            max_limit,
        )?,
        offset: optional_u32_argument(
            arguments,
            "offset",
            DEFAULT_PAGINATION_OFFSET,
            DEFAULT_PAGINATION_OFFSET,
            u32::MAX,
        )?,
    })
}

fn optional_u32_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
    default: u32,
    min: u32,
    max: u32,
) -> Result<u32, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(default);
    };
    let value = value.as_i64().ok_or_else(|| {
        ErrorData::invalid_params(format!("argument '{key}' must be an integer"), None)
    })?;
    if value < i64::from(min) || value > i64::from(max) {
        return Err(ErrorData::invalid_params(
            format!("argument '{key}' must be between {min} and {max}"),
            None,
        ));
    }
    u32::try_from(value).map_err(|_err| {
        ErrorData::invalid_params(
            format!("argument '{key}' must be between {min} and {max}"),
            None,
        )
    })
}

fn default_pagination_limit() -> u32 {
    DEFAULT_PAGINATION_LIMIT
}

fn default_search_pagination_limit() -> u32 {
    DEFAULT_SEARCH_PAGINATION_LIMIT
}

fn default_pagination_offset() -> u32 {
    DEFAULT_PAGINATION_OFFSET
}
