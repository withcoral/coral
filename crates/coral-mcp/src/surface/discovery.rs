use rmcp::ErrorData;
use schemars::JsonSchema;
use serde_json::{Map, Value};

use super::arguments::optional_u32_argument;

pub(crate) const MIN_PAGINATION_LIMIT: u32 = 1;
pub(crate) const DEFAULT_PAGINATION_LIMIT: u32 = 50;
pub(crate) const MAX_PAGINATION_LIMIT: u32 = 200;
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

pub(crate) fn parse_pagination(
    arguments: Option<&Map<String, Value>>,
) -> Result<Pagination, ErrorData> {
    parse_pagination_with_limits(arguments, DEFAULT_PAGINATION_LIMIT, MAX_PAGINATION_LIMIT)
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

fn default_pagination_limit() -> u32 {
    DEFAULT_PAGINATION_LIMIT
}

fn default_pagination_offset() -> u32 {
    DEFAULT_PAGINATION_OFFSET
}
