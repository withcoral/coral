use rmcp::ErrorData;
use serde_json::{Map, Value};

const DEFAULT_LIMIT: u32 = 50;
const MAX_LIMIT: u32 = 200;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Pagination {
    pub(crate) limit: u32,
    pub(crate) offset: u32,
}

pub(crate) fn parse_pagination(
    arguments: Option<&Map<String, Value>>,
) -> Result<Pagination, ErrorData> {
    parse_pagination_with_limits(arguments, DEFAULT_LIMIT, MAX_LIMIT)
}

pub(crate) fn parse_pagination_with_limits(
    arguments: Option<&Map<String, Value>>,
    default_limit: u32,
    max_limit: u32,
) -> Result<Pagination, ErrorData> {
    Ok(Pagination {
        limit: optional_u32_argument(arguments, "limit", default_limit, 1, max_limit)?,
        offset: optional_u32_argument(arguments, "offset", 0, 0, u32::MAX)?,
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
