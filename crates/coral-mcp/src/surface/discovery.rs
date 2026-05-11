use coral_api::v1::TableSummary as ProtoTableSummary;
use regex::{Regex, RegexBuilder};
use rmcp::ErrorData;
use serde_json::{Map, Value, json};

use super::resources::format_schema_table_equivalent;

const DEFAULT_LIMIT: u32 = 50;
const MAX_LIMIT: u32 = 200;
const MAX_METADATA_PATTERN_BYTES: usize = 256;
const REGEX_SIZE_LIMIT_BYTES: usize = 1 << 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Pagination {
    pub(crate) limit: u32,
    pub(crate) offset: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Page<T> {
    pub(crate) items: Vec<T>,
    pub(crate) total: u32,
    pub(crate) limit: u32,
    pub(crate) offset: u32,
    pub(crate) has_more: bool,
    pub(crate) next_offset: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableSummary {
    pub(crate) schema_name: String,
    pub(crate) table_name: String,
    pub(crate) description: String,
    pub(crate) guide: String,
    pub(crate) required_filters: Vec<String>,
}

#[expect(
    dead_code,
    reason = "Column summaries are shared discovery scaffolding for the follow-up column tool."
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ColumnSummary {
    pub(crate) name: String,
    pub(crate) data_type: String,
    pub(crate) nullable: bool,
    pub(crate) is_virtual: bool,
    pub(crate) is_required_filter: bool,
    pub(crate) description: String,
    pub(crate) ordinal_position: u32,
}

impl TableSummary {
    pub(crate) fn from_proto(table: &ProtoTableSummary) -> Self {
        Self {
            schema_name: table.schema_name.clone(),
            table_name: table.name.clone(),
            description: table.description.clone(),
            guide: table.guide.clone(),
            required_filters: table.required_filters.clone(),
        }
    }

    pub(crate) fn matched_fields(&self, regex: &Regex) -> Vec<&'static str> {
        let name = format!("{}.{}", self.schema_name, self.table_name);
        let candidates = [
            ("schema_name", self.schema_name.as_str()),
            ("table_name", self.table_name.as_str()),
            ("name", name.as_str()),
            ("description", self.description.as_str()),
            ("guide", self.guide.as_str()),
        ];
        let mut matches = candidates
            .into_iter()
            .filter_map(|(field, value)| regex.is_match(value).then_some(field))
            .collect::<Vec<_>>();
        if self
            .required_filters
            .iter()
            .any(|filter| regex.is_match(filter))
        {
            matches.push("required_filters");
        }
        matches
    }

    pub(crate) fn search_result_value(&self, matched_fields: &[&'static str]) -> Value {
        json!({
            "schema_name": self.schema_name,
            "table_name": self.table_name,
            "name": format!("{}.{}", self.schema_name, self.table_name),
            "sql_reference": format_schema_table_equivalent(&self.schema_name, &self.table_name),
            "description": self.description,
            "required_filters": self.required_filters,
            "matched_fields": matched_fields,
        })
    }
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

pub(crate) fn compile_metadata_regex(pattern: &str, ignore_case: bool) -> Result<Regex, ErrorData> {
    if pattern.trim().is_empty() {
        return Err(ErrorData::invalid_params(
            "argument 'pattern' must not be empty",
            None,
        ));
    }
    if pattern.len() > MAX_METADATA_PATTERN_BYTES {
        return Err(ErrorData::invalid_params(
            format!("argument 'pattern' must be at most {MAX_METADATA_PATTERN_BYTES} bytes"),
            None,
        ));
    }
    RegexBuilder::new(pattern)
        .case_insensitive(ignore_case)
        .size_limit(REGEX_SIZE_LIMIT_BYTES)
        .build()
        .map_err(|error| ErrorData::invalid_params(format!("invalid regex pattern: {error}"), None))
}

pub(crate) fn page_items<T>(items: Vec<T>, pagination: Pagination) -> Page<T> {
    let total = u32::try_from(items.len()).unwrap_or(u32::MAX);
    let offset = usize::try_from(pagination.offset).unwrap_or(usize::MAX);
    let limit = usize::try_from(pagination.limit).unwrap_or(usize::MAX);
    let items = items
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    let returned_count = u32::try_from(items.len()).unwrap_or(u32::MAX);
    let advanced_offset = pagination.offset.saturating_add(returned_count);
    let has_more = advanced_offset < total;
    Page {
        items,
        total,
        limit: pagination.limit,
        offset: pagination.offset,
        has_more,
        next_offset: has_more.then_some(advanced_offset),
    }
}

pub(crate) fn paged_value(key: &str, page: Page<Value>) -> Value {
    let Page {
        items,
        total,
        limit,
        offset,
        has_more,
        next_offset,
    } = page;
    let mut value = json!({
        key: items,
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": has_more,
    });
    if let Some(next_offset) = next_offset {
        value
            .as_object_mut()
            .expect("paged value is initialized as a JSON object")
            .insert("next_offset".to_string(), json!(next_offset));
    }
    value
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

#[cfg(test)]
mod tests {
    #![expect(
        clippy::indexing_slicing,
        reason = "JSON shape assertions intentionally fail loudly in tests"
    )]

    use regex::Regex;

    use super::TableSummary;

    fn table(required_filters: &[&str]) -> TableSummary {
        TableSummary {
            schema_name: "github".to_string(),
            table_name: "Pull.Requests".to_string(),
            description: "Pull request table".to_string(),
            guide: "Query pull requests.".to_string(),
            required_filters: required_filters.iter().map(ToString::to_string).collect(),
        }
    }

    #[test]
    fn search_result_includes_sql_reference() {
        let value = table(&[]).search_result_value(&["table_name"]);

        assert_eq!(value["name"], "github.Pull.Requests");
        assert_eq!(value["sql_reference"], "github.\"Pull.Requests\"");
    }

    #[test]
    fn required_filters_match_each_filter_independently() {
        let summary = table(&["owner", "repo"]);

        assert_eq!(
            summary.matched_fields(&Regex::new("^repo$").expect("regex")),
            vec!["required_filters"]
        );
        assert!(
            summary
                .matched_fields(&Regex::new("r.r").expect("regex"))
                .is_empty()
        );
    }
}
