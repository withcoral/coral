//! Token-efficiency benchmark for the MCP `list_columns` result.

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use tiktoken_rs::o200k_base_singleton;

const COLUMN_FIELDS: [&str; 7] = [
    "column_name",
    "data_type",
    "is_nullable",
    "is_virtual",
    "is_required_filter",
    "description",
    "ordinal_position",
];

#[derive(Debug, clap::Args)]
pub(crate) struct Args {
    /// Positional `list_columns` fixture to benchmark.
    #[arg(
        long,
        default_value = "xtask/fixtures/benchmarks/list-columns/github-issues.json"
    )]
    fixture: PathBuf,
}

pub(crate) fn run(args: &Args) -> Result<bool> {
    let raw = fs::read_to_string(&args.fixture)
        .with_context(|| format!("reading {}", args.fixture.display()))?;
    let result = benchmark(&raw)?;

    println!(
        "list_columns {}.{} ({} of {} columns, o200k_base)",
        result.schema_name, result.table_name, result.row_count, result.total
    );
    println!(
        "objects:    {:>6} bytes  {:>6} tokens",
        result.objects.bytes, result.objects.tokens
    );
    println!(
        "positional: {:>6} bytes  {:>6} tokens",
        result.positional.bytes, result.positional.tokens
    );
    println!(
        "saved:      {:>6} bytes  {:>6} tokens",
        percent_saved(result.objects.bytes, result.positional.bytes),
        percent_saved(result.objects.tokens, result.positional.tokens)
    );
    Ok(true)
}

fn benchmark(raw: &str) -> Result<ListColumnsBenchmark> {
    let fixture: Fixture = serde_json::from_str(raw).context("parsing list_columns fixture")?;
    fixture.validate()?;

    let positional_json =
        serde_json::to_string(&fixture).context("serializing positional list_columns fixture")?;
    let objects_json = serde_json::to_string(&fixture.as_objects())
        .context("serializing object list_columns fixture")?;

    Ok(ListColumnsBenchmark {
        schema_name: fixture.schema_name,
        table_name: fixture.table_name,
        row_count: fixture.rows.len(),
        total: fixture.total,
        objects: measure_json(&objects_json),
        positional: measure_json(&positional_json),
    })
}

fn measure_json(json: &str) -> JsonMeasurement {
    JsonMeasurement {
        bytes: json.len(),
        tokens: o200k_base_singleton().encode_ordinary(json).len(),
    }
}

fn percent_saved(before: usize, after: usize) -> String {
    let tenths = before
        .saturating_sub(after)
        .saturating_mul(1_000)
        .saturating_add(before / 2)
        .checked_div(before)
        .unwrap_or_default();
    format!("{}.{:01}%", tenths / 10, tenths % 10)
}

type ColumnRow = (String, String, bool, bool, bool, String, u32);

#[derive(Debug, Deserialize, Serialize)]
struct Fixture {
    #[serde(rename = "source", skip_serializing)]
    _source: String,
    schema_name: String,
    table_name: String,
    fields: [String; COLUMN_FIELDS.len()],
    rows: Vec<ColumnRow>,
    total: u32,
    limit: u32,
    offset: u32,
    has_more: bool,
    next_offset: u32,
}

impl Fixture {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.fields.iter().map(String::as_str).eq(COLUMN_FIELDS),
            "fixture fields do not match the list_columns row contract"
        );
        ensure!(
            self.rows.len() == self.limit as usize,
            "fixture row count must equal its page limit"
        );
        ensure!(
            self.rows.iter().enumerate().all(|(index, row)| {
                u32::try_from(index)
                    .ok()
                    .and_then(|index| self.offset.checked_add(index))
                    .is_some_and(|ordinal| row.6 == ordinal)
            }),
            "fixture ordinal positions must be contiguous from its offset"
        );
        Ok(())
    }

    fn as_objects(&self) -> ObjectPage<'_> {
        ObjectPage {
            schema_name: &self.schema_name,
            table_name: &self.table_name,
            columns: self.rows.iter().map(ColumnObject::from).collect(),
            total: self.total,
            limit: self.limit,
            offset: self.offset,
            has_more: self.has_more,
            next_offset: self.next_offset,
        }
    }
}

#[derive(Serialize)]
struct ObjectPage<'a> {
    schema_name: &'a str,
    table_name: &'a str,
    columns: Vec<ColumnObject<'a>>,
    total: u32,
    limit: u32,
    offset: u32,
    has_more: bool,
    next_offset: u32,
}

#[derive(Serialize)]
struct ColumnObject<'a> {
    column_name: &'a str,
    data_type: &'a str,
    is_nullable: bool,
    is_virtual: bool,
    is_required_filter: bool,
    description: &'a str,
    ordinal_position: u32,
}

impl<'a> From<&'a ColumnRow> for ColumnObject<'a> {
    fn from(row: &'a ColumnRow) -> Self {
        Self {
            column_name: &row.0,
            data_type: &row.1,
            is_nullable: row.2,
            is_virtual: row.3,
            is_required_filter: row.4,
            description: &row.5,
            ordinal_position: row.6,
        }
    }
}

struct ListColumnsBenchmark {
    schema_name: String,
    table_name: String,
    row_count: usize,
    total: u32,
    objects: JsonMeasurement,
    positional: JsonMeasurement,
}

struct JsonMeasurement {
    bytes: usize,
    tokens: usize,
}

#[cfg(test)]
mod tests {
    use super::benchmark;

    const GITHUB_ISSUES_FIXTURE: &str =
        include_str!("../../fixtures/benchmarks/list-columns/github-issues.json");

    #[test]
    fn positional_rows_use_fewer_tokens_than_objects() {
        let result = benchmark(GITHUB_ISSUES_FIXTURE).expect("benchmark fixture");

        assert_eq!(result.row_count, 50);
        assert_eq!(result.total, 420);
        assert!(result.positional.tokens < result.objects.tokens);
    }
}
