use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use coral_engine::CoralQuery;
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{build_source, dir_url, execution_to_rows, test_runtime};

#[derive(Debug, Clone, Copy)]
enum FixtureFormat {
    Parquet,
    Jsonl,
    Json,
    Csv,
}

#[derive(Debug, Clone)]
struct OrderRow {
    order_id: i64,
    customer: &'static str,
    region: &'static str,
    channel: &'static str,
    amount: f64,
    refunded: bool,
    sale_date: &'static str,
}

impl FixtureFormat {
    fn format_name(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Jsonl => "jsonl",
            Self::Json => "json",
            Self::Csv => "csv",
        }
    }

    fn glob(self) -> &'static str {
        match self {
            Self::Parquet => "**/*.parquet",
            Self::Jsonl => "**/*.jsonl",
            Self::Json => "**/*.json",
            Self::Csv => "**/*.csv",
        }
    }

    fn schema_name(self) -> &'static str {
        match self {
            Self::Parquet => "orders_parquet",
            Self::Jsonl => "orders_jsonl",
            Self::Json => "orders_json",
            Self::Csv => "orders_csv",
        }
    }
}

#[tokio::test]
async fn realistic_partitioned_orders_query_consistently_across_file_formats() {
    for format in [
        FixtureFormat::Parquet,
        FixtureFormat::Jsonl,
        FixtureFormat::Json,
        FixtureFormat::Csv,
    ] {
        let temp = TempDir::new().expect("temp dir");
        write_orders_fixture(temp.path(), format, &orders());
        let sources = vec![build_source(orders_manifest(format, temp.path()))];
        let schema = format.schema_name();

        let gross_by_region = execution_to_rows(
            &CoralQuery::execute_sql(
                &sources,
                test_runtime(),
                &format!(
                    "SELECT sale_date, region, COUNT(*) AS order_count, \
                     ROUND(SUM(amount), 2) AS gross, \
                     SUM(CASE WHEN refunded THEN 1 ELSE 0 END) AS refunded_count \
                     FROM {schema}.orders \
                     WHERE amount >= 80 \
                     GROUP BY sale_date, region \
                     ORDER BY sale_date, region"
                ),
            )
            .await
            .unwrap_or_else(|error| panic!("{format:?} aggregate query failed: {error:?}")),
        );
        assert_eq!(
            gross_by_region,
            vec![
                json!({
                    "sale_date": "2026-04-01",
                    "region": "EU",
                    "order_count": 1,
                    "gross": 90.0,
                    "refunded_count": 0
                }),
                json!({
                    "sale_date": "2026-04-01",
                    "region": "NA",
                    "order_count": 2,
                    "gross": 430.75,
                    "refunded_count": 1
                }),
                json!({
                    "sale_date": "2026-04-02",
                    "region": "EU",
                    "order_count": 1,
                    "gross": 150.25,
                    "refunded_count": 0
                }),
            ],
            "{format:?} aggregate rows differed"
        );

        let flagged_orders = execution_to_rows(
            &CoralQuery::execute_sql(
                &sources,
                test_runtime(),
                &format!(
                    "SELECT order_id, customer, sale_date \
                     FROM {schema}.orders \
                     WHERE refunded OR channel = 'partner' \
                     ORDER BY order_id"
                ),
            )
            .await
            .unwrap_or_else(|error| panic!("{format:?} predicate query failed: {error:?}")),
        );
        assert_eq!(
            flagged_orders,
            vec![
                json!({
                    "order_id": 1002,
                    "customer": "Beta Group",
                    "sale_date": "2026-04-01"
                }),
                json!({
                    "order_id": 1003,
                    "customer": "Acme Labs",
                    "sale_date": "2026-04-01"
                }),
            ],
            "{format:?} predicate rows differed"
        );
    }
}

#[tokio::test]
async fn json_file_formats_preserve_nested_json_for_udfs() {
    for format in [FixtureFormat::Jsonl, FixtureFormat::Json] {
        let temp = TempDir::new().expect("temp dir");
        let rows = vec![
            json!({
                "id": 1,
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": "hello"
                }
            }),
            json!({
                "id": 2,
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": "hi"
                }
            }),
        ];
        match format {
            FixtureFormat::Jsonl => {
                let body = rows
                    .iter()
                    .map(|row| serde_json::to_string(row).expect("json row"))
                    .collect::<Vec<_>>()
                    .join("\n");
                std::fs::write(temp.path().join("events.jsonl"), format!("{body}\n"))
                    .expect("jsonl fixture should write");
            }
            FixtureFormat::Json => {
                std::fs::write(
                    temp.path().join("events.json"),
                    serde_json::to_string(&rows).expect("json array"),
                )
                .expect("json fixture should write");
            }
            FixtureFormat::Parquet | FixtureFormat::Csv => unreachable!("json-only test"),
        }

        let sources = vec![build_source(nested_events_manifest(format, temp.path()))];
        let schema = format.schema_name();
        let actual = execution_to_rows(
            &CoralQuery::execute_sql(
                &sources,
                test_runtime(),
                &format!(
                    "SELECT id, json_get_str(payload, 'role') AS payload_role \
                     FROM {schema}.events \
                     ORDER BY id"
                ),
            )
            .await
            .unwrap_or_else(|error| panic!("{format:?} nested JSON query failed: {error:?}")),
        );

        assert_eq!(
            actual,
            vec![
                json!({ "id": 1, "payload_role": "user" }),
                json!({ "id": 2, "payload_role": "assistant" }),
            ],
            "{format:?} nested JSON rows differed"
        );
    }
}

fn orders_manifest(format: FixtureFormat, root: &Path) -> Value {
    let mut table = json!({
        "name": "orders",
        "description": "Partitioned ecommerce order events",
        "format": format.format_name(),
        "source": {
            "location": dir_url(root),
            "glob": format.glob(),
            "partitions": [
                { "name": "sale_date", "type": "Utf8" }
            ]
        },
        "columns": [
            { "name": "order_id", "type": "Int64" },
            { "name": "customer", "type": "Utf8" },
            { "name": "region", "type": "Utf8" },
            { "name": "channel", "type": "Utf8" },
            { "name": "amount", "type": "Float64" },
            { "name": "refunded", "type": "Boolean" }
        ]
    });

    if matches!(format, FixtureFormat::Csv) {
        table
            .as_object_mut()
            .expect("table object")
            .insert("format_options".to_string(), json!({ "has_header": true }));
    }

    json!({
        "name": format.schema_name(),
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [table],
    })
}

fn nested_events_manifest(format: FixtureFormat, root: &Path) -> Value {
    json!({
        "name": format.schema_name(),
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "events",
            "description": "Nested JSON events",
            "format": format.format_name(),
            "source": {
                "location": dir_url(root),
                "glob": format.glob(),
            },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "payload", "type": "Json" }
            ]
        }],
    })
}

fn orders() -> Vec<OrderRow> {
    vec![
        OrderRow {
            order_id: 1001,
            customer: "Acme Labs",
            region: "NA",
            channel: "web",
            amount: 120.50,
            refunded: false,
            sale_date: "2026-04-01",
        },
        OrderRow {
            order_id: 1002,
            customer: "Beta Group",
            region: "EU",
            channel: "partner",
            amount: 90.00,
            refunded: false,
            sale_date: "2026-04-01",
        },
        OrderRow {
            order_id: 1003,
            customer: "Acme Labs",
            region: "NA",
            channel: "web",
            amount: 310.25,
            refunded: true,
            sale_date: "2026-04-01",
        },
        OrderRow {
            order_id: 1004,
            customer: "Delta Retail",
            region: "APAC",
            channel: "direct",
            amount: 45.00,
            refunded: false,
            sale_date: "2026-04-02",
        },
        OrderRow {
            order_id: 1005,
            customer: "Echo Supply",
            region: "EU",
            channel: "web",
            amount: 150.25,
            refunded: false,
            sale_date: "2026-04-02",
        },
    ]
}

fn write_orders_fixture(root: &Path, format: FixtureFormat, rows: &[OrderRow]) {
    for sale_date in ["2026-04-01", "2026-04-02"] {
        let partition_rows: Vec<_> = rows
            .iter()
            .filter(|row| row.sale_date == sale_date)
            .cloned()
            .collect();
        let partition_dir = root.join(format!("sale_date={sale_date}"));
        std::fs::create_dir_all(&partition_dir).expect("partition directory should exist");

        match format {
            FixtureFormat::Parquet => {
                write_orders_parquet(&partition_dir.join("orders.parquet"), &partition_rows);
            }
            FixtureFormat::Jsonl => {
                let body = partition_rows
                    .iter()
                    .map(|row| serde_json::to_string(&order_json(row)).expect("order json"))
                    .collect::<Vec<_>>()
                    .join("\n");
                std::fs::write(partition_dir.join("orders.jsonl"), format!("{body}\n"))
                    .expect("jsonl fixture should write");
            }
            FixtureFormat::Json => {
                let body = partition_rows.iter().map(order_json).collect::<Vec<_>>();
                std::fs::write(
                    partition_dir.join("orders.json"),
                    serde_json::to_string(&body).expect("json array"),
                )
                .expect("json fixture should write");
            }
            FixtureFormat::Csv => {
                let mut body = String::from("order_id,customer,region,channel,amount,refunded\n");
                for row in &partition_rows {
                    writeln!(
                        &mut body,
                        "{},{},{},{},{:.2},{}",
                        row.order_id,
                        row.customer,
                        row.region,
                        row.channel,
                        row.amount,
                        row.refunded
                    )
                    .expect("csv row should write");
                }
                std::fs::write(partition_dir.join("orders.csv"), body)
                    .expect("csv fixture should write");
            }
        }
    }
}

fn order_json(row: &OrderRow) -> Value {
    json!({
        "order_id": row.order_id,
        "customer": row.customer,
        "region": row.region,
        "channel": row.channel,
        "amount": row.amount,
        "refunded": row.refunded,
    })
}

fn write_orders_parquet(path: &PathBuf, rows: &[OrderRow]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("refunded", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|row| row.order_id),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.customer),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.region),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.channel),
            )),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|row| row.amount),
            )),
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.refunded))
                    .collect::<BooleanArray>(),
            ),
        ],
    )
    .expect("record batch should build");

    let file = std::fs::File::create(path).expect("parquet file should open");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer should start");
    writer.write(&batch).expect("parquet batch should write");
    writer.close().expect("parquet writer should close");
}
