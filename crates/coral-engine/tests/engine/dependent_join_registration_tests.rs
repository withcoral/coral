use coral_engine::CoralQuery;
use serde_json::{Value, json};

use crate::harness::{build_source, test_runtime};

fn http_bindable_manifest(name: &str, base_url: &str) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": base_url,
        "tables": [{
            "name": "users",
            "description": "Users",
            "filters": [{ "name": "id", "bindable": true }],
            "request": {
                "path": "/users"
            },
            "columns": [
                { "name": "id", "type": "Utf8" }
            ]
        }]
    })
}

#[tokio::test]
async fn bindable_filter_on_http_backend_registers() {
    let source = build_source(http_bindable_manifest(
        "http_bindable",
        "https://example.invalid",
    ));

    let report = CoralQuery::validate_source(&source, test_runtime(), &[])
        .await
        .expect("http bindable source should register");

    assert_eq!(report.tables.len(), 1);
    let table = report.tables.first().expect("one registered table");
    assert_eq!(table.schema_name, "http_bindable");
}
