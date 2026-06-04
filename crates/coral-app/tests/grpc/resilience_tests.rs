#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test code: assertion-style indexing is idiomatic in tests"
)]

use std::fs;

use crate::harness::{GrpcHarness, assert_table_absent, assert_table_present, source_dir};

#[tokio::test]
async fn broken_source_does_not_block_healthy_sources() {
    let harness = GrpcHarness::new().await;

    harness.import_local_messages_source().await;
    harness.import_secured_messages_source().await;

    fs::remove_file(source_dir(harness.config_dir(), "secured_messages").join("secrets.env"))
        .expect("remove broken source secret file");

    let tables = harness.list_tables().await;
    assert_table_present(&tables, "local_messages");
    assert_table_absent(&tables, "secured_messages");

    let healthy_rows = harness
        .execute_sql_rows("SELECT COUNT(*) AS n FROM local_messages.messages")
        .await;
    assert_eq!(healthy_rows[0]["n"], 2);

    let broken = harness
        .execute_sql_error("SELECT * FROM secured_messages.messages")
        .await;
    assert_eq!(broken.code(), tonic::Code::NotFound);
}
