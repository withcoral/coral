//! Pins external OTLP trace export behavior against a real HTTP collector.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

use std::path::Path;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use opentelemetry_proto::tonic::metrics::v1::{Metric, metric, number_data_point};
use opentelemetry_proto::tonic::trace::v1::Span;
use prost::Message as _;
use tempfile::TempDir;
use tonic::Request;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request as WiremockRequest, ResponseTemplate};

use coral_api::v1::{ExecuteSqlRequest, SearchRequest};
use coral_app::{ServerBuilder, shutdown_tracing};
use coral_client::{AppClient, decode_execute_sql_response, default_workspace};

const LOCAL_ONLY_SENTINEL: &str = "LOCAL_ONLY_FUTURE_TOOL_SENTINEL";
const SEARCH_QUERY_SENTINEL: &str = "LOCAL_ONLY_SEARCH_QUERY_SENTINEL";

#[tokio::test]
async fn otlp_export_loopback_covers_traces_logs_and_metrics() {
    let collector = MockServer::start().await;
    for signal_path in ["/v1/traces", "/v1/logs", "/v1/metrics"] {
        Mock::given(method("POST"))
            .and(path(signal_path))
            .respond_with(ResponseTemplate::new(200))
            .mount(&collector)
            .await;
    }

    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    std::fs::create_dir_all(&config_dir).expect("create config dir");
    std::fs::write(
        config_dir.join("config.toml"),
        format!(
            r#"
version = 1

[otel]
endpoint = "{}"
trace_filter = "coral_app=trace,coral.http.body=trace,coral.mcp.body=trace"
log_filter = "coral_app=info,coral_engine=info"

[trace_history]
enabled = true
"#,
            collector.uri()
        ),
    )
    .expect("write telemetry config");

    let server = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .start()
        .await
        .expect("start server with OTLP endpoint");

    emit_test_telemetry(server.endpoint_uri()).await;

    shutdown_tracing();
    server.shutdown().await.expect("shutdown server");

    let local_trace_history = read_local_trace_history(&config_dir);
    assert_local_trace_history_contract(&local_trace_history);

    let trace_requests = trace_requests(&collector).await;
    assert!(
        !trace_requests.is_empty(),
        "collector should receive trace export"
    );
    let trace_exports = decode_trace_exports(&trace_requests);
    let spans = exported_spans(&trace_exports);
    assert_exported_trace_contract(&trace_exports, &spans);

    let log_requests = log_requests(&collector).await;
    assert!(
        !log_requests.is_empty(),
        "collector should receive log export"
    );
    let logs = decode_exported_logs(&log_requests);
    assert_exported_log_contract(&logs);

    let metric_requests = metric_requests(&collector).await;
    assert!(
        !metric_requests.is_empty(),
        "collector should receive metric export"
    );
    let metrics = decode_exported_metrics(&metric_requests);
    assert_exported_metric_contract(&metrics);
}

async fn emit_test_telemetry(endpoint_uri: &str) {
    let app = AppClient::connect(endpoint_uri)
        .await
        .expect("connect loopback client");
    let response = app
        .query_client()
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: "SELECT 1 AS loopback_value".to_string(),
            guide_read_context: None,
            task_attribution: None,
        }))
        .await
        .expect("execute loopback query")
        .into_inner();
    let result = decode_execute_sql_response(&response).expect("decode loopback query");
    assert_eq!(result.row_count(), 1);

    app.search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: SEARCH_QUERY_SENTINEL.to_string(),
            limit: 0,
        }))
        .await
        .expect("search empty catalog");

    let query = tracing::info_span!(
        target: "coral_app",
        "loopback_query",
        sql = "SELECT 'secret-loopback-sql'",
        coral.local.future_tool.input = LOCAL_ONLY_SENTINEL,
        status = "ok"
    );
    let _query = query.enter();

    tracing::info!(
        target: "coral_app",
        event_name = "loopback.log",
        log_value = "loopback-log-value"
    );

    let http_body = tracing::trace_span!(
        target: "coral.http.body",
        "loopback_http_body",
        coral.http.request.body = "trace-body-secret"
    );
    let _http_body = http_body.enter();

    let mcp_body = tracing::trace_span!(
        target: "coral.mcp.body",
        "loopback_mcp_body",
        coral.mcp.request.body = "mcp-body-secret"
    );
    let _mcp_body = mcp_body.enter();
}

async fn trace_requests(collector: &MockServer) -> Vec<WiremockRequest> {
    requests_for_path(collector, "/v1/traces").await
}

async fn log_requests(collector: &MockServer) -> Vec<WiremockRequest> {
    requests_for_path(collector, "/v1/logs").await
}

async fn metric_requests(collector: &MockServer) -> Vec<WiremockRequest> {
    requests_for_path(collector, "/v1/metrics").await
}

async fn requests_for_path(collector: &MockServer, signal_path: &str) -> Vec<WiremockRequest> {
    collector
        .received_requests()
        .await
        .expect("collector request recording should be enabled")
        .into_iter()
        .filter(|request| request.url.path() == signal_path)
        .collect()
}

fn decode_trace_exports(trace_requests: &[WiremockRequest]) -> Vec<ExportTraceServiceRequest> {
    trace_requests
        .iter()
        .map(|request| {
            ExportTraceServiceRequest::decode(request.body.as_slice())
                .expect("decode OTLP trace export")
        })
        .collect()
}

fn exported_spans(trace_exports: &[ExportTraceServiceRequest]) -> Vec<Span> {
    trace_exports
        .iter()
        .flat_map(|export| export.resource_spans.clone())
        .flat_map(|resource_spans| resource_spans.scope_spans)
        .flat_map(|scope_spans| scope_spans.spans)
        .collect()
}

fn decode_exported_logs(log_requests: &[WiremockRequest]) -> Vec<LogRecord> {
    log_requests
        .iter()
        .flat_map(|request| {
            ExportLogsServiceRequest::decode(request.body.as_slice())
                .expect("decode OTLP log export")
                .resource_logs
        })
        .flat_map(|resource_logs| resource_logs.scope_logs)
        .flat_map(|scope_logs| scope_logs.log_records)
        .collect()
}

fn decode_exported_metrics(metric_requests: &[WiremockRequest]) -> Vec<Metric> {
    metric_requests
        .iter()
        .flat_map(|request| {
            ExportMetricsServiceRequest::decode(request.body.as_slice())
                .expect("decode OTLP metric export")
                .resource_metrics
        })
        .flat_map(|resource_metrics| resource_metrics.scope_metrics)
        .flat_map(|scope_metrics| scope_metrics.metrics)
        .collect()
}

fn assert_exported_trace_contract(trace_exports: &[ExportTraceServiceRequest], spans: &[Span]) {
    let query = span_named(spans, "loopback_query");
    assert_eq!(string_attr(&query.attributes, "status"), Some("ok"));

    assert!(!has_span_named(spans, "loopback_http_body"));
    assert!(!has_span_named(spans, "loopback_mcp_body"));
    assert!(!trace_exports_contain_string(
        trace_exports,
        "trace-body-secret"
    ));
    assert!(!trace_exports_contain_string(
        trace_exports,
        "mcp-body-secret"
    ));
    assert!(!trace_exports_contain_string(
        trace_exports,
        LOCAL_ONLY_SENTINEL
    ));
    assert!(!trace_exports_contain_string(
        trace_exports,
        SEARCH_QUERY_SENTINEL
    ));
    assert!(spans.iter().all(|span| {
        span.attributes.iter().all(|attribute| {
            !attribute
                .key
                .starts_with(coral_telemetry::LOCAL_ONLY_SPAN_ATTRIBUTE_PREFIX)
        })
    }));
    for expected in ["loopback.log", "loopback-log-value"] {
        assert!(
            trace_exports_contain_string(trace_exports, expected),
            "contextual application log events should also be exported as OTLP span events: {trace_exports:?}"
        );
    }
}

fn assert_exported_log_contract(logs: &[LogRecord]) {
    assert!(
        logs.iter()
            .any(|log| log_contains_string(log, "loopback.log")),
        "OTLP log export should keep application log events: {logs:?}"
    );
    assert!(
        logs.iter()
            .any(|log| log_contains_string(log, "loopback-log-value")),
        "OTLP log export should keep application log attributes: {logs:?}"
    );
    assert!(
        logs.iter()
            .all(|log| !log_contains_string(log, SEARCH_QUERY_SENTINEL)),
        "OTLP logs must not contain local-only Search text: {logs:?}"
    );
}

fn assert_exported_metric_contract(metrics: &[Metric]) {
    assert_sum_metric_has_point(
        metrics,
        "coral.query.count",
        &[("operation", "execute_sql"), ("status", "ok")],
        1,
    );
    assert_histogram_metric_has_point(
        metrics,
        "coral.query.duration",
        &[("operation", "execute_sql"), ("status", "ok")],
    );
    assert_histogram_metric_has_point_with_sum(
        metrics,
        "coral.query.rows",
        &[("operation", "execute_sql"), ("status", "ok")],
        1,
        1.0,
    );
}

fn assert_sum_metric_has_point(
    metrics: &[Metric],
    name: &str,
    attributes: &[(&str, &str)],
    expected_value: i64,
) {
    let metric = metric_named(metrics, name);
    let Some(metric::Data::Sum(sum)) = metric.data.as_ref() else {
        panic!("expected {name} to export as sum: {metric:?}");
    };
    assert!(
        sum.data_points
            .iter()
            .any(|point| attrs_include(&point.attributes, attributes)
                && number_value_is(point.value.as_ref(), expected_value)),
        "{name} should include attributes {attributes:?} and value {expected_value}: {metric:?}"
    );
}

fn assert_histogram_metric_has_point(metrics: &[Metric], name: &str, attributes: &[(&str, &str)]) {
    let metric = metric_named(metrics, name);
    let Some(metric::Data::Histogram(histogram)) = metric.data.as_ref() else {
        panic!("expected {name} to export as histogram: {metric:?}");
    };
    assert!(
        histogram
            .data_points
            .iter()
            .any(|point| point.count > 0 && attrs_include(&point.attributes, attributes)),
        "{name} should include attributes {attributes:?}: {metric:?}"
    );
}

fn assert_histogram_metric_has_point_with_sum(
    metrics: &[Metric],
    name: &str,
    attributes: &[(&str, &str)],
    expected_count: u64,
    expected_sum: f64,
) {
    let metric = metric_named(metrics, name);
    let Some(metric::Data::Histogram(histogram)) = metric.data.as_ref() else {
        panic!("expected {name} to export as histogram: {metric:?}");
    };
    assert!(
        histogram.data_points.iter().any(|point| {
            point.count == expected_count
                && point
                    .sum
                    .is_some_and(|sum| (sum - expected_sum).abs() < f64::EPSILON)
                && attrs_include(&point.attributes, attributes)
        }),
        "{name} should include attributes {attributes:?}, count {expected_count}, and sum {expected_sum}: {metric:?}"
    );
}

fn assert_local_trace_history_contract(local_trace_history: &str) {
    for expected in [
        "loopback_query",
        "SELECT 'secret-loopback-sql'",
        "loopback_http_body",
        "trace-body-secret",
        "loopback_mcp_body",
        "mcp-body-secret",
        LOCAL_ONLY_SENTINEL,
        SEARCH_QUERY_SENTINEL,
    ] {
        assert!(
            local_trace_history.contains(expected),
            "local trace history should contain {expected}: {local_trace_history}"
        );
    }
}

fn span_named<'a>(spans: &'a [Span], name: &str) -> &'a Span {
    spans
        .iter()
        .find(|span| span.name == name)
        .unwrap_or_else(|| panic!("expected exported span {name}; got {:?}", span_names(spans)))
}

fn has_span_named(spans: &[Span], name: &str) -> bool {
    spans.iter().any(|span| span.name == name)
}

fn span_names(spans: &[Span]) -> Vec<&str> {
    spans.iter().map(|span| span.name.as_str()).collect()
}

fn metric_named<'a>(metrics: &'a [Metric], name: &str) -> &'a Metric {
    metrics
        .iter()
        .find(|metric| metric.name == name)
        .unwrap_or_else(|| panic!("expected metric {name}; got {:?}", metric_names(metrics)))
}

fn metric_names(metrics: &[Metric]) -> Vec<&str> {
    metrics.iter().map(|metric| metric.name.as_str()).collect()
}

fn attrs_include(attributes: &[KeyValue], expected: &[(&str, &str)]) -> bool {
    expected
        .iter()
        .all(|(key, value)| string_attr(attributes, key) == Some(*value))
}

fn number_value_is(value: Option<&number_data_point::Value>, expected: i64) -> bool {
    matches!(value, Some(number_data_point::Value::AsInt(value)) if *value == expected)
}

fn string_attr<'a>(attributes: &'a [KeyValue], key: &str) -> Option<&'a str> {
    attributes
        .iter()
        .find(|attribute| attribute.key == key)
        .and_then(|attribute| string_value(attribute.value.as_ref()))
}

fn trace_exports_contain_string(trace_exports: &[ExportTraceServiceRequest], value: &str) -> bool {
    trace_exports.iter().any(|export| {
        export.resource_spans.iter().any(|resource_spans| {
            resource_spans
                .resource
                .as_ref()
                .is_some_and(|resource| attributes_contain_string(&resource.attributes, value))
                || resource_spans.scope_spans.iter().any(|scope_spans| {
                    scope_spans.scope.as_ref().is_some_and(|scope| {
                        scope.name.contains(value)
                            || scope.version.contains(value)
                            || attributes_contain_string(&scope.attributes, value)
                    }) || scope_spans
                        .spans
                        .iter()
                        .any(|span| span_contains_string(span, value))
                })
        })
    })
}

fn span_contains_string(span: &Span, value: &str) -> bool {
    span.name.contains(value)
        || span
            .status
            .as_ref()
            .is_some_and(|status| status.message.contains(value))
        || attributes_contain_string(&span.attributes, value)
        || span.events.iter().any(|event| {
            event.name.contains(value) || attributes_contain_string(&event.attributes, value)
        })
        || span
            .links
            .iter()
            .any(|link| attributes_contain_string(&link.attributes, value))
}

fn log_contains_string(log: &LogRecord, value: &str) -> bool {
    log.severity_text.contains(value)
        || log
            .body
            .as_ref()
            .is_some_and(|body| any_value_contains_string(body, value))
        || attributes_contain_string(&log.attributes, value)
}

fn attributes_contain_string(attributes: &[KeyValue], value: &str) -> bool {
    attributes.iter().any(|attribute| {
        attribute.key.contains(value)
            || any_value_contains_string_opt(attribute.value.as_ref(), value)
    })
}

fn any_value_contains_string_opt(any_value: Option<&AnyValue>, value: &str) -> bool {
    any_value.is_some_and(|any_value| any_value_contains_string(any_value, value))
}

fn any_value_contains_string(any_value: &AnyValue, value: &str) -> bool {
    match any_value.value.as_ref() {
        Some(any_value::Value::StringValue(string)) => string.contains(value),
        Some(any_value::Value::ArrayValue(array)) => array
            .values
            .iter()
            .any(|nested| any_value_contains_string(nested, value)),
        Some(any_value::Value::KvlistValue(kv_list)) => {
            attributes_contain_string(&kv_list.values, value)
        }
        _ => false,
    }
}

fn string_value(value: Option<&AnyValue>) -> Option<&str> {
    match value?.value.as_ref()? {
        any_value::Value::StringValue(value) => Some(value),
        _ => None,
    }
}

fn read_local_trace_history(config_dir: &Path) -> String {
    let trace_dir = config_dir.join("telemetry").join("traces");
    let mut trace_history = String::new();
    for entry in std::fs::read_dir(&trace_dir).expect("read local trace dir") {
        let path = entry.expect("trace file entry").path();
        if path
            .extension()
            .is_some_and(|extension| extension == "jsonl")
        {
            trace_history.push_str(&std::fs::read_to_string(path).expect("read trace file"));
        }
    }
    trace_history
}
