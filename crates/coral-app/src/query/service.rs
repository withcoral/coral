//! Implements the gRPC `QueryService`.

use std::collections::HashSet;
use std::io::Write;

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use coral_api::v1::query_service_server::QueryService as QueryServiceApi;
use coral_api::v1::{
    ExecuteSqlRequest, ExecuteSqlResponse, ExplainSqlRequest, ExplainSqlResponse, QueryGuide,
    QueryGuideReadContext, QueryGuideRequired, QueryPlan as QueryPlanProto,
};
use opentelemetry::trace::Status as OtelStatus;
use tonic::{Request, Response, Status};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::bootstrap::core_status;
use crate::query::QueryAttribution;
use crate::query::manager::{ExecuteSqlOutcome, QueryManager, RequiredQueryGuide};
use crate::task::manager::TaskManager;
use crate::task::service::task_manager_status;
use crate::transport::{
    grpc_span, instrument_grpc, query_status, request_context, workspace_name_from_proto,
};

#[derive(Clone)]
pub(crate) struct QueryService {
    queries: QueryManager,
    tasks: TaskManager,
}

impl QueryService {
    pub(crate) fn new(query_manager: QueryManager, task_manager: TaskManager) -> Self {
        Self {
            queries: query_manager,
            tasks: task_manager,
        }
    }
}

#[tonic::async_trait]
impl QueryServiceApi for QueryService {
    async fn execute_sql(
        &self,
        request: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ExecuteSqlResponse>, Status> {
        let span = grpc_span(&request);
        let queries = self.queries.clone();
        let tasks = self.tasks.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            let shown_guide_ids = shown_guide_ids(inner.guide_read_context);
            let attribution = QueryAttribution::new(
                tasks
                    .validate_attribution(&workspace_name, request_context.task_id())
                    .await
                    .map_err(task_manager_status)?,
            );
            let outcome = Box::pin(queries.execute_sql(
                &workspace_name,
                &inner.sql,
                shown_guide_ids.as_ref(),
                &attribution,
            ))
            .await
            .map_err(query_status)?;
            let response = match outcome {
                ExecuteSqlOutcome::Executed(execution) => ExecuteSqlResponse {
                    arrow_ipc_stream: encode_arrow_ipc_stream(
                        execution.arrow_schema(),
                        execution.batches(),
                    )
                    .map_err(coral_engine::CoreError::from)
                    .map_err(core_status)?,
                    row_count: i64::try_from(execution.row_count()).unwrap_or(i64::MAX),
                    guide_required: None,
                },
                ExecuteSqlOutcome::GuideRequired(guides) => ExecuteSqlResponse {
                    arrow_ipc_stream: Vec::new(),
                    row_count: 0,
                    guide_required: Some(QueryGuideRequired {
                        guides: guides
                            .into_iter()
                            .map(required_query_guide_to_proto)
                            .collect(),
                    }),
                },
            };
            Ok(Response::new(response))
        }))
        .await
    }

    async fn explain_sql(
        &self,
        request: Request<ExplainSqlRequest>,
    ) -> Result<Response<ExplainSqlResponse>, Status> {
        let span = grpc_span(&request);
        let queries = self.queries.clone();
        let tasks = self.tasks.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            let attribution = QueryAttribution::new(
                tasks
                    .validate_attribution(&workspace_name, request_context.task_id())
                    .await
                    .map_err(task_manager_status)?,
            );
            let plan = queries
                .explain_sql(&workspace_name, &inner.sql, &attribution)
                .await
                .map_err(query_status)?;
            Ok(Response::new(ExplainSqlResponse {
                plan: Some(query_plan_to_proto(&plan)),
            }))
        }))
        .await
    }
}

fn shown_guide_ids(context: Option<QueryGuideReadContext>) -> Option<HashSet<String>> {
    context.map(|context| context.shown_guide_ids.into_iter().collect())
}

fn query_plan_to_proto(plan: &coral_engine::QueryPlan) -> QueryPlanProto {
    QueryPlanProto {
        unoptimized_logical_plan: plan.unoptimized_logical_plan().to_string(),
        optimized_logical_plan: plan.optimized_logical_plan().to_string(),
        physical_plan: plan.physical_plan().to_string(),
    }
}

fn required_query_guide_to_proto(guide: RequiredQueryGuide) -> QueryGuide {
    QueryGuide {
        schema_name: guide.schema_name,
        resource_name: guide.resource_name,
        guide: guide.guide,
        guide_id: guide.guide_id,
    }
}

fn encode_arrow_ipc_stream(
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<Vec<u8>, arrow::error::ArrowError> {
    observe_arrow_ipc_encoding(|| serialize_arrow_ipc_stream(Vec::new(), schema, batches))
}

fn serialize_arrow_ipc_stream<W: Write>(
    sink: W,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<W, arrow::error::ArrowError> {
    let mut writer = StreamWriter::try_new(sink, schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    writer.into_inner()
}

fn observe_arrow_ipc_encoding(
    encode: impl FnOnce() -> Result<Vec<u8>, arrow::error::ArrowError>,
) -> Result<Vec<u8>, arrow::error::ArrowError> {
    observe_arrow_ipc_encoding_with_size(encode, |len| u64::try_from(len).ok())
}

fn observe_arrow_ipc_encoding_with_size(
    encode: impl FnOnce() -> Result<Vec<u8>, arrow::error::ArrowError>,
    encoded_size: impl FnOnce(usize) -> Option<u64>,
) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let span = tracing::info_span!(
        "coral.query.result.ipc",
        encoded_size_bytes = tracing::field::Empty,
    );
    let _entered = span.enter();
    match encode() {
        Ok(bytes) => {
            if let Some(encoded_size) = encoded_size(bytes.len()) {
                if let Ok(span_size) = i64::try_from(encoded_size) {
                    span.record("encoded_size_bytes", span_size);
                }
                crate::telemetry::metrics::metrics()
                    .record_query_result_ipc_encoded_size(encoded_size);
            }
            span.set_status(OtelStatus::Ok);
            Ok(bytes)
        }
        Err(error) => {
            coral_telemetry::record_failure(
                &span,
                "arrow.ipc.encoding",
                "Arrow IPC encoding failed",
            );
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use opentelemetry::trace::{Status as OtelStatus, TracerProvider as _};
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing_subscriber::layer::SubscriberExt as _;

    use super::{
        encode_arrow_ipc_stream, observe_arrow_ipc_encoding, observe_arrow_ipc_encoding_with_size,
        serialize_arrow_ipc_stream,
    };

    #[derive(Default)]
    struct FinishFailWriter(Vec<u8>);

    impl Write for FinishFailWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.write(bytes)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::other("forced finish failure"))
        }
    }

    #[test]
    fn ipc_success_records_exact_returned_length_as_grpc_sibling_after_query() {
        crate::telemetry::metrics::test_support::reset_metrics();
        let metric_exporter = crate::telemetry::metrics::test_support::install_metrics_exporter();
        let span_exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(span_exporter.clone())
            .build();
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("ipc-result-test")));
        let _guard = tracing::subscriber::set_default(subscriber);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("test batch");
        let grpc = tracing::info_span!("grpc.request");
        let bytes = {
            let _grpc_entered = grpc.enter();
            let query = tracing::info_span!("coral.query");
            {
                let _query_entered = query.enter();
            }
            drop(query);
            encode_arrow_ipc_stream(&schema, &[batch]).expect("IPC encoding succeeds")
        };
        drop(grpc);

        crate::telemetry::metrics::test_support::flush_metrics();
        let finished = metric_exporter
            .get_finished_metrics()
            .expect("finished metrics");
        let metric =
            find_metric(&finished, "coral.query.result.ipc.encoded_size").expect("IPC size metric");
        let AggregatedMetrics::U64(MetricData::Histogram(histogram)) = metric.data() else {
            panic!("IPC size should be a u64 histogram");
        };
        let points = histogram.data_points().collect::<Vec<_>>();
        assert_eq!(points.len(), 1);
        let point = points.first().expect("single IPC metric point");
        assert_eq!(
            point.sum(),
            u64::try_from(bytes.len()).expect("payload length")
        );

        provider.force_flush().expect("flush spans");
        let spans = span_exporter.get_finished_spans().expect("finished spans");
        let grpc = find_span(&spans, "grpc.request");
        let query = find_span(&spans, "coral.query");
        let ipc = find_span(&spans, "coral.query.result.ipc");
        assert_eq!(query.parent_span_id, grpc.span_context.span_id());
        assert_eq!(ipc.parent_span_id, grpc.span_context.span_id());
        assert!(ipc.start_time >= query.end_time);
        assert_eq!(ipc.status, OtelStatus::Ok);
        assert_eq!(
            span_i64(ipc, "encoded_size_bytes"),
            Some(i64::try_from(bytes.len()).expect("payload length"))
        );
        for forbidden in ["sql", "workspace", "task.id", "source", "table", "user"] {
            assert!(!has_span_attribute(ipc, forbidden));
        }
    }

    #[test]
    fn ipc_encoding_failure_sets_error_and_omits_size_metric() {
        crate::telemetry::metrics::test_support::reset_metrics();
        let metric_exporter = crate::telemetry::metrics::test_support::install_metrics_exporter();
        let span_exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(span_exporter.clone())
            .build();
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("ipc-failure-test")));
        let _guard = tracing::subscriber::set_default(subscriber);

        let stream_schema = Arc::new(Schema::empty());
        assert!(
            observe_arrow_ipc_encoding(|| {
                serialize_arrow_ipc_stream(FinishFailWriter::default(), &stream_schema, &[])
                    .map(|_| Vec::new())
            })
            .is_err(),
            "forced flush failure should make StreamWriter::finish fail"
        );

        crate::telemetry::metrics::test_support::flush_metrics();
        let finished = metric_exporter
            .get_finished_metrics()
            .expect("finished metrics");
        assert!(
            find_metric(&finished, "coral.query.result.ipc.encoded_size").is_none(),
            "failed IPC encoding must omit the histogram"
        );

        provider.force_flush().expect("flush spans");
        let spans = span_exporter.get_finished_spans().expect("finished spans");
        let ipc = find_span(&spans, "coral.query.result.ipc");
        assert!(matches!(ipc.status, OtelStatus::Error { .. }));
        assert!(span_i64(ipc, "encoded_size_bytes").is_none());
    }

    #[test]
    fn ipc_size_conversion_failure_omits_numeric_telemetry() {
        crate::telemetry::metrics::test_support::reset_metrics();
        let metric_exporter = crate::telemetry::metrics::test_support::install_metrics_exporter();
        let span_exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(span_exporter.clone())
            .build();
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("ipc-size-test")));
        let _guard = tracing::subscriber::set_default(subscriber);

        let bytes = observe_arrow_ipc_encoding_with_size(|| Ok(vec![1, 2, 3]), |_| None)
            .expect("encoding still succeeds when its size cannot be represented");
        assert_eq!(bytes, vec![1, 2, 3]);

        crate::telemetry::metrics::test_support::flush_metrics();
        let finished = metric_exporter
            .get_finished_metrics()
            .expect("finished metrics");
        assert!(
            find_metric(&finished, "coral.query.result.ipc.encoded_size").is_none(),
            "unrepresentable IPC size must omit the histogram"
        );

        provider.force_flush().expect("flush spans");
        let spans = span_exporter.get_finished_spans().expect("finished spans");
        let ipc = find_span(&spans, "coral.query.result.ipc");
        assert_eq!(ipc.status, OtelStatus::Ok);
        assert!(span_i64(ipc, "encoded_size_bytes").is_none());
    }

    fn find_metric<'a>(
        metrics: &'a [ResourceMetrics],
        name: &str,
    ) -> Option<&'a opentelemetry_sdk::metrics::data::Metric> {
        metrics
            .iter()
            .rev()
            .flat_map(ResourceMetrics::scope_metrics)
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .find(|metric| metric.name() == name)
    }

    fn find_span<'a>(
        spans: &'a [opentelemetry_sdk::trace::SpanData],
        name: &str,
    ) -> &'a opentelemetry_sdk::trace::SpanData {
        spans
            .iter()
            .find(|span| span.name == name)
            .unwrap_or_else(|| panic!("span {name} missing"))
    }

    fn span_i64(span: &opentelemetry_sdk::trace::SpanData, name: &str) -> Option<i64> {
        span.attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == name)
            .and_then(|attribute| match attribute.value {
                opentelemetry::Value::I64(value) => Some(value),
                _ => None,
            })
    }

    fn has_span_attribute(span: &opentelemetry_sdk::trace::SpanData, name: &str) -> bool {
        span.attributes
            .iter()
            .any(|attribute| attribute.key.as_str() == name)
    }
}
