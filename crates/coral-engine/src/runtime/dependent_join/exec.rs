use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use coral_spec::backends::http::HttpTableSpec;
use datafusion::common::{Result, Statistics, plan_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{EmissionType, ExecutionPlanProperties};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::{StreamExt, stream};

use crate::backends::http::HttpSourceClient;
use crate::runtime::dependent_join::bindings::BindingProjector;
use crate::runtime::dependent_join::driver::run_binding_phase;
use crate::runtime::dependent_join::fetcher::{BindingFetcher, BindingFetcherConfig};
use crate::runtime::dependent_join::logical::BindingKey;
use crate::runtime::dependent_join::output::{BuildJoinedBatchesConfig, build_joined_batches};
use crate::runtime::dependent_join::state::{DependentJoinRuntimeState, ResolverCaps};

pub(crate) struct DependentJoinExec {
    resolver: Arc<dyn ExecutionPlan>,
    dependent: HttpSourceClient,
    dependent_source_schema: String,
    table: Arc<HttpTableSpec>,
    binding_keys: Arc<[BindingKey]>,
    literal_filters: Arc<BTreeMap<String, String>>,
    dependent_projection: Arc<[usize]>,
    resolver_projection_len: usize,
    dependent_first: bool,
    max_bindings: usize,
    max_resolver_rows: usize,
    max_rows_per_binding: usize,
    max_resolver_rows_per_binding: usize,
    max_concurrency: usize,
    page_hint: Option<usize>,
    output_schema: SchemaRef,
    props: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

pub(crate) struct DependentJoinExecConfig {
    pub(crate) resolver: Arc<dyn ExecutionPlan>,
    pub(crate) dependent: HttpSourceClient,
    pub(crate) dependent_source_schema: String,
    pub(crate) table: Arc<HttpTableSpec>,
    pub(crate) binding_keys: Arc<[BindingKey]>,
    pub(crate) literal_filters: Arc<BTreeMap<String, String>>,
    pub(crate) dependent_projection: Arc<[usize]>,
    pub(crate) resolver_projection_len: usize,
    pub(crate) dependent_first: bool,
    pub(crate) max_bindings: usize,
    pub(crate) max_resolver_rows: usize,
    pub(crate) max_rows_per_binding: usize,
    pub(crate) max_resolver_rows_per_binding: usize,
    pub(crate) max_concurrency: usize,
    pub(crate) page_hint: Option<usize>,
    pub(crate) output_schema: SchemaRef,
}

impl DependentJoinExec {
    pub(crate) fn new(config: DependentJoinExecConfig) -> Self {
        let props = Arc::new(dependent_join_properties(
            &config.output_schema,
            &config.resolver,
        ));

        Self {
            resolver: config.resolver,
            dependent: config.dependent,
            dependent_source_schema: config.dependent_source_schema,
            table: config.table,
            binding_keys: config.binding_keys,
            literal_filters: config.literal_filters,
            dependent_projection: config.dependent_projection,
            resolver_projection_len: config.resolver_projection_len,
            dependent_first: config.dependent_first,
            max_bindings: config.max_bindings,
            max_resolver_rows: config.max_resolver_rows,
            max_rows_per_binding: config.max_rows_per_binding,
            max_resolver_rows_per_binding: config.max_resolver_rows_per_binding,
            max_concurrency: config.max_concurrency,
            page_hint: config.page_hint,
            output_schema: config.output_schema,
            props,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn with_resolver(&self, resolver: Arc<dyn ExecutionPlan>) -> Self {
        let props = Arc::new(dependent_join_properties(&self.output_schema, &resolver));

        Self {
            resolver,
            dependent: self.dependent.clone(),
            dependent_source_schema: self.dependent_source_schema.clone(),
            table: Arc::clone(&self.table),
            binding_keys: Arc::clone(&self.binding_keys),
            literal_filters: Arc::clone(&self.literal_filters),
            dependent_projection: Arc::clone(&self.dependent_projection),
            resolver_projection_len: self.resolver_projection_len,
            dependent_first: self.dependent_first,
            max_bindings: self.max_bindings,
            max_resolver_rows: self.max_resolver_rows,
            max_rows_per_binding: self.max_rows_per_binding,
            max_resolver_rows_per_binding: self.max_resolver_rows_per_binding,
            max_concurrency: self.max_concurrency,
            page_hint: self.page_hint,
            output_schema: Arc::clone(&self.output_schema),
            props,
            metrics: self.metrics.clone(),
        }
    }
}

fn dependent_join_properties(
    output_schema: &SchemaRef,
    resolver: &Arc<dyn ExecutionPlan>,
) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(Arc::clone(output_schema)),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        resolver.boundedness(),
    )
}

#[derive(Clone)]
struct DependentJoinMetrics {
    binding_count: Count,
    fetch_count: Count,
    resolver_rows: Count,
    resolver_null_binding_rows: Count,
    dependent_rows_returned: Count,
}

impl DependentJoinMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            binding_count: MetricBuilder::new(metrics).counter("binding_count", partition),
            fetch_count: MetricBuilder::new(metrics).counter("fetch_count", partition),
            resolver_rows: MetricBuilder::new(metrics).counter("resolver_rows", partition),
            resolver_null_binding_rows: MetricBuilder::new(metrics)
                .counter("resolver_null_binding_rows", partition),
            dependent_rows_returned: MetricBuilder::new(metrics)
                .counter("dependent_rows_returned", partition),
        }
    }

    fn record(&self, state: &crate::runtime::dependent_join::state::DependentJoinRuntimeState) {
        self.binding_count.add(state.distinct_tuples());
        self.fetch_count.add(state.fetch_count());
        self.resolver_rows.add(state.resolver_rows());
        self.resolver_null_binding_rows
            .add(state.resolver_null_binding_rows());
        self.dependent_rows_returned
            .add(state.dependent_rows_returned());
    }
}

impl fmt::Debug for DependentJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DependentJoinExec")
            .field("source", &self.dependent_source_schema)
            .field("table", &self.table.name())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DependentJoinExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DependentJoinExec: table={}.{}, binding_keys={}, literal_filters={}, max_bindings={}, max_resolver_rows={}, max_rows_per_binding={}, max_resolver_rows_per_binding={}, max_concurrency={}, page_hint={}",
            self.dependent_source_schema,
            self.table.name(),
            format_binding_keys(&self.binding_keys),
            format_literal_filters(&self.literal_filters),
            self.max_bindings,
            self.max_resolver_rows,
            self.max_rows_per_binding,
            self.max_resolver_rows_per_binding,
            self.max_concurrency,
            format_page_hint(self.page_hint),
        )
    }
}

fn format_binding_keys(binding_keys: &[BindingKey]) -> String {
    let rendered = binding_keys
        .iter()
        .map(|key| format!("{} <- {}", key.dependent_filter, key.resolver_column))
        .collect::<Vec<_>>()
        .join(", ");

    format!("[{rendered}]")
}

fn format_literal_filters(filters: &BTreeMap<String, String>) -> String {
    if filters.is_empty() {
        return "{}".to_string();
    }

    let rendered = filters
        .iter()
        .map(|(name, value)| format!("{name}={value:?}"))
        .collect::<Vec<_>>()
        .join(", ");

    format!("{{{rendered}}}")
}

fn format_page_hint(page_hint: Option<usize>) -> String {
    page_hint.map_or_else(|| "None".to_string(), |value| value.to_string())
}

impl ExecutionPlan for DependentJoinExec {
    fn name(&self) -> &'static str {
        "DependentJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.resolver]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!("DependentJoinExec expects one resolver child");
        }

        Ok(Arc::new(
            self.with_resolver(
                children
                    .into_iter()
                    .next()
                    .expect("child length was checked"),
            ),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return plan_err!("DependentJoinExec has one output partition, got {partition}");
        }

        let resolver = Arc::clone(&self.resolver);
        let resolver_partition_count = resolver
            .properties()
            .output_partitioning()
            .partition_count();
        let dependent = self.dependent.clone();
        let dependent_source_schema = self.dependent_source_schema.clone();
        let table = Arc::clone(&self.table);
        let binding_keys = Arc::clone(&self.binding_keys);
        let dependent_projection = Arc::clone(&self.dependent_projection);
        let resolver_projection_len = self.resolver_projection_len;
        let binding_filters = binding_keys
            .iter()
            .map(|key| key.dependent_filter.clone())
            .collect::<Vec<_>>();
        let literal_filters = Arc::clone(&self.literal_filters);
        let dependent_first = self.dependent_first;
        let caps = ResolverCaps {
            source_schema: dependent_source_schema.clone(),
            table: table.name().to_string(),
            max_bindings: self.max_bindings,
            max_resolver_rows: self.max_resolver_rows,
            max_resolver_rows_per_binding: self.max_resolver_rows_per_binding,
            binding_filters: binding_filters.clone(),
        };
        let max_concurrency = self.max_concurrency;
        let max_rows_per_binding = self.max_rows_per_binding;
        let page_hint = self.page_hint;
        let output_schema = Arc::clone(&self.output_schema);
        let stream_schema = Arc::clone(&self.output_schema);
        let metrics = DependentJoinMetrics::new(&self.metrics, partition);

        let output = stream::once(async move {
            execute_dependent_join(
                resolver,
                resolver_partition_count,
                context,
                dependent,
                dependent_source_schema,
                table,
                binding_keys,
                binding_filters,
                literal_filters,
                dependent_projection,
                resolver_projection_len,
                dependent_first,
                caps,
                max_concurrency,
                max_rows_per_binding,
                page_hint,
                metrics,
                output_schema,
            )
            .await
        })
        .flat_map(|result| match result {
            Ok(batches) => stream::iter(batches.into_iter().map(Ok)).boxed(),
            Err(error) => stream::iter(vec![Err(error)]).boxed(),
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            output,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "ExecutionPlan::execute clones immutable config fields before entering the async boundary"
)]
async fn execute_dependent_join(
    resolver: Arc<dyn ExecutionPlan>,
    resolver_partition_count: usize,
    context: Arc<TaskContext>,
    dependent: HttpSourceClient,
    dependent_source_schema: String,
    table: Arc<HttpTableSpec>,
    binding_keys: Arc<[BindingKey]>,
    binding_filters: Vec<String>,
    literal_filters: Arc<BTreeMap<String, String>>,
    dependent_projection: Arc<[usize]>,
    resolver_projection_len: usize,
    dependent_first: bool,
    caps: ResolverCaps,
    max_concurrency: usize,
    max_rows_per_binding: usize,
    page_hint: Option<usize>,
    metrics: DependentJoinMetrics,
    output_schema: SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let projector = BindingProjector::new(binding_keys);
    let mut state = DependentJoinRuntimeState::default();
    let mut tuples = Vec::new();

    for resolver_partition in 0..resolver_partition_count {
        let mut resolver_stream = resolver.execute(resolver_partition, Arc::clone(&context))?;
        while let Some(batch) = resolver_stream.next().await.transpose()? {
            tuples.extend(state.ingest_resolver_batch(&batch, &projector, &caps)?);
        }
    }

    let fetcher = BindingFetcher::new(BindingFetcherConfig {
        client: dependent,
        source_schema: dependent_source_schema.clone(),
        table: Arc::clone(&table),
        binding_filters: Arc::from(binding_filters.clone()),
        literal_filters: Arc::clone(&literal_filters),
        max_concurrency,
        max_rows_per_binding,
        page_hint,
    });
    let state = run_binding_phase(state, tuples, &fetcher).await?;
    metrics.record(&state);

    build_joined_batches(BuildJoinedBatchesConfig {
        state: &state,
        dependent_source_schema: &dependent_source_schema,
        dependent_table: &table,
        binding_filters: &binding_filters,
        literal_filters: &literal_filters,
        dependent_projection: &dependent_projection,
        resolver_projection_len,
        dependent_first,
        output_schema: &output_schema,
    })
}
