//! Shared `TableProvider` decorator that publishes scanned rows as source
//! observations.
//!
//! Backends whose tables are plain `Arc<dyn TableProvider>` values (file
//! today, database next) get observed-value capture by wrapping the provider
//! instead of threading publishers through every provider implementation.
//!
//! Two properties keep the decorator invisible to query results:
//!
//! - [`ObservedTableProvider::supports_filters_pushdown`] mirrors the inner
//!   provider's verdicts and upgrades `Unsupported` to `Inexact`. Both keep
//!   `DataFusion`'s own `FilterExec` above the scan, so plan shape and results
//!   are unchanged, but the predicates now reach `scan()` where the tap can
//!   use them to pick the rows worth observing.
//! - [`ObservingExec`] is a transparent proxy, not a new plan node: it
//!   delegates its name, display, plan properties, children, and every
//!   optimizer hook to the plan it wraps, rewrapping whenever an optimizer
//!   rewrites that plan. `DataFusion` added
//!   [`ExecutionPlan::downcast_delegate`] for exactly this shape, and
//!   `datafusion-tracing`'s `InstrumentedExec` is the reference
//!   implementation. A visible single-child node would instead hide the file
//!   scan from every rule that downcasts to `DataSourceExec`, costing filter
//!   pushdown, limit pushdown, and file-group repartitioning.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "wired by the file backend in the next PR of this stack"
    )
)]

use std::any::Any;
use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{ScanArgs, ScanResult, Session, TableProvider};
use datafusion::common::{Constraints, DFSchema, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{
    Expr, LogicalPlan, TableProviderFilterPushDown, TableType, dml::InsertOp,
};
use datafusion::physical_expr::{
    Distribution, OrderingRequirements, PhysicalSortExpr, create_physical_expr,
};
use datafusion::physical_plan::execution_plan::{CardinalityEffect, InvariantLevel};
use datafusion::physical_plan::filter::batch_filter;
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sort_pushdown::SortOrderPushdownResult;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
};
use futures::StreamExt as _;

use crate::backends::shared::source_observation::{
    SourceObservationConfig, publish_source_scan_batch,
};

/// Wraps a table provider so every scanned batch is published as a source
/// observation.
///
/// Returns `inner` untouched when `observation` is `None`, which is what an
/// engine built without observation publishers always sees: capture that is
/// off costs nothing, not even a passthrough wrapper.
pub(crate) fn observed_table_provider(
    inner: Arc<dyn TableProvider>,
    source_name: &str,
    surface_name: &str,
    observation: Option<SourceObservationConfig>,
) -> Arc<dyn TableProvider> {
    match observation {
        Some(observation) => Arc::new(ObservedTableProvider {
            inner,
            source_name: Arc::from(source_name),
            surface_name: Arc::from(surface_name),
            observation,
        }),
        None => inner,
    }
}

struct ObservedTableProvider {
    inner: Arc<dyn TableProvider>,
    source_name: Arc<str>,
    surface_name: Arc<str>,
    observation: SourceObservationConfig,
}

impl fmt::Debug for ObservedTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservedTableProvider")
            .field("source", &self.source_name)
            .field("surface", &self.surface_name)
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl ObservedTableProvider {
    /// Filters the inner provider is willing to receive.
    ///
    /// `supports_filters_pushdown` promises `DataFusion` more than the inner
    /// provider does, so the extra predicates must not reach a provider that
    /// declared it cannot use them.
    fn filters_for_inner(&self, filters: &[Expr]) -> Result<Vec<Expr>> {
        let candidates = filters.iter().collect::<Vec<_>>();
        let support = self.inner.supports_filters_pushdown(&candidates)?;
        if support.len() != filters.len() {
            return Err(DataFusionError::Internal(format!(
                "table provider for {}.{} returned {} pushdown verdicts for {} filters",
                self.source_name,
                self.surface_name,
                support.len(),
                filters.len()
            )));
        }
        Ok(filters
            .iter()
            .zip(support)
            .filter(|(_, support)| !matches!(support, TableProviderFilterPushDown::Unsupported))
            .map(|(filter, _)| filter.clone())
            .collect())
    }

    fn observe(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        filters: &[Expr],
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(ObservingExec {
            inner: plan,
            source_name: Arc::clone(&self.source_name),
            surface_name: Arc::clone(&self.surface_name),
            observation: self.observation.clone(),
            filters: Arc::from(filters.to_vec()),
            execution_props: state.execution_props().clone(),
        })
    }
}

#[async_trait]
impl TableProvider for ObservedTableProvider {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    /// Mirrors the inner provider and upgrades `Unsupported` to `Inexact`.
    ///
    /// `Inexact` and `Unsupported` both leave `DataFusion`'s `FilterExec`
    /// above the scan, so this changes no results; it only makes the
    /// predicates over this table visible to `scan()`.
    ///
    /// A predicate that spans several tables sits above the join, so it
    /// narrows what this table observes only where `DataFusion` can infer an
    /// equivalent single-table predicate from an equijoin. Anything it cannot
    /// infer leaves this table observing rows a later join discards.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(self
            .inner
            .supports_filters_pushdown(filters)?
            .into_iter()
            .map(|support| match support {
                TableProviderFilterPushDown::Unsupported => TableProviderFilterPushDown::Inexact,
                supported => supported,
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inner_filters = self.filters_for_inner(filters)?;
        let plan = self
            .inner
            .scan(state, projection, &inner_filters, limit)
            .await?;
        Ok(self.observe(plan, state, filters))
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> Result<ScanResult> {
        let filters = args.filters().unwrap_or(&[]).to_vec();
        let inner_filters = self.filters_for_inner(&filters)?;
        let inner_args = args.with_filters(Some(&inner_filters));
        let plan = self.inner.scan_with_args(state, inner_args).await?;
        Ok(ScanResult::new(self.observe(
            plan.into_inner(),
            state,
            &filters,
        )))
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.delete_from(state, filters).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.update(state, assignments, filters).await
    }

    async fn truncate(&self, state: &dyn Session) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.truncate(state).await
    }
}

/// Per-batch observation tap over a scan plan.
///
/// Every method that is not `execute` either delegates to the wrapped plan or
/// delegates and rewraps the result, so the optimizer sees the plan it would
/// have seen without observation.
struct ObservingExec {
    inner: Arc<dyn ExecutionPlan>,
    source_name: Arc<str>,
    surface_name: Arc<str>,
    observation: SourceObservationConfig,
    filters: Arc<[Expr]>,
    execution_props: ExecutionProps,
}

impl ObservingExec {
    fn with_new_inner(&self, inner: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            inner,
            source_name: Arc::clone(&self.source_name),
            surface_name: Arc::clone(&self.surface_name),
            observation: self.observation.clone(),
            filters: Arc::clone(&self.filters),
            execution_props: self.execution_props.clone(),
        })
    }
}

impl fmt::Debug for ObservingExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservingExec")
            .field("source", &self.source_name)
            .field("surface", &self.surface_name)
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for ObservingExec {
    fn fmt_as(&self, format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt_as(format, f)
    }
}

impl ExecutionPlan for ObservingExec {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn static_name() -> &'static str {
        "ObservingExec"
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        self.inner.check_invariants(check)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.inner.required_input_distribution()
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.inner.required_input_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.inner.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.inner.benefits_from_input_partitioning()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.inner.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.inner.supports_limit_pushdown()
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.inner.cardinality_effect()
    }

    fn gather_filters_for_pushdown(
        &self,
        phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        self.inner
            .gather_filters_for_pushdown(phase, parent_filters, config)
    }

    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        Some(self.inner.as_ref())
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inner = Arc::clone(&self.inner).with_new_children(children)?;
        Ok(self.with_new_inner(inner))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let inner = Arc::clone(&self.inner).reset_state()?;
        Ok(self.with_new_inner(inner))
    }

    fn with_new_state(&self, state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner
            .with_new_state(state)
            .map(|inner| self.with_new_inner(inner))
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(Arc::clone(&self.inner)
            .repartitioned(target_partitions, config)?
            .map(|inner| self.with_new_inner(inner)))
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner
            .with_fetch(limit)
            .map(|inner| self.with_new_inner(inner))
    }

    /// Passes the order-sensitivity signal down to the wrapped scan.
    ///
    /// `LimitPushdown` calls this right after `with_fetch` and treats `None` as
    /// "no order-preserving variant available", so a wrapper that swallows it
    /// leaves a parquet source free to skip row groups in an order the plan
    /// above already assumed was preserved.
    fn with_preserve_order(&self, preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner
            .with_preserve_order(preserve_order)
            .map(|inner| self.with_new_inner(inner))
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(Arc::clone(&self.inner)
            .try_swapping_with_projection(projection)?
            .map(|inner| self.with_new_inner(inner)))
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let FilterPushdownPropagation {
            filters,
            updated_node,
        } = self
            .inner
            .handle_child_pushdown_result(phase, child_pushdown_result, config)?;
        Ok(FilterPushdownPropagation {
            filters,
            updated_node: updated_node.map(|inner| self.with_new_inner(inner)),
        })
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        Ok(match self.inner.try_pushdown_sort(order)? {
            SortOrderPushdownResult::Exact { inner } => SortOrderPushdownResult::Exact {
                inner: self.with_new_inner(inner),
            },
            SortOrderPushdownResult::Inexact { inner } => SortOrderPushdownResult::Inexact {
                inner: self.with_new_inner(inner),
            },
            SortOrderPushdownResult::Unsupported => SortOrderPushdownResult::Unsupported,
        })
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.inner.execute(partition, context)?;
        let schema = stream.schema();
        let predicate = observation_predicate(&schema, &self.filters, &self.execution_props);
        let source_name = Arc::clone(&self.source_name);
        let surface_name = Arc::clone(&self.surface_name);
        let observation = self.observation.clone();
        let observed = stream.map(move |item| {
            if let Ok(batch) = &item {
                observe_batch(
                    &source_name,
                    &surface_name,
                    &observation,
                    predicate.as_ref(),
                    batch,
                );
            }
            item
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, observed)))
    }
}

/// Builds the predicate that selects the rows worth observing.
///
/// Built against the stream's own schema, so a scan whose projection was
/// rewritten after planning simply drops the predicates whose columns are no
/// longer there instead of failing to evaluate them. `None` means every
/// scanned row is observed.
fn observation_predicate(
    schema: &SchemaRef,
    filters: &[Expr],
    execution_props: &ExecutionProps,
) -> Option<Arc<dyn PhysicalExpr>> {
    let evaluable = filters
        .iter()
        .filter(|filter| {
            filter
                .column_refs()
                .iter()
                .all(|column| schema.index_of(column.name.as_str()).is_ok())
        })
        .cloned();
    let predicate = conjunction(evaluable)?;
    let built = DFSchema::try_from(Arc::clone(schema))
        .and_then(|df_schema| create_physical_expr(&predicate, &df_schema, execution_props));
    match built {
        Ok(predicate) => Some(predicate),
        Err(error) => {
            // Widening what a scan observes is the safe direction, but it
            // undoes the point of predicate-aware selection, so say so at a
            // level someone will see. Built once per partition, not per batch.
            tracing::warn!(
                error = %error,
                "failed to build the observed-row predicate; observing every scanned row"
            );
            None
        }
    }
}

fn observe_batch(
    source_name: &str,
    surface_name: &str,
    observation: &SourceObservationConfig,
    predicate: Option<&Arc<dyn PhysicalExpr>>,
    batch: &RecordBatch,
) {
    if batch.num_rows() == 0 {
        return;
    }
    let Some(predicate) = predicate else {
        publish_source_scan_batch(source_name, surface_name, observation, batch);
        return;
    };
    match batch_filter(batch, predicate) {
        Ok(selected) if selected.num_rows() > 0 => {
            publish_source_scan_batch(source_name, surface_name, observation, &selected);
        }
        Ok(_) => {}
        Err(error) => {
            tracing::debug!(
                source = source_name,
                surface = surface_name,
                error = %error,
                "failed to select observed rows; dropping source-scan observation"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::catalog::MemTable;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionContext, col, lit};
    use parquet::arrow::ArrowWriter;
    use tempfile::tempdir;

    use super::*;
    use crate::SourceObservationSurfaceKind;
    use crate::backends::shared::source_observation::{
        source_observation_publishers, test_support::RecordingSourceObservationPublisher,
    };

    const SOURCE: &str = "fixture";
    const SURFACE: &str = "events";

    fn events_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("kind", DataType::Utf8, false),
        ]))
    }

    fn events_batch() -> RecordBatch {
        RecordBatch::try_new(
            events_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alpha", "beta", "alpha"])),
            ],
        )
        .expect("events batch should build")
    }

    fn write_events_parquet(dir: &Path) {
        let file = std::fs::File::create(dir.join("events.parquet")).expect("parquet file");
        let mut writer = ArrowWriter::try_new(file, events_schema(), None).expect("parquet writer");
        writer.write(&events_batch()).expect("parquet write");
        writer.close().expect("parquet close");
    }

    /// A parquet table whose scan carries an output ordering, which is the
    /// plan property a wrapper is most likely to lose.
    async fn ordered_parquet_provider(ctx: &SessionContext, dir: &Path) -> Arc<dyn TableProvider> {
        write_events_parquet(dir);
        let url = ListingTableUrl::parse(
            url::Url::from_directory_path(dir)
                .expect("directory url")
                .as_str(),
        )
        .expect("listing table url");
        let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet")
            .with_file_sort_order(vec![vec![col("id").sort(true, false)]]);
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .infer_schema(&ctx.state())
            .await
            .expect("parquet schema inference");
        Arc::new(ListingTable::try_new(config).expect("listing table"))
    }

    fn recording_observation(
        publisher: &Arc<RecordingSourceObservationPublisher>,
    ) -> Option<SourceObservationConfig> {
        SourceObservationConfig::new(
            SourceObservationSurfaceKind::Table,
            source_observation_publishers(&[Arc::clone(publisher) as _]),
        )
    }

    async fn physical_plan_text(ctx: &SessionContext, sql: &str) -> String {
        let plan = ctx
            .sql(sql)
            .await
            .expect("query should plan")
            .create_physical_plan()
            .await
            .expect("physical plan");
        displayable(plan.as_ref()).indent(true).to_string()
    }

    async fn rendered_rows(ctx: &SessionContext, sql: &str) -> String {
        let batches = ctx
            .sql(sql)
            .await
            .expect("query should plan")
            .collect()
            .await
            .expect("query should execute");
        pretty_format_batches(&batches)
            .expect("batches should render")
            .to_string()
    }

    fn register(ctx: &SessionContext, provider: Arc<dyn TableProvider>) {
        ctx.register_table(SURFACE, provider)
            .expect("table registration");
    }

    #[tokio::test]
    async fn wrapping_preserves_the_plan_of_an_ordered_parquet_scan() {
        let dir = tempdir().expect("tempdir");
        let plain_ctx = SessionContext::new();
        let observed_ctx = SessionContext::new();
        let provider = ordered_parquet_provider(&plain_ctx, dir.path()).await;
        let publisher = Arc::new(RecordingSourceObservationPublisher::default());
        register(&plain_ctx, Arc::clone(&provider));
        register(
            &observed_ctx,
            observed_table_provider(provider, SOURCE, SURFACE, recording_observation(&publisher)),
        );

        let sql = "SELECT id FROM events WHERE kind = 'alpha' ORDER BY id";
        assert_eq!(
            physical_plan_text(&observed_ctx, sql).await,
            physical_plan_text(&plain_ctx, sql).await,
            "observing an ordered parquet scan must not change its plan"
        );
        assert_eq!(
            rendered_rows(&observed_ctx, sql).await,
            rendered_rows(&plain_ctx, sql).await
        );
    }

    #[tokio::test]
    async fn wrapping_a_provider_without_pushdown_preserves_the_plan() {
        let plain_ctx = SessionContext::new();
        let observed_ctx = SessionContext::new();
        let publisher = Arc::new(RecordingSourceObservationPublisher::default());
        register(&plain_ctx, memtable());
        register(
            &observed_ctx,
            observed_table_provider(
                memtable(),
                SOURCE,
                SURFACE,
                recording_observation(&publisher),
            ),
        );

        let sql = "SELECT id FROM events WHERE kind = 'alpha' ORDER BY id";
        assert_eq!(
            physical_plan_text(&observed_ctx, sql).await,
            physical_plan_text(&plain_ctx, sql).await,
            "upgrading Unsupported filters to Inexact must not change the plan"
        );
        assert_eq!(
            rendered_rows(&observed_ctx, sql).await,
            rendered_rows(&plain_ctx, sql).await
        );
    }

    fn memtable() -> Arc<dyn TableProvider> {
        Arc::new(MemTable::try_new(events_schema(), vec![vec![events_batch()]]).expect("mem table"))
    }

    #[tokio::test]
    async fn observes_only_the_rows_the_query_predicate_selects() {
        let ctx = SessionContext::new();
        let publisher = Arc::new(RecordingSourceObservationPublisher::default());
        register(
            &ctx,
            observed_table_provider(
                memtable(),
                SOURCE,
                SURFACE,
                recording_observation(&publisher),
            ),
        );

        rendered_rows(&ctx, "SELECT id, kind FROM events WHERE id = 2").await;

        let observations = publisher.observations();
        assert_eq!(observations.len(), 1);
        let observation = observations.first().expect("one observation");
        assert_eq!(observation.source_name, SOURCE);
        assert_eq!(observation.surface_name, SURFACE);
        assert_eq!(observation.row_count, 1);
        assert!(
            pretty_format_batches(std::slice::from_ref(&observation.batch))
                .expect("observation should render")
                .to_string()
                .contains("beta"),
            "the observed row should be the one the predicate selected"
        );
    }

    #[tokio::test]
    async fn observes_every_row_when_the_query_has_no_predicate() {
        let ctx = SessionContext::new();
        let publisher = Arc::new(RecordingSourceObservationPublisher::default());
        register(
            &ctx,
            observed_table_provider(
                memtable(),
                SOURCE,
                SURFACE,
                recording_observation(&publisher),
            ),
        );

        rendered_rows(&ctx, "SELECT id, kind FROM events").await;

        let observed_rows: usize = publisher
            .observations()
            .iter()
            .map(|observation| observation.row_count)
            .sum();
        assert_eq!(observed_rows, 3);
    }

    #[tokio::test]
    async fn observes_only_the_projected_columns() {
        let ctx = SessionContext::new();
        let publisher = Arc::new(RecordingSourceObservationPublisher::default());
        register(
            &ctx,
            observed_table_provider(
                memtable(),
                SOURCE,
                SURFACE,
                recording_observation(&publisher),
            ),
        );

        rendered_rows(&ctx, "SELECT kind FROM events").await;

        let observations = publisher.observations();
        assert!(!observations.is_empty(), "the scan should be observed");
        for observation in &observations {
            assert_eq!(observation.column_names, vec!["kind".to_string()]);
        }
    }

    /// `DataFusion`'s planner enters through `scan_with_args`, so every other
    /// test here exercises that path. `scan` is still a required trait method
    /// and the entry point for any caller that skips the planner, so it gets
    /// its own coverage rather than shipping untested.
    #[tokio::test]
    async fn scanning_through_the_plain_scan_entry_point_also_observes() {
        let ctx = SessionContext::new();
        let publisher = Arc::new(RecordingSourceObservationPublisher::default());
        let provider = observed_table_provider(
            memtable(),
            SOURCE,
            SURFACE,
            recording_observation(&publisher),
        );
        let predicate = col("kind").eq(lit("alpha"));

        let plan = provider
            .scan(&ctx.state(), None, std::slice::from_ref(&predicate), None)
            .await
            .expect("scan should plan");
        datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("scan should execute");

        let observed_rows: usize = publisher
            .observations()
            .iter()
            .map(|observation| observation.row_count)
            .sum();
        assert_eq!(
            observed_rows, 2,
            "scan() must apply the same predicate-aware selection as scan_with_args"
        );
    }

    #[test]
    fn without_publishers_the_provider_is_not_wrapped() {
        let inner = memtable();
        let observation =
            SourceObservationConfig::new(SourceObservationSurfaceKind::Table, Arc::from(vec![]));
        let wrapped = observed_table_provider(Arc::clone(&inner), SOURCE, SURFACE, observation);
        assert!(
            Arc::ptr_eq(&inner, &wrapped),
            "an engine without observation publishers must register the provider untouched"
        );
    }

    #[tokio::test]
    async fn wrapping_does_not_disturb_a_manifest_style_scan_with_no_matches() {
        let ctx = SessionContext::new();
        let publisher = Arc::new(RecordingSourceObservationPublisher::default());
        register(
            &ctx,
            observed_table_provider(
                memtable(),
                SOURCE,
                SURFACE,
                recording_observation(&publisher),
            ),
        );

        rendered_rows(&ctx, "SELECT id FROM events WHERE kind = 'missing'").await;

        assert!(
            publisher.observations().is_empty(),
            "a predicate that selects no row should publish nothing"
        );
    }
}
