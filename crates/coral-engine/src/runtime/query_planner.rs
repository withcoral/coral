use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};

/// Query planner that installs Coral-owned `DataFusion` extension planners while
/// leaving ordinary physical planning delegated to `DataFusion`.
pub(crate) struct CoralQueryPlanner {
    extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

impl CoralQueryPlanner {
    pub(crate) fn new(extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>) -> Self {
        Self { extension_planners }
    }
}

impl fmt::Debug for CoralQueryPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoralQueryPlanner")
            .field("extension_planner_count", &self.extension_planners.len())
            .finish()
    }
}

#[async_trait]
impl QueryPlanner for CoralQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner =
            DefaultPhysicalPlanner::with_extension_planners(self.extension_planners.clone());
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
