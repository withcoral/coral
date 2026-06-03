use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{Result, plan_err};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use crate::runtime::dependent_join::logical::DependentJoinNode;

#[derive(Debug, Default)]
pub(crate) struct DependentJoinExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for DependentJoinExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if node.as_any().downcast_ref::<DependentJoinNode>().is_none() {
            return Ok(None);
        }

        if physical_inputs.len() != 1 {
            return plan_err!("DependentJoinNode expected one physical resolver input");
        }

        plan_err!("DependentJoinNode physical execution is not implemented")
    }
}
