use std::fmt;

use datafusion::common::Result;
use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

/// Optimizer rule shell for dependent predicate pushdown.
///
/// This rule is intentionally inert until the physical dependent join executor
/// exists. Rewriting supported joins before execution is available would turn
/// otherwise-valid fallback plans into runtime failures.
#[derive(Default)]
pub(crate) struct DependentJoinOptimizerRule;

impl fmt::Debug for DependentJoinOptimizerRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DependentJoinOptimizerRule").finish()
    }
}

impl OptimizerRule for DependentJoinOptimizerRule {
    fn name(&self) -> &'static str {
        "dependent_join_pushdown"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        Ok(Transformed::no(plan))
    }
}

pub(crate) fn rule() -> DependentJoinOptimizerRule {
    DependentJoinOptimizerRule
}

#[cfg(test)]
mod tests {
    use super::DependentJoinOptimizerRule;
    use datafusion::optimizer::OptimizerRule;

    #[test]
    fn rule_is_registered_under_stable_name() {
        let rule = DependentJoinOptimizerRule;
        assert_eq!(rule.name(), "dependent_join_pushdown");
    }
}
