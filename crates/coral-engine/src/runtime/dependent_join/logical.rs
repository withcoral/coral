use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use datafusion::common::{DFSchemaRef, Result, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

/// Logical extension node reserved for dependent predicate pushdown plans.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct DependentJoinNode {
    resolver: LogicalPlan,
    schema: DFSchemaRef,
}

impl PartialOrd for DependentJoinNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self == other {
            return Some(Ordering::Equal);
        }

        match self.resolver.partial_cmp(&other.resolver) {
            Some(Ordering::Equal) => {}
            ordering => return ordering,
        }

        let schema_ordering = format!("{:?}", self.schema).cmp(&format!("{:?}", other.schema));
        if schema_ordering != Ordering::Equal {
            return Some(schema_ordering);
        }

        Some((Arc::as_ptr(&self.schema) as usize).cmp(&(Arc::as_ptr(&other.schema) as usize)))
    }
}

impl UserDefinedLogicalNodeCore for DependentJoinNode {
    fn name(&self) -> &'static str {
        "DependentJoinNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.resolver]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DependentJoinNode")
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return plan_err!("DependentJoinNode expects exactly one input");
        }

        Ok(Self {
            resolver: inputs.into_iter().next().expect("input length was checked"),
            schema: self.schema.clone(),
        })
    }
}
