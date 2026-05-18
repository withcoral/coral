use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};

use coral_spec::WireType;
use datafusion::common::{Column, DFSchemaRef, Result, TableReference, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

/// Logical extension node reserved for dependent predicate pushdown plans.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct DependentJoinNode {
    pub(crate) resolver: LogicalPlan,
    pub(crate) dependent_table: TableReference,
    pub(crate) binding_keys: Vec<BindingKey>,
    pub(crate) literal_filters: BTreeMap<String, String>,
    pub(crate) dependent_projection: Vec<usize>,
    pub(crate) resolver_projection_len: usize,
    pub(crate) dependent_first: bool,
    pub(crate) schema: DFSchemaRef,
    pub(crate) max_bindings: usize,
    pub(crate) max_resolver_rows: usize,
    pub(crate) max_rows_per_binding: usize,
    pub(crate) max_resolver_rows_per_binding: usize,
    pub(crate) max_concurrency: usize,
    pub(crate) page_hint: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BindingKey {
    pub(crate) resolver_column: Column,
    pub(crate) resolver_binding_name: String,
    pub(crate) dependent_filter: String,
    pub(crate) wire_type: WireType,
}

impl Hash for BindingKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.resolver_column.hash(state);
        self.resolver_binding_name.hash(state);
        self.dependent_filter.hash(state);
        match self.wire_type {
            WireType::String => "string".hash(state),
        }
    }
}

impl PartialOrd for DependentJoinNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self == other {
            return Some(std::cmp::Ordering::Equal);
        }

        Some(format!("{self:?}").cmp(&format!("{other:?}")))
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
            dependent_table: self.dependent_table.clone(),
            binding_keys: self.binding_keys.clone(),
            literal_filters: self.literal_filters.clone(),
            dependent_projection: self.dependent_projection.clone(),
            resolver_projection_len: self.resolver_projection_len,
            dependent_first: self.dependent_first,
            schema: self.schema.clone(),
            max_bindings: self.max_bindings,
            max_resolver_rows: self.max_resolver_rows,
            max_rows_per_binding: self.max_rows_per_binding,
            max_resolver_rows_per_binding: self.max_resolver_rows_per_binding,
            max_concurrency: self.max_concurrency,
            page_hint: self.page_hint,
        })
    }
}
