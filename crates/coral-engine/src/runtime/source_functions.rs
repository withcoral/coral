//! Source-scoped table function relation planning.
//!
//! Coral exposes source functions as scoped SQL relations like
//! `github.find_issues(...)`. The relation planner intercepts those calls,
//! validates the named-argument syntax, and parks the call as a
//! [`SourceFunctionNode`] logical-plan extension. Argument values stay logical
//! expressions long enough for `DataFusion` query parameters
//! (`owner => $owner`) to bind into them; [`SourceFunctionAnalyzerRule`] then
//! resolves each fully-bound node into an ordinary provider table scan before
//! optimization, so projection and limit pushdown behave exactly as they do
//! for any registered table.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, DFSchemaRef, TableReference};
use datafusion::datasource::provider_as_source;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion::logical_expr::sqlparser::ast::TableFactor;
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;

use crate::backends::{
    RegisteredTableFunction, RegisteredTableFunctionArgument, SourceFunctionProviderFactory,
};
use crate::runtime::scoped_table_functions::{
    ScopedTableFunctionCall, ScopedTableFunctionName, ScopedTableFunctionSignature, call_parts,
    find_placeholder, lower_named_args_to_positional_exprs, original_relation, qualified_name,
    reject_settings, reject_unsupported_modifiers,
};
use coral_spec::ManifestDataType;

pub(crate) const SOURCE_FUNCTION_NODE_NAME: &str = "CoralSourceFunction";
const SOURCE_FUNCTION_ANALYZER_RULE_NAME: &str = "coral_source_functions";

#[derive(Debug)]
pub(crate) struct SourceFunctionRegistry {
    functions: HashMap<ScopedTableFunctionName, SourceFunction>,
    source_schemas: HashSet<String>,
}

impl SourceFunctionRegistry {
    pub(crate) fn new<'a>(
        functions: impl IntoIterator<Item = &'a RegisteredTableFunction>,
    ) -> Self {
        let mut source_schemas = HashSet::new();
        let mut functions_by_name = HashMap::new();

        for function in functions {
            let lookup_key = ScopedTableFunctionName::from_manifest(function);
            source_schemas.insert(lookup_key.schema.clone());
            functions_by_name.insert(lookup_key, SourceFunction::from_registered(function));
        }

        Self {
            functions: functions_by_name,
            source_schemas,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.functions.is_empty()
    }

    /// Installs source-function planning and binding for one session.
    ///
    /// The two hooks are a pair: any session that can plan source-function
    /// calls must also be able to bind parked [`SourceFunctionNode`] plans.
    pub(crate) fn install(self, ctx: &SessionContext) -> Result<()> {
        self.install_relation_planner(ctx)?;
        Self::install_analyzer(ctx);
        Ok(())
    }

    /// Installs only the relation planner that parks source-function calls.
    ///
    /// Use this only when the caller installs the analyzer separately for the
    /// same session.
    pub(crate) fn install_relation_planner(self, ctx: &SessionContext) -> Result<()> {
        ctx.register_relation_planner(Arc::new(self))
    }

    /// Installs the analyzer that resolves parked source-function calls.
    ///
    /// `DataFusion` appends analyzer rules, so keep this idempotent for callers
    /// that share one session across source-function and UDF planning hooks.
    pub(crate) fn install_analyzer(ctx: &SessionContext) {
        let state_ref = ctx.state_ref();
        let mut state = state_ref.write();
        if state
            .analyzer()
            .rules
            .iter()
            .any(|rule| rule.name() == SOURCE_FUNCTION_ANALYZER_RULE_NAME)
        {
            return;
        }
        state.add_analyzer_rule(Arc::new(SourceFunctionAnalyzerRule));
    }

    fn find(&self, call: &ScopedTableFunctionCall) -> Option<&SourceFunction> {
        self.functions.get(&call.lookup_key)
    }

    fn owns_schema(&self, call: &ScopedTableFunctionCall) -> bool {
        self.source_schemas.contains(&call.lookup_key.schema)
    }

    fn available_functions_hint(&self, schema: &str) -> String {
        let mut names: Vec<&str> = self
            .functions
            .iter()
            .filter_map(|(key, function)| {
                (key.schema == schema).then_some(function.display_name.as_str())
            })
            .collect();
        names.sort_unstable();

        if names.is_empty() {
            String::new()
        } else {
            format!("; available functions: {}", names.join(", "))
        }
    }
}

impl RelationPlanner for SourceFunctionRegistry {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        let Some(call) = ScopedTableFunctionCall::parse(&relation, context) else {
            return Ok(original_relation(relation));
        };

        let Some(function) = self.find(&call) else {
            if self.owns_schema(&call) {
                let hint = self.available_functions_hint(&call.lookup_key.schema);
                return Err(call.unknown_function_error("source table function", &hint));
            }
            return Ok(original_relation(relation));
        };

        reject_unsupported_modifiers(&call, &relation)?;
        let (call_args, alias) = call_parts(relation);
        reject_settings(&call, &call_args)?;

        let lowered_args = lower_named_args_to_positional_exprs(function, &call_args, context)?;
        let node = SourceFunctionNode::new(function, lowered_args)?;

        // Fully-literal calls validate eagerly so argument-value errors keep
        // surfacing at planning time, exactly as they did when binding ran
        // inside SQL planning. Binding is pure value capture (no I/O), so the
        // analyzer repeating it later is cheap.
        if !node.has_parameter_placeholders() {
            node.factory.provider_for_args(&node.call_args)?;
        }

        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            plan, alias,
        ))))
    }
}

#[derive(Debug)]
struct SourceFunction {
    display_name: String,
    table_reference: TableReference,
    arguments: Vec<SourceFunctionArgument>,
    factory: Arc<dyn SourceFunctionProviderFactory>,
}

impl SourceFunction {
    fn from_registered(function: &RegisteredTableFunction) -> Self {
        let arguments = function
            .arguments
            .iter()
            .map(SourceFunctionArgument::from_registered)
            .collect::<Vec<_>>();
        Self {
            display_name: qualified_name(&function.schema_name, &function.function_name),
            table_reference: TableReference::partial(
                function.schema_name.clone(),
                function.function_name.clone(),
            ),
            arguments,
            factory: Arc::clone(&function.factory),
        }
    }

    fn contains(&self, name: &str) -> bool {
        self.arguments.iter().any(|argument| argument.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SourceFunctionArgument {
    pub(crate) name: String,
    pub(crate) data_type: ManifestDataType,
}

impl SourceFunctionArgument {
    fn from_registered(argument: &RegisteredTableFunctionArgument) -> Self {
        Self {
            name: argument.name.clone(),
            data_type: argument.data_type,
        }
    }
}

impl ScopedTableFunctionSignature for SourceFunction {
    fn display_name(&self) -> &str {
        &self.display_name
    }

    fn arg_count(&self) -> usize {
        self.arguments.len()
    }

    fn arg_name(&self, index: usize) -> Option<&str> {
        self.arguments
            .get(index)
            .map(|argument| argument.name.as_str())
    }

    fn contains(&self, name: &str) -> bool {
        self.contains(name)
    }
}

/// One parked source-function call inside a logical plan.
///
/// The call's arguments are exposed through
/// [`UserDefinedLogicalNodeCore::expressions`], which is what lets
/// `DataFrame::with_param_values` rewrite `$name` placeholders inside the node
/// before [`SourceFunctionAnalyzerRule`] binds the call.
///
/// The node snapshots everything it needs from its [`SourceFunction`] registry
/// entry: plans are `'static` and outlive planning, so the node cannot borrow
/// from the registry.
#[derive(Debug, Clone)]
pub(crate) struct SourceFunctionNode {
    display_name: String,
    /// Two-part `schema.function` reference, so result columns qualify the
    /// same way table columns do (`github.pulls.id`, `pulls.id`).
    table_reference: TableReference,
    declared_args: Vec<SourceFunctionArgument>,
    call_args: Vec<Expr>,
    schema: DFSchemaRef,
    factory: Arc<dyn SourceFunctionProviderFactory>,
}

impl SourceFunctionNode {
    fn new(function: &SourceFunction, call_args: Vec<Expr>) -> Result<Self> {
        let schema = Arc::new(DFSchema::try_from_qualified_schema(
            function.table_reference.clone(),
            function.factory.schema().as_ref(),
        )?);
        Ok(Self {
            display_name: function.display_name.clone(),
            table_reference: function.table_reference.clone(),
            declared_args: function.arguments.clone(),
            call_args,
            schema,
            factory: Arc::clone(&function.factory),
        })
    }

    fn has_parameter_placeholders(&self) -> bool {
        self.call_args
            .iter()
            .any(|arg| find_placeholder(arg).is_some())
    }

    pub(crate) fn table_reference(&self) -> &TableReference {
        &self.table_reference
    }

    pub(crate) fn display_name(&self) -> &str {
        &self.display_name
    }

    pub(crate) fn declared_args_with_call_exprs(
        &self,
    ) -> impl Iterator<Item = (&SourceFunctionArgument, &Expr)> {
        self.declared_args.iter().zip(&self.call_args)
    }

    fn reject_unbound_parameters(&self) -> Result<()> {
        for (declared_arg, call_expr) in self.declared_args_with_call_exprs() {
            if let Some(placeholder) = find_placeholder(call_expr) {
                return Err(DataFusionError::Plan(format!(
                    "{} argument '{}' is bound to parameter {placeholder}, \
                     but no value was provided for it",
                    self.display_name, declared_arg.name
                )));
            }
        }
        Ok(())
    }

    fn to_provider_scan(&self) -> Result<LogicalPlan> {
        self.reject_unbound_parameters()?;
        let provider = self.factory.provider_for_args(&self.call_args)?;
        LogicalPlanBuilder::scan(
            self.table_reference.clone(),
            provider_as_source(provider),
            None,
        )?
        .build()
    }
}

// Node identity is the function name plus its declared argument signature and
// call expressions. `schema` participates so renamed manifests never compare
// equal. The factory is derived from the same registry entry as `display_name`
// and cannot implement `PartialEq`, so it is deliberately excluded.
impl PartialEq for SourceFunctionNode {
    fn eq(&self, other: &Self) -> bool {
        self.display_name == other.display_name
            && self.call_args == other.call_args
            && self.declared_args == other.declared_args
            && self.schema == other.schema
    }
}

impl Eq for SourceFunctionNode {}

impl Hash for SourceFunctionNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.display_name.hash(state);
        self.call_args.hash(state);
        self.declared_args.hash(state);
        self.schema.hash(state);
    }
}

impl PartialOrd for SourceFunctionNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self == other {
            return Some(Ordering::Equal);
        }
        Some(format!("{self:?}").cmp(&format!("{other:?}")))
    }
}

impl UserDefinedLogicalNodeCore for SourceFunctionNode {
    fn name(&self) -> &'static str {
        SOURCE_FUNCTION_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        Vec::new()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.call_args.clone()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SourceFunction: {}", self.display_name)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !inputs.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "source function {} takes no plan inputs",
                self.display_name
            )));
        }
        if exprs.len() != self.call_args.len() {
            return Err(DataFusionError::Plan(format!(
                "source function {} expected {} argument expressions, got {}",
                self.display_name,
                self.call_args.len(),
                exprs.len()
            )));
        }
        // Plan rewrites (parameter substitution in particular) alias rewritten
        // expressions to preserve their display names. Arguments are
        // positional here, so the alias carries no meaning -- strip it before
        // binding sees the value.
        Ok(Self {
            call_args: exprs.into_iter().map(Expr::unalias).collect(),
            ..self.clone()
        })
    }
}

/// Resolves parked [`SourceFunctionNode`]s into provider table scans.
///
/// Runs after `DataFrame::with_param_values` has bound query parameters and
/// before the optimizer, so the optimized plan is the same provider scan the
/// engine has always produced and pushdown rules apply unchanged.
#[derive(Debug, Default)]
pub(crate) struct SourceFunctionAnalyzerRule;

impl AnalyzerRule for SourceFunctionAnalyzerRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        // Subquery plans live inside expressions, not in the plan's child
        // list -- a plain transform_up would never see a source-function call
        // written inside EXISTS/IN/scalar subqueries.
        plan.transform_up_with_subqueries(|plan| {
            let LogicalPlan::Extension(extension) = &plan else {
                return Ok(Transformed::no(plan));
            };
            let Some(node) = extension.node.as_any().downcast_ref::<SourceFunctionNode>() else {
                return Ok(Transformed::no(plan));
            };
            Ok(Transformed::yes(node.to_provider_scan()?))
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str {
        SOURCE_FUNCTION_ANALYZER_RULE_NAME
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_analyzer_is_idempotent() {
        let ctx = SessionContext::new();

        SourceFunctionRegistry::install_analyzer(&ctx);
        SourceFunctionRegistry::install_analyzer(&ctx);

        let count = ctx
            .state()
            .analyzer()
            .rules
            .iter()
            .filter(|rule| rule.name() == SOURCE_FUNCTION_ANALYZER_RULE_NAME)
            .count();
        assert_eq!(count, 1);
    }
}
