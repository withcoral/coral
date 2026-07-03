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

use crate::backends::{RegisteredTableFunction, SourceFunctionProviderFactory};
use crate::runtime::scoped_table_functions::{
    ScopedTableFunctionCall, ScopedTableFunctionName, ScopedTableFunctionSignature, call_parts,
    find_placeholder, lower_named_args_to_positional_exprs, original_relation, qualified_name,
    reject_settings, reject_unsupported_modifiers,
};

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

    /// Installs this relation planner together with the analyzer rule that
    /// resolves the nodes it parks. The two are a pair: any session that can
    /// plan source-function calls must also be able to bind them.
    pub(crate) fn install(self, ctx: &SessionContext) -> Result<()> {
        ctx.register_relation_planner(Arc::new(self))?;
        ctx.add_analyzer_rule(Arc::new(SourceFunctionAnalyzerRule));
        Ok(())
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

        let args = lower_named_args_to_positional_exprs(function, &call_args, context)?;
        let node = SourceFunctionNode::new(function, args)?;

        // Fully-literal calls validate eagerly so argument-value errors keep
        // surfacing at planning time, exactly as they did when binding ran
        // inside SQL planning. Binding is pure value capture (no I/O), so the
        // analyzer repeating it later is cheap.
        if !node.has_parameter_placeholders() {
            node.factory.provider_for_args(&node.args)?;
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
    arg_names: Vec<String>,
    known_args: HashSet<String>,
    factory: Arc<dyn SourceFunctionProviderFactory>,
}

impl SourceFunction {
    fn from_registered(function: &RegisteredTableFunction) -> Self {
        let arg_names = function.arg_names.clone();
        Self {
            display_name: qualified_name(&function.schema_name, &function.function_name),
            table_reference: TableReference::partial(
                function.schema_name.clone(),
                function.function_name.clone(),
            ),
            known_args: arg_names.iter().cloned().collect(),
            arg_names,
            factory: Arc::clone(&function.factory),
        }
    }

    fn contains(&self, name: &str) -> bool {
        self.known_args.contains(name)
    }
}

impl ScopedTableFunctionSignature for SourceFunction {
    fn display_name(&self) -> &str {
        &self.display_name
    }

    fn arg_names(&self) -> &[String] {
        &self.arg_names
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
    arg_names: Vec<String>,
    args: Vec<Expr>,
    schema: DFSchemaRef,
    factory: Arc<dyn SourceFunctionProviderFactory>,
}

impl SourceFunctionNode {
    fn new(function: &SourceFunction, args: Vec<Expr>) -> Result<Self> {
        let schema = Arc::new(DFSchema::try_from_qualified_schema(
            function.table_reference.clone(),
            function.factory.schema().as_ref(),
        )?);
        Ok(Self {
            display_name: function.display_name.clone(),
            table_reference: function.table_reference.clone(),
            arg_names: function.arg_names.clone(),
            args,
            schema,
            factory: Arc::clone(&function.factory),
        })
    }

    fn has_parameter_placeholders(&self) -> bool {
        self.args.iter().any(|arg| find_placeholder(arg).is_some())
    }

    pub(crate) fn table_reference(&self) -> &TableReference {
        &self.table_reference
    }

    fn reject_unbound_parameters(&self) -> Result<()> {
        for (name, arg) in self.arg_names.iter().zip(&self.args) {
            if let Some(placeholder) = find_placeholder(arg) {
                return Err(DataFusionError::Plan(format!(
                    "{} argument '{name}' is bound to parameter {placeholder}, \
                     but no value was provided for it",
                    self.display_name
                )));
            }
        }
        Ok(())
    }

    fn to_provider_scan(&self) -> Result<LogicalPlan> {
        self.reject_unbound_parameters()?;
        let provider = self.factory.provider_for_args(&self.args)?;
        LogicalPlanBuilder::scan(
            self.table_reference.clone(),
            provider_as_source(provider),
            None,
        )?
        .build()
    }
}

// Node identity is the function name plus its argument expressions; `schema`
// participates so renamed manifests never compare equal. The remaining fields
// are derived from the same registry entry as `display_name` (and `factory`
// cannot implement `PartialEq`), so they are deliberately excluded.
impl PartialEq for SourceFunctionNode {
    fn eq(&self, other: &Self) -> bool {
        self.display_name == other.display_name
            && self.args == other.args
            && self.schema == other.schema
    }
}

impl Eq for SourceFunctionNode {}

impl Hash for SourceFunctionNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.display_name.hash(state);
        self.args.hash(state);
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
        "CoralSourceFunction"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        Vec::new()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.args.clone()
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
        if exprs.len() != self.args.len() {
            return Err(DataFusionError::Plan(format!(
                "source function {} expected {} argument expressions, got {}",
                self.display_name,
                self.args.len(),
                exprs.len()
            )));
        }
        // Plan rewrites (parameter substitution in particular) alias rewritten
        // expressions to preserve their display names. Arguments are
        // positional here, so the alias carries no meaning -- strip it before
        // binding sees the value.
        Ok(Self {
            args: exprs.into_iter().map(Expr::unalias).collect(),
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
        "coral_source_functions"
    }
}
