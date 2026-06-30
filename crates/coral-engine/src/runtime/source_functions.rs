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
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, DFSchemaRef, ScalarValue, TableReference};
use datafusion::datasource::provider_as_source;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion::logical_expr::sqlparser::ast::{
    Expr as SqlExpr, FunctionArg, FunctionArgExpr, Ident, TableAlias, TableFactor,
    TableFunctionArgs,
};
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;

use crate::backends::{RegisteredTableFunction, SourceFunctionProviderFactory};

#[derive(Debug)]
pub(crate) struct SourceFunctionRegistry {
    functions: HashMap<FunctionLookupKey, SourceFunction>,
    source_schemas: HashSet<String>,
}

impl SourceFunctionRegistry {
    pub(crate) fn new<'a>(
        functions: impl IntoIterator<Item = &'a RegisteredTableFunction>,
    ) -> Self {
        let mut source_schemas = HashSet::new();
        let mut functions_by_name = HashMap::new();

        for function in functions {
            let lookup_key = FunctionLookupKey::from_manifest(function);
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

    fn find(&self, call: &SourceFunctionCall) -> Option<&SourceFunction> {
        self.functions.get(&call.lookup_key)
    }

    fn owns_schema(&self, call: &SourceFunctionCall) -> bool {
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
        let Some(call) = SourceFunctionCall::parse(&relation, context) else {
            return Ok(original_relation(relation));
        };

        let Some(function) = self.find(&call) else {
            if self.owns_schema(&call) {
                let hint = self.available_functions_hint(&call.lookup_key.schema);
                return Err(call.unknown_function_error(&hint));
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FunctionLookupKey {
    schema: String,
    function: String,
}

impl FunctionLookupKey {
    fn from_manifest(function: &RegisteredTableFunction) -> Self {
        Self {
            schema: function.schema_name.clone(),
            function: function.function_name.clone(),
        }
    }

    fn from_sql(schema: Ident, function: Ident, context: &dyn RelationPlannerContext) -> Self {
        Self {
            schema: context.normalize_ident(schema),
            function: context.normalize_ident(function),
        }
    }
}

#[derive(Debug)]
struct SourceFunctionCall {
    lookup_key: FunctionLookupKey,
    display_name: String,
}

impl SourceFunctionCall {
    fn parse(relation: &TableFactor, context: &dyn RelationPlannerContext) -> Option<Self> {
        let TableFactor::Table {
            name,
            args: Some(_),
            ..
        } = relation
        else {
            return None;
        };

        // Coral source functions are exactly `source.function(...)`. Longer
        // names belong to DataFusion's normal relation/function planner.
        let [schema, function] = name.0.as_slice() else {
            return None;
        };

        let schema = schema.as_ident()?.clone();
        let function = function.as_ident()?.clone();
        let display_name = qualified_name(&schema.value, &function.value);
        let lookup_key = FunctionLookupKey::from_sql(schema, function, context);

        Some(Self {
            lookup_key,
            display_name,
        })
    }

    fn unknown_function_error(&self, hint: &str) -> DataFusionError {
        DataFusionError::Plan(format!(
            "unknown source table function {}{}",
            self.display_name, hint
        ))
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

fn find_placeholder(expr: &Expr) -> Option<String> {
    let mut found = None;
    expr.apply(|expr| {
        if let Expr::Placeholder(placeholder) = expr {
            found = Some(placeholder.id.clone());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("placeholder search never fails");
    found
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
        // positional here, so the alias carries no meaning — strip it before
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
        // list — a plain transform_up would never see a source-function call
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

fn qualified_name(schema: &str, function: &str) -> String {
    format!("{schema}.{function}")
}

fn original_relation(relation: TableFactor) -> RelationPlanning {
    RelationPlanning::Original(Box::new(relation))
}

/// Takes ownership of a committed call's argument list and alias.
///
/// Shape-checking and destructuring are split on purpose:
/// [`SourceFunctionCall::parse`] inspects the relation by reference because
/// the not-our-function fallthroughs must hand the relation back to
/// `DataFusion` untouched. Only once the call is committed may the relation
/// be consumed, which is what makes the `unreachable!` here truly so.
fn call_parts(relation: TableFactor) -> (TableFunctionArgs, Option<TableAlias>) {
    let TableFactor::Table {
        args: Some(args),
        alias,
        ..
    } = relation
    else {
        unreachable!("SourceFunctionCall::parse only matches table function calls");
    };
    (args, alias)
}

/// Rejects table-factor modifiers the source-function planner does not
/// support, so user-written SQL semantics are never silently dropped.
///
/// Destructures every field without `..` on purpose: a sqlparser upgrade that
/// adds a new modifier must fail compilation here and force a decision,
/// instead of executing while ignoring the modifier.
fn reject_unsupported_modifiers(call: &SourceFunctionCall, relation: &TableFactor) -> Result<()> {
    let TableFactor::Table {
        name: _,
        alias: _,
        args: _,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = relation
    else {
        unreachable!("SourceFunctionCall::parse only matches table relations");
    };

    let unsupported_modifiers = [
        (*with_ordinality, "WITH ORDINALITY"),
        (sample.is_some(), "TABLESAMPLE"),
        (!with_hints.is_empty(), "table hints"),
        (!index_hints.is_empty(), "index hints"),
        (version.is_some(), "time-travel syntax"),
        (!partitions.is_empty(), "PARTITION selection"),
        (json_path.is_some(), "JSON path access"),
    ];
    for (present, modifier) in unsupported_modifiers {
        if present {
            return Err(DataFusionError::Plan(format!(
                "source table function {} does not support {modifier}",
                call.display_name
            )));
        }
    }
    Ok(())
}

fn reject_settings(call: &SourceFunctionCall, args: &TableFunctionArgs) -> Result<()> {
    if args.settings.is_some() {
        return Err(DataFusionError::Plan(format!(
            "source table function {} does not support SETTINGS",
            call.display_name
        )));
    }
    Ok(())
}

/// Lowers named call arguments into positional logical expressions in manifest
/// order. Missing optional args become NULL literals; the backend binder
/// treats NULL as absent and performs required-argument validation after that
/// interpretation.
fn lower_named_args_to_positional_exprs(
    function: &SourceFunction,
    args: &TableFunctionArgs,
    context: &mut dyn RelationPlannerContext,
) -> Result<Vec<Expr>> {
    let mut supplied = collect_named_args(function, args, context)?;

    function
        .arg_names
        .iter()
        .map(|name| match supplied.remove(name) {
            Some(sql_expr) => context.sql_to_expr(sql_expr, &DFSchema::empty()),
            None => Ok(Expr::Literal(ScalarValue::Null, None)),
        })
        .collect()
}

fn collect_named_args(
    function: &SourceFunction,
    args: &TableFunctionArgs,
    context: &dyn RelationPlannerContext,
) -> Result<HashMap<String, SqlExpr>> {
    let mut supplied = HashMap::new();
    let mut seen = HashSet::new();

    for arg in &args.args {
        let FunctionArg::Named { name, arg, .. } = arg else {
            return Err(non_named_arg_error(function, arg));
        };
        insert_named_arg(function, &mut supplied, &mut seen, name, arg, context)?;
    }

    Ok(supplied)
}

fn insert_named_arg(
    function: &SourceFunction,
    supplied: &mut HashMap<String, SqlExpr>,
    seen: &mut HashSet<String>,
    name: &Ident,
    arg: &FunctionArgExpr,
    context: &dyn RelationPlannerContext,
) -> Result<()> {
    let lookup_name = context.normalize_ident(name.clone());
    if !seen.insert(lookup_name.clone()) {
        return Err(DataFusionError::Plan(format!(
            "{} duplicate argument '{}'",
            function.display_name, name.value
        )));
    }
    if !function.contains(&lookup_name) {
        return Err(DataFusionError::Plan(format!(
            "{} unknown argument '{}'",
            function.display_name, name.value
        )));
    }
    let FunctionArgExpr::Expr(sql_expr) = arg else {
        return Err(DataFusionError::Plan(format!(
            "{} argument '{}' does not support wildcard values",
            function.display_name, name.value
        )));
    };
    supplied.insert(lookup_name, sql_expr.clone());
    Ok(())
}

fn non_named_arg_error(function: &SourceFunction, arg: &FunctionArg) -> DataFusionError {
    match arg {
        FunctionArg::Unnamed(_) => DataFusionError::Plan(format!(
            "{} requires named arguments",
            function.display_name
        )),
        FunctionArg::ExprNamed { .. } => DataFusionError::Plan(format!(
            "{} requires identifier argument names",
            function.display_name
        )),
        FunctionArg::Named { .. } => unreachable!("named arguments are handled by the caller"),
    }
}
