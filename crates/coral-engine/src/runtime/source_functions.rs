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

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, DFSchemaRef, ScalarValue, TableReference};
use datafusion::datasource::{TableProvider, provider_as_source};
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

use crate::backends::shared::filter_expr::literal_to_string;
use crate::backends::shared::scalar::timestamp_to_rfc3339;
use crate::backends::{
    BoundSourceFunctionArg, BoundSourceFunctionValue, CatalogPublication,
    RegisteredTableFunctionArgument, SourceFunctionProviderFactory,
};
use crate::runtime::literal_scalar_value;
use crate::runtime::scoped_table_functions::{
    ScopedTableFunctionSignature, TableFunctionCall, TableFunctionIdentity,
    available_functions_hint, call_parts, catalog_qualified_name, find_placeholder,
    lower_named_args_to_positional_exprs, original_relation, qualified_name, reject_settings,
    reject_unbound_parameters as reject_unbound_table_function_parameters,
    reject_unsupported_modifiers,
};
use coral_spec::ManifestDataType;

pub(crate) const SOURCE_FUNCTION_NODE_NAME: &str = "CoralSourceFunction";
const SOURCE_FUNCTION_ANALYZER_RULE_NAME: &str = "coral_source_functions";

#[derive(Debug)]
pub(crate) struct SourceFunctionRegistry {
    functions: HashMap<TableFunctionIdentity, SourceFunction>,
    scopes: HashSet<(String, String, bool)>,
}

impl SourceFunctionRegistry {
    pub(crate) fn new(publications: &[CatalogPublication]) -> Self {
        let mut scopes = HashSet::new();
        let mut functions_by_name = HashMap::new();

        for publication in publications {
            let catalog_qualified =
                publication.catalog_name != crate::runtime::DATAFUSION_DEFAULT_CATALOG;
            for schema in publication.schema_publications() {
                for (function_name, function) in &schema.table_functions {
                    let lookup_key = if catalog_qualified {
                        TableFunctionIdentity::from_parts(
                            &publication.catalog_name,
                            &schema.schema_name,
                            function_name,
                        )
                    } else {
                        TableFunctionIdentity::from_legacy_source_parts(
                            &schema.schema_name,
                            function_name,
                        )
                    };
                    scopes.insert((
                        lookup_key.catalog_name.clone(),
                        lookup_key.schema_name.clone(),
                        catalog_qualified,
                    ));
                    functions_by_name.insert(
                        lookup_key,
                        SourceFunction::from_publication(
                            catalog_qualified.then_some(publication.catalog_name.as_str()),
                            &schema.schema_name,
                            function_name,
                            function,
                        ),
                    );
                }
            }
        }

        Self {
            functions: functions_by_name,
            scopes,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.functions.is_empty()
    }

    pub(crate) fn names(&self) -> HashSet<TableFunctionIdentity> {
        self.functions.keys().cloned().collect()
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

    fn find(&self, call: &TableFunctionCall) -> Option<&SourceFunction> {
        self.functions
            .get(&call.lookup_key)
            .filter(|function| function.catalog_qualified == call.is_catalog_qualified())
    }

    fn owns_scope(&self, call: &TableFunctionCall) -> bool {
        self.scopes.contains(&(
            call.lookup_key.catalog_name.clone(),
            call.lookup_key.schema_name.clone(),
            call.is_catalog_qualified(),
        ))
    }

    fn available_functions_hint(&self, call: &TableFunctionCall) -> String {
        available_functions_hint(
            &call.lookup_key,
            self.functions.iter().filter_map(|(key, function)| {
                (function.catalog_qualified == call.is_catalog_qualified())
                    .then_some((key, function.display_name.as_str()))
            }),
        )
    }
}

impl RelationPlanner for SourceFunctionRegistry {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        let Some(call) = TableFunctionCall::parse(&relation, context) else {
            return Ok(original_relation(relation));
        };

        let Some(function) = self.find(&call) else {
            if call.is_catalog_qualified() || self.owns_scope(&call) {
                let hint = self.available_functions_hint(&call);
                return Err(call.unknown_function_error("source table function", &hint));
            }
            return Ok(original_relation(relation));
        };

        reject_unsupported_modifiers(&call.display_name, &relation)?;
        let (call_args, alias) = call_parts(relation);
        reject_settings(&call.display_name, &call_args)?;

        let lowered_args = lower_named_args_to_positional_exprs(function, &call_args, context)?;
        let node = SourceFunctionNode::new(function, lowered_args)?;

        // Fully-literal calls validate eagerly so argument-value errors keep
        // surfacing at planning time, exactly as they did when binding ran
        // inside SQL planning. Binding is pure value capture (no I/O), so the
        // analyzer repeating it later is cheap.
        if !node.has_parameter_placeholders() {
            node.provider_for_bound_args()?;
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
    catalog_qualified: bool,
    arguments: Vec<SourceFunctionArgument>,
    factory: Arc<dyn SourceFunctionProviderFactory>,
}

impl SourceFunction {
    fn from_publication(
        catalog_name: Option<&str>,
        schema_name: &str,
        function_name: &str,
        function: &crate::backends::common::TableFunctionPublication,
    ) -> Self {
        let arguments = function
            .metadata
            .arguments
            .iter()
            .map(SourceFunctionArgument::from_registered)
            .collect::<Vec<_>>();
        Self {
            display_name: catalog_name.map_or_else(
                || qualified_name(schema_name, function_name),
                |catalog_name| catalog_qualified_name(catalog_name, schema_name, function_name),
            ),
            table_reference: catalog_name.map_or_else(
                || TableReference::partial(schema_name.to_string(), function_name.to_string()),
                |catalog_name| {
                    TableReference::full(
                        catalog_name.to_string(),
                        schema_name.to_string(),
                        function_name.to_string(),
                    )
                },
            ),
            catalog_qualified: catalog_name.is_some(),
            arguments,
            factory: Arc::clone(&function.factory),
        }
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
    /// Complete SQL reference, so result columns retain the same qualifier as
    /// the invoked legacy or catalog-qualified function.
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

    pub(crate) fn declared_args_with_call_exprs(
        &self,
    ) -> impl Iterator<Item = (&SourceFunctionArgument, &Expr)> {
        self.declared_args.iter().zip(&self.call_args)
    }

    fn reject_unbound_parameters(&self) -> Result<()> {
        reject_unbound_table_function_parameters(
            &self.display_name,
            self.declared_args_with_call_exprs()
                .map(|(argument, arg)| (argument.name.as_str(), arg)),
        )
    }

    fn provider_for_bound_args(&self) -> Result<Arc<dyn TableProvider>> {
        let args = self
            .declared_args_with_call_exprs()
            .map(|(argument, expr)| bind_function_arg(&self.display_name, argument, expr))
            .collect::<Result<Vec<_>>>()?;
        self.factory.provider_for_args(&args)
    }

    fn to_provider_scan(&self) -> Result<LogicalPlan> {
        self.reject_unbound_parameters()?;
        let provider = self.provider_for_bound_args()?;
        LogicalPlanBuilder::scan(
            self.table_reference.clone(),
            provider_as_source(provider),
            None,
        )?
        .build()
    }
}

fn bind_function_arg(
    function: &str,
    argument: &SourceFunctionArgument,
    expr: &Expr,
) -> Result<BoundSourceFunctionArg> {
    let Some(value) = literal_scalar_value(expr)? else {
        return Err(DataFusionError::Plan(format!(
            "{function} argument '{}' must be a literal",
            argument.name
        )));
    };
    if value.is_null() {
        return Ok(None);
    }
    let source_text = function_arg_source_text(expr);

    if argument.data_type == ManifestDataType::Json {
        let Some(value) = value.try_as_str().flatten() else {
            return Err(argument_type_error(function, argument, &value));
        };
        return serde_json::from_str(value)
            .map(|value: serde_json::Value| {
                let source_text = source_text.unwrap_or_else(|| value.to_string());
                Some(BoundSourceFunctionValue { value, source_text })
            })
            .map_err(|error| {
                DataFusionError::Plan(format!(
                    "{function} argument '{}' expected Json: {error}",
                    argument.name
                ))
            });
    }

    let target_type = crate::types::arrow_data_type(argument.data_type);
    let source_type = value.data_type();
    let compatible = source_type == target_type
        || crate::types::is_string_family(&source_type)
        || (argument.data_type == ManifestDataType::Timestamp
            && matches!(source_type, DataType::Timestamp(_, _)))
        || (argument.data_type == ManifestDataType::Int64
            && matches!(
                source_type,
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
            ))
        || (argument.data_type == ManifestDataType::Float64
            && matches!(
                source_type,
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float32
            ))
        || (argument.data_type == ManifestDataType::Utf8
            && matches!(
                source_type,
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Boolean
                    | DataType::Timestamp(_, _)
            ));
    if !compatible {
        return Err(argument_type_error(function, argument, &value));
    }

    let value = value.cast_to(&target_type).map_err(|error| {
        DataFusionError::Plan(format!(
            "{function} argument '{}' expected {}: {error}",
            argument.name,
            argument.data_type.as_manifest_str()
        ))
    })?;
    let value = bound_value_to_json(value, argument.data_type).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{function} argument '{}' could not be encoded",
            argument.name
        ))
    })?;
    let source_text = source_text.unwrap_or_else(|| match &value {
        serde_json::Value::String(value) => value.clone(),
        value => value.to_string(),
    });
    let value = if argument.data_type == ManifestDataType::Utf8 {
        serde_json::Value::String(source_text.clone())
    } else {
        value
    };
    Ok(Some(BoundSourceFunctionValue { value, source_text }))
}

fn function_arg_source_text(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Negative(value) => literal_to_string(value).map(|value| format!("-{value}")),
        _ => literal_to_string(expr),
    }
}

fn bound_value_to_json(
    value: ScalarValue,
    data_type: ManifestDataType,
) -> Option<serde_json::Value> {
    match data_type {
        ManifestDataType::Utf8 => value
            .try_as_str()
            .flatten()
            .map(|value| serde_json::Value::String(value.to_owned())),
        ManifestDataType::Int64 => i64::try_from(value).ok().map(serde_json::Value::from),
        ManifestDataType::Float64 => f64::try_from(value)
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number),
        ManifestDataType::Boolean => bool::try_from(value).ok().map(serde_json::Value::Bool),
        ManifestDataType::Timestamp => timestamp_to_rfc3339(&value).map(serde_json::Value::String),
        ManifestDataType::Json => None,
    }
}

fn argument_type_error(
    function: &str,
    argument: &SourceFunctionArgument,
    value: &ScalarValue,
) -> DataFusionError {
    DataFusionError::Plan(format!(
        "{function} argument '{}' expected {}, got {value:?}",
        argument.name,
        argument.data_type.as_manifest_str()
    ))
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

    fn argument(data_type: ManifestDataType) -> SourceFunctionArgument {
        SourceFunctionArgument {
            name: "value".to_string(),
            data_type,
        }
    }

    #[test]
    fn numeric_binding_accepts_unsigned_values_and_rejects_overflow() {
        let int_value = Expr::Literal(ScalarValue::UInt64(Some(10)), None);
        assert_eq!(
            bind_function_arg(
                "test.function",
                &argument(ManifestDataType::Int64),
                &int_value
            )
            .expect("UInt64 value should fit in Int64")
            .map(|value| value.value),
            Some(serde_json::json!(10))
        );

        let float_value = Expr::Literal(ScalarValue::UInt32(Some(10)), None);
        assert_eq!(
            bind_function_arg(
                "test.function",
                &argument(ManifestDataType::Float64),
                &float_value
            )
            .expect("UInt32 value should convert to Float64")
            .map(|value| value.value),
            Some(serde_json::json!(10.0))
        );

        let overflow = Expr::Literal(ScalarValue::UInt64(Some(u64::MAX)), None);
        let error = bind_function_arg(
            "test.function",
            &argument(ManifestDataType::Int64),
            &overflow,
        )
        .expect_err("UInt64 overflow should be rejected");
        assert!(error.to_string().contains("expected Int64"));
    }

    #[test]
    fn utf8_binding_preserves_negative_zero_source_text() {
        let expr = Expr::Negative(Box::new(Expr::Literal(ScalarValue::Int64(Some(0)), None)));
        let value = bind_function_arg("test.function", &argument(ManifestDataType::Utf8), &expr)
            .expect("negative zero should bind")
            .expect("negative zero should not bind as NULL");

        assert_eq!(value.source_text, "-0");
        assert_eq!(value.value, serde_json::json!("-0"));
    }

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
