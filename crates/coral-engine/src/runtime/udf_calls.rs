//! UDF SQL call planning.
//!
//! UDFs publish as ordinary scoped SQL table functions, but their body is
//! Coral SQL. The planner parks a UDF call until query parameters have been
//! bound, then expands the call into the udf body plan with udf arguments
//! supplied as `DataFusion` parameter values.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, DFSchemaRef, TableReference};
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

use crate::runtime::query::{read_only_sql_options, reject_unknown_parameters};
use crate::runtime::scoped_table_functions::{
    ScopedTableFunctionCall, ScopedTableFunctionName, ScopedTableFunctionSignature,
    available_functions_hint, call_parts, find_placeholder,
    lower_required_named_args_to_positional_exprs, original_relation, qualified_name,
    reject_settings, reject_unbound_parameters as reject_unbound_table_function_parameters,
    reject_unsupported_modifiers,
};
use crate::runtime::udfs::{udf_argument_values, udf_arrow_schema, udf_param_values, udf_sql};
use crate::{QueryParameters, UdfRuntimeDefinition};

pub(crate) const UDF_CALL_NODE_NAME: &str = "CoralUdfCall";

#[derive(Debug)]
pub(crate) struct UdfCallRegistry {
    functions: HashMap<ScopedTableFunctionName, UdfCallTarget>,
    udf_schemas: HashSet<String>,
    source_function_schemas: HashSet<String>,
}

impl UdfCallRegistry {
    pub(crate) async fn new(
        ctx: &SessionContext,
        udfs: &[UdfRuntimeDefinition],
        source_functions: HashSet<ScopedTableFunctionName>,
    ) -> Result<Self> {
        let source_function_schemas = source_functions
            .iter()
            .map(|function| function.schema.clone())
            .collect();
        let mut registry = Self {
            functions: HashMap::new(),
            udf_schemas: HashSet::new(),
            source_function_schemas,
        };

        for udf in udfs {
            let body_plan = ctx.state().create_logical_plan(udf_sql(udf)).await?;
            read_only_sql_options().verify_plan(&body_plan)?;
            registry.insert_function(udf, &body_plan)?;
        }

        Ok(registry)
    }

    /// Installs the relation planner that parks UDF calls. Runtime bootstrap
    /// installs the expansion rule before `DataFusion`'s default analyzer rules
    /// so the expanded body receives normal analysis.
    pub(crate) fn install(self, ctx: &SessionContext) -> Result<()> {
        ctx.register_relation_planner(Arc::new(self))
    }

    fn insert_function(
        &mut self,
        udf: &UdfRuntimeDefinition,
        body_plan: &LogicalPlan,
    ) -> Result<()> {
        let publish = &udf.publish.table_function;
        let key = ScopedTableFunctionName::from_parts(&publish.schema, &publish.name);
        if self
            .functions
            .insert(
                key.clone(),
                UdfCallTarget::new(&key.schema, &key.function, udf, body_plan)?,
            )
            .is_some()
        {
            return Err(DataFusionError::Internal(format!(
                "validated udf table function {} was registered twice",
                qualified_name(&key.schema, &key.function)
            )));
        }
        self.udf_schemas.insert(key.schema);
        Ok(())
    }

    fn find(&self, call: &ScopedTableFunctionCall) -> Option<&UdfCallTarget> {
        self.functions.get(&call.lookup_key)
    }

    fn owns_udf_only_schema(&self, call: &ScopedTableFunctionCall) -> bool {
        self.udf_schemas.contains(&call.lookup_key.schema)
            && !self
                .source_function_schemas
                .contains(&call.lookup_key.schema)
    }

    fn available_functions_hint(&self, schema: &str) -> String {
        available_functions_hint(
            schema,
            self.functions
                .iter()
                .map(|(key, function)| (key, function.display_name.as_str())),
        )
    }
}

impl RelationPlanner for UdfCallRegistry {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        let Some(call) = ScopedTableFunctionCall::parse(&relation, context) else {
            return Ok(original_relation(relation));
        };

        let Some(function) = self.find(&call) else {
            if self.owns_udf_only_schema(&call) {
                let hint = self.available_functions_hint(&call.lookup_key.schema);
                return Err(call.unknown_function_error("udf table function", &hint));
            }
            return Ok(original_relation(relation));
        };

        reject_unsupported_modifiers(&call, &relation)?;
        let (call_args, alias) = call_parts(relation);
        reject_settings(&call, &call_args)?;

        let args = lower_required_named_args_to_positional_exprs(function, &call_args, context)?;
        let node = UdfCallNode::new(function, args);

        if !node.has_parameter_placeholders() {
            node.validate_arguments()?;
        }

        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            plan, alias,
        ))))
    }
}

#[derive(Debug, Clone)]
struct UdfCallTarget {
    display_name: String,
    table_reference: TableReference,
    arg_names: Vec<String>,
    udf: UdfRuntimeDefinition,
    body_plan: LogicalPlan,
    schema: DFSchemaRef,
}

impl UdfCallTarget {
    fn new(
        schema: &str,
        name: &str,
        udf: &UdfRuntimeDefinition,
        body_plan: &LogicalPlan,
    ) -> Result<Self> {
        validate_argument_definitions(udf)?;
        let table_reference = TableReference::partial(schema.to_string(), name.to_string());
        let arg_names = udf
            .arguments
            .iter()
            .map(|argument| argument.name.clone())
            .collect::<Vec<_>>();
        let arrow_schema = udf_arrow_schema(udf)?;
        validate_declared_result_schema(udf, body_plan, &arrow_schema)?;
        let qualified_schema = Arc::new(DFSchema::try_from_qualified_schema(
            table_reference.clone(),
            arrow_schema.as_ref(),
        )?);

        Ok(Self {
            display_name: qualified_name(schema, name),
            table_reference,
            arg_names,
            udf: udf.clone(),
            body_plan: body_plan.clone(),
            schema: qualified_schema,
        })
    }
}

impl ScopedTableFunctionSignature for UdfCallTarget {
    fn display_name(&self) -> &str {
        &self.display_name
    }

    fn arg_count(&self) -> usize {
        self.arg_names.len()
    }

    fn arg_name(&self, index: usize) -> Option<&str> {
        self.arg_names.get(index).map(String::as_str)
    }

    fn canonical_arg_name(&self, name: &str) -> Option<&str> {
        self.arg_names
            .iter()
            .map(String::as_str)
            .find(|argument| argument.eq_ignore_ascii_case(name))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct UdfCallNode {
    display_name: String,
    table_reference: TableReference,
    arg_names: Vec<String>,
    args: Vec<Expr>,
    schema: DFSchemaRef,
    udf: UdfRuntimeDefinition,
    body_plan: LogicalPlan,
}

impl UdfCallNode {
    fn new(function: &UdfCallTarget, args: Vec<Expr>) -> Self {
        Self {
            display_name: function.display_name.clone(),
            table_reference: function.table_reference.clone(),
            arg_names: function.arg_names.clone(),
            args,
            schema: Arc::clone(&function.schema),
            udf: function.udf.clone(),
            body_plan: function.body_plan.clone(),
        }
    }

    fn has_parameter_placeholders(&self) -> bool {
        self.args.iter().any(|arg| find_placeholder(arg).is_some())
    }

    pub(crate) fn body_plan(&self) -> &LogicalPlan {
        &self.body_plan
    }

    pub(crate) fn table_reference(&self) -> &TableReference {
        &self.table_reference
    }

    fn reject_unbound_parameters(&self) -> Result<()> {
        reject_unbound_table_function_parameters(
            &self.display_name,
            self.arg_names
                .iter()
                .zip(&self.args)
                .map(|(name, arg)| (name.as_str(), arg)),
        )
    }

    fn validate_arguments(&self) -> Result<()> {
        udf_argument_values(&self.udf, &self.args)?;
        Ok(())
    }

    fn to_expanded_plan(&self) -> Result<LogicalPlan> {
        self.reject_unbound_parameters()?;
        let params = udf_argument_values(&self.udf, &self.args)?;
        self.reject_missing_body_parameters(&params)?;
        reject_unknown_parameters(&self.body_plan, &params)?;
        let plan = self
            .body_plan
            .clone()
            .with_param_values(udf_param_values(&params))?;
        LogicalPlanBuilder::from(plan)
            .alias(self.table_reference.clone())?
            .build()
    }

    fn reject_missing_body_parameters(&self, params: &QueryParameters) -> Result<()> {
        let mut referenced = self
            .body_plan
            .get_parameter_names()?
            .into_iter()
            .collect::<Vec<_>>();
        referenced.sort();

        for placeholder in referenced {
            let name = placeholder.strip_prefix('$').unwrap_or(&placeholder);
            if params.get(name).is_none() {
                return Err(DataFusionError::Plan(format!(
                    "udf '{}' body references parameter '{}' not declared as an argument",
                    self.display_name, placeholder
                )));
            }
        }
        Ok(())
    }
}

fn validate_argument_definitions(udf: &UdfRuntimeDefinition) -> Result<()> {
    let mut seen = BTreeSet::new();
    for argument in &udf.arguments {
        if !seen.insert(argument.name.to_ascii_lowercase()) {
            return Err(DataFusionError::Plan(format!(
                "udf '{}' argument '{}' is declared more than once",
                udf.name, argument.name
            )));
        }
    }
    Ok(())
}

fn validate_declared_result_schema(
    udf: &UdfRuntimeDefinition,
    body_plan: &LogicalPlan,
    declared_schema: &Schema,
) -> Result<()> {
    let declared_fields = declared_schema.fields();
    let body_fields = body_plan.schema().fields();

    if declared_fields.len() != body_fields.len() {
        return Err(DataFusionError::Plan(format!(
            "udf '{}' declares {} result columns but its SQL body produces {}",
            udf.name,
            declared_fields.len(),
            body_fields.len()
        )));
    }

    for (index, (declared, actual)) in declared_fields.iter().zip(body_fields.iter()).enumerate() {
        let column_position = index + 1;
        if declared.name() != actual.name() {
            return Err(DataFusionError::Plan(format!(
                "udf '{}' declared column {column_position} as '{}' but its SQL body produces '{}'",
                udf.name,
                declared.name(),
                actual.name()
            )));
        }
        if !result_data_types_match(declared.data_type(), actual.data_type()) {
            return Err(DataFusionError::Plan(format!(
                "udf '{}' declared column '{}' as {} but its SQL body produces {}",
                udf.name,
                declared.name(),
                declared.data_type(),
                actual.data_type()
            )));
        }
        if !declared.is_nullable() && actual.is_nullable() {
            return Err(DataFusionError::Plan(format!(
                "udf '{}' declared column '{}' as non-nullable but its SQL body produces nullable values",
                udf.name,
                declared.name()
            )));
        }
    }

    Ok(())
}

fn result_data_types_match(left: &DataType, right: &DataType) -> bool {
    left == right || (crate::types::is_string_family(left) && crate::types::is_string_family(right))
}

// Node identity is the published function name plus its argument expressions and
// result schema. The remaining fields are derived from the same registered UDF
// definition, so they are deliberately excluded from equality and hashing.
impl PartialEq for UdfCallNode {
    fn eq(&self, other: &Self) -> bool {
        self.display_name == other.display_name
            && self.args == other.args
            && self.schema == other.schema
    }
}

impl Eq for UdfCallNode {}

impl Hash for UdfCallNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.display_name.hash(state);
        self.args.hash(state);
        self.schema.hash(state);
    }
}

impl PartialOrd for UdfCallNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self == other {
            return Some(Ordering::Equal);
        }
        Some(format!("{self:?}").cmp(&format!("{other:?}")))
    }
}

impl UserDefinedLogicalNodeCore for UdfCallNode {
    fn name(&self) -> &'static str {
        UDF_CALL_NODE_NAME
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
        write!(f, "UdfCall: {}", self.display_name)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !inputs.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "UDF {} takes no plan inputs",
                self.display_name
            )));
        }
        if exprs.len() != self.args.len() {
            return Err(DataFusionError::Plan(format!(
                "UDF {} expected {} argument expressions, got {}",
                self.display_name,
                self.args.len(),
                exprs.len()
            )));
        }
        Ok(Self {
            args: exprs.into_iter().map(Expr::unalias).collect(),
            ..self.clone()
        })
    }
}

#[derive(Debug, Default)]
pub(crate) struct UdfCallAnalyzerRule;

impl AnalyzerRule for UdfCallAnalyzerRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| {
            let LogicalPlan::Extension(extension) = &plan else {
                return Ok(Transformed::no(plan));
            };
            let Some(node) = extension.node.as_any().downcast_ref::<UdfCallNode>() else {
                return Ok(Transformed::no(plan));
            };
            Ok(Transformed::yes(node.to_expanded_plan()?))
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str {
        "coral_udf_calls"
    }
}
