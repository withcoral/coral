//! Recipe table-function relation planning.
//!
//! Recipes publish as ordinary scoped SQL table functions, but their body is
//! Coral SQL. The planner parks a recipe call until query parameters have been
//! bound, then expands the call into the recipe body plan with recipe arguments
//! supplied as `DataFusion` parameter values.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

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

use crate::RecipeRuntimeDefinition;
use crate::runtime::query::{read_only_sql_options, reject_unknown_parameters};
use crate::runtime::recipes::{
    recipe_argument_values, recipe_arrow_schema, recipe_param_values, recipe_query_parameters,
    recipe_sql,
};
use crate::runtime::table_function_calls::{
    FunctionCall, FunctionLookupKey, FunctionSignature, call_parts, find_placeholder,
    lower_named_args_to_positional_exprs, original_relation, qualified_name, reject_settings,
    reject_unsupported_modifiers,
};

#[derive(Debug)]
pub(crate) struct RecipeFunctionRegistry {
    functions: HashMap<FunctionLookupKey, RecipeFunction>,
    recipe_schemas: HashSet<String>,
    source_function_schemas: HashSet<String>,
}

impl RecipeFunctionRegistry {
    pub(crate) async fn new(
        ctx: &SessionContext,
        recipes: &[RecipeRuntimeDefinition],
        source_function_schemas: HashSet<String>,
    ) -> Result<Self> {
        let mut registry = Self {
            functions: HashMap::new(),
            recipe_schemas: HashSet::new(),
            source_function_schemas,
        };

        for recipe in recipes {
            let body_plan = ctx.state().create_logical_plan(recipe_sql(recipe)).await?;
            read_only_sql_options().verify_plan(&body_plan)?;
            registry.insert_function(recipe, &body_plan)?;
        }

        Ok(registry)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.functions.is_empty()
    }

    pub(crate) fn install_relation_planner(self, ctx: &SessionContext) -> Result<()> {
        ctx.register_relation_planner(Arc::new(self))
    }

    pub(crate) fn install_analyzer(ctx: &SessionContext) {
        ctx.add_analyzer_rule(Arc::new(RecipeFunctionAnalyzerRule));
    }

    fn insert_function(
        &mut self,
        recipe: &RecipeRuntimeDefinition,
        body_plan: &LogicalPlan,
    ) -> Result<()> {
        let publish = &recipe.publish.table_function;
        let key = FunctionLookupKey::from_parts(publish.schema.clone(), publish.name.clone());
        self.functions.insert(
            key.clone(),
            RecipeFunction::new(&publish.schema, &publish.name, recipe, body_plan)?,
        );
        self.recipe_schemas.insert(key.schema);
        Ok(())
    }

    fn find(&self, call: &FunctionCall) -> Option<&RecipeFunction> {
        self.functions.get(&call.lookup_key)
    }

    fn owns_recipe_only_schema(&self, call: &FunctionCall) -> bool {
        self.recipe_schemas.contains(&call.lookup_key.schema)
            && !self
                .source_function_schemas
                .contains(&call.lookup_key.schema)
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

impl RelationPlanner for RecipeFunctionRegistry {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        let Some(call) = FunctionCall::parse(&relation, context) else {
            return Ok(original_relation(relation));
        };

        let Some(function) = self.find(&call) else {
            if self.owns_recipe_only_schema(&call) {
                let hint = self.available_functions_hint(&call.lookup_key.schema);
                return Err(call.unknown_function_error("recipe table function", &hint));
            }
            return Ok(original_relation(relation));
        };

        reject_unsupported_modifiers(&call, &relation)?;
        let (call_args, alias) = call_parts(relation);
        reject_settings(&call, &call_args)?;

        let args = lower_named_args_to_positional_exprs(function, &call_args, context)?;
        let node = RecipeFunctionNode::new(function, args);

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
struct RecipeFunction {
    display_name: String,
    table_reference: TableReference,
    arg_names: Vec<String>,
    known_args: HashSet<String>,
    recipe: RecipeRuntimeDefinition,
    body_plan: LogicalPlan,
    schema: DFSchemaRef,
}

impl RecipeFunction {
    fn new(
        schema: &str,
        name: &str,
        recipe: &RecipeRuntimeDefinition,
        body_plan: &LogicalPlan,
    ) -> Result<Self> {
        let table_reference = TableReference::partial(schema.to_string(), name.to_string());
        let arg_names = recipe
            .arguments
            .iter()
            .map(|argument| argument.name.clone())
            .collect::<Vec<_>>();
        let arrow_schema = recipe_arrow_schema(recipe)?;
        let qualified_schema = Arc::new(DFSchema::try_from_qualified_schema(
            table_reference.clone(),
            arrow_schema.as_ref(),
        )?);

        Ok(Self {
            display_name: qualified_name(schema, name),
            table_reference,
            known_args: arg_names.iter().cloned().collect(),
            arg_names,
            recipe: recipe.clone(),
            body_plan: body_plan.clone(),
            schema: qualified_schema,
        })
    }

    fn contains(&self, name: &str) -> bool {
        self.known_args.contains(name)
    }
}

impl FunctionSignature for RecipeFunction {
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

#[derive(Debug, Clone)]
struct RecipeFunctionNode {
    display_name: String,
    table_reference: TableReference,
    arg_names: Vec<String>,
    args: Vec<Expr>,
    schema: DFSchemaRef,
    recipe: RecipeRuntimeDefinition,
    body_plan: LogicalPlan,
}

impl RecipeFunctionNode {
    fn new(function: &RecipeFunction, args: Vec<Expr>) -> Self {
        Self {
            display_name: function.display_name.clone(),
            table_reference: function.table_reference.clone(),
            arg_names: function.arg_names.clone(),
            args,
            schema: Arc::clone(&function.schema),
            recipe: function.recipe.clone(),
            body_plan: function.body_plan.clone(),
        }
    }

    fn has_parameter_placeholders(&self) -> bool {
        self.args.iter().any(|arg| find_placeholder(arg).is_some())
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

    fn validate_arguments(&self) -> Result<()> {
        recipe_argument_values(&self.recipe, &self.args)?;
        Ok(())
    }

    fn to_expanded_plan(&self) -> Result<LogicalPlan> {
        self.reject_unbound_parameters()?;
        let arguments = recipe_argument_values(&self.recipe, &self.args)?;
        let params = recipe_query_parameters(&self.recipe, &arguments)?;
        reject_unknown_parameters(&self.body_plan, &params)?;
        let plan = self
            .body_plan
            .clone()
            .with_param_values(recipe_param_values(&params))?;
        LogicalPlanBuilder::from(plan)
            .alias(self.table_reference.clone())?
            .build()
    }
}

impl PartialEq for RecipeFunctionNode {
    fn eq(&self, other: &Self) -> bool {
        self.display_name == other.display_name
            && self.args == other.args
            && self.schema == other.schema
    }
}

impl Eq for RecipeFunctionNode {}

impl Hash for RecipeFunctionNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.display_name.hash(state);
        self.args.hash(state);
        self.schema.hash(state);
    }
}

impl PartialOrd for RecipeFunctionNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self == other {
            return Some(Ordering::Equal);
        }
        Some(format!("{self:?}").cmp(&format!("{other:?}")))
    }
}

impl UserDefinedLogicalNodeCore for RecipeFunctionNode {
    fn name(&self) -> &'static str {
        "CoralRecipeFunction"
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
        write!(f, "RecipeFunction: {}", self.display_name)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !inputs.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "recipe function {} takes no plan inputs",
                self.display_name
            )));
        }
        if exprs.len() != self.args.len() {
            return Err(DataFusionError::Plan(format!(
                "recipe function {} expected {} argument expressions, got {}",
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
pub(crate) struct RecipeFunctionAnalyzerRule;

impl AnalyzerRule for RecipeFunctionAnalyzerRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| {
            let LogicalPlan::Extension(extension) = &plan else {
                return Ok(Transformed::no(plan));
            };
            let Some(node) = extension.node.as_any().downcast_ref::<RecipeFunctionNode>() else {
                return Ok(Transformed::no(plan));
            };
            Ok(Transformed::yes(node.to_expanded_plan()?))
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str {
        "coral_recipe_functions"
    }
}
