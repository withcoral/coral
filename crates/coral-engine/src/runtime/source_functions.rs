//! Source-scoped table function relation planning.

use std::collections::{HashMap, HashSet};

use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion::logical_expr::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, Ident, ObjectName, TableFactor, TableFunctionArgs, Value,
};

use crate::backends::RegisteredTableFunction;

enum SourceFunctionRewrite {
    PassThrough(TableFactor),
    Rewritten(TableFactor),
}

#[derive(Debug, Clone)]
pub(crate) struct SourceFunctionRegistry {
    functions: HashMap<(String, String), SourceFunctionEntry>,
    schemas: HashSet<String>,
}

#[derive(Debug, Clone)]
struct SourceFunctionEntry {
    registered: RegisteredTableFunction,
    arg_lookup_names: Vec<String>,
}

impl SourceFunctionRegistry {
    pub(crate) fn new(functions: Vec<RegisteredTableFunction>) -> Result<Self> {
        let mut schemas = HashSet::new();
        let mut registered = HashMap::new();
        for function in functions {
            let schema_name = function.schema_name.clone();
            let function_name = function.function_name.clone();
            let arg_lookup_names = function.arg_names.clone();
            schemas.insert(schema_name.clone());
            registered.insert(
                (schema_name, function_name),
                SourceFunctionEntry {
                    registered: function,
                    arg_lookup_names,
                },
            );
        }
        Ok(Self {
            functions: registered,
            schemas,
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.functions.is_empty()
    }

    fn rewrite_relation(
        &self,
        mut relation: TableFactor,
        context: &dyn RelationPlannerContext,
    ) -> Result<SourceFunctionRewrite> {
        let Some((schema, function_name)) = source_function_name(&relation, context) else {
            return Ok(SourceFunctionRewrite::PassThrough(relation));
        };
        let Some(function) = self.functions.get(&(schema.clone(), function_name.clone())) else {
            if self.schemas.contains(&schema) {
                return Err(DataFusionError::Plan(format!(
                    "unknown source table function {schema}.{function_name}"
                )));
            }
            return Ok(SourceFunctionRewrite::PassThrough(relation));
        };

        let TableFactor::Table { name, args, .. } = &mut relation else {
            unreachable!("source_function_name only matches table relations");
        };
        let lowered_args = named_args_to_positional(
            function,
            args.as_ref()
                .expect("source_function_name only matches table relations with args"),
            context,
        )?;

        *name = ObjectName::from(vec![Ident::new(function.registered.internal_name.clone())]);
        *args = Some(TableFunctionArgs {
            args: lowered_args,
            settings: None,
        });

        Ok(SourceFunctionRewrite::Rewritten(relation))
    }
}

impl RelationPlanner for SourceFunctionRegistry {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        match self.rewrite_relation(relation, context)? {
            SourceFunctionRewrite::PassThrough(relation) => {
                Ok(RelationPlanning::Original(Box::new(relation)))
            }
            SourceFunctionRewrite::Rewritten(relation) => {
                let plan = context.plan(relation)?;
                Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
                    plan, None,
                ))))
            }
        }
    }
}

fn source_function_name(
    relation: &TableFactor,
    context: &dyn RelationPlannerContext,
) -> Option<(String, String)> {
    let TableFactor::Table {
        name,
        args: Some(_),
        ..
    } = relation
    else {
        return None;
    };
    if name.0.len() != 2 {
        return None;
    }
    let schema = context.normalize_ident(name.0[0].as_ident()?.clone());
    let function = context.normalize_ident(name.0[1].as_ident()?.clone());
    Some((schema, function))
}

fn named_args_to_positional(
    function: &SourceFunctionEntry,
    args: &TableFunctionArgs,
    context: &dyn RelationPlannerContext,
) -> Result<Vec<FunctionArg>> {
    let display_name = source_function_display_name(&function.registered);
    let mut named = HashMap::new();
    let mut seen = HashSet::new();
    for arg in &args.args {
        match arg {
            FunctionArg::Named { name, arg, .. } => {
                let key = context.normalize_ident(name.clone());
                if !seen.insert(key.clone()) {
                    return Err(DataFusionError::Plan(format!(
                        "{display_name} duplicate argument '{}'",
                        name.value
                    )));
                }
                named.insert(key, arg.clone());
            }
            FunctionArg::Unnamed(_) => {
                return Err(DataFusionError::Plan(format!(
                    "{display_name} requires named arguments"
                )));
            }
            FunctionArg::ExprNamed { .. } => {
                return Err(DataFusionError::Plan(format!(
                    "{display_name} requires identifier argument names"
                )));
            }
        }
    }

    for key in named.keys() {
        if !function.arg_lookup_names.iter().any(|arg| arg == key) {
            return Err(DataFusionError::Plan(format!(
                "{display_name} unknown argument '{key}'"
            )));
        }
    }

    Ok(function
        .arg_lookup_names
        .iter()
        .map(|arg| {
            let expr = named
                .remove(arg)
                .unwrap_or_else(|| FunctionArgExpr::Expr(Expr::value(Value::Null)));
            FunctionArg::Unnamed(expr)
        })
        .collect())
}

fn source_function_display_name(function: &RegisteredTableFunction) -> String {
    format!("{}.{}", function.schema_name, function.function_name)
}
