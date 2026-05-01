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

#[derive(Debug, Clone)]
pub(crate) struct SourceFunctionRegistry {
    functions: HashMap<(String, String), RegisteredTableFunction>,
    schemas: HashSet<String>,
}

impl SourceFunctionRegistry {
    pub(crate) fn new(functions: Vec<RegisteredTableFunction>) -> Self {
        let schemas = functions
            .iter()
            .map(|function| function.schema_name.clone())
            .collect();
        let functions = functions
            .into_iter()
            .map(|function| {
                (
                    (function.schema_name.clone(), function.function_name.clone()),
                    function,
                )
            })
            .collect();
        Self { functions, schemas }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.functions.is_empty()
    }
}

impl RelationPlanner for SourceFunctionRegistry {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        let TableFactor::Table {
            name,
            alias,
            args: Some(args),
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } = relation
        else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };

        let Some((schema, function_name)) = source_function_name(&name) else {
            return Ok(RelationPlanning::Original(Box::new(TableFactor::Table {
                name,
                alias,
                args: Some(args),
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
            })));
        };
        let Some(function) = self.functions.get(&(schema.clone(), function_name.clone())) else {
            if self.schemas.contains(&schema) {
                return Err(DataFusionError::Plan(format!(
                    "unknown source table function {schema}.{function_name}"
                )));
            }
            return Ok(RelationPlanning::Original(Box::new(TableFactor::Table {
                name,
                alias,
                args: Some(args),
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
            })));
        };

        let internal_relation = TableFactor::Table {
            name: ObjectName::from(vec![Ident::new(function.internal_name.clone())]),
            alias,
            args: Some(TableFunctionArgs {
                args: lower_args(function, &args)?,
                settings: None,
            }),
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        };
        let plan = context.plan(internal_relation)?;
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            plan, None,
        ))))
    }
}

fn source_function_name(name: &ObjectName) -> Option<(String, String)> {
    if name.0.len() != 2 {
        return None;
    }
    let schema = canonical_ident(name.0[0].as_ident()?);
    let function = canonical_ident(name.0[1].as_ident()?);
    Some((schema, function))
}

fn canonical_ident(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

fn lower_args(
    function: &RegisteredTableFunction,
    args: &TableFunctionArgs,
) -> Result<Vec<FunctionArg>> {
    let display_name = source_function_display_name(function);
    let mut named = HashMap::new();
    let mut seen = HashSet::new();
    for arg in &args.args {
        match arg {
            FunctionArg::Named { name, arg, .. } => {
                let key = canonical_ident(name);
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
        if !function.arg_names.iter().any(|arg| arg == key) {
            return Err(DataFusionError::Plan(format!(
                "{display_name} unknown argument '{key}'"
            )));
        }
    }

    Ok(function
        .arg_names
        .iter()
        .map(|arg| {
            let expr = named
                .remove(&arg.to_ascii_lowercase())
                .unwrap_or_else(|| FunctionArgExpr::Expr(Expr::value(Value::Null)));
            FunctionArg::Unnamed(expr)
        })
        .collect())
}

fn source_function_display_name(function: &RegisteredTableFunction) -> String {
    format!("{}.{}", function.schema_name, function.function_name)
}
