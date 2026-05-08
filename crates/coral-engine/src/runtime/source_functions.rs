//! Source-scoped table function relation planning.
//!
//! `DataFusion` registers UDTFs in a flat namespace, while Coral exposes them
//! as source-scoped SQL relations like `github.find_issues(...)`. Backends
//! register hidden internal UDTF names, and this relation planner rewrites the
//! source-scoped relation into the internal function call. Argument names are
//! lowered into manifest order here; backend-specific validation and execution
//! stay with the table function implementation.

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
    internal_name: String,
    display_name: String,
    arg_lookup_names: Vec<String>,
    arg_lookup: HashSet<String>,
}

struct SourceFunctionName {
    lookup_schema: String,
    lookup_function: String,
    display_schema: String,
    display_function: String,
}

impl SourceFunctionRegistry {
    pub(crate) fn new(functions: Vec<RegisteredTableFunction>) -> Result<Self> {
        let mut schemas = HashSet::new();
        let mut registered = HashMap::new();
        for function in functions {
            let schema_name = function.schema_name.clone();
            let function_name = function.function_name.clone();
            let arg_lookup_names = function.arg_names.clone();
            let display_name = source_function_display_name(&function);
            schemas.insert(schema_name.clone());
            registered.insert(
                (schema_name, function_name),
                SourceFunctionEntry {
                    internal_name: function.internal_name,
                    display_name,
                    arg_lookup: arg_lookup_names.iter().cloned().collect(),
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
        let Some(source_function) = source_function_name(&relation, context) else {
            return Ok(SourceFunctionRewrite::PassThrough(relation));
        };
        let Some(function) = self.functions.get(&(
            source_function.lookup_schema.clone(),
            source_function.lookup_function.clone(),
        )) else {
            if self.schemas.contains(&source_function.lookup_schema) {
                return Err(DataFusionError::Plan(format!(
                    "unknown source table function {}.{}{}",
                    source_function.display_schema,
                    source_function.display_function,
                    self.available_functions_hint(&source_function.lookup_schema)
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

        *name = ObjectName::from(vec![Ident::new(function.internal_name.clone())]);
        *args = Some(TableFunctionArgs {
            args: lowered_args,
            settings: None,
        });

        Ok(SourceFunctionRewrite::Rewritten(relation))
    }

    fn available_functions_hint(&self, schema: &str) -> String {
        let mut names: Vec<&str> = self
            .functions
            .iter()
            .filter_map(|((entry_schema, _), function)| {
                (entry_schema == schema).then_some(function.display_name.as_str())
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
) -> Option<SourceFunctionName> {
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
    let schema = name.0[0].as_ident()?.clone();
    let function = name.0[1].as_ident()?.clone();
    Some(SourceFunctionName {
        lookup_schema: context.normalize_ident(schema.clone()),
        lookup_function: context.normalize_ident(function.clone()),
        display_schema: schema.value,
        display_function: function.value,
    })
}

fn named_args_to_positional(
    function: &SourceFunctionEntry,
    args: &TableFunctionArgs,
    context: &dyn RelationPlannerContext,
) -> Result<Vec<FunctionArg>> {
    let display_name = &function.display_name;
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
                if !function.arg_lookup.contains(&key) {
                    return Err(DataFusionError::Plan(format!(
                        "{display_name} unknown argument '{}'",
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

    // CONTRACT with `HttpSourceTableFunction::call`: the internal UDTF is
    // positional, so this planner emits exactly one slot per manifest argument.
    // Missing named args are padded with NULL; the backend binder interprets
    // those NULLs as absent and performs required-argument validation there.
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
