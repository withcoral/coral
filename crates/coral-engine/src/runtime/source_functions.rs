//! Source-scoped table function relation planning.
//!
//! `DataFusion` registers UDTFs in one flat namespace. Coral exposes source
//! functions as scoped SQL relations like `github.find_issues(...)`. Backends
//! therefore register hidden internal UDTF names, and this planner rewrites the
//! scoped relation into the hidden function call before handing planning back
//! to `DataFusion`.

use std::collections::{HashMap, HashSet};

use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion::logical_expr::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, Ident, ObjectName, TableFactor, TableFunctionArgs, Value,
};

use crate::backends::RegisteredTableFunction;

#[derive(Debug)]
pub(crate) struct SourceFunctionRegistry {
    functions: HashMap<FunctionLookupKey, SourceFunction>,
    source_schemas: HashSet<String>,
}

#[derive(Debug)]
struct SourceFunction {
    internal_name: String,
    display_name: String,
    arg_names: Vec<String>,
    known_args: HashSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FunctionLookupKey {
    schema: String,
    function: String,
}

#[derive(Debug)]
struct SourceFunctionCall {
    lookup_key: FunctionLookupKey,
    display_name: String,
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

        let rewritten = rewrite_to_internal_udtf(relation, &call, function, context)?;
        let plan = context.plan(rewritten)?;
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            plan, None,
        ))))
    }
}

impl SourceFunction {
    fn from_registered(function: &RegisteredTableFunction) -> Self {
        let arg_names = function.arg_names.clone();
        Self {
            internal_name: function.internal_name.clone(),
            display_name: qualified_name(&function.schema_name, &function.function_name),
            known_args: arg_names.iter().cloned().collect(),
            arg_names,
        }
    }

    fn contains(&self, name: &str) -> bool {
        self.known_args.contains(name)
    }
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

fn qualified_name(schema: &str, function: &str) -> String {
    format!("{schema}.{function}")
}

fn original_relation(relation: TableFactor) -> RelationPlanning {
    RelationPlanning::Original(Box::new(relation))
}

fn rewrite_to_internal_udtf(
    mut relation: TableFactor,
    call: &SourceFunctionCall,
    function: &SourceFunction,
    context: &dyn RelationPlannerContext,
) -> Result<TableFactor> {
    let TableFactor::Table {
        name,
        args: table_args,
        ..
    } = &mut relation
    else {
        unreachable!("SourceFunctionCall::parse only matches table relations");
    };

    let call_args = table_args
        .as_ref()
        .expect("SourceFunctionCall::parse only matches function calls");
    reject_settings(call, call_args)?;

    *name = ObjectName::from(vec![Ident::new(function.internal_name.clone())]);
    *table_args = Some(TableFunctionArgs {
        args: lower_named_args_to_internal_positions(function, call_args, context)?,
        settings: None,
    });

    Ok(relation)
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

fn lower_named_args_to_internal_positions(
    function: &SourceFunction,
    args: &TableFunctionArgs,
    context: &dyn RelationPlannerContext,
) -> Result<Vec<FunctionArg>> {
    let mut supplied = collect_named_args(function, args, context)?;

    // The internal UDTF is positional. Missing optional args are represented as
    // NULL placeholders; the backend binder treats NULL as absent and performs
    // required-argument validation after that interpretation.
    Ok(function
        .arg_names
        .iter()
        .map(|name| {
            let expr = supplied.remove(name).unwrap_or_else(null_arg);
            FunctionArg::Unnamed(expr)
        })
        .collect())
}

fn collect_named_args(
    function: &SourceFunction,
    args: &TableFunctionArgs,
    context: &dyn RelationPlannerContext,
) -> Result<HashMap<String, FunctionArgExpr>> {
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
    supplied: &mut HashMap<String, FunctionArgExpr>,
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
    reject_wildcard_arg(function, name, arg)?;
    supplied.insert(lookup_name, arg.clone());
    Ok(())
}

fn reject_wildcard_arg(
    function: &SourceFunction,
    name: &Ident,
    arg: &FunctionArgExpr,
) -> Result<()> {
    if matches!(
        arg,
        FunctionArgExpr::Wildcard | FunctionArgExpr::QualifiedWildcard(_)
    ) {
        return Err(DataFusionError::Plan(format!(
            "{} argument '{}' does not support wildcard values",
            function.display_name, name.value
        )));
    }
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

fn null_arg() -> FunctionArgExpr {
    FunctionArgExpr::Expr(Expr::value(Value::Null))
}
