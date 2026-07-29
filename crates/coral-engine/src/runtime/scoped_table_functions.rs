//! Shared parsing and argument lowering for Coral scoped table functions.

use std::collections::{HashMap, HashSet};

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::planner::{RelationPlannerContext, RelationPlanning};
use datafusion::logical_expr::sqlparser::ast::{
    Expr as SqlExpr, FunctionArg, FunctionArgExpr, Ident, TableAlias, TableFactor,
    TableFunctionArgs,
};

use crate::backends::RegisteredTableFunction;
use crate::runtime::DATAFUSION_DEFAULT_CATALOG;

pub(crate) trait ScopedTableFunctionSignature {
    fn display_name(&self) -> &str;
    fn arg_count(&self) -> usize;
    fn arg_name(&self, index: usize) -> Option<&str>;

    fn canonical_arg_name(&self, name: &str) -> Option<&str> {
        for index in 0..self.arg_count() {
            let candidate = self.arg_name(index)?;
            if candidate == name {
                return Some(candidate);
            }
        }
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_field_names,
    reason = "The repeated suffix makes each component of the SQL catalog/schema/function identity explicit."
)]
pub(crate) struct TableFunctionIdentity {
    pub(crate) catalog_name: String,
    pub(crate) schema_name: String,
    pub(crate) function_name: String,
}

impl TableFunctionIdentity {
    pub(crate) fn from_parts(catalog_name: &str, schema_name: &str, function_name: &str) -> Self {
        Self {
            catalog_name: normalize_runtime_identifier(catalog_name),
            schema_name: normalize_runtime_identifier(schema_name),
            function_name: normalize_runtime_identifier(function_name),
        }
    }

    pub(crate) fn from_default_catalog_parts(schema_name: &str, function_name: &str) -> Self {
        Self::from_parts(DATAFUSION_DEFAULT_CATALOG, schema_name, function_name)
    }

    pub(crate) fn from_registered(function: &RegisteredTableFunction) -> Self {
        function.catalog_name.as_deref().map_or_else(
            || Self {
                catalog_name: DATAFUSION_DEFAULT_CATALOG.to_string(),
                schema_name: function.schema_name.clone(),
                function_name: function.function_name.clone(),
            },
            |catalog_name| {
                Self::from_parts(catalog_name, &function.schema_name, &function.function_name)
            },
        )
    }

    fn from_sql_parts(
        catalog_name: &str,
        schema: Ident,
        function: Ident,
        context: &dyn RelationPlannerContext,
    ) -> Self {
        Self {
            catalog_name: normalize_runtime_identifier(catalog_name),
            schema_name: context.normalize_ident(schema),
            function_name: context.normalize_ident(function),
        }
    }

    fn from_sql(
        catalog: Ident,
        schema: Ident,
        function: Ident,
        context: &dyn RelationPlannerContext,
    ) -> Self {
        Self {
            catalog_name: context.normalize_ident(catalog),
            schema_name: context.normalize_ident(schema),
            function_name: context.normalize_ident(function),
        }
    }
}

fn normalize_runtime_identifier(identifier: &str) -> String {
    identifier.to_ascii_lowercase()
}

#[derive(Debug)]
pub(crate) struct TableFunctionCall {
    pub(crate) lookup_key: TableFunctionIdentity,
    pub(crate) display_name: String,
    catalog_qualified: bool,
}

impl TableFunctionCall {
    pub(crate) fn parse(
        relation: &TableFactor,
        context: &dyn RelationPlannerContext,
    ) -> Option<Self> {
        let TableFactor::Table {
            name,
            args: Some(_),
            ..
        } = relation
        else {
            return None;
        };

        match name.0.as_slice() {
            [schema, function] => {
                let schema = schema.as_ident()?.clone();
                let function = function.as_ident()?.clone();
                let display_name = qualified_name(&schema.value, &function.value);
                let lookup_key = TableFunctionIdentity::from_sql_parts(
                    DATAFUSION_DEFAULT_CATALOG,
                    schema,
                    function,
                    context,
                );
                Some(Self {
                    lookup_key,
                    display_name,
                    catalog_qualified: false,
                })
            }
            [catalog, schema, function] => {
                let catalog = catalog.as_ident()?.clone();
                let schema = schema.as_ident()?.clone();
                let function = function.as_ident()?.clone();
                let display_name =
                    catalog_qualified_name(&catalog.value, &schema.value, &function.value);
                let lookup_key =
                    TableFunctionIdentity::from_sql(catalog, schema, function, context);
                Some(Self {
                    lookup_key,
                    display_name,
                    catalog_qualified: true,
                })
            }
            _ => None,
        }
    }

    pub(crate) fn parse_legacy(
        relation: &TableFactor,
        context: &dyn RelationPlannerContext,
    ) -> Option<Self> {
        Self::parse(relation, context).filter(|call| !call.catalog_qualified)
    }

    pub(crate) fn is_catalog_qualified(&self) -> bool {
        self.catalog_qualified
    }

    pub(crate) fn unknown_function_error(&self, kind: &str, hint: &str) -> DataFusionError {
        DataFusionError::Plan(format!("unknown {kind} {}{hint}", self.display_name))
    }
}

pub(crate) fn find_placeholder(expr: &Expr) -> Option<String> {
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

pub(crate) fn qualified_name(schema: &str, function: &str) -> String {
    format!("{schema}.{function}")
}

pub(crate) fn catalog_qualified_name(catalog: &str, schema: &str, function: &str) -> String {
    format!("{catalog}.{schema}.{function}")
}

pub(crate) fn available_functions_hint<'a>(
    scope: &TableFunctionIdentity,
    functions: impl IntoIterator<Item = (&'a TableFunctionIdentity, &'a str)>,
) -> String {
    let mut names: Vec<&str> = functions
        .into_iter()
        .filter_map(|(key, display_name)| {
            (key.catalog_name == scope.catalog_name && key.schema_name == scope.schema_name)
                .then_some(display_name)
        })
        .collect();
    names.sort_unstable();

    if names.is_empty() {
        String::new()
    } else {
        format!("; available functions: {}", names.join(", "))
    }
}

pub(crate) fn reject_unbound_parameters<'a>(
    display_name: &str,
    args: impl IntoIterator<Item = (&'a str, &'a Expr)>,
) -> Result<()> {
    for (name, arg) in args {
        if let Some(placeholder) = find_placeholder(arg) {
            return Err(DataFusionError::Plan(format!(
                "{display_name} argument '{name}' is bound to parameter {placeholder}, \
                 but no value was provided for it"
            )));
        }
    }
    Ok(())
}

pub(crate) fn original_relation(relation: TableFactor) -> RelationPlanning {
    RelationPlanning::Original(Box::new(relation))
}

/// Takes ownership of a committed call's argument list and alias.
///
/// Shape-checking and destructuring are split on purpose:
/// [`TableFunctionCall::parse`] inspects the relation by reference because
/// the not-our-function fallthroughs must hand the relation back to
/// `DataFusion` untouched. Only once the call is committed may the relation
/// be consumed, which is what makes the `unreachable!` here truly so.
pub(crate) fn call_parts(relation: TableFactor) -> (TableFunctionArgs, Option<TableAlias>) {
    let TableFactor::Table {
        args: Some(args),
        alias,
        ..
    } = relation
    else {
        unreachable!("TableFunctionCall::parse only matches table function calls");
    };
    (args, alias)
}

/// Rejects table-factor modifiers Coral table-function planners do not support,
/// so user-written SQL semantics are never silently dropped.
///
/// Destructures every field without `..` on purpose: a sqlparser upgrade that
/// adds a new modifier must fail compilation here and force a decision, instead
/// of executing while ignoring the modifier.
pub(crate) fn reject_unsupported_modifiers(
    display_name: &str,
    relation: &TableFactor,
) -> Result<()> {
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
        unreachable!("TableFunctionCall::parse only matches table relations");
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
                "table function {display_name} does not support {modifier}"
            )));
        }
    }
    Ok(())
}

pub(crate) fn reject_settings(display_name: &str, args: &TableFunctionArgs) -> Result<()> {
    if args.settings.is_some() {
        return Err(DataFusionError::Plan(format!(
            "table function {display_name} does not support SETTINGS"
        )));
    }
    Ok(())
}

/// Lowers named call arguments into positional logical expressions in declared
/// order. Missing optional args become NULL literals; each function binder
/// interprets NULL according to its own argument rules.
pub(crate) fn lower_named_args_to_positional_exprs(
    function: &dyn ScopedTableFunctionSignature,
    args: &TableFunctionArgs,
    context: &mut dyn RelationPlannerContext,
) -> Result<Vec<Expr>> {
    let mut supplied = collect_named_args(function, args, context)?;

    lower_supplied_args_to_positional_exprs(function, &mut supplied, context)
}

/// Lowers named call arguments when every declared argument must be present.
/// Explicit NULL values still count as supplied and are interpreted by the
/// function's type binder.
pub(crate) fn lower_required_named_args_to_positional_exprs(
    function: &dyn ScopedTableFunctionSignature,
    args: &TableFunctionArgs,
    context: &mut dyn RelationPlannerContext,
) -> Result<Vec<Expr>> {
    let mut supplied = collect_named_args(function, args, context)?;
    reject_missing_args(function, &supplied)?;

    lower_supplied_args_to_positional_exprs(function, &mut supplied, context)
}

fn lower_supplied_args_to_positional_exprs(
    function: &dyn ScopedTableFunctionSignature,
    supplied: &mut HashMap<String, SqlExpr>,
    context: &mut dyn RelationPlannerContext,
) -> Result<Vec<Expr>> {
    (0..function.arg_count())
        .map(|index| lower_positional_arg(function, index, supplied, context))
        .collect()
}

fn reject_missing_args(
    function: &dyn ScopedTableFunctionSignature,
    supplied: &HashMap<String, SqlExpr>,
) -> Result<()> {
    for index in 0..function.arg_count() {
        let name = declared_arg_name(function, index)?;
        if !supplied.contains_key(name) {
            return Err(DataFusionError::Plan(format!(
                "{} is missing argument '{name}'",
                function.display_name()
            )));
        }
    }
    Ok(())
}

fn lower_positional_arg(
    function: &dyn ScopedTableFunctionSignature,
    index: usize,
    supplied: &mut HashMap<String, SqlExpr>,
    context: &mut dyn RelationPlannerContext,
) -> Result<Expr> {
    let name = declared_arg_name(function, index)?;
    match supplied.remove(name) {
        Some(sql_expr) => context.sql_to_expr(sql_expr, &DFSchema::empty()),
        None => Ok(Expr::Literal(ScalarValue::Null, None)),
    }
}

fn declared_arg_name(function: &dyn ScopedTableFunctionSignature, index: usize) -> Result<&str> {
    function.arg_name(index).ok_or_else(|| {
        DataFusionError::Internal(format!(
            "{} argument index {index} missing from signature",
            function.display_name()
        ))
    })
}

fn collect_named_args(
    function: &dyn ScopedTableFunctionSignature,
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
    function: &dyn ScopedTableFunctionSignature,
    supplied: &mut HashMap<String, SqlExpr>,
    seen: &mut HashSet<String>,
    name: &Ident,
    arg: &FunctionArgExpr,
    context: &dyn RelationPlannerContext,
) -> Result<()> {
    let lookup_name = context.normalize_ident(name.clone());
    let Some(canonical_name) = function.canonical_arg_name(&lookup_name) else {
        return Err(DataFusionError::Plan(format!(
            "{} unknown argument '{}'",
            function.display_name(),
            name.value
        )));
    };
    let canonical_name = canonical_name.to_string();
    if !seen.insert(canonical_name.clone()) {
        return Err(DataFusionError::Plan(format!(
            "{} duplicate argument '{}'",
            function.display_name(),
            name.value
        )));
    }
    let FunctionArgExpr::Expr(sql_expr) = arg else {
        return Err(DataFusionError::Plan(format!(
            "{} argument '{}' does not support wildcard values",
            function.display_name(),
            name.value
        )));
    };
    supplied.insert(canonical_name, sql_expr.clone());
    Ok(())
}

fn non_named_arg_error(
    function: &dyn ScopedTableFunctionSignature,
    arg: &FunctionArg,
) -> DataFusionError {
    match arg {
        FunctionArg::Unnamed(_) => DataFusionError::Plan(format!(
            "{} requires named arguments",
            function.display_name()
        )),
        FunctionArg::ExprNamed { .. } => DataFusionError::Plan(format!(
            "{} requires identifier argument names",
            function.display_name()
        )),
        FunctionArg::Named { .. } => unreachable!("named arguments are handled by the caller"),
    }
}
