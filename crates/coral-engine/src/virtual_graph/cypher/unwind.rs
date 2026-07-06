//! UNWIND row-source, static-expansion, and staged-collect lowering helpers.

use super::staged::{literal_list_element_type_for_data_type, stage_export_column};
#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "UNWIND lowering helpers intentionally inherit parent-private Cypher helpers."
)]
use super::*;

#[derive(Debug, Clone, Copy)]
enum StaticUnwindSite {
    SinglePart {
        reading_clause_index: usize,
    },
    MultiPart {
        query_part: MultiPartAlternativePart,
        reading_clause_index: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StaticUnwindValue {
    presence_variable: Option<String>,
    literals: Vec<Literal>,
}

pub(super) fn compile_single_query_row_source_before_expansion(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
    path: &str,
) -> Result<Option<GraphQuery>, CoreError> {
    if let Some(query) = compile_direct_unwind_row_source(single_query, context, path)? {
        return Ok(Some(query));
    }
    let SingleQueryKind::MultiPart(multi_part) = &single_query.kind else {
        return Ok(None);
    };
    compile_staged_collect_unwind_multi_part(multi_part, context)
}

fn compile_direct_unwind_row_source(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
    path: &str,
) -> Result<Option<GraphQuery>, CoreError> {
    if let Some(query) = compile_literal_unwind_row_source(single_query, context, path)? {
        return Ok(Some(query));
    }
    if let Some(query) =
        compile_literal_unwind_terminal_with_row_source(single_query, context, path)?
    {
        return Ok(Some(query));
    }
    if let Some(query) =
        compile_large_static_metadata_unwind_row_source(single_query, context, path)?
    {
        return Ok(Some(query));
    }
    compile_dynamic_unwind_row_source(single_query, context, path)
}

fn compile_literal_unwind_row_source(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
    path: &str,
) -> Result<Option<GraphQuery>, CoreError> {
    let SingleQueryKind::SinglePart(single_part) = &single_query.kind else {
        return Ok(None);
    };
    let [reading_clause] = single_part.reading_clauses.as_slice() else {
        return Ok(None);
    };
    let ReadingClause::Unwind(unwind) = reading_clause else {
        return Ok(None);
    };

    let variable = dynamic_unwind_variable_name(unwind, context);
    let return_clause = return_clause_from_single_part(single_part, path)?;
    let Some(list) = compile_optional_literal_unwind_row_source_list(
        &unwind.expression,
        format!("{path}.reading_clauses[0].unwind.expression"),
        context,
    )?
    else {
        return Ok(None);
    };

    let element_type = graph_unwind_list_element_type(&list)?;
    compile_unwind_terminal_query(None, list, element_type, variable, return_clause, context)
        .map(Some)
}

fn compile_dynamic_unwind_row_source(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
    path: &str,
) -> Result<Option<GraphQuery>, CoreError> {
    let SingleQueryKind::MultiPart(multi_part) = &single_query.kind else {
        return Ok(None);
    };
    let Some((input, input_types)) = compile_dynamic_unwind_input(multi_part, context)? else {
        return Ok(None);
    };
    let [ReadingClause::Unwind(unwind), remaining @ ..] =
        multi_part.final_part.reading_clauses.as_slice()
    else {
        return Ok(None);
    };

    let variable = dynamic_unwind_variable_name(unwind, context);
    if input_types.contains_key(&variable) {
        return Err(unsupported(
            format!("{path}.final_part.reading_clauses[0].unwind.variable"),
            format!("UNWIND variable '{variable}' conflicts with an in-scope WITH alias"),
        ));
    }
    let source = compile_dynamic_unwind_source_expression(
        unwind,
        format!("{path}.final_part.reading_clauses[0].unwind.expression"),
        &input_types,
        context,
    )?
    .ok_or_else(|| {
        unsupported(
            format!("{path}.final_part.reading_clauses[0].unwind.expression"),
            "dynamic UNWIND currently supports WITH list aliases and concatenations of those aliases",
        )
    })?;

    if remaining.is_empty() {
        let return_clause =
            return_clause_from_single_part(&multi_part.final_part, format!("{path}.final_part"))?;
        return compile_unwind_terminal_query(
            Some(input),
            source.expression,
            source.element_type,
            variable,
            return_clause,
            context,
        )
        .map(Some);
    }

    let mut final_plan = GraphPlan::default();
    let mut final_state = CypherCompileState::default();
    final_state.scalar_aliases.push(Projection::Expression {
        expression: ScalarExpression::StageValue {
            alias: variable.clone(),
        },
        alias: variable.clone(),
    });
    record_unwind_list_alias(&mut final_state, &variable, source.element_type);
    compile_reading_clauses_into(
        remaining,
        format!("{path}.final_part.reading_clauses"),
        &mut final_plan,
        &mut final_state,
        context,
    )?;
    let return_clause =
        return_clause_from_single_part(&multi_part.final_part, format!("{path}.final_part"))?;
    compile_return(return_clause, &mut final_plan, &final_state, context)?;
    reject_ignored_path_variable_references(&final_plan, &final_state, "final_part.return")?;

    Ok(Some(GraphQuery::UnwindPipeline(GraphUnwindPipeline {
        unwind: GraphUnwind {
            input: Some(input),
            list: source.expression,
            element_type: source.element_type,
            variable: variable.clone(),
            projections: vec![GraphUnwindProjection::Variable { alias: variable }],
        },
        final_plan,
    })))
}

fn compile_large_static_metadata_unwind_row_source(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
    path: &str,
) -> Result<Option<GraphQuery>, CoreError> {
    let Some(site) = first_static_unwind_site(single_query) else {
        return Ok(None);
    };
    let StaticUnwindSite::SinglePart {
        reading_clause_index,
    } = site
    else {
        return Ok(None);
    };
    if reading_clause_index == 0 {
        return Ok(None);
    }
    let SingleQueryKind::SinglePart(single_part) = &single_query.kind else {
        return Ok(None);
    };
    let unwind = static_unwind_at_site(single_query, site)?;
    if !static_unwind_expression_uses_graph_metadata(unwind, context)?
        || first_static_label_type_alternative_site(single_query, context)?.is_some()
    {
        return Ok(None);
    }

    let variable = dynamic_unwind_variable_name(unwind, context);
    validate_static_unwind_scope(single_query, site, &variable, path)?;
    let reading_clause_path = static_unwind_reading_clause_path(path, site);
    let metadata_plan = compile_static_unwind_metadata_plan(single_query, site, context, path)?;
    let value = compile_static_unwind_values(
        unwind,
        &reading_clause_path,
        metadata_plan.as_ref(),
        context,
    )?;
    if value.literals.len() <= MAX_STATIC_UNWIND_BRANCHES {
        return Ok(None);
    }

    let expression_path = format!("{reading_clause_path}.unwind.expression");
    let element_type = literal_unwind_row_source_element_type(&value.literals, &expression_path)?;
    let final_plan = compile_large_static_metadata_unwind_final_plan(
        single_part,
        reading_clause_index,
        &variable,
        value.presence_variable,
        context,
        path,
    )?;

    Ok(Some(GraphQuery::UnwindPipeline(GraphUnwindPipeline {
        unwind: GraphUnwind {
            input: None,
            list: ScalarExpression::TypedLiteralList {
                literals: value.literals,
                element_type,
            },
            element_type,
            variable: variable.clone(),
            projections: vec![GraphUnwindProjection::Variable { alias: variable }],
        },
        final_plan,
    })))
}

fn compile_large_static_metadata_unwind_final_plan(
    single_part: &SinglePartQuery,
    reading_clause_index: usize,
    variable: &str,
    presence_variable: Option<String>,
    context: &CypherCompileContext,
    path: &str,
) -> Result<GraphPlan, CoreError> {
    let mut plan = GraphPlan::default();
    let mut state = compile_state_for_single_part(single_part, context);
    let prefix = single_part
        .reading_clauses
        .get(..reading_clause_index)
        .ok_or_else(|| CoreError::internal("metadata UNWIND prefix was out of bounds"))?;
    compile_reading_clauses_into(prefix, "match", &mut plan, &mut state, context)?;

    state.scalar_aliases.push(Projection::Expression {
        expression: ScalarExpression::StageValue {
            alias: variable.to_string(),
        },
        alias: variable.to_string(),
    });

    let suffix_start = reading_clause_index.saturating_add(1);
    let suffix = single_part
        .reading_clauses
        .get(suffix_start..)
        .ok_or_else(|| CoreError::internal("metadata UNWIND suffix was out of bounds"))?;
    if !suffix.is_empty() {
        compile_reading_clauses_into(suffix, "match", &mut plan, &mut state, context)?;
    }
    let return_clause = return_clause_from_single_part(single_part, path)?;
    compile_return(return_clause, &mut plan, &state, context)?;
    reject_ignored_path_variable_references(&plan, &state, "return")?;
    if let Some(presence_variable) = presence_variable {
        apply_required_presence_predicates(&mut plan, &BTreeSet::from([presence_variable]));
    }
    Ok(plan)
}

fn compile_literal_unwind_terminal_with_row_source(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
    path: &str,
) -> Result<Option<GraphQuery>, CoreError> {
    let SingleQueryKind::MultiPart(multi_part) = &single_query.kind else {
        return Ok(None);
    };
    let [part] = multi_part.parts.as_slice() else {
        return Ok(None);
    };
    let [ReadingClause::Unwind(unwind)] = part.reading_clauses.as_slice() else {
        return Ok(None);
    };
    if !part.updating_clauses.is_empty() || !multi_part.final_part.reading_clauses.is_empty() {
        return Ok(None);
    }

    let variable = dynamic_unwind_variable_name(unwind, context);
    let Some(list) = compile_optional_literal_unwind_row_source_list(
        &unwind.expression,
        format!("{path}.parts[0].reading_clauses[0].unwind.expression"),
        context,
    )?
    else {
        return Ok(None);
    };
    let element_type = graph_unwind_list_element_type(&list)?;
    let unwind = GraphUnwind {
        input: None,
        list,
        element_type,
        variable: variable.clone(),
        projections: vec![GraphUnwindProjection::Variable {
            alias: variable.clone(),
        }],
    };
    let mut state = CypherCompileState::default();
    state.scalar_aliases.push(Projection::Expression {
        expression: ScalarExpression::StageValue { alias: variable },
        alias: unwind.variable.clone(),
    });
    record_unwind_list_alias(&mut state, &unwind.variable, element_type);
    let mut final_plan = GraphPlan::default();
    compile_unwind_terminal_with_clause(&part.with, &mut final_plan, &state, context)?;
    let return_clause = return_clause_from_single_part(&multi_part.final_part, "final_part")?;
    apply_terminal_return_projection_aliases(
        return_clause,
        &mut final_plan,
        &state,
        context,
        part.with.star,
    )?;
    apply_terminal_return_modifiers(return_clause, &mut final_plan, context)?;
    reject_ignored_path_variable_references(&final_plan, &state, "with")?;

    Ok(Some(GraphQuery::UnwindPipeline(GraphUnwindPipeline {
        unwind,
        final_plan,
    })))
}

fn compile_unwind_terminal_with_clause(
    with: &With,
    plan: &mut GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    if with.star {
        return Err(unsupported(
            "with.star",
            "WITH * over UNWIND row sources requires broader row-source scope planning and is not supported yet",
        ));
    }
    plan.distinct = with.distinct;
    if with.items.is_empty() {
        return Err(unsupported(
            "with.items",
            "WITH must include at least one projection",
        ));
    }

    let mut aliases = BTreeSet::new();
    for (index, item) in with.items.iter().enumerate() {
        let projection = if let Some(projection) =
            compile_optional_scalar_alias_return_item(item, state, format!("with.items[{index}]"))?
        {
            projection
        } else {
            compile_projection(item, format!("with.items[{index}]"), context, plan, state)?
        };
        let alias = projection.output_name();
        if !aliases.insert(alias.clone()) {
            return Err(unsupported(
                format!("with.items[{index}].alias"),
                format!("WITH projection alias '{alias}' is defined more than once"),
            ));
        }
        plan.projections.push(projection);
    }
    if let Some(where_clause) = &with.where_clause {
        plan.post_projection_predicate = Some(compile_projection_predicate_expression(
            where_clause,
            "with.where",
            context,
        )?);
    }
    if let Some(order) = &with.order {
        for (index, item) in order.items.iter().enumerate() {
            plan.order_by.push(OrderKey {
                expression: compile_terminal_alias_order_expression(
                    &item.expression,
                    &plan.projections,
                    format!("with.order.items[{index}].expression"),
                )?,
                direction: match item.direction {
                    Some(SortDirection::Descending) => OrderDirection::Descending,
                    Some(SortDirection::Ascending) | None => OrderDirection::Ascending,
                },
                nulls: context.order_null_placement(item),
            });
        }
    }
    if let Some(skip) = &with.skip {
        plan.skip = Some(compile_skip(skip, "with.skip", context)?);
    }
    if let Some(limit) = &with.limit {
        plan.limit = Some(compile_limit(limit, "with.limit", context)?);
    }
    Ok(())
}

fn compile_unwind_terminal_query(
    input: Option<GraphUnwindInput>,
    list: ScalarExpression,
    element_type: LiteralListElementType,
    variable: String,
    return_clause: &Return,
    context: &CypherCompileContext,
) -> Result<GraphQuery, CoreError> {
    if let Some(projections) =
        compile_literal_unwind_row_source_projections(return_clause, &variable)
    {
        return Ok(GraphQuery::Unwind(GraphUnwind {
            input,
            list,
            element_type,
            variable,
            projections,
        }));
    }

    let unwind = GraphUnwind {
        input,
        list,
        element_type,
        variable: variable.clone(),
        projections: vec![GraphUnwindProjection::Variable {
            alias: variable.clone(),
        }],
    };
    let mut final_plan = GraphPlan::default();
    let mut final_state = CypherCompileState::default();
    final_state.scalar_aliases.push(Projection::Expression {
        expression: ScalarExpression::StageValue {
            alias: variable.clone(),
        },
        alias: variable,
    });
    record_unwind_list_alias(&mut final_state, &unwind.variable, element_type);
    compile_return(return_clause, &mut final_plan, &final_state, context)?;
    reject_ignored_path_variable_references(&final_plan, &final_state, "return")?;

    Ok(GraphQuery::UnwindPipeline(GraphUnwindPipeline {
        unwind,
        final_plan,
    }))
}

fn record_unwind_list_alias(
    state: &mut CypherCompileState,
    variable: &str,
    element_type: LiteralListElementType,
) {
    if let Some(inner_type) = element_type.list_element_type() {
        state
            .list_alias_element_types
            .insert(variable.to_string(), inner_type);
    }
}

pub(super) fn dynamic_unwind_variable_name(
    unwind: &Unwind,
    context: &CypherCompileContext,
) -> String {
    context
        .unwind_variable(unwind)
        .map_or_else(|| variable_name(&unwind.variable), ToString::to_string)
}

fn compile_dynamic_unwind_source_expression(
    unwind: &Unwind,
    path: impl Into<String>,
    input_types: &BTreeMap<String, LiteralListElementType>,
    context: &CypherCompileContext,
) -> Result<Option<DynamicUnwindListExpression>, CoreError> {
    let path = path.into();
    if let Some(source) = context.unwind_expression_source(unwind) {
        let (expression, fragment_context) =
            parse_cypher_expression_fragment(source, path.clone(), context)?;
        return compile_dynamic_unwind_list_expression(
            &expression,
            path,
            input_types,
            &fragment_context,
        );
    }
    compile_dynamic_unwind_list_expression(&unwind.expression, path, input_types, context)
}

type DynamicUnwindInput = (GraphUnwindInput, BTreeMap<String, LiteralListElementType>);

fn compile_dynamic_unwind_input(
    multi_part: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<DynamicUnwindInput>, CoreError> {
    let [part] = multi_part.parts.as_slice() else {
        return Ok(None);
    };
    if !part.reading_clauses.is_empty()
        || !part.updating_clauses.is_empty()
        || part.with.distinct
        || part.with.star
        || part.with.where_clause.is_some()
        || part.with.order.is_some()
        || part.with.skip.is_some()
        || part.with.limit.is_some()
        || part.with.items.is_empty()
    {
        return Ok(None);
    }

    let mut projections = Vec::with_capacity(part.with.items.len());
    let mut aliases = BTreeMap::new();
    for (index, item) in part.with.items.iter().enumerate() {
        let Some(alias) = item.alias.as_ref().map(variable_name) else {
            return Ok(None);
        };
        let path = format!("parts[0].with.items[{index}].expression");
        let Some(value) =
            compile_optional_static_list_value(&item.expression, path.clone(), None, context)?
        else {
            return Ok(None);
        };
        let (expression, element_type) = graph_unwind_static_list_expression(value, &path)?;
        match aliases.entry(alias.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(element_type);
            }
            Entry::Occupied(_) => {
                return Err(unsupported(
                    format!("parts[0].with.items[{index}].alias"),
                    format!("WITH alias '{alias}' is projected more than once"),
                ));
            }
        }
        projections.push(GraphUnwindInputProjection {
            expression,
            alias,
            element_type,
        });
    }

    Ok(Some((GraphUnwindInput { projections }, aliases)))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DynamicUnwindListExpression {
    expression: ScalarExpression,
    element_type: LiteralListElementType,
}

fn compile_dynamic_unwind_list_expression(
    expression: &Expression,
    path: impl Into<String>,
    input_types: &BTreeMap<String, LiteralListElementType>,
    context: &CypherCompileContext,
) -> Result<Option<DynamicUnwindListExpression>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_dynamic_unwind_list_expression(inner, path, input_types, context)
        }
        Expression::Variable(variable) => {
            let alias = variable_name(variable);
            let Some(element_type) = input_types.get(&alias).copied() else {
                return Ok(None);
            };
            Ok(Some(DynamicUnwindListExpression {
                expression: ScalarExpression::StageValue { alias },
                element_type,
            }))
        }
        Expression::BinaryOp {
            op: CypherBinaryOperator::Add,
            lhs,
            rhs,
            ..
        } => {
            let Some(left) = compile_dynamic_unwind_list_expression(
                lhs,
                format!("{path}.lhs"),
                input_types,
                context,
            )?
            else {
                return Ok(None);
            };
            let Some(right) = compile_dynamic_unwind_list_expression(
                rhs,
                format!("{path}.rhs"),
                input_types,
                context,
            )?
            else {
                return Ok(None);
            };
            if left.element_type != right.element_type {
                return Err(unsupported(
                    path,
                    "UNWIND list alias concatenation requires both operands to have the same element type",
                ));
            }
            Ok(Some(DynamicUnwindListExpression {
                expression: ScalarExpression::ListConcat {
                    left: Box::new(left.expression),
                    right: Box::new(right.expression),
                },
                element_type: left.element_type,
            }))
        }
        _ => {
            let Some(value) =
                compile_optional_static_list_value(expression, path.clone(), None, context)?
            else {
                return Ok(None);
            };
            let (expression, element_type) = graph_unwind_static_list_expression(value, &path)?;
            Ok(Some(DynamicUnwindListExpression {
                expression,
                element_type,
            }))
        }
    }
}

fn graph_unwind_static_list_expression(
    value: StaticListValue,
    path: &str,
) -> Result<(ScalarExpression, LiteralListElementType), CoreError> {
    let element_type = value.element_type.map_or_else(
        || literal_unwind_row_source_element_type(&value.literals, path),
        Ok,
    )?;
    let expression = presence_gate_scalar_expression(
        value.presence_variable,
        ScalarExpression::TypedLiteralList {
            literals: value.literals,
            element_type,
        },
    );
    Ok((expression, element_type))
}

fn graph_unwind_list_element_type(
    expression: &ScalarExpression,
) -> Result<LiteralListElementType, CoreError> {
    match expression {
        ScalarExpression::TypedLiteralList { element_type, .. } => Ok(*element_type),
        _ => Err(CoreError::internal(
            "UNWIND row source requires a typed literal-list expression",
        )),
    }
}

fn compile_literal_unwind_row_source_projections(
    return_clause: &Return,
    variable: &str,
) -> Option<Vec<GraphUnwindProjection>> {
    if return_clause.star
        || return_clause.distinct
        || return_clause.order.is_some()
        || return_clause.skip.is_some()
        || return_clause.limit.is_some()
        || return_clause.items.len() != 1
    {
        return None;
    }

    let item = return_clause.items.first()?;
    let projected_variable = expression_variable_name(&item.expression)?;
    if projected_variable != variable {
        return None;
    }

    Some(vec![GraphUnwindProjection::Variable {
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| variable.to_string(), variable_name),
    }])
}

fn compile_optional_literal_unwind_row_source_list(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_literal_unwind_row_source_list(inner, path, context)
        }
        Expression::Literal(CypherLiteral::List(list)) => {
            let literals = list
                .elements
                .iter()
                .enumerate()
                .map(|(index, expression)| {
                    compile_literal_list_element(expression, format!("{path}[{index}]"), context)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let element_type = literal_unwind_row_source_element_type(&literals, &path)?;
            Ok(Some(ScalarExpression::TypedLiteralList {
                literals,
                element_type,
            }))
        }
        _ => Ok(None),
    }
}

fn literal_unwind_row_source_element_type(
    literals: &[Literal],
    path: &str,
) -> Result<LiteralListElementType, CoreError> {
    let mut contains_nested_list = false;
    let mut contains_scalar = false;
    for literal in literals {
        match literal {
            Literal::List(values) => {
                contains_nested_list = true;
                if values.iter().any(|value| !matches!(value, Literal::Null))
                    && infer_scalar_literal_list_element_type(values).is_none()
                {
                    return Err(unsupported(
                        path,
                        "literal UNWIND row-source nested lists require each non-empty nested list to have one scalar element type",
                    ));
                }
            }
            Literal::Null => {}
            Literal::String(_) | Literal::Integer(_) | Literal::Float(_) | Literal::Boolean(_) => {
                contains_scalar = true;
            }
        }
    }
    if contains_nested_list && contains_scalar {
        return Err(unsupported(
            path,
            "literal UNWIND row-source lists cannot mix nested-list elements with scalar elements",
        ));
    }
    if let Some(element_type) = infer_literal_list_element_type(literals) {
        return Ok(element_type);
    }
    if literals
        .iter()
        .all(|literal| matches!(literal, Literal::Null))
    {
        return Ok(LiteralListElementType::Integer);
    }
    if contains_nested_list && !contains_scalar {
        return Ok(LiteralListElementType::IntegerList);
    }
    Err(unsupported(
        path,
        "literal UNWIND row-source lists require all non-null elements to have the same scalar or nested-list element type",
    ))
}

pub(super) fn expand_single_query_static_unwinds(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
    path: &str,
) -> Result<Vec<ExpandedSingleQuery>, CoreError> {
    let mut expanded = vec![ExpandedSingleQuery {
        query: single_query.clone(),
        force_empty: false,
        required_presences: BTreeSet::new(),
    }];

    loop {
        let mut progressed = false;
        let mut next = Vec::new();
        for variant in expanded {
            let Some(site) = first_static_unwind_site(&variant.query) else {
                next.push(variant);
                continue;
            };
            let unwind = static_unwind_at_site(&variant.query, site)?;
            if static_unwind_expression_uses_graph_metadata(unwind, context)?
                && first_static_label_type_alternative_site(&variant.query, context)?.is_some()
            {
                progressed = true;
                let pattern_variants =
                    expand_single_query_pattern_alternatives(&variant.query, context)?;
                for pattern_variant in pattern_variants {
                    if next.len() >= MAX_PATTERN_ALTERNATIVE_BRANCHES {
                        return Err(unsupported(
                            path,
                            format!(
                                "static branch expansion produced more than {MAX_PATTERN_ALTERNATIVE_BRANCHES} branches; simplify the query or split it explicitly"
                            ),
                        ));
                    }
                    next.push(ExpandedSingleQuery {
                        query: pattern_variant.query,
                        force_empty: variant.force_empty || pattern_variant.force_empty,
                        required_presences: variant.required_presences.clone(),
                    });
                }
                continue;
            }
            progressed = true;
            let alternatives = expand_static_unwind_at_site(&variant, site, context, path)?;
            for alternative in alternatives {
                if next.len() >= MAX_STATIC_UNWIND_BRANCHES {
                    return Err(unsupported(
                        path,
                        format!(
                            "static UNWIND expands to more than {MAX_STATIC_UNWIND_BRANCHES} branches; use a smaller static list or split the query explicitly"
                        ),
                    ));
                }
                next.push(alternative);
            }
        }
        expanded = next;
        if !progressed {
            return Ok(expanded);
        }
    }
}

fn first_static_unwind_site(single_query: &SingleQuery) -> Option<StaticUnwindSite> {
    match &single_query.kind {
        SingleQueryKind::SinglePart(query) => query
            .reading_clauses
            .iter()
            .position(|clause| matches!(clause, ReadingClause::Unwind(_)))
            .map(|reading_clause_index| StaticUnwindSite::SinglePart {
                reading_clause_index,
            }),
        SingleQueryKind::MultiPart(query) => {
            for (part_index, part) in query.parts.iter().enumerate() {
                if let Some(reading_clause_index) = part
                    .reading_clauses
                    .iter()
                    .position(|clause| matches!(clause, ReadingClause::Unwind(_)))
                {
                    return Some(StaticUnwindSite::MultiPart {
                        query_part: MultiPartAlternativePart::Part(part_index),
                        reading_clause_index,
                    });
                }
            }
            query
                .final_part
                .reading_clauses
                .iter()
                .position(|clause| matches!(clause, ReadingClause::Unwind(_)))
                .map(|reading_clause_index| StaticUnwindSite::MultiPart {
                    query_part: MultiPartAlternativePart::FinalPart,
                    reading_clause_index,
                })
        }
    }
}

fn multi_part_query_contains_unwind(query: &MultiPartQuery) -> bool {
    query.parts.iter().any(|part| {
        part.reading_clauses
            .iter()
            .any(|clause| matches!(clause, ReadingClause::Unwind(_)))
    }) || query
        .final_part
        .reading_clauses
        .iter()
        .any(|clause| matches!(clause, ReadingClause::Unwind(_)))
}

pub(super) fn single_query_contains_unwind(single_query: &SingleQuery) -> bool {
    match &single_query.kind {
        SingleQueryKind::SinglePart(query) => query
            .reading_clauses
            .iter()
            .any(|clause| matches!(clause, ReadingClause::Unwind(_))),
        SingleQueryKind::MultiPart(query) => multi_part_query_contains_unwind(query),
    }
}

fn expand_static_unwind_at_site(
    variant: &ExpandedSingleQuery,
    site: StaticUnwindSite,
    context: &CypherCompileContext,
    path: &str,
) -> Result<Vec<ExpandedSingleQuery>, CoreError> {
    let unwind = static_unwind_at_site(&variant.query, site)?;
    let variable = variable_name(&unwind.variable);
    validate_static_unwind_scope(&variant.query, site, &variable, path)?;
    let reading_clause_path = static_unwind_reading_clause_path(path, site);
    let metadata_plan = compile_static_unwind_metadata_plan(&variant.query, site, context, path)?;
    let value = compile_static_unwind_values(
        unwind,
        &reading_clause_path,
        metadata_plan.as_ref(),
        context,
    )?;

    if value.literals.is_empty() {
        return Ok(vec![expand_static_unwind_literal_branch(
            variant,
            site,
            &variable,
            &Literal::Null,
            true,
            value.presence_variable.as_deref(),
        )?]);
    }

    value
        .literals
        .iter()
        .map(|literal| {
            expand_static_unwind_literal_branch(
                variant,
                site,
                &variable,
                literal,
                variant.force_empty,
                value.presence_variable.as_deref(),
            )
        })
        .collect()
}

fn compile_static_unwind_metadata_plan(
    query: &SingleQuery,
    site: StaticUnwindSite,
    context: &CypherCompileContext,
    path: &str,
) -> Result<Option<GraphPlan>, CoreError> {
    let (reading_clauses, prefix_len, path) = match (&query.kind, site) {
        (
            SingleQueryKind::SinglePart(single_part),
            StaticUnwindSite::SinglePart {
                reading_clause_index,
            },
        ) => (
            single_part.reading_clauses.as_slice(),
            reading_clause_index,
            format!("{path}.reading_clauses"),
        ),
        _ => return Ok(None),
    };
    if prefix_len == 0 {
        return Ok(None);
    }
    let mut plan = GraphPlan::default();
    let mut state = CypherCompileState::default();
    let prefix = reading_clauses.get(..prefix_len).ok_or_else(|| {
        CoreError::internal("static UNWIND metadata plan prefix exceeded reading clauses")
    })?;
    compile_reading_clauses_into(prefix, path, &mut plan, &mut state, context)?;
    Ok(Some(plan))
}

fn static_unwind_at_site(
    single_query: &SingleQuery,
    site: StaticUnwindSite,
) -> Result<&Unwind, CoreError> {
    let reading_clauses = match (&single_query.kind, site) {
        (SingleQueryKind::SinglePart(single_part), StaticUnwindSite::SinglePart { .. }) => {
            &single_part.reading_clauses
        }
        (
            SingleQueryKind::MultiPart(multi_part),
            StaticUnwindSite::MultiPart {
                query_part: MultiPartAlternativePart::Part(index),
                ..
            },
        ) => {
            &multi_part
                .parts
                .get(index)
                .ok_or_else(|| {
                    CoreError::internal("static UNWIND multipart site is out of bounds")
                })?
                .reading_clauses
        }
        (
            SingleQueryKind::MultiPart(multi_part),
            StaticUnwindSite::MultiPart {
                query_part: MultiPartAlternativePart::FinalPart,
                ..
            },
        ) => &multi_part.final_part.reading_clauses,
        (SingleQueryKind::SinglePart(_), StaticUnwindSite::MultiPart { .. }) => {
            return Err(CoreError::internal(
                "multipart static UNWIND site applied to single-part query",
            ));
        }
        (SingleQueryKind::MultiPart(_), StaticUnwindSite::SinglePart { .. }) => {
            return Err(CoreError::internal(
                "single-part static UNWIND site applied to multipart query",
            ));
        }
    };
    let reading_clause_index = static_unwind_reading_clause_index(site);
    let ReadingClause::Unwind(unwind) = reading_clauses
        .get(reading_clause_index)
        .ok_or_else(|| CoreError::internal("static UNWIND site was out of bounds"))?
    else {
        return Err(CoreError::internal(
            "static UNWIND site did not point at UNWIND",
        ));
    };
    Ok(unwind)
}

fn static_unwind_reading_clause_index(site: StaticUnwindSite) -> usize {
    match site {
        StaticUnwindSite::SinglePart {
            reading_clause_index,
        }
        | StaticUnwindSite::MultiPart {
            reading_clause_index,
            ..
        } => reading_clause_index,
    }
}

fn static_unwind_reading_clause_path(path: &str, site: StaticUnwindSite) -> String {
    let reading_clause_index = static_unwind_reading_clause_index(site);
    match site {
        StaticUnwindSite::SinglePart { .. } => {
            format!("{path}.reading_clauses[{reading_clause_index}]")
        }
        StaticUnwindSite::MultiPart {
            query_part: MultiPartAlternativePart::Part(part_index),
            ..
        } => format!("{path}.parts[{part_index}].reading_clauses[{reading_clause_index}]"),
        StaticUnwindSite::MultiPart {
            query_part: MultiPartAlternativePart::FinalPart,
            ..
        } => format!("{path}.final_part.reading_clauses[{reading_clause_index}]"),
    }
}

fn compile_static_unwind_values(
    unwind: &Unwind,
    reading_clause_path: &str,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<StaticUnwindValue, CoreError> {
    let expression_path = format!("{reading_clause_path}.unwind.expression");
    let value = if let Some(source) = context.unwind_expression_source(unwind) {
        compile_static_unwind_value_source(source, expression_path.clone(), plan, context)?
    } else {
        compile_optional_static_unwind_value(
            &unwind.expression,
            expression_path.clone(),
            plan,
            context,
        )?
    }
    .ok_or_else(|| {
        unsupported(
            expression_path.clone(),
            "UNWIND currently supports literal lists, list parameters, and folded static list expressions; dynamic graph property lists require row-source planning",
        )
    })?;
    Ok(StaticUnwindValue {
        presence_variable: value.presence_variable,
        literals: value.literals,
    })
}

fn static_unwind_expression_uses_graph_metadata(
    unwind: &Unwind,
    context: &CypherCompileContext,
) -> Result<bool, CoreError> {
    if let Some(source) = context.unwind_expression_source(unwind) {
        let (expression, _) =
            parse_cypher_expression_fragment(source, "unwind.expression", context)?;
        return Ok(expression_uses_graph_metadata_list(&expression));
    }
    Ok(expression_uses_graph_metadata_list(&unwind.expression))
}

fn compile_static_unwind_value_source(
    source: &str,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<StaticListValue>, CoreError> {
    let path = path.into();
    let (expression, fragment_context) =
        parse_cypher_expression_fragment(source, path.clone(), context)?;
    compile_optional_static_unwind_value(&expression, path, plan, &fragment_context)
}

fn compile_optional_static_unwind_value(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<StaticListValue>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_static_unwind_value(inner, path, plan, context)
        }
        Expression::Case(case) => {
            compile_optional_static_unwind_case_value(case, path, plan, context)
        }
        Expression::ListSlice {
            list, start, end, ..
        } => {
            if let Some(value) =
                compile_optional_static_unwind_value(list, format!("{path}.list"), plan, context)?
            {
                return slice_static_list_value(
                    value,
                    start.as_deref(),
                    end.as_deref(),
                    path,
                    context,
                )
                .map(Some);
            }
            compile_optional_static_list_value(expression, path, plan, context)
        }
        _ => compile_optional_static_list_value(expression, path, plan, context),
    }
}

fn compile_optional_static_unwind_case_value(
    case: &CaseExpression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<StaticListValue>, CoreError> {
    compile_optional_static_folded_case_list_value(
        case,
        path,
        plan,
        context,
        "UNWIND over list-valued CASE expressions requires statically foldable WHEN predicates",
    )
}

fn validate_static_unwind_scope(
    query: &SingleQuery,
    site: StaticUnwindSite,
    variable: &str,
    path: &str,
) -> Result<(), CoreError> {
    match (&query.kind, site) {
        (
            SingleQueryKind::SinglePart(single_part),
            StaticUnwindSite::SinglePart {
                reading_clause_index,
            },
        ) => validate_static_unwind_single_part_scope(
            &single_part.reading_clauses,
            reading_clause_index,
            variable,
            path,
        ),
        (
            SingleQueryKind::MultiPart(multi_part),
            StaticUnwindSite::MultiPart {
                query_part,
                reading_clause_index,
            },
        ) => validate_static_unwind_multi_part_scope(
            multi_part,
            query_part,
            reading_clause_index,
            variable,
            path,
        ),
        (SingleQueryKind::SinglePart(_), StaticUnwindSite::MultiPart { .. }) => Err(
            CoreError::internal("multipart static UNWIND scope applied to single-part query"),
        ),
        (SingleQueryKind::MultiPart(_), StaticUnwindSite::SinglePart { .. }) => Err(
            CoreError::internal("single-part static UNWIND scope applied to multipart query"),
        ),
    }
}

fn validate_static_unwind_single_part_scope(
    reading_clauses: &[ReadingClause],
    index: usize,
    variable: &str,
    path: &str,
) -> Result<(), CoreError> {
    let target_path = format!("{path}.reading_clauses[{index}]");
    validate_static_unwind_prior_reading_clauses(
        reading_clauses.iter().take(index).enumerate(),
        variable,
        &target_path,
    )?;
    validate_static_unwind_later_reading_clauses(
        reading_clauses.iter().enumerate().skip(index + 1),
        variable,
        path,
    )
}

fn validate_static_unwind_multi_part_scope(
    query: &MultiPartQuery,
    query_part: MultiPartAlternativePart,
    index: usize,
    variable: &str,
    path: &str,
) -> Result<(), CoreError> {
    let target_path = static_unwind_reading_clause_path(
        path,
        StaticUnwindSite::MultiPart {
            query_part,
            reading_clause_index: index,
        },
    );
    match query_part {
        MultiPartAlternativePart::Part(part_index) => {
            for part in query.parts.iter().take(part_index) {
                validate_static_unwind_prior_reading_clauses(
                    part.reading_clauses.iter().enumerate(),
                    variable,
                    &target_path,
                )?;
                if with_projects_variable(&part.with, variable) {
                    return Err(unsupported(
                        target_path.as_str(),
                        format!(
                            "UNWIND variable '{variable}' is already bound before this reading clause"
                        ),
                    ));
                }
            }
            let part = query.parts.get(part_index).ok_or_else(|| {
                CoreError::internal("static UNWIND multipart scope is out of bounds")
            })?;
            validate_static_unwind_prior_reading_clauses(
                part.reading_clauses.iter().take(index).enumerate(),
                variable,
                &target_path,
            )?;
            validate_static_unwind_later_reading_clauses(
                part.reading_clauses.iter().enumerate().skip(index + 1),
                variable,
                &format!("{path}.parts[{part_index}]"),
            )?;
            for (later_index, part) in query.parts.iter().enumerate().skip(part_index + 1) {
                validate_static_unwind_later_reading_clauses(
                    part.reading_clauses.iter().enumerate(),
                    variable,
                    &format!("{path}.parts[{later_index}]"),
                )?;
            }
            validate_static_unwind_later_reading_clauses(
                query.final_part.reading_clauses.iter().enumerate(),
                variable,
                &format!("{path}.final_part"),
            )
        }
        MultiPartAlternativePart::FinalPart => {
            for part in &query.parts {
                validate_static_unwind_prior_reading_clauses(
                    part.reading_clauses.iter().enumerate(),
                    variable,
                    &target_path,
                )?;
                if with_projects_variable(&part.with, variable) {
                    return Err(unsupported(
                        target_path.as_str(),
                        format!(
                            "UNWIND variable '{variable}' is already bound before this reading clause"
                        ),
                    ));
                }
            }
            validate_static_unwind_single_part_scope(
                &query.final_part.reading_clauses,
                index,
                variable,
                &format!("{path}.final_part"),
            )
        }
    }
}

fn validate_static_unwind_prior_reading_clauses<'a>(
    clauses: impl Iterator<Item = (usize, &'a ReadingClause)>,
    variable: &str,
    target_path: &str,
) -> Result<(), CoreError> {
    for (clause_index, clause) in clauses {
        if reading_clause_binds_variable(clause, variable) {
            return Err(unsupported(
                format!("{target_path}.unwind.variable"),
                format!(
                    "UNWIND variable '{variable}' is already bound before reading clause {clause_index}"
                ),
            ));
        }
    }
    Ok(())
}

fn validate_static_unwind_later_reading_clauses<'a>(
    clauses: impl Iterator<Item = (usize, &'a ReadingClause)>,
    variable: &str,
    path: &str,
) -> Result<(), CoreError> {
    for (clause_index, clause) in clauses {
        if reading_clause_binds_variable(clause, variable) {
            return Err(unsupported(
                format!("{path}.reading_clauses[{clause_index}]"),
                format!(
                    "static UNWIND variable '{variable}' cannot be rebound by a later reading clause"
                ),
            ));
        }
    }
    Ok(())
}

fn expand_static_unwind_literal_branch(
    variant: &ExpandedSingleQuery,
    site: StaticUnwindSite,
    variable: &str,
    literal: &Literal,
    force_empty: bool,
    presence_variable: Option<&str>,
) -> Result<ExpandedSingleQuery, CoreError> {
    let mut query = variant.query.clone();
    substitute_static_unwind_literal(&mut query, site, variable, literal)?;
    let mut required_presences = variant.required_presences.clone();
    if let Some(presence_variable) = presence_variable {
        required_presences.insert(presence_variable.to_string());
    }
    Ok(ExpandedSingleQuery {
        query,
        force_empty,
        required_presences,
    })
}

fn substitute_static_unwind_literal(
    query: &mut SingleQuery,
    site: StaticUnwindSite,
    variable: &str,
    literal: &Literal,
) -> Result<(), CoreError> {
    match (&mut query.kind, site) {
        (
            SingleQueryKind::SinglePart(single_part),
            StaticUnwindSite::SinglePart {
                reading_clause_index,
            },
        ) => {
            single_part.reading_clauses.remove(reading_clause_index);
            substitute_static_unwind_literal_in_single_part(
                single_part,
                reading_clause_index,
                variable,
                literal,
            );
            Ok(())
        }
        (
            SingleQueryKind::MultiPart(multi_part),
            StaticUnwindSite::MultiPart {
                query_part,
                reading_clause_index,
            },
        ) => substitute_static_unwind_literal_in_multi_part(
            multi_part,
            query_part,
            reading_clause_index,
            variable,
            literal,
        ),
        (SingleQueryKind::SinglePart(_), StaticUnwindSite::MultiPart { .. }) => Err(
            CoreError::internal("multipart static UNWIND branch applied to single-part query"),
        ),
        (SingleQueryKind::MultiPart(_), StaticUnwindSite::SinglePart { .. }) => Err(
            CoreError::internal("single-part static UNWIND branch applied to multipart query"),
        ),
    }
}

fn substitute_static_unwind_literal_in_single_part(
    query: &mut SinglePartQuery,
    start_index: usize,
    variable: &str,
    literal: &Literal,
) {
    let mut substitution = StaticUnwindSubstitution { variable, literal };
    for clause in query.reading_clauses.iter_mut().skip(start_index) {
        substitution.visit_reading_clause(clause);
    }
    substitution.visit_single_part_body(&mut query.body);
}

fn substitute_static_unwind_literal_in_multi_part(
    query: &mut MultiPartQuery,
    query_part: MultiPartAlternativePart,
    start_index: usize,
    variable: &str,
    literal: &Literal,
) -> Result<(), CoreError> {
    let mut substitution = StaticUnwindSubstitution { variable, literal };
    match query_part {
        MultiPartAlternativePart::Part(part_index) => {
            let part = query.parts.get_mut(part_index).ok_or_else(|| {
                CoreError::internal("static UNWIND multipart branch is out of bounds")
            })?;
            part.reading_clauses.remove(start_index);
            for clause in part.reading_clauses.iter_mut().skip(start_index) {
                substitution.visit_reading_clause(clause);
            }
            substitution.visit_with(&mut part.with);
            for part in query.parts.iter_mut().skip(part_index + 1) {
                for clause in &mut part.reading_clauses {
                    substitution.visit_reading_clause(clause);
                }
                substitution.visit_with(&mut part.with);
            }
            for clause in &mut query.final_part.reading_clauses {
                substitution.visit_reading_clause(clause);
            }
            substitution.visit_single_part_body(&mut query.final_part.body);
        }
        MultiPartAlternativePart::FinalPart => {
            query.final_part.reading_clauses.remove(start_index);
            for clause in query
                .final_part
                .reading_clauses
                .iter_mut()
                .skip(start_index)
            {
                substitution.visit_reading_clause(clause);
            }
            substitution.visit_single_part_body(&mut query.final_part.body);
        }
    }
    Ok(())
}

struct StaticUnwindSubstitution<'a> {
    variable: &'a str,
    literal: &'a Literal,
}

impl StaticUnwindSubstitution<'_> {
    fn visit_reading_clause(&mut self, clause: &mut ReadingClause) {
        match clause {
            ReadingClause::Match(match_clause) => self.visit_match(match_clause),
            ReadingClause::Unwind(unwind) => self.visit_unwind(unwind),
            ReadingClause::InQueryCall(call) => self.visit_in_query_call(call),
            ReadingClause::CallSubquery(subquery) => self.visit_call_subquery_mut(subquery),
            ReadingClause::LoadCsv(load_csv) => self.visit_load_csv_mut(load_csv),
        }
    }

    fn visit_single_part_body(&mut self, body: &mut SinglePartBody) {
        match body {
            SinglePartBody::Return(return_clause) => self.visit_return(return_clause),
            SinglePartBody::Updating {
                updating,
                return_clause,
            } => {
                for clause in updating {
                    match clause {
                        decypher::ast::query::UpdatingClause::Create(create) => {
                            self.visit_create(create);
                        }
                        decypher::ast::query::UpdatingClause::Merge(merge) => {
                            self.visit_merge(merge);
                        }
                        decypher::ast::query::UpdatingClause::Delete(delete) => {
                            self.visit_delete(delete);
                        }
                        decypher::ast::query::UpdatingClause::Set(set) => {
                            self.visit_set(set);
                        }
                        decypher::ast::query::UpdatingClause::Remove(remove) => {
                            self.visit_remove(remove);
                        }
                        decypher::ast::query::UpdatingClause::Foreach(foreach) => {
                            self.visit_foreach_mut(foreach);
                        }
                    }
                }
                if let Some(return_clause) = return_clause {
                    self.visit_return(return_clause);
                }
            }
            SinglePartBody::Finish(finish) => self.visit_finish_mut(finish),
        }
    }
}

impl VisitMut for StaticUnwindSubstitution<'_> {
    fn visit_projection_item(&mut self, item: &mut ProjectionItem) {
        if item.alias.is_none()
            && let Expression::Variable(variable) = &item.expression
            && variable_name(variable) == self.variable
        {
            item.alias = Some(variable.clone());
        }
        visit::walk_projection_item_mut(self, item);
    }

    fn visit_expression(&mut self, expression: &mut Expression) {
        if let Expression::Variable(variable) = expression
            && variable_name(variable) == self.variable
        {
            *expression = cypher_literal_expression(self.literal, variable.name.span);
            return;
        }
        visit::walk_expression_mut(self, expression);
    }
}

struct StagedCollectUnwindShape<'a> {
    part: &'a MultiPartQueryPart,
    remaining_reading_clauses: &'a [ReadingClause],
    return_clause: &'a Return,
    group_variables: Vec<String>,
    aggregate_item_index: usize,
    aggregate_alias: String,
    unwind_variable: String,
}

pub(super) fn compile_staged_collect_unwind_multi_part(
    query: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<GraphQuery>, CoreError> {
    let Some(shape) = staged_collect_unwind_multi_part_shape(query, context)? else {
        return Ok(None);
    };
    let Some((stage_plan, exports)) = compile_staged_collect_unwind_stage(query, &shape, context)?
    else {
        return Ok(None);
    };
    let aggregate_projection = stage_plan
        .projections
        .iter()
        .find(|projection| projection.output_name() == shape.aggregate_alias)
        .ok_or_else(|| CoreError::internal("staged collect projection was missing"))?;
    let binding = staged_collect_unwind_binding(
        aggregate_projection,
        &stage_plan,
        context,
        format!(
            "parts[0].with.items[{}].expression",
            shape.aggregate_item_index
        ),
    )?;
    let final_plan =
        compile_staged_collect_unwind_final_plan(&shape, &stage_plan, &binding, context)?;

    Ok(Some(GraphQuery::StagedUnwind(Box::new(
        GraphStagedUnwindQuery {
            stage: GraphStage {
                plan: stage_plan,
                exports,
            },
            unwind: GraphStagedUnwind {
                source_alias: shape.aggregate_alias,
                variable: shape.unwind_variable,
                binding,
            },
            final_plan,
        },
    ))))
}

fn staged_collect_unwind_multi_part_shape<'a>(
    query: &'a MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<StagedCollectUnwindShape<'a>>, CoreError> {
    let [part] = query.parts.as_slice() else {
        return Ok(None);
    };
    if !part.updating_clauses.is_empty()
        || part.with.distinct
        || part.with.star
        || part.with.where_clause.is_some()
        || part.with.order.is_some()
        || part.with.skip.is_some()
        || part.with.limit.is_some()
    {
        return Ok(None);
    }
    let [
        ReadingClause::Unwind(unwind),
        remaining_reading_clauses @ ..,
    ] = query.final_part.reading_clauses.as_slice()
    else {
        return Ok(None);
    };
    let return_clause = return_clause_from_single_part(&query.final_part, "final_part")?;
    let Some((group_variables, aggregate_item_index, aggregate_alias)) =
        staged_collect_unwind_with_items(&part.with, context)?
    else {
        return Ok(None);
    };
    let Expression::Variable(source_variable) = &unwind.expression else {
        return Ok(None);
    };
    if variable_name(source_variable) != aggregate_alias {
        return Ok(None);
    }
    let unwind_variable = dynamic_unwind_variable_name(unwind, context);
    if group_variables
        .iter()
        .any(|variable| variable == &unwind_variable)
        || aggregate_alias == unwind_variable
    {
        return Err(unsupported(
            "final_part.reading_clauses[0].unwind.variable",
            format!("UNWIND variable '{unwind_variable}' conflicts with a staged WITH alias"),
        ));
    }

    Ok(Some(StagedCollectUnwindShape {
        part,
        remaining_reading_clauses,
        return_clause,
        group_variables,
        aggregate_item_index,
        aggregate_alias,
        unwind_variable,
    }))
}

fn staged_collect_unwind_with_items(
    with: &With,
    context: &CypherCompileContext,
) -> Result<Option<(Vec<String>, usize, String)>, CoreError> {
    if with.items.is_empty() {
        return Ok(None);
    }
    let mut group_variables = Vec::new();
    let mut aggregate = None;
    for (index, item) in with.items.iter().enumerate() {
        if item.alias.is_none()
            && let Expression::Variable(variable) = &item.expression
        {
            group_variables.push(variable_name(variable));
            continue;
        }
        let Some(alias) = item.alias.as_ref().map(variable_name) else {
            return Ok(None);
        };
        if !staged_collect_unwind_expression_is_collect(
            &item.expression,
            format!("parts[0].with.items[{index}].expression"),
            context,
        )? {
            return Ok(None);
        }
        if aggregate.replace((index, alias)).is_some() {
            return Ok(None);
        }
    }
    let Some((aggregate_item_index, aggregate_alias)) = aggregate else {
        return Ok(None);
    };
    let mut unique = BTreeSet::new();
    if !group_variables
        .iter()
        .all(|variable| unique.insert(variable.clone()))
        || unique.contains(&aggregate_alias)
    {
        return Ok(None);
    }
    Ok(Some((
        group_variables,
        aggregate_item_index,
        aggregate_alias,
    )))
}

fn staged_collect_unwind_expression_is_collect(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<bool, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            staged_collect_unwind_expression_is_collect(inner, path, context)
        }
        Expression::FunctionCall(function) => {
            Ok(compile_aggregate_function(function, &path, context)?
                .is_some_and(|function| function == AggregateFunction::Collect))
        }
        _ => Ok(false),
    }
}

fn compile_staged_collect_unwind_stage(
    query: &MultiPartQuery,
    shape: &StagedCollectUnwindShape<'_>,
    context: &CypherCompileContext,
) -> Result<Option<(GraphPlan, Vec<GraphStageExport>)>, CoreError> {
    let mut stage_plan = GraphPlan::default();
    let mut stage_state = compile_state_for_multi_part(query, context);
    compile_reading_clauses_into(
        &shape.part.reading_clauses,
        "parts[0].match",
        &mut stage_plan,
        &mut stage_state,
        context,
    )?;
    if !stage_state.path_variables.is_empty()
        || !stage_state.relationship_element_path_variables.is_empty()
        || !stage_state.scalar_aliases.is_empty()
    {
        return Ok(None);
    }
    let visible = visible_graph_variables(&stage_plan, &stage_state);
    if !shape
        .group_variables
        .iter()
        .all(|variable| visible.contains(variable))
    {
        return Ok(None);
    }

    let mut exports = Vec::with_capacity(shape.group_variables.len() + 1);
    for variable in &shape.group_variables {
        let export_column = stage_export_column(variable);
        stage_plan.projections.push(Projection::Key {
            variable: variable.clone(),
            alias: export_column.clone(),
        });
        exports.push(GraphStageExport::NodeKey {
            variable: variable.clone(),
            column: export_column,
        });
    }
    let aggregate_item = shape
        .part
        .with
        .items
        .get(shape.aggregate_item_index)
        .ok_or_else(|| CoreError::internal("staged collect item index was out of bounds"))?;
    reject_staged_collect_unwind_list_argument(
        &aggregate_item.expression,
        format!(
            "parts[0].with.items[{}].expression",
            shape.aggregate_item_index
        ),
    )?;
    let aggregate_projection = compile_projection(
        aggregate_item,
        format!("parts[0].with.items[{}]", shape.aggregate_item_index),
        context,
        &stage_plan,
        &stage_state,
    )?;
    let Projection::Aggregate {
        function: AggregateFunction::Collect,
        ..
    } = &aggregate_projection
    else {
        return Ok(None);
    };
    let aggregate_column = aggregate_projection.output_name();
    stage_plan.projections.push(aggregate_projection);
    exports.push(GraphStageExport::AggregateValue {
        alias: shape.aggregate_alias.clone(),
        column: aggregate_column,
    });
    Ok(Some((stage_plan, exports)))
}

fn reject_staged_collect_unwind_list_argument(
    expression: &Expression,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let Some(function) = aggregate_function_call(expression) else {
        return Ok(());
    };
    let [argument] = function.arguments.as_slice() else {
        return Ok(());
    };
    if collect_unwind_argument_is_list_valued(argument) {
        return Err(unsupported(
            format!("{}.arguments[0]", path.into()),
            "UNWIND collect(...) currently requires scalar string, integer, float, boolean, property, node-key, or supported scalar-expression elements; list-valued collect elements are not supported yet",
        ));
    }
    Ok(())
}

fn collect_unwind_argument_is_list_valued(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => collect_unwind_argument_is_list_valued(inner),
        Expression::Literal(CypherLiteral::List(_))
        | Expression::ListSlice { .. }
        | Expression::ListComprehension(_) => true,
        _ => false,
    }
}

fn compile_staged_collect_unwind_final_plan(
    shape: &StagedCollectUnwindShape<'_>,
    stage_plan: &GraphPlan,
    binding: &GraphStagedUnwindBinding,
    context: &CypherCompileContext,
) -> Result<GraphPlan, CoreError> {
    let mut final_nodes = shape
        .group_variables
        .iter()
        .map(|variable| {
            stage_plan
                .nodes
                .iter()
                .find(|node| node.variable == *variable)
                .cloned()
                .ok_or_else(|| CoreError::internal("staged group variable was not a node"))
        })
        .collect::<Result<Vec<_>, CoreError>>()?;
    if let GraphStagedUnwindBinding::NodeKey { label } = binding {
        final_nodes.push(NodePattern {
            variable: shape.unwind_variable.clone(),
            label: label.clone(),
        });
    }

    let mut final_plan = GraphPlan {
        nodes: final_nodes,
        ..GraphPlan::default()
    };
    let mut final_state = CypherCompileState::default();
    if matches!(binding, GraphStagedUnwindBinding::Scalar { .. }) {
        final_state.scalar_aliases.push(Projection::Expression {
            expression: ScalarExpression::StageValue {
                alias: shape.unwind_variable.clone(),
            },
            alias: shape.unwind_variable.clone(),
        });
    }
    if !shape.remaining_reading_clauses.is_empty() {
        compile_reading_clauses_into(
            shape.remaining_reading_clauses,
            "final_part.reading_clauses",
            &mut final_plan,
            &mut final_state,
            context,
        )?;
    }
    compile_return(shape.return_clause, &mut final_plan, &final_state, context)?;
    reject_ignored_path_variable_references(&final_plan, &final_state, "final_part.return")?;
    Ok(final_plan)
}

fn staged_collect_unwind_binding(
    aggregate_projection: &Projection,
    stage_plan: &GraphPlan,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<GraphStagedUnwindBinding, CoreError> {
    let path = path.into();
    let Projection::Aggregate {
        function: AggregateFunction::Collect,
        target,
        ..
    } = aggregate_projection
    else {
        return Err(CoreError::internal(
            "staged collect UNWIND source was not a collect aggregate",
        ));
    };
    match target {
        AggregateTarget::VariableKey { variable } => {
            let node = stage_plan
                .nodes
                .iter()
                .find(|node| node.variable == *variable)
                .ok_or_else(|| {
                    unsupported(
                        path.clone(),
                        "UNWIND collect(variable) currently supports collected node variables",
                    )
                })?;
            Ok(GraphStagedUnwindBinding::NodeKey {
                label: node.label.clone(),
            })
        }
        AggregateTarget::Property(property) => Ok(GraphStagedUnwindBinding::Scalar {
            element_type: collect_unwind_property_element_type(
                property, stage_plan, context, path,
            )?,
        }),
        AggregateTarget::Expression(expression) => Ok(GraphStagedUnwindBinding::Scalar {
            element_type: collect_unwind_scalar_expression_element_type(
                expression, stage_plan, context, path,
            )?,
        }),
        AggregateTarget::PresenceGatedProperty { .. }
        | AggregateTarget::PresenceGatedVariableKey { .. } => Err(unsupported(
            path,
            "UNWIND collect(...) over optional presence-gated targets requires nullable staged row-source planning and is not supported yet",
        )),
    }
}

fn collect_unwind_scalar_expression_element_type(
    expression: &ScalarExpression,
    stage_plan: &GraphPlan,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<LiteralListElementType, CoreError> {
    let path = path.into();
    match expression {
        ScalarExpression::Literal(literal) => literal_list_element_kind(literal).ok_or_else(|| {
            unsupported(
                path,
                "UNWIND collect(NULL) requires an explicit non-null element type",
            )
        }),
        ScalarExpression::Property(property) => {
            collect_unwind_property_element_type(property, stage_plan, context, path)
        }
        ScalarExpression::Predicate(_)
        | ScalarExpression::ToBoolean { .. }
        | ScalarExpression::ToBooleanOrNull { .. }
        | ScalarExpression::IsNaN { .. } => Ok(LiteralListElementType::Boolean),
        ScalarExpression::ToString { .. }
        | ScalarExpression::ToStringOrNull { .. }
        | ScalarExpression::ToLower { .. }
        | ScalarExpression::ToUpper { .. }
        | ScalarExpression::Trim { .. }
        | ScalarExpression::LTrim { .. }
        | ScalarExpression::RTrim { .. }
        | ScalarExpression::Replace { .. }
        | ScalarExpression::Substring { .. }
        | ScalarExpression::Left { .. }
        | ScalarExpression::Right { .. }
        | ScalarExpression::Reverse { .. } => Ok(LiteralListElementType::String),
        ScalarExpression::ToInteger { .. }
        | ScalarExpression::ToIntegerOrNull { .. }
        | ScalarExpression::CharacterLength { .. } => Ok(LiteralListElementType::Integer),
        ScalarExpression::ToFloat { .. }
        | ScalarExpression::ToFloatOrNull { .. }
        | ScalarExpression::Abs { .. }
        | ScalarExpression::Ceil { .. }
        | ScalarExpression::Floor { .. }
        | ScalarExpression::Round { .. }
        | ScalarExpression::Sqrt { .. }
        | ScalarExpression::Exp { .. }
        | ScalarExpression::Log { .. }
        | ScalarExpression::Log10 { .. }
        | ScalarExpression::Sin { .. }
        | ScalarExpression::Cos { .. }
        | ScalarExpression::Tan { .. }
        | ScalarExpression::Cot { .. }
        | ScalarExpression::Asin { .. }
        | ScalarExpression::Acos { .. }
        | ScalarExpression::Atan { .. }
        | ScalarExpression::Atan2 { .. }
        | ScalarExpression::Degrees { .. }
        | ScalarExpression::Radians { .. } => Ok(LiteralListElementType::Float),
        ScalarExpression::Negate { expression } => {
            collect_unwind_scalar_expression_element_type(expression, stage_plan, context, path)
        }
        ScalarExpression::Arithmetic {
            operator,
            left,
            right,
        } => {
            if matches!(
                operator,
                ArithmeticOperator::Divide | ArithmeticOperator::Power
            ) {
                return Ok(LiteralListElementType::Float);
            }
            let left =
                collect_unwind_scalar_expression_element_type(left, stage_plan, context, &path)?;
            let right =
                collect_unwind_scalar_expression_element_type(right, stage_plan, context, &path)?;
            Ok(
                if matches!(left, LiteralListElementType::Float)
                    || matches!(right, LiteralListElementType::Float)
                {
                    LiteralListElementType::Float
                } else {
                    left
                },
            )
        }
        _ => Err(unsupported(
            path,
            "UNWIND collect(...) currently requires scalar string, integer, float, boolean, property, node-key, or supported scalar-expression elements",
        )),
    }
}

fn collect_unwind_property_element_type(
    property: &PropertyRef,
    stage_plan: &GraphPlan,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<LiteralListElementType, CoreError> {
    let path = path.into();
    let (Some(graph), Some(catalog)) = (context.graph.as_ref(), context.catalog.as_ref()) else {
        return Err(unsupported(
            path,
            "UNWIND collect(property) requires catalog-backed graph compilation so the collected element type is known",
        ));
    };
    if let Some(node_pattern) = stage_plan
        .nodes
        .iter()
        .find(|node| node.variable == property.variable)
    {
        let node = graph.node(&node_pattern.label).ok_or_else(|| {
            CoreError::internal(format!(
                "staged collect referenced unknown node label '{}'",
                node_pattern.label
            ))
        })?;
        let column = node
            .column_for_property(&property.property)
            .ok_or_else(|| {
                unsupported(
                    path.clone(),
                    format!(
                        "UNWIND collect(property) references unknown property '{}.{}'",
                        property.variable, property.property
                    ),
                )
            })?;
        let data_type =
            catalog_column_data_type(catalog, &node.table, column).ok_or_else(|| {
                unsupported(
                    path.clone(),
                    format!(
                        "UNWIND collect(property) could not resolve catalog type for '{}.{}'",
                        property.variable, property.property
                    ),
                )
            })?;
        return literal_list_element_type_for_data_type(data_type, path);
    }
    Err(unsupported(
        path,
        "UNWIND collect(relationship.property) requires relationship-property row-source typing and is not supported yet",
    ))
}
