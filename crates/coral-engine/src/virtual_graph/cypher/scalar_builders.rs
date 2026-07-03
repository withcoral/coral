//! Pure `ScalarExpression` IR construction: graph-value accessors, presence
//! gating, and statically-folded list / coalesce / case scalar nodes built from
//! value inputs (`GraphValueRef`, `SameLabelUndirectedEndpointRef`, `StaticListValue`,
//! `Literal`) — no compile context, plan, or state.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Pure scalar-expression builders intentionally inherit parent-private Cypher helpers."
)]
use super::*;

pub(super) fn graph_value_key_scalar_expression(value: GraphValueRef) -> ScalarExpression {
    let expression = ScalarExpression::Key {
        variable: value.variable,
    };
    presence_gate_scalar_expression(value.presence_variable, expression)
}

pub(super) fn graph_value_element_id_scalar_expression(value: GraphValueRef) -> ScalarExpression {
    let expression = ScalarExpression::ElementId {
        variable: value.variable,
    };
    presence_gate_scalar_expression(value.presence_variable, expression)
}

pub(super) fn graph_value_presence_scalar_expression(value: GraphValueRef) -> ScalarExpression {
    let expression = ScalarExpression::GraphPresence {
        variable: value.variable,
    };
    presence_gate_scalar_expression(value.presence_variable, expression)
}

pub(super) fn graph_value_labels_scalar_expression(
    value: GraphValueRef,
    label: String,
) -> ScalarExpression {
    let expression = ScalarExpression::NodeLabels {
        variable: value.variable,
        label,
    };
    presence_gate_scalar_expression(value.presence_variable, expression)
}

pub(super) fn graph_value_keys_scalar_expression(value: GraphValueRef) -> ScalarExpression {
    let expression = ScalarExpression::PropertyKeys {
        variable: value.variable,
    };
    presence_gate_scalar_expression(value.presence_variable, expression)
}

pub(super) fn graph_value_property_scalar_expression(
    value: GraphValueRef,
    property: String,
) -> ScalarExpression {
    let expression = ScalarExpression::Property(PropertyRef {
        variable: value.variable,
        property,
    });
    presence_gate_scalar_expression(value.presence_variable, expression)
}

pub(super) fn same_label_undirected_endpoint_key_scalar_expression(
    value: SameLabelUndirectedEndpointRef,
) -> ScalarExpression {
    ScalarExpression::UndirectedEndpointKey {
        relationship: value.relationship,
        endpoint: value.endpoint,
    }
}

pub(super) fn same_label_undirected_endpoint_element_id_scalar_expression(
    value: SameLabelUndirectedEndpointRef,
) -> ScalarExpression {
    ScalarExpression::UndirectedEndpointElementId {
        relationship: value.relationship,
        endpoint: value.endpoint,
    }
}

pub(super) fn same_label_undirected_endpoint_labels_scalar_expression(
    value: SameLabelUndirectedEndpointRef,
) -> ScalarExpression {
    ScalarExpression::UndirectedEndpointLabels {
        relationship: value.relationship,
        endpoint: value.endpoint,
        label: value.label,
    }
}

pub(super) fn same_label_undirected_endpoint_keys_scalar_expression(
    value: SameLabelUndirectedEndpointRef,
) -> ScalarExpression {
    ScalarExpression::UndirectedEndpointPropertyKeys {
        relationship: value.relationship,
        endpoint: value.endpoint,
    }
}

pub(super) fn presence_gate_scalar_expression(
    presence_variable: Option<String>,
    expression: ScalarExpression,
) -> ScalarExpression {
    match presence_variable {
        Some(presence_variable) => ScalarExpression::PresenceGated {
            presence_variable,
            expression: Box::new(expression),
        },
        None => expression,
    }
}

pub(super) fn static_list_case_result_scalar_expression(
    result: StaticListCaseResult,
    element_type: LiteralListElementType,
) -> ScalarExpression {
    match result {
        StaticListCaseResult::Null => ScalarExpression::Literal(Literal::Null),
        StaticListCaseResult::List(value) => {
            static_list_value_scalar_expression_with_element_type(value, element_type)
        }
        StaticListCaseResult::Coalesce(coalesce) => ScalarExpression::Coalesce {
            expressions: coalesce
                .arguments
                .into_iter()
                .map(|argument| match argument {
                    StaticListCoalesceArgument::Null => ScalarExpression::Literal(Literal::Null),
                    StaticListCoalesceArgument::List(value) => {
                        static_list_value_scalar_expression_with_element_type(value, element_type)
                    }
                })
                .collect(),
        },
    }
}

pub(super) fn static_list_case_result_length_scalar_expression(
    result: StaticListCaseResult,
) -> Result<ScalarExpression, CoreError> {
    match result {
        StaticListCaseResult::Null => Ok(ScalarExpression::Literal(Literal::Null)),
        StaticListCaseResult::List(value) => static_list_length_scalar_expression(value),
        StaticListCaseResult::Coalesce(coalesce) => {
            static_list_coalesce_length_scalar_expression(coalesce)
        }
    }
}

pub(super) fn static_list_case_result_endpoint_scalar_expression(
    result: StaticListCaseResult,
    endpoint: ListEndpoint,
) -> ScalarExpression {
    match result {
        StaticListCaseResult::Null => ScalarExpression::Literal(Literal::Null),
        StaticListCaseResult::List(value) => {
            static_list_value_endpoint_scalar_expression(value, endpoint)
        }
        StaticListCaseResult::Coalesce(coalesce) => {
            static_list_coalesce_endpoint_scalar_expression(coalesce, endpoint)
        }
    }
}

pub(super) fn static_list_case_result_is_empty_scalar_expression(
    result: StaticListCaseResult,
) -> ScalarExpression {
    match result {
        StaticListCaseResult::Null => ScalarExpression::Literal(Literal::Null),
        StaticListCaseResult::List(value) => static_list_is_empty_scalar_expression(value),
        StaticListCaseResult::Coalesce(coalesce) => {
            static_list_coalesce_is_empty_scalar_expression(coalesce)
        }
    }
}

pub(super) fn static_list_coalesce_endpoint_scalar_expression(
    coalesce: StaticListCoalesceArguments,
    endpoint: ListEndpoint,
) -> ScalarExpression {
    ScalarExpression::Coalesce {
        expressions: coalesce
            .arguments
            .into_iter()
            .map(|argument| match argument {
                StaticListCoalesceArgument::Null => ScalarExpression::Literal(Literal::Null),
                StaticListCoalesceArgument::List(value) => {
                    static_list_value_endpoint_scalar_expression(value, endpoint)
                }
            })
            .collect(),
    }
}

pub(super) fn static_list_coalesce_length_scalar_expression(
    coalesce: StaticListCoalesceArguments,
) -> Result<ScalarExpression, CoreError> {
    let expressions = coalesce
        .arguments
        .into_iter()
        .map(|argument| match argument {
            StaticListCoalesceArgument::Null => Ok(ScalarExpression::Literal(Literal::Null)),
            StaticListCoalesceArgument::List(value) => static_list_length_scalar_expression(value),
        })
        .collect::<Result<Vec<_>, CoreError>>()?;
    Ok(ScalarExpression::Coalesce { expressions })
}

pub(super) fn static_list_coalesce_is_empty_scalar_expression(
    coalesce: StaticListCoalesceArguments,
) -> ScalarExpression {
    ScalarExpression::Coalesce {
        expressions: coalesce
            .arguments
            .into_iter()
            .map(|argument| match argument {
                StaticListCoalesceArgument::Null => ScalarExpression::Literal(Literal::Null),
                StaticListCoalesceArgument::List(value) => {
                    static_list_is_empty_scalar_expression(value)
                }
            })
            .collect(),
    }
}

pub(super) fn static_list_value_scalar_expression_with_element_type(
    value: StaticListValue,
    element_type: LiteralListElementType,
) -> ScalarExpression {
    presence_gate_scalar_expression(
        value.presence_variable,
        ScalarExpression::TypedLiteralList {
            literals: value.literals,
            element_type: value.element_type.unwrap_or(element_type),
        },
    )
}

pub(super) fn add_one_to_path_length_scalar_expression(
    expression: ScalarExpression,
) -> Result<ScalarExpression, CoreError> {
    match expression {
        ScalarExpression::Literal(Literal::Integer(value)) => Ok(ScalarExpression::Literal(
            Literal::Integer(value.checked_add(1).ok_or_else(|| {
                CoreError::internal("path node count overflow while adding path endpoint")
            })?),
        )),
        ScalarExpression::Literal(Literal::Null) => Ok(ScalarExpression::Literal(Literal::Null)),
        ScalarExpression::PresenceGated {
            presence_variable,
            expression,
        } => Ok(ScalarExpression::PresenceGated {
            presence_variable,
            expression: Box::new(add_one_to_path_length_scalar_expression(*expression)?),
        }),
        ScalarExpression::Case {
            alternatives,
            else_expression,
        } => Ok(ScalarExpression::Case {
            alternatives: alternatives
                .into_iter()
                .map(|alternative| {
                    Ok(ScalarCaseAlternative {
                        when: alternative.when,
                        then: add_one_to_path_length_scalar_expression(alternative.then)?,
                    })
                })
                .collect::<Result<Vec<_>, CoreError>>()?,
            else_expression: else_expression
                .map(|expression| {
                    add_one_to_path_length_scalar_expression(*expression).map(Box::new)
                })
                .transpose()?,
        }),
        expression => Ok(ScalarExpression::Arithmetic {
            operator: ArithmeticOperator::Add,
            left: Box::new(expression),
            right: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
        }),
    }
}

pub(super) fn static_list_length_scalar_expression(
    value: StaticListValue,
) -> Result<ScalarExpression, CoreError> {
    Ok(presence_gate_scalar_expression(
        value.presence_variable,
        list_length_scalar_expression(value.literals.len())?,
    ))
}

pub(super) fn static_list_is_empty_scalar_expression(value: StaticListValue) -> ScalarExpression {
    presence_gate_scalar_expression(
        value.presence_variable,
        ScalarExpression::Literal(Literal::Boolean(value.literals.is_empty())),
    )
}

pub(super) fn list_length_scalar_expression(length: usize) -> Result<ScalarExpression, CoreError> {
    let length = i64::try_from(length)
        .map_err(|error| CoreError::internal(format!("literal list length overflow: {error}")))?;
    Ok(ScalarExpression::Literal(Literal::Integer(length)))
}

pub(super) fn compile_pi_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    compile_zero_scalar_function_arguments(function, path, "pi")?;
    Ok(ScalarExpression::Literal(Literal::Float(
        ordered_float::OrderedFloat(std::f64::consts::PI),
    )))
}

pub(super) fn compile_e_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    compile_zero_scalar_function_arguments(function, path, "e")?;
    Ok(ScalarExpression::Literal(Literal::Float(
        ordered_float::OrderedFloat(std::f64::consts::E),
    )))
}

pub(super) fn static_list_value_endpoint_scalar_expression(
    value: StaticListValue,
    endpoint: ListEndpoint,
) -> ScalarExpression {
    let literal = match endpoint {
        ListEndpoint::Head => value.literals.first(),
        ListEndpoint::Last => value.literals.last(),
    }
    .cloned()
    .unwrap_or(Literal::Null);
    presence_gate_scalar_expression(value.presence_variable, ScalarExpression::Literal(literal))
}
