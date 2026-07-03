//! Static literal folding: pure arithmetic, comparison, membership, and quantifier
//! evaluation over `Literal` values — no compile context, plan, or state.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Static literal folding helpers intentionally inherit parent-private Cypher helpers."
)]
use super::*;

pub(super) fn evaluate_static_literal_arithmetic(
    lhs: &Literal,
    operator: ArithmeticOperator,
    rhs: &Literal,
    path: impl Into<String>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let Some(lhs) = StaticNumericLiteral::from_literal(lhs, format!("{path}.lhs"))? else {
        return Ok(Literal::Null);
    };
    let Some(rhs) = StaticNumericLiteral::from_literal(rhs, format!("{path}.rhs"))? else {
        return Ok(Literal::Null);
    };
    if lhs.is_integer() && rhs.is_integer() {
        let left = lhs.as_i64();
        let right = rhs.as_i64();
        match operator {
            ArithmeticOperator::Add => {
                return left
                    .checked_add(right)
                    .map(Literal::Integer)
                    .ok_or_else(|| {
                        unsupported(path, "static integer map addition overflowed i64")
                    });
            }
            ArithmeticOperator::Subtract => {
                return left
                    .checked_sub(right)
                    .map(Literal::Integer)
                    .ok_or_else(|| {
                        unsupported(path, "static integer map subtraction overflowed i64")
                    });
            }
            ArithmeticOperator::Multiply => {
                return left
                    .checked_mul(right)
                    .map(Literal::Integer)
                    .ok_or_else(|| {
                        unsupported(path, "static integer map multiplication overflowed i64")
                    });
            }
            ArithmeticOperator::Modulo => {
                if right == 0 {
                    return Err(unsupported(path, "static integer map modulo by zero"));
                }
                return left
                    .checked_rem(right)
                    .map(Literal::Integer)
                    .ok_or_else(|| unsupported(path, "static integer map modulo overflowed i64"));
            }
            ArithmeticOperator::Divide | ArithmeticOperator::Power => {}
        }
    }

    let left = lhs.as_f64();
    let right = rhs.as_f64();
    let value = match operator {
        ArithmeticOperator::Add => left + right,
        ArithmeticOperator::Subtract => left - right,
        ArithmeticOperator::Multiply => left * right,
        ArithmeticOperator::Divide => {
            if right == 0.0 {
                return Err(unsupported(path, "static numeric map division by zero"));
            }
            left / right
        }
        ArithmeticOperator::Modulo => {
            if right == 0.0 {
                return Err(unsupported(path, "static numeric map modulo by zero"));
            }
            left % right
        }
        ArithmeticOperator::Power => left.powf(right),
    };
    if !value.is_finite() {
        return Err(unsupported(
            path,
            "static numeric map expression produced a non-finite float",
        ));
    }
    Ok(Literal::Float(OrderedFloat(value)))
}

pub(super) fn evaluate_static_is_empty_literal(
    literal: &Literal,
    path: impl Into<String>,
) -> Result<StaticBooleanOutcome, CoreError> {
    match literal {
        Literal::String(value) => Ok(StaticBooleanOutcome::from_bool(value.is_empty())),
        Literal::Null => Ok(StaticBooleanOutcome::Unknown),
        _ => Err(unsupported(
            path,
            "isEmpty() in static collection predicates requires string or static list arguments",
        )),
    }
}

pub(super) fn evaluate_static_literal_comparison(
    lhs: &Literal,
    operator: ComparisonOperator,
    rhs: &Literal,
    path: impl Into<String>,
) -> Result<StaticBooleanOutcome, CoreError> {
    let path = path.into();
    if matches!(lhs, Literal::Null) || matches!(rhs, Literal::Null) {
        return Ok(StaticBooleanOutcome::Unknown);
    }
    evaluate_literal_comparison(lhs, operator, rhs, path).map(StaticBooleanOutcome::from_bool)
}

pub(super) fn evaluate_static_literal_in_list(
    literal: &Literal,
    literals: &[Literal],
    path: impl Into<String>,
) -> Result<StaticBooleanOutcome, CoreError> {
    let path = path.into();
    if literals.is_empty() {
        return Ok(StaticBooleanOutcome::False);
    }
    if matches!(literal, Literal::Null) {
        return Ok(StaticBooleanOutcome::Unknown);
    }

    let mut saw_unknown = false;
    for candidate in literals {
        let outcome = evaluate_static_literal_comparison(
            literal,
            ComparisonOperator::Equal,
            candidate,
            path.clone(),
        )?;
        match outcome {
            StaticBooleanOutcome::True => return Ok(StaticBooleanOutcome::True),
            StaticBooleanOutcome::False => {}
            StaticBooleanOutcome::Unknown => saw_unknown = true,
        }
    }

    Ok(if saw_unknown {
        StaticBooleanOutcome::Unknown
    } else {
        StaticBooleanOutcome::False
    })
}

pub(super) fn evaluate_static_list_quantifier(
    quantifier: StaticListQuantifier,
    outcomes: impl Iterator<Item = StaticBooleanOutcome>,
) -> StaticBooleanOutcome {
    let mut true_count = 0usize;
    let mut saw_unknown = false;

    for outcome in outcomes {
        match (quantifier, outcome) {
            (StaticListQuantifier::All, StaticBooleanOutcome::False)
            | (StaticListQuantifier::None, StaticBooleanOutcome::True) => {
                return StaticBooleanOutcome::False;
            }
            (StaticListQuantifier::Any, StaticBooleanOutcome::True) => {
                return StaticBooleanOutcome::True;
            }
            (StaticListQuantifier::Single, StaticBooleanOutcome::True) => {
                true_count += 1;
                if true_count > 1 {
                    return StaticBooleanOutcome::False;
                }
            }
            (_, StaticBooleanOutcome::Unknown) => saw_unknown = true,
            _ => {}
        }
    }

    match quantifier {
        StaticListQuantifier::All | StaticListQuantifier::None => {
            if saw_unknown {
                StaticBooleanOutcome::Unknown
            } else {
                StaticBooleanOutcome::True
            }
        }
        StaticListQuantifier::Any => {
            if saw_unknown {
                StaticBooleanOutcome::Unknown
            } else {
                StaticBooleanOutcome::False
            }
        }
        StaticListQuantifier::Single => match (true_count, saw_unknown) {
            (1, false) => StaticBooleanOutcome::True,
            (_, true) => StaticBooleanOutcome::Unknown,
            _ => StaticBooleanOutcome::False,
        },
    }
}

pub(super) fn evaluate_static_literal_list_comparison(
    actual: &StaticListValue,
    operator: ComparisonOperator,
    expected: &StaticListValue,
    path: &str,
) -> Result<StaticBooleanOutcome, CoreError> {
    match operator {
        ComparisonOperator::Equal => Ok(StaticBooleanOutcome::from_bool(
            actual.literals == expected.literals,
        )),
        ComparisonOperator::NotEqual => Ok(StaticBooleanOutcome::from_bool(
            actual.literals != expected.literals,
        )),
        ComparisonOperator::GreaterThan
        | ComparisonOperator::GreaterThanOrEqual
        | ComparisonOperator::LessThan
        | ComparisonOperator::LessThanOrEqual => {
            validate_ordered_static_list_element_family(actual, expected, path)?;
            match compare_static_literal_lists(&actual.literals, &expected.literals, path)? {
                StaticListOrderingOutcome::Known(ordering) => Ok(StaticBooleanOutcome::from_bool(
                    evaluate_ordering_comparison(ordering, operator),
                )),
                StaticListOrderingOutcome::Unknown => Ok(StaticBooleanOutcome::Unknown),
            }
        }
        _ => Err(unsupported(
            path.to_string(),
            "static list predicates support =, <>, and lexicographic ordered comparisons over string or numeric lists",
        )),
    }
}
