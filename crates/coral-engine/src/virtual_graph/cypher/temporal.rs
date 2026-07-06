//! Temporal openCypher scalar lowering: construction, component access, and
//! duration arithmetic helpers split out of `cypher.rs` without changing the
//! parent module API.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Temporal lowering helpers intentionally inherit parent-private Cypher compile context."
)]
use super::*;

#[expect(
    clippy::too_many_lines,
    reason = "Terminal temporal component projection stays exhaustive over regular components and zoned datetime accessors."
)]
pub(super) fn compile_optional_terminal_temporal_component_projection(
    expression: &Expression,
    output_alias: Option<&Variable>,
    projections: &[Projection],
    plan: &GraphPlan,
    context: &CypherCompileContext,
    path: String,
) -> Result<Option<(Projection, String)>, CoreError> {
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_terminal_temporal_component_projection(
                inner,
                output_alias,
                projections,
                plan,
                context,
                path,
            )
        }
        Expression::PropertyLookup { base, property, .. } => {
            let component = property.name.name.as_str();
            if !temporal_component_name_is_reserved(component) {
                return Ok(None);
            }
            let Expression::Variable(variable) = base.as_ref() else {
                return Ok(None);
            };
            let consumed_alias = variable_name(variable);
            let Some(projection) = projections.iter().find(|projection| {
                projection_output_alias(projection) == Some(consumed_alias.as_str())
            }) else {
                return Err(unsupported(
                    path,
                    format!("terminal RETURN references unknown WITH alias '{consumed_alias}'"),
                ));
            };
            let base_expression =
                scalar_alias_projection_expression(projection, format!("{path}.base"))?;
            if let Some(accessor) = compile_zoned_datetime_accessor(component) {
                match classify_temporal_component_base(
                    &base_expression,
                    PredicateCompileMode::Graph {
                        plan,
                        path_state: None,
                    },
                    context,
                )? {
                    TemporalComponentBaseType::Temporal { kind, timezone } => {
                        if kind != TemporalKind::ZonedDateTime {
                            return Err(unsupported(
                                format!("{path}.property"),
                                format!("{component} is not supported for {} values", kind.name()),
                            ));
                        }
                        return Ok(Some((
                            Projection::Expression {
                                expression: ScalarExpression::Temporal(
                                    TemporalExpr::ZonedDateTimeAccessor {
                                        expression: Box::new(base_expression),
                                        accessor,
                                        timezone,
                                    },
                                ),
                                alias: output_alias
                                    .map_or_else(|| component.to_string(), variable_name),
                            },
                            consumed_alias,
                        )));
                    }
                    TemporalComponentBaseType::NonTemporal | TemporalComponentBaseType::Unknown => {
                        return Err(unsupported(
                            format!("{path}.base"),
                            "temporal component access requires a temporal value",
                        ));
                    }
                }
            }
            let unit = compile_temporal_component_unit(component, format!("{path}.property"))?;
            match classify_temporal_component_base(
                &base_expression,
                PredicateCompileMode::Graph {
                    plan,
                    path_state: None,
                },
                context,
            )? {
                TemporalComponentBaseType::Temporal { kind, .. } => {
                    if !unit.supports_kind(kind) {
                        return Err(unsupported(
                            format!("{path}.property"),
                            format!("{component} is not supported for {} values", kind.name()),
                        ));
                    }
                }
                TemporalComponentBaseType::NonTemporal | TemporalComponentBaseType::Unknown => {
                    return Err(unsupported(
                        format!("{path}.base"),
                        "temporal component access requires a temporal value",
                    ));
                }
            }
            Ok(Some((
                Projection::Expression {
                    expression: ScalarExpression::Temporal(TemporalExpr::Component {
                        expression: Box::new(base_expression),
                        unit,
                    }),
                    alias: output_alias.map_or_else(|| component.to_string(), variable_name),
                },
                consumed_alias,
            )))
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_temporal_scalar_function_expression(
    function: &FunctionInvocation,
    path: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    if is_date_function(function) {
        compile_date_scalar_expression(function, path).map(Some)
    } else if is_datetime_function(function) {
        compile_zoneddatetime_scalar_expression(function, path).map(Some)
    } else if is_localdatetime_function(function) {
        compile_localdatetime_scalar_expression(function, path).map(Some)
    } else if is_localtime_function(function) {
        compile_localtime_scalar_expression(function, path).map(Some)
    } else if is_duration_function(function) {
        compile_duration_scalar_expression(function, path, mode, context).map(Some)
    } else if let Some(unit) = duration_namespaced_function(function) {
        compile_duration_unit_scalar_expression(function, unit, path, mode, context).map(Some)
    } else {
        Ok(None)
    }
}

fn compile_date_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "date() requires exactly one argument",
        ));
    };
    compile_date_argument_scalar_expression(argument, format!("{path}.arguments[0]"))
}

fn compile_date_argument_scalar_expression(
    argument: &Expression,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match argument {
        Expression::Parenthesized(inner) => compile_date_argument_scalar_expression(inner, path),
        Expression::Literal(CypherLiteral::Map(map)) => {
            compile_date_map_scalar_expression(map, path)
        }
        Expression::Literal(CypherLiteral::String(value)) => {
            Ok(make_date_from_string_scalar_expression(value.value.clone()))
        }
        Expression::Literal(_) => Err(unsupported(
            path,
            "date() requires a literal map or string argument",
        )),
        _ => Err(unsupported(
            path,
            "dynamic date() string argument not supported yet",
        )),
    }
}

fn compile_date_map_scalar_expression(
    map: &MapLiteral,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let mut year = None;
    let mut month = None;
    let mut day = None;
    for (key, value) in &map.entries {
        let field = key.name.name.as_str();
        let field_path = format!("{path}.{field}");
        match field {
            "year" => {
                year = Some(compile_date_integer_field(value, field_path)?);
            }
            "month" => {
                month = Some(compile_date_integer_field(value, field_path)?);
            }
            "day" => {
                day = Some(compile_date_integer_field(value, field_path)?);
            }
            _ => {
                return Err(unsupported(
                    field_path,
                    format!("date() temporal field '{field}' is not supported yet"),
                ));
            }
        }
    }
    let Some(year) = year else {
        return Err(unsupported(
            format!("{path}.year"),
            "date() map constructor requires a literal integer year",
        ));
    };
    Ok(make_date_scalar_expression(
        year,
        month.unwrap_or_else(default_date_component),
        day.unwrap_or_else(default_date_component),
    ))
}

fn compile_date_integer_field(
    value: &Expression,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match value {
        Expression::Parenthesized(inner) => compile_date_integer_field(inner, path),
        Expression::Literal(CypherLiteral::Number(NumberLiteral::Integer(value))) => {
            Ok(ScalarExpression::Literal(Literal::Integer(*value)))
        }
        _ => Err(unsupported(
            path,
            "dynamic temporal fields not supported yet",
        )),
    }
}

fn default_date_component() -> ScalarExpression {
    ScalarExpression::Literal(Literal::Integer(1))
}

fn make_date_from_string_scalar_expression(text: String) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::DateFromString {
        text: Box::new(ScalarExpression::Literal(Literal::String(text))),
    })
}

fn compile_localdatetime_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "localdatetime() requires exactly one argument",
        ));
    };
    compile_localdatetime_argument_scalar_expression(argument, format!("{path}.arguments[0]"))
}

fn compile_localdatetime_argument_scalar_expression(
    argument: &Expression,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match argument {
        Expression::Parenthesized(inner) => {
            compile_localdatetime_argument_scalar_expression(inner, path)
        }
        Expression::Literal(CypherLiteral::Map(map)) => {
            compile_localdatetime_map_scalar_expression(map, path)
        }
        Expression::Literal(CypherLiteral::String(value)) => {
            if localdatetime_literal_has_timezone(&value.value) {
                return Err(unsupported(
                    path,
                    "localdatetime() does not accept a timezone; use a naive date-time string",
                ));
            }
            Ok(make_localdatetime_from_string_scalar_expression(
                value.value.clone(),
            ))
        }
        Expression::Literal(_) => Err(unsupported(
            path,
            "localdatetime() requires a literal map or string argument",
        )),
        _ => Err(unsupported(
            path,
            "dynamic localdatetime() string argument not supported yet",
        )),
    }
}

fn compile_localdatetime_map_scalar_expression(
    map: &MapLiteral,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let mut year = None;
    let mut month = None;
    let mut day = None;
    let mut hour = None;
    let mut minute = None;
    let mut second = None;
    let mut millisecond = None;
    let mut microsecond = None;
    let mut nanosecond = None;
    for (key, value) in &map.entries {
        let field = key.name.name.as_str();
        let field_path = format!("{path}.{field}");
        match field {
            "year" => {
                year = Some(compile_date_integer_field(value, field_path)?);
            }
            "month" => {
                month = Some(compile_date_integer_field(value, field_path)?);
            }
            "day" => {
                day = Some(compile_date_integer_field(value, field_path)?);
            }
            "hour" => {
                hour = Some(compile_date_integer_field(value, field_path)?);
            }
            "minute" => {
                minute = Some(compile_date_integer_field(value, field_path)?);
            }
            "second" => {
                second = Some(compile_date_integer_field(value, field_path)?);
            }
            "millisecond" => {
                millisecond = Some(compile_date_integer_field(value, field_path)?);
            }
            "microsecond" => {
                microsecond = Some(compile_date_integer_field(value, field_path)?);
            }
            "nanosecond" => {
                nanosecond = Some(compile_date_integer_field(value, field_path)?);
            }
            _ => {
                return Err(unsupported(
                    field_path,
                    format!("localdatetime() temporal field '{field}' is not supported yet"),
                ));
            }
        }
    }
    let Some(year) = year else {
        return Err(unsupported(
            format!("{path}.year"),
            "localdatetime() map constructor requires a literal integer year",
        ));
    };
    Ok(make_localdatetime_scalar_expression(
        year,
        month.unwrap_or_else(default_date_component),
        day.unwrap_or_else(default_date_component),
        hour.unwrap_or_else(default_time_component),
        minute.unwrap_or_else(default_time_component),
        second.unwrap_or_else(default_time_component),
        millisecond.unwrap_or_else(default_time_component),
        microsecond.unwrap_or_else(default_time_component),
        nanosecond.unwrap_or_else(default_time_component),
    ))
}

fn default_time_component() -> ScalarExpression {
    ScalarExpression::Literal(Literal::Integer(0))
}

fn localdatetime_literal_has_timezone(text: &str) -> bool {
    if !looks_like_datetime_text(text) {
        return false;
    }
    text.ends_with('Z') || text.ends_with('z') || has_offset_suffix(text)
}

fn looks_like_datetime_text(text: &str) -> bool {
    let Some((_, time)) = text.split_once(['T', 't', ' ']) else {
        return false;
    };
    time.contains(':')
}

fn has_offset_suffix(text: &str) -> bool {
    let Some(suffix_start) = text.len().checked_sub(6) else {
        return false;
    };
    matches!(
        text.as_bytes().get(suffix_start..),
        Some([b'+' | b'-', hour_tens, hour_ones, b':', minute_tens, minute_ones])
            if hour_tens.is_ascii_digit()
                && hour_ones.is_ascii_digit()
                && minute_tens.is_ascii_digit()
                && minute_ones.is_ascii_digit()
    )
}

fn make_localdatetime_from_string_scalar_expression(text: String) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::LocalDateTimeFromString {
        text: Box::new(ScalarExpression::Literal(Literal::String(text))),
    })
}

fn compile_zoneddatetime_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "datetime() requires exactly one argument",
        ));
    };
    compile_zoneddatetime_argument_scalar_expression(argument, format!("{path}.arguments[0]"))
}

fn compile_zoneddatetime_argument_scalar_expression(
    argument: &Expression,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match argument {
        Expression::Parenthesized(inner) => {
            compile_zoneddatetime_argument_scalar_expression(inner, path)
        }
        Expression::Literal(CypherLiteral::Map(map)) => {
            compile_zoneddatetime_map_scalar_expression(map, path)
        }
        Expression::Literal(CypherLiteral::String(value)) => {
            let literal = parse_zoneddatetime_string_literal(&value.value, &path)?;
            Ok(make_zoneddatetime_from_string_scalar_expression(
                literal.text,
                literal.timezone,
            ))
        }
        Expression::Literal(_) => Err(unsupported(
            path,
            "datetime() requires a literal map or string argument",
        )),
        _ => Err(unsupported(
            path,
            "dynamic datetime() string argument not supported yet",
        )),
    }
}

fn compile_zoneddatetime_map_scalar_expression(
    map: &MapLiteral,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let mut year = None;
    let mut month = None;
    let mut day = None;
    let mut hour = None;
    let mut minute = None;
    let mut second = None;
    let mut millisecond = None;
    let mut microsecond = None;
    let mut nanosecond = None;
    let mut timezone = None;
    for (key, value) in &map.entries {
        let field = key.name.name.as_str();
        let field_path = format!("{path}.{field}");
        match field {
            "year" => {
                year = Some(compile_date_integer_field(value, field_path)?);
            }
            "month" => {
                month = Some(compile_date_integer_field(value, field_path)?);
            }
            "day" => {
                day = Some(compile_date_integer_field(value, field_path)?);
            }
            "hour" => {
                hour = Some(compile_date_integer_field(value, field_path)?);
            }
            "minute" => {
                minute = Some(compile_date_integer_field(value, field_path)?);
            }
            "second" => {
                second = Some(compile_date_integer_field(value, field_path)?);
            }
            "millisecond" => {
                millisecond = Some(compile_date_integer_field(value, field_path)?);
            }
            "microsecond" => {
                microsecond = Some(compile_date_integer_field(value, field_path)?);
            }
            "nanosecond" => {
                nanosecond = Some(compile_date_integer_field(value, field_path)?);
            }
            "timezone" => {
                timezone = Some(compile_zoneddatetime_timezone_field(value, field_path)?);
            }
            _ => {
                return Err(unsupported(
                    field_path,
                    format!("datetime() temporal field '{field}' is not supported yet"),
                ));
            }
        }
    }
    let Some(year) = year else {
        return Err(unsupported(
            format!("{path}.year"),
            "datetime() map constructor requires a literal integer year",
        ));
    };
    let Some(timezone) = timezone else {
        return Err(unsupported(
            format!("{path}.timezone"),
            "datetime() map constructor requires a literal string timezone",
        ));
    };
    let timezone = normalize_zoneddatetime_timezone(&timezone, format!("{path}.timezone"))?;
    let month = month.unwrap_or_else(default_date_component);
    let day = day.unwrap_or_else(default_date_component);
    let hour = hour.unwrap_or_else(default_time_component);
    let minute = minute.unwrap_or_else(default_time_component);
    let second = second.unwrap_or_else(default_time_component);
    let millisecond = millisecond.unwrap_or_else(default_time_component);
    let microsecond = microsecond.unwrap_or_else(default_time_component);
    let nanosecond = nanosecond.unwrap_or_else(default_time_component);

    if let Some(local_text) = literal_zoneddatetime_local_text(
        &year,
        &month,
        &day,
        &hour,
        &minute,
        &second,
        &millisecond,
        &microsecond,
        &nanosecond,
    ) {
        validate_named_zoneddatetime_resolution(&local_text, &timezone, None, &path)?;
    }

    Ok(make_zoneddatetime_scalar_expression(
        year,
        month,
        day,
        hour,
        minute,
        second,
        millisecond,
        microsecond,
        nanosecond,
        timezone,
    ))
}

fn compile_zoneddatetime_timezone_field(
    value: &Expression,
    path: impl Into<String>,
) -> Result<String, CoreError> {
    let path = path.into();
    match value {
        Expression::Parenthesized(inner) => compile_zoneddatetime_timezone_field(inner, path),
        Expression::Literal(CypherLiteral::String(value)) => Ok(value.value.clone()),
        _ => Err(unsupported(
            path,
            "datetime() map constructor requires a literal string timezone",
        )),
    }
}

struct ParsedZonedDateTimeLiteral {
    text: String,
    timezone: String,
}

fn parse_zoneddatetime_string_literal(
    text: &str,
    path: &str,
) -> Result<ParsedZonedDateTimeLiteral, CoreError> {
    let (body, bracket_timezone) = split_bracket_timezone(text, path)?;
    let offset = datetime_offset_suffix(body);
    let timezone = if let Some(timezone) = bracket_timezone {
        let timezone = normalize_zoneddatetime_timezone(timezone, path)?;
        validate_named_zoneddatetime_resolution(
            offset.as_ref().map_or(body, |offset| offset.local_text),
            &timezone,
            offset.as_ref().map(|offset| offset.seconds),
            path,
        )?;
        timezone
    } else if let Some(offset) = offset {
        offset.normalized
    } else if body.ends_with('Z') || body.ends_with('z') {
        "+00:00".to_string()
    } else {
        return Err(unsupported(
            path,
            "datetime() requires a timezone offset or bracketed timezone",
        ));
    };
    Ok(ParsedZonedDateTimeLiteral {
        text: body.to_string(),
        timezone,
    })
}

fn split_bracket_timezone<'a>(
    text: &'a str,
    path: &str,
) -> Result<(&'a str, Option<&'a str>), CoreError> {
    if !text.contains(['[', ']']) {
        return Ok((text, None));
    }
    let Some(prefix) = text.strip_suffix(']') else {
        return Err(unsupported(
            path,
            "datetime() bracketed timezone suffix must end with ']'",
        ));
    };
    let Some(start) = prefix.rfind('[') else {
        return Err(unsupported(
            path,
            "datetime() bracketed timezone suffix must include '['",
        ));
    };
    let body = prefix.get(..start).ok_or_else(|| {
        unsupported(
            path,
            "datetime() bracketed timezone suffix must be a non-empty trailing [timezone]",
        )
    })?;
    let timezone = prefix.get(start + '['.len_utf8()..).ok_or_else(|| {
        unsupported(
            path,
            "datetime() bracketed timezone suffix must be a non-empty trailing [timezone]",
        )
    })?;
    if body.is_empty() || timezone.is_empty() || body.contains('[') || timezone.contains('[') {
        return Err(unsupported(
            path,
            "datetime() bracketed timezone suffix must be a non-empty trailing [timezone]",
        ));
    }
    Ok((body, Some(timezone)))
}

#[derive(Clone)]
struct DateTimeOffsetSuffix<'a> {
    local_text: &'a str,
    normalized: String,
    seconds: i32,
}

fn datetime_offset_suffix(text: &str) -> Option<DateTimeOffsetSuffix<'_>> {
    let separator = text.find(['T', 't', ' '])?;
    let after_separator = text.get(separator + 1..)?;
    let suffix_start = after_separator
        .rfind(['+', '-'])
        .map(|index| separator + 1 + index)?;
    let suffix = text.get(suffix_start..)?;
    let (normalized, seconds) = normalize_offset_timezone(suffix)?;
    Some(DateTimeOffsetSuffix {
        local_text: text.get(..suffix_start)?,
        normalized,
        seconds,
    })
}

fn normalize_zoneddatetime_timezone(
    timezone: &str,
    path: impl Into<String>,
) -> Result<String, CoreError> {
    if let Some((normalized, _)) = normalize_offset_timezone(timezone) {
        return Ok(normalized);
    }
    if timezone.eq_ignore_ascii_case("z") {
        return Ok("+00:00".to_string());
    }
    if timezone.parse::<Tz>().is_ok() {
        return Ok(timezone.to_string());
    }
    Err(unsupported(
        path,
        "datetime() timezone must be a fixed offset or IANA timezone",
    ))
}

fn normalize_offset_timezone(timezone: &str) -> Option<(String, i32)> {
    if timezone.eq_ignore_ascii_case("z") {
        return Some(("+00:00".to_string(), 0));
    }
    let mut chars = timezone.chars();
    let sign = chars.next()?;
    if !matches!(sign, '+' | '-') {
        return None;
    }
    let rest = chars.as_str().as_bytes();
    let (hour, minute) = match rest {
        [hour_tens, hour_ones] => (parse_two_digits(*hour_tens, *hour_ones)?, 0),
        [hour_tens, hour_ones, minute_tens, minute_ones]
        | [hour_tens, hour_ones, b':', minute_tens, minute_ones] => (
            parse_two_digits(*hour_tens, *hour_ones)?,
            parse_two_digits(*minute_tens, *minute_ones)?,
        ),
        _ => return None,
    };
    if hour > 23 || minute > 59 {
        return None;
    }
    let unsigned_seconds = hour
        .checked_mul(3600)?
        .checked_add(minute.checked_mul(60)?)?;
    let seconds = if sign == '-' {
        -unsigned_seconds
    } else {
        unsigned_seconds
    };
    let normalized_sign = if seconds < 0 { '-' } else { '+' };
    Some((format!("{normalized_sign}{hour:02}:{minute:02}"), seconds))
}

fn parse_two_digits(tens: u8, ones: u8) -> Option<i32> {
    if !tens.is_ascii_digit() || !ones.is_ascii_digit() {
        return None;
    }
    Some(i32::from(tens - b'0') * 10 + i32::from(ones - b'0'))
}

fn validate_named_zoneddatetime_resolution(
    local_text: &str,
    timezone: &str,
    explicit_offset_seconds: Option<i32>,
    path: &str,
) -> Result<(), CoreError> {
    let Ok(timezone) = timezone.parse::<Tz>() else {
        return Ok(());
    };
    let Some(local_datetime) = parse_zoneddatetime_local_text(local_text) else {
        return Ok(());
    };
    match timezone.offset_from_local_datetime(&local_datetime) {
        LocalResult::None => Err(unsupported(
            path,
            "datetime() named timezone local time falls in a daylight-saving gap; specify an explicit offset or valid local time",
        )),
        LocalResult::Ambiguous(first, second) => {
            if let Some(explicit_offset_seconds) = explicit_offset_seconds
                && [first, second]
                    .iter()
                    .any(|offset| offset.fix().local_minus_utc() == explicit_offset_seconds)
            {
                return Ok(());
            }
            Err(unsupported(
                path,
                "datetime() named timezone local time is ambiguous at a daylight-saving overlap; specify an explicit offset",
            ))
        }
        LocalResult::Single(offset) => {
            if let Some(explicit_offset_seconds) = explicit_offset_seconds
                && offset.fix().local_minus_utc() != explicit_offset_seconds
            {
                return Err(unsupported(
                    path,
                    "datetime() explicit offset does not match the bracketed timezone at that local time",
                ));
            }
            Ok(())
        }
    }
}

fn parse_zoneddatetime_local_text(text: &str) -> Option<NaiveDateTime> {
    [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
    ]
    .into_iter()
    .find_map(|format| NaiveDateTime::parse_from_str(text, format).ok())
}

#[expect(
    clippy::too_many_arguments,
    reason = "Literal datetime validation mirrors openCypher datetime fields."
)]
fn literal_zoneddatetime_local_text(
    year: &ScalarExpression,
    month: &ScalarExpression,
    day: &ScalarExpression,
    hour: &ScalarExpression,
    minute: &ScalarExpression,
    second: &ScalarExpression,
    millisecond: &ScalarExpression,
    microsecond: &ScalarExpression,
    nanosecond: &ScalarExpression,
) -> Option<String> {
    let year = i32::try_from(literal_integer_value(year)?).ok()?;
    let month = u32::try_from(literal_integer_value(month)?).ok()?;
    let day = u32::try_from(literal_integer_value(day)?).ok()?;
    let hour = u32::try_from(literal_integer_value(hour)?).ok()?;
    let minute = u32::try_from(literal_integer_value(minute)?).ok()?;
    let second = u32::try_from(literal_integer_value(second)?).ok()?;
    let millisecond = u32::try_from(literal_integer_value(millisecond)?).ok()?;
    let microsecond = u32::try_from(literal_integer_value(microsecond)?).ok()?;
    let nanosecond = u32::try_from(literal_integer_value(nanosecond)?).ok()?;
    let nanos = millisecond
        .checked_mul(1_000_000)?
        .checked_add(microsecond.checked_mul(1_000)?)?
        .checked_add(nanosecond)?;
    NaiveDate::from_ymd_opt(year, month, day)?;
    NaiveTime::from_hms_nano_opt(hour, minute, second, nanos)?;
    let mut fractional = format!("{nanos:09}");
    while fractional.ends_with('0') {
        fractional.pop();
    }
    let suffix = if fractional.is_empty() {
        String::new()
    } else {
        format!(".{fractional}")
    };
    Some(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}{suffix}"
    ))
}

fn literal_integer_value(expression: &ScalarExpression) -> Option<i64> {
    match expression {
        ScalarExpression::Literal(Literal::Integer(value)) => Some(*value),
        _ => None,
    }
}

fn make_zoneddatetime_from_string_scalar_expression(
    text: String,
    timezone: String,
) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::ZonedDateTimeFromString {
        text: Box::new(ScalarExpression::Literal(Literal::String(text))),
        timezone,
    })
}

fn compile_localtime_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "localtime() requires exactly one argument",
        ));
    };
    compile_localtime_argument_scalar_expression(argument, format!("{path}.arguments[0]"))
}

fn compile_localtime_argument_scalar_expression(
    argument: &Expression,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match argument {
        Expression::Parenthesized(inner) => {
            compile_localtime_argument_scalar_expression(inner, path)
        }
        Expression::Literal(CypherLiteral::Map(map)) => {
            compile_localtime_map_scalar_expression(map, path)
        }
        Expression::Literal(CypherLiteral::String(value)) => {
            if localtime_literal_has_timezone(&value.value) {
                return Err(unsupported(
                    path,
                    "localtime() does not accept a timezone; use a naive time string",
                ));
            }
            Ok(make_localtime_from_string_scalar_expression(
                value.value.clone(),
            ))
        }
        Expression::Literal(_) => Err(unsupported(
            path,
            "localtime() requires a literal map or string argument",
        )),
        _ => Err(unsupported(
            path,
            "dynamic localtime() string argument not supported yet",
        )),
    }
}

fn compile_localtime_map_scalar_expression(
    map: &MapLiteral,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let mut hour = None;
    let mut minute = None;
    let mut second = None;
    let mut millisecond = None;
    let mut microsecond = None;
    let mut nanosecond = None;
    for (key, value) in &map.entries {
        let field = key.name.name.as_str();
        let field_path = format!("{path}.{field}");
        match field {
            "hour" => {
                hour = Some(compile_date_integer_field(value, field_path)?);
            }
            "minute" => {
                minute = Some(compile_date_integer_field(value, field_path)?);
            }
            "second" => {
                second = Some(compile_date_integer_field(value, field_path)?);
            }
            "millisecond" => {
                millisecond = Some(compile_date_integer_field(value, field_path)?);
            }
            "microsecond" => {
                microsecond = Some(compile_date_integer_field(value, field_path)?);
            }
            "nanosecond" => {
                nanosecond = Some(compile_date_integer_field(value, field_path)?);
            }
            _ => {
                return Err(unsupported(
                    field_path,
                    format!("localtime() temporal field '{field}' is not supported yet"),
                ));
            }
        }
    }
    let Some(hour) = hour else {
        return Err(unsupported(
            format!("{path}.hour"),
            "localtime() map constructor requires a literal integer hour",
        ));
    };
    Ok(make_localtime_scalar_expression(
        hour,
        minute.unwrap_or_else(default_time_component),
        second.unwrap_or_else(default_time_component),
        millisecond.unwrap_or_else(default_time_component),
        microsecond.unwrap_or_else(default_time_component),
        nanosecond.unwrap_or_else(default_time_component),
    ))
}

fn localtime_literal_has_timezone(text: &str) -> bool {
    let without_zone = text
        .strip_suffix('Z')
        .or_else(|| text.strip_suffix('z'))
        .unwrap_or(text);
    (without_zone.len() != text.len() || has_offset_suffix(text))
        && without_zone_contains_time(without_zone)
}

fn without_zone_contains_time(text: &str) -> bool {
    let time = if has_offset_suffix(text) {
        text.get(..text.len().saturating_sub(6)).unwrap_or(text)
    } else {
        text
    };
    time.contains(':')
}

fn make_localtime_from_string_scalar_expression(text: String) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::LocalTimeFromString {
        text: Box::new(ScalarExpression::Literal(Literal::String(text))),
    })
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct DurationParts {
    months: i64,
    days: i64,
    seconds: i64,
    nanos: i64,
}

impl DurationParts {
    fn into_scalar_expression(self) -> ScalarExpression {
        ScalarExpression::Temporal(TemporalExpr::MakeDuration {
            months: self.months,
            days: self.days,
            seconds: self.seconds,
            nanos: self.nanos,
        })
    }
}

fn compile_duration_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "duration() requires exactly one argument",
        ));
    };
    compile_duration_argument_scalar_expression(
        argument,
        format!("{path}.arguments[0]"),
        mode,
        context,
    )
}

fn compile_duration_unit_scalar_expression(
    function: &FunctionInvocation,
    unit: TemporalDurationUnit,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let [start, end] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!("{}() requires exactly two arguments", unit.function_name()),
        ));
    };
    for (index, argument) in [(0, start), (1, end)] {
        if expression_is_zoned_temporal_constructor(argument) {
            return Err(unsupported(
                format!("{path}.arguments[{index}]"),
                format!(
                    "{}() does not support zoned datetime() or time() arguments yet",
                    unit.function_name()
                ),
            ));
        }
    }
    Ok(ScalarExpression::Temporal(TemporalExpr::DurationInUnits {
        unit,
        start: Box::new(compile_scalar_expression_in_predicate_mode(
            start,
            format!("{path}.arguments[0]"),
            mode,
            context,
        )?),
        end: Box::new(compile_scalar_expression_in_predicate_mode(
            end,
            format!("{path}.arguments[1]"),
            mode,
            context,
        )?),
    }))
}

fn expression_is_zoned_temporal_constructor(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => expression_is_zoned_temporal_constructor(inner),
        Expression::FunctionCall(function) => {
            is_datetime_function(function) || is_time_function(function)
        }
        _ => false,
    }
}

fn compile_duration_argument_scalar_expression(
    argument: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match argument {
        Expression::Parenthesized(inner) => {
            compile_duration_argument_scalar_expression(inner, path, mode, context)
        }
        Expression::Literal(CypherLiteral::Map(map)) => {
            compile_duration_map_scalar_expression(map, path)
        }
        Expression::Literal(CypherLiteral::String(value)) => {
            parse_iso_duration_literal(&value.value, &path)
                .map(DurationParts::into_scalar_expression)
        }
        Expression::FunctionCall(function) if is_to_string_function(function) => {
            let expression = compile_single_scalar_function_argument(
                function,
                path.clone(),
                "toString",
                mode,
                context,
            )?;
            if scalar_expression_has_duration_value(&expression) {
                return Ok(expression);
            }
            Err(unsupported(
                path,
                "duration(toString(...)) requires a duration-valued argument",
            ))
        }
        Expression::Literal(_) => Err(unsupported(
            path,
            "duration() requires a literal map or string argument",
        )),
        _ => Err(unsupported(
            path,
            "dynamic duration() argument not supported yet",
        )),
    }
}

fn compile_duration_map_scalar_expression(
    map: &MapLiteral,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let mut parts = DurationParts::default();
    for (key, value) in &map.entries {
        let field = key.name.name.as_str();
        let field_path = format!("{path}.{field}");
        let value = compile_duration_integer_field(value, &field_path)?;
        match field {
            "years" => add_duration_months(
                &mut parts,
                multiply_duration_field(value, 12, &field_path, field)?,
                &field_path,
                field,
            )?,
            "months" => add_duration_months(&mut parts, value, &field_path, field)?,
            "weeks" => add_duration_days(
                &mut parts,
                multiply_duration_field(value, 7, &field_path, field)?,
                &field_path,
                field,
            )?,
            "days" => add_duration_days(&mut parts, value, &field_path, field)?,
            "hours" => add_duration_seconds(
                &mut parts,
                multiply_duration_field(value, 3_600, &field_path, field)?,
                &field_path,
                field,
            )?,
            "minutes" => add_duration_seconds(
                &mut parts,
                multiply_duration_field(value, 60, &field_path, field)?,
                &field_path,
                field,
            )?,
            "seconds" => add_duration_seconds(&mut parts, value, &field_path, field)?,
            "milliseconds" => add_duration_nanos(
                &mut parts,
                multiply_duration_field(value, 1_000_000, &field_path, field)?,
                &field_path,
                field,
            )?,
            "microseconds" => add_duration_nanos(
                &mut parts,
                multiply_duration_field(value, 1_000, &field_path, field)?,
                &field_path,
                field,
            )?,
            "nanoseconds" => add_duration_nanos(&mut parts, value, &field_path, field)?,
            _ => {
                return Err(unsupported(
                    field_path,
                    format!("duration() temporal field '{field}' is not supported yet"),
                ));
            }
        }
    }
    Ok(parts.into_scalar_expression())
}

fn compile_duration_integer_field(
    value: &Expression,
    path: impl Into<String>,
) -> Result<i64, CoreError> {
    let path = path.into();
    match value {
        Expression::Parenthesized(inner) => compile_duration_integer_field(inner, path),
        Expression::Literal(CypherLiteral::Number(NumberLiteral::Integer(value))) => Ok(*value),
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            operand,
            ..
        } => compile_duration_integer_field(operand, &path)?
            .checked_neg()
            .ok_or_else(|| unsupported(path, "duration() field is out of range")),
        _ => Err(unsupported(
            path,
            "dynamic duration fields not supported yet",
        )),
    }
}

fn parse_iso_duration_literal(text: &str, path: &str) -> Result<DurationParts, CoreError> {
    let (sign, rest) = if let Some(rest) = text.strip_prefix('-') {
        (-1, rest)
    } else {
        (1, text)
    };
    let Some(mut rest) = rest.strip_prefix('P').or_else(|| rest.strip_prefix('p')) else {
        return Err(invalid_duration_literal(path));
    };
    if rest.is_empty() {
        return Err(invalid_duration_literal(path));
    }

    let mut parts = DurationParts::default();
    let mut in_time = false;
    let mut saw_component = false;
    while !rest.is_empty() {
        if let Some(after_time_marker) = rest.strip_prefix('T').or_else(|| rest.strip_prefix('t')) {
            if in_time {
                return Err(invalid_duration_literal(path));
            }
            in_time = true;
            rest = after_time_marker;
            continue;
        }

        let number_end = rest
            .char_indices()
            .find_map(|(index, character)| {
                (!character.is_ascii_digit() && character != '.').then_some(index)
            })
            .ok_or_else(|| invalid_duration_literal(path))?;
        if number_end == 0 {
            return Err(invalid_duration_literal(path));
        }
        let (number, after_number) = rest.split_at(number_end);
        let unit = after_number
            .chars()
            .next()
            .ok_or_else(|| invalid_duration_literal(path))?;
        rest = after_number
            .strip_prefix(unit)
            .ok_or_else(|| invalid_duration_literal(path))?;
        saw_component = true;

        match (unit, in_time) {
            ('Y' | 'y', false) => {
                let value = parse_duration_integer_component(number, path)?;
                let months = multiply_duration_field(value, 12 * sign, path, "years")?;
                add_duration_months(&mut parts, months, path, "years")?;
            }
            ('M' | 'm', false) => {
                let value = parse_duration_integer_component(number, path)?;
                let months = multiply_duration_field(value, sign, path, "months")?;
                add_duration_months(&mut parts, months, path, "months")?;
            }
            ('W' | 'w', false) => {
                let value = parse_duration_integer_component(number, path)?;
                let days = multiply_duration_field(value, 7 * sign, path, "weeks")?;
                add_duration_days(&mut parts, days, path, "weeks")?;
            }
            ('D' | 'd', false) => {
                let value = parse_duration_integer_component(number, path)?;
                let days = multiply_duration_field(value, sign, path, "days")?;
                add_duration_days(&mut parts, days, path, "days")?;
            }
            ('H' | 'h', true) => {
                let value = parse_duration_integer_component(number, path)?;
                let seconds = multiply_duration_field(value, 3_600 * sign, path, "hours")?;
                add_duration_seconds(&mut parts, seconds, path, "hours")?;
            }
            ('M' | 'm', true) => {
                let value = parse_duration_integer_component(number, path)?;
                let seconds = multiply_duration_field(value, 60 * sign, path, "minutes")?;
                add_duration_seconds(&mut parts, seconds, path, "minutes")?;
            }
            ('S' | 's', true) => {
                let (seconds, nanos) = parse_duration_seconds_component(number, sign, path)?;
                add_duration_seconds(&mut parts, seconds, path, "seconds")?;
                add_duration_nanos(&mut parts, nanos, path, "seconds")?;
            }
            _ => return Err(invalid_duration_literal(path)),
        }
    }

    if !saw_component {
        return Err(invalid_duration_literal(path));
    }
    Ok(parts)
}

fn parse_duration_integer_component(text: &str, path: &str) -> Result<i64, CoreError> {
    if text.contains('.') {
        return Err(invalid_duration_literal(path));
    }
    text.parse::<i64>()
        .map_err(|_error| invalid_duration_literal(path))
}

fn parse_duration_seconds_component(
    text: &str,
    sign: i64,
    path: &str,
) -> Result<(i64, i64), CoreError> {
    let (whole, fractional) = text
        .split_once('.')
        .map_or((text, ""), |(whole, fractional)| (whole, fractional));
    if whole.is_empty() || fractional.len() > 9 || !fractional.chars().all(|c| c.is_ascii_digit()) {
        return Err(invalid_duration_literal(path));
    }
    let seconds = whole
        .parse::<i64>()
        .map_err(|_error| invalid_duration_literal(path))?
        .checked_mul(sign)
        .ok_or_else(|| invalid_duration_literal(path))?;
    let nanos = if fractional.is_empty() {
        0
    } else {
        let mut nanos = fractional.to_string();
        while nanos.len() < 9 {
            nanos.push('0');
        }
        nanos
            .parse::<i64>()
            .map_err(|_error| invalid_duration_literal(path))?
            .checked_mul(sign)
            .ok_or_else(|| invalid_duration_literal(path))?
    };
    Ok((seconds, nanos))
}

fn invalid_duration_literal(path: &str) -> CoreError {
    unsupported(
        path.to_string(),
        "duration() requires an ISO-8601 duration string literal",
    )
}

fn multiply_duration_field(
    value: i64,
    multiplier: i64,
    path: &str,
    field: &str,
) -> Result<i64, CoreError> {
    value.checked_mul(multiplier).ok_or_else(|| {
        unsupported(
            path.to_string(),
            format!("duration() field '{field}' is out of range"),
        )
    })
}

fn add_duration_months(
    parts: &mut DurationParts,
    value: i64,
    path: &str,
    field: &str,
) -> Result<(), CoreError> {
    parts.months = parts.months.checked_add(value).ok_or_else(|| {
        unsupported(
            path.to_string(),
            format!("duration() field '{field}' is out of range"),
        )
    })?;
    Ok(())
}

fn add_duration_days(
    parts: &mut DurationParts,
    value: i64,
    path: &str,
    field: &str,
) -> Result<(), CoreError> {
    parts.days = parts.days.checked_add(value).ok_or_else(|| {
        unsupported(
            path.to_string(),
            format!("duration() field '{field}' is out of range"),
        )
    })?;
    Ok(())
}

fn add_duration_seconds(
    parts: &mut DurationParts,
    value: i64,
    path: &str,
    field: &str,
) -> Result<(), CoreError> {
    parts.seconds = parts.seconds.checked_add(value).ok_or_else(|| {
        unsupported(
            path.to_string(),
            format!("duration() field '{field}' is out of range"),
        )
    })?;
    Ok(())
}

fn add_duration_nanos(
    parts: &mut DurationParts,
    value: i64,
    path: &str,
    field: &str,
) -> Result<(), CoreError> {
    let total_nanos =
        i128::from(parts.seconds) * 1_000_000_000 + i128::from(parts.nanos) + i128::from(value);
    let (seconds, nanos) = normalize_duration_nanos(total_nanos, path, field)?;
    parts.seconds = seconds;
    parts.nanos = nanos;
    Ok(())
}

fn normalize_duration_nanos(
    total_nanos: i128,
    path: &str,
    field: &str,
) -> Result<(i64, i64), CoreError> {
    let seconds = total_nanos.div_euclid(1_000_000_000);
    let nanos = total_nanos.rem_euclid(1_000_000_000);
    let seconds = i64::try_from(seconds).map_err(|_error| {
        unsupported(
            path.to_string(),
            format!("duration() field '{field}' is out of range"),
        )
    })?;
    let nanos = i64::try_from(nanos).map_err(|_error| {
        unsupported(
            path.to_string(),
            format!("duration() field '{field}' is out of range"),
        )
    })?;
    Ok((seconds, nanos))
}

pub(super) fn compile_duration_multiply_expression(
    operator: ArithmeticOperator,
    left: &ScalarExpression,
    right: &ScalarExpression,
    path: &str,
) -> Result<Option<ScalarExpression>, CoreError> {
    if !matches!(operator, ArithmeticOperator::Multiply) {
        return Ok(None);
    }
    let Some(duration) = duration_parts_from_scalar_expression(left) else {
        if duration_parts_from_scalar_expression(right).is_some() {
            return Err(unsupported(
                path.to_string(),
                "duration multiplication requires duration * numeric literal",
            ));
        }
        return Ok(None);
    };
    let factor = duration_integer_factor(right, format!("{path}.rhs"))?;
    Ok(Some(
        scale_duration_parts(duration, factor, path)?.into_scalar_expression(),
    ))
}

fn scalar_expression_has_duration_value(expression: &ScalarExpression) -> bool {
    match expression {
        ScalarExpression::Temporal(
            TemporalExpr::MakeDuration { .. } | TemporalExpr::DurationInUnits { .. },
        ) => true,
        ScalarExpression::Arithmetic {
            operator,
            left,
            right,
        } => match operator {
            ArithmeticOperator::Add | ArithmeticOperator::Subtract => {
                scalar_expression_has_duration_value(left)
                    && scalar_expression_has_duration_value(right)
            }
            ArithmeticOperator::Multiply => scalar_expression_has_duration_value(left),
            ArithmeticOperator::Divide | ArithmeticOperator::Modulo | ArithmeticOperator::Power => {
                false
            }
        },
        _ => false,
    }
}

fn duration_parts_from_scalar_expression(expression: &ScalarExpression) -> Option<DurationParts> {
    match expression {
        ScalarExpression::Temporal(TemporalExpr::MakeDuration {
            months,
            days,
            seconds,
            nanos,
        }) => Some(DurationParts {
            months: *months,
            days: *days,
            seconds: *seconds,
            nanos: *nanos,
        }),
        _ => None,
    }
}

fn duration_integer_factor(
    expression: &ScalarExpression,
    path: impl Into<String>,
) -> Result<i64, CoreError> {
    let path = path.into();
    match expression {
        ScalarExpression::Literal(Literal::Integer(value)) => Ok(*value),
        ScalarExpression::Literal(Literal::Float(value)) => {
            integral_float_to_i64(value.into_inner(), &path)
        }
        _ => Err(unsupported(
            path,
            "duration multiplication requires a numeric literal factor",
        )),
    }
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    reason = "Duration scaling accepts float literals only after integral and bounds checks."
)]
fn integral_float_to_i64(value: f64, path: &str) -> Result<i64, CoreError> {
    if !value.is_finite() || value.fract() != 0.0 {
        return Err(unsupported(
            path.to_string(),
            "duration multiplication requires an integral numeric literal factor",
        ));
    }
    if value < i64::MIN as f64 || value > i64::MAX as f64 {
        return Err(unsupported(
            path.to_string(),
            "duration multiplication factor is out of range",
        ));
    }
    Ok(value as i64)
}

fn scale_duration_parts(
    duration: DurationParts,
    factor: i64,
    path: &str,
) -> Result<DurationParts, CoreError> {
    let months = duration.months.checked_mul(factor).ok_or_else(|| {
        unsupported(
            path.to_string(),
            "duration multiplication result is out of range",
        )
    })?;
    let days = duration.days.checked_mul(factor).ok_or_else(|| {
        unsupported(
            path.to_string(),
            "duration multiplication result is out of range",
        )
    })?;
    let total_nanos = (i128::from(duration.seconds) * 1_000_000_000 + i128::from(duration.nanos))
        .checked_mul(i128::from(factor))
        .ok_or_else(|| {
            unsupported(
                path.to_string(),
                "duration multiplication result is out of range",
            )
        })?;
    let (seconds, nanos) = normalize_duration_nanos(total_nanos, path, "seconds")?;
    Ok(DurationParts {
        months,
        days,
        seconds,
        nanos,
    })
}

pub(super) fn compile_optional_temporal_component_scalar_expression(
    expression: &Expression,
    path: String,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_temporal_component_scalar_expression(inner, path, mode, context)
        }
        Expression::PropertyLookup { base, property, .. } => {
            let component = property.name.name.as_str();
            if !temporal_component_name_is_reserved(component)
                && matches!(
                    base.as_ref(),
                    Expression::PropertyLookup { .. } | Expression::Variable(_)
                )
            {
                return Ok(None);
            }
            if !is_potential_temporal_component_base(base, mode) {
                return Ok(None);
            }
            let base_expression = compile_scalar_expression_in_predicate_mode(
                base,
                format!("{path}.base"),
                mode,
                context,
            )?;
            if let Some(accessor) = compile_zoned_datetime_accessor(component) {
                match classify_temporal_component_base(&base_expression, mode, context)? {
                    TemporalComponentBaseType::Temporal { kind, timezone } => {
                        if kind != TemporalKind::ZonedDateTime {
                            return Err(unsupported(
                                format!("{path}.property"),
                                format!("{component} is not supported for {} values", kind.name()),
                            ));
                        }
                        return Ok(Some(ScalarExpression::Temporal(
                            TemporalExpr::ZonedDateTimeAccessor {
                                expression: Box::new(base_expression),
                                accessor,
                                timezone,
                            },
                        )));
                    }
                    TemporalComponentBaseType::NonTemporal | TemporalComponentBaseType::Unknown => {
                        return Err(unsupported(
                            format!("{path}.base"),
                            "temporal component access requires a temporal value",
                        ));
                    }
                }
            }
            let unit = compile_temporal_component_unit(component, format!("{path}.property"))?;
            match classify_temporal_component_base(&base_expression, mode, context)? {
                TemporalComponentBaseType::Temporal { kind, .. } => {
                    if !unit.supports_kind(kind) {
                        return Err(unsupported(
                            format!("{path}.property"),
                            format!("{component} is not supported for {} values", kind.name()),
                        ));
                    }
                }
                TemporalComponentBaseType::NonTemporal | TemporalComponentBaseType::Unknown => {
                    return Err(unsupported(
                        format!("{path}.base"),
                        "temporal component access requires a temporal value",
                    ));
                }
            }
            Ok(Some(ScalarExpression::Temporal(TemporalExpr::Component {
                expression: Box::new(base_expression),
                unit,
            })))
        }
        _ => Ok(None),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TemporalComponentBaseType {
    Temporal {
        kind: TemporalKind,
        timezone: Option<String>,
    },
    NonTemporal,
    Unknown,
}

fn classify_temporal_component_base(
    expression: &ScalarExpression,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<TemporalComponentBaseType, CoreError> {
    match expression {
        ScalarExpression::Temporal(temporal) => Ok(temporal_expression_value_kind(temporal)
            .map_or(TemporalComponentBaseType::NonTemporal, |kind| {
                TemporalComponentBaseType::Temporal {
                    kind,
                    timezone: temporal_expression_zoned_timezone(temporal),
                }
            })),
        ScalarExpression::Property(property) => {
            classify_temporal_property_ref(property, mode.static_metadata_plan(), context)
        }
        ScalarExpression::Arithmetic {
            operator,
            left,
            right,
        } => classify_temporal_arithmetic_component_base(*operator, left, right, mode, context),
        ScalarExpression::Literal(_)
        | ScalarExpression::LiteralList { .. }
        | ScalarExpression::TypedLiteralList { .. }
        | ScalarExpression::GraphKeyList { .. }
        | ScalarExpression::Predicate(_)
        | ScalarExpression::Key { .. }
        | ScalarExpression::ElementId { .. }
        | ScalarExpression::GraphIdentity { .. }
        | ScalarExpression::GraphPresence { .. }
        | ScalarExpression::NodeLabels { .. }
        | ScalarExpression::PropertyKeys { .. }
        | ScalarExpression::RelationshipType { .. } => Ok(TemporalComponentBaseType::NonTemporal),
        _ => Ok(TemporalComponentBaseType::Unknown),
    }
}

fn classify_temporal_arithmetic_component_base(
    operator: ArithmeticOperator,
    left: &ScalarExpression,
    right: &ScalarExpression,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<TemporalComponentBaseType, CoreError> {
    let left = classify_temporal_component_base(left, mode, context)?;
    let right = classify_temporal_component_base(right, mode, context)?;
    Ok(match (operator, left, right) {
        (
            ArithmeticOperator::Add | ArithmeticOperator::Subtract,
            TemporalComponentBaseType::Temporal { kind, timezone },
            TemporalComponentBaseType::Temporal {
                kind: TemporalKind::Duration,
                ..
            },
        ) if kind != TemporalKind::Duration => {
            TemporalComponentBaseType::Temporal { kind, timezone }
        }
        (
            ArithmeticOperator::Subtract,
            TemporalComponentBaseType::Temporal {
                kind: TemporalKind::ZonedDateTime,
                ..
            },
            TemporalComponentBaseType::Temporal {
                kind: TemporalKind::ZonedDateTime,
                ..
            },
        )
        | (
            ArithmeticOperator::Multiply,
            TemporalComponentBaseType::Temporal {
                kind: TemporalKind::Duration,
                ..
            },
            TemporalComponentBaseType::NonTemporal | TemporalComponentBaseType::Unknown,
        )
        | (
            ArithmeticOperator::Add | ArithmeticOperator::Subtract,
            TemporalComponentBaseType::Temporal {
                kind: TemporalKind::Duration,
                ..
            },
            TemporalComponentBaseType::Temporal {
                kind: TemporalKind::Duration,
                ..
            },
        ) => TemporalComponentBaseType::Temporal {
            kind: TemporalKind::Duration,
            timezone: None,
        },
        (_, TemporalComponentBaseType::Unknown, _) | (_, _, TemporalComponentBaseType::Unknown) => {
            TemporalComponentBaseType::Unknown
        }
        (_, TemporalComponentBaseType::Temporal { .. }, _)
        | (_, _, TemporalComponentBaseType::Temporal { .. }) => TemporalComponentBaseType::Unknown,
        _ => TemporalComponentBaseType::NonTemporal,
    })
}

fn classify_temporal_property_ref(
    property: &PropertyRef,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<TemporalComponentBaseType, CoreError> {
    let (Some(plan), Some(graph), Some(catalog)) =
        (plan, context.graph.as_ref(), context.catalog.as_ref())
    else {
        return Ok(TemporalComponentBaseType::Unknown);
    };
    let Some((table, column)) = property_ref_table_column(property, plan, graph)? else {
        return Ok(TemporalComponentBaseType::Unknown);
    };
    let Some(data_type) = catalog_column_data_type(catalog, table, column) else {
        return Ok(TemporalComponentBaseType::Unknown);
    };
    Ok(temporal_kind_for_data_type(data_type).map_or(
        TemporalComponentBaseType::NonTemporal,
        |kind| TemporalComponentBaseType::Temporal {
            kind,
            timezone: zoned_timezone_for_data_type(data_type),
        },
    ))
}

fn property_ref_table_column<'a>(
    property: &PropertyRef,
    plan: &GraphPlan,
    graph: &'a Declaration,
) -> Result<Option<(&'a TableRef, &'a str)>, CoreError> {
    if let Some(node_pattern) = plan
        .nodes
        .iter()
        .find(|node| node.variable == property.variable)
    {
        let Some(node) = graph.node(&node_pattern.label) else {
            return Ok(None);
        };
        return Ok(node
            .column_for_property(&property.property)
            .map(|column| (&node.table, column)));
    }

    let Some((relationship_pattern, relationship_index)) = plan
        .relationships
        .iter()
        .enumerate()
        .find_map(|(index, relationship)| {
            (relationship.variable.as_deref() == Some(property.variable.as_str()))
                .then_some((relationship, index))
        })
    else {
        return Ok(None);
    };
    let Some(left_label) = plan_node_label(plan, &relationship_pattern.left) else {
        return Ok(None);
    };
    let Some(right_label) = plan_node_label(plan, &relationship_pattern.right) else {
        return Ok(None);
    };
    let mut matches = graph
        .relationships_for_type(&relationship_pattern.relationship_type)
        .filter(|relationship| {
            relationship_matches_temporal_property_ref_pattern(
                relationship,
                relationship_pattern.direction,
                left_label,
                right_label,
            )
        });
    let Some(relationship) = matches.next() else {
        return Ok(None);
    };
    if matches.next().is_some() {
        return Err(unsupported(
            format!("relationships[{relationship_index}]"),
            "ambiguous relationship property component type; add direction or use distinct relationship types",
        ));
    }
    Ok(relationship
        .column_for_property(&property.property)
        .map(|column| (&relationship.table, column)))
}

fn plan_node_label<'a>(plan: &'a GraphPlan, variable: &str) -> Option<&'a str> {
    plan.nodes
        .iter()
        .find(|node| node.variable == variable)
        .map(|node| node.label.as_str())
}

fn relationship_matches_temporal_property_ref_pattern(
    relationship: &DeclaredRelationship,
    direction: Direction,
    left_label: &str,
    right_label: &str,
) -> bool {
    let matches_forward =
        left_label == relationship.from.label && right_label == relationship.to.label;
    let matches_reverse =
        left_label == relationship.to.label && right_label == relationship.from.label;
    match direction {
        Direction::Outgoing => matches_forward,
        Direction::Incoming => matches_reverse,
        Direction::Undirected => matches_forward || matches_reverse,
    }
}

pub(super) fn catalog_column_data_type<'a>(
    catalog: &'a CatalogInfo,
    table: &TableRef,
    column: &str,
) -> Option<&'a str> {
    catalog
        .tables
        .iter()
        .find(|candidate| {
            candidate.schema_name == table.schema && candidate.table_name == table.name
        })
        .and_then(|table| {
            table
                .columns
                .iter()
                .find(|candidate| candidate.name == column)
        })
        .map(|column| column.data_type.as_str())
}

fn temporal_kind_for_data_type(data_type: &str) -> Option<TemporalKind> {
    let data_type = data_type.trim();
    if data_type.starts_with("Date") {
        return Some(TemporalKind::Date);
    }
    if data_type.starts_with("Time") && !data_type.starts_with("Timestamp") {
        return Some(TemporalKind::LocalTime);
    }
    if data_type.starts_with("Timestamp") {
        return Some(timestamp_data_type_temporal_kind(data_type));
    }
    if data_type.starts_with("Interval") {
        return Some(TemporalKind::Duration);
    }
    if data_type.starts_with("Dictionary") {
        return temporal_kind_for_dictionary_data_type(data_type);
    }
    None
}

fn temporal_kind_for_dictionary_data_type(data_type: &str) -> Option<TemporalKind> {
    if data_type.contains("Date") {
        Some(TemporalKind::Date)
    } else if data_type.contains("Time") && !data_type.contains("Timestamp") {
        Some(TemporalKind::LocalTime)
    } else if data_type.contains("Timestamp") {
        Some(timestamp_data_type_temporal_kind(data_type))
    } else if data_type.contains("Interval") {
        Some(TemporalKind::Duration)
    } else {
        None
    }
}

fn timestamp_data_type_temporal_kind(data_type: &str) -> TemporalKind {
    if data_type.contains("Some(") {
        TemporalKind::ZonedDateTime
    } else {
        TemporalKind::LocalDateTime
    }
}

fn zoned_timezone_for_data_type(data_type: &str) -> Option<String> {
    let start = data_type.find("Some(\"")? + "Some(\"".len();
    let rest = data_type.get(start..)?;
    let end = rest.find('"')?;
    Some(rest.get(..end)?.to_string())
}

fn is_potential_temporal_component_base(
    expression: &Expression,
    mode: PredicateCompileMode<'_>,
) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_potential_temporal_component_base(inner, mode),
        Expression::FunctionCall(function) => {
            is_date_function(function)
                || is_datetime_function(function)
                || is_localdatetime_function(function)
                || is_localtime_function(function)
                || is_duration_function(function)
        }
        Expression::Variable(variable) => mode.scalar_alias_state().is_some_and(|state| {
            scalar_alias_projection(state, &variable_name(variable)).is_some()
        }),
        Expression::PropertyLookup { .. } => true,
        Expression::BinaryOp { op, .. } => matches!(
            op,
            CypherBinaryOperator::Add
                | CypherBinaryOperator::Subtract
                | CypherBinaryOperator::Multiply
                | CypherBinaryOperator::Divide
                | CypherBinaryOperator::Modulo
                | CypherBinaryOperator::Power
        ),
        _ => false,
    }
}

fn compile_zoned_datetime_accessor(component: &str) -> Option<ZonedDateTimeAccessor> {
    match component {
        "timezone" => Some(ZonedDateTimeAccessor::Timezone),
        "offset" => Some(ZonedDateTimeAccessor::Offset),
        "offsetSeconds" => Some(ZonedDateTimeAccessor::OffsetSeconds),
        "offsetMinutes" => Some(ZonedDateTimeAccessor::OffsetMinutes),
        "epochSeconds" => Some(ZonedDateTimeAccessor::EpochSeconds),
        "epochMillis" => Some(ZonedDateTimeAccessor::EpochMillis),
        _ => None,
    }
}

fn compile_temporal_component_unit(
    component: &str,
    path: impl Into<String>,
) -> Result<TemporalComponentUnit, CoreError> {
    match component {
        "year" => Ok(TemporalComponentUnit::Year),
        "quarter" => Ok(TemporalComponentUnit::Quarter),
        "month" => Ok(TemporalComponentUnit::Month),
        "week" => Ok(TemporalComponentUnit::Week),
        "day" => Ok(TemporalComponentUnit::Day),
        "hour" => Ok(TemporalComponentUnit::Hour),
        "minute" => Ok(TemporalComponentUnit::Minute),
        "second" => Ok(TemporalComponentUnit::Second),
        "millisecond" => Ok(TemporalComponentUnit::Millisecond),
        "microsecond" => Ok(TemporalComponentUnit::Microsecond),
        "years" => Ok(TemporalComponentUnit::Years),
        "quarters" => Ok(TemporalComponentUnit::Quarters),
        "months" => Ok(TemporalComponentUnit::Months),
        "weeks" => Ok(TemporalComponentUnit::Weeks),
        "days" => Ok(TemporalComponentUnit::Days),
        "hours" => Ok(TemporalComponentUnit::Hours),
        "minutes" => Ok(TemporalComponentUnit::Minutes),
        "seconds" => Ok(TemporalComponentUnit::Seconds),
        "milliseconds" => Ok(TemporalComponentUnit::Milliseconds),
        "microseconds" => Ok(TemporalComponentUnit::Microseconds),
        "nanoseconds" => Ok(TemporalComponentUnit::Nanoseconds),
        "quartersOfYear" => Ok(TemporalComponentUnit::QuartersOfYear),
        "monthsOfQuarter" => Ok(TemporalComponentUnit::MonthsOfQuarter),
        "monthsOfYear" => Ok(TemporalComponentUnit::MonthsOfYear),
        "daysOfWeek" => Ok(TemporalComponentUnit::DaysOfWeek),
        "minutesOfHour" => Ok(TemporalComponentUnit::MinutesOfHour),
        "secondsOfMinute" => Ok(TemporalComponentUnit::SecondsOfMinute),
        "millisecondsOfSecond" => Ok(TemporalComponentUnit::MillisecondsOfSecond),
        "microsecondsOfSecond" => Ok(TemporalComponentUnit::MicrosecondsOfSecond),
        "nanosecondsOfSecond" => Ok(TemporalComponentUnit::NanosecondsOfSecond),
        _ => Err(unsupported(
            path,
            format!("{component} is not supported yet"),
        )),
    }
}

fn temporal_component_name_is_reserved(component: &str) -> bool {
    matches!(
        component,
        "year"
            | "quarter"
            | "month"
            | "week"
            | "day"
            | "hour"
            | "minute"
            | "second"
            | "millisecond"
            | "microsecond"
            | "years"
            | "quarters"
            | "months"
            | "weeks"
            | "days"
            | "hours"
            | "minutes"
            | "seconds"
            | "milliseconds"
            | "microseconds"
            | "nanoseconds"
            | "quartersOfYear"
            | "monthsOfQuarter"
            | "monthsOfYear"
            | "daysOfWeek"
            | "minutesOfHour"
            | "secondsOfMinute"
            | "millisecondsOfSecond"
            | "microsecondsOfSecond"
            | "nanosecondsOfSecond"
            | "nanosecond"
            | "weekYear"
            | "weekDay"
            | "ordinalDay"
            | "dayOfQuarter"
            | "timezone"
            | "offset"
            | "offsetMinutes"
            | "offsetSeconds"
            | "epochSeconds"
            | "epochMillis"
    )
}

fn temporal_expression_value_kind(expression: &TemporalExpr) -> Option<TemporalKind> {
    match expression {
        TemporalExpr::MakeDate { .. } | TemporalExpr::DateFromString { .. } => {
            Some(TemporalKind::Date)
        }
        TemporalExpr::MakeLocalDateTime { .. } | TemporalExpr::LocalDateTimeFromString { .. } => {
            Some(TemporalKind::LocalDateTime)
        }
        TemporalExpr::MakeZonedDateTime { .. } | TemporalExpr::ZonedDateTimeFromString { .. } => {
            Some(TemporalKind::ZonedDateTime)
        }
        TemporalExpr::MakeLocalTime { .. } | TemporalExpr::LocalTimeFromString { .. } => {
            Some(TemporalKind::LocalTime)
        }
        TemporalExpr::MakeDuration { .. } | TemporalExpr::DurationInUnits { .. } => {
            Some(TemporalKind::Duration)
        }
        TemporalExpr::Component { .. } | TemporalExpr::ZonedDateTimeAccessor { .. } => None,
    }
}

fn temporal_expression_zoned_timezone(expression: &TemporalExpr) -> Option<String> {
    match expression {
        TemporalExpr::MakeZonedDateTime { timezone, .. }
        | TemporalExpr::ZonedDateTimeFromString { timezone, .. } => Some(timezone.clone()),
        _ => None,
    }
}
