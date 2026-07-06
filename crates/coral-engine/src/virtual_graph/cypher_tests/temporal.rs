use super::*;

#[test]
fn compiles_date_map_constructor_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN date({year: 1984, month: 10, day: 11}) AS full, \
                date({year: 1984, month: 10}) AS default_day, \
                date({year: 1984}) AS default_month_day, \
                toString(date({year: 1984, month: 10, day: 11})) AS text",
    )
    .expect("literal date map constructors should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: date_expression(1984, 10, 11),
                alias: "full".to_string(),
            },
            Projection::Expression {
                expression: date_expression(1984, 10, 1),
                alias: "default_day".to_string(),
            },
            Projection::Expression {
                expression: date_expression(1984, 1, 1),
                alias: "default_month_day".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToString {
                    expression: Box::new(date_expression(1984, 10, 11)),
                },
                alias: "text".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_date_string_constructor_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN date('2020-01-01') AS d, \
                toString(date('2020-01-01')) AS text",
    )
    .expect("literal date string constructors should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: date_from_string_expression("2020-01-01"),
                alias: "d".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToString {
                    expression: Box::new(date_from_string_expression("2020-01-01")),
                },
                alias: "text".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_unsupported_date_constructor_forms() {
    for (cypher, expected) in [
        (
            "MATCH (person:Person) RETURN date(person.name) AS d",
            "dynamic date() string argument not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN date(2020) AS d",
            "date() requires a literal map or string argument",
        ),
        (
            "MATCH (person:Person) RETURN date({year: person.age}) AS d",
            "dynamic temporal fields not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN date({year: 2020, week: 1}) AS d",
            "date() temporal field 'week' is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN date({year: 2020, quarter: 1}) AS d",
            "date() temporal field 'quarter' is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN date({year: 2020, ordinalDay: 1}) AS d",
            "date() temporal field 'ordinalDay' is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN date({date: date({year: 2020})}) AS d",
            "date() temporal field 'date' is not supported yet",
        ),
    ] {
        let error = compile_cypher(cypher).expect_err("unsupported date form should be rejected");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
    }
}

#[test]
fn compiles_localdatetime_map_constructor_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN localdatetime({year: 2020, month: 1, day: 15, hour: 12, minute: 34, second: 56}) AS full, \
                localdatetime({year: 2020, month: 1, day: 15}) AS default_time, \
                toString(localdatetime({year: 2020, month: 1, day: 15, hour: 12, minute: 34, second: 56})) AS text",
    )
    .expect("literal localdatetime map constructors should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: localdatetime_expression(2020, 1, 15, 12, 34, 56, 0, 0, 0),
                alias: "full".to_string(),
            },
            Projection::Expression {
                expression: localdatetime_expression(2020, 1, 15, 0, 0, 0, 0, 0, 0),
                alias: "default_time".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToString {
                    expression: Box::new(localdatetime_expression(
                        2020, 1, 15, 12, 34, 56, 0, 0, 0,
                    )),
                },
                alias: "text".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_localdatetime_string_constructor_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN localdatetime('2020-01-15T12:34:56') AS d, \
                toString(localdatetime('2020-01-15T12:34:56')) AS text",
    )
    .expect("literal localdatetime string constructors should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: localdatetime_from_string_expression("2020-01-15T12:34:56"),
                alias: "d".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToString {
                    expression: Box::new(localdatetime_from_string_expression(
                        "2020-01-15T12:34:56",
                    )),
                },
                alias: "text".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_unsupported_localdatetime_constructor_forms() {
    for (cypher, expected) in [
        (
            "MATCH (person:Person) RETURN localdatetime(person.name) AS d",
            "dynamic localdatetime() string argument not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN localdatetime('2020-01-15T12:34:56Z') AS d",
            "localdatetime() does not accept a timezone; use a naive date-time string",
        ),
        (
            "MATCH (person:Person) RETURN localdatetime('2020-01-15T12:34:56+01:00') AS d",
            "localdatetime() does not accept a timezone; use a naive date-time string",
        ),
        (
            "MATCH (person:Person) RETURN localdatetime(2020) AS d",
            "localdatetime() requires a literal map or string argument",
        ),
        (
            "MATCH (person:Person) RETURN localdatetime({year: person.age}) AS d",
            "dynamic temporal fields not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN localdatetime({month: 1, day: 15}) AS d",
            "localdatetime() map constructor requires a literal integer year",
        ),
        (
            "MATCH (person:Person) RETURN localdatetime({year: 2020, timezone: 'UTC'}) AS d",
            "localdatetime() temporal field 'timezone' is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN localdatetime({year: 2020, date: date({year: 2020})}) AS d",
            "localdatetime() temporal field 'date' is not supported yet",
        ),
    ] {
        let error =
            compile_cypher(cypher).expect_err("unsupported localdatetime form should be rejected");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
    }
}

#[test]
fn compiles_zoneddatetime_string_constructor_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN datetime('2020-06-01T09:00:00+01:00') AS offset_datetime, \
                datetime('2015-07-21T21:40:32.142+02:00[Europe/Stockholm]') AS named_offset_datetime, \
                datetime('2015-07-21T21:40:32.142[Europe/London]') AS named_datetime, \
                toString(datetime('2020-06-01T09:00:00+01:00')) AS offset_text, \
                toString(datetime('2015-07-21T21:40:32.142[Europe/London]')) AS named_text",
    )
    .expect("literal datetime string constructors should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: zoneddatetime_from_string_expression(
                    "2020-06-01T09:00:00+01:00",
                    "+01:00",
                ),
                alias: "offset_datetime".to_string(),
            },
            Projection::Expression {
                expression: zoneddatetime_from_string_expression(
                    "2015-07-21T21:40:32.142+02:00",
                    "Europe/Stockholm",
                ),
                alias: "named_offset_datetime".to_string(),
            },
            Projection::Expression {
                expression: zoneddatetime_from_string_expression(
                    "2015-07-21T21:40:32.142",
                    "Europe/London",
                ),
                alias: "named_datetime".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToString {
                    expression: Box::new(zoneddatetime_from_string_expression(
                        "2020-06-01T09:00:00+01:00",
                        "+01:00",
                    )),
                },
                alias: "offset_text".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToString {
                    expression: Box::new(zoneddatetime_from_string_expression(
                        "2015-07-21T21:40:32.142",
                        "Europe/London",
                    )),
                },
                alias: "named_text".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_zoneddatetime_map_constructor_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: 'Europe/Stockholm'}) AS named_datetime, \
                datetime({year: 1984, timezone: '+01:00'}) AS default_fields",
    )
    .expect("literal datetime map constructors should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: zoneddatetime_expression(
                    1984,
                    10,
                    11,
                    12,
                    31,
                    14,
                    0,
                    0,
                    645_876_123,
                    "Europe/Stockholm",
                ),
                alias: "named_datetime".to_string(),
            },
            Projection::Expression {
                expression: zoneddatetime_expression(1984, 1, 1, 0, 0, 0, 0, 0, 0, "+01:00"),
                alias: "default_fields".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_unsupported_zoneddatetime_constructor_forms() {
    for (cypher, expected) in [
        (
            "MATCH (person:Person) RETURN datetime(person.name) AS d",
            "dynamic datetime() string argument not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN datetime('2020-06-01T09:00:00') AS d",
            "datetime() requires a timezone offset or bracketed timezone",
        ),
        (
            "MATCH (person:Person) RETURN datetime({year: person.age, timezone: 'UTC'}) AS d",
            "dynamic temporal fields not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN datetime({month: 1, day: 15, timezone: 'UTC'}) AS d",
            "datetime() map constructor requires a literal integer year",
        ),
        (
            "MATCH (person:Person) RETURN datetime({year: 2020}) AS d",
            "datetime() map constructor requires a literal string timezone",
        ),
        (
            "MATCH (person:Person) RETURN datetime({year: 2020, timezone: person.name}) AS d",
            "datetime() map constructor requires a literal string timezone",
        ),
        (
            "MATCH (person:Person) RETURN datetime({year: 2020, timezone: 'Mars/Olympus'}) AS d",
            "datetime() timezone must be a fixed offset or IANA timezone",
        ),
        (
            "MATCH (person:Person) RETURN datetime('2021-03-28T01:30:00[Europe/London]') AS d",
            "daylight-saving gap",
        ),
        (
            "MATCH (person:Person) RETURN datetime('2021-10-31T01:30:00[Europe/London]') AS d",
            "daylight-saving overlap",
        ),
    ] {
        let error =
            compile_cypher(cypher).expect_err("unsupported datetime form should be rejected");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
    }
}

#[test]
fn compiles_localtime_map_constructor_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN localtime({hour: 12, minute: 34, second: 56}) AS full, \
                localtime({hour: 12}) AS default_time, \
                toString(localtime({hour: 12, minute: 34, second: 56})) AS text",
    )
    .expect("literal localtime map constructors should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: localtime_expression(12, 34, 56, 0, 0, 0),
                alias: "full".to_string(),
            },
            Projection::Expression {
                expression: localtime_expression(12, 0, 0, 0, 0, 0),
                alias: "default_time".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToString {
                    expression: Box::new(localtime_expression(12, 34, 56, 0, 0, 0)),
                },
                alias: "text".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_localtime_string_constructor_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN localtime('12:34:56') AS t, \
                toString(localtime('12:34:56')) AS text",
    )
    .expect("literal localtime string constructors should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: localtime_from_string_expression("12:34:56"),
                alias: "t".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToString {
                    expression: Box::new(localtime_from_string_expression("12:34:56")),
                },
                alias: "text".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_unsupported_localtime_constructor_forms() {
    for (cypher, expected) in [
        (
            "MATCH (person:Person) RETURN localtime(person.name) AS t",
            "dynamic localtime() string argument not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN localtime('12:34:56Z') AS t",
            "localtime() does not accept a timezone; use a naive time string",
        ),
        (
            "MATCH (person:Person) RETURN localtime('12:34:56+01:00') AS t",
            "localtime() does not accept a timezone; use a naive time string",
        ),
        (
            "MATCH (person:Person) RETURN localtime(12) AS t",
            "localtime() requires a literal map or string argument",
        ),
        (
            "MATCH (person:Person) RETURN localtime({hour: person.age}) AS t",
            "dynamic temporal fields not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN localtime({minute: 34, second: 56}) AS t",
            "localtime() map constructor requires a literal integer hour",
        ),
        (
            "MATCH (person:Person) RETURN localtime({hour: 12, timezone: 'UTC'}) AS t",
            "localtime() temporal field 'timezone' is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN localtime({hour: 12, date: date({year: 2020})}) AS t",
            "localtime() temporal field 'date' is not supported yet",
        ),
    ] {
        let error =
            compile_cypher(cypher).expect_err("unsupported localtime form should be rejected");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
    }
}

#[test]
fn compiles_duration_constructor_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN duration('P1Y2M3DT4H') AS iso, \
                duration({years: 1, months: 2, weeks: 1, days: 3, hours: 4, minutes: 5, seconds: 6, milliseconds: 7, microseconds: 8, nanoseconds: 9}) AS map, \
                toString(duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 1})) AS text, \
                duration(toString(duration({seconds: 2, milliseconds: -1}))) AS roundtrip",
    )
    .expect("literal duration constructors should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: duration_expression(14, 3, 14_400, 0),
                alias: "iso".to_string(),
            },
            Projection::Expression {
                expression: duration_expression(14, 10, 14_706, 7_008_009),
                alias: "map".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToString {
                    expression: Box::new(duration_expression(149, 14, 58_390, 1)),
                },
                alias: "text".to_string(),
            },
            Projection::Expression {
                expression: duration_expression(0, 0, 1, 999_000_000),
                alias: "roundtrip".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_temporal_duration_unit_total_functions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN duration.between(date('1984-10-11'), date('2015-06-24')) AS betweenDuration, \
                duration.inMonths(date('1984-10-11'), date('2015-06-24')) AS monthsDuration, \
                duration.inSeconds(localdatetime('2020-01-01T00:00:00'), localdatetime('2020-03-01T12:00:00')) AS secondsDuration, \
                duration.inDays(date('1984-10-11'), date('2015-06-24')) AS daysDuration, \
                toString(duration.inSeconds(null, null)) AS nullDuration",
    )
    .expect("duration unit-total functions should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: duration_in_units_expression(
                    TemporalDurationUnit::Between,
                    date_from_string_expression("1984-10-11"),
                    date_from_string_expression("2015-06-24"),
                ),
                alias: "betweenDuration".to_string(),
            },
            Projection::Expression {
                expression: duration_in_units_expression(
                    TemporalDurationUnit::Months,
                    date_from_string_expression("1984-10-11"),
                    date_from_string_expression("2015-06-24"),
                ),
                alias: "monthsDuration".to_string(),
            },
            Projection::Expression {
                expression: duration_in_units_expression(
                    TemporalDurationUnit::Seconds,
                    localdatetime_from_string_expression("2020-01-01T00:00:00"),
                    localdatetime_from_string_expression("2020-03-01T12:00:00"),
                ),
                alias: "secondsDuration".to_string(),
            },
            Projection::Expression {
                expression: duration_in_units_expression(
                    TemporalDurationUnit::Days,
                    date_from_string_expression("1984-10-11"),
                    date_from_string_expression("2015-06-24"),
                ),
                alias: "daysDuration".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToString {
                    expression: Box::new(duration_in_units_expression(
                        TemporalDurationUnit::Seconds,
                        ScalarExpression::Literal(Literal::Null),
                        ScalarExpression::Literal(Literal::Null),
                    )),
                },
                alias: "nullDuration".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_temporal_duration_arithmetic_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN date('2020-01-31') + duration('P1M') AS dateShift, \
                date('2020-03-15') - duration({months: 1}) AS dateBack, \
                localdatetime('2020-01-01T00:00:00') + duration('PT1H30M') AS datetimeShift, \
                localtime('12:00:00') + duration('PT90M') AS timeShift, \
                date('2020-01-01') + duration('P1D') * 2 AS scaled, \
                datetime('2020-03-29T00:30:00[Europe/London]') + duration('PT1H') AS zonedShift, \
                datetime('2020-06-01T13:00:00+01:00[Europe/London]') - datetime('2020-06-01T12:00:00Z') AS zonedDelta",
    )
    .expect("temporal duration arithmetic should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Add,
                    left: Box::new(date_from_string_expression("2020-01-31")),
                    right: Box::new(duration_expression(1, 0, 0, 0)),
                },
                alias: "dateShift".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Subtract,
                    left: Box::new(date_from_string_expression("2020-03-15")),
                    right: Box::new(duration_expression(1, 0, 0, 0)),
                },
                alias: "dateBack".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Add,
                    left: Box::new(localdatetime_from_string_expression("2020-01-01T00:00:00",)),
                    right: Box::new(duration_expression(0, 0, 5_400, 0)),
                },
                alias: "datetimeShift".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Add,
                    left: Box::new(localtime_from_string_expression("12:00:00")),
                    right: Box::new(duration_expression(0, 0, 5_400, 0)),
                },
                alias: "timeShift".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Add,
                    left: Box::new(date_from_string_expression("2020-01-01")),
                    right: Box::new(duration_expression(0, 2, 0, 0)),
                },
                alias: "scaled".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Add,
                    left: Box::new(zoneddatetime_from_string_expression(
                        "2020-03-29T00:30:00",
                        "Europe/London",
                    )),
                    right: Box::new(duration_expression(0, 0, 3_600, 0)),
                },
                alias: "zonedShift".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Subtract,
                    left: Box::new(zoneddatetime_from_string_expression(
                        "2020-06-01T13:00:00+01:00",
                        "Europe/London",
                    )),
                    right: Box::new(zoneddatetime_from_string_expression(
                        "2020-06-01T12:00:00Z",
                        "+00:00",
                    )),
                },
                alias: "zonedDelta".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_unsupported_duration_constructor_and_multiply_forms() {
    for (cypher, expected) in [
        (
            "MATCH (person:Person) RETURN duration(person.name) AS d",
            "dynamic duration() argument not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN duration(1) AS d",
            "duration() requires a literal map or string argument",
        ),
        (
            "MATCH (person:Person) RETURN duration({days: person.age}) AS d",
            "dynamic duration fields not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN duration({quarters: 1}) AS d",
            "duration() temporal field 'quarters' is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN duration('P') AS d",
            "duration() requires an ISO-8601 duration string literal",
        ),
        (
            "MATCH (person:Person) RETURN duration(toString(date('2020-01-01'))) AS d",
            "duration(toString(...)) requires a duration-valued argument",
        ),
        (
            "MATCH (person:Person) RETURN duration('P1D').day AS days",
            "day is not supported for duration values",
        ),
        (
            "MATCH (person:Person) RETURN duration.inDays(datetime('2020-01-01T00:00:00+01:00'), date('2020-01-02')) AS d",
            "duration.inDays() does not support zoned datetime() or time() arguments yet",
        ),
        (
            "MATCH (person:Person) RETURN duration.between(datetime('2020-01-01T00:00:00+01:00'), date('2020-01-02')) AS d",
            "duration.between() does not support zoned datetime() or time() arguments yet",
        ),
        (
            "MATCH (person:Person) RETURN date('2020-01-01') + duration('P1D') * person.age AS shifted",
            "duration multiplication requires a numeric literal factor",
        ),
    ] {
        let error = compile_cypher(cypher).expect_err("unsupported duration form should reject");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
    }
}

#[test]
fn compiles_constructed_temporal_component_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN date('2020-01-15').year AS year, \
                date('2020-01-15').month AS month, \
                date('2020-01-15').day AS day, \
                localdatetime('2020-01-15T12:34:56').hour AS hour, \
                localdatetime('2020-01-15T12:34:56').minute AS minute, \
                localdatetime('2020-01-15T12:34:56').second AS second, \
                localdatetime('2020-01-15T12:34:56.789123456').millisecond AS millisecond, \
                localdatetime('2020-01-15T12:34:56.789123456').microsecond AS microsecond, \
                localtime('12:34:56').hour AS timeHour",
    )
    .expect("constructed temporal component access should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: temporal_component_expression(
                    date_from_string_expression("2020-01-15"),
                    TemporalComponentUnit::Year,
                ),
                alias: "year".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    date_from_string_expression("2020-01-15"),
                    TemporalComponentUnit::Month,
                ),
                alias: "month".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    date_from_string_expression("2020-01-15"),
                    TemporalComponentUnit::Day,
                ),
                alias: "day".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    localdatetime_from_string_expression("2020-01-15T12:34:56"),
                    TemporalComponentUnit::Hour,
                ),
                alias: "hour".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    localdatetime_from_string_expression("2020-01-15T12:34:56"),
                    TemporalComponentUnit::Minute,
                ),
                alias: "minute".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    localdatetime_from_string_expression("2020-01-15T12:34:56"),
                    TemporalComponentUnit::Second,
                ),
                alias: "second".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    localdatetime_from_string_expression("2020-01-15T12:34:56.789123456"),
                    TemporalComponentUnit::Millisecond,
                ),
                alias: "millisecond".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    localdatetime_from_string_expression("2020-01-15T12:34:56.789123456"),
                    TemporalComponentUnit::Microsecond,
                ),
                alias: "microsecond".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    localtime_from_string_expression("12:34:56"),
                    TemporalComponentUnit::Hour,
                ),
                alias: "timeHour".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_constructed_zoneddatetime_component_and_accessor_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN datetime('2020-06-01T13:00:00+01:00[Europe/London]').hour AS londonHour, \
                datetime('2020-06-01T23:30:00-04:00[America/New_York]').day AS newYorkDay, \
                datetime('2020-06-01T13:00:00+01:00[Europe/London]').timezone AS timezone, \
                datetime('2020-06-01T13:00:00+01:00[Europe/London]').offset AS offset, \
                datetime('2020-06-01T13:00:00+01:00[Europe/London]').offsetSeconds AS offsetSeconds, \
                datetime('2020-06-01T13:00:00+01:00[Europe/London]').offsetMinutes AS offsetMinutes, \
                datetime('2020-06-01T13:00:00+01:00[Europe/London]').epochSeconds AS epochSeconds, \
                datetime('2020-06-01T13:00:00+01:00[Europe/London]').epochMillis AS epochMillis, \
                (datetime('2020-03-29T00:30:00[Europe/London]') + duration('PT1H')).hour AS dstHour",
    )
    .expect("constructed zoned datetime component and accessor access should compile");

    let london =
        || zoneddatetime_from_string_expression("2020-06-01T13:00:00+01:00", "Europe/London");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: temporal_component_expression(london(), TemporalComponentUnit::Hour),
                alias: "londonHour".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    zoneddatetime_from_string_expression(
                        "2020-06-01T23:30:00-04:00",
                        "America/New_York",
                    ),
                    TemporalComponentUnit::Day,
                ),
                alias: "newYorkDay".to_string(),
            },
            Projection::Expression {
                expression: zoneddatetime_accessor_expression(
                    london(),
                    ZonedDateTimeAccessor::Timezone,
                    Some("Europe/London"),
                ),
                alias: "timezone".to_string(),
            },
            Projection::Expression {
                expression: zoneddatetime_accessor_expression(
                    london(),
                    ZonedDateTimeAccessor::Offset,
                    Some("Europe/London"),
                ),
                alias: "offset".to_string(),
            },
            Projection::Expression {
                expression: zoneddatetime_accessor_expression(
                    london(),
                    ZonedDateTimeAccessor::OffsetSeconds,
                    Some("Europe/London"),
                ),
                alias: "offsetSeconds".to_string(),
            },
            Projection::Expression {
                expression: zoneddatetime_accessor_expression(
                    london(),
                    ZonedDateTimeAccessor::OffsetMinutes,
                    Some("Europe/London"),
                ),
                alias: "offsetMinutes".to_string(),
            },
            Projection::Expression {
                expression: zoneddatetime_accessor_expression(
                    london(),
                    ZonedDateTimeAccessor::EpochSeconds,
                    Some("Europe/London"),
                ),
                alias: "epochSeconds".to_string(),
            },
            Projection::Expression {
                expression: zoneddatetime_accessor_expression(
                    london(),
                    ZonedDateTimeAccessor::EpochMillis,
                    Some("Europe/London"),
                ),
                alias: "epochMillis".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    ScalarExpression::Arithmetic {
                        operator: ArithmeticOperator::Add,
                        left: Box::new(zoneddatetime_from_string_expression(
                            "2020-03-29T00:30:00",
                            "Europe/London",
                        )),
                        right: Box::new(duration_expression(0, 0, 3_600, 0)),
                    },
                    TemporalComponentUnit::Hour,
                ),
                alias: "dstHour".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_constructed_duration_component_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN duration({years: 1, months: 4, days: 10, hours: 1, minutes: 1, seconds: 1, nanoseconds: 111111111}).years AS years, \
                duration({months: 16}).quarters AS quarters, \
                duration({months: 16}).months AS months, \
                duration({days: 10}).weeks AS weeks, \
                duration({days: 10}).days AS days, \
                duration({hours: 1, minutes: 1, seconds: 1, nanoseconds: 111111111}).hours AS hours, \
                duration({hours: 1, minutes: 1, seconds: 1, nanoseconds: 111111111}).minutes AS minutes, \
                duration({hours: 1, minutes: 1, seconds: 1, nanoseconds: 111111111}).seconds AS seconds, \
                duration({hours: 1, minutes: 1, seconds: 1, nanoseconds: 111111111}).milliseconds AS milliseconds, \
                duration({hours: 1, minutes: 1, seconds: 1, nanoseconds: 111111111}).microseconds AS microseconds, \
                duration({hours: 1, minutes: 1, seconds: 1, nanoseconds: 111111111}).nanoseconds AS nanoseconds, \
                duration({months: 16}).quartersOfYear AS quartersOfYear, \
                duration({months: 16}).monthsOfQuarter AS monthsOfQuarter, \
                duration({months: 16}).monthsOfYear AS monthsOfYear, \
                duration({days: 10}).daysOfWeek AS daysOfWeek, \
                duration({hours: 1, minutes: 1, seconds: 1, nanoseconds: 111111111}).minutesOfHour AS minutesOfHour, \
                duration({hours: 1, minutes: 1, seconds: 1, nanoseconds: 111111111}).secondsOfMinute AS secondsOfMinute, \
                duration({hours: 1, minutes: 1, seconds: 1, nanoseconds: 111111111}).millisecondsOfSecond AS millisecondsOfSecond, \
                duration({hours: 1, minutes: 1, seconds: 1, nanoseconds: 111111111}).microsecondsOfSecond AS microsecondsOfSecond, \
                duration({hours: 1, minutes: 1, seconds: 1, nanoseconds: 111111111}).nanosecondsOfSecond AS nanosecondsOfSecond",
    )
    .expect("constructed duration component access should compile");

    let units =
        plan.projections
            .iter()
            .map(|projection| match projection {
                Projection::Expression {
                    expression:
                        ScalarExpression::Temporal(TemporalExpr::Component { expression, unit }),
                    alias,
                } => {
                    assert!(
                        matches!(
                            expression.as_ref(),
                            ScalarExpression::Temporal(TemporalExpr::MakeDuration { .. })
                        ),
                        "duration component base should be a duration expression"
                    );
                    (alias.as_str(), *unit)
                }
                projection => panic!("unexpected projection: {projection:?}"),
            })
            .collect::<Vec<_>>();

    assert_eq!(
        units,
        vec![
            ("years", TemporalComponentUnit::Years),
            ("quarters", TemporalComponentUnit::Quarters),
            ("months", TemporalComponentUnit::Months),
            ("weeks", TemporalComponentUnit::Weeks),
            ("days", TemporalComponentUnit::Days),
            ("hours", TemporalComponentUnit::Hours),
            ("minutes", TemporalComponentUnit::Minutes),
            ("seconds", TemporalComponentUnit::Seconds),
            ("milliseconds", TemporalComponentUnit::Milliseconds),
            ("microseconds", TemporalComponentUnit::Microseconds),
            ("nanoseconds", TemporalComponentUnit::Nanoseconds),
            ("quartersOfYear", TemporalComponentUnit::QuartersOfYear),
            ("monthsOfQuarter", TemporalComponentUnit::MonthsOfQuarter),
            ("monthsOfYear", TemporalComponentUnit::MonthsOfYear),
            ("daysOfWeek", TemporalComponentUnit::DaysOfWeek),
            ("minutesOfHour", TemporalComponentUnit::MinutesOfHour),
            ("secondsOfMinute", TemporalComponentUnit::SecondsOfMinute),
            (
                "millisecondsOfSecond",
                TemporalComponentUnit::MillisecondsOfSecond,
            ),
            (
                "microsecondsOfSecond",
                TemporalComponentUnit::MicrosecondsOfSecond,
            ),
            (
                "nanosecondsOfSecond",
                TemporalComponentUnit::NanosecondsOfSecond
            ),
        ]
    );
}

#[test]
fn compiles_stored_temporal_component_scalar_expressions_with_catalog() {
    let graph = temporal_columns_test_graph();
    let catalog = temporal_columns_catalog();
    let query = compile_cypher_query_for_graph_with_parameters_and_catalog(
        &graph,
        "MATCH (person:Person) \
         RETURN person.joined.year AS joinedYear, \
                person.birthday.month AS birthdayMonth, \
                person.zoned.timezone AS timezone",
        &BTreeMap::new(),
        &catalog,
    )
    .expect("stored temporal component access should compile with catalog types");
    let GraphQuery::Plan(plan) = query else {
        panic!("stored temporal component query should compile to one plan");
    };

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: temporal_component_expression(
                    ScalarExpression::Property(PropertyRef {
                        variable: "person".to_string(),
                        property: "joined".to_string(),
                    }),
                    TemporalComponentUnit::Year,
                ),
                alias: "joinedYear".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    ScalarExpression::Property(PropertyRef {
                        variable: "person".to_string(),
                        property: "birthday".to_string(),
                    }),
                    TemporalComponentUnit::Month,
                ),
                alias: "birthdayMonth".to_string(),
            },
            Projection::Expression {
                expression: zoneddatetime_accessor_expression(
                    ScalarExpression::Property(PropertyRef {
                        variable: "person".to_string(),
                        property: "zoned".to_string(),
                    }),
                    ZonedDateTimeAccessor::Timezone,
                    Some("Europe/London"),
                ),
                alias: "timezone".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_terminal_with_stored_temporal_component_scalar_expression_with_catalog() {
    let graph = temporal_columns_test_graph();
    let catalog = temporal_columns_catalog();
    let query = compile_cypher_query_for_graph_with_parameters_and_catalog(
        &graph,
        "MATCH (person:Person) WITH person.joined AS t RETURN t.year AS year",
        &BTreeMap::new(),
        &catalog,
    )
    .expect("terminal WITH stored temporal component access should compile with catalog types");
    let GraphQuery::Plan(plan) = query else {
        panic!("terminal WITH stored temporal component query should compile to one plan");
    };

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: temporal_component_expression(
                ScalarExpression::Property(PropertyRef {
                    variable: "person".to_string(),
                    property: "joined".to_string(),
                }),
                TemporalComponentUnit::Year,
            ),
            alias: "year".to_string(),
        }]
    );
}

#[test]
fn compiles_terminal_with_multiple_duration_components_from_one_alias() {
    let query = compile_cypher(
        "MATCH (person:Person) \
         WITH duration({months: 14}) AS d \
         RETURN d.years AS years, d.months AS months, d.monthsOfYear AS monthsOfYear",
    )
    .expect("terminal WITH duration component access should compile");

    assert_eq!(
        query.projections,
        vec![
            Projection::Expression {
                expression: temporal_component_expression(
                    duration_expression(14, 0, 0, 0),
                    TemporalComponentUnit::Years,
                ),
                alias: "years".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    duration_expression(14, 0, 0, 0),
                    TemporalComponentUnit::Months,
                ),
                alias: "months".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    duration_expression(14, 0, 0, 0),
                    TemporalComponentUnit::MonthsOfYear,
                ),
                alias: "monthsOfYear".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_stored_temporal_component_kind_mismatch_with_catalog() {
    let graph = temporal_columns_test_graph();
    let catalog = temporal_columns_catalog();
    let error = compile_cypher_query_for_graph_with_parameters_and_catalog(
        &graph,
        "MATCH (person:Person) RETURN person.birthday.hour AS hour",
        &BTreeMap::new(),
        &catalog,
    )
    .expect_err("stored date hour component should reject");

    assert!(
        error
            .to_string()
            .contains("hour is not supported for date values"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_unsupported_temporal_component_access() {
    for (cypher, expected) in [
        (
            "MATCH (person:Person) RETURN date('2020-01-15').weekYear AS weekYear",
            "weekYear is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN date('2020-01-15').weekDay AS weekDay",
            "weekDay is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN date('2020-01-15').ordinalDay AS ordinalDay",
            "ordinalDay is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN date('2020-01-15').dayOfQuarter AS dayOfQuarter",
            "dayOfQuarter is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN localtime('12:34:56.789123456').nanosecond AS ns",
            "nanosecond is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN localtime('12:34:56').year AS year",
            "year is not supported for localtime values",
        ),
        (
            "MATCH (person:Person) RETURN duration('P1D').day AS day",
            "day is not supported for duration values",
        ),
        (
            "MATCH (person:Person) RETURN date('2020-01-15').years AS years",
            "years is not supported for date values",
        ),
        (
            "MATCH (person:Person) RETURN localdatetime('2020-01-15T12:34:56').timezone AS timezone",
            "timezone is not supported for localdatetime values",
        ),
        (
            "MATCH (person:Person) RETURN date('2020-01-15').epochSeconds AS epochSeconds",
            "epochSeconds is not supported for date values",
        ),
        (
            "MATCH (person:Person) RETURN duration('P1D').offset AS offset",
            "offset is not supported for duration values",
        ),
        (
            "MATCH (person:Person) WITH person.name AS d RETURN d.year AS year",
            "temporal component access requires a temporal value",
        ),
    ] {
        let error =
            compile_cypher(cypher).expect_err("unsupported temporal component should reject");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
    }
}
