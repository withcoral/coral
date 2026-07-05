//! Scalar-expression rendering for the SQL `SqlRenderer`: translates graph-plan
//! `ScalarExpression`s into `DataFusion` SQL — structural and graph-metadata references,
//! `CAST/TRY_CAST`, unary/binary/ternary string and math functions, COALESCE, REPLACE,
//! ROUND, SUBSTRING, arithmetic, CASE, and presence-gated expressions. The top-level
//! (non-scoped) counterpart to the scoped-subquery scalar rendering in `scoped`.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "SQL scalar helpers are split into a child module while preserving parent-private access."
)]
use super::*;
use crate::virtual_graph::ir::TemporalDurationUnit;

#[allow(
    clippy::allow_attributes,
    clippy::elidable_lifetime_names,
    reason = "SQL child modules use the same explicit SqlRenderer lifetime shape as the parent impl."
)]
impl<'a> SqlRenderer<'a> {
    pub(super) fn render_scalar_expression(
        &self,
        expression: &ScalarExpression,
    ) -> Result<String, CoreError> {
        if let Some(rendered) = self.render_simple_scalar_expression(expression)? {
            return Ok(rendered);
        }
        if let Some(rendered) = self.render_graph_metadata_scalar_expression(expression)? {
            return Ok(rendered);
        }

        self.render_structural_scalar_expression(expression)
    }

    fn render_scalar_in_scope<'b, 'c>(
        &self,
        expr: &ScalarExpression,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        match scope {
            ScalarScope::TopLevel => self.render_scalar_expression(expr),
            ScalarScope::Scoped {
                relationships,
                local_nodes,
                local_aliases,
            } => self.render_scoped_scalar_expression(
                expr,
                relationships,
                local_nodes,
                local_aliases,
            ),
        }
    }

    fn render_structural_scalar_expression(
        &self,
        expression: &ScalarExpression,
    ) -> Result<String, CoreError> {
        match expression {
            ScalarExpression::StageValue { alias } => {
                let (stage_alias, value_column) = self.validated.stage_scalar_column_ref(alias)?;
                Ok(format!(
                    "{}.{}",
                    quote_ident(stage_alias),
                    quote_ident(value_column)
                ))
            }
            ScalarExpression::Property(property) => self.render_property_ref(property),
            ScalarExpression::Literal(literal) => Ok(render_literal(literal)),
            ScalarExpression::LiteralList { literals } => Ok(render_literal_list(literals)),
            ScalarExpression::TypedLiteralList {
                literals,
                element_type,
            } => Ok(render_typed_literal_list(literals, *element_type)),
            ScalarExpression::ListConcat { left, right } => Ok(format!(
                "array_concat({}, {})",
                self.render_scalar_expression(left)?,
                self.render_scalar_expression(right)?
            )),
            ScalarExpression::ListIndex { list, index, .. } => Ok(format!(
                "{}[{}]",
                self.render_scalar_expression(list)?,
                index + 1
            )),
            ScalarExpression::PathValue {
                node_variables,
                relationship_variables,
            } => self.render_path_value_ref(node_variables, relationship_variables),
            ScalarExpression::Predicate(predicate) => {
                self.render_scalar_predicate_expression(predicate)
            }
            ScalarExpression::CountSubquery {
                pattern,
                distinct_target,
            } => self.render_count_subquery_expression(pattern, distinct_target.as_deref()),
            ScalarExpression::CollectSubquery {
                pattern,
                target,
                distinct,
            } => self.render_collect_subquery_expression(pattern, target, *distinct),
            ScalarExpression::PresenceGated {
                presence_variable,
                expression,
            } => self.render_presence_gated_scalar_expression(presence_variable, expression),
            ScalarExpression::Coalesce { expressions } => {
                self.render_coalesce_expression(expressions, ScalarScope::TopLevel)
            }
            ScalarExpression::NullIf { expression, value } => {
                self.render_null_if(expression, value, ScalarScope::TopLevel)
            }
            ScalarExpression::Replace {
                expression,
                search,
                replacement,
            } => self.render_replace_expression(
                expression,
                search,
                replacement,
                ScalarScope::TopLevel,
            ),
            ScalarExpression::Substring {
                expression,
                start,
                length,
            } => self.render_substring_expression(
                expression,
                start,
                length.as_deref(),
                ScalarScope::TopLevel,
            ),
            ScalarExpression::Temporal(temporal) => {
                self.render_temporal_expression(temporal, ScalarScope::TopLevel)
            }
            ScalarExpression::Round { expression, places } => {
                self.render_round_expression(expression, places.as_deref(), ScalarScope::TopLevel)
            }
            ScalarExpression::Arithmetic {
                operator,
                left,
                right,
            } => self.render_arithmetic_expression(*operator, left, right, ScalarScope::TopLevel),
            ScalarExpression::Case {
                alternatives,
                else_expression,
            } => self.render_case_expression(alternatives, else_expression.as_deref()),
            _ => unreachable!("scalar expression handled above"),
        }
    }

    pub(super) fn render_coalesce_expression<'b, 'c>(
        &self,
        expressions: &[ScalarExpression],
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        let rendered = expressions
            .iter()
            .map(|expression| self.render_scalar_in_scope(expression, scope))
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");
        Ok(format!("COALESCE({rendered})"))
    }

    pub(super) fn render_null_if<'b, 'c>(
        &self,
        expression: &ScalarExpression,
        value: &ScalarExpression,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "NULLIF({}, {})",
            self.render_scalar_in_scope(expression, scope)?,
            self.render_scalar_in_scope(value, scope)?
        ))
    }

    fn render_graph_metadata_scalar_expression(
        &self,
        expression: &ScalarExpression,
    ) -> Result<Option<String>, CoreError> {
        match expression {
            ScalarExpression::Key { variable } => self.render_binding_key_ref(variable).map(Some),
            ScalarExpression::GraphKeyList { variables } => {
                self.render_graph_key_list_ref(variables).map(Some)
            }
            ScalarExpression::ElementId { variable } => {
                self.render_binding_element_id_ref(variable).map(Some)
            }
            ScalarExpression::GraphIdentity { variable } => {
                self.render_binding_graph_identity_ref(variable).map(Some)
            }
            ScalarExpression::GraphPresence { variable } => {
                self.render_binding_graph_presence_ref(variable).map(Some)
            }
            ScalarExpression::NodeLabels { variable, label } => {
                self.render_node_labels_ref(variable, label).map(Some)
            }
            ScalarExpression::PropertyKeys { variable } => {
                self.render_property_keys_ref(variable).map(Some)
            }
            ScalarExpression::UndirectedEndpointProperty {
                relationship,
                endpoint,
                property,
            } => self
                .render_undirected_endpoint_property_ref(relationship, *endpoint, property)
                .map(Some),
            ScalarExpression::UndirectedEndpointKey {
                relationship,
                endpoint,
            } => self
                .render_undirected_endpoint_key_ref(relationship, *endpoint)
                .map(Some),
            ScalarExpression::UndirectedEndpointElementId {
                relationship,
                endpoint,
            } => self
                .render_undirected_endpoint_element_id_ref(relationship, *endpoint)
                .map(Some),
            ScalarExpression::UndirectedEndpointLabels {
                relationship,
                label,
                ..
            } => self
                .render_undirected_endpoint_labels_ref(relationship, label)
                .map(Some),
            ScalarExpression::UndirectedEndpointPropertyKeys { relationship, .. } => self
                .render_undirected_endpoint_property_keys_ref(relationship)
                .map(Some),
            ScalarExpression::RelationshipType {
                variable,
                relationship_type,
            } => self
                .render_relationship_type_ref(variable, relationship_type)
                .map(Some),
            _ => Ok(None),
        }
    }

    fn render_simple_scalar_expression(
        &self,
        expression: &ScalarExpression,
    ) -> Result<Option<String>, CoreError> {
        if let Some(rendered) = self.render_scalar_cast_expression(expression)? {
            return Ok(Some(rendered));
        }
        if let Some((function_name, expression, pattern)) =
            Self::string_predicate_function_expression(expression)
        {
            return self
                .render_binary_function_expression(function_name, expression, pattern)
                .map(Some);
        }
        if let Some((function_name, expression)) = Self::unary_sql_function_expression(expression) {
            return self
                .render_unary_function_expression(function_name, expression)
                .map(Some);
        }

        match expression {
            ScalarExpression::Left { expression, count } => self
                .render_binary_function_expression("left", expression, count)
                .map(Some),
            ScalarExpression::Right { expression, count } => self
                .render_binary_function_expression("right", expression, count)
                .map(Some),
            ScalarExpression::StringIndices {
                expression,
                pattern,
            } => self
                .render_binary_function_expression("coral_string_indices", expression, pattern)
                .map(Some),
            ScalarExpression::LPad {
                expression,
                length,
                fill,
            } => self
                .render_ternary_function_expression("lpad", expression, length, fill)
                .map(Some),
            ScalarExpression::RPad {
                expression,
                length,
                fill,
            } => self
                .render_ternary_function_expression("rpad", expression, length, fill)
                .map(Some),
            ScalarExpression::Atan2 { y, x } => self
                .render_binary_function_expression("atan2", y, x)
                .map(Some),
            ScalarExpression::Negate { expression } => Ok(Some(format!(
                "-({})",
                self.render_scalar_expression(expression)?
            ))),
            _ => Ok(None),
        }
    }

    fn unary_sql_function_expression(
        expression: &ScalarExpression,
    ) -> Option<(&'static str, &ScalarExpression)> {
        match expression {
            ScalarExpression::ToLower { expression } => Some(("LOWER", expression)),
            ScalarExpression::ToUpper { expression } => Some(("UPPER", expression)),
            ScalarExpression::Trim { expression } => Some(("TRIM", expression)),
            ScalarExpression::LTrim { expression } => Some(("LTRIM", expression)),
            ScalarExpression::RTrim { expression } => Some(("RTRIM", expression)),
            ScalarExpression::CharacterLength { expression } => {
                Some(("character_length", expression))
            }
            ScalarExpression::Reverse { expression } => Some(("reverse", expression)),
            ScalarExpression::Abs { expression } => Some(("abs", expression)),
            ScalarExpression::Ceil { expression } => Some(("ceil", expression)),
            ScalarExpression::Floor { expression } => Some(("floor", expression)),
            ScalarExpression::Sqrt { expression } => Some(("sqrt", expression)),
            ScalarExpression::Sign { expression } => Some(("signum", expression)),
            ScalarExpression::Exp { expression } => Some(("exp", expression)),
            ScalarExpression::Log { expression } => Some(("ln", expression)),
            ScalarExpression::Log10 { expression } => Some(("log10", expression)),
            ScalarExpression::Sin { expression } => Some(("sin", expression)),
            ScalarExpression::Cos { expression } => Some(("cos", expression)),
            ScalarExpression::Tan { expression } => Some(("tan", expression)),
            ScalarExpression::Cot { expression } => Some(("cot", expression)),
            ScalarExpression::Asin { expression } => Some(("asin", expression)),
            ScalarExpression::Acos { expression } => Some(("acos", expression)),
            ScalarExpression::Atan { expression } => Some(("atan", expression)),
            ScalarExpression::Degrees { expression } => Some(("degrees", expression)),
            ScalarExpression::Radians { expression } => Some(("radians", expression)),
            ScalarExpression::IsNaN { expression } => Some(("isnan", expression)),
            _ => None,
        }
    }

    pub(super) fn string_predicate_function_expression(
        expression: &ScalarExpression,
    ) -> Option<(&'static str, &ScalarExpression, &ScalarExpression)> {
        match expression {
            ScalarExpression::StringContains {
                expression,
                pattern,
            } => Some(("contains", expression, pattern)),
            ScalarExpression::StringStartsWith {
                expression,
                pattern,
            } => Some(("starts_with", expression, pattern)),
            ScalarExpression::StringEndsWith {
                expression,
                pattern,
            } => Some(("ends_with", expression, pattern)),
            _ => None,
        }
    }

    fn render_scalar_cast_expression(
        &self,
        expression: &ScalarExpression,
    ) -> Result<Option<String>, CoreError> {
        match expression {
            ScalarExpression::ToString { expression }
            | ScalarExpression::ToStringOrNull { expression } => {
                if scalar_expression_is_duration(expression) {
                    return self
                        .render_duration_to_iso_expression(expression, ScalarScope::TopLevel)
                        .map(Some);
                }
                self.render_try_cast_expression(expression, "VARCHAR")
                    .map(Some)
            }
            ScalarExpression::ToInteger { expression }
            | ScalarExpression::ToIntegerOrNull { expression } => self
                .render_try_cast_expression(expression, "BIGINT")
                .map(Some),
            ScalarExpression::ToFloat { expression }
            | ScalarExpression::ToFloatOrNull { expression } => self
                .render_try_cast_expression(expression, "DOUBLE")
                .map(Some),
            ScalarExpression::ToBoolean { expression }
            | ScalarExpression::ToBooleanOrNull { expression } => self
                .render_try_cast_expression(expression, "BOOLEAN")
                .map(Some),
            _ => Ok(None),
        }
    }

    fn render_try_cast_expression(
        &self,
        expression: &ScalarExpression,
        target_type: &str,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "TRY_CAST({} AS {target_type})",
            self.render_scalar_expression(expression)?
        ))
    }

    pub(super) fn render_duration_to_iso_expression<'b, 'c>(
        &self,
        expression: &ScalarExpression,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "coral_duration_to_iso({})",
            self.render_scalar_in_scope(expression, scope)?
        ))
    }

    fn render_unary_function_expression(
        &self,
        function_name: &str,
        expression: &ScalarExpression,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "{function_name}({})",
            self.render_scalar_expression(expression)?
        ))
    }

    pub(super) fn render_replace_expression<'b, 'c>(
        &self,
        expression: &ScalarExpression,
        search: &ScalarExpression,
        replacement: &ScalarExpression,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "REPLACE({}, {}, {})",
            self.render_scalar_in_scope(expression, scope)?,
            self.render_scalar_in_scope(search, scope)?,
            self.render_scalar_in_scope(replacement, scope)?
        ))
    }

    fn render_binary_function_expression(
        &self,
        function_name: &str,
        left: &ScalarExpression,
        right: &ScalarExpression,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "{function_name}({}, {})",
            self.render_scalar_expression(left)?,
            self.render_scalar_expression(right)?
        ))
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Temporal SQL rendering is intentionally exhaustive over constructor, component, duration, and accessor variants."
    )]
    pub(super) fn render_temporal_expression<'b, 'c>(
        &self,
        temporal: &TemporalExpr,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        match temporal {
            TemporalExpr::MakeDate { year, month, day } => {
                self.render_make_date_expression(year, month, day, scope)
            }
            TemporalExpr::DateFromString { text } => {
                self.render_date_from_string_expression(text, scope)
            }
            TemporalExpr::MakeLocalDateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                millisecond,
                microsecond,
                nanosecond,
            } => self.render_make_localdatetime_expression(
                year,
                month,
                day,
                hour,
                minute,
                second,
                millisecond,
                microsecond,
                nanosecond,
                scope,
            ),
            TemporalExpr::LocalDateTimeFromString { text } => {
                self.render_localdatetime_from_string_expression(text, scope)
            }
            TemporalExpr::MakeZonedDateTime {
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
            } => self.render_make_zoneddatetime_expression(
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
                scope,
            ),
            TemporalExpr::ZonedDateTimeFromString { text, timezone } => {
                self.render_zoneddatetime_from_string_expression(text, timezone, scope)
            }
            TemporalExpr::MakeLocalTime {
                hour,
                minute,
                second,
                millisecond,
                microsecond,
                nanosecond,
            } => self.render_make_localtime_expression(
                hour,
                minute,
                second,
                millisecond,
                microsecond,
                nanosecond,
                scope,
            ),
            TemporalExpr::LocalTimeFromString { text } => {
                self.render_localtime_from_string_expression(text, scope)
            }
            TemporalExpr::MakeDuration {
                months,
                days,
                seconds,
                nanos,
            } => Ok(render_make_duration_expression(
                *months, *days, *seconds, *nanos,
            )),
            TemporalExpr::DurationInUnits { unit, start, end } => {
                self.render_duration_in_units_expression(*unit, start, end, scope)
            }
            TemporalExpr::Component { expression, unit } => {
                self.render_temporal_component_expression(expression, *unit, scope)
            }
            TemporalExpr::ZonedDateTimeAccessor {
                expression,
                accessor,
                timezone,
            } => self.render_zoneddatetime_accessor_expression(
                expression,
                *accessor,
                timezone.as_deref(),
                scope,
            ),
        }
    }

    pub(super) fn render_make_date_expression<'b, 'c>(
        &self,
        year: &ScalarExpression,
        month: &ScalarExpression,
        day: &ScalarExpression,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "make_date({}, {}, {})",
            self.render_scalar_in_scope(year, scope)?,
            self.render_scalar_in_scope(month, scope)?,
            self.render_scalar_in_scope(day, scope)?
        ))
    }

    pub(super) fn render_date_from_string_expression<'b, 'c>(
        &self,
        text: &ScalarExpression,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "CAST({} AS DATE)",
            self.render_scalar_in_scope(text, scope)?
        ))
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Temporal constructor components mirror openCypher localdatetime fields."
    )]
    pub(super) fn render_make_localdatetime_expression<'b, 'c>(
        &self,
        year: &ScalarExpression,
        month: &ScalarExpression,
        day: &ScalarExpression,
        hour: &ScalarExpression,
        minute: &ScalarExpression,
        second: &ScalarExpression,
        millisecond: &ScalarExpression,
        microsecond: &ScalarExpression,
        nanosecond: &ScalarExpression,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        if let Some(timestamp) = literal_localdatetime(
            year,
            month,
            day,
            hour,
            minute,
            second,
            millisecond,
            microsecond,
            nanosecond,
        ) {
            return Ok(format!(
                "CAST({} AS TIMESTAMP)",
                quote_string_literal(&timestamp)
            ));
        }

        let total_nanoseconds = format!(
            "(({} * 1000000) + ({} * 1000) + {})",
            self.render_scalar_in_scope(millisecond, scope)?,
            self.render_scalar_in_scope(microsecond, scope)?,
            self.render_scalar_in_scope(nanosecond, scope)?
        );
        let timestamp = format!(
            "concat({}, '-', {}, '-', {}, 'T', {}, ':', {}, ':', {}, '.', lpad(CAST({total_nanoseconds} AS VARCHAR), 9, '0'))",
            self.render_zero_padded_component(year, 4, scope)?,
            self.render_zero_padded_component(month, 2, scope)?,
            self.render_zero_padded_component(day, 2, scope)?,
            self.render_zero_padded_component(hour, 2, scope)?,
            self.render_zero_padded_component(minute, 2, scope)?,
            self.render_zero_padded_component(second, 2, scope)?,
        );
        Ok(format!("CAST({timestamp} AS TIMESTAMP)"))
    }

    pub(super) fn render_localdatetime_from_string_expression<'b, 'c>(
        &self,
        text: &ScalarExpression,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "CAST({} AS TIMESTAMP)",
            self.render_scalar_in_scope(text, scope)?
        ))
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Temporal constructor components mirror openCypher datetime fields."
    )]
    pub(super) fn render_make_zoneddatetime_expression<'b, 'c>(
        &self,
        year: &ScalarExpression,
        month: &ScalarExpression,
        day: &ScalarExpression,
        hour: &ScalarExpression,
        minute: &ScalarExpression,
        second: &ScalarExpression,
        millisecond: &ScalarExpression,
        microsecond: &ScalarExpression,
        nanosecond: &ScalarExpression,
        timezone: &str,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        if let Some(timestamp) = literal_localdatetime(
            year,
            month,
            day,
            hour,
            minute,
            second,
            millisecond,
            microsecond,
            nanosecond,
        ) {
            return Ok(render_zoneddatetime_cast_expression(
                &quote_string_literal(&timestamp),
                timezone,
            ));
        }

        let total_nanoseconds = format!(
            "(({} * 1000000) + ({} * 1000) + {})",
            self.render_scalar_in_scope(millisecond, scope)?,
            self.render_scalar_in_scope(microsecond, scope)?,
            self.render_scalar_in_scope(nanosecond, scope)?
        );
        let timestamp = format!(
            "concat({}, '-', {}, '-', {}, 'T', {}, ':', {}, ':', {}, '.', lpad(CAST({total_nanoseconds} AS VARCHAR), 9, '0'))",
            self.render_zero_padded_component(year, 4, scope)?,
            self.render_zero_padded_component(month, 2, scope)?,
            self.render_zero_padded_component(day, 2, scope)?,
            self.render_zero_padded_component(hour, 2, scope)?,
            self.render_zero_padded_component(minute, 2, scope)?,
            self.render_zero_padded_component(second, 2, scope)?,
        );
        Ok(render_zoneddatetime_cast_expression(&timestamp, timezone))
    }

    pub(super) fn render_zoneddatetime_from_string_expression<'b, 'c>(
        &self,
        text: &ScalarExpression,
        timezone: &str,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        Ok(render_zoneddatetime_cast_expression(
            &self.render_scalar_in_scope(text, scope)?,
            timezone,
        ))
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Temporal constructor components mirror openCypher localtime fields."
    )]
    pub(super) fn render_make_localtime_expression<'b, 'c>(
        &self,
        hour: &ScalarExpression,
        minute: &ScalarExpression,
        second: &ScalarExpression,
        millisecond: &ScalarExpression,
        microsecond: &ScalarExpression,
        nanosecond: &ScalarExpression,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        if let Some(time) =
            literal_localtime(hour, minute, second, millisecond, microsecond, nanosecond)
        {
            return Ok(format!("CAST({} AS TIME)", quote_string_literal(&time)));
        }

        let total_nanoseconds = format!(
            "(({} * 1000000) + ({} * 1000) + {})",
            self.render_scalar_in_scope(millisecond, scope)?,
            self.render_scalar_in_scope(microsecond, scope)?,
            self.render_scalar_in_scope(nanosecond, scope)?
        );
        let time = format!(
            "concat({}, ':', {}, ':', {}, '.', lpad(CAST({total_nanoseconds} AS VARCHAR), 9, '0'))",
            self.render_zero_padded_component(hour, 2, scope)?,
            self.render_zero_padded_component(minute, 2, scope)?,
            self.render_zero_padded_component(second, 2, scope)?,
        );
        Ok(format!("CAST({time} AS TIME)"))
    }

    pub(super) fn render_localtime_from_string_expression<'b, 'c>(
        &self,
        text: &ScalarExpression,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "CAST({} AS TIME)",
            self.render_scalar_in_scope(text, scope)?
        ))
    }

    pub(super) fn render_temporal_component_expression<'b, 'c>(
        &self,
        expression: &ScalarExpression,
        unit: TemporalComponentUnit,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        if unit.is_duration_component() {
            return Ok(format!(
                "coral_duration_part({}, '{}')",
                self.render_scalar_in_scope(expression, scope)?,
                unit.component_name()
            ));
        }
        let date_part_sql = format!(
            "CAST(date_part('{}', {}) AS BIGINT)",
            unit.date_part_unit(),
            self.render_scalar_in_scope(expression, scope)?
        );
        match unit {
            TemporalComponentUnit::Millisecond => Ok(format!("({date_part_sql} % 1000)")),
            TemporalComponentUnit::Microsecond => Ok(format!("({date_part_sql} % 1000000)")),
            _ => Ok(date_part_sql),
        }
    }

    pub(super) fn render_zoneddatetime_accessor_expression<'b, 'c>(
        &self,
        expression: &ScalarExpression,
        accessor: ZonedDateTimeAccessor,
        timezone: Option<&str>,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        let timestamp = self.render_scalar_in_scope(expression, scope)?;
        match accessor {
            ZonedDateTimeAccessor::Timezone => {
                let timezone = timezone.ok_or_else(|| {
                    CoreError::internal(
                        "zoned datetime timezone accessor requires a compile-time timezone",
                    )
                })?;
                Ok(quote_string_literal(timezone))
            }
            ZonedDateTimeAccessor::Offset => Ok(render_zoneddatetime_offset_expression(&timestamp)),
            ZonedDateTimeAccessor::OffsetSeconds => Ok(
                render_zoneddatetime_offset_units_expression(&timestamp, 3600, 60),
            ),
            ZonedDateTimeAccessor::OffsetMinutes => Ok(
                render_zoneddatetime_offset_units_expression(&timestamp, 60, 1),
            ),
            ZonedDateTimeAccessor::EpochSeconds => Ok(format!(
                "CAST(trunc(date_part('epoch', {timestamp})) AS BIGINT)"
            )),
            ZonedDateTimeAccessor::EpochMillis => Ok(format!(
                "CAST(trunc(date_part('epoch', {timestamp}) * 1000) AS BIGINT)"
            )),
        }
    }

    pub(super) fn render_duration_in_units_expression<'b, 'c>(
        &self,
        unit: TemporalDurationUnit,
        start: &ScalarExpression,
        end: &ScalarExpression,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        if matches!(
            unit,
            TemporalDurationUnit::Between | TemporalDurationUnit::Months
        ) {
            let function_name = if matches!(unit, TemporalDurationUnit::Between) {
                "coral_duration_between"
            } else {
                "coral_duration_in_months"
            };
            return Ok(format!(
                "{function_name}({}, {})",
                self.render_temporal_duration_between_argument(start, scope)?,
                self.render_temporal_duration_between_argument(end, scope)?
            ));
        }

        let start_timestamp = self.render_temporal_duration_timestamp(start, end, scope)?;
        let end_timestamp = self.render_temporal_duration_timestamp(end, start, scope)?;
        if matches!(unit, TemporalDurationUnit::Days)
            && matches!(
                (temporal_scalar_kind(start), temporal_scalar_kind(end)),
                (Some(TemporalKind::LocalTime), _) | (_, Some(TemporalKind::LocalTime))
            )
        {
            return Ok(render_null_checked_interval(
                &start_timestamp,
                &end_timestamp,
                "CAST('0 months 0 days 0 seconds' AS INTERVAL)",
            ));
        }
        let epoch_diff = format!("date_part('epoch', ({end_timestamp} - {start_timestamp}))");
        let interval = if matches!(unit, TemporalDurationUnit::Seconds) {
            dynamic_seconds_interval(&epoch_diff)
        } else {
            dynamic_days_interval(&format!("trunc({epoch_diff} / 86400)"))
        };
        Ok(render_null_checked_interval(
            &start_timestamp,
            &end_timestamp,
            &interval,
        ))
    }

    fn render_temporal_duration_between_argument<'b, 'c>(
        &self,
        expression: &ScalarExpression,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        let expression_sql = self.render_scalar_in_scope(expression, scope)?;
        match temporal_scalar_kind(expression) {
            Some(TemporalKind::Date | TemporalKind::LocalDateTime | TemporalKind::LocalTime) => {
                Ok(expression_sql)
            }
            Some(TemporalKind::ZonedDateTime | TemporalKind::Duration) | None => {
                Ok(format!("CAST({expression_sql} AS TIMESTAMP)"))
            }
        }
    }

    fn render_temporal_duration_timestamp<'b, 'c>(
        &self,
        expression: &ScalarExpression,
        peer: &ScalarExpression,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        let expression_sql = self.render_scalar_in_scope(expression, scope)?;
        match temporal_scalar_kind(expression) {
            Some(TemporalKind::Date) => Ok(format!("CAST({expression_sql} AS TIMESTAMP)")),
            Some(TemporalKind::LocalDateTime) => Ok(expression_sql),
            Some(TemporalKind::LocalTime) => {
                let anchor = self.render_temporal_duration_anchor_date(peer, scope)?;
                Ok(format!(
                    "CAST(concat(CAST({anchor} AS VARCHAR), 'T', CAST({expression_sql} AS VARCHAR)) AS TIMESTAMP)"
                ))
            }
            Some(TemporalKind::ZonedDateTime | TemporalKind::Duration) | None => {
                Ok(format!("CAST({expression_sql} AS TIMESTAMP)"))
            }
        }
    }

    fn render_temporal_duration_anchor_date<'b, 'c>(
        &self,
        peer: &ScalarExpression,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        let peer_sql = self.render_scalar_in_scope(peer, scope)?;
        match temporal_scalar_kind(peer) {
            Some(TemporalKind::Date) => Ok(peer_sql),
            Some(TemporalKind::LocalDateTime) | None => Ok(format!("CAST({peer_sql} AS DATE)")),
            Some(
                TemporalKind::ZonedDateTime | TemporalKind::LocalTime | TemporalKind::Duration,
            ) => Ok("CAST('1970-01-01' AS DATE)".to_string()),
        }
    }

    fn render_zero_padded_component<'b, 'c>(
        &self,
        expression: &ScalarExpression,
        width: usize,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "lpad(CAST({} AS VARCHAR), {width}, '0')",
            self.render_scalar_in_scope(expression, scope)?
        ))
    }

    fn render_ternary_function_expression(
        &self,
        function_name: &str,
        first: &ScalarExpression,
        second: &ScalarExpression,
        third: &ScalarExpression,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "{function_name}({}, {}, {})",
            self.render_scalar_expression(first)?,
            self.render_scalar_expression(second)?,
            self.render_scalar_expression(third)?
        ))
    }

    pub(super) fn render_round_expression<'b, 'c>(
        &self,
        expression: &ScalarExpression,
        places: Option<&ScalarExpression>,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        let expression_sql = self.render_scalar_in_scope(expression, scope)?;
        let Some(places) = places else {
            return Ok(format!("round({expression_sql})"));
        };
        Ok(format!(
            "round({expression_sql}, {})",
            self.render_scalar_in_scope(places, scope)?
        ))
    }

    pub(super) fn render_arithmetic_expression<'b, 'c>(
        &self,
        operator: ArithmeticOperator,
        left: &ScalarExpression,
        right: &ScalarExpression,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        if let Some(expression) = render_folded_duration_multiply_expression(operator, left, right)?
        {
            return Ok(expression);
        }
        if matches!(operator, ArithmeticOperator::Subtract)
            && scalar_expression_is_zoneddatetime(left)
            && scalar_expression_is_zoneddatetime(right)
        {
            let left_timestamp = self.render_scalar_in_scope(left, scope)?;
            let right_timestamp = self.render_scalar_in_scope(right, scope)?;
            let epoch_diff = format!("date_part('epoch', ({left_timestamp} - {right_timestamp}))");
            return Ok(render_null_checked_interval(
                &left_timestamp,
                &right_timestamp,
                &dynamic_seconds_interval(&epoch_diff),
            ));
        }
        let casts_to_time = matches!(
            (
                operator,
                temporal_scalar_kind(left),
                scalar_expression_is_duration(right)
            ),
            (
                ArithmeticOperator::Add | ArithmeticOperator::Subtract,
                Some(TemporalKind::LocalTime),
                true
            )
        );
        let left = self.render_scalar_in_scope(left, scope)?;
        let right = self.render_scalar_in_scope(right, scope)?;
        let op = match operator {
            ArithmeticOperator::Power => return Ok(format!("power({left}, {right})")),
            ArithmeticOperator::Add => InfixArithmeticOperator::Add,
            ArithmeticOperator::Subtract => InfixArithmeticOperator::Subtract,
            ArithmeticOperator::Multiply => InfixArithmeticOperator::Multiply,
            ArithmeticOperator::Divide => InfixArithmeticOperator::Divide,
            ArithmeticOperator::Modulo => InfixArithmeticOperator::Modulo,
        };
        if casts_to_time {
            let operator = render_arithmetic_operator(op);
            let anchored_time =
                format!("CAST(concat('1970-01-01T', CAST({left} AS VARCHAR)) AS TIMESTAMP)");
            return Ok(format!(
                "CAST(({anchored_time} {operator} {right}) AS TIME)"
            ));
        }
        Ok(format!(
            "({left} {} {right})",
            render_arithmetic_operator(op)
        ))
    }

    pub(super) fn render_substring_expression<'b, 'c>(
        &self,
        expression: &ScalarExpression,
        start: &ScalarExpression,
        length: Option<&ScalarExpression>,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        let mut sql = format!(
            "SUBSTRING({} FROM ({} + 1)",
            self.render_scalar_in_scope(expression, scope)?,
            self.render_scalar_in_scope(start, scope)?
        );
        if let Some(length) = length {
            write!(
                &mut sql,
                " FOR {}",
                self.render_scalar_in_scope(length, scope)?
            )
            .map_err(|error| CoreError::internal(error.to_string()))?;
        }
        sql.push(')');
        Ok(sql)
    }

    fn render_case_expression(
        &self,
        alternatives: &[ScalarCaseAlternative],
        else_expression: Option<&ScalarExpression>,
    ) -> Result<String, CoreError> {
        let mut sql = String::from("CASE");
        for alternative in alternatives {
            write!(
                &mut sql,
                " WHEN {} THEN {}",
                self.render_scalar_predicate_expression(&alternative.when)?,
                self.render_scalar_expression(&alternative.then)?
            )
            .map_err(|error| CoreError::internal(error.to_string()))?;
        }
        if let Some(else_expression) = else_expression {
            write!(
                &mut sql,
                " ELSE {}",
                self.render_scalar_expression(else_expression)?
            )
            .map_err(|error| CoreError::internal(error.to_string()))?;
        }
        sql.push_str(" END");
        Ok(sql)
    }

    fn render_presence_gated_scalar_expression(
        &self,
        presence_variable: &str,
        expression: &ScalarExpression,
    ) -> Result<String, CoreError> {
        let presence = self.render_binding_presence_ref(presence_variable)?;
        let expression = self.render_scalar_expression(expression)?;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE {expression} END"
        ))
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "Temporal constructor components mirror openCypher localdatetime fields."
)]
fn literal_localdatetime(
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
    let total_nanoseconds = literal_integer(millisecond)? * 1_000_000
        + literal_integer(microsecond)? * 1_000
        + literal_integer(nanosecond)?;
    let mut timestamp = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        literal_integer(year)?,
        literal_integer(month)?,
        literal_integer(day)?,
        literal_integer(hour)?,
        literal_integer(minute)?,
        literal_integer(second)?,
    );
    if total_nanoseconds != 0 {
        let mut fractional = format!("{total_nanoseconds:09}");
        while fractional.ends_with('0') {
            fractional.pop();
        }
        timestamp.push('.');
        timestamp.push_str(&fractional);
    }
    Some(timestamp)
}

fn literal_localtime(
    hour: &ScalarExpression,
    minute: &ScalarExpression,
    second: &ScalarExpression,
    millisecond: &ScalarExpression,
    microsecond: &ScalarExpression,
    nanosecond: &ScalarExpression,
) -> Option<String> {
    let total_nanoseconds = literal_integer(millisecond)? * 1_000_000
        + literal_integer(microsecond)? * 1_000
        + literal_integer(nanosecond)?;
    let mut time = format!(
        "{:02}:{:02}:{:02}",
        literal_integer(hour)?,
        literal_integer(minute)?,
        literal_integer(second)?,
    );
    if total_nanoseconds != 0 {
        let mut fractional = format!("{total_nanoseconds:09}");
        while fractional.ends_with('0') {
            fractional.pop();
        }
        time.push('.');
        time.push_str(&fractional);
    }
    Some(time)
}

fn render_make_duration_expression(months: i64, days: i64, seconds: i64, nanos: i64) -> String {
    let seconds = render_duration_seconds(seconds, nanos);
    let interval = format!("{months} months {days} days {seconds} seconds");
    format!("CAST({} AS INTERVAL)", quote_string_literal(&interval))
}

fn render_zoneddatetime_cast_expression(timestamp: &str, timezone: &str) -> String {
    let data_type = format!("Timestamp(ns, Some(\"{timezone}\"))");
    format!(
        "arrow_cast({timestamp}, {})",
        quote_string_literal(&data_type)
    )
}

fn render_zoneddatetime_offset_expression(timestamp: &str) -> String {
    format!("right(TRY_CAST({timestamp} AS VARCHAR), 6)")
}

fn render_zoneddatetime_offset_units_expression(
    timestamp: &str,
    hours_multiplier: i32,
    minutes_multiplier: i32,
) -> String {
    let offset = render_zoneddatetime_offset_expression(timestamp);
    format!(
        "CASE WHEN {offset} IS NULL THEN CAST(NULL AS BIGINT) ELSE ((CASE WHEN left({offset}, 1) = '-' THEN -1 ELSE 1 END) * ((CAST(SUBSTRING({offset} FROM 2 FOR 2) AS BIGINT) * {hours_multiplier}) + (CAST(SUBSTRING({offset} FROM 5 FOR 2) AS BIGINT) * {minutes_multiplier}))) END"
    )
}

fn dynamic_seconds_interval(total_seconds: &str) -> String {
    format!(
        "CAST(concat('0 months 0 days ', coalesce(CAST({total_seconds} AS VARCHAR), '0'), ' seconds') AS INTERVAL)"
    )
}

fn dynamic_days_interval(total_days: &str) -> String {
    format!(
        "CAST(concat('0 months ', coalesce(CAST({total_days} AS VARCHAR), '0'), ' days 0 seconds') AS INTERVAL)"
    )
}

fn render_null_checked_interval(
    start_timestamp: &str,
    end_timestamp: &str,
    interval: &str,
) -> String {
    format!(
        "CASE WHEN {start_timestamp} IS NULL OR {end_timestamp} IS NULL THEN CAST(NULL AS INTERVAL) ELSE {interval} END"
    )
}

fn render_folded_duration_multiply_expression(
    operator: ArithmeticOperator,
    left: &ScalarExpression,
    right: &ScalarExpression,
) -> Result<Option<String>, CoreError> {
    if operator != ArithmeticOperator::Multiply {
        return Ok(None);
    }
    let Some((months, days, seconds, nanos)) = duration_parts(left) else {
        if scalar_expression_is_duration(right) {
            return Err(duration_multiply_error(
                "duration multiplication requires duration * numeric literal",
            ));
        }
        return Ok(None);
    };
    let factor = duration_multiply_factor(right)?;
    let (months, days, seconds, nanos) =
        scale_duration_parts(months, days, seconds, nanos, factor)?;
    Ok(Some(render_make_duration_expression(
        months, days, seconds, nanos,
    )))
}

fn duration_parts(expression: &ScalarExpression) -> Option<(i64, i64, i64, i64)> {
    match expression {
        ScalarExpression::Temporal(TemporalExpr::MakeDuration {
            months,
            days,
            seconds,
            nanos,
        }) => Some((*months, *days, *seconds, *nanos)),
        _ => None,
    }
}

fn duration_multiply_factor(expression: &ScalarExpression) -> Result<i64, CoreError> {
    match expression {
        ScalarExpression::Literal(Literal::Integer(value)) => Ok(*value),
        ScalarExpression::Literal(Literal::Float(value)) => {
            integral_duration_factor(value.into_inner())
        }
        _ => Err(duration_multiply_error(
            "duration multiplication requires a numeric literal factor",
        )),
    }
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    reason = "Duration scaling accepts float literals only after integral and bounds checks."
)]
fn integral_duration_factor(value: f64) -> Result<i64, CoreError> {
    if !value.is_finite() || value.fract() != 0.0 {
        return Err(duration_multiply_error(
            "duration multiplication requires an integral numeric literal factor",
        ));
    }
    if value < i64::MIN as f64 || value > i64::MAX as f64 {
        return Err(duration_multiply_error(
            "duration multiplication factor is out of range",
        ));
    }
    Ok(value as i64)
}

fn scale_duration_parts(
    months: i64,
    days: i64,
    seconds: i64,
    nanos: i64,
    factor: i64,
) -> Result<(i64, i64, i64, i64), CoreError> {
    let months = months
        .checked_mul(factor)
        .ok_or_else(|| duration_multiply_error("duration multiplication result is out of range"))?;
    let days = days
        .checked_mul(factor)
        .ok_or_else(|| duration_multiply_error("duration multiplication result is out of range"))?;
    let total_nanos = i128::from(seconds) * 1_000_000_000 + i128::from(nanos);
    let total_nanos = total_nanos
        .checked_mul(i128::from(factor))
        .ok_or_else(|| duration_multiply_error("duration multiplication result is out of range"))?;
    let seconds = i64::try_from(total_nanos.div_euclid(1_000_000_000)).map_err(|_error| {
        duration_multiply_error("duration multiplication result is out of range")
    })?;
    let nanos = i64::try_from(total_nanos.rem_euclid(1_000_000_000)).map_err(|_error| {
        duration_multiply_error("duration multiplication result is out of range")
    })?;
    Ok((months, days, seconds, nanos))
}

fn duration_multiply_error(message: &'static str) -> CoreError {
    Diagnostic::new(
        diagnostic_codes::INVALID_SCALAR_TYPE,
        "scalar.expression",
        message,
    )
    .into_core_error()
}

fn render_duration_seconds(seconds: i64, nanos: i64) -> String {
    let total_nanos = i128::from(seconds) * 1_000_000_000 + i128::from(nanos);
    let sign = if total_nanos < 0 { "-" } else { "" };
    let absolute = total_nanos.abs();
    let whole_seconds = absolute / 1_000_000_000;
    let fractional_nanos = absolute % 1_000_000_000;
    if fractional_nanos == 0 {
        return format!("{sign}{whole_seconds}");
    }
    let mut fractional = format!("{fractional_nanos:09}");
    while fractional.ends_with('0') {
        fractional.pop();
    }
    format!("{sign}{whole_seconds}.{fractional}")
}

fn scalar_expression_is_duration(expression: &ScalarExpression) -> bool {
    match expression {
        ScalarExpression::Temporal(
            TemporalExpr::MakeDuration { .. } | TemporalExpr::DurationInUnits { .. },
        ) => true,
        ScalarExpression::Arithmetic {
            operator,
            left,
            right,
        } => match operator {
            ArithmeticOperator::Subtract
                if scalar_expression_is_zoneddatetime(left)
                    && scalar_expression_is_zoneddatetime(right) =>
            {
                true
            }
            ArithmeticOperator::Add | ArithmeticOperator::Subtract => {
                scalar_expression_is_duration(left) && scalar_expression_is_duration(right)
            }
            ArithmeticOperator::Multiply => scalar_expression_is_duration(left),
            ArithmeticOperator::Divide | ArithmeticOperator::Modulo | ArithmeticOperator::Power => {
                false
            }
        },
        _ => false,
    }
}

fn temporal_scalar_kind(expression: &ScalarExpression) -> Option<TemporalKind> {
    match expression {
        ScalarExpression::Temporal(
            TemporalExpr::MakeDate { .. } | TemporalExpr::DateFromString { .. },
        ) => Some(TemporalKind::Date),
        ScalarExpression::Temporal(
            TemporalExpr::MakeLocalDateTime { .. } | TemporalExpr::LocalDateTimeFromString { .. },
        ) => Some(TemporalKind::LocalDateTime),
        ScalarExpression::Temporal(
            TemporalExpr::MakeZonedDateTime { .. } | TemporalExpr::ZonedDateTimeFromString { .. },
        ) => Some(TemporalKind::ZonedDateTime),
        ScalarExpression::Temporal(
            TemporalExpr::MakeLocalTime { .. } | TemporalExpr::LocalTimeFromString { .. },
        ) => Some(TemporalKind::LocalTime),
        ScalarExpression::Temporal(
            TemporalExpr::MakeDuration { .. } | TemporalExpr::DurationInUnits { .. },
        ) => Some(TemporalKind::Duration),
        _ => None,
    }
}

fn scalar_expression_is_zoneddatetime(expression: &ScalarExpression) -> bool {
    matches!(
        expression,
        ScalarExpression::Temporal(
            TemporalExpr::MakeZonedDateTime { .. } | TemporalExpr::ZonedDateTimeFromString { .. }
        )
    )
}

fn literal_integer(expression: &ScalarExpression) -> Option<i64> {
    match expression {
        ScalarExpression::Literal(Literal::Integer(value)) => Some(*value),
        _ => None,
    }
}
