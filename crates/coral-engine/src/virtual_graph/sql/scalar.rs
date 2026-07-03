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

    fn render_structural_scalar_expression(
        &self,
        expression: &ScalarExpression,
    ) -> Result<String, CoreError> {
        match expression {
            ScalarExpression::Property(property) => self.render_property_ref(property),
            ScalarExpression::Literal(literal) => Ok(render_literal(literal)),
            ScalarExpression::LiteralList { literals } => Ok(render_literal_list(literals)),
            ScalarExpression::TypedLiteralList {
                literals,
                element_type,
            } => Ok(render_typed_literal_list(literals, *element_type)),
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
                self.render_coalesce_expression(expressions)
            }
            ScalarExpression::NullIf { expression, value } => Ok(format!(
                "NULLIF({}, {})",
                self.render_scalar_expression(expression)?,
                self.render_scalar_expression(value)?
            )),
            ScalarExpression::Replace {
                expression,
                search,
                replacement,
            } => self.render_replace_expression(expression, search, replacement),
            ScalarExpression::Substring {
                expression,
                start,
                length,
            } => self.render_substring_expression(expression, start, length.as_deref()),
            ScalarExpression::Round { expression, places } => {
                self.render_round_expression(expression, places.as_deref())
            }
            ScalarExpression::Arithmetic {
                operator,
                left,
                right,
            } => self.render_arithmetic_expression(*operator, left, right),
            ScalarExpression::Case {
                alternatives,
                else_expression,
            } => self.render_case_expression(alternatives, else_expression.as_deref()),
            _ => unreachable!("scalar expression handled above"),
        }
    }

    fn render_coalesce_expression(
        &self,
        expressions: &[ScalarExpression],
    ) -> Result<String, CoreError> {
        let rendered = expressions
            .iter()
            .map(|expression| self.render_scalar_expression(expression))
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");
        Ok(format!("COALESCE({rendered})"))
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
            ScalarExpression::ToString { expression } => {
                self.render_cast_expression(expression, "VARCHAR").map(Some)
            }
            ScalarExpression::ToInteger { expression } => {
                self.render_cast_expression(expression, "BIGINT").map(Some)
            }
            ScalarExpression::ToFloat { expression } => {
                self.render_cast_expression(expression, "DOUBLE").map(Some)
            }
            ScalarExpression::ToBoolean { expression } => {
                self.render_cast_expression(expression, "BOOLEAN").map(Some)
            }
            ScalarExpression::ToStringOrNull { expression } => self
                .render_try_cast_expression(expression, "VARCHAR")
                .map(Some),
            ScalarExpression::ToIntegerOrNull { expression } => self
                .render_try_cast_expression(expression, "BIGINT")
                .map(Some),
            ScalarExpression::ToFloatOrNull { expression } => self
                .render_try_cast_expression(expression, "DOUBLE")
                .map(Some),
            ScalarExpression::ToBooleanOrNull { expression } => self
                .render_try_cast_expression(expression, "BOOLEAN")
                .map(Some),
            _ => Ok(None),
        }
    }

    fn render_cast_expression(
        &self,
        expression: &ScalarExpression,
        target_type: &str,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "CAST({} AS {target_type})",
            self.render_scalar_expression(expression)?
        ))
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

    fn render_replace_expression(
        &self,
        expression: &ScalarExpression,
        search: &ScalarExpression,
        replacement: &ScalarExpression,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "REPLACE({}, {}, {})",
            self.render_scalar_expression(expression)?,
            self.render_scalar_expression(search)?,
            self.render_scalar_expression(replacement)?
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

    fn render_round_expression(
        &self,
        expression: &ScalarExpression,
        places: Option<&ScalarExpression>,
    ) -> Result<String, CoreError> {
        let expression_sql = self.render_scalar_expression(expression)?;
        let Some(places) = places else {
            return Ok(format!("round({expression_sql})"));
        };
        Ok(format!(
            "round({expression_sql}, {})",
            self.render_scalar_expression(places)?
        ))
    }

    fn render_arithmetic_expression(
        &self,
        operator: ArithmeticOperator,
        left: &ScalarExpression,
        right: &ScalarExpression,
    ) -> Result<String, CoreError> {
        let left = self.render_scalar_expression(left)?;
        let right = self.render_scalar_expression(right)?;
        if operator == ArithmeticOperator::Power {
            return Ok(format!("power({left}, {right})"));
        }
        Ok(format!(
            "({left} {} {right})",
            render_arithmetic_operator(operator)
        ))
    }

    fn render_substring_expression(
        &self,
        expression: &ScalarExpression,
        start: &ScalarExpression,
        length: Option<&ScalarExpression>,
    ) -> Result<String, CoreError> {
        let mut sql = format!(
            "SUBSTRING({} FROM ({} + 1)",
            self.render_scalar_expression(expression)?,
            self.render_scalar_expression(start)?
        );
        if let Some(length) = length {
            write!(&mut sql, " FOR {}", self.render_scalar_expression(length)?)
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
