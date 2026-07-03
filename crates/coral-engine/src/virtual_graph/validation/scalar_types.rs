//! Scalar-type inference for `GraphPlanValidator`: resolves the `ScalarType` of a scalar
//! expression by structural recursion — atomic terms (property refs, literals, keys,
//! undirected endpoints), scalar/string/numeric function calls, and property-ref column
//! lookups. Read-only over the validated binding/plan/catalog context.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Scalar-type validation methods are split into a child module while preserving parent call sites."
)]
use super::*;

#[allow(
    clippy::allow_attributes,
    clippy::elidable_lifetime_names,
    reason = "Keep split validation impl blocks in the same explicit lifetime form as the parent validator impl."
)]
impl<'a> GraphPlanValidator<'a> {
    pub(super) fn validate_scalar_expression(
        &self,
        expression: &ScalarExpression,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        self.infer_scalar_expression_type(expression, path)
            .map(|_| ())
    }

    pub(super) fn infer_scalar_expression_type(
        &self,
        expression: &ScalarExpression,
        path: impl Into<String>,
    ) -> Result<ScalarType, CoreError> {
        let path = path.into();
        match expression {
            ScalarExpression::Property(_)
            | ScalarExpression::UndirectedEndpointProperty { .. }
            | ScalarExpression::UndirectedEndpointKey { .. }
            | ScalarExpression::UndirectedEndpointElementId { .. }
            | ScalarExpression::UndirectedEndpointLabels { .. }
            | ScalarExpression::UndirectedEndpointPropertyKeys { .. }
            | ScalarExpression::Literal(_)
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
            | ScalarExpression::PresenceGated { .. }
            | ScalarExpression::RelationshipType { .. } => {
                self.infer_atomic_scalar_type(expression, &path)
            }
            ScalarExpression::Coalesce { expressions } => {
                self.infer_coalesce_scalar_type(expressions, &path)
            }
            ScalarExpression::CountSubquery {
                pattern,
                distinct_target,
            } => {
                self.validate_count_subquery_pattern(pattern, format!("{path}.pattern"))?;
                if let Some(target) = distinct_target {
                    self.validate_collect_subquery_pattern(
                        pattern,
                        target,
                        format!("{path}.distinct_target"),
                    )?;
                }
                Ok(ScalarType::Integer)
            }
            ScalarExpression::CollectSubquery {
                pattern, target, ..
            } => {
                self.validate_collect_subquery_pattern(pattern, target, format!("{path}.pattern"))?;
                Ok(ScalarType::Other)
            }
            ScalarExpression::NullIf { expression, value } => {
                self.infer_null_if_scalar_type(expression, value, &path)
            }
            ScalarExpression::Case {
                alternatives,
                else_expression,
            } => self.infer_case_scalar_type(alternatives, else_expression.as_deref(), &path),
            ScalarExpression::Temporal(temporal) => {
                self.infer_temporal_scalar_type(temporal, &path)
            }
            _ => self.infer_scalar_function_type(expression, &path),
        }
    }

    pub(super) fn infer_atomic_scalar_type(
        &self,
        expression: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        match expression {
            ScalarExpression::Property(property) => {
                self.validate_property_ref(property, path)?;
                self.property_ref_scalar_type(property)
            }
            ScalarExpression::UndirectedEndpointProperty {
                relationship,
                endpoint,
                property,
            } => self.undirected_endpoint_property_scalar_type(
                relationship,
                *endpoint,
                property,
                path,
            ),
            ScalarExpression::UndirectedEndpointKey { relationship, .. } => {
                self.undirected_endpoint_key_scalar_type(relationship, path)
            }
            ScalarExpression::UndirectedEndpointElementId { relationship, .. } => {
                self.validate_same_label_undirected_endpoint(relationship, path)?;
                Ok(ScalarType::String)
            }
            ScalarExpression::UndirectedEndpointLabels {
                relationship,
                label,
                ..
            } => {
                let (left_node, _) =
                    self.same_label_undirected_endpoint_nodes(relationship, path)?;
                if left_node.label != *label {
                    return Err(CoreError::internal(
                        "validated same-label undirected endpoint labels did not match node label",
                    ));
                }
                Ok(ScalarType::Other)
            }
            ScalarExpression::UndirectedEndpointPropertyKeys { relationship, .. } => {
                self.validate_same_label_undirected_endpoint(relationship, path)?;
                Ok(ScalarType::Other)
            }
            ScalarExpression::Literal(literal) => Ok(literal_scalar_type(literal)),
            ScalarExpression::LiteralList { literals } => {
                Self::validate_literal_list_projection(literals, path)?;
                Ok(ScalarType::Other)
            }
            ScalarExpression::TypedLiteralList {
                literals,
                element_type,
            } => Self::infer_typed_literal_list_scalar_type(literals, *element_type, path),
            ScalarExpression::GraphKeyList { variables } => {
                self.infer_graph_key_list_scalar_type(variables, path)
            }
            ScalarExpression::Predicate(predicate) => {
                self.validate_predicate_expression(predicate, path)?;
                Ok(ScalarType::Boolean)
            }
            ScalarExpression::Key { variable } => {
                self.validate_key_projection(variable, path)?;
                self.key_scalar_type(variable)
            }
            ScalarExpression::ElementId { variable } => {
                self.validate_element_id_projection(variable, path)?;
                Ok(ScalarType::String)
            }
            ScalarExpression::GraphIdentity { variable } => {
                self.validate_graph_identity_projection(variable, path)?;
                Ok(ScalarType::String)
            }
            ScalarExpression::GraphPresence { variable } => {
                self.validate_graph_presence_projection(variable, path)?;
                Ok(ScalarType::String)
            }
            ScalarExpression::NodeLabels { variable, label } => {
                self.validate_node_labels_projection(variable, label, path)?;
                Ok(ScalarType::Other)
            }
            ScalarExpression::PropertyKeys { variable } => {
                self.validate_property_keys_projection(variable, path)?;
                Ok(ScalarType::Other)
            }
            ScalarExpression::PresenceGated {
                presence_variable,
                expression,
            } => {
                self.validate_graph_presence_projection(presence_variable, path)?;
                self.infer_scalar_expression_type(expression, format!("{path}.expression"))
            }
            ScalarExpression::RelationshipType {
                variable,
                relationship_type,
            } => {
                self.validate_relationship_type_projection(variable, relationship_type, path)?;
                Ok(ScalarType::String)
            }
            _ => unreachable!("non-atomic scalar expression reached atomic type inference"),
        }
    }

    fn infer_typed_literal_list_scalar_type(
        literals: &[Literal],
        element_type: LiteralListElementType,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        Self::validate_typed_literal_list(literals, element_type, path)?;
        Ok(ScalarType::Other)
    }

    fn infer_graph_key_list_scalar_type(
        &self,
        variables: &[String],
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        for variable in variables {
            self.validate_key_projection(variable, path)?;
        }
        Ok(ScalarType::Other)
    }

    fn infer_null_if_scalar_type(
        &self,
        expression: &ScalarExpression,
        value: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type =
            self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
        let value_type = self.infer_scalar_expression_type(value, format!("{path}.value"))?;
        Self::validate_compatible_scalar_types(
            expression_type,
            value_type,
            path,
            "nullIf arguments",
        )?;
        Ok(expression_type)
    }

    fn infer_scalar_function_type(
        &self,
        expression: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        if let Some(scalar_type) = self.infer_string_scalar_function_type(expression, path)? {
            return Ok(scalar_type);
        }

        match expression {
            ScalarExpression::ToString { expression }
            | ScalarExpression::ToStringOrNull { expression } => {
                self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
                Ok(ScalarType::String)
            }
            ScalarExpression::ToInteger { expression }
            | ScalarExpression::ToIntegerOrNull { expression } => {
                self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
                Ok(ScalarType::Integer)
            }
            ScalarExpression::ToFloat { expression }
            | ScalarExpression::ToFloatOrNull { expression } => {
                self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
                Ok(ScalarType::Float)
            }
            ScalarExpression::ToBoolean { expression }
            | ScalarExpression::ToBooleanOrNull { expression } => {
                self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
                Ok(ScalarType::Boolean)
            }
            ScalarExpression::IsNaN { expression } => {
                self.infer_is_nan_scalar_type(expression, path)
            }
            ScalarExpression::Abs { expression }
            | ScalarExpression::Ceil { expression }
            | ScalarExpression::Floor { expression }
            | ScalarExpression::Sqrt { expression }
            | ScalarExpression::Sign { expression }
            | ScalarExpression::Exp { expression }
            | ScalarExpression::Log { expression }
            | ScalarExpression::Log10 { expression }
            | ScalarExpression::Sin { expression }
            | ScalarExpression::Cos { expression }
            | ScalarExpression::Tan { expression }
            | ScalarExpression::Cot { expression }
            | ScalarExpression::Asin { expression }
            | ScalarExpression::Acos { expression }
            | ScalarExpression::Atan { expression }
            | ScalarExpression::Degrees { expression }
            | ScalarExpression::Radians { expression }
            | ScalarExpression::Negate { expression } => {
                self.infer_numeric_unary_scalar_type(expression, path)
            }
            ScalarExpression::Round { expression, places } => {
                self.infer_round_scalar_type(expression, places.as_deref(), path)
            }
            ScalarExpression::Arithmetic { left, right, .. } => {
                self.infer_arithmetic_scalar_type(left, right, path)
            }
            ScalarExpression::Atan2 { y, x } => self.infer_atan2_scalar_type(y, x, path),
            _ => unreachable!("non-function scalar expression reached function type inference"),
        }
    }

    fn infer_temporal_scalar_type(
        &self,
        expression: &TemporalExpr,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        match expression {
            TemporalExpr::MakeDate { year, month, day } => {
                for (name, expression) in [("year", year), ("month", month), ("day", day)] {
                    let expression_type =
                        self.infer_scalar_expression_type(expression, format!("{path}.{name}"))?;
                    Self::require_integer_compatible_type(
                        expression_type,
                        format!("{path}.{name}"),
                        "date constructor field",
                    )?;
                }
                Ok(ScalarType::Temporal(TemporalKind::Date))
            }
        }
    }

    fn infer_string_scalar_function_type(
        &self,
        expression: &ScalarExpression,
        path: &str,
    ) -> Result<Option<ScalarType>, CoreError> {
        match expression {
            ScalarExpression::ToLower { expression }
            | ScalarExpression::ToUpper { expression }
            | ScalarExpression::Trim { expression }
            | ScalarExpression::LTrim { expression }
            | ScalarExpression::RTrim { expression }
            | ScalarExpression::Reverse { expression } => self
                .infer_string_unary_scalar_type(expression, path)
                .map(Some),
            ScalarExpression::CharacterLength { expression } => {
                let expression_type =
                    self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
                Self::require_string_compatible_type(
                    expression_type,
                    format!("{path}.expression"),
                    "character length",
                )?;
                Ok(Some(ScalarType::Integer))
            }
            ScalarExpression::Left { expression, count }
            | ScalarExpression::Right { expression, count } => self
                .infer_sized_string_scalar_type(expression, count, path)
                .map(Some),
            ScalarExpression::StringIndices {
                expression,
                pattern,
            } => self
                .infer_string_indices_scalar_type(expression, pattern, path)
                .map(Some),
            ScalarExpression::LPad {
                expression,
                length,
                fill,
            }
            | ScalarExpression::RPad {
                expression,
                length,
                fill,
            } => self
                .infer_padding_scalar_type(expression, length, fill, path)
                .map(Some),
            ScalarExpression::StringContains {
                expression,
                pattern: operand,
            }
            | ScalarExpression::StringStartsWith {
                expression,
                pattern: operand,
            }
            | ScalarExpression::StringEndsWith {
                expression,
                pattern: operand,
            } => self
                .infer_string_predicate_function_scalar_type(expression, operand, path)
                .map(Some),
            ScalarExpression::Replace {
                expression,
                search,
                replacement,
            } => self
                .infer_replace_scalar_type(expression, search, replacement, path)
                .map(Some),
            ScalarExpression::Substring {
                expression,
                start,
                length,
            } => self
                .infer_substring_scalar_type(expression, start, length.as_deref(), path)
                .map(Some),
            _ => Ok(None),
        }
    }

    fn infer_case_scalar_type(
        &self,
        alternatives: &[ScalarCaseAlternative],
        else_expression: Option<&ScalarExpression>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        if alternatives.is_empty() {
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_SCALAR_EXPRESSION,
                path,
                "CASE expressions require at least one WHEN/THEN alternative",
            )
            .into_core_error());
        }
        let mut result_type = ScalarType::Null;
        for (index, alternative) in alternatives.iter().enumerate() {
            self.validate_predicate_expression(
                &alternative.when,
                format!("{path}.alternatives[{index}].when"),
            )?;
            let then_type = self.infer_scalar_expression_type(
                &alternative.then,
                format!("{path}.alternatives[{index}].then"),
            )?;
            result_type = Self::merge_scalar_types(
                result_type,
                then_type,
                format!("{path}.alternatives[{index}].then"),
                "CASE result branches",
            )?;
        }
        if let Some(else_expression) = else_expression {
            let else_type =
                self.infer_scalar_expression_type(else_expression, format!("{path}.else"))?;
            result_type = Self::merge_scalar_types(
                result_type,
                else_type,
                format!("{path}.else"),
                "CASE result branches",
            )?;
        }
        Ok(result_type)
    }

    fn infer_coalesce_scalar_type(
        &self,
        expressions: &[ScalarExpression],
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        if expressions.len() < 2 {
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_SCALAR_EXPRESSION,
                path,
                "coalesce expressions require at least two arguments",
            )
            .into_core_error());
        }

        let mut result_type = ScalarType::Null;
        for (index, expression) in expressions.iter().enumerate() {
            let expression_type =
                self.infer_scalar_expression_type(expression, format!("{path}[{index}]"))?;
            result_type = Self::merge_scalar_types(
                result_type,
                expression_type,
                format!("{path}[{index}]"),
                "coalesce arguments",
            )?;
        }
        Ok(result_type)
    }

    fn infer_string_unary_scalar_type(
        &self,
        expression: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type =
            self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
        Self::require_string_compatible_type(
            expression_type,
            format!("{path}.expression"),
            "string function",
        )?;
        Ok(ScalarType::String)
    }

    fn infer_numeric_unary_scalar_type(
        &self,
        expression: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type =
            self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
        Self::require_numeric_compatible_type(
            expression_type,
            format!("{path}.expression"),
            "numeric function",
        )?;
        numeric_result_type(expression_type, path, "numeric function")
    }

    fn infer_is_nan_scalar_type(
        &self,
        expression: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type =
            self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
        Self::require_numeric_compatible_type(
            expression_type,
            format!("{path}.expression"),
            "isNaN",
        )?;
        Ok(ScalarType::Boolean)
    }

    fn infer_round_scalar_type(
        &self,
        expression: &ScalarExpression,
        places: Option<&ScalarExpression>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type =
            self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
        Self::require_numeric_compatible_type(
            expression_type,
            format!("{path}.expression"),
            "round",
        )?;
        if let Some(places) = places {
            let places_type =
                self.infer_scalar_expression_type(places, format!("{path}.places"))?;
            Self::require_integer_compatible_type(
                places_type,
                format!("{path}.places"),
                "round precision",
            )?;
        }
        numeric_result_type(expression_type, path, "round")
    }

    fn infer_sized_string_scalar_type(
        &self,
        expression: &ScalarExpression,
        count: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type =
            self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
        Self::require_string_compatible_type(
            expression_type,
            format!("{path}.expression"),
            "sized string function",
        )?;
        let count_type = self.infer_scalar_expression_type(count, format!("{path}.count"))?;
        Self::require_integer_compatible_type(
            count_type,
            format!("{path}.count"),
            "sized string count",
        )?;
        Ok(ScalarType::String)
    }

    fn infer_string_indices_scalar_type(
        &self,
        expression: &ScalarExpression,
        pattern: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        for (name, expression) in [("expression", expression), ("pattern", pattern)] {
            let expression_type =
                self.infer_scalar_expression_type(expression, format!("{path}.{name}"))?;
            Self::require_string_compatible_type(
                expression_type,
                format!("{path}.{name}"),
                "indices",
            )?;
        }
        Ok(ScalarType::Other)
    }

    fn infer_padding_scalar_type(
        &self,
        expression: &ScalarExpression,
        length: &ScalarExpression,
        fill: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        for (name, expression) in [("expression", expression), ("fill", fill)] {
            let expression_type =
                self.infer_scalar_expression_type(expression, format!("{path}.{name}"))?;
            Self::require_string_compatible_type(
                expression_type,
                format!("{path}.{name}"),
                "padding string function",
            )?;
        }
        let length_type = self.infer_scalar_expression_type(length, format!("{path}.length"))?;
        Self::require_integer_compatible_type(
            length_type,
            format!("{path}.length"),
            "padding length",
        )?;
        Ok(ScalarType::String)
    }

    fn infer_replace_scalar_type(
        &self,
        expression: &ScalarExpression,
        search: &ScalarExpression,
        replacement: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        for (name, expression) in [
            ("expression", expression),
            ("search", search),
            ("replacement", replacement),
        ] {
            let expression_type =
                self.infer_scalar_expression_type(expression, format!("{path}.{name}"))?;
            Self::require_string_compatible_type(
                expression_type,
                format!("{path}.{name}"),
                "replace",
            )?;
        }
        Ok(ScalarType::String)
    }

    fn infer_string_predicate_function_scalar_type(
        &self,
        expression: &ScalarExpression,
        pattern: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        for (name, expression) in [("expression", expression), ("pattern", pattern)] {
            let expression_type =
                self.infer_scalar_expression_type(expression, format!("{path}.{name}"))?;
            Self::require_string_compatible_type(
                expression_type,
                format!("{path}.{name}"),
                "string predicate function",
            )?;
        }
        Ok(ScalarType::Boolean)
    }

    fn infer_substring_scalar_type(
        &self,
        expression: &ScalarExpression,
        start: &ScalarExpression,
        length: Option<&ScalarExpression>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type =
            self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
        Self::require_string_compatible_type(
            expression_type,
            format!("{path}.expression"),
            "substring",
        )?;
        let start_type = self.infer_scalar_expression_type(start, format!("{path}.start"))?;
        Self::require_integer_compatible_type(
            start_type,
            format!("{path}.start"),
            "substring start",
        )?;
        if let Some(length) = length {
            let length_type =
                self.infer_scalar_expression_type(length, format!("{path}.length"))?;
            Self::require_integer_compatible_type(
                length_type,
                format!("{path}.length"),
                "substring length",
            )?;
        }
        Ok(ScalarType::String)
    }

    fn infer_atan2_scalar_type(
        &self,
        y: &ScalarExpression,
        x: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let y_type = self.infer_scalar_expression_type(y, format!("{path}.y"))?;
        let x_type = self.infer_scalar_expression_type(x, format!("{path}.x"))?;
        Self::require_numeric_compatible_type(y_type, format!("{path}.y"), "atan2")?;
        Self::require_numeric_compatible_type(x_type, format!("{path}.x"), "atan2")?;
        Ok(ScalarType::Float)
    }

    fn infer_arithmetic_scalar_type(
        &self,
        left: &ScalarExpression,
        right: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let left_type = self.infer_scalar_expression_type(left, format!("{path}.left"))?;
        let right_type = self.infer_scalar_expression_type(right, format!("{path}.right"))?;
        Self::require_numeric_compatible_type(left_type, format!("{path}.left"), "arithmetic")?;
        Self::require_numeric_compatible_type(right_type, format!("{path}.right"), "arithmetic")?;
        numeric_binary_result_type(left_type, right_type, path, "arithmetic")
    }

    pub(super) fn property_ref_scalar_type(
        &self,
        property: &PropertyRef,
    ) -> Result<ScalarType, CoreError> {
        let binding = self
            .bindings
            .get(property.variable.as_str())
            .ok_or_else(|| {
                Diagnostic::new(
                    diagnostic_codes::UNKNOWN_VARIABLE,
                    "property.variable",
                    format!("unknown graph variable '{}'", property.variable),
                )
                .into_core_error()
            })?;
        let Some(column) = binding.column_for_property(&property.property) else {
            return Ok(ScalarType::Unknown);
        };
        let table = match binding.kind() {
            ValidatedBindingKind::Node(node) => &node.table,
            ValidatedBindingKind::Relationship(relationship) => &relationship.table,
        };
        Ok(self.column_scalar_type(table, column))
    }

    fn same_label_undirected_endpoint_nodes(
        &self,
        relationship_variable: &str,
        path: &str,
    ) -> Result<(&Node, &Node), CoreError> {
        let (relationship_index, relationship_pattern) = self
            .plan
            .relationships
            .iter()
            .enumerate()
            .find(|(_, relationship)| {
                relationship.variable.as_deref() == Some(relationship_variable)
            })
            .ok_or_else(|| {
                Diagnostic::new(
                    diagnostic_codes::UNKNOWN_VARIABLE,
                    path,
                    format!("unknown relationship variable '{relationship_variable}'"),
                )
                .into_core_error()
            })?;
        if relationship_pattern.direction != Direction::Undirected {
            return Err(CoreError::internal(
                "undirected endpoint scalar referenced a directed relationship",
            ));
        }
        let relationship = self
            .relationship_mappings
            .get(relationship_index)
            .ok_or_else(|| {
                CoreError::internal(
                    "validated relationship mapping was missing for undirected endpoint scalar",
                )
            })?;
        let left_node =
            self.node_binding_for_path(&relationship_pattern.left, format!("{path}.left"))?;
        let right_node =
            self.node_binding_for_path(&relationship_pattern.right, format!("{path}.right"))?;
        if left_node.label != right_node.label {
            return Err(CoreError::internal(
                "undirected endpoint scalar referenced a cross-label relationship",
            ));
        }
        if relationship.from.label != left_node.label || relationship.to.label != right_node.label {
            return Err(CoreError::internal(
                "validated same-label undirected relationship mapping did not match endpoint labels",
            ));
        }
        Ok((left_node, right_node))
    }

    fn validate_same_label_undirected_endpoint(
        &self,
        relationship_variable: &str,
        path: &str,
    ) -> Result<(), CoreError> {
        self.same_label_undirected_endpoint_nodes(relationship_variable, path)
            .map(|_| ())
    }

    fn undirected_endpoint_key_scalar_type(
        &self,
        relationship_variable: &str,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let (left_node, _) =
            self.same_label_undirected_endpoint_nodes(relationship_variable, path)?;
        Ok(self.column_scalar_type(&left_node.table, &left_node.key))
    }

    fn undirected_endpoint_property_scalar_type(
        &self,
        relationship_variable: &str,
        endpoint: UndirectedRelationshipEndpoint,
        property: &str,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let (left_node, right_node) =
            self.same_label_undirected_endpoint_nodes(relationship_variable, path)?;
        let Some(left_column) = left_node.column_for_property(property) else {
            let function = match endpoint {
                UndirectedRelationshipEndpoint::Start => "startNode",
                UndirectedRelationshipEndpoint::End => "endNode",
            };
            return Err(Diagnostic::new(
                diagnostic_codes::UNKNOWN_PROPERTY,
                path,
                format!(
                    "{function}({relationship_variable}) does not expose property '{property}'"
                ),
            )
            .into_core_error());
        };
        if right_node.column_for_property(property).is_none() {
            return Err(CoreError::internal(
                "same-label undirected relationship endpoints exposed different property sets",
            ));
        }
        Ok(self.column_scalar_type(&left_node.table, left_column))
    }
}
