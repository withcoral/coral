//! Predicate validation for `WHERE` filters and `HAVING` (post-projection) predicates:
//! property, scalar, key, element-id, presence and property-key-membership comparison
//! leaves, their boolean-tree walkers, and shared literal/string operand checks.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Predicate validation methods are split into a child module while preserving parent-private access."
)]
use super::*;

#[allow(
    clippy::allow_attributes,
    clippy::elidable_lifetime_names,
    reason = "Keep the split validation impl shape aligned with the parent GraphPlanValidator impl."
)]
impl<'a> GraphPlanValidator<'a> {
    pub(super) fn validate_predicate(
        &self,
        index: usize,
        predicate: &PropertyPredicate,
    ) -> Result<(), CoreError> {
        self.validate_property_predicate(predicate, format!("predicates[{index}]"))
    }
    pub(super) fn validate_predicate_expression(
        &self,
        expression: &PredicateExpression,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        match expression {
            PredicateExpression::Boolean(_) => Ok(()),
            PredicateExpression::Comparison(predicate) => {
                self.validate_property_predicate(predicate, path)
            }
            PredicateExpression::KeyComparison(predicate) => {
                self.validate_key_predicate(predicate, path)
            }
            PredicateExpression::ElementIdComparison(predicate) => {
                self.validate_element_id_predicate(predicate, path)
            }
            PredicateExpression::Presence(predicate) => {
                self.validate_presence_predicate(predicate, path)
            }
            PredicateExpression::PropertyKeyMembership(predicate) => {
                self.validate_property_key_membership_predicate(predicate, path)
            }
            PredicateExpression::ExistsPattern(predicate) => {
                self.validate_exists_pattern_predicate(predicate, path)
            }
            PredicateExpression::ScalarComparison(predicate) => {
                self.validate_scalar_predicate(predicate, path)
            }
            PredicateExpression::And { left, right }
            | PredicateExpression::Or { left, right }
            | PredicateExpression::Xor { left, right } => {
                self.validate_predicate_expression(left, format!("{path}.left"))?;
                self.validate_predicate_expression(right, format!("{path}.right"))
            }
            PredicateExpression::Not { expression } => {
                self.validate_predicate_expression(expression, format!("{path}.expression"))
            }
        }
    }
    pub(super) fn validate_projection_predicate_expression(
        &self,
        expression: &ProjectionPredicateExpression,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        match expression {
            ProjectionPredicateExpression::Boolean(_) => Ok(()),
            ProjectionPredicateExpression::Comparison(predicate) => {
                self.validate_projection_predicate(predicate, path)
            }
            ProjectionPredicateExpression::And { left, right }
            | ProjectionPredicateExpression::Or { left, right }
            | ProjectionPredicateExpression::Xor { left, right } => {
                self.validate_projection_predicate_expression(left, format!("{path}.left"))?;
                self.validate_projection_predicate_expression(right, format!("{path}.right"))
            }
            ProjectionPredicateExpression::Not { expression } => self
                .validate_projection_predicate_expression(expression, format!("{path}.expression")),
        }
    }
    fn validate_property_predicate(
        &self,
        predicate: &PropertyPredicate,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        self.validate_property_ref(&predicate.property, format!("{path}.property"))?;
        match &predicate.rhs {
            PredicateRhs::Literal(literal) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                Self::validate_string_predicate(path.clone(), predicate.operator, literal)?;
                Self::validate_literal_predicate(path.clone(), predicate.operator, literal)
            }
            PredicateRhs::TemporalCoercion { source } => {
                Self::validate_temporal_coercion_literal_predicate(
                    &path,
                    predicate.operator,
                    source,
                )
            }
            PredicateRhs::TemporalCoercionList(_) => {
                Self::validate_temporal_coercion_list_literal_predicate(&path, predicate.operator)
            }
            PredicateRhs::Property(property) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                Self::validate_non_literal_string_predicate_operand(
                    path.clone(),
                    predicate.operator,
                )?;
                self.validate_property_ref(property, format!("{path}.rhs"))
            }
            PredicateRhs::Key { variable } => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                Self::validate_non_literal_string_predicate_operand(
                    path.clone(),
                    predicate.operator,
                )?;
                self.validate_key_projection(variable, format!("{path}.rhs"))
            }
            PredicateRhs::ElementId { variable } => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                Self::validate_non_literal_string_predicate_operand(
                    path.clone(),
                    predicate.operator,
                )?;
                self.validate_element_id_projection(variable, format!("{path}.rhs"))
            }
            PredicateRhs::List(_) => {
                if predicate.operator != ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "literal lists are only supported with IN predicates",
                    )
                    .into_core_error());
                }
                Ok(())
            }
        }?;
        let lhs_type = self.property_ref_scalar_type(&predicate.property)?;
        self.validate_predicate_rhs_operand_types(
            predicate.operator,
            lhs_type,
            &predicate.rhs,
            &path,
        )
    }
    fn validate_scalar_predicate(
        &self,
        predicate: &ScalarPredicate,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let lhs_type = self.infer_scalar_expression_type(&predicate.lhs, format!("{path}.lhs"))?;
        match &predicate.rhs {
            ScalarPredicateRhs::Expression(expression) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                let rhs_type =
                    self.infer_scalar_expression_type(expression, format!("{path}.rhs"))?;
                Self::validate_scalar_predicate_operand_types(
                    predicate.operator,
                    lhs_type,
                    rhs_type,
                    &path,
                )
            }
            ScalarPredicateRhs::List(literals) => {
                if predicate.operator != ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "literal lists are only supported with IN predicates",
                    )
                    .into_core_error());
                }
                Self::validate_scalar_in_list_operand_types(lhs_type, literals, &path)
            }
        }
    }
    fn validate_key_predicate(
        &self,
        predicate: &KeyPredicate,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        self.validate_key_projection(&predicate.variable, format!("{path}.variable"))?;
        match &predicate.rhs {
            PredicateRhs::Literal(literal) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                Self::validate_string_predicate(path.clone(), predicate.operator, literal)?;
                Self::validate_literal_predicate(path.clone(), predicate.operator, literal)
            }
            PredicateRhs::TemporalCoercion { source } => {
                Self::validate_temporal_coercion_literal_predicate(
                    &path,
                    predicate.operator,
                    source,
                )
            }
            PredicateRhs::TemporalCoercionList(_) => {
                Self::validate_temporal_coercion_list_literal_predicate(&path, predicate.operator)
            }
            PredicateRhs::Property(property) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                if matches!(
                    predicate.operator,
                    ComparisonOperator::StartsWith
                        | ComparisonOperator::EndsWith
                        | ComparisonOperator::Contains
                        | ComparisonOperator::RegexMatch
                ) {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "string predicates require a string literal right-hand side",
                    )
                    .into_core_error());
                }
                self.validate_property_ref(property, format!("{path}.rhs"))
            }
            PredicateRhs::Key { variable } => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                if matches!(
                    predicate.operator,
                    ComparisonOperator::StartsWith
                        | ComparisonOperator::EndsWith
                        | ComparisonOperator::Contains
                        | ComparisonOperator::RegexMatch
                ) {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "string predicates require a string literal right-hand side",
                    )
                    .into_core_error());
                }
                self.validate_key_projection(variable, format!("{path}.rhs"))
            }
            PredicateRhs::ElementId { .. } => Err(Diagnostic::new(
                diagnostic_codes::INVALID_PREDICATE_OPERAND,
                path.clone(),
                "id() predicates cannot compare against elementId(); compare id() to mapped keys or elementId() to string values",
            )
            .into_core_error()),
            PredicateRhs::List(_) => {
                if predicate.operator != ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "literal lists are only supported with IN predicates",
                    )
                    .into_core_error());
                }
                Ok(())
            }
        }?;
        let lhs_type = self.key_scalar_type(&predicate.variable)?;
        self.validate_predicate_rhs_operand_types(
            predicate.operator,
            lhs_type,
            &predicate.rhs,
            &path,
        )
    }
    fn validate_element_id_predicate(
        &self,
        predicate: &ElementIdPredicate,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        self.validate_element_id_projection(&predicate.variable, format!("{path}.variable"))?;
        match &predicate.rhs {
            PredicateRhs::Literal(literal) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                Self::validate_element_id_literal(path.clone(), literal)?;
                Self::validate_string_predicate(path.clone(), predicate.operator, literal)?;
                Self::validate_literal_predicate(path.clone(), predicate.operator, literal)
            }
            PredicateRhs::TemporalCoercion { source } => {
                Self::validate_element_id_temporal_coercion_predicate(
                    &path,
                    predicate.operator,
                    source,
                )
            }
            PredicateRhs::TemporalCoercionList(sources) => {
                Self::validate_element_id_temporal_coercion_list_predicate(
                    &path,
                    predicate.operator,
                    sources,
                )
            }
            PredicateRhs::ElementId { variable } => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                if matches!(
                    predicate.operator,
                    ComparisonOperator::StartsWith
                        | ComparisonOperator::EndsWith
                        | ComparisonOperator::Contains
                        | ComparisonOperator::RegexMatch
                ) {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "string predicates require a string literal right-hand side",
                    )
                    .into_core_error());
                }
                self.validate_element_id_projection(variable, format!("{path}.rhs"))
            }
            PredicateRhs::List(literals) => {
                Self::validate_element_id_literal_list_predicate(
                    &path,
                    predicate.operator,
                    literals,
                )
            }
            PredicateRhs::Property(property) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                if matches!(
                    predicate.operator,
                    ComparisonOperator::StartsWith
                        | ComparisonOperator::EndsWith
                        | ComparisonOperator::Contains
                        | ComparisonOperator::RegexMatch
                ) {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "string predicates require a string literal right-hand side",
                    )
                    .into_core_error());
                }
                self.validate_property_ref(property, format!("{path}.rhs"))
            }
            PredicateRhs::Key { .. } => Err(Diagnostic::new(
                diagnostic_codes::INVALID_PREDICATE_OPERAND,
                path.clone(),
                "elementId() predicates cannot compare against id(); compare elementId() to string values or id() to mapped keys",
            )
            .into_core_error()),
        }?;
        self.validate_predicate_rhs_operand_types(
            predicate.operator,
            ScalarType::String,
            &predicate.rhs,
            &path,
        )
    }
    fn validate_element_id_literal(
        path: impl Into<String>,
        literal: &Literal,
    ) -> Result<(), CoreError> {
        match literal {
            Literal::String(_) | Literal::Null => Ok(()),
            Literal::Integer(_) | Literal::Float(_) | Literal::Boolean(_) | Literal::List(_) => {
                Err(Diagnostic::new(
                    diagnostic_codes::INVALID_PREDICATE_OPERAND,
                    path,
                    "elementId() predicates require string or null literal operands",
                )
                .into_core_error())
            }
        }
    }

    fn validate_temporal_coercion_literal_predicate(
        path: &str,
        operator: ComparisonOperator,
        source: &str,
    ) -> Result<(), CoreError> {
        if operator == ComparisonOperator::In {
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_PREDICATE_OPERAND,
                path,
                "IN predicates require a literal list right-hand side",
            )
            .into_core_error());
        }
        let literal = Literal::String(source.to_string());
        Self::validate_string_predicate(path, operator, &literal)?;
        Self::validate_literal_predicate(path, operator, &literal)
    }

    fn validate_temporal_coercion_list_literal_predicate(
        path: &str,
        operator: ComparisonOperator,
    ) -> Result<(), CoreError> {
        if operator != ComparisonOperator::In {
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_PREDICATE_OPERAND,
                path,
                "literal lists are only supported with IN predicates",
            )
            .into_core_error());
        }
        Ok(())
    }

    fn validate_element_id_temporal_coercion_predicate(
        path: &str,
        operator: ComparisonOperator,
        source: &str,
    ) -> Result<(), CoreError> {
        if operator == ComparisonOperator::In {
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_PREDICATE_OPERAND,
                path,
                "IN predicates require a literal list right-hand side",
            )
            .into_core_error());
        }
        let literal = Literal::String(source.to_string());
        Self::validate_element_id_literal(path, &literal)?;
        Self::validate_string_predicate(path, operator, &literal)?;
        Self::validate_literal_predicate(path, operator, &literal)
    }

    fn validate_element_id_temporal_coercion_list_predicate(
        path: &str,
        operator: ComparisonOperator,
        sources: &[String],
    ) -> Result<(), CoreError> {
        Self::validate_temporal_coercion_list_literal_predicate(path, operator)?;
        for (index, source) in sources.iter().enumerate() {
            Self::validate_element_id_literal(
                format!("{path}.rhs[{index}]"),
                &Literal::String(source.clone()),
            )?;
        }
        Ok(())
    }

    fn validate_element_id_literal_list_predicate(
        path: &str,
        operator: ComparisonOperator,
        literals: &[Literal],
    ) -> Result<(), CoreError> {
        Self::validate_temporal_coercion_list_literal_predicate(path, operator)?;
        for (index, literal) in literals.iter().enumerate() {
            Self::validate_element_id_literal(format!("{path}.rhs[{index}]"), literal)?;
        }
        Ok(())
    }

    fn validate_presence_predicate(
        &self,
        predicate: &PresencePredicate,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        validate_variable(format!("{path}.variable"), &predicate.variable)?;
        if !self.bindings.contains_key(predicate.variable.as_str()) {
            return Err(Diagnostic::new(
                diagnostic_codes::UNKNOWN_VARIABLE,
                format!("{path}.variable"),
                format!("unknown graph variable '{}'", predicate.variable),
            )
            .into_core_error());
        }
        match predicate.operator {
            ComparisonOperator::Equal | ComparisonOperator::NotEqual => Ok(()),
            ComparisonOperator::GreaterThan
            | ComparisonOperator::GreaterThanOrEqual
            | ComparisonOperator::LessThan
            | ComparisonOperator::LessThanOrEqual
            | ComparisonOperator::In
            | ComparisonOperator::StartsWith
            | ComparisonOperator::EndsWith
            | ComparisonOperator::Contains
            | ComparisonOperator::RegexMatch => Err(Diagnostic::new(
                diagnostic_codes::INVALID_PRESENCE_PREDICATE,
                path,
                "graph variable presence predicates only support IS NULL and IS NOT NULL",
            )
            .into_core_error()),
        }
    }
    fn validate_property_key_membership_predicate(
        &self,
        predicate: &PropertyKeyMembershipPredicate,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        if !self.bindings.contains_key(predicate.variable.as_str()) {
            return Err(Diagnostic::new(
                diagnostic_codes::UNKNOWN_VARIABLE,
                format!("{path}.variable"),
                format!("unknown graph variable '{}'", predicate.variable),
            )
            .into_core_error());
        }
        if let Some(presence_variable) = &predicate.presence_variable
            && !self.bindings.contains_key(presence_variable.as_str())
        {
            return Err(Diagnostic::new(
                diagnostic_codes::UNKNOWN_VARIABLE,
                format!("{path}.presence_variable"),
                format!("unknown graph variable '{presence_variable}'"),
            )
            .into_core_error());
        }
        Ok(())
    }
    fn validate_projection_predicate(
        &self,
        predicate: &ProjectionPredicate,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let lhs_type =
            self.projection_alias_scalar_type(&predicate.alias, format!("{path}.alias"))?;
        match &predicate.rhs {
            ProjectionPredicateRhs::Literal(literal) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path,
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                Self::validate_string_predicate(path.clone(), predicate.operator, literal)?;
                Self::validate_literal_predicate(path.clone(), predicate.operator, literal)?;
                Self::validate_scalar_predicate_operand_types(
                    predicate.operator,
                    lhs_type,
                    literal_scalar_type(literal),
                    &path,
                )
            }
            ProjectionPredicateRhs::Alias(alias) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path,
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                if matches!(
                    predicate.operator,
                    ComparisonOperator::StartsWith
                        | ComparisonOperator::EndsWith
                        | ComparisonOperator::Contains
                        | ComparisonOperator::RegexMatch
                ) {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path,
                        "string predicates require a string literal right-hand side",
                    )
                    .into_core_error());
                }
                let rhs_type = self.projection_alias_scalar_type(alias, format!("{path}.rhs"))?;
                Self::validate_scalar_predicate_operand_types(
                    predicate.operator,
                    lhs_type,
                    rhs_type,
                    &path,
                )
            }
            ProjectionPredicateRhs::List(literals) => {
                if predicate.operator != ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path,
                        "literal lists are only supported with IN predicates",
                    )
                    .into_core_error());
                }
                Self::validate_scalar_in_list_operand_types(lhs_type, literals, &path)
            }
        }
    }
    pub(super) fn validate_string_predicate(
        path: impl Into<String>,
        operator: ComparisonOperator,
        literal: &Literal,
    ) -> Result<(), CoreError> {
        if !matches!(
            operator,
            ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains
                | ComparisonOperator::RegexMatch
        ) {
            return Ok(());
        }
        if !matches!(literal, Literal::String(_)) {
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_PREDICATE_OPERAND,
                path,
                "string predicates require a string literal right-hand side",
            )
            .into_core_error());
        }
        Ok(())
    }
    pub(super) fn validate_non_literal_string_predicate_operand(
        path: impl Into<String>,
        operator: ComparisonOperator,
    ) -> Result<(), CoreError> {
        if !matches!(
            operator,
            ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains
                | ComparisonOperator::RegexMatch
        ) {
            return Ok(());
        }
        Err(Diagnostic::new(
            diagnostic_codes::INVALID_PREDICATE_OPERAND,
            path,
            "string predicates require a string literal right-hand side",
        )
        .into_core_error())
    }
    pub(super) fn validate_literal_predicate(
        path: impl Into<String>,
        operator: ComparisonOperator,
        literal: &Literal,
    ) -> Result<(), CoreError> {
        let path = path.into();
        match (operator, literal) {
            (
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual,
                Literal::Null,
            ) => Err(Diagnostic::new(
                diagnostic_codes::INVALID_NULL_COMPARISON,
                path,
                "null can only be compared with equality or inequality",
            )
            .into_core_error()),
            _ => Ok(()),
        }
    }
}
