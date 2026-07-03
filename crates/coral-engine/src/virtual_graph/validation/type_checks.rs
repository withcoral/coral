//! Leaf type-check primitives: scalar-type merge/coercion, string/integer/numeric/orderable
//! requirements, catalog scalar-type lookup, property-ref checks, and scalar type-error constructors.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Leaf type-check helpers are split into a child module while preserving parent-private access."
)]
use super::*;

#[allow(
    clippy::allow_attributes,
    clippy::elidable_lifetime_names,
    reason = "Keep the split impl shape aligned with the parent GraphPlanValidator impl."
)]
impl<'a> GraphPlanValidator<'a> {
    pub(super) fn key_scalar_type(&self, variable: &str) -> Result<ScalarType, CoreError> {
        let binding = self.bindings.get(variable).ok_or_else(|| {
            Diagnostic::new(
                diagnostic_codes::UNKNOWN_VARIABLE,
                "variable",
                format!("unknown graph variable '{variable}'"),
            )
            .into_core_error()
        })?;
        let (table, column) = match binding.kind() {
            ValidatedBindingKind::Node(node) => (&node.table, node.key.as_str()),
            ValidatedBindingKind::Relationship(relationship) => {
                let Some(key) = relationship.key.as_deref() else {
                    return Ok(ScalarType::Unknown);
                };
                (&relationship.table, key)
            }
        };
        Ok(self.column_scalar_type(table, column))
    }

    pub(super) fn column_scalar_type(&self, table: &TableRef, column: &str) -> ScalarType {
        self.catalog
            .and_then(|catalog| {
                catalog.tables.iter().find(|candidate| {
                    candidate.schema_name == table.schema && candidate.table_name == table.name
                })
            })
            .and_then(|table| {
                table
                    .columns
                    .iter()
                    .find(|candidate| candidate.name == column)
            })
            .map_or(ScalarType::Unknown, |column| {
                scalar_type_for_data_type(&column.data_type)
            })
    }

    pub(super) fn validate_scalar_predicate_operand_types(
        operator: ComparisonOperator,
        lhs_type: ScalarType,
        rhs_type: ScalarType,
        path: &str,
    ) -> Result<(), CoreError> {
        if matches!(
            operator,
            ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains
                | ComparisonOperator::RegexMatch
        ) {
            Self::require_string_compatible_type(
                lhs_type,
                format!("{path}.lhs"),
                "string predicate",
            )?;
            Self::require_string_compatible_type(
                rhs_type,
                format!("{path}.rhs"),
                "string predicate",
            )?;
            return Ok(());
        }

        if matches!(
            operator,
            ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual
        ) {
            if matches!(rhs_type, ScalarType::Null) {
                return Err(Diagnostic::new(
                    diagnostic_codes::INVALID_NULL_COMPARISON,
                    path,
                    "null can only be compared with equality or inequality",
                )
                .into_core_error());
            }
            Self::validate_orderable_scalar_type(lhs_type, format!("{path}.lhs"))?;
            Self::validate_orderable_scalar_type(rhs_type, format!("{path}.rhs"))?;
        }

        Self::validate_compatible_scalar_types(
            lhs_type,
            rhs_type,
            path,
            "scalar predicate operands",
        )
    }

    pub(super) fn validate_predicate_rhs_operand_types(
        &self,
        operator: ComparisonOperator,
        lhs_type: ScalarType,
        rhs: &PredicateRhs,
        path: &str,
    ) -> Result<(), CoreError> {
        match rhs {
            PredicateRhs::Literal(literal) => Self::validate_scalar_predicate_operand_types(
                operator,
                lhs_type,
                literal_scalar_type(literal),
                path,
            ),
            PredicateRhs::Property(property) => {
                let rhs_type = self.property_ref_scalar_type(property)?;
                Self::validate_scalar_predicate_operand_types(operator, lhs_type, rhs_type, path)
            }
            PredicateRhs::Key { variable } => {
                let rhs_type = self.key_scalar_type(variable)?;
                Self::validate_scalar_predicate_operand_types(operator, lhs_type, rhs_type, path)
            }
            PredicateRhs::ElementId { .. } => Self::validate_scalar_predicate_operand_types(
                operator,
                lhs_type,
                ScalarType::String,
                path,
            ),
            PredicateRhs::List(literals) => {
                Self::validate_scalar_in_list_operand_types(lhs_type, literals, path)
            }
        }
    }

    pub(super) fn validate_scalar_in_list_operand_types(
        lhs_type: ScalarType,
        literals: &[Literal],
        path: &str,
    ) -> Result<(), CoreError> {
        let list_type = literal_list_scalar_type(literals)?;
        Self::validate_compatible_scalar_types(lhs_type, list_type, path, "IN predicate operands")
    }

    pub(super) fn merge_scalar_types(
        left: ScalarType,
        right: ScalarType,
        path: impl Into<String>,
        context: &str,
    ) -> Result<ScalarType, CoreError> {
        if matches!(left, ScalarType::Null) {
            return Ok(right);
        }
        if matches!(right, ScalarType::Null) {
            return Ok(left);
        }
        if matches!(left, ScalarType::Unknown) || matches!(right, ScalarType::Unknown) {
            return Ok(if matches!(left, ScalarType::Unknown) {
                right
            } else {
                left
            });
        }
        if left == right {
            return Ok(left);
        }
        if left.is_numeric() && right.is_numeric() {
            return Ok(ScalarType::Float);
        }
        Err(Self::scalar_type_error(path, context, left, right))
    }

    pub(super) fn validate_compatible_scalar_types(
        left: ScalarType,
        right: ScalarType,
        path: impl Into<String>,
        context: &str,
    ) -> Result<(), CoreError> {
        Self::merge_scalar_types(left, right, path, context).map(|_| ())
    }

    pub(super) fn require_string_compatible_type(
        scalar_type: ScalarType,
        path: impl Into<String>,
        context: &str,
    ) -> Result<(), CoreError> {
        if matches!(
            scalar_type,
            ScalarType::Unknown | ScalarType::Null | ScalarType::String
        ) {
            return Ok(());
        }
        Err(Self::expected_type_error(
            path,
            context,
            "string",
            scalar_type,
        ))
    }

    pub(super) fn require_integer_compatible_type(
        scalar_type: ScalarType,
        path: impl Into<String>,
        context: &str,
    ) -> Result<(), CoreError> {
        if matches!(
            scalar_type,
            ScalarType::Unknown | ScalarType::Null | ScalarType::Integer
        ) {
            return Ok(());
        }
        Err(Self::expected_type_error(
            path,
            context,
            "integer",
            scalar_type,
        ))
    }

    pub(super) fn require_numeric_compatible_type(
        scalar_type: ScalarType,
        path: impl Into<String>,
        context: &str,
    ) -> Result<(), CoreError> {
        if scalar_type.is_numeric() || matches!(scalar_type, ScalarType::Unknown | ScalarType::Null)
        {
            return Ok(());
        }
        Err(Self::expected_type_error(
            path,
            context,
            "numeric",
            scalar_type,
        ))
    }

    fn validate_orderable_scalar_type(
        scalar_type: ScalarType,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        if matches!(scalar_type, ScalarType::Boolean) {
            return Err(Self::expected_type_error(
                path,
                "range predicate",
                "orderable",
                scalar_type,
            ));
        }
        Ok(())
    }

    fn scalar_type_error(
        path: impl Into<String>,
        context: &str,
        left: ScalarType,
        right: ScalarType,
    ) -> CoreError {
        Diagnostic::new(
            diagnostic_codes::INVALID_SCALAR_TYPE,
            path,
            format!(
                "{context} require compatible scalar types, got {} and {}",
                left.name(),
                right.name()
            ),
        )
        .into_core_error()
    }

    fn expected_type_error(
        path: impl Into<String>,
        context: &str,
        expected: &str,
        actual: ScalarType,
    ) -> CoreError {
        Diagnostic::new(
            diagnostic_codes::INVALID_SCALAR_TYPE,
            path,
            format!(
                "{context} requires a {expected} scalar expression, got {}",
                actual.name()
            ),
        )
        .into_core_error()
    }

    pub(super) fn validate_property_ref(
        &self,
        property: &PropertyRef,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let binding = self
            .bindings
            .get(property.variable.as_str())
            .ok_or_else(|| {
                Diagnostic::new(
                    diagnostic_codes::UNKNOWN_VARIABLE,
                    path.clone(),
                    format!("unknown graph variable '{}'", property.variable),
                )
                .into_core_error()
            })?;
        if binding.column_for_property(&property.property).is_none() {
            return Err(Diagnostic::new(
                diagnostic_codes::UNKNOWN_PROPERTY,
                path,
                format!(
                    "variable '{}' does not expose property '{}'",
                    property.variable, property.property
                ),
            )
            .into_core_error());
        }
        Ok(())
    }
}
