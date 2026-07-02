//! Aggregation validation: aggregate plan shape (presence and ORDER BY compatibility),
//! DISTINCT aggregate option rules, and aggregate target/type validation and inference
//! for the graph plan validator.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Aggregation validation methods are split into a child module while preserving parent-private access."
)]
use super::*;

#[allow(
    clippy::allow_attributes,
    clippy::elidable_lifetime_names,
    reason = "Keep the split validation impl shape aligned with the parent GraphPlanValidator impl."
)]
impl<'a> GraphPlanValidator<'a> {
    pub(super) fn validate_aggregation(&self) -> Result<(), CoreError> {
        if !self.plan_has_aggregation() {
            return Ok(());
        }
        self.validate_distinct_keyless_relationship_counts()?;
        self.validate_distinct_aggregate_options()?;
        let projected_properties = self.projected_properties();
        for (index, order_key) in self.plan.order_by.iter().enumerate() {
            if !self.order_expression_is_projected_property_alias_or_aggregate(
                &order_key.expression,
                &projected_properties,
            ) {
                return Err(Diagnostic::new(
                    "UNSUPPORTED_AGGREGATION",
                    format!("order_by[{index}]"),
                    "ORDER BY with aggregate projections must use a projected property or projection alias",
                )
                .into_core_error());
            }
        }
        Ok(())
    }

    fn plan_has_aggregation(&self) -> bool {
        self.plan.projections.iter().any(Projection::is_aggregate)
            || self.plan.order_by.iter().any(|key| {
                matches!(
                    &key.expression,
                    OrderExpression::CountAll | OrderExpression::Aggregate { .. }
                )
            })
    }

    fn validate_distinct_aggregate_options(&self) -> Result<(), CoreError> {
        for (index, projection) in self.plan.projections.iter().enumerate() {
            let Projection::Aggregate {
                function: AggregateFunction::PercentileCont { .. },
                distinct: true,
                ..
            } = projection
            else {
                continue;
            };
            return Err(unsupported_distinct_percentile_cont_error(format!(
                "projections[{index}].distinct"
            )));
        }
        for (index, key) in self.plan.order_by.iter().enumerate() {
            let OrderExpression::Aggregate {
                function: AggregateFunction::PercentileCont { .. },
                distinct: true,
                ..
            } = &key.expression
            else {
                continue;
            };
            return Err(unsupported_distinct_percentile_cont_error(format!(
                "order_by[{index}].aggregate.distinct"
            )));
        }
        Ok(())
    }

    fn validate_distinct_keyless_relationship_counts(&self) -> Result<(), CoreError> {
        for (index, projection) in self.plan.projections.iter().enumerate() {
            let Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::VariableKey { variable },
                distinct: true,
                ..
            } = projection
            else {
                continue;
            };
            self.validate_distinct_keyless_relationship_count_target(
                variable,
                format!("projections[{index}].target"),
            )?;
        }
        for (index, key) in self.plan.order_by.iter().enumerate() {
            let OrderExpression::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::VariableKey { variable },
                distinct: true,
            } = &key.expression
            else {
                continue;
            };
            self.validate_distinct_keyless_relationship_count_target(
                variable,
                format!("order_by[{index}].aggregate.target"),
            )?;
        }
        Ok(())
    }

    fn validate_distinct_keyless_relationship_count_target(
        &self,
        variable: &str,
        path: String,
    ) -> Result<(), CoreError> {
        let Some(ValidatedBindingKind::Relationship(relationship)) =
            self.bindings.get(variable).map(ValidatedBinding::kind)
        else {
            return Ok(());
        };
        if relationship.key.is_none() {
            return Err(Diagnostic::new(
                "INVALID_AGGREGATE_TARGET",
                path,
                format!(
                    "count(DISTINCT {variable}) requires relationship mapping '{}' to declare a key",
                    relationship.relationship_type
                ),
            )
            .into_core_error());
        }
        Ok(())
    }

    pub(super) fn validate_aggregate_target(
        &self,
        function: AggregateFunction,
        target: &AggregateTarget,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        match target {
            AggregateTarget::Property(property) => {
                self.validate_property_ref(property, path.clone())?;
                self.validate_aggregate_property_type(function, property, path)
            }
            AggregateTarget::PresenceGatedProperty {
                property,
                presence_variable,
            } => {
                self.validate_property_ref(property, format!("{path}.property"))?;
                validate_variable(format!("{path}.presence_variable"), presence_variable)?;
                self.bindings
                    .get(presence_variable.as_str())
                    .ok_or_else(|| {
                        Diagnostic::new(
                            "UNKNOWN_VARIABLE",
                            format!("{path}.presence_variable"),
                            format!("unknown graph variable '{presence_variable}'"),
                        )
                        .into_core_error()
                    })?;
                self.validate_aggregate_property_type(function, property, path)
            }
            AggregateTarget::Expression(expression) => {
                self.validate_scalar_expression(expression, format!("{path}.expression"))?;
                self.validate_aggregate_expression_type(function, expression, path)
            }
            AggregateTarget::VariableKey { variable } => {
                self.validate_graph_variable_aggregate_target(function, variable, path)
            }
            AggregateTarget::PresenceGatedVariableKey {
                variable,
                presence_variable,
            } => {
                validate_variable(format!("{path}.presence_variable"), presence_variable)?;
                self.bindings
                    .get(presence_variable.as_str())
                    .ok_or_else(|| {
                        Diagnostic::new(
                            "UNKNOWN_VARIABLE",
                            format!("{path}.presence_variable"),
                            format!("unknown graph variable '{presence_variable}'"),
                        )
                        .into_core_error()
                    })?;
                self.validate_graph_variable_aggregate_target(
                    function,
                    variable,
                    format!("{path}.variable"),
                )
            }
        }
    }

    fn validate_graph_variable_aggregate_target(
        &self,
        function: AggregateFunction,
        variable: &str,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        if !aggregate_function_accepts_graph_variable_key(function) {
            return Err(Diagnostic::new(
                "INVALID_AGGREGATE_TARGET",
                path.clone(),
                format!(
                    "{}({variable}) requires a graph property argument; only count(variable) and collect(variable) can aggregate a graph variable key",
                    aggregate_function_name(function)
                ),
            )
            .into_core_error());
        }
        validate_variable(path.clone(), variable)?;
        let binding = self.bindings.get(variable).ok_or_else(|| {
            Diagnostic::new(
                "UNKNOWN_VARIABLE",
                path.clone(),
                format!("unknown graph variable '{variable}'"),
            )
            .into_core_error()
        })?;
        validate_collect_graph_variable_aggregate_binding(function, variable, binding.kind(), path)
    }

    fn validate_aggregate_property_type(
        &self,
        function: AggregateFunction,
        property: &PropertyRef,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        if self.catalog.is_none() || !aggregate_function_requires_numeric_target(function) {
            return Ok(());
        }
        let scalar_type = self.property_ref_scalar_type(property)?;
        validate_aggregate_scalar_type(function, scalar_type, path)
    }

    fn validate_aggregate_expression_type(
        &self,
        function: AggregateFunction,
        expression: &ScalarExpression,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        if !aggregate_function_requires_numeric_target(function) {
            return Ok(());
        }
        let scalar_type =
            self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
        validate_aggregate_scalar_type(function, scalar_type, path)
    }

    pub(super) fn infer_aggregate_projection_type(
        &self,
        function: AggregateFunction,
        target: &AggregateTarget,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        self.validate_aggregate_target(function, target, path)?;
        match function {
            AggregateFunction::Count => Ok(ScalarType::Integer),
            AggregateFunction::Collect => Ok(ScalarType::Other),
            AggregateFunction::Sum
            | AggregateFunction::Avg
            | AggregateFunction::Median
            | AggregateFunction::PercentileCont { .. }
            | AggregateFunction::StdDev
            | AggregateFunction::StdDevP => Ok(ScalarType::Float),
            AggregateFunction::Min | AggregateFunction::Max => match target {
                AggregateTarget::Property(property)
                | AggregateTarget::PresenceGatedProperty { property, .. } => {
                    self.property_ref_scalar_type(property)
                }
                AggregateTarget::Expression(expression) => {
                    self.infer_scalar_expression_type(expression, "expression")
                }
                AggregateTarget::VariableKey { .. }
                | AggregateTarget::PresenceGatedVariableKey { .. } => Ok(ScalarType::Unknown),
            },
        }
    }
}

fn validate_collect_graph_variable_aggregate_binding(
    function: AggregateFunction,
    variable: &str,
    binding: &ValidatedBindingKind<'_>,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    let ValidatedBindingKind::Relationship(relationship) = binding else {
        return Ok(());
    };
    if function == AggregateFunction::Collect && relationship.key.is_none() {
        return Err(Diagnostic::new(
            "INVALID_AGGREGATE_TARGET",
            path,
            format!(
                "collect({variable}) requires relationship mapping '{}' to declare a key",
                relationship.relationship_type
            ),
        )
        .into_core_error());
    }
    Ok(())
}

pub(super) fn validate_aggregate_scalar_type(
    function: AggregateFunction,
    scalar_type: ScalarType,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    if !aggregate_function_requires_numeric_target(function)
        || scalar_type.is_numeric()
        || matches!(scalar_type, ScalarType::Unknown | ScalarType::Null)
    {
        return Ok(());
    }
    Err(Diagnostic::new(
        "INVALID_AGGREGATE_TARGET",
        path,
        format!(
            "{}(property) requires a numeric property, got {}",
            aggregate_function_name(function),
            scalar_type.name()
        ),
    )
    .into_core_error())
}
