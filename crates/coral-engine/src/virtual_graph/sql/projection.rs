#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "SQL projection helpers are split into a child module while preserving parent-private access."
)]
use super::*;

#[allow(
    clippy::allow_attributes,
    clippy::elidable_lifetime_names,
    reason = "SQL child modules use the same explicit Lowerer lifetime shape as the parent impl."
)]
impl<'a> Lowerer<'a> {
    pub(super) fn render_select(&self) -> Result<String, CoreError> {
        let mut rendered = Vec::with_capacity(self.validated.plan().projections.len());
        for projection in &self.validated.plan().projections {
            rendered.push(self.render_projection_select_item(projection)?);
        }
        Ok(format!(
            "SELECT {}{}",
            if self.validated.plan().distinct {
                "DISTINCT "
            } else {
                ""
            },
            rendered.join(", ")
        ))
    }

    fn render_projection_select_item(&self, projection: &Projection) -> Result<String, CoreError> {
        match projection {
            Projection::Property { property, alias } => {
                let expression = self.render_property_ref(property)?;
                let alias = alias
                    .as_deref()
                    .map_or_else(|| projection.output_name(), ToString::to_string);
                Ok(format!("{expression} AS {}", quote_ident(&alias)))
            }
            Projection::Key { variable, alias } => Ok(format!(
                "{} AS {}",
                self.render_binding_key_ref(variable)?,
                quote_ident(alias)
            )),
            Projection::ElementId { variable, alias } => Ok(format!(
                "{} AS {}",
                self.render_binding_element_id_ref(variable)?,
                quote_ident(alias)
            )),
            Projection::NodeLabels {
                variable,
                label,
                alias,
            } => Ok(format!(
                "{} AS {}",
                self.render_node_labels_ref(variable, label)?,
                quote_ident(alias)
            )),
            Projection::PropertyKeys { variable, alias } => Ok(format!(
                "{} AS {}",
                self.render_property_keys_ref(variable)?,
                quote_ident(alias)
            )),
            Projection::RelationshipType {
                variable,
                relationship_type,
                alias,
            } => Ok(format!(
                "{} AS {}",
                self.render_relationship_type_ref(variable, relationship_type)?,
                quote_ident(alias)
            )),
            Projection::Literal { literal, alias } => Ok(format!(
                "{} AS {}",
                render_literal(literal),
                quote_ident(alias)
            )),
            Projection::LiteralList { literals, alias } => Ok(format!(
                "{} AS {}",
                render_literal_list(literals),
                quote_ident(alias)
            )),
            Projection::Expression { expression, alias } => Ok(format!(
                "{} AS {}",
                self.render_projection_scalar_expression(expression)?,
                quote_ident(alias)
            )),
            Projection::CountAll { alias } => Ok(format!("COUNT(*) AS {}", quote_ident(alias))),
            Projection::Aggregate {
                function,
                target,
                distinct,
                alias,
            } => Ok(format!(
                "{} AS {}",
                self.render_aggregate_invocation(*function, target, *distinct)?,
                quote_ident(alias)
            )),
        }
    }

    pub(super) fn plan_has_aggregation(&self) -> bool {
        self.validated
            .plan()
            .projections
            .iter()
            .any(Projection::is_aggregate)
            || self.validated.plan().order_by.iter().any(|key| {
                matches!(
                    &key.expression,
                    OrderExpression::CountAll | OrderExpression::Aggregate { .. }
                )
            })
    }

    pub(super) fn render_group_by(&self) -> Result<String, CoreError> {
        if !self.plan_has_aggregation() {
            return Ok(String::new());
        }

        let expressions = self.render_group_by_expressions()?;
        if expressions.is_empty() {
            Ok(String::new())
        } else {
            Ok(format!(" GROUP BY {}", expressions.join(", ")))
        }
    }

    fn render_group_by_expressions(&self) -> Result<Vec<String>, CoreError> {
        let mut expressions = Vec::new();
        for projection in &self.validated.plan().projections {
            match projection {
                Projection::Property { property, .. } => {
                    expressions.push(self.render_property_ref(property)?);
                }
                Projection::Key { variable, .. } => {
                    expressions.push(self.render_binding_key_ref(variable)?);
                }
                Projection::ElementId { variable, .. } => {
                    expressions.push(self.render_binding_element_id_ref(variable)?);
                }
                Projection::RelationshipType {
                    variable,
                    relationship_type,
                    ..
                } => {
                    expressions
                        .push(self.render_relationship_type_ref(variable, relationship_type)?);
                }
                Projection::NodeLabels {
                    variable, label, ..
                } => {
                    expressions.push(self.render_node_labels_ref(variable, label)?);
                }
                Projection::PropertyKeys { variable, .. } => {
                    expressions.push(self.render_property_keys_ref(variable)?);
                }
                Projection::Expression { expression, .. } => {
                    expressions.push(self.render_scalar_expression(expression)?);
                }
                Projection::Literal { .. }
                | Projection::LiteralList { .. }
                | Projection::CountAll { .. }
                | Projection::Aggregate { .. } => {}
            }
        }
        Ok(expressions)
    }

    fn render_projection_scalar_expression(
        &self,
        expression: &ScalarExpression,
    ) -> Result<String, CoreError> {
        self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
        self.render_scalar_expression(expression)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "This exhaustive scalar IR dispatcher keeps projection subquery checks total over every scalar variant"
    )]
    fn reject_unprecomputed_projection_scalar_subqueries(
        &self,
        expression: &ScalarExpression,
    ) -> Result<(), CoreError> {
        if let Some(expression) = scalar_expression_unary_operand(expression) {
            return self.reject_unprecomputed_projection_scalar_subqueries(expression);
        }

        match expression {
            ScalarExpression::CountSubquery {
                pattern,
                distinct_target,
            } => {
                if let CountSubqueryPattern::Relationships(predicate) = pattern.as_ref()
                    && predicate.references_outer_variables()
                    && self
                        .render_precomputed_count_subquery_ref(pattern, distinct_target.as_deref())
                        .is_none()
                {
                    return Err(CoreError::InvalidInput(
                        "correlated relationship COUNT subqueries in projections must be precomputable through a single outer node anchor; move complex outer-dependent predicates to WHERE EXISTS or simplify the COUNT pattern"
                            .to_string(),
                    ));
                }
            }
            ScalarExpression::CollectSubquery {
                pattern,
                target,
                distinct,
            } => {
                self.reject_unprecomputed_projection_collect_subquery(pattern, target, *distinct)?;
            }
            ScalarExpression::Predicate(predicate) => {
                self.reject_unprecomputed_projection_predicate_subqueries(predicate)?;
            }
            ScalarExpression::PresenceGated { .. }
            | ScalarExpression::Coalesce { .. }
            | ScalarExpression::NullIf { .. }
            | ScalarExpression::Round { .. }
            | ScalarExpression::Left { .. }
            | ScalarExpression::Right { .. }
            | ScalarExpression::StringIndices { .. }
            | ScalarExpression::LPad { .. }
            | ScalarExpression::RPad { .. }
            | ScalarExpression::StringContains { .. }
            | ScalarExpression::StringStartsWith { .. }
            | ScalarExpression::StringEndsWith { .. }
            | ScalarExpression::Replace { .. }
            | ScalarExpression::Substring { .. }
            | ScalarExpression::Arithmetic { .. }
            | ScalarExpression::Case { .. }
            | ScalarExpression::Atan2 { .. } => {
                self.reject_unprecomputed_projection_structural_subqueries(expression)?;
            }
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
            | ScalarExpression::Key { .. }
            | ScalarExpression::ElementId { .. }
            | ScalarExpression::GraphIdentity { .. }
            | ScalarExpression::GraphPresence { .. }
            | ScalarExpression::NodeLabels { .. }
            | ScalarExpression::PropertyKeys { .. }
            | ScalarExpression::RelationshipType { .. } => {}
            ScalarExpression::ToString { .. }
            | ScalarExpression::ToInteger { .. }
            | ScalarExpression::ToFloat { .. }
            | ScalarExpression::ToBoolean { .. }
            | ScalarExpression::ToStringOrNull { .. }
            | ScalarExpression::ToIntegerOrNull { .. }
            | ScalarExpression::ToFloatOrNull { .. }
            | ScalarExpression::ToBooleanOrNull { .. }
            | ScalarExpression::ToLower { .. }
            | ScalarExpression::ToUpper { .. }
            | ScalarExpression::Trim { .. }
            | ScalarExpression::LTrim { .. }
            | ScalarExpression::RTrim { .. }
            | ScalarExpression::CharacterLength { .. }
            | ScalarExpression::Reverse { .. }
            | ScalarExpression::Abs { .. }
            | ScalarExpression::Ceil { .. }
            | ScalarExpression::Floor { .. }
            | ScalarExpression::Sqrt { .. }
            | ScalarExpression::Sign { .. }
            | ScalarExpression::Exp { .. }
            | ScalarExpression::Log { .. }
            | ScalarExpression::Log10 { .. }
            | ScalarExpression::Sin { .. }
            | ScalarExpression::Cos { .. }
            | ScalarExpression::Tan { .. }
            | ScalarExpression::Cot { .. }
            | ScalarExpression::Asin { .. }
            | ScalarExpression::Acos { .. }
            | ScalarExpression::Atan { .. }
            | ScalarExpression::Degrees { .. }
            | ScalarExpression::Radians { .. }
            | ScalarExpression::IsNaN { .. }
            | ScalarExpression::Negate { .. } => {
                unreachable!("unary scalar expressions handled before projection subquery checks")
            }
        }
        Ok(())
    }

    fn reject_unprecomputed_projection_structural_subqueries(
        &self,
        expression: &ScalarExpression,
    ) -> Result<(), CoreError> {
        if let Some((left, right)) = Self::structural_scalar_binary_operands(expression) {
            self.reject_unprecomputed_projection_scalar_subqueries(left)?;
            self.reject_unprecomputed_projection_scalar_subqueries(right)?;
            return Ok(());
        }
        if let Some((first, second, third)) = Self::structural_scalar_ternary_operands(expression) {
            self.reject_unprecomputed_projection_scalar_subqueries(first)?;
            self.reject_unprecomputed_projection_scalar_subqueries(second)?;
            self.reject_unprecomputed_projection_scalar_subqueries(third)?;
            return Ok(());
        }

        match expression {
            ScalarExpression::PresenceGated { expression, .. } => {
                self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
            }
            ScalarExpression::Coalesce { expressions } => {
                for expression in expressions {
                    self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
                }
            }
            ScalarExpression::Round { expression, places } => {
                self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
                if let Some(places) = places {
                    self.reject_unprecomputed_projection_scalar_subqueries(places)?;
                }
            }
            ScalarExpression::Substring {
                expression,
                start,
                length,
            } => {
                self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
                self.reject_unprecomputed_projection_scalar_subqueries(start)?;
                if let Some(length) = length {
                    self.reject_unprecomputed_projection_scalar_subqueries(length)?;
                }
            }
            ScalarExpression::Case {
                alternatives,
                else_expression,
            } => {
                for alternative in alternatives {
                    self.reject_unprecomputed_projection_predicate_subqueries(&alternative.when)?;
                    self.reject_unprecomputed_projection_scalar_subqueries(&alternative.then)?;
                }
                if let Some(else_expression) = else_expression {
                    self.reject_unprecomputed_projection_scalar_subqueries(else_expression)?;
                }
            }
            _ => {
                unreachable!("projection subquery dispatcher called structural helper incorrectly")
            }
        }
        Ok(())
    }

    fn reject_unprecomputed_projection_collect_subquery(
        &self,
        pattern: &CountSubqueryPattern,
        target: &ScalarExpression,
        distinct: bool,
    ) -> Result<(), CoreError> {
        if let CountSubqueryPattern::Relationships(predicate) = pattern
            && predicate.references_outer_variables()
            && self
                .render_precomputed_collect_subquery_ref(predicate, target, distinct)
                .is_none()
        {
            return Err(CoreError::InvalidInput(
                "correlated relationship COLLECT subqueries in projections must be precomputable through a single outer node anchor and an inner-only return target; move complex outer-dependent logic to the scoped WHERE predicate or simplify the COLLECT pattern"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn reject_unprecomputed_projection_predicate_subqueries(
        &self,
        predicate: &PredicateExpression,
    ) -> Result<(), CoreError> {
        match predicate {
            PredicateExpression::ScalarComparison(predicate) => {
                self.reject_unprecomputed_projection_scalar_subqueries(&predicate.lhs)?;
                if let ScalarPredicateRhs::Expression(expression) = &predicate.rhs {
                    self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
                }
            }
            PredicateExpression::And { left, right }
            | PredicateExpression::Or { left, right }
            | PredicateExpression::Xor { left, right } => {
                self.reject_unprecomputed_projection_predicate_subqueries(left)?;
                self.reject_unprecomputed_projection_predicate_subqueries(right)?;
            }
            PredicateExpression::Not { expression } => {
                self.reject_unprecomputed_projection_predicate_subqueries(expression)?;
            }
            PredicateExpression::Boolean(_)
            | PredicateExpression::Comparison(_)
            | PredicateExpression::KeyComparison(_)
            | PredicateExpression::ElementIdComparison(_)
            | PredicateExpression::Presence(_)
            | PredicateExpression::PropertyKeyMembership(_)
            | PredicateExpression::ExistsPattern(_) => {}
        }
        Ok(())
    }

    pub(super) fn render_order_by(&self) -> Result<String, CoreError> {
        if self.validated.plan().order_by.is_empty() {
            return Ok(String::new());
        }

        let mut keys = Vec::with_capacity(self.validated.plan().order_by.len());
        for key in &self.validated.plan().order_by {
            let nulls = render_null_order(key.nulls);
            keys.push(format!(
                "{} {}{}",
                self.render_order_expression(&key.expression)?,
                match key.direction {
                    OrderDirection::Ascending => "ASC",
                    OrderDirection::Descending => "DESC",
                },
                nulls,
            ));
        }
        Ok(format!(" ORDER BY {}", keys.join(", ")))
    }

    fn render_order_expression(&self, expression: &OrderExpression) -> Result<String, CoreError> {
        match expression {
            OrderExpression::Property(property) => self.render_property_ref(property),
            OrderExpression::Key { variable } => self.render_binding_key_ref(variable),
            OrderExpression::ElementId { variable } => self.render_binding_element_id_ref(variable),
            OrderExpression::NodeLabels { variable, label } => {
                self.render_node_labels_ref(variable, label)
            }
            OrderExpression::PropertyKeys { variable } => self.render_property_keys_ref(variable),
            OrderExpression::RelationshipType {
                variable,
                relationship_type,
            } => self.render_relationship_type_ref(variable, relationship_type),
            OrderExpression::CountAll => Ok("COUNT(*)".to_string()),
            OrderExpression::Aggregate {
                function,
                target,
                distinct,
            } => self.render_aggregate_invocation(*function, target, *distinct),
            OrderExpression::Scalar(ScalarExpression::Literal(literal)) => {
                Ok(render_order_literal(literal))
            }
            OrderExpression::Scalar(ScalarExpression::Predicate(predicate)) => {
                self.render_order_predicate_expression(predicate)
            }
            OrderExpression::Scalar(expression) => self.render_scalar_expression(expression),
            OrderExpression::Literal(literal) => Ok(render_order_literal(literal)),
            OrderExpression::ProjectionAlias(alias) => Ok(quote_ident(alias)),
        }
    }

    fn render_order_predicate_expression(
        &self,
        predicate: &PredicateExpression,
    ) -> Result<String, CoreError> {
        if let PredicateExpression::ExistsPattern(pattern) = predicate
            && let Some(precomputed) =
                self.precomputed_scalar_subqueries
                    .iter()
                    .find(|precomputed| {
                        precomputed.candidate == ScalarSubqueryCandidate::Exists(pattern.clone())
                    })
        {
            return Ok(Self::render_precomputed_exists_ref(precomputed));
        }
        self.render_scalar_predicate_expression(predicate)
    }

    fn render_aggregate_target(
        &self,
        function: AggregateFunction,
        target: &AggregateTarget,
    ) -> Result<String, CoreError> {
        match target {
            AggregateTarget::Property(property) => self.render_property_ref(property),
            AggregateTarget::PresenceGatedProperty {
                property,
                presence_variable,
            } => {
                let presence = self.render_binding_presence_ref(presence_variable)?;
                let property = self.render_property_ref(property)?;
                Ok(format!(
                    "CASE WHEN {presence} IS NULL THEN NULL ELSE {property} END"
                ))
            }
            AggregateTarget::Expression(expression) => self.render_scalar_expression(expression),
            AggregateTarget::VariableKey { variable } => {
                if function == AggregateFunction::Count {
                    self.render_binding_presence_ref(variable)
                } else {
                    self.render_binding_key_ref(variable)
                }
            }
            AggregateTarget::PresenceGatedVariableKey {
                variable,
                presence_variable,
            } => {
                let presence = self.render_binding_presence_ref(presence_variable)?;
                let key = self.render_binding_key_ref(variable)?;
                Ok(format!(
                    "CASE WHEN {presence} IS NULL THEN NULL ELSE {key} END"
                ))
            }
        }
    }

    pub(super) fn render_aggregate_invocation(
        &self,
        function: AggregateFunction,
        target: &AggregateTarget,
        distinct: bool,
    ) -> Result<String, CoreError> {
        let target = self.render_aggregate_target(function, target)?;
        Ok(render_aggregate_invocation_sql(function, &target, distinct))
    }
}
