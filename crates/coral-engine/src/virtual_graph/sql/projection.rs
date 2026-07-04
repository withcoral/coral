//! Projection rendering for the SQL `SqlRenderer`: builds the `SELECT`, `GROUP BY` and `ORDER BY`
//! clauses from graph-plan projections — projected columns, aggregate targets/invocations,
//! aggregation detection and grouping keys, and order expressions — and rejects projection
//! scalar/structural/COLLECT/predicate subqueries that were not precomputed into joins.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "SQL projection helpers are split into a child module while preserving parent-private access."
)]
use super::*;

#[allow(
    clippy::allow_attributes,
    clippy::elidable_lifetime_names,
    reason = "SQL child modules use the same explicit SqlRenderer lifetime shape as the parent impl."
)]
impl<'a> SqlRenderer<'a> {
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
        if scalar_expression_projects_duration(expression) {
            return self.render_duration_to_iso_expression(expression, ScalarScope::TopLevel);
        }
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
            | ScalarExpression::Temporal(_)
            | ScalarExpression::Arithmetic { .. }
            | ScalarExpression::ListConcat { .. }
            | ScalarExpression::Case { .. }
            | ScalarExpression::Atan2 { .. } => {
                self.reject_unprecomputed_projection_structural_subqueries(expression)?;
            }
            ScalarExpression::Property(_)
            | ScalarExpression::StageValue { .. }
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
            ScalarExpression::Temporal(TemporalExpr::MakeLocalDateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                millisecond,
                microsecond,
                nanosecond,
            }) => {
                for expression in [
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    millisecond,
                    microsecond,
                    nanosecond,
                ] {
                    self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
                }
            }
            ScalarExpression::Temporal(TemporalExpr::MakeLocalTime {
                hour,
                minute,
                second,
                millisecond,
                microsecond,
                nanosecond,
            }) => {
                for expression in [hour, minute, second, millisecond, microsecond, nanosecond] {
                    self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
                }
            }
            ScalarExpression::Temporal(TemporalExpr::MakeDuration { .. }) => {}
            ScalarExpression::Temporal(TemporalExpr::DurationInUnits { start, end, .. }) => {
                self.reject_unprecomputed_projection_scalar_subqueries(start)?;
                self.reject_unprecomputed_projection_scalar_subqueries(end)?;
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
            && let Some(rendered) = self.subquery_plan.exists_ref(pattern)
        {
            return Ok(rendered);
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
        if matches!(function, AggregateFunction::PercentileDisc { .. }) {
            let aggregate = PercentileDiscAggregate {
                function,
                target: target.clone(),
                distinct,
            };
            return self
                .percentile_disc_plan
                .aggregate_ref(&aggregate)
                .ok_or_else(|| {
                    CoreError::internal("percentileDisc aggregate was not precomputed")
                });
        }
        let target = self.render_aggregate_target(function, target)?;
        Ok(render_aggregate_invocation_sql(function, &target, distinct))
    }

    pub(super) fn build_percentile_disc_aggregate_plan(
        &self,
    ) -> Result<PercentileDiscAggregatePlan, CoreError> {
        let candidates = self.percentile_disc_aggregate_candidates();
        if candidates.is_empty() {
            return Ok(PercentileDiscAggregatePlan::default());
        }

        let mut aggregates = Vec::new();
        let mut from_joins = String::new();
        for candidate in candidates {
            if aggregates
                .iter()
                .any(|precomputed: &PrecomputedPercentileDiscAggregate| {
                    precomputed.aggregate == candidate
                })
            {
                continue;
            }
            let index = aggregates.len();
            let precomputed = PrecomputedPercentileDiscAggregate {
                aggregate: candidate,
                table_alias: format!("__coral_percentile_disc_{index}"),
                value_alias: "__coral_value".to_string(),
                group_aliases: self
                    .render_group_by_expressions()?
                    .iter()
                    .enumerate()
                    .map(|(group_index, _)| format!("__coral_group_{group_index}"))
                    .collect(),
            };
            from_joins.push(' ');
            from_joins.push_str(&self.render_percentile_disc_aggregate_join(&precomputed)?);
            aggregates.push(precomputed);
        }

        Ok(PercentileDiscAggregatePlan {
            aggregates,
            from_joins,
        })
    }

    fn percentile_disc_aggregate_candidates(&self) -> Vec<PercentileDiscAggregate> {
        let mut candidates = Vec::new();
        for projection in &self.validated.plan().projections {
            if let Projection::Aggregate {
                function: function @ AggregateFunction::PercentileDisc { .. },
                target,
                distinct,
                ..
            } = projection
            {
                candidates.push(PercentileDiscAggregate {
                    function: *function,
                    target: target.clone(),
                    distinct: *distinct,
                });
            }
        }
        for key in &self.validated.plan().order_by {
            if let OrderExpression::Aggregate {
                function: function @ AggregateFunction::PercentileDisc { .. },
                target,
                distinct,
            } = &key.expression
            {
                candidates.push(PercentileDiscAggregate {
                    function: *function,
                    target: target.clone(),
                    distinct: *distinct,
                });
            }
        }
        candidates
    }

    #[expect(
        clippy::too_many_lines,
        reason = "The percentileDisc lowering assembles one derived-table SQL shape with correlated group keys."
    )]
    fn render_percentile_disc_aggregate_join(
        &self,
        precomputed: &PrecomputedPercentileDiscAggregate,
    ) -> Result<String, CoreError> {
        if precomputed.aggregate.distinct {
            return Err(CoreError::InvalidInput(
                "percentileDisc(DISTINCT ...) is not supported because DataFusion 53 cannot execute distinct percentile_disc aggregates"
                    .to_string(),
            ));
        }
        let AggregateFunction::PercentileDisc { percentile } = precomputed.aggregate.function
        else {
            return Err(CoreError::internal(
                "percentileDisc precompute was requested for a non-percentileDisc aggregate",
            ));
        };

        let outer_groups = self.render_group_by_expressions()?;
        let inner_validated = self
            .validated
            .with_alias_prefix(&format!("{}_", precomputed.table_alias));
        let mut inner = SqlRenderer::new(inner_validated);
        let mut inner_from = FromClauseBuilder::new(&inner).build()?;
        let inner_subquery_plan = inner.build_scalar_subquery_plan()?;
        inner_from.push_str(&inner_subquery_plan.from_joins);
        inner.subquery_plan = inner_subquery_plan;

        let inner_groups = inner.render_group_by_expressions()?;
        if inner_groups.len() != outer_groups.len() {
            return Err(CoreError::internal(
                "percentileDisc group correlation had mismatched key counts",
            ));
        }

        let inner_target = inner.render_aggregate_target(
            precomputed.aggregate.function,
            &precomputed.aggregate.target,
        )?;
        let mut predicates = inner.render_pre_projection_predicates()?;
        predicates.push(format!("({inner_target}) IS NOT NULL"));
        let inner_where = if predicates.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", predicates.join(" AND "))
        };

        let value_alias = quote_ident(&precomputed.value_alias);
        let row_number_alias = quote_ident("__coral_rn");
        let count_alias = quote_ident("__coral_n");
        let row_source_alias = format!("{}_rows", precomputed.table_alias);
        let quoted_row_source_alias = quote_ident(&row_source_alias);
        let inner_group_selects = inner_groups
            .iter()
            .zip(precomputed.group_aliases.iter())
            .map(|(expression, alias)| format!("{expression} AS {}", quote_ident(alias)));
        let partition = if inner_groups.is_empty() {
            String::new()
        } else {
            format!("PARTITION BY {} ", inner_groups.join(", "))
        };
        let mut row_selects = inner_group_selects.collect::<Vec<_>>();
        row_selects.push(format!("{inner_target} AS {value_alias}"));
        row_selects.push(format!(
            "CAST(row_number() OVER ({partition}ORDER BY {inner_target}) AS BIGINT) AS {row_number_alias}"
        ));
        row_selects.push(format!("COUNT(*) OVER ({partition}) AS {count_alias}"));

        let percentile = render_percentile_disc_literal(percentile.into_inner());
        let qualified_count = qualified_ref(&row_source_alias, "__coral_n");
        let qualified_row_number = qualified_ref(&row_source_alias, "__coral_rn");
        let qualified_value = qualified_ref(&row_source_alias, &precomputed.value_alias);
        let requested_position = format!("CAST(ceil({percentile} * {qualified_count}) AS BIGINT)");
        let selected_position =
            format!("CASE WHEN {requested_position} < 1 THEN 1 ELSE {requested_position} END");

        let group_selects = precomputed
            .group_aliases
            .iter()
            .map(|alias| {
                format!(
                    "{} AS {}",
                    qualified_ref(&row_source_alias, alias),
                    quote_ident(alias)
                )
            })
            .collect::<Vec<_>>();
        let mut aggregate_selects = group_selects;
        aggregate_selects.push(format!(
            "MAX(CASE WHEN {qualified_row_number} = {selected_position} THEN {qualified_value} ELSE NULL END) AS {value_alias}"
        ));
        let group_by = if precomputed.group_aliases.is_empty() {
            String::new()
        } else {
            format!(
                " GROUP BY {}",
                precomputed
                    .group_aliases
                    .iter()
                    .map(|alias| qualified_ref(&row_source_alias, alias))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };

        let join_condition = if precomputed.group_aliases.is_empty() {
            "TRUE".to_string()
        } else {
            precomputed
                .group_aliases
                .iter()
                .zip(outer_groups.iter())
                .map(|(alias, outer)| {
                    render_null_safe_correlation(
                        &qualified_ref(&precomputed.table_alias, alias),
                        outer,
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND ")
        };

        Ok(format!(
            "LEFT JOIN (SELECT {} FROM (SELECT {} {inner_from}{inner_where}) AS {quoted_row_source_alias}{group_by}) AS {} ON {join_condition}",
            aggregate_selects.join(", "),
            row_selects.join(", "),
            quote_ident(&precomputed.table_alias),
        ))
    }
}

impl PercentileDiscAggregatePlan {
    fn aggregate_ref(&self, aggregate: &PercentileDiscAggregate) -> Option<String> {
        self.aggregates
            .iter()
            .find(|precomputed| precomputed.aggregate == *aggregate)
            .map(|precomputed| {
                format!(
                    "MAX({})",
                    qualified_ref(&precomputed.table_alias, &precomputed.value_alias)
                )
            })
    }
}

fn qualified_ref(alias: &str, column: &str) -> String {
    format!("{}.{}", quote_ident(alias), quote_ident(column))
}

fn render_null_safe_correlation(inner: &str, outer: &str) -> String {
    format!("(({inner} = {outer}) OR ({inner} IS NULL AND {outer} IS NULL))")
}

fn render_percentile_disc_literal(value: f64) -> String {
    let rendered = value.to_string();
    if rendered.contains('.') || rendered.contains('e') || rendered.contains('E') {
        rendered
    } else {
        format!("{rendered}.0")
    }
}

fn scalar_expression_projects_duration(expression: &ScalarExpression) -> bool {
    match expression {
        ScalarExpression::Temporal(
            TemporalExpr::MakeDuration { .. } | TemporalExpr::DurationInUnits { .. },
        ) => true,
        ScalarExpression::Arithmetic {
            operator,
            left,
            right,
        } => arithmetic_expression_projects_duration(*operator, left, right),
        _ => false,
    }
}

fn arithmetic_expression_projects_duration(
    operator: ArithmeticOperator,
    left: &ScalarExpression,
    right: &ScalarExpression,
) -> bool {
    match operator {
        ArithmeticOperator::Add | ArithmeticOperator::Subtract => {
            scalar_expression_projects_duration(left) && scalar_expression_projects_duration(right)
        }
        ArithmeticOperator::Multiply => scalar_expression_projects_duration(left),
        ArithmeticOperator::Divide | ArithmeticOperator::Modulo | ArithmeticOperator::Power => {
            false
        }
    }
}
