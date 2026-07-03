//! Precomputed scalar-subquery lowering for the SQL Lowerer: discovers EXISTS/COUNT/COLLECT
//! scalar-subquery candidates across projections, predicates and scalar expressions, then
//! renders them as precomputed LEFT JOIN subqueries — correlated and uncorrelated, over node
//! or relationship patterns, including DISTINCT-count targets — with their correlation
//! conditions. Populates the Lowerer's `precomputed_scalar_subqueries` set.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "SQL scalar subquery precomputation helpers are split into a child module while preserving parent-private access."
)]
use super::*;

impl<'a> Lowerer<'a> {
    pub(super) fn join_precomputed_scalar_subqueries(&mut self) -> Result<(), CoreError> {
        let candidates = self.scalar_subquery_candidates();
        if candidates.is_empty() {
            return Ok(());
        }

        let mut unsupported = 0usize;
        for candidate_use in candidates {
            let candidate = candidate_use.candidate;
            let required = candidate_use.required;
            if self
                .precomputed_scalar_subqueries
                .iter()
                .any(|precomputed| precomputed.candidate == candidate)
            {
                continue;
            }
            let index = self.precomputed_scalar_subqueries.len();
            let precomputed = PrecomputedScalarSubquery {
                candidate,
                table_alias: format!("__coral_scalar_subquery_{index}"),
                outer_key_alias: "__coral_outer_key".to_string(),
                value_alias: "__coral_value".to_string(),
            };
            let Some(join_sql) = self.render_precomputed_scalar_subquery_join(&precomputed)? else {
                if required {
                    return Err(CoreError::InvalidInput(
                        "hidden ORDER BY over correlated scalar subqueries requires a precomputable single-anchor relationship or node pattern"
                            .to_string(),
                    ));
                }
                unsupported += 1;
                continue;
            };
            write!(self.from_clause, " {join_sql}")
                .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
            self.precomputed_scalar_subqueries.push(precomputed);
        }

        if unsupported > 1 {
            return Err(CoreError::InvalidInput(
                "multiple correlated scalar subqueries in one projection require relationship-pattern COUNT { ... } / EXISTS { MATCH ... } subqueries with a single outer node anchor or node-only COUNT/EXISTS subqueries with one equality correlation"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn scalar_subquery_candidates(&self) -> Vec<ScalarSubqueryCandidateUse> {
        let mut candidates = Vec::new();
        for projection in &self.validated.plan().projections {
            if let Projection::Expression { expression, .. } = projection {
                self.collect_scalar_expression_subquery_candidates(
                    expression,
                    false,
                    &mut candidates,
                );
            }
        }
        if let Some(predicate) = &self.validated.plan().predicate {
            self.collect_predicate_expression_subquery_candidates(
                predicate,
                false,
                &mut candidates,
            );
        }
        for order_key in &self.validated.plan().order_by {
            if let OrderExpression::Scalar(expression) = &order_key.expression {
                self.collect_scalar_expression_subquery_candidates(
                    expression,
                    true,
                    &mut candidates,
                );
            }
        }
        candidates
    }

    fn collect_scalar_expression_subquery_candidates(
        &self,
        expression: &ScalarExpression,
        required: bool,
        candidates: &mut Vec<ScalarSubqueryCandidateUse>,
    ) {
        if let Some(expression) = scalar_expression_unary_operand(expression) {
            self.collect_scalar_expression_subquery_candidates(expression, required, candidates);
            return;
        }

        match expression {
            ScalarExpression::Predicate(predicate) => {
                self.collect_predicate_expression_subquery_candidates(
                    predicate, required, candidates,
                );
            }
            ScalarExpression::CountSubquery {
                pattern,
                distinct_target,
            } => Self::collect_count_subquery_candidate(
                pattern,
                distinct_target.as_deref(),
                required,
                candidates,
            ),
            ScalarExpression::CollectSubquery {
                pattern,
                target,
                distinct,
            } => {
                Self::collect_collect_subquery_candidate(
                    pattern, target, *distinct, required, candidates,
                );
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
            | ScalarExpression::Key { .. }
            | ScalarExpression::ElementId { .. }
            | ScalarExpression::GraphIdentity { .. }
            | ScalarExpression::GraphPresence { .. }
            | ScalarExpression::GraphKeyList { .. }
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
                unreachable!("unary scalar expressions handled before candidate collection")
            }
            _ => self.collect_structural_scalar_expression_subquery_candidates(
                expression, required, candidates,
            ),
        }
    }

    fn collect_count_subquery_candidate(
        pattern: &CountSubqueryPattern,
        distinct_target: Option<&ScalarExpression>,
        required: bool,
        candidates: &mut Vec<ScalarSubqueryCandidateUse>,
    ) {
        let should_precompute = match pattern {
            CountSubqueryPattern::Relationships(predicate) => {
                required || predicate.references_outer_variables()
            }
            CountSubqueryPattern::Nodes { .. } => required || pattern.references_outer_variables(),
        };
        if should_precompute {
            candidates.push(ScalarSubqueryCandidateUse {
                candidate: ScalarSubqueryCandidate::Count {
                    pattern: pattern.clone(),
                    distinct_target: distinct_target.cloned(),
                },
                required,
            });
        }
    }

    fn collect_collect_subquery_candidate(
        pattern: &CountSubqueryPattern,
        target: &ScalarExpression,
        distinct: bool,
        required: bool,
        candidates: &mut Vec<ScalarSubqueryCandidateUse>,
    ) {
        if let CountSubqueryPattern::Relationships(predicate) = pattern
            && predicate.references_outer_variables()
        {
            candidates.push(ScalarSubqueryCandidateUse {
                candidate: ScalarSubqueryCandidate::Collect {
                    pattern: predicate.clone(),
                    target: target.clone(),
                    distinct,
                },
                required,
            });
        }
    }

    fn collect_structural_scalar_expression_subquery_candidates(
        &self,
        expression: &ScalarExpression,
        required: bool,
        candidates: &mut Vec<ScalarSubqueryCandidateUse>,
    ) {
        if let Some((left, right)) = Self::structural_scalar_binary_operands(expression) {
            self.collect_scalar_expression_subquery_candidates(left, required, candidates);
            self.collect_scalar_expression_subquery_candidates(right, required, candidates);
            return;
        }

        match expression {
            ScalarExpression::PresenceGated { expression, .. } => {
                self.collect_scalar_expression_subquery_candidates(
                    expression, required, candidates,
                );
            }
            ScalarExpression::Coalesce { expressions } => {
                for expression in expressions {
                    self.collect_scalar_expression_subquery_candidates(
                        expression, required, candidates,
                    );
                }
            }
            ScalarExpression::Round { expression, places } => {
                self.collect_scalar_expression_subquery_candidates(
                    expression, required, candidates,
                );
                if let Some(places) = places {
                    self.collect_scalar_expression_subquery_candidates(
                        places, required, candidates,
                    );
                }
            }
            ScalarExpression::LPad {
                expression,
                length,
                fill,
            }
            | ScalarExpression::RPad {
                expression,
                length,
                fill,
            } => {
                self.collect_scalar_expression_subquery_candidates(
                    expression, required, candidates,
                );
                self.collect_scalar_expression_subquery_candidates(length, required, candidates);
                self.collect_scalar_expression_subquery_candidates(fill, required, candidates);
            }
            ScalarExpression::Replace {
                expression,
                search,
                replacement,
            } => {
                self.collect_scalar_expression_subquery_candidates(
                    expression, required, candidates,
                );
                self.collect_scalar_expression_subquery_candidates(search, required, candidates);
                self.collect_scalar_expression_subquery_candidates(
                    replacement,
                    required,
                    candidates,
                );
            }
            ScalarExpression::Substring {
                expression,
                start,
                length,
            } => {
                self.collect_scalar_expression_subquery_candidates(
                    expression, required, candidates,
                );
                self.collect_scalar_expression_subquery_candidates(start, required, candidates);
                if let Some(length) = length {
                    self.collect_scalar_expression_subquery_candidates(
                        length, required, candidates,
                    );
                }
            }
            ScalarExpression::Case {
                alternatives,
                else_expression,
            } => {
                self.collect_case_scalar_expression_subquery_candidates(
                    alternatives,
                    else_expression.as_deref(),
                    required,
                    candidates,
                );
            }
            _ => {
                unreachable!("unary scalar expressions handled before candidate collection")
            }
        }
    }

    pub(super) fn structural_scalar_binary_operands(
        expression: &ScalarExpression,
    ) -> Option<(&ScalarExpression, &ScalarExpression)> {
        match expression {
            ScalarExpression::NullIf { expression, value } => Some((expression, value)),
            ScalarExpression::Left { expression, count }
            | ScalarExpression::Right { expression, count } => Some((expression, count)),
            ScalarExpression::StringIndices {
                expression,
                pattern,
            }
            | ScalarExpression::StringContains {
                expression,
                pattern,
            }
            | ScalarExpression::StringStartsWith {
                expression,
                pattern,
            }
            | ScalarExpression::StringEndsWith {
                expression,
                pattern,
            } => Some((expression, pattern)),
            ScalarExpression::Arithmetic { left, right, .. } => Some((left, right)),
            ScalarExpression::Atan2 { y, x } => Some((y, x)),
            _ => None,
        }
    }

    pub(super) fn structural_scalar_ternary_operands(
        expression: &ScalarExpression,
    ) -> Option<(&ScalarExpression, &ScalarExpression, &ScalarExpression)> {
        match expression {
            ScalarExpression::LPad {
                expression,
                length,
                fill,
            }
            | ScalarExpression::RPad {
                expression,
                length,
                fill,
            } => Some((expression, length, fill)),
            ScalarExpression::Replace {
                expression,
                search,
                replacement,
            } => Some((expression, search, replacement)),
            _ => None,
        }
    }

    fn collect_case_scalar_expression_subquery_candidates(
        &self,
        alternatives: &[ScalarCaseAlternative],
        else_expression: Option<&ScalarExpression>,
        required: bool,
        candidates: &mut Vec<ScalarSubqueryCandidateUse>,
    ) {
        for alternative in alternatives {
            self.collect_predicate_expression_subquery_candidates(
                &alternative.when,
                required,
                candidates,
            );
            self.collect_scalar_expression_subquery_candidates(
                &alternative.then,
                required,
                candidates,
            );
        }
        if let Some(else_expression) = else_expression {
            self.collect_scalar_expression_subquery_candidates(
                else_expression,
                required,
                candidates,
            );
        }
    }

    fn collect_predicate_expression_subquery_candidates(
        &self,
        predicate: &PredicateExpression,
        required: bool,
        candidates: &mut Vec<ScalarSubqueryCandidateUse>,
    ) {
        match predicate {
            PredicateExpression::ExistsPattern(predicate) => {
                if required || predicate.references_outer_variables() {
                    candidates.push(ScalarSubqueryCandidateUse {
                        candidate: ScalarSubqueryCandidate::Exists(predicate.clone()),
                        required,
                    });
                }
            }
            PredicateExpression::ScalarComparison(predicate) => {
                if !required && Self::scalar_predicate_renders_as_count_existence(predicate) {
                    return;
                }
                self.collect_scalar_expression_subquery_candidates(
                    &predicate.lhs,
                    required,
                    candidates,
                );
                if let ScalarPredicateRhs::Expression(expression) = &predicate.rhs {
                    self.collect_scalar_expression_subquery_candidates(
                        expression, required, candidates,
                    );
                }
            }
            PredicateExpression::And { left, right }
            | PredicateExpression::Or { left, right }
            | PredicateExpression::Xor { left, right } => {
                self.collect_predicate_expression_subquery_candidates(left, required, candidates);
                self.collect_predicate_expression_subquery_candidates(right, required, candidates);
            }
            PredicateExpression::Not { expression } => {
                self.collect_predicate_expression_subquery_candidates(
                    expression, required, candidates,
                );
            }
            PredicateExpression::Boolean(_)
            | PredicateExpression::Comparison(_)
            | PredicateExpression::KeyComparison(_)
            | PredicateExpression::ElementIdComparison(_)
            | PredicateExpression::Presence(_)
            | PredicateExpression::PropertyKeyMembership(_) => {}
        }
    }

    fn scalar_predicate_renders_as_count_existence(predicate: &ScalarPredicate) -> bool {
        let ScalarExpression::CountSubquery {
            distinct_target: None,
            ..
        } = &predicate.lhs
        else {
            return false;
        };
        Self::count_existence_predicate(predicate.operator, &predicate.rhs).is_some()
    }

    fn render_precomputed_scalar_subquery_join(
        &self,
        precomputed: &PrecomputedScalarSubquery,
    ) -> Result<Option<String>, CoreError> {
        match &precomputed.candidate {
            ScalarSubqueryCandidate::Count {
                pattern: CountSubqueryPattern::Relationships(predicate),
                ..
            } => {
                if predicate.references_outer_variables() {
                    self.render_precomputed_relationship_scalar_subquery_join(
                        predicate,
                        precomputed,
                    )
                } else {
                    self.render_precomputed_uncorrelated_relationship_scalar_subquery_join(
                        predicate,
                        precomputed,
                    )
                    .map(Some)
                }
            }
            ScalarSubqueryCandidate::Exists(predicate) => {
                if predicate.references_outer_variables() {
                    self.render_precomputed_exists_scalar_subquery_join(predicate, precomputed)
                } else {
                    self.render_precomputed_uncorrelated_relationship_scalar_subquery_join(
                        predicate,
                        precomputed,
                    )
                    .map(Some)
                }
            }
            ScalarSubqueryCandidate::Collect { pattern, .. } => {
                self.render_precomputed_relationship_scalar_subquery_join(pattern, precomputed)
            }
            ScalarSubqueryCandidate::Count {
                pattern: pattern @ CountSubqueryPattern::Nodes { .. },
                ..
            } => self.render_precomputed_node_count_scalar_subquery_join(pattern, precomputed),
        }
    }

    fn render_precomputed_uncorrelated_relationship_scalar_subquery_join(
        &self,
        predicate: &ExistsPatternPredicate,
        precomputed: &PrecomputedScalarSubquery,
    ) -> Result<String, CoreError> {
        let value_expression = match &precomputed.candidate {
            ScalarSubqueryCandidate::Exists(_) => "COUNT(*) > 0",
            ScalarSubqueryCandidate::Count {
                distinct_target: None,
                ..
            } => "COUNT(*)",
            ScalarSubqueryCandidate::Count {
                distinct_target: Some(target),
                ..
            } => {
                let value_expression =
                    self.render_count_distinct_scoped_pattern_select(predicate, target)?;
                let select_expression = format!(
                    "{value_expression} AS {}",
                    quote_ident(&precomputed.value_alias)
                );
                return Ok(format!(
                    "CROSS JOIN (SELECT {select_expression}) AS {}",
                    quote_ident(&precomputed.table_alias)
                ));
            }
            ScalarSubqueryCandidate::Collect { .. } => {
                return Err(CoreError::internal(
                    "uncorrelated collect subqueries are not precomputed",
                ));
            }
        };
        let select_expression = format!(
            "{value_expression} AS {}",
            quote_ident(&precomputed.value_alias)
        );
        Ok(format!(
            "CROSS JOIN {} AS {}",
            self.render_scoped_pattern_select(predicate, &select_expression)?,
            quote_ident(&precomputed.table_alias)
        ))
    }

    fn render_precomputed_node_count_scalar_subquery_join(
        &self,
        pattern: &CountSubqueryPattern,
        precomputed: &PrecomputedScalarSubquery,
    ) -> Result<Option<String>, CoreError> {
        let distinct_target = match &precomputed.candidate {
            ScalarSubqueryCandidate::Count {
                distinct_target, ..
            } => distinct_target.as_ref(),
            _ => None,
        };
        let CountSubqueryPattern::Nodes {
            nodes,
            predicates,
            predicate,
        } = pattern
        else {
            return Err(CoreError::internal(
                "precomputed node count renderer received a relationship pattern",
            ));
        };
        if pattern.references_outer_variables() {
            let local_aliases = Self::count_local_node_aliases(nodes);
            if let Some(target) = distinct_target {
                return self.render_precomputed_correlated_node_distinct_count_subquery_join(
                    nodes,
                    predicates,
                    predicate.as_deref(),
                    &local_aliases,
                    precomputed,
                    target,
                );
            }
            return self.render_precomputed_correlated_node_scalar_subquery_join(
                nodes,
                predicates,
                predicate.as_deref(),
                &local_aliases,
                precomputed,
                "COUNT(*)",
            );
        }
        if let Some(target) = distinct_target {
            let value_expression = self.render_count_distinct_node_subquery(
                nodes,
                predicates,
                predicate.as_deref(),
                target,
            )?;
            let select_expression = format!(
                "{value_expression} AS {}",
                quote_ident(&precomputed.value_alias)
            );
            return Ok(Some(format!(
                "CROSS JOIN (SELECT {select_expression}) AS {}",
                quote_ident(&precomputed.table_alias)
            )));
        }
        let select_expression = format!("COUNT(*) AS {}", quote_ident(&precomputed.value_alias));
        Ok(Some(format!(
            "CROSS JOIN {} AS {}",
            self.render_count_node_select(
                nodes,
                predicates,
                predicate.as_deref(),
                &select_expression,
            )?,
            quote_ident(&precomputed.table_alias)
        )))
    }

    fn render_precomputed_exists_scalar_subquery_join(
        &self,
        predicate: &ExistsPatternPredicate,
        precomputed: &PrecomputedScalarSubquery,
    ) -> Result<Option<String>, CoreError> {
        let local_nodes = self.exists_local_node_map(predicate)?;
        if self
            .exists_relationship_bindings(predicate, &local_nodes)?
            .is_empty()
        {
            let local_aliases = Self::exists_local_node_aliases(predicate);
            return self.render_precomputed_correlated_node_scalar_subquery_join(
                &predicate.nodes,
                &predicate.predicates,
                predicate.predicate.as_deref(),
                &local_aliases,
                precomputed,
                "COUNT(*) > 0",
            );
        }
        self.render_precomputed_relationship_scalar_subquery_join(predicate, precomputed)
    }

    fn render_precomputed_correlated_node_distinct_count_subquery_join(
        &self,
        nodes: &[NodePattern],
        predicates: &[PropertyPredicate],
        predicate: Option<&PredicateExpression>,
        local_aliases: &BTreeMap<&str, String>,
        precomputed: &PrecomputedScalarSubquery,
        target: &ScalarExpression,
    ) -> Result<Option<String>, CoreError> {
        let local_nodes = self.scoped_local_node_map(nodes)?;
        let Some(correlation) =
            self.precomputed_node_correlation(predicates, predicate, &local_nodes, local_aliases)?
        else {
            return Ok(None);
        };
        let relationship_bindings = Vec::new();
        if !Self::scoped_scalar_expression_is_inner(target, &relationship_bindings, &local_nodes) {
            return Ok(None);
        }

        let mut conditions = Vec::with_capacity(
            predicates
                .len()
                .saturating_sub(1)
                .saturating_add(usize::from(predicate.is_some())),
        );
        for (index, property_predicate) in predicates.iter().enumerate() {
            if index == correlation.predicate_index {
                continue;
            }
            conditions.push(self.render_exists_property_predicate(
                property_predicate,
                &relationship_bindings,
                &local_nodes,
                local_aliases,
            )?);
        }
        if let Some(predicate) = predicate {
            conditions.push(self.render_scoped_predicate_expression(
                predicate,
                &relationship_bindings,
                &local_nodes,
                local_aliases,
            )?);
        }
        let target_sql = self.render_scoped_scalar_expression(
            target,
            &relationship_bindings,
            &local_nodes,
            local_aliases,
        )?;
        let from_clause =
            Self::render_precomputed_node_from_clause(nodes, &local_nodes, local_aliases)?;
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };
        let outer_key_alias = quote_ident(&precomputed.outer_key_alias);
        let value_alias = quote_ident(&precomputed.value_alias);
        let distinct_alias = quote_ident("__coral_count_distinct");
        let distinct_value_alias = quote_ident("__coral_count_value");
        let distinct_rows = format!(
            "SELECT DISTINCT {} AS {outer_key_alias}, {target_sql} AS {distinct_value_alias} FROM {from_clause}{where_clause}",
            correlation.local_expression
        );
        let subquery = format!(
            "SELECT {outer_key_alias}, COUNT(*) AS {value_alias} FROM ({distinct_rows}) AS {distinct_alias} GROUP BY {outer_key_alias}"
        );
        Ok(Some(format!(
            "LEFT JOIN ({subquery}) AS {} ON {}.{} = {}",
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.table_alias),
            outer_key_alias,
            correlation.outer_expression
        )))
    }

    fn render_precomputed_correlated_node_scalar_subquery_join(
        &self,
        nodes: &[NodePattern],
        predicates: &[PropertyPredicate],
        predicate: Option<&PredicateExpression>,
        local_aliases: &BTreeMap<&str, String>,
        precomputed: &PrecomputedScalarSubquery,
        value_expression: &str,
    ) -> Result<Option<String>, CoreError> {
        let local_nodes = self.scoped_local_node_map(nodes)?;
        let Some(correlation) =
            self.precomputed_node_correlation(predicates, predicate, &local_nodes, local_aliases)?
        else {
            return Ok(None);
        };

        let relationship_bindings = Vec::new();
        let mut conditions = Vec::with_capacity(
            predicates
                .len()
                .saturating_sub(1)
                .saturating_add(usize::from(predicate.is_some())),
        );
        for (index, property_predicate) in predicates.iter().enumerate() {
            if index == correlation.predicate_index {
                continue;
            }
            conditions.push(self.render_exists_property_predicate(
                property_predicate,
                &relationship_bindings,
                &local_nodes,
                local_aliases,
            )?);
        }
        if let Some(predicate) = predicate {
            conditions.push(self.render_scoped_predicate_expression(
                predicate,
                &relationship_bindings,
                &local_nodes,
                local_aliases,
            )?);
        }
        let from_clause =
            Self::render_precomputed_node_from_clause(nodes, &local_nodes, local_aliases)?;
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };
        let subquery = format!(
            "SELECT {} AS {}, {value_expression} AS {} FROM {from_clause}{where_clause} GROUP BY {}",
            correlation.local_expression,
            quote_ident(&precomputed.outer_key_alias),
            quote_ident(&precomputed.value_alias),
            correlation.local_expression
        );
        Ok(Some(format!(
            "LEFT JOIN ({subquery}) AS {} ON {}.{} = {}",
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.outer_key_alias),
            correlation.outer_expression
        )))
    }

    fn precomputed_node_correlation<'b>(
        &self,
        predicates: &[PropertyPredicate],
        predicate: Option<&PredicateExpression>,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<Option<PrecomputedNodeCorrelation>, CoreError> {
        let relationship_bindings = Vec::new();
        if predicate.is_some_and(|predicate| {
            !Self::scoped_predicate_expression_is_inner(
                predicate,
                &relationship_bindings,
                local_nodes,
            )
        }) {
            return Ok(None);
        }

        let mut correlation = None;
        for (index, property_predicate) in predicates.iter().enumerate() {
            if let Some(candidate) = self.precomputed_node_property_correlation(
                index,
                property_predicate,
                local_nodes,
                local_aliases,
            )? {
                if correlation.is_some() {
                    return Ok(None);
                }
                correlation = Some(candidate);
                continue;
            }
            if !Self::scoped_property_predicate_is_inner(
                property_predicate,
                &relationship_bindings,
                local_nodes,
            ) {
                return Ok(None);
            }
        }
        Ok(correlation)
    }

    fn precomputed_node_property_correlation<'b>(
        &self,
        predicate_index: usize,
        predicate: &PropertyPredicate,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<Option<PrecomputedNodeCorrelation>, CoreError> {
        if predicate.operator != ComparisonOperator::Equal {
            return Ok(None);
        }
        let lhs = self.node_property_correlation_operand(
            &predicate.property,
            local_nodes,
            local_aliases,
        )?;
        let Some(rhs) =
            self.node_rhs_correlation_operand(&predicate.rhs, local_nodes, local_aliases)?
        else {
            return Ok(None);
        };
        Ok(match (lhs, rhs) {
            (
                NodeCorrelationOperand::Local(local_expression),
                NodeCorrelationOperand::Outer(outer_expression),
            )
            | (
                NodeCorrelationOperand::Outer(outer_expression),
                NodeCorrelationOperand::Local(local_expression),
            ) => Some(PrecomputedNodeCorrelation {
                predicate_index,
                local_expression,
                outer_expression,
            }),
            _ => None,
        })
    }

    fn node_rhs_correlation_operand<'b>(
        &self,
        rhs: &PredicateRhs,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<Option<NodeCorrelationOperand>, CoreError> {
        match rhs {
            PredicateRhs::Property(property) => self
                .node_property_correlation_operand(property, local_nodes, local_aliases)
                .map(Some),
            PredicateRhs::Key { variable } => self
                .node_key_correlation_operand(variable, local_nodes, local_aliases)
                .map(Some),
            PredicateRhs::ElementId { variable } => self
                .node_element_id_correlation_operand(variable, local_nodes, local_aliases)
                .map(Some),
            PredicateRhs::Literal(_) | PredicateRhs::List(_) => Ok(None),
        }
    }

    fn node_property_correlation_operand<'b>(
        &self,
        property: &PropertyRef,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<NodeCorrelationOperand, CoreError> {
        if local_nodes.contains_key(property.variable.as_str()) {
            let relationship_bindings = Vec::new();
            return Ok(NodeCorrelationOperand::Local(
                self.render_exists_property_ref(
                    property,
                    &relationship_bindings,
                    local_nodes,
                    local_aliases,
                )?,
            ));
        }
        let binding = self.validated.binding(&property.variable)?;
        if !matches!(binding.kind(), ValidatedBindingKind::Node(_)) {
            return Err(CoreError::InvalidInput(
                "hidden ORDER BY node precompute supports correlations to one outer node binding"
                    .to_string(),
            ));
        }
        Ok(NodeCorrelationOperand::Outer(
            self.render_property_ref(property)?,
        ))
    }

    fn node_key_correlation_operand<'b>(
        &self,
        variable: &str,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<NodeCorrelationOperand, CoreError> {
        if local_nodes.contains_key(variable) {
            let relationship_bindings = Vec::new();
            return Ok(NodeCorrelationOperand::Local(self.render_exists_key_ref(
                variable,
                &relationship_bindings,
                local_nodes,
                local_aliases,
            )?));
        }
        let binding = self.validated.binding(variable)?;
        if !matches!(binding.kind(), ValidatedBindingKind::Node(_)) {
            return Err(CoreError::InvalidInput(
                "hidden ORDER BY node precompute supports correlations to one outer node binding"
                    .to_string(),
            ));
        }
        Ok(NodeCorrelationOperand::Outer(
            self.render_binding_key_ref(variable)?,
        ))
    }

    fn node_element_id_correlation_operand<'b>(
        &self,
        variable: &str,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<NodeCorrelationOperand, CoreError> {
        let operand = self.node_key_correlation_operand(variable, local_nodes, local_aliases)?;
        Ok(match operand {
            NodeCorrelationOperand::Local(expression) => {
                NodeCorrelationOperand::Local(format!("CAST({expression} AS VARCHAR)"))
            }
            NodeCorrelationOperand::Outer(expression) => {
                NodeCorrelationOperand::Outer(format!("CAST({expression} AS VARCHAR)"))
            }
        })
    }

    fn render_precomputed_node_from_clause<'b>(
        nodes: &'b [NodePattern],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        let mut from_clause = String::new();
        for (index, node) in nodes.iter().enumerate() {
            let node_mapping = local_nodes.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal("validated precomputed node local mapping was missing")
            })?;
            let alias = local_aliases.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal("validated precomputed node local alias was missing")
            })?;
            if index > 0 {
                from_clause.push_str(" JOIN ");
            }
            write!(
                from_clause,
                "{} AS {}",
                render_table_ref(&node_mapping.table),
                quote_ident(alias)
            )
            .map_err(|_| CoreError::internal("failed to render precomputed node SQL"))?;
            if index > 0 {
                from_clause.push_str(" ON TRUE");
            }
        }
        Ok(from_clause)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Precomputed scalar subquery joins carry scoped bindings, predicates, and aggregate rendering in one SQL shape"
    )]
    fn render_precomputed_relationship_scalar_subquery_join(
        &self,
        predicate: &ExistsPatternPredicate,
        precomputed: &PrecomputedScalarSubquery,
    ) -> Result<Option<String>, CoreError> {
        let local_nodes = self.exists_local_node_map(predicate)?;
        let Some(outer_variable) = self.precomputed_outer_anchor(predicate, &local_nodes)? else {
            return Ok(None);
        };
        let relationship_bindings = self.exists_relationship_bindings(predicate, &local_nodes)?;
        if relationship_bindings.is_empty()
            || !Self::scoped_predicates_are_precomputable(
                predicate,
                &relationship_bindings,
                &local_nodes,
            )
        {
            return Ok(None);
        }
        let local_aliases = Self::exists_local_node_aliases(predicate);
        let collect_target_sql = match &precomputed.candidate {
            ScalarSubqueryCandidate::Collect { target, .. } => {
                if !Self::scoped_scalar_expression_is_inner(
                    target,
                    &relationship_bindings,
                    &local_nodes,
                ) {
                    return Ok(None);
                }
                Some(self.render_scoped_scalar_expression(
                    target,
                    &relationship_bindings,
                    &local_nodes,
                    &local_aliases,
                )?)
            }
            _ => None,
        };
        let Some((outer_key_ref, mut conditions)) = self
            .render_precomputed_relationship_conditions(
                &relationship_bindings,
                &local_nodes,
                &local_aliases,
                &outer_variable,
            )?
        else {
            return Ok(None);
        };
        conditions.extend(self.render_scoped_conditions(
            &predicate.predicates,
            predicate.predicate.as_deref(),
            &relationship_bindings,
            &local_nodes,
            &local_aliases,
        )?);
        let from_clause = Self::render_precomputed_relationship_from_clause(
            predicate,
            &relationship_bindings,
            &local_nodes,
            &local_aliases,
            "precomputed scalar",
        )?;
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };
        let value_expression = match &precomputed.candidate {
            ScalarSubqueryCandidate::Exists(_) => "COUNT(*) > 0".to_string(),
            ScalarSubqueryCandidate::Count {
                distinct_target: None,
                ..
            } => "COUNT(*)".to_string(),
            ScalarSubqueryCandidate::Count {
                distinct_target: Some(target),
                ..
            } => {
                let Some(target_sql) = self.render_precomputed_relationship_distinct_count_target(
                    target,
                    &relationship_bindings,
                    &local_nodes,
                    &local_aliases,
                )?
                else {
                    return Ok(None);
                };
                return self
                    .render_precomputed_relationship_distinct_count_join(
                        precomputed,
                        &outer_variable,
                        &outer_key_ref,
                        &from_clause,
                        &where_clause,
                        &target_sql,
                    )
                    .map(Some);
            }
            ScalarSubqueryCandidate::Collect { distinct, .. } => {
                let target_sql = collect_target_sql.as_deref().ok_or_else(|| {
                    CoreError::internal("precomputed collect target SQL was not rendered")
                })?;
                Self::render_collect_target_select_expression(target_sql, *distinct)
            }
        };
        let subquery = format!(
            "SELECT {outer_key_ref} AS {}, {value_expression} AS {} FROM {from_clause}{where_clause} GROUP BY {outer_key_ref}",
            quote_ident(&precomputed.outer_key_alias),
            quote_ident(&precomputed.value_alias)
        );
        Ok(Some(format!(
            "LEFT JOIN ({subquery}) AS {} ON {}.{} = {}",
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.outer_key_alias),
            self.render_binding_key_ref(&outer_variable)?
        )))
    }

    fn render_precomputed_relationship_distinct_count_target<'b>(
        &self,
        target: &ScalarExpression,
        relationship_bindings: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<Option<String>, CoreError> {
        if !Self::scoped_scalar_expression_is_inner(target, relationship_bindings, local_nodes) {
            return Ok(None);
        }
        self.render_scoped_scalar_expression(
            target,
            relationship_bindings,
            local_nodes,
            local_aliases,
        )
        .map(Some)
    }

    fn render_precomputed_relationship_distinct_count_join(
        &self,
        precomputed: &PrecomputedScalarSubquery,
        outer_variable: &str,
        outer_key_ref: &str,
        from_clause: &str,
        where_clause: &str,
        target_sql: &str,
    ) -> Result<String, CoreError> {
        let outer_key_alias = quote_ident(&precomputed.outer_key_alias);
        let value_alias = quote_ident(&precomputed.value_alias);
        let distinct_alias = quote_ident("__coral_count_distinct");
        let distinct_value_alias = quote_ident("__coral_count_value");
        let distinct_rows = format!(
            "SELECT DISTINCT {outer_key_ref} AS {outer_key_alias}, {target_sql} AS {distinct_value_alias} FROM {from_clause}{where_clause}"
        );
        let subquery = format!(
            "SELECT {outer_key_alias}, COUNT(*) AS {value_alias} FROM ({distinct_rows}) AS {distinct_alias} GROUP BY {outer_key_alias}"
        );
        Ok(format!(
            "LEFT JOIN ({subquery}) AS {} ON {}.{} = {}",
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.table_alias),
            outer_key_alias,
            self.render_binding_key_ref(outer_variable)?
        ))
    }

    fn precomputed_outer_anchor<'b>(
        &self,
        predicate: &'b ExistsPatternPredicate,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> Result<Option<String>, CoreError> {
        let mut outer_counts = BTreeMap::<&str, usize>::new();
        for relationship in &predicate.relationships {
            for variable in [relationship.left.as_str(), relationship.right.as_str()] {
                if !local_nodes.contains_key(variable) {
                    *outer_counts.entry(variable).or_default() += 1;
                }
            }
        }
        let mut outer_counts = outer_counts.iter();
        let Some((&outer_variable, &occurrence_count)) = outer_counts.next() else {
            return Ok(None);
        };
        if outer_counts.next().is_some() || occurrence_count != 1 {
            return Ok(None);
        }
        let binding = self.validated.binding(outer_variable)?;
        if !matches!(binding.kind(), ValidatedBindingKind::Node(_)) {
            return Ok(None);
        }
        Ok(Some(outer_variable.to_string()))
    }

    pub(super) fn render_precomputed_relationship_from_clause<'b>(
        predicate: &'b ExistsPatternPredicate,
        relationship_bindings: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
        label: &str,
    ) -> Result<String, CoreError> {
        let mut from_clause = relationship_bindings
            .iter()
            .enumerate()
            .map(|(index, binding)| {
                let table_ref = format!(
                    "{} AS {}",
                    render_table_ref(&binding.relationship.table),
                    quote_ident(&binding.alias)
                );
                if index == 0 {
                    table_ref
                } else {
                    format!("JOIN {table_ref} ON TRUE")
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        for node in &predicate.nodes {
            let node_mapping = local_nodes.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal(format!("validated {label} local node mapping was missing"))
            })?;
            let alias = local_aliases.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal(format!("validated {label} local node alias was missing"))
            })?;
            write!(
                from_clause,
                " JOIN {} AS {} ON TRUE",
                render_table_ref(&node_mapping.table),
                quote_ident(alias)
            )
            .map_err(|_| CoreError::internal(format!("failed to render {label} pattern SQL")))?;
        }
        Ok(from_clause)
    }

    fn render_precomputed_relationship_conditions<'b>(
        &self,
        relationship_bindings: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
        outer_variable: &str,
    ) -> Result<Option<(String, Vec<String>)>, CoreError> {
        let mut outer_key_ref = None;
        let mut conditions = Vec::with_capacity(relationship_bindings.len());
        for binding in relationship_bindings {
            let left_is_outer = binding.pattern.left == outer_variable;
            let right_is_outer = binding.pattern.right == outer_variable;
            if left_is_outer && right_is_outer {
                return Ok(None);
            }
            if !left_is_outer && !right_is_outer {
                conditions.push(self.exists_relationship_condition(
                    binding.pattern,
                    binding.relationship,
                    &binding.alias,
                    local_nodes,
                    local_aliases,
                )?);
                continue;
            }

            let left_node = self.exists_node_mapping(local_nodes, &binding.pattern.left)?;
            let right_node = self.exists_node_mapping(local_nodes, &binding.pattern.right)?;
            let orientations = Self::relationship_orientations_for_labels(
                binding.relationship,
                binding.pattern.direction,
                &left_node.label,
                &right_node.label,
            )?;
            let inner_variable = if left_is_outer {
                binding.pattern.right.as_str()
            } else {
                binding.pattern.left.as_str()
            };
            if !local_nodes.contains_key(inner_variable) {
                return Ok(None);
            }
            let inner_node = local_nodes.get(inner_variable).ok_or_else(|| {
                CoreError::internal("validated precomputed scalar local node mapping was missing")
            })?;
            let inner_alias = local_aliases.get(inner_variable).ok_or_else(|| {
                CoreError::internal("validated precomputed scalar local node alias was missing")
            })?;
            let Some((current_outer_key_ref, condition)) =
                Self::precomputed_outer_key_and_inner_condition(
                    &binding.alias,
                    &orientations,
                    left_is_outer,
                    inner_alias,
                    &inner_node.key,
                )?
            else {
                return Ok(None);
            };
            outer_key_ref = Some(current_outer_key_ref);
            conditions.push(condition);
        }
        Ok(outer_key_ref.map(|outer_key_ref| (outer_key_ref, conditions)))
    }

    fn precomputed_outer_key_and_inner_condition(
        relationship_alias: &str,
        orientations: &[RelationshipOrientation],
        left_is_outer: bool,
        inner_alias: &str,
        inner_key: &str,
    ) -> Result<Option<(String, String)>, CoreError> {
        if orientations.is_empty() {
            return Ok(None);
        }

        let mut branches = Vec::with_capacity(orientations.len());
        let mut conditions = Vec::with_capacity(orientations.len());
        for orientation in orientations {
            let (outer_relationship_key, inner_relationship_key) = if left_is_outer {
                (
                    orientation.left_relationship_key.as_str(),
                    orientation.right_relationship_key.as_str(),
                )
            } else {
                (
                    orientation.right_relationship_key.as_str(),
                    orientation.left_relationship_key.as_str(),
                )
            };
            let outer_ref = format!(
                "{}.{}",
                quote_ident(relationship_alias),
                quote_ident(outer_relationship_key)
            );
            let inner_condition = format!(
                "{}.{} = {}.{}",
                quote_ident(relationship_alias),
                quote_ident(inner_relationship_key),
                quote_ident(inner_alias),
                quote_ident(inner_key)
            );
            branches.push((inner_condition.clone(), outer_ref));
            conditions.push(inner_condition);
        }

        let outer_key_ref = if let [(condition, outer_ref)] = branches.as_slice() {
            let _ = condition;
            outer_ref.clone()
        } else {
            let when_clauses = branches
                .iter()
                .map(|(condition, outer_ref)| format!("WHEN {condition} THEN {outer_ref}"))
                .collect::<Vec<_>>()
                .join(" ");
            format!("CASE {when_clauses} ELSE NULL END")
        };
        Ok(Some((
            outer_key_ref,
            Self::render_condition_disjunction(&conditions)?,
        )))
    }
}
