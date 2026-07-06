//! Predicate rendering for the SQL `SqlRenderer`: emits `WHERE` (pre-projection) and `HAVING`
//! (post-projection) SQL from graph-plan predicate trees — property, scalar, key,
//! element-id, presence, property-key-membership and EXISTS-pattern comparison leaves,
//! their boolean-expression walkers, and the right-hand-side operand rendering.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "SQL predicate renderers are split into a child module while preserving parent-private access."
)]
use super::*;

impl<'a> SqlRenderer<'a> {
    pub(super) fn render_where(&self) -> Result<String, CoreError> {
        let mut predicates = self.render_pre_projection_predicates()?;
        if !self.plan_has_aggregation()
            && let Some(predicate) = &self.validated.plan().post_projection_predicate
        {
            predicates.push(self.render_projection_predicate_expression(predicate)?);
        }
        if predicates.is_empty() {
            return Ok(String::new());
        }
        Ok(format!(" WHERE {}", predicates.join(" AND ")))
    }

    pub(super) fn render_pre_projection_predicates(&self) -> Result<Vec<String>, CoreError> {
        let mut predicates = Vec::with_capacity(
            self.validated.plan().predicates.len()
                + usize::from(self.validated.plan().predicate.is_some()),
        );
        for predicate in &self.validated.plan().predicates {
            predicates.push(self.render_predicate(predicate)?);
        }
        if let Some(predicate) = &self.validated.plan().predicate {
            predicates.push(self.render_predicate_expression(predicate)?);
        }
        Ok(predicates)
    }

    pub(super) fn render_having(&self) -> Result<String, CoreError> {
        if !self.plan_has_aggregation() {
            return Ok(String::new());
        }
        let Some(predicate) = &self.validated.plan().post_projection_predicate else {
            return Ok(String::new());
        };
        Ok(format!(
            " HAVING {}",
            self.render_projection_predicate_expression(predicate)?
        ))
    }

    pub(super) fn render_predicate_expression(
        &self,
        predicate: &PredicateExpression,
    ) -> Result<String, CoreError> {
        self.render_predicate_expression_in_scope(predicate, ScalarScope::TopLevel)
    }

    pub(super) fn render_scoped_predicate_expression<'b>(
        &self,
        predicate: &PredicateExpression,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        self.render_predicate_expression_in_scope(
            predicate,
            ScalarScope::Scoped {
                relationships,
                local_nodes,
                local_aliases,
            },
        )
    }

    fn render_predicate_expression_in_scope<'b, 'c>(
        &self,
        predicate: &PredicateExpression,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        match predicate {
            PredicateExpression::Boolean(value) => Ok(value.to_string().to_uppercase()),
            PredicateExpression::Comparison(predicate) => {
                self.render_property_predicate_in_scope(predicate, scope)
            }
            PredicateExpression::KeyComparison(predicate) => {
                self.render_key_predicate_in_scope(predicate, scope)
            }
            PredicateExpression::ElementIdComparison(predicate) => {
                self.render_element_id_predicate_in_scope(predicate, scope)
            }
            PredicateExpression::Presence(predicate) => {
                self.render_presence_predicate_in_scope(predicate, scope)
            }
            PredicateExpression::PropertyKeyMembership(predicate) => {
                self.render_property_key_membership_predicate_in_scope(predicate, scope)
            }
            PredicateExpression::ExistsPattern(predicate) => match scope {
                ScalarScope::TopLevel => self.render_exists_pattern_predicate(predicate),
                ScalarScope::Scoped {
                    relationships,
                    local_nodes,
                    local_aliases,
                } => self.render_nested_scoped_exists_pattern_predicate(
                    predicate,
                    relationships,
                    local_nodes,
                    local_aliases,
                ),
            },
            PredicateExpression::ScalarComparison(predicate) => {
                self.render_scalar_predicate_in_scope(predicate, scope)
            }
            PredicateExpression::And { left, right } => Ok(format!(
                "({} AND {})",
                self.render_predicate_expression_in_scope(left, scope)?,
                self.render_predicate_expression_in_scope(right, scope)?
            )),
            PredicateExpression::Or { left, right } => Ok(format!(
                "({} OR {})",
                self.render_predicate_expression_in_scope(left, scope)?,
                self.render_predicate_expression_in_scope(right, scope)?
            )),
            PredicateExpression::Xor { left, right } => {
                let left = self.render_predicate_expression_in_scope(left, scope)?;
                let right = self.render_predicate_expression_in_scope(right, scope)?;
                Ok(render_xor_predicate(&left, &right))
            }
            PredicateExpression::Not { expression } => Ok(format!(
                "NOT ({})",
                self.render_predicate_expression_in_scope(expression, scope)?
            )),
        }
    }

    pub(super) fn render_scalar_predicate_expression(
        &self,
        predicate: &PredicateExpression,
    ) -> Result<String, CoreError> {
        match predicate {
            PredicateExpression::ExistsPattern(predicate) => {
                if let Some(rendered) = self.render_precomputed_exists_pattern_ref(predicate) {
                    return Ok(rendered);
                }
                let alias = self.next_scalar_subquery_alias("__coral_exists_count");
                Ok(format!(
                    "{} > 0",
                    self.render_scoped_pattern_select(predicate, &format!("COUNT(*) AS {alias}"))?
                ))
            }
            PredicateExpression::And { left, right } => Ok(format!(
                "({} AND {})",
                self.render_scalar_predicate_expression(left)?,
                self.render_scalar_predicate_expression(right)?
            )),
            PredicateExpression::Or { left, right } => Ok(format!(
                "({} OR {})",
                self.render_scalar_predicate_expression(left)?,
                self.render_scalar_predicate_expression(right)?
            )),
            PredicateExpression::Xor { left, right } => {
                let left = self.render_scalar_predicate_expression(left)?;
                let right = self.render_scalar_predicate_expression(right)?;
                Ok(render_xor_predicate(&left, &right))
            }
            PredicateExpression::Not { expression } => Ok(format!(
                "NOT ({})",
                self.render_scalar_predicate_expression(expression)?
            )),
            PredicateExpression::Boolean(_)
            | PredicateExpression::Comparison(_)
            | PredicateExpression::KeyComparison(_)
            | PredicateExpression::ElementIdComparison(_)
            | PredicateExpression::Presence(_)
            | PredicateExpression::PropertyKeyMembership(_)
            | PredicateExpression::ScalarComparison(_) => {
                self.render_predicate_expression(predicate)
            }
        }
    }

    fn render_projection_predicate_expression(
        &self,
        predicate: &ProjectionPredicateExpression,
    ) -> Result<String, CoreError> {
        match predicate {
            ProjectionPredicateExpression::Boolean(value) => Ok(value.to_string().to_uppercase()),
            ProjectionPredicateExpression::Comparison(predicate) => {
                self.render_projection_predicate(predicate)
            }
            ProjectionPredicateExpression::And { left, right } => Ok(format!(
                "({} AND {})",
                self.render_projection_predicate_expression(left)?,
                self.render_projection_predicate_expression(right)?
            )),
            ProjectionPredicateExpression::Or { left, right } => Ok(format!(
                "({} OR {})",
                self.render_projection_predicate_expression(left)?,
                self.render_projection_predicate_expression(right)?
            )),
            ProjectionPredicateExpression::Xor { left, right } => {
                let left = self.render_projection_predicate_expression(left)?;
                let right = self.render_projection_predicate_expression(right)?;
                Ok(render_xor_predicate(&left, &right))
            }
            ProjectionPredicateExpression::Not { expression } => Ok(format!(
                "NOT ({})",
                self.render_projection_predicate_expression(expression)?
            )),
        }
    }

    fn render_projection_predicate(
        &self,
        predicate: &ProjectionPredicate,
    ) -> Result<String, CoreError> {
        let alias = self.render_projection_alias_ref(&predicate.alias)?;
        match (&predicate.operator, &predicate.rhs) {
            (ComparisonOperator::In, ProjectionPredicateRhs::List(literals)) => {
                if literals.is_empty() {
                    return Ok("FALSE".to_string());
                }
                let rendered = literals
                    .iter()
                    .map(render_literal)
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("{alias} IN ({rendered})"))
            }
            (ComparisonOperator::In, _) => Err(CoreError::internal(
                "validated projected IN predicate did not contain a literal list",
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                ProjectionPredicateRhs::Literal(Literal::String(value)),
            ) => {
                let operator = StringMatchOperator::from_comparison(predicate.operator)
                    .ok_or_else(|| {
                        CoreError::internal(
                            "validated projected string predicate used a non-string operator",
                        )
                    })?;
                Ok(format!(
                    "{alias} LIKE {} ESCAPE '\\'",
                    render_like_pattern(operator, value)
                ))
            }
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                _,
            ) => Err(CoreError::internal(
                "validated projected string predicate did not contain a string literal",
            )),
            (
                ComparisonOperator::RegexMatch,
                ProjectionPredicateRhs::Literal(Literal::String(value)),
            ) => Ok(render_regex_predicate(&alias, &quote_string_literal(value))),
            (ComparisonOperator::RegexMatch, _) => Err(CoreError::internal(
                "validated projected regex predicate did not contain a string literal",
            )),
            (ComparisonOperator::Equal, ProjectionPredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{alias} IS NULL"))
            }
            (ComparisonOperator::NotEqual, ProjectionPredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{alias} IS NOT NULL"))
            }
            (
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual,
                ProjectionPredicateRhs::Literal(Literal::Null),
            ) => Err(CoreError::internal(
                "validated projected predicate contained an invalid null comparison",
            )),
            _ => Ok(format!(
                "{alias} {} {}",
                render_operator(predicate.operator),
                self.render_projection_predicate_rhs(&predicate.rhs)?
            )),
        }
    }

    fn render_predicate(&self, predicate: &PropertyPredicate) -> Result<String, CoreError> {
        self.render_property_predicate_in_scope(predicate, ScalarScope::TopLevel)
    }

    fn render_property_predicate_in_scope<'b, 'c>(
        &self,
        predicate: &PropertyPredicate,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        let (property, context) = match scope {
            ScalarScope::TopLevel => (
                self.render_property_ref(&predicate.property)?,
                SimplePredicateContext::Graph,
            ),
            ScalarScope::Scoped {
                relationships,
                local_nodes,
                local_aliases,
            } => (
                self.render_exists_property_ref(
                    &predicate.property,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?,
                SimplePredicateContext::Exists,
            ),
        };
        self.render_simple_predicate_in_scope(
            &property,
            predicate.operator,
            &predicate.rhs,
            Some(&predicate.property),
            scope,
            context,
        )
    }

    fn render_scalar_predicate_in_scope<'b, 'c>(
        &self,
        predicate: &ScalarPredicate,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        if let Some(rendered) =
            self.try_render_count_existence_predicate_in_scope(predicate, scope)?
        {
            return Ok(rendered);
        }

        let context = ScalarPredicateContext::from_scope(scope);
        let lhs = self.render_scalar_expression_in_scope(&predicate.lhs, scope)?;
        match (&predicate.operator, &predicate.rhs) {
            (ComparisonOperator::In, ScalarPredicateRhs::List(literals)) => {
                if literals.is_empty() {
                    return Ok("FALSE".to_string());
                }
                let rendered = literals
                    .iter()
                    .map(render_literal)
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("{lhs} IN ({rendered})"))
            }
            (ComparisonOperator::In, _) => Err(CoreError::internal(context.in_list_error())),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(value))),
            ) if matches!(scope, ScalarScope::TopLevel) => {
                let operator = StringMatchOperator::from_comparison(predicate.operator)
                    .ok_or_else(|| CoreError::internal(context.string_operator_error()))?;
                Ok(format!(
                    "{lhs} LIKE {} ESCAPE '\\'",
                    render_like_pattern(operator, value)
                ))
            }
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                ScalarPredicateRhs::Expression(expression),
            ) => {
                let operator = StringMatchOperator::from_comparison(predicate.operator)
                    .ok_or_else(|| CoreError::internal(context.string_operator_error()))?;
                let rhs = self.render_scalar_expression_in_scope(expression, scope)?;
                Ok(render_string_function_predicate(operator, &lhs, &rhs))
            }
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                _,
            ) => Err(CoreError::internal(context.string_rhs_error())),
            (ComparisonOperator::RegexMatch, ScalarPredicateRhs::List(_)) => {
                Err(CoreError::internal(context.regex_rhs_error()))
            }
            (ComparisonOperator::RegexMatch, ScalarPredicateRhs::Expression(expression)) => {
                let rhs = self.render_scalar_expression_in_scope(expression, scope)?;
                Ok(render_regex_predicate(&lhs, &rhs))
            }
            (
                ComparisonOperator::Equal,
                ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null)),
            ) => Ok(format!("{lhs} IS NULL")),
            (
                ComparisonOperator::NotEqual,
                ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null)),
            ) => Ok(format!("{lhs} IS NOT NULL")),
            (
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual,
                ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null)),
            ) => Err(CoreError::internal(context.null_comparison_error())),
            (_, ScalarPredicateRhs::Expression(rhs)) => Ok(format!(
                "{lhs} {} {}",
                render_operator(predicate.operator),
                self.render_scalar_expression_in_scope(rhs, scope)?
            )),
            (_, ScalarPredicateRhs::List(_)) => Err(CoreError::internal(context.list_rhs_error())),
        }
    }

    fn try_render_count_existence_predicate_in_scope<'b, 'c>(
        &self,
        predicate: &ScalarPredicate,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<Option<String>, CoreError> {
        let ScalarExpression::CountSubquery {
            pattern,
            distinct_target: None,
        } = &predicate.lhs
        else {
            return Ok(None);
        };
        let Some(existence) = Self::count_existence_predicate(predicate.operator, &predicate.rhs)
        else {
            return Ok(None);
        };
        match scope {
            ScalarScope::TopLevel => self
                .render_count_existence_predicate(pattern, existence)
                .map(Some),
            ScalarScope::Scoped {
                relationships,
                local_nodes,
                local_aliases,
            } => self
                .render_scoped_count_existence_predicate(
                    pattern,
                    existence,
                    relationships,
                    local_nodes,
                    local_aliases,
                )
                .map(Some),
        }
    }

    fn render_key_predicate_in_scope<'b, 'c>(
        &self,
        predicate: &KeyPredicate,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        let key = self.render_binding_key_ref_in_scope(&predicate.variable, scope)?;
        self.render_simple_predicate_in_scope(
            &key,
            predicate.operator,
            &predicate.rhs,
            None,
            scope,
            SimplePredicateContext::key_from_scope(scope),
        )
    }

    fn render_element_id_predicate_in_scope<'b, 'c>(
        &self,
        predicate: &ElementIdPredicate,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        let element_id = self.render_binding_element_id_ref_in_scope(&predicate.variable, scope)?;
        self.render_simple_predicate_in_scope(
            &element_id,
            predicate.operator,
            &predicate.rhs,
            None,
            scope,
            SimplePredicateContext::element_id_from_scope(scope),
        )
    }

    fn render_presence_predicate_in_scope<'b, 'c>(
        &self,
        predicate: &PresencePredicate,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        let presence = self.render_binding_presence_ref_in_scope(&predicate.variable, scope)?;
        match predicate.operator {
            ComparisonOperator::Equal => Ok(format!("{presence} IS NULL")),
            ComparisonOperator::NotEqual => Ok(format!("{presence} IS NOT NULL")),
            ComparisonOperator::GreaterThan
            | ComparisonOperator::GreaterThanOrEqual
            | ComparisonOperator::LessThan
            | ComparisonOperator::LessThanOrEqual
            | ComparisonOperator::In
            | ComparisonOperator::StartsWith
            | ComparisonOperator::EndsWith
            | ComparisonOperator::Contains
            | ComparisonOperator::RegexMatch => Err(CoreError::internal(match scope {
                ScalarScope::TopLevel => {
                    "validated presence predicate contained an invalid operator"
                }
                ScalarScope::Scoped { .. } => {
                    "validated scoped presence predicate contained invalid operator"
                }
            })),
        }
    }

    pub(super) fn render_property_key_membership_predicate(
        &self,
        predicate: &PropertyKeyMembershipPredicate,
    ) -> Result<String, CoreError> {
        self.render_property_key_membership_predicate_in_scope(predicate, ScalarScope::TopLevel)
    }

    fn render_property_key_membership_predicate_in_scope<'b, 'c>(
        &self,
        predicate: &PropertyKeyMembershipPredicate,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        let has_key = match scope {
            ScalarScope::TopLevel => {
                let binding = self.validated.binding(&predicate.variable)?;
                Some(match binding.kind() {
                    ValidatedBindingKind::Node(node)
                    | ValidatedBindingKind::StageColumn { node, .. } => {
                        node.properties.contains_key(&predicate.key)
                    }
                    ValidatedBindingKind::Relationship(relationship) => {
                        relationship.properties.contains_key(&predicate.key)
                    }
                })
            }
            ScalarScope::Scoped {
                relationships,
                local_nodes,
                ..
            } => {
                if let Some(relationship) =
                    Self::exists_relationship_for_variable(relationships, &predicate.variable)
                {
                    Some(
                        relationship
                            .relationship
                            .properties
                            .contains_key(&predicate.key),
                    )
                } else {
                    local_nodes
                        .get(predicate.variable.as_str())
                        .map(|node| node.properties.contains_key(&predicate.key))
                }
            }
        };
        let Some(has_key) = has_key else {
            return self.render_property_key_membership_predicate(predicate);
        };
        let presence_variable = predicate
            .presence_variable
            .as_deref()
            .unwrap_or(&predicate.variable);
        let presence = self.render_binding_presence_ref_in_scope(presence_variable, scope)?;
        let value = if has_key { "TRUE" } else { "FALSE" };
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE {value} END"
        ))
    }

    fn render_exists_pattern_predicate(
        &self,
        predicate: &ExistsPatternPredicate,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "EXISTS {}",
            self.render_scoped_pattern_select(predicate, "1")?
        ))
    }

    pub(super) fn render_exists_property_predicate<'b>(
        &self,
        predicate: &PropertyPredicate,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        self.render_property_predicate_in_scope(
            predicate,
            ScalarScope::Scoped {
                relationships,
                local_nodes,
                local_aliases,
            },
        )
    }

    fn render_simple_predicate_in_scope<'b, 'c>(
        &self,
        lhs: &str,
        operator: ComparisonOperator,
        rhs: &PredicateRhs,
        temporal_property: Option<&PropertyRef>,
        scope: ScalarScope<'a, 'b, 'c>,
        context: SimplePredicateContext,
    ) -> Result<String, CoreError> {
        match (operator, rhs) {
            (ComparisonOperator::In, PredicateRhs::List(literals)) => {
                Ok(render_literal_in_predicate(lhs, literals))
            }
            (ComparisonOperator::In, PredicateRhs::TemporalCoercionList(sources)) => {
                if sources.is_empty() {
                    return Ok("FALSE".to_string());
                }
                let rendered = if let Some(property) = temporal_property {
                    self.render_temporal_coercion_list_predicate_rhs_in_scope(
                        property, sources, scope,
                    )?
                } else {
                    Self::render_temporal_coercion_list_rhs_for_kind(sources, None)
                };
                Ok(format!("{lhs} IN ({rendered})"))
            }
            (ComparisonOperator::In, _) => Err(CoreError::internal(context.in_list_error())),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                PredicateRhs::Literal(Literal::String(value))
                | PredicateRhs::TemporalCoercion { source: value },
            ) => {
                let operator = StringMatchOperator::from_comparison(operator)
                    .ok_or_else(|| CoreError::internal(context.string_operator_error()))?;
                Ok(format!(
                    "{lhs} LIKE {} ESCAPE '\\'",
                    render_like_pattern(operator, value)
                ))
            }
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                _,
            ) => Err(CoreError::internal(context.string_rhs_error())),
            (
                ComparisonOperator::RegexMatch,
                PredicateRhs::List(_) | PredicateRhs::TemporalCoercionList(_),
            ) => Err(CoreError::internal(context.regex_rhs_error())),
            (ComparisonOperator::RegexMatch, rhs) => Ok(render_regex_predicate(
                lhs,
                &self.render_predicate_rhs_in_scope(rhs, scope)?,
            )),
            (ComparisonOperator::Equal, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{lhs} IS NULL"))
            }
            (ComparisonOperator::NotEqual, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{lhs} IS NOT NULL"))
            }
            (
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual,
                PredicateRhs::Literal(Literal::Null),
            ) => Err(CoreError::internal(context.null_comparison_error())),
            (_, PredicateRhs::TemporalCoercion { source }) if temporal_property.is_some() => {
                let property = temporal_property.ok_or_else(|| {
                    CoreError::internal(
                        "validated temporal coercion predicate was missing a property",
                    )
                })?;
                Ok(format!(
                    "{lhs} {} {}",
                    render_operator(operator),
                    self.render_temporal_coercion_predicate_rhs_in_scope(property, source, scope)?
                ))
            }
            _ => Ok(format!(
                "{lhs} {} {}",
                render_operator(operator),
                self.render_predicate_rhs_in_scope(rhs, scope)?
            )),
        }
    }

    fn render_predicate_rhs_in_scope<'b, 'c>(
        &self,
        rhs: &PredicateRhs,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        match scope {
            ScalarScope::TopLevel => self.render_predicate_rhs(rhs),
            ScalarScope::Scoped {
                relationships,
                local_nodes,
                local_aliases,
            } => self.render_exists_predicate_rhs(rhs, relationships, local_nodes, local_aliases),
        }
    }

    fn render_temporal_coercion_predicate_rhs_in_scope<'b, 'c>(
        &self,
        property: &PropertyRef,
        source: &str,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        match scope {
            ScalarScope::TopLevel => self.render_temporal_coercion_predicate_rhs(property, source),
            ScalarScope::Scoped {
                relationships,
                local_nodes,
                ..
            } => self.render_exists_temporal_coercion_predicate_rhs(
                property,
                source,
                relationships,
                local_nodes,
            ),
        }
    }

    fn render_temporal_coercion_list_predicate_rhs_in_scope<'b, 'c>(
        &self,
        property: &PropertyRef,
        sources: &[String],
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        match scope {
            ScalarScope::TopLevel => {
                self.render_temporal_coercion_list_predicate_rhs(property, sources)
            }
            ScalarScope::Scoped {
                relationships,
                local_nodes,
                ..
            } => self.render_exists_temporal_coercion_list_predicate_rhs(
                property,
                sources,
                relationships,
                local_nodes,
            ),
        }
    }

    fn render_binding_key_ref_in_scope<'b, 'c>(
        &self,
        variable: &str,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        match scope {
            ScalarScope::TopLevel => self.render_binding_key_ref(variable),
            ScalarScope::Scoped {
                relationships,
                local_nodes,
                local_aliases,
            } => self.render_exists_key_ref(variable, relationships, local_nodes, local_aliases),
        }
    }

    fn render_binding_element_id_ref_in_scope<'b, 'c>(
        &self,
        variable: &str,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        match scope {
            ScalarScope::TopLevel => self.render_binding_element_id_ref(variable),
            ScalarScope::Scoped { .. } => Ok(format!(
                "CAST({} AS VARCHAR)",
                self.render_binding_key_ref_in_scope(variable, scope)?
            )),
        }
    }

    fn render_binding_presence_ref_in_scope<'b, 'c>(
        &self,
        variable: &str,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        match scope {
            ScalarScope::TopLevel => self.render_binding_presence_ref(variable),
            ScalarScope::Scoped {
                relationships,
                local_nodes,
                local_aliases,
            } => self.render_scoped_binding_presence_ref(
                variable,
                relationships,
                local_nodes,
                local_aliases,
            ),
        }
    }

    fn render_scalar_expression_in_scope<'b, 'c>(
        &self,
        expression: &ScalarExpression,
        scope: ScalarScope<'a, 'b, 'c>,
    ) -> Result<String, CoreError> {
        match scope {
            ScalarScope::TopLevel => self.render_scalar_expression(expression),
            ScalarScope::Scoped {
                relationships,
                local_nodes,
                local_aliases,
            } => self.render_scoped_scalar_expression(
                expression,
                relationships,
                local_nodes,
                local_aliases,
            ),
        }
    }

    fn render_projection_predicate_rhs(
        &self,
        rhs: &ProjectionPredicateRhs,
    ) -> Result<String, CoreError> {
        match rhs {
            ProjectionPredicateRhs::Literal(literal) => Ok(render_literal(literal)),
            ProjectionPredicateRhs::Alias(alias) => self.render_projection_alias_ref(alias),
            ProjectionPredicateRhs::List(_) => Err(CoreError::internal(
                "validated projected literal list predicate reached generic RHS renderer",
            )),
        }
    }

    fn render_predicate_rhs(&self, rhs: &PredicateRhs) -> Result<String, CoreError> {
        match rhs {
            PredicateRhs::Literal(literal) => Ok(render_literal(literal)),
            PredicateRhs::TemporalCoercion { source } => Ok(quote_string_literal(source)),
            PredicateRhs::TemporalCoercionList(_) => Err(CoreError::internal(
                "validated temporal coercion list predicate reached generic RHS renderer",
            )),
            PredicateRhs::Property(property) => self.render_property_ref(property),
            PredicateRhs::Key { variable } => self.render_binding_key_ref(variable),
            PredicateRhs::ElementId { variable } => self.render_binding_element_id_ref(variable),
            PredicateRhs::List(_) => Err(CoreError::internal(
                "validated literal list predicate reached generic RHS renderer",
            )),
        }
    }

    fn render_temporal_coercion_predicate_rhs(
        &self,
        property: &PropertyRef,
        source: &str,
    ) -> Result<String, CoreError> {
        let kind = self.validated.property_ref_temporal_kind(property)?;
        Ok(Self::render_temporal_coercion_rhs_for_kind(source, kind))
    }

    fn render_temporal_coercion_list_predicate_rhs(
        &self,
        property: &PropertyRef,
        sources: &[String],
    ) -> Result<String, CoreError> {
        let kind = self.validated.property_ref_temporal_kind(property)?;
        Ok(Self::render_temporal_coercion_list_rhs_for_kind(
            sources, kind,
        ))
    }

    pub(super) fn render_temporal_coercion_list_rhs_for_kind(
        sources: &[String],
        kind: Option<TemporalKind>,
    ) -> String {
        sources
            .iter()
            .map(|source| Self::render_temporal_coercion_rhs_for_kind(source, kind))
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub(super) fn render_temporal_coercion_rhs_for_kind(
        source: &str,
        kind: Option<TemporalKind>,
    ) -> String {
        let literal = quote_string_literal(source);
        match kind {
            Some(TemporalKind::Date) => format!("CAST({literal} AS DATE)"),
            Some(TemporalKind::LocalDateTime) => format!("CAST({literal} AS TIMESTAMP)"),
            Some(TemporalKind::LocalTime) => format!("CAST({literal} AS TIME)"),
            Some(TemporalKind::ZonedDateTime | TemporalKind::Duration) | None => literal,
        }
    }
}

#[derive(Clone, Copy)]
enum SimplePredicateContext {
    Graph,
    Key,
    ElementId,
    Exists,
    Scoped,
}

impl SimplePredicateContext {
    fn key_from_scope(scope: ScalarScope<'_, '_, '_>) -> Self {
        match scope {
            ScalarScope::TopLevel => Self::Key,
            ScalarScope::Scoped { .. } => Self::Scoped,
        }
    }

    fn element_id_from_scope(scope: ScalarScope<'_, '_, '_>) -> Self {
        match scope {
            ScalarScope::TopLevel => Self::ElementId,
            ScalarScope::Scoped { .. } => Self::Scoped,
        }
    }

    fn in_list_error(self) -> &'static str {
        match self {
            Self::Graph => "validated IN predicate did not contain a literal list",
            Self::Key => "validated id() IN predicate did not contain a literal list",
            Self::ElementId => "validated elementId() IN predicate did not contain a literal list",
            Self::Exists => "validated EXISTS IN predicate did not contain a literal list",
            Self::Scoped => "validated scoped IN predicate did not contain a literal list",
        }
    }

    fn string_operator_error(self) -> &'static str {
        match self {
            Self::Graph => "validated string predicate used a non-string operator",
            Self::Key => "validated id() string predicate used a non-string operator",
            Self::ElementId => "validated elementId() string predicate used a non-string operator",
            Self::Exists => "validated EXISTS string predicate used a non-string operator",
            Self::Scoped => "validated scoped string predicate used a non-string operator",
        }
    }

    fn string_rhs_error(self) -> &'static str {
        match self {
            Self::Graph => "validated string predicate did not contain a string literal",
            Self::Key => "validated id() string predicate did not contain a string literal",
            Self::ElementId => {
                "validated elementId() string predicate did not contain a string literal"
            }
            Self::Exists => "validated EXISTS string predicate did not contain a string literal",
            Self::Scoped => "validated scoped string predicate did not contain a string literal",
        }
    }

    fn regex_rhs_error(self) -> &'static str {
        match self {
            Self::Graph => "validated regex predicate did not contain a scalar RHS",
            Self::Key => "validated id() regex predicate did not contain a scalar RHS",
            Self::ElementId => "validated elementId() regex predicate did not contain a scalar RHS",
            Self::Exists => "validated EXISTS regex predicate did not contain a scalar RHS",
            Self::Scoped => "validated scoped regex predicate did not contain a scalar RHS",
        }
    }

    fn null_comparison_error(self) -> &'static str {
        match self {
            Self::Graph => "validated graph predicate contained an invalid null comparison",
            Self::Key => "validated id() predicate contained an invalid null comparison",
            Self::ElementId => {
                "validated elementId() predicate contained an invalid null comparison"
            }
            Self::Exists => "validated EXISTS predicate contained an invalid null comparison",
            Self::Scoped => "validated scoped predicate contained an invalid null comparison",
        }
    }
}

#[derive(Clone, Copy)]
enum ScalarPredicateContext {
    TopLevel,
    Scoped,
}

impl ScalarPredicateContext {
    fn from_scope(scope: ScalarScope<'_, '_, '_>) -> Self {
        match scope {
            ScalarScope::TopLevel => Self::TopLevel,
            ScalarScope::Scoped { .. } => Self::Scoped,
        }
    }

    fn in_list_error(self) -> &'static str {
        match self {
            Self::TopLevel => "validated scalar IN predicate did not contain a literal list",
            Self::Scoped => "validated scoped scalar IN predicate did not contain a literal list",
        }
    }

    fn string_operator_error(self) -> &'static str {
        match self {
            Self::TopLevel => "validated scalar string predicate used a non-string operator",
            Self::Scoped => "validated scoped scalar string predicate used a non-string operator",
        }
    }

    fn string_rhs_error(self) -> &'static str {
        match self {
            Self::TopLevel => "validated scalar string predicate did not contain a string literal",
            Self::Scoped => "validated scoped scalar string predicate did not contain a scalar RHS",
        }
    }

    fn regex_rhs_error(self) -> &'static str {
        match self {
            Self::TopLevel => "validated scalar regex predicate did not contain a scalar RHS",
            Self::Scoped => "validated scoped scalar regex predicate did not contain a scalar RHS",
        }
    }

    fn null_comparison_error(self) -> &'static str {
        match self {
            Self::TopLevel => "validated scalar predicate contained an invalid null comparison",
            Self::Scoped => {
                "validated scoped scalar predicate contained an invalid null comparison"
            }
        }
    }

    fn list_rhs_error(self) -> &'static str {
        match self {
            Self::TopLevel => {
                "validated scalar literal list predicate reached generic RHS renderer"
            }
            Self::Scoped => {
                "validated scoped scalar literal list predicate reached generic RHS renderer"
            }
        }
    }
}

fn render_literal_in_predicate(lhs: &str, literals: &[Literal]) -> String {
    if literals.is_empty() {
        return "FALSE".to_string();
    }
    let rendered = literals
        .iter()
        .map(render_literal)
        .collect::<Vec<_>>()
        .join(", ");
    format!("{lhs} IN ({rendered})")
}
