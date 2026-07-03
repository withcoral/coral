//! Predicate rendering for the SQL Lowerer: emits `WHERE` (pre-projection) and `HAVING`
//! (post-projection) SQL from graph-plan predicate trees — property, scalar, key,
//! element-id, presence, property-key-membership and EXISTS-pattern comparison leaves,
//! their boolean-expression walkers, and the right-hand-side operand rendering.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "SQL predicate renderers are split into a child module while preserving parent-private access."
)]
use super::*;

impl<'a> Lowerer<'a> {
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

    fn render_pre_projection_predicates(&self) -> Result<Vec<String>, CoreError> {
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
        match predicate {
            PredicateExpression::Boolean(value) => Ok(value.to_string().to_uppercase()),
            PredicateExpression::Comparison(predicate) => self.render_predicate(predicate),
            PredicateExpression::KeyComparison(predicate) => self.render_key_predicate(predicate),
            PredicateExpression::ElementIdComparison(predicate) => {
                self.render_element_id_predicate(predicate)
            }
            PredicateExpression::Presence(predicate) => self.render_presence_predicate(predicate),
            PredicateExpression::PropertyKeyMembership(predicate) => {
                self.render_property_key_membership_predicate(predicate)
            }
            PredicateExpression::ExistsPattern(predicate) => {
                self.render_exists_pattern_predicate(predicate)
            }
            PredicateExpression::ScalarComparison(predicate) => {
                self.render_scalar_predicate(predicate)
            }
            PredicateExpression::And { left, right } => Ok(format!(
                "({} AND {})",
                self.render_predicate_expression(left)?,
                self.render_predicate_expression(right)?
            )),
            PredicateExpression::Or { left, right } => Ok(format!(
                "({} OR {})",
                self.render_predicate_expression(left)?,
                self.render_predicate_expression(right)?
            )),
            PredicateExpression::Xor { left, right } => {
                let left = self.render_predicate_expression(left)?;
                let right = self.render_predicate_expression(right)?;
                Ok(render_xor_predicate(&left, &right))
            }
            PredicateExpression::Not { expression } => Ok(format!(
                "NOT ({})",
                self.render_predicate_expression(expression)?
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
            ) => Ok(format!(
                "{alias} LIKE {} ESCAPE '\\'",
                render_like_pattern(predicate.operator, value)
            )),
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
        let property = self.render_property_ref(&predicate.property)?;
        match (&predicate.operator, &predicate.rhs) {
            (ComparisonOperator::In, PredicateRhs::List(literals)) => {
                if literals.is_empty() {
                    return Ok("FALSE".to_string());
                }
                let rendered = literals
                    .iter()
                    .map(render_literal)
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("{property} IN ({rendered})"))
            }
            (ComparisonOperator::In, _) => Err(CoreError::internal(
                "validated IN predicate did not contain a literal list",
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                PredicateRhs::Literal(Literal::String(value)),
            ) => Ok(format!(
                "{property} LIKE {} ESCAPE '\\'",
                render_like_pattern(predicate.operator, value)
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                _,
            ) => Err(CoreError::internal(
                "validated string predicate did not contain a string literal",
            )),
            (ComparisonOperator::RegexMatch, PredicateRhs::List(_)) => Err(CoreError::internal(
                "validated regex predicate did not contain a scalar RHS",
            )),
            (ComparisonOperator::RegexMatch, rhs) => Ok(render_regex_predicate(
                &property,
                &self.render_predicate_rhs(rhs)?,
            )),
            (ComparisonOperator::Equal, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{property} IS NULL"))
            }
            (ComparisonOperator::NotEqual, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{property} IS NOT NULL"))
            }
            (
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual,
                PredicateRhs::Literal(Literal::Null),
            ) => Err(CoreError::internal(
                "validated graph predicate contained an invalid null comparison",
            )),
            _ => Ok(format!(
                "{property} {} {}",
                render_operator(predicate.operator),
                self.render_predicate_rhs(&predicate.rhs)?
            )),
        }
    }

    fn render_scalar_predicate(&self, predicate: &ScalarPredicate) -> Result<String, CoreError> {
        if let Some(rendered) = self.try_render_count_existence_predicate(predicate)? {
            return Ok(rendered);
        }

        let lhs = self.render_scalar_expression(&predicate.lhs)?;
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
            (ComparisonOperator::In, _) => Err(CoreError::internal(
                "validated scalar IN predicate did not contain a literal list",
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(value))),
            ) => Ok(format!(
                "{lhs} LIKE {} ESCAPE '\\'",
                render_like_pattern(predicate.operator, value)
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                ScalarPredicateRhs::Expression(expression),
            ) => {
                let rhs = self.render_scalar_expression(expression)?;
                Ok(render_string_function_predicate(
                    predicate.operator,
                    &lhs,
                    &rhs,
                ))
            }
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                _,
            ) => Err(CoreError::internal(
                "validated scalar string predicate did not contain a string literal",
            )),
            (ComparisonOperator::RegexMatch, ScalarPredicateRhs::List(_)) => {
                Err(CoreError::internal(
                    "validated scalar regex predicate did not contain a scalar RHS",
                ))
            }
            (ComparisonOperator::RegexMatch, ScalarPredicateRhs::Expression(expression)) => {
                let rhs = self.render_scalar_expression(expression)?;
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
            ) => Err(CoreError::internal(
                "validated scalar predicate contained an invalid null comparison",
            )),
            _ => Ok(format!(
                "{lhs} {} {}",
                render_operator(predicate.operator),
                self.render_scalar_predicate_rhs(&predicate.rhs)?
            )),
        }
    }

    fn try_render_count_existence_predicate(
        &self,
        predicate: &ScalarPredicate,
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
        self.render_count_existence_predicate(pattern, existence)
            .map(Some)
    }

    fn render_key_predicate(&self, predicate: &KeyPredicate) -> Result<String, CoreError> {
        let key = self.render_binding_key_ref(&predicate.variable)?;
        match (&predicate.operator, &predicate.rhs) {
            (ComparisonOperator::In, PredicateRhs::List(literals)) => {
                if literals.is_empty() {
                    return Ok("FALSE".to_string());
                }
                let rendered = literals
                    .iter()
                    .map(render_literal)
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("{key} IN ({rendered})"))
            }
            (ComparisonOperator::In, _) => Err(CoreError::internal(
                "validated id() IN predicate did not contain a literal list",
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                PredicateRhs::Literal(Literal::String(value)),
            ) => Ok(format!(
                "{key} LIKE {} ESCAPE '\\'",
                render_like_pattern(predicate.operator, value)
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                _,
            ) => Err(CoreError::internal(
                "validated id() string predicate did not contain a string literal",
            )),
            (ComparisonOperator::RegexMatch, PredicateRhs::List(_)) => Err(CoreError::internal(
                "validated id() regex predicate did not contain a scalar RHS",
            )),
            (ComparisonOperator::RegexMatch, rhs) => Ok(render_regex_predicate(
                &key,
                &self.render_predicate_rhs(rhs)?,
            )),
            (ComparisonOperator::Equal, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{key} IS NULL"))
            }
            (ComparisonOperator::NotEqual, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{key} IS NOT NULL"))
            }
            (
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual,
                PredicateRhs::Literal(Literal::Null),
            ) => Err(CoreError::internal(
                "validated id() predicate contained an invalid null comparison",
            )),
            _ => Ok(format!(
                "{key} {} {}",
                render_operator(predicate.operator),
                self.render_predicate_rhs(&predicate.rhs)?
            )),
        }
    }

    fn render_element_id_predicate(
        &self,
        predicate: &ElementIdPredicate,
    ) -> Result<String, CoreError> {
        let element_id = self.render_binding_element_id_ref(&predicate.variable)?;
        match (&predicate.operator, &predicate.rhs) {
            (ComparisonOperator::In, PredicateRhs::List(literals)) => {
                if literals.is_empty() {
                    return Ok("FALSE".to_string());
                }
                let rendered = literals
                    .iter()
                    .map(render_literal)
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("{element_id} IN ({rendered})"))
            }
            (ComparisonOperator::In, _) => Err(CoreError::internal(
                "validated elementId() IN predicate did not contain a literal list",
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                PredicateRhs::Literal(Literal::String(value)),
            ) => Ok(format!(
                "{element_id} LIKE {} ESCAPE '\\'",
                render_like_pattern(predicate.operator, value)
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                _,
            ) => Err(CoreError::internal(
                "validated elementId() string predicate did not contain a string literal",
            )),
            (ComparisonOperator::RegexMatch, PredicateRhs::List(_)) => Err(CoreError::internal(
                "validated elementId() regex predicate did not contain a scalar RHS",
            )),
            (ComparisonOperator::RegexMatch, rhs) => Ok(render_regex_predicate(
                &element_id,
                &self.render_predicate_rhs(rhs)?,
            )),
            (ComparisonOperator::Equal, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{element_id} IS NULL"))
            }
            (ComparisonOperator::NotEqual, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{element_id} IS NOT NULL"))
            }
            (
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual,
                PredicateRhs::Literal(Literal::Null),
            ) => Err(CoreError::internal(
                "validated elementId() predicate contained an invalid null comparison",
            )),
            _ => Ok(format!(
                "{element_id} {} {}",
                render_operator(predicate.operator),
                self.render_predicate_rhs(&predicate.rhs)?
            )),
        }
    }

    fn render_presence_predicate(
        &self,
        predicate: &PresencePredicate,
    ) -> Result<String, CoreError> {
        let presence = self.render_binding_presence_ref(&predicate.variable)?;
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
            | ComparisonOperator::RegexMatch => Err(CoreError::internal(
                "validated presence predicate contained an invalid operator",
            )),
        }
    }

    pub(super) fn render_property_key_membership_predicate(
        &self,
        predicate: &PropertyKeyMembershipPredicate,
    ) -> Result<String, CoreError> {
        let binding = self.validated.binding(&predicate.variable)?;
        let has_key = match binding.kind() {
            ValidatedBindingKind::Node(node) => node.properties.contains_key(&predicate.key),
            ValidatedBindingKind::Relationship(relationship) => {
                relationship.properties.contains_key(&predicate.key)
            }
        };
        let presence = self.render_binding_presence_ref(
            predicate
                .presence_variable
                .as_deref()
                .unwrap_or(&predicate.variable),
        )?;
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
        let property = self.render_exists_property_ref(
            &predicate.property,
            relationships,
            local_nodes,
            local_aliases,
        )?;
        match (&predicate.operator, &predicate.rhs) {
            (ComparisonOperator::In, PredicateRhs::List(literals)) => {
                if literals.is_empty() {
                    return Ok("FALSE".to_string());
                }
                let rendered = literals
                    .iter()
                    .map(render_literal)
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("{property} IN ({rendered})"))
            }
            (ComparisonOperator::In, _) => Err(CoreError::internal(
                "validated EXISTS IN predicate did not contain a literal list",
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                PredicateRhs::Literal(Literal::String(value)),
            ) => Ok(format!(
                "{property} LIKE {} ESCAPE '\\'",
                render_like_pattern(predicate.operator, value)
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                _,
            ) => Err(CoreError::internal(
                "validated EXISTS string predicate did not contain a string literal",
            )),
            (ComparisonOperator::RegexMatch, PredicateRhs::List(_)) => Err(CoreError::internal(
                "validated EXISTS regex predicate did not contain a scalar RHS",
            )),
            (ComparisonOperator::RegexMatch, rhs) => Ok(render_regex_predicate(
                &property,
                &self.render_exists_predicate_rhs(
                    rhs,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?,
            )),
            (ComparisonOperator::Equal, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{property} IS NULL"))
            }
            (ComparisonOperator::NotEqual, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{property} IS NOT NULL"))
            }
            (
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual,
                PredicateRhs::Literal(Literal::Null),
            ) => Err(CoreError::internal(
                "validated EXISTS predicate contained an invalid null comparison",
            )),
            _ => Ok(format!(
                "{property} {} {}",
                render_operator(predicate.operator),
                self.render_exists_predicate_rhs(
                    &predicate.rhs,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?
            )),
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
            PredicateRhs::Property(property) => self.render_property_ref(property),
            PredicateRhs::Key { variable } => self.render_binding_key_ref(variable),
            PredicateRhs::ElementId { variable } => self.render_binding_element_id_ref(variable),
            PredicateRhs::List(_) => Err(CoreError::internal(
                "validated literal list predicate reached generic RHS renderer",
            )),
        }
    }

    fn render_scalar_predicate_rhs(&self, rhs: &ScalarPredicateRhs) -> Result<String, CoreError> {
        match rhs {
            ScalarPredicateRhs::Expression(expression) => self.render_scalar_expression(expression),
            ScalarPredicateRhs::List(_) => Err(CoreError::internal(
                "validated scalar literal list predicate reached generic RHS renderer",
            )),
        }
    }
}
