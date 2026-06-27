use ordered_float::OrderedFloat;

/// Direction of a relationship pattern relative to its left and right nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// `(left)-[:TYPE]->(right)`.
    Outgoing,
    /// `(left)<-[:TYPE]-(right)`.
    Incoming,
    /// `(left)-[:TYPE]-(right)`.
    Undirected,
}

/// Sort direction for graph query ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    /// Ascending order.
    Ascending,
    /// Descending order.
    Descending,
}

/// Comparison operator for property predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOperator {
    /// Equality comparison.
    Equal,
    /// Inequality comparison.
    NotEqual,
    /// Greater-than comparison.
    GreaterThan,
    /// Greater-than-or-equal comparison.
    GreaterThanOrEqual,
    /// Less-than comparison.
    LessThan,
    /// Less-than-or-equal comparison.
    LessThanOrEqual,
    /// List membership comparison.
    In,
    /// String prefix comparison.
    StartsWith,
    /// String suffix comparison.
    EndsWith,
    /// String substring comparison.
    Contains,
}

/// Literal value supported by the initial graph IR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Literal {
    /// String literal.
    String(String),
    /// Signed integer literal.
    Integer(i64),
    /// Floating-point literal.
    Float(OrderedFloat<f64>),
    /// Boolean literal.
    Boolean(bool),
    /// Null literal.
    Null,
}

/// Property reference bound to a node or relationship variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyRef {
    /// Query variable.
    pub variable: String,
    /// Graph property name.
    pub property: String,
}

/// Right-hand side of a property predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateRhs {
    /// Compare against a literal value.
    Literal(Literal),
    /// Compare against another graph property.
    Property(PropertyRef),
    /// Compare against the stable mapped key for a graph variable.
    Key {
        /// Graph variable.
        variable: String,
    },
    /// Compare against the string element id for a graph variable.
    ElementId {
        /// Graph variable.
        variable: String,
    },
    /// Compare against a literal list.
    List(Vec<Literal>),
}

/// Right-hand side of a scalar expression predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarPredicateRhs {
    /// Compare against another scalar expression.
    Expression(ScalarExpression),
    /// Compare against a literal list.
    List(Vec<Literal>),
}

/// Right-hand side of a post-projection predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionPredicateRhs {
    /// Compare against a literal value.
    Literal(Literal),
    /// Compare against another projected alias.
    Alias(String),
    /// Compare against a literal list.
    List(Vec<Literal>),
}

/// Node pattern in the shared graph IR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodePattern {
    /// Query variable.
    pub variable: String,
    /// Graph node label.
    pub label: String,
}

/// Relationship pattern in the shared graph IR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipPattern {
    /// Optional relationship variable.
    pub variable: Option<String>,
    /// Graph relationship type.
    pub relationship_type: String,
    /// Left node variable in the source query pattern.
    pub left: String,
    /// Relationship direction relative to `left` and `right`.
    pub direction: Direction,
    /// Right node variable in the source query pattern.
    pub right: String,
}

/// Scalar value expression in the shared graph IR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarExpression {
    /// A mapped graph property.
    Property(PropertyRef),
    /// A scalar literal.
    Literal(Literal),
    /// Return the first non-null scalar expression.
    Coalesce {
        /// Candidate expressions in priority order.
        expressions: Vec<ScalarExpression>,
    },
    /// Cast a scalar expression to a string value.
    ToString {
        /// Inner expression to cast.
        expression: Box<ScalarExpression>,
    },
    /// Cast a scalar expression to an integer value.
    ToInteger {
        /// Inner expression to cast.
        expression: Box<ScalarExpression>,
    },
    /// Cast a scalar expression to a floating-point value.
    ToFloat {
        /// Inner expression to cast.
        expression: Box<ScalarExpression>,
    },
    /// Cast a scalar expression to a boolean value.
    ToBoolean {
        /// Inner expression to cast.
        expression: Box<ScalarExpression>,
    },
    /// Convert a scalar expression to lowercase.
    ToLower {
        /// Inner expression to normalize.
        expression: Box<ScalarExpression>,
    },
    /// Convert a scalar expression to uppercase.
    ToUpper {
        /// Inner expression to normalize.
        expression: Box<ScalarExpression>,
    },
    /// Trim whitespace from both ends of a scalar expression.
    Trim {
        /// Inner expression to normalize.
        expression: Box<ScalarExpression>,
    },
    /// Trim whitespace from the left side of a scalar expression.
    LTrim {
        /// Inner expression to normalize.
        expression: Box<ScalarExpression>,
    },
    /// Trim whitespace from the right side of a scalar expression.
    RTrim {
        /// Inner expression to normalize.
        expression: Box<ScalarExpression>,
    },
    /// Replace occurrences of one scalar expression inside another.
    Replace {
        /// Source expression.
        expression: Box<ScalarExpression>,
        /// Search expression.
        search: Box<ScalarExpression>,
        /// Replacement expression.
        replacement: Box<ScalarExpression>,
    },
    /// Count characters in a scalar string expression.
    CharacterLength {
        /// Inner expression to measure.
        expression: Box<ScalarExpression>,
    },
    /// Extract a substring from a scalar string expression using Cypher's
    /// zero-based start index.
    Substring {
        /// Source expression.
        expression: Box<ScalarExpression>,
        /// Zero-based start index expression.
        start: Box<ScalarExpression>,
        /// Optional length expression.
        length: Option<Box<ScalarExpression>>,
    },
    /// Return the leftmost characters from a scalar string expression.
    Left {
        /// Source expression.
        expression: Box<ScalarExpression>,
        /// Character count expression.
        count: Box<ScalarExpression>,
    },
    /// Return the rightmost characters from a scalar string expression.
    Right {
        /// Source expression.
        expression: Box<ScalarExpression>,
        /// Character count expression.
        count: Box<ScalarExpression>,
    },
    /// Reverse the characters in a scalar string expression.
    Reverse {
        /// Inner expression to reverse.
        expression: Box<ScalarExpression>,
    },
    /// Absolute value of a numeric scalar expression.
    Abs {
        /// Inner expression to normalize.
        expression: Box<ScalarExpression>,
    },
    /// Ceiling of a numeric scalar expression.
    Ceil {
        /// Inner expression to round up.
        expression: Box<ScalarExpression>,
    },
    /// Floor of a numeric scalar expression.
    Floor {
        /// Inner expression to round down.
        expression: Box<ScalarExpression>,
    },
    /// Round a numeric scalar expression, optionally to a decimal precision.
    Round {
        /// Inner expression to round.
        expression: Box<ScalarExpression>,
        /// Optional decimal places expression.
        places: Option<Box<ScalarExpression>>,
    },
    /// Negate a numeric scalar expression.
    Negate {
        /// Inner expression to negate.
        expression: Box<ScalarExpression>,
    },
    /// Numeric binary arithmetic over scalar expressions.
    Arithmetic {
        /// Arithmetic operator.
        operator: ArithmeticOperator,
        /// Left operand.
        left: Box<ScalarExpression>,
        /// Right operand.
        right: Box<ScalarExpression>,
    },
    /// Searched CASE expression.
    Case {
        /// Ordered WHEN/THEN alternatives.
        alternatives: Vec<ScalarCaseAlternative>,
        /// Optional ELSE fallback.
        else_expression: Option<Box<ScalarExpression>>,
    },
}

/// Numeric binary arithmetic operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithmeticOperator {
    /// Addition.
    Add,
    /// Subtraction.
    Subtract,
    /// Multiplication.
    Multiply,
    /// Division.
    Divide,
    /// Modulo.
    Modulo,
}

/// One searched CASE WHEN/THEN branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalarCaseAlternative {
    /// Predicate tested by the WHEN clause.
    pub when: PredicateExpression,
    /// Scalar expression returned by the THEN clause.
    pub then: ScalarExpression,
}

/// Projection in the shared graph IR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    /// Project a graph property.
    Property {
        /// Property to project.
        property: PropertyRef,
        /// Optional output alias.
        alias: Option<String>,
    },
    /// Project the stable mapped key for a graph variable.
    Key {
        /// Graph variable.
        variable: String,
        /// Output alias.
        alias: String,
    },
    /// Project the string element id for a graph variable.
    ElementId {
        /// Graph variable.
        variable: String,
        /// Output alias.
        alias: String,
    },
    /// Project the static type of a relationship variable, preserving null for
    /// unmatched optional relationships.
    RelationshipType {
        /// Graph relationship variable.
        variable: String,
        /// Static graph relationship type.
        relationship_type: String,
        /// Output alias.
        alias: String,
    },
    /// Project the static labels of a node variable as a one-element list,
    /// preserving null for unmatched optional nodes.
    NodeLabels {
        /// Graph node variable.
        variable: String,
        /// Static graph node label.
        label: String,
        /// Output alias.
        alias: String,
    },
    /// Project the statically declared property keys for a graph variable.
    PropertyKeys {
        /// Graph variable.
        variable: String,
        /// Output alias.
        alias: String,
    },
    /// Project a literal value.
    Literal {
        /// Literal to project.
        literal: Literal,
        /// Output alias.
        alias: String,
    },
    /// Project a literal list value.
    LiteralList {
        /// Literal list elements.
        literals: Vec<Literal>,
        /// Output alias.
        alias: String,
    },
    /// Project a scalar expression.
    Expression {
        /// Scalar expression to project.
        expression: ScalarExpression,
        /// Output alias.
        alias: String,
    },
    /// Project `COUNT(*)`.
    CountAll {
        /// Output alias.
        alias: String,
    },
    /// Project an aggregate over a graph property.
    Aggregate {
        /// Aggregate function.
        function: AggregateFunction,
        /// Value to aggregate.
        target: AggregateTarget,
        /// Whether the aggregate applies distinct semantics.
        distinct: bool,
        /// Output alias.
        alias: String,
    },
}

impl Projection {
    /// Returns whether this projection is an aggregate.
    #[must_use]
    pub fn is_aggregate(&self) -> bool {
        matches!(self, Self::CountAll { .. } | Self::Aggregate { .. })
    }
}

/// Aggregate functions in the shared graph IR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    /// `COUNT(property)`.
    Count,
    /// `COLLECT(property)`.
    Collect,
    /// `SUM(property)`.
    Sum,
    /// `AVG(property)`.
    Avg,
    /// `MIN(property)`.
    Min,
    /// `MAX(property)`.
    Max,
}

/// Aggregate argument in the shared graph IR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateTarget {
    /// Aggregate a graph property.
    Property(PropertyRef),
    /// Aggregate a graph variable by its mapped stable key column.
    VariableKey {
        /// Graph variable to aggregate.
        variable: String,
    },
}

/// Property comparison predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyPredicate {
    /// Left-hand property reference.
    pub property: PropertyRef,
    /// Comparison operator.
    pub operator: ComparisonOperator,
    /// Right-hand comparison operand.
    pub rhs: PredicateRhs,
}

/// Key comparison predicate, compiled from `id(variable)` expressions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPredicate {
    /// Graph variable whose mapped key is compared.
    pub variable: String,
    /// Comparison operator.
    pub operator: ComparisonOperator,
    /// Right-hand comparison operand.
    pub rhs: PredicateRhs,
}

/// Element id comparison predicate, compiled from `elementId(variable)` expressions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElementIdPredicate {
    /// Graph variable whose mapped key is compared as a string element id.
    pub variable: String,
    /// Comparison operator.
    pub operator: ComparisonOperator,
    /// Right-hand comparison operand.
    pub rhs: PredicateRhs,
}

/// Predicate over graph binding presence, compiled from `variable IS NULL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresencePredicate {
    /// Graph variable whose binding presence is tested.
    pub variable: String,
    /// Null comparison operator. Only `Equal` and `NotEqual` are valid.
    pub operator: ComparisonOperator,
}

/// Predicate checking whether a statically declared graph property key exists
/// on a graph variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyKeyMembershipPredicate {
    /// Graph variable whose declared property keys should be tested.
    pub variable: String,
    /// Property key name to check.
    pub key: String,
}

/// Predicate over scalar graph expressions such as `coalesce(...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalarPredicate {
    /// Left-hand scalar expression.
    pub lhs: ScalarExpression,
    /// Comparison operator.
    pub operator: ComparisonOperator,
    /// Right-hand comparison operand.
    pub rhs: ScalarPredicateRhs,
}

/// Scope introduced by one `OPTIONAL MATCH` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionalMatchScope {
    /// Relationship indices introduced by the optional pattern.
    pub relationship_indices: Vec<usize>,
    /// Predicate to apply in the null-preserving optional match scope.
    pub predicate: Option<PredicateExpression>,
}

/// Boolean predicate expression over graph properties.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateExpression {
    /// Boolean constant.
    Boolean(bool),
    /// Leaf property comparison.
    Comparison(PropertyPredicate),
    /// Leaf key comparison.
    KeyComparison(KeyPredicate),
    /// Leaf element id comparison.
    ElementIdComparison(ElementIdPredicate),
    /// Leaf graph binding presence comparison.
    Presence(PresencePredicate),
    /// Leaf declared property key membership test.
    PropertyKeyMembership(PropertyKeyMembershipPredicate),
    /// Leaf scalar expression comparison.
    ScalarComparison(ScalarPredicate),
    /// Boolean conjunction.
    And {
        /// Left-hand expression.
        left: Box<PredicateExpression>,
        /// Right-hand expression.
        right: Box<PredicateExpression>,
    },
    /// Boolean disjunction.
    Or {
        /// Left-hand expression.
        left: Box<PredicateExpression>,
        /// Right-hand expression.
        right: Box<PredicateExpression>,
    },
    /// Boolean negation.
    Not {
        /// Negated expression.
        expression: Box<PredicateExpression>,
    },
}

/// Predicate over projected aliases, applied after terminal `WITH` projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionPredicate {
    /// Left-hand projected alias.
    pub alias: String,
    /// Comparison operator.
    pub operator: ComparisonOperator,
    /// Right-hand comparison operand.
    pub rhs: ProjectionPredicateRhs,
}

/// Boolean predicate expression over projected aliases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionPredicateExpression {
    /// Boolean constant.
    Boolean(bool),
    /// Leaf projected alias comparison.
    Comparison(ProjectionPredicate),
    /// Boolean conjunction.
    And {
        /// Left-hand expression.
        left: Box<ProjectionPredicateExpression>,
        /// Right-hand expression.
        right: Box<ProjectionPredicateExpression>,
    },
    /// Boolean disjunction.
    Or {
        /// Left-hand expression.
        left: Box<ProjectionPredicateExpression>,
        /// Right-hand expression.
        right: Box<ProjectionPredicateExpression>,
    },
    /// Boolean negation.
    Not {
        /// Negated expression.
        expression: Box<ProjectionPredicateExpression>,
    },
}

/// Ordering key in the shared graph IR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderExpression {
    /// Order by a graph property.
    Property(PropertyRef),
    /// Order by the stable mapped key for a graph variable.
    Key {
        /// Graph variable.
        variable: String,
    },
    /// Order by the string element id for a graph variable.
    ElementId {
        /// Graph variable.
        variable: String,
    },
    /// Order by the static type of a relationship variable.
    RelationshipType {
        /// Graph relationship variable.
        variable: String,
        /// Static graph relationship type.
        relationship_type: String,
    },
    /// Order by the static labels of a node variable.
    NodeLabels {
        /// Graph node variable.
        variable: String,
        /// Static graph node label.
        label: String,
    },
    /// Order by a scalar expression.
    Scalar(ScalarExpression),
    /// Order by a literal value.
    Literal(Literal),
    /// Order by a projected output alias.
    ProjectionAlias(String),
}

/// Ordering key in the shared graph IR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderKey {
    /// Expression to order by.
    pub expression: OrderExpression,
    /// Sort direction.
    pub direction: OrderDirection,
}

/// Shared logical graph query plan consumed by SQL lowering.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphPlan {
    /// Node patterns bound in the query.
    pub nodes: Vec<NodePattern>,
    /// Relationship patterns between bound nodes.
    pub relationships: Vec<RelationshipPattern>,
    /// Relationship indices that should be lowered as null-preserving optional
    /// matches.
    pub optional_relationships: Vec<usize>,
    /// Optional pattern scopes. These preserve the clause-level boundary needed
    /// for optional predicates and future multi-hop optional lowering.
    pub optional_matches: Vec<OptionalMatchScope>,
    /// Whether duplicate projected rows should be removed.
    pub distinct: bool,
    /// Projected expressions.
    pub projections: Vec<Projection>,
    /// Conjunctive property predicates.
    pub predicates: Vec<PropertyPredicate>,
    /// Optional boolean predicate tree for expressions that cannot be flattened
    /// into the conjunctive predicate vector.
    pub predicate: Option<PredicateExpression>,
    /// Optional predicate over projected aliases, applied after terminal `WITH`.
    pub post_projection_predicate: Option<ProjectionPredicateExpression>,
    /// Ordering expressions.
    pub order_by: Vec<OrderKey>,
    /// Optional row offset.
    pub skip: Option<u64>,
    /// Optional row limit.
    pub limit: Option<u64>,
}
