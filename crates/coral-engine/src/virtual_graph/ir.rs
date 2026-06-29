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

/// Explicit null placement for graph query ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullOrder {
    /// Null values sort before non-null values.
    First,
    /// Null values sort after non-null values.
    Last,
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
    /// Regular expression match comparison.
    RegexMatch,
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

/// Element type carried by a statically folded list when the list may be empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiteralListElementType {
    /// String elements.
    String,
    /// Signed integer elements.
    Integer,
    /// Floating-point elements.
    Float,
    /// Boolean elements.
    Boolean,
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

/// Declared endpoint side for a relationship, independent of query direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UndirectedRelationshipEndpoint {
    /// Relationship `from` endpoint.
    Start,
    /// Relationship `to` endpoint.
    End,
}

/// Scalar value expression in the shared graph IR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarExpression {
    /// A mapped graph property.
    Property(PropertyRef),
    /// A mapped node property chosen from either side of a same-label
    /// undirected relationship using the relationship's declared orientation.
    UndirectedEndpointProperty {
        /// Relationship variable whose declared orientation controls endpoint selection.
        relationship: String,
        /// Declared endpoint side to select.
        endpoint: UndirectedRelationshipEndpoint,
        /// Graph property name to read from the selected node endpoint.
        property: String,
    },
    /// Stable mapped key for a same-label undirected relationship endpoint
    /// selected by the relationship's declared orientation.
    UndirectedEndpointKey {
        /// Relationship variable whose declared orientation controls endpoint selection.
        relationship: String,
        /// Declared endpoint side to select.
        endpoint: UndirectedRelationshipEndpoint,
    },
    /// String element id for a same-label undirected relationship endpoint
    /// selected by the relationship's declared orientation.
    UndirectedEndpointElementId {
        /// Relationship variable whose declared orientation controls endpoint selection.
        relationship: String,
        /// Declared endpoint side to select.
        endpoint: UndirectedRelationshipEndpoint,
    },
    /// Static labels for a same-label undirected relationship endpoint.
    UndirectedEndpointLabels {
        /// Relationship variable whose declared orientation controls endpoint selection.
        relationship: String,
        /// Declared endpoint side to select.
        endpoint: UndirectedRelationshipEndpoint,
        /// Static graph node label.
        label: String,
    },
    /// Statically declared property keys for a same-label undirected
    /// relationship endpoint.
    UndirectedEndpointPropertyKeys {
        /// Relationship variable whose declared orientation controls endpoint selection.
        relationship: String,
        /// Declared endpoint side to select.
        endpoint: UndirectedRelationshipEndpoint,
    },
    /// A scalar literal.
    Literal(Literal),
    /// A statically folded literal list value.
    LiteralList {
        /// Literal list elements.
        literals: Vec<Literal>,
    },
    /// A statically folded list value whose element type is known even when
    /// the list is empty.
    TypedLiteralList {
        /// Literal list elements.
        literals: Vec<Literal>,
        /// Known non-null element type.
        element_type: LiteralListElementType,
    },
    /// A boolean predicate used as a scalar value.
    Predicate(Box<PredicateExpression>),
    /// Count rows produced by a read-only graph subquery.
    CountSubquery {
        /// Scoped graph pattern counted by the subquery.
        pattern: Box<CountSubqueryPattern>,
    },
    /// Stable mapped key for a graph variable.
    Key {
        /// Graph variable.
        variable: String,
    },
    /// String element id for a graph variable.
    ElementId {
        /// Graph variable.
        variable: String,
    },
    /// Internal label/type-qualified graph identity for cross-label distinct
    /// aggregation.
    GraphIdentity {
        /// Graph variable.
        variable: String,
    },
    /// Internal graph binding presence value used for `count(variable)`.
    GraphPresence {
        /// Graph variable.
        variable: String,
    },
    /// Static labels of a node variable as a one-element list.
    NodeLabels {
        /// Graph node variable.
        variable: String,
        /// Static graph node label.
        label: String,
    },
    /// Statically declared property keys for a graph variable.
    PropertyKeys {
        /// Graph variable.
        variable: String,
    },
    /// Return `expression` only when `presence_variable` is bound. This is used
    /// for values derived from optional relationships where the endpoint node
    /// may itself be a mandatory anchor.
    PresenceGated {
        /// Graph variable whose binding controls nullability.
        presence_variable: String,
        /// Scalar expression to return when the presence variable is bound.
        expression: Box<ScalarExpression>,
    },
    /// Static type of a relationship variable, preserving null for unmatched
    /// optional relationships.
    RelationshipType {
        /// Graph relationship variable.
        variable: String,
        /// Static graph relationship type.
        relationship_type: String,
    },
    /// Return the first non-null scalar expression.
    Coalesce {
        /// Candidate expressions in priority order.
        expressions: Vec<ScalarExpression>,
    },
    /// Return null when two scalar expressions compare equal.
    NullIf {
        /// Expression to return when it does not equal `value`.
        expression: Box<ScalarExpression>,
        /// Expression to compare against.
        value: Box<ScalarExpression>,
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
    /// Try to cast a scalar expression to a string value, returning null on failure.
    ToStringOrNull {
        /// Inner expression to cast.
        expression: Box<ScalarExpression>,
    },
    /// Try to cast a scalar expression to an integer value, returning null on failure.
    ToIntegerOrNull {
        /// Inner expression to cast.
        expression: Box<ScalarExpression>,
    },
    /// Try to cast a scalar expression to a floating-point value, returning null on failure.
    ToFloatOrNull {
        /// Inner expression to cast.
        expression: Box<ScalarExpression>,
    },
    /// Try to cast a scalar expression to a boolean value, returning null on failure.
    ToBooleanOrNull {
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
    /// Square root of a numeric scalar expression.
    Sqrt {
        /// Inner expression.
        expression: Box<ScalarExpression>,
    },
    /// Sign of a numeric scalar expression.
    Sign {
        /// Inner expression.
        expression: Box<ScalarExpression>,
    },
    /// Exponential function of a numeric scalar expression.
    Exp {
        /// Inner expression.
        expression: Box<ScalarExpression>,
    },
    /// Natural logarithm of a numeric scalar expression.
    Log {
        /// Inner expression.
        expression: Box<ScalarExpression>,
    },
    /// Base-10 logarithm of a numeric scalar expression.
    Log10 {
        /// Inner expression.
        expression: Box<ScalarExpression>,
    },
    /// Sine of a numeric scalar expression in radians.
    Sin {
        /// Inner expression.
        expression: Box<ScalarExpression>,
    },
    /// Cosine of a numeric scalar expression in radians.
    Cos {
        /// Inner expression.
        expression: Box<ScalarExpression>,
    },
    /// Tangent of a numeric scalar expression in radians.
    Tan {
        /// Inner expression.
        expression: Box<ScalarExpression>,
    },
    /// Cotangent of a numeric scalar expression in radians.
    Cot {
        /// Inner expression.
        expression: Box<ScalarExpression>,
    },
    /// Arc sine of a numeric scalar expression.
    Asin {
        /// Inner expression.
        expression: Box<ScalarExpression>,
    },
    /// Arc cosine of a numeric scalar expression.
    Acos {
        /// Inner expression.
        expression: Box<ScalarExpression>,
    },
    /// Arc tangent of a numeric scalar expression.
    Atan {
        /// Inner expression.
        expression: Box<ScalarExpression>,
    },
    /// Arc tangent of `y / x`, preserving quadrant.
    Atan2 {
        /// Y coordinate expression.
        y: Box<ScalarExpression>,
        /// X coordinate expression.
        x: Box<ScalarExpression>,
    },
    /// Convert radians to degrees.
    Degrees {
        /// Inner expression.
        expression: Box<ScalarExpression>,
    },
    /// Convert degrees to radians.
    Radians {
        /// Inner expression.
        expression: Box<ScalarExpression>,
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

/// Read-only scoped graph pattern counted by `COUNT { ... }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CountSubqueryPattern {
    /// Count rows produced by a relationship-backed scoped pattern.
    Relationships(ExistsPatternPredicate),
    /// Count rows produced by one or more scoped node patterns.
    Nodes {
        /// Node variables introduced by the count subquery. Previously bound
        /// outer variables are not repeated here.
        nodes: Vec<NodePattern>,
        /// Inline node property predicates applied inside the count subquery.
        predicates: Vec<PropertyPredicate>,
        /// Scoped `WHERE` predicate expression applied inside the count
        /// subquery after local node bindings are introduced.
        predicate: Option<Box<PredicateExpression>>,
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
    /// Exponentiation.
    Power,
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

    /// Returns the tabular output name rendered for this projection.
    #[must_use]
    pub fn output_name(&self) -> String {
        match self {
            Self::Property { property, alias } => alias
                .clone()
                .unwrap_or_else(|| format!("{}_{}", property.variable, property.property)),
            Self::Key { alias, .. }
            | Self::ElementId { alias, .. }
            | Self::NodeLabels { alias, .. }
            | Self::PropertyKeys { alias, .. }
            | Self::RelationshipType { alias, .. }
            | Self::Literal { alias, .. }
            | Self::LiteralList { alias, .. }
            | Self::Expression { alias, .. }
            | Self::CountAll { alias }
            | Self::Aggregate { alias, .. } => alias.clone(),
        }
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
    /// `MEDIAN(property)`.
    Median,
    /// `STDEV(property)` / sample standard deviation.
    StdDev,
    /// `STDEVP(property)` / population standard deviation.
    StdDevP,
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
    /// Aggregate a graph property only when another graph binding is present.
    PresenceGatedProperty {
        /// Graph property to aggregate.
        property: PropertyRef,
        /// Graph variable whose binding controls nullability.
        presence_variable: String,
    },
    /// Aggregate a scalar expression.
    Expression(ScalarExpression),
    /// Aggregate a graph variable by its mapped stable key column.
    VariableKey {
        /// Graph variable to aggregate.
        variable: String,
    },
    /// Aggregate a graph variable key only when another graph binding is
    /// present. Used for relationship endpoint aggregates over optional
    /// relationships.
    PresenceGatedVariableKey {
        /// Graph variable whose key is aggregated.
        variable: String,
        /// Graph variable whose binding controls nullability.
        presence_variable: String,
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
    /// Optional graph variable whose binding controls nullability.
    pub presence_variable: Option<String>,
}

/// Existential graph pattern predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistsPatternPredicate {
    /// Node variables introduced by the existential pattern. Previously bound
    /// outer variables are not repeated here.
    pub nodes: Vec<NodePattern>,
    /// Relationship chain tested by the existential predicate.
    pub relationships: Vec<RelationshipPattern>,
    /// Inline node/relationship property predicates applied inside the
    /// existential subquery.
    pub predicates: Vec<PropertyPredicate>,
    /// Scoped `WHERE` predicate expression applied inside the existential
    /// subquery after local node and relationship bindings are introduced.
    pub predicate: Option<Box<PredicateExpression>>,
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
    /// Node indices introduced by the optional pattern.
    pub node_indices: Vec<usize>,
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
    /// Leaf existential one-hop pattern test.
    ExistsPattern(ExistsPatternPredicate),
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
    /// Boolean exclusive disjunction.
    Xor {
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
    /// Boolean exclusive disjunction.
    Xor {
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
    /// Order by the static property keys of a graph variable.
    PropertyKeys {
        /// Graph variable.
        variable: String,
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
    /// Optional explicit null placement.
    pub nulls: Option<NullOrder>,
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

impl GraphPlan {
    /// Returns the tabular output names rendered for this plan's projections.
    #[must_use]
    pub fn projection_output_names(&self) -> Vec<String> {
        self.projections
            .iter()
            .map(Projection::output_name)
            .collect()
    }
}

/// Read-only virtual graph query shape consumed by SQL lowering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphQuery {
    /// A single graph query plan.
    Plan(GraphPlan),
    /// A top-level set union of graph query plans.
    Union(GraphUnion),
}

/// Top-level `UNION` / `UNION ALL` over graph plans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphUnion {
    /// Initial branch before the first `UNION` operator.
    pub first: GraphPlan,
    /// Subsequent branches and their leading union operator.
    pub branches: Vec<GraphUnionBranch>,
    /// Optional outer projection applied after all union branches are combined.
    pub outer_projection: Option<GraphUnionOuterProjection>,
    /// Whether the union result should be deduplicated after all branches are combined.
    pub distinct: bool,
    /// Outer ordering applied after all union branches have been combined.
    pub order_by: Vec<OrderKey>,
    /// Outer row offset applied after all union branches have been combined.
    pub skip: Option<u64>,
    /// Outer row limit applied after all union branches have been combined.
    pub limit: Option<u64>,
}

/// One branch after a `UNION` or `UNION ALL` operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphUnionBranch {
    /// Whether this branch uses `UNION ALL` and preserves duplicates.
    pub all: bool,
    /// Branch plan.
    pub plan: GraphPlan,
}

/// Projection applied after a graph union has combined all branch rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphUnionOuterProjection {
    /// Select-list items rendered by the outer union query.
    pub items: Vec<GraphUnionOuterProjectionItem>,
    /// Branch output columns used to group the outer union query.
    pub group_by: Vec<String>,
}

impl GraphUnionOuterProjection {
    /// Returns the tabular output names rendered for this outer projection.
    #[must_use]
    pub fn output_names(&self) -> Vec<String> {
        self.items
            .iter()
            .map(GraphUnionOuterProjectionItem::output_name)
            .collect()
    }
}

/// One select-list item projected after a graph union.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphUnionOuterProjectionItem {
    /// Re-project a column produced by every union branch.
    Column {
        /// Branch output column name.
        name: String,
    },
    /// Count all combined rows.
    CountAll {
        /// Output alias for the count projection.
        alias: String,
    },
    /// Aggregate a column produced by every union branch.
    Aggregate {
        /// Aggregate function.
        function: AggregateFunction,
        /// Branch output column to aggregate.
        source: String,
        /// Whether the aggregate applies distinct semantics.
        distinct: bool,
        /// Output alias.
        alias: String,
    },
}

impl GraphUnionOuterProjectionItem {
    /// Returns the output name rendered for this outer projection item.
    #[must_use]
    pub fn output_name(&self) -> String {
        match self {
            Self::Column { name } => name.clone(),
            Self::CountAll { alias } | Self::Aggregate { alias, .. } => alias.clone(),
        }
    }
}
