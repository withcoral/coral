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

/// Boolean predicate expression over graph properties.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateExpression {
    /// Boolean constant.
    Boolean(bool),
    /// Leaf property comparison.
    Comparison(PropertyPredicate),
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

/// Ordering key in the shared graph IR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderExpression {
    /// Order by a graph property.
    Property(PropertyRef),
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
    /// Whether duplicate projected rows should be removed.
    pub distinct: bool,
    /// Projected expressions.
    pub projections: Vec<Projection>,
    /// Conjunctive property predicates.
    pub predicates: Vec<PropertyPredicate>,
    /// Optional boolean predicate tree for expressions that cannot be flattened
    /// into the conjunctive predicate vector.
    pub predicate: Option<PredicateExpression>,
    /// Ordering expressions.
    pub order_by: Vec<OrderKey>,
    /// Optional row offset.
    pub skip: Option<u64>,
    /// Optional row limit.
    pub limit: Option<u64>,
}
