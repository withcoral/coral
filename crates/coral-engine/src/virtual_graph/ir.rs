/// Direction of a relationship pattern relative to its left and right nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// `(left)-[:TYPE]->(right)`.
    Outgoing,
    /// `(left)<-[:TYPE]-(right)`.
    Incoming,
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
}

/// Literal value supported by the initial graph IR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Literal {
    /// String literal.
    String(String),
    /// Signed integer literal.
    Integer(i64),
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

/// Ordering key in the shared graph IR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderKey {
    /// Property to order by.
    pub property: PropertyRef,
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
    /// Ordering expressions.
    pub order_by: Vec<OrderKey>,
    /// Optional row offset.
    pub skip: Option<u64>,
    /// Optional row limit.
    pub limit: Option<u64>,
}
