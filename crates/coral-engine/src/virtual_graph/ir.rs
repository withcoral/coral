//! Graph-plan intermediate representation: the shared, frontend- and
//! backend-agnostic data model that the openCypher and GraphQL frontends compile
//! into and that the `GraphPlanValidator` and SQL `SqlRenderer` consume. Defines
//! `GraphQuery` (a single `GraphPlan` or a `GraphUnion`), node/relationship
//! patterns and `Direction`, projections, scalar expressions, the predicate
//! family (property, key, element-id, presence, EXISTS/COUNT patterns),
//! aggregates, ordering, and literals. Mostly plain data types with a few
//! convenience impls; holds no parsing, validation, or lowering logic.

use std::collections::BTreeSet;

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
    /// Literal list value used when a statically folded outer list contains
    /// list-valued elements.
    List(Vec<Literal>),
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
    /// List elements whose non-null members are strings.
    StringList,
    /// List elements whose non-null members are signed integers.
    IntegerList,
    /// List elements whose non-null members are floating-point values.
    FloatList,
    /// List elements whose non-null members are booleans.
    BooleanList,
}

impl LiteralListElementType {
    /// Returns the scalar element type for a list-valued element type.
    #[must_use]
    pub fn list_element_type(self) -> Option<Self> {
        match self {
            Self::StringList => Some(Self::String),
            Self::IntegerList => Some(Self::Integer),
            Self::FloatList => Some(Self::Float),
            Self::BooleanList => Some(Self::Boolean),
            Self::String | Self::Integer | Self::Float | Self::Boolean => None,
        }
    }

    /// Returns the list-valued element type for scalar list members.
    #[must_use]
    pub fn list_of(element_type: Self) -> Option<Self> {
        match element_type {
            Self::String => Some(Self::StringList),
            Self::Integer => Some(Self::IntegerList),
            Self::Float => Some(Self::FloatList),
            Self::Boolean => Some(Self::BooleanList),
            Self::StringList | Self::IntegerList | Self::FloatList | Self::BooleanList => None,
        }
    }
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
    /// Compare against an adapter-marked string that validation may coerce to
    /// the left-hand property's temporal kind.
    TemporalCoercion {
        /// Original string operand from the adapter.
        source: String,
    },
    /// Compare against adapter-marked strings that validation may coerce to
    /// the left-hand property's temporal kind for list membership.
    TemporalCoercionList(Vec<String>),
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
    /// Scalar value exported by a non-terminal graph stage.
    StageValue {
        /// Export alias visible to later stages.
        alias: String,
    },
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
    /// Runtime concatenation of two list expressions with compatible element
    /// types.
    ListConcat {
        /// Left list operand.
        left: Box<ScalarExpression>,
        /// Right list operand.
        right: Box<ScalarExpression>,
    },
    /// Runtime index into a list-valued scalar expression.
    ListIndex {
        /// List-valued expression.
        list: Box<ScalarExpression>,
        /// Zero-based Cypher index.
        index: i64,
        /// Known scalar element type returned by this index.
        element_type: LiteralListElementType,
    },
    /// Ordered mapped keys for graph variables that make up a materialized path
    /// element list such as `nodes(path)` or `relationships(path)`.
    GraphKeyList {
        /// Graph variables in path order.
        variables: Vec<String>,
    },
    /// Returnable fixed-hop path value represented by ordered mapped node and
    /// relationship keys.
    PathValue {
        /// Node graph variables in path order.
        node_variables: Vec<String>,
        /// Relationship graph variables in path order.
        relationship_variables: Vec<String>,
    },
    /// A boolean predicate used as a scalar value.
    Predicate(Box<PredicateExpression>),
    /// Temporal value expression.
    Temporal(TemporalExpr),
    /// Count rows produced by a read-only graph subquery.
    CountSubquery {
        /// Scoped graph pattern counted by the subquery.
        pattern: Box<CountSubqueryPattern>,
        /// Optional scalar projection used for `RETURN DISTINCT scalar` inside
        /// the counted subquery. Distinct counting is row-based and preserves a
        /// returned `NULL` as one distinct row.
        distinct_target: Option<Box<ScalarExpression>>,
    },
    /// Collect scalar values produced by a read-only graph subquery.
    CollectSubquery {
        /// Scoped graph pattern that produces rows for the collection.
        pattern: Box<CountSubqueryPattern>,
        /// Scalar expression returned by each scoped subquery row.
        target: Box<ScalarExpression>,
        /// Whether the scoped return applies row-level distinctness before collection.
        distinct: bool,
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
    /// Return all zero-based positions where one scalar string expression
    /// occurs inside another.
    StringIndices {
        /// Source expression.
        expression: Box<ScalarExpression>,
        /// Substring expression.
        pattern: Box<ScalarExpression>,
    },
    /// Left-pad a scalar string expression to a target length.
    LPad {
        /// Source expression.
        expression: Box<ScalarExpression>,
        /// Target character length.
        length: Box<ScalarExpression>,
        /// Fill expression.
        fill: Box<ScalarExpression>,
    },
    /// Right-pad a scalar string expression to a target length.
    RPad {
        /// Source expression.
        expression: Box<ScalarExpression>,
        /// Target character length.
        length: Box<ScalarExpression>,
        /// Fill expression.
        fill: Box<ScalarExpression>,
    },
    /// Test whether one scalar string expression contains another.
    StringContains {
        /// Source expression.
        expression: Box<ScalarExpression>,
        /// Substring expression.
        pattern: Box<ScalarExpression>,
    },
    /// Test whether a scalar string expression starts with another.
    StringStartsWith {
        /// Source expression.
        expression: Box<ScalarExpression>,
        /// Prefix expression.
        pattern: Box<ScalarExpression>,
    },
    /// Test whether a scalar string expression ends with another.
    StringEndsWith {
        /// Source expression.
        expression: Box<ScalarExpression>,
        /// Suffix expression.
        pattern: Box<ScalarExpression>,
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
    /// Test whether a floating-point scalar expression is NaN.
    IsNaN {
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

/// Temporal component units supported by Cypher component access.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporalComponentUnit {
    /// Calendar year.
    Year,
    /// Calendar quarter.
    Quarter,
    /// Calendar month.
    Month,
    /// ISO week number.
    Week,
    /// Calendar day of month.
    Day,
    /// Hour of day.
    Hour,
    /// Minute of hour.
    Minute,
    /// Second of minute.
    Second,
    /// Millisecond fraction.
    Millisecond,
    /// Microsecond fraction.
    Microsecond,
    /// Total years in a duration's month group.
    Years,
    /// Total quarters in a duration's month group.
    Quarters,
    /// Total months in a duration's month group.
    Months,
    /// Total weeks in a duration's day group.
    Weeks,
    /// Total days in a duration's day group.
    Days,
    /// Total hours in a duration's nanosecond group.
    Hours,
    /// Total minutes in a duration's nanosecond group.
    Minutes,
    /// Total seconds in a duration's nanosecond group.
    Seconds,
    /// Total milliseconds in a duration's nanosecond group.
    Milliseconds,
    /// Total microseconds in a duration's nanosecond group.
    Microseconds,
    /// Total nanoseconds in a duration's nanosecond group.
    Nanoseconds,
    /// Quarter component within a duration's current year.
    QuartersOfYear,
    /// Month component within a duration's current quarter.
    MonthsOfQuarter,
    /// Month component within a duration's current year.
    MonthsOfYear,
    /// Day component within a duration's current week.
    DaysOfWeek,
    /// Minute component within a duration's current hour.
    MinutesOfHour,
    /// Second component within a duration's current minute.
    SecondsOfMinute,
    /// Millisecond component within a duration's current second.
    MillisecondsOfSecond,
    /// Microsecond component within a duration's current second.
    MicrosecondsOfSecond,
    /// Nanosecond component within a duration's current second.
    NanosecondsOfSecond,
}

/// Zoned datetime accessors supported by Cypher property access.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZonedDateTimeAccessor {
    /// Declared fixed-offset or IANA timezone identifier.
    Timezone,
    /// Effective offset at the instant, formatted as `+HH:MM`.
    Offset,
    /// Effective offset at the instant in total seconds.
    OffsetSeconds,
    /// Effective offset at the instant in total minutes.
    OffsetMinutes,
    /// Unix epoch seconds for the instant.
    EpochSeconds,
    /// Unix epoch milliseconds for the instant.
    EpochMillis,
}

/// Duration decomposition and unit-total functions supported by the temporal IR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporalDurationUnit {
    /// Calendar-aware duration between two temporal values.
    Between,
    /// Calendar month duration between two temporal values.
    Months,
    /// Whole-day duration between two temporal instants.
    Days,
    /// Total seconds duration between two temporal instants.
    Seconds,
}

/// Temporal scalar expressions in the shared graph IR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemporalExpr {
    /// Construct a native DATE value from year, month, and day components.
    MakeDate {
        /// Year component.
        year: Box<ScalarExpression>,
        /// Month component.
        month: Box<ScalarExpression>,
        /// Day component.
        day: Box<ScalarExpression>,
    },
    /// Construct a native DATE value from an ISO date string.
    DateFromString {
        /// ISO date string expression.
        text: Box<ScalarExpression>,
    },
    /// Construct a native local date-time value from date and time components.
    MakeLocalDateTime {
        /// Year component.
        year: Box<ScalarExpression>,
        /// Month component.
        month: Box<ScalarExpression>,
        /// Day component.
        day: Box<ScalarExpression>,
        /// Hour component.
        hour: Box<ScalarExpression>,
        /// Minute component.
        minute: Box<ScalarExpression>,
        /// Second component.
        second: Box<ScalarExpression>,
        /// Millisecond component.
        millisecond: Box<ScalarExpression>,
        /// Microsecond component.
        microsecond: Box<ScalarExpression>,
        /// Nanosecond component.
        nanosecond: Box<ScalarExpression>,
    },
    /// Construct a native local date-time value from an ISO local date-time string.
    LocalDateTimeFromString {
        /// ISO local date-time string expression.
        text: Box<ScalarExpression>,
    },
    /// Construct a native zoned date-time value from date/time components and a timezone.
    MakeZonedDateTime {
        /// Year component.
        year: Box<ScalarExpression>,
        /// Month component.
        month: Box<ScalarExpression>,
        /// Day component.
        day: Box<ScalarExpression>,
        /// Hour component.
        hour: Box<ScalarExpression>,
        /// Minute component.
        minute: Box<ScalarExpression>,
        /// Second component.
        second: Box<ScalarExpression>,
        /// Millisecond component.
        millisecond: Box<ScalarExpression>,
        /// Microsecond component.
        microsecond: Box<ScalarExpression>,
        /// Nanosecond component.
        nanosecond: Box<ScalarExpression>,
        /// Arrow/DataFusion timezone identifier, either a fixed offset or IANA name.
        timezone: String,
    },
    /// Construct a native zoned date-time value from an ISO date-time string and timezone.
    ZonedDateTimeFromString {
        /// ISO date-time string expression, with any Cypher bracketed zone suffix removed.
        text: Box<ScalarExpression>,
        /// Arrow/DataFusion timezone identifier, either a fixed offset or IANA name.
        timezone: String,
    },
    /// Construct a native local time value from time components.
    MakeLocalTime {
        /// Hour component.
        hour: Box<ScalarExpression>,
        /// Minute component.
        minute: Box<ScalarExpression>,
        /// Second component.
        second: Box<ScalarExpression>,
        /// Millisecond component.
        millisecond: Box<ScalarExpression>,
        /// Microsecond component.
        microsecond: Box<ScalarExpression>,
        /// Nanosecond component.
        nanosecond: Box<ScalarExpression>,
    },
    /// Construct a native local time value from an ISO local time string.
    LocalTimeFromString {
        /// ISO local time string expression.
        text: Box<ScalarExpression>,
    },
    /// Construct a native duration value from folded calendar and clock components.
    MakeDuration {
        /// Calendar month component, with years folded into months.
        months: i64,
        /// Calendar day component, with weeks folded into days.
        days: i64,
        /// Whole-second clock component.
        seconds: i64,
        /// Nanosecond clock component within the current second.
        nanos: i64,
    },
    /// Construct a duration between temporals, optionally truncated to one unit family.
    DurationInUnits {
        /// Duration function to evaluate.
        unit: TemporalDurationUnit,
        /// Start temporal expression.
        start: Box<ScalarExpression>,
        /// End temporal expression.
        end: Box<ScalarExpression>,
    },
    /// Extract an integer component from a native temporal value.
    Component {
        /// Temporal value expression.
        expression: Box<ScalarExpression>,
        /// Component unit to extract.
        unit: TemporalComponentUnit,
    },
    /// Access a zoned datetime's timezone, offset, or epoch instant component.
    ZonedDateTimeAccessor {
        /// Zoned datetime value expression.
        expression: Box<ScalarExpression>,
        /// Accessor kind.
        accessor: ZonedDateTimeAccessor,
        /// Compile-time timezone identifier for `timezone` access.
        timezone: Option<String>,
    },
}

/// Supported temporal scalar kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TemporalKind {
    /// Native DATE value.
    Date,
    /// Native local date-time value.
    LocalDateTime,
    /// Native zoned date-time value.
    ZonedDateTime,
    /// Native local time value.
    LocalTime,
    /// Native duration value.
    Duration,
}

impl TemporalKind {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Date => "date",
            Self::LocalDateTime => "localdatetime",
            Self::ZonedDateTime => "datetime",
            Self::LocalTime => "localtime",
            Self::Duration => "duration",
        }
    }
}

impl TemporalComponentUnit {
    pub(super) fn component_name(self) -> &'static str {
        match self {
            Self::Year => "year",
            Self::Quarter => "quarter",
            Self::Month => "month",
            Self::Week => "week",
            Self::Day => "day",
            Self::Hour => "hour",
            Self::Minute => "minute",
            Self::Second => "second",
            Self::Millisecond => "millisecond",
            Self::Microsecond => "microsecond",
            Self::Years => "years",
            Self::Quarters => "quarters",
            Self::Months => "months",
            Self::Weeks => "weeks",
            Self::Days => "days",
            Self::Hours => "hours",
            Self::Minutes => "minutes",
            Self::Seconds => "seconds",
            Self::Milliseconds => "milliseconds",
            Self::Microseconds => "microseconds",
            Self::Nanoseconds => "nanoseconds",
            Self::QuartersOfYear => "quartersOfYear",
            Self::MonthsOfQuarter => "monthsOfQuarter",
            Self::MonthsOfYear => "monthsOfYear",
            Self::DaysOfWeek => "daysOfWeek",
            Self::MinutesOfHour => "minutesOfHour",
            Self::SecondsOfMinute => "secondsOfMinute",
            Self::MillisecondsOfSecond => "millisecondsOfSecond",
            Self::MicrosecondsOfSecond => "microsecondsOfSecond",
            Self::NanosecondsOfSecond => "nanosecondsOfSecond",
        }
    }

    pub(super) fn date_part_unit(self) -> &'static str {
        self.component_name()
    }

    pub(super) fn supports_kind(self, kind: TemporalKind) -> bool {
        match self {
            Self::Year | Self::Quarter | Self::Month | Self::Week | Self::Day => {
                matches!(
                    kind,
                    TemporalKind::Date | TemporalKind::LocalDateTime | TemporalKind::ZonedDateTime
                )
            }
            Self::Hour | Self::Minute | Self::Second | Self::Millisecond | Self::Microsecond => {
                matches!(
                    kind,
                    TemporalKind::LocalDateTime
                        | TemporalKind::ZonedDateTime
                        | TemporalKind::LocalTime
                )
            }
            Self::Years
            | Self::Quarters
            | Self::Months
            | Self::Weeks
            | Self::Days
            | Self::Hours
            | Self::Minutes
            | Self::Seconds
            | Self::Milliseconds
            | Self::Microseconds
            | Self::Nanoseconds
            | Self::QuartersOfYear
            | Self::MonthsOfQuarter
            | Self::MonthsOfYear
            | Self::DaysOfWeek
            | Self::MinutesOfHour
            | Self::SecondsOfMinute
            | Self::MillisecondsOfSecond
            | Self::MicrosecondsOfSecond
            | Self::NanosecondsOfSecond => matches!(kind, TemporalKind::Duration),
        }
    }

    pub(super) fn is_duration_component(self) -> bool {
        matches!(
            self,
            Self::Years
                | Self::Quarters
                | Self::Months
                | Self::Weeks
                | Self::Days
                | Self::Hours
                | Self::Minutes
                | Self::Seconds
                | Self::Milliseconds
                | Self::Microseconds
                | Self::Nanoseconds
                | Self::QuartersOfYear
                | Self::MonthsOfQuarter
                | Self::MonthsOfYear
                | Self::DaysOfWeek
                | Self::MinutesOfHour
                | Self::SecondsOfMinute
                | Self::MillisecondsOfSecond
                | Self::MicrosecondsOfSecond
                | Self::NanosecondsOfSecond
        )
    }
}

impl ZonedDateTimeAccessor {
    pub(super) fn accessor_name(self) -> &'static str {
        match self {
            Self::Timezone => "timezone",
            Self::Offset => "offset",
            Self::OffsetSeconds => "offsetSeconds",
            Self::OffsetMinutes => "offsetMinutes",
            Self::EpochSeconds => "epochSeconds",
            Self::EpochMillis => "epochMillis",
        }
    }

    pub(super) fn is_string(self) -> bool {
        matches!(self, Self::Timezone | Self::Offset)
    }
}

impl TemporalDurationUnit {
    pub(super) fn function_name(self) -> &'static str {
        match self {
            Self::Between => "duration.between",
            Self::Months => "duration.inMonths",
            Self::Days => "duration.inDays",
            Self::Seconds => "duration.inSeconds",
        }
    }
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
    /// `PERCENTILECONT(property, percentile)`.
    PercentileCont {
        /// Requested percentile in the inclusive range `[0.0, 1.0]`.
        percentile: OrderedFloat<f64>,
    },
    /// `PERCENTILEDISC(property, percentile)`.
    PercentileDisc {
        /// Requested percentile in the inclusive range `[0.0, 1.0]`.
        percentile: OrderedFloat<f64>,
    },
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

impl CountSubqueryPattern {
    /// Returns whether this scoped count pattern references variables outside
    /// the nodes and relationships introduced by the pattern itself.
    #[must_use]
    pub(crate) fn references_outer_variables(&self) -> bool {
        count_subquery_pattern_references_outside_scope(self, &BTreeSet::new())
    }
}

impl ExistsPatternPredicate {
    /// Returns whether this scoped existential pattern references variables
    /// outside the nodes and relationships introduced by the pattern itself.
    #[must_use]
    pub(crate) fn references_outer_variables(&self) -> bool {
        exists_pattern_references_outside_scope(self, &BTreeSet::new())
    }
}

fn count_subquery_pattern_references_outside_scope(
    pattern: &CountSubqueryPattern,
    scope: &BTreeSet<String>,
) -> bool {
    match pattern {
        CountSubqueryPattern::Relationships(predicate) => {
            exists_pattern_references_outside_scope(predicate, scope)
        }
        CountSubqueryPattern::Nodes {
            nodes,
            predicates,
            predicate,
        } => {
            let mut local_scope = scope.clone();
            for node in nodes {
                local_scope.insert(node.variable.clone());
            }
            property_predicate_list_references_outside_scope(predicates, &local_scope)
                || predicate.as_deref().is_some_and(|predicate| {
                    predicate_expression_references_outside_scope(predicate, &local_scope)
                })
        }
    }
}

fn collect_subquery_references_outside_scope(
    pattern: &CountSubqueryPattern,
    target: &ScalarExpression,
    scope: &BTreeSet<String>,
) -> bool {
    if count_subquery_pattern_references_outside_scope(pattern, scope) {
        return true;
    }
    let mut local_scope = scope.clone();
    match pattern {
        CountSubqueryPattern::Relationships(predicate) => {
            for node in &predicate.nodes {
                local_scope.insert(node.variable.clone());
            }
            for relationship in &predicate.relationships {
                if let Some(variable) = &relationship.variable {
                    local_scope.insert(variable.clone());
                }
            }
        }
        CountSubqueryPattern::Nodes { nodes, .. } => {
            for node in nodes {
                local_scope.insert(node.variable.clone());
            }
        }
    }
    scalar_expression_references_outside_scope(target, &local_scope)
}

fn exists_pattern_references_outside_scope(
    predicate: &ExistsPatternPredicate,
    scope: &BTreeSet<String>,
) -> bool {
    let mut local_scope = scope.clone();
    for node in &predicate.nodes {
        local_scope.insert(node.variable.clone());
    }
    for relationship in &predicate.relationships {
        if let Some(variable) = &relationship.variable {
            local_scope.insert(variable.clone());
        }
    }
    predicate.relationships.iter().any(|relationship| {
        variable_references_outside_scope(&relationship.left, &local_scope)
            || variable_references_outside_scope(&relationship.right, &local_scope)
    }) || property_predicate_list_references_outside_scope(&predicate.predicates, &local_scope)
        || predicate.predicate.as_deref().is_some_and(|predicate| {
            predicate_expression_references_outside_scope(predicate, &local_scope)
        })
}

fn property_predicate_list_references_outside_scope(
    predicates: &[PropertyPredicate],
    scope: &BTreeSet<String>,
) -> bool {
    predicates
        .iter()
        .any(|predicate| property_predicate_references_outside_scope(predicate, scope))
}

fn property_predicate_references_outside_scope(
    predicate: &PropertyPredicate,
    scope: &BTreeSet<String>,
) -> bool {
    variable_references_outside_scope(&predicate.property.variable, scope)
        || predicate_rhs_references_outside_scope(&predicate.rhs, scope)
}

fn predicate_rhs_references_outside_scope(rhs: &PredicateRhs, scope: &BTreeSet<String>) -> bool {
    match rhs {
        PredicateRhs::Property(property) => {
            variable_references_outside_scope(&property.variable, scope)
        }
        PredicateRhs::Key { variable } | PredicateRhs::ElementId { variable } => {
            variable_references_outside_scope(variable, scope)
        }
        PredicateRhs::Literal(_)
        | PredicateRhs::TemporalCoercion { .. }
        | PredicateRhs::TemporalCoercionList(_)
        | PredicateRhs::List(_) => false,
    }
}

fn scalar_predicate_rhs_references_outside_scope(
    rhs: &ScalarPredicateRhs,
    scope: &BTreeSet<String>,
) -> bool {
    match rhs {
        ScalarPredicateRhs::Expression(expression) => {
            scalar_expression_references_outside_scope(expression, scope)
        }
        ScalarPredicateRhs::List(_) => false,
    }
}

fn predicate_expression_references_outside_scope(
    predicate: &PredicateExpression,
    scope: &BTreeSet<String>,
) -> bool {
    match predicate {
        PredicateExpression::Boolean(_) => false,
        PredicateExpression::Comparison(predicate) => {
            property_predicate_references_outside_scope(predicate, scope)
        }
        PredicateExpression::KeyComparison(predicate) => {
            variable_references_outside_scope(&predicate.variable, scope)
                || predicate_rhs_references_outside_scope(&predicate.rhs, scope)
        }
        PredicateExpression::ElementIdComparison(predicate) => {
            variable_references_outside_scope(&predicate.variable, scope)
                || predicate_rhs_references_outside_scope(&predicate.rhs, scope)
        }
        PredicateExpression::Presence(predicate) => {
            variable_references_outside_scope(&predicate.variable, scope)
        }
        PredicateExpression::PropertyKeyMembership(predicate) => {
            variable_references_outside_scope(&predicate.variable, scope)
                || predicate
                    .presence_variable
                    .as_deref()
                    .is_some_and(|variable| variable_references_outside_scope(variable, scope))
        }
        PredicateExpression::ExistsPattern(predicate) => {
            exists_pattern_references_outside_scope(predicate, scope)
        }
        PredicateExpression::ScalarComparison(predicate) => {
            scalar_expression_references_outside_scope(&predicate.lhs, scope)
                || scalar_predicate_rhs_references_outside_scope(&predicate.rhs, scope)
        }
        PredicateExpression::And { left, right }
        | PredicateExpression::Or { left, right }
        | PredicateExpression::Xor { left, right } => {
            predicate_expression_references_outside_scope(left, scope)
                || predicate_expression_references_outside_scope(right, scope)
        }
        PredicateExpression::Not { expression } => {
            predicate_expression_references_outside_scope(expression, scope)
        }
    }
}

fn scalar_expression_references_outside_scope(
    expression: &ScalarExpression,
    scope: &BTreeSet<String>,
) -> bool {
    if let Some(references_outside_scope) =
        direct_scalar_expression_references_outside_scope(expression, scope)
    {
        return references_outside_scope;
    }
    match expression {
        ScalarExpression::Literal(_)
        | ScalarExpression::LiteralList { .. }
        | ScalarExpression::TypedLiteralList { .. }
        | ScalarExpression::StageValue { .. } => false,
        ScalarExpression::Predicate(predicate) => {
            predicate_expression_references_outside_scope(predicate, scope)
        }
        ScalarExpression::CountSubquery {
            pattern,
            distinct_target,
        } => count_subquery_scalar_expression_references_outside_scope(
            pattern,
            distinct_target.as_deref(),
            scope,
        ),
        ScalarExpression::CollectSubquery {
            pattern, target, ..
        } => collect_subquery_references_outside_scope(pattern, target, scope),
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
        | ScalarExpression::ListConcat { .. }
        | ScalarExpression::ListIndex { .. }
        | ScalarExpression::Atan2 { .. }
        | ScalarExpression::Case { .. } => {
            structural_scalar_expression_references_outside_scope(expression, scope)
        }
        ScalarExpression::Property(_)
        | ScalarExpression::UndirectedEndpointProperty { .. }
        | ScalarExpression::UndirectedEndpointKey { .. }
        | ScalarExpression::UndirectedEndpointElementId { .. }
        | ScalarExpression::UndirectedEndpointLabels { .. }
        | ScalarExpression::UndirectedEndpointPropertyKeys { .. }
        | ScalarExpression::GraphKeyList { .. }
        | ScalarExpression::PathValue { .. }
        | ScalarExpression::Key { .. }
        | ScalarExpression::ElementId { .. }
        | ScalarExpression::GraphIdentity { .. }
        | ScalarExpression::GraphPresence { .. }
        | ScalarExpression::NodeLabels { .. }
        | ScalarExpression::PropertyKeys { .. }
        | ScalarExpression::RelationshipType { .. }
        | ScalarExpression::ToString { .. }
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
        | ScalarExpression::Temporal(_)
        | ScalarExpression::Negate { .. } => unreachable!(
            "direct variable and unary scalar expressions handled before scope recursion"
        ),
    }
}

fn direct_scalar_expression_references_outside_scope(
    expression: &ScalarExpression,
    scope: &BTreeSet<String>,
) -> Option<bool> {
    if let ScalarExpression::GraphKeyList { variables } = expression {
        return Some(
            variables
                .iter()
                .any(|variable| variable_references_outside_scope(variable, scope)),
        );
    }
    if let ScalarExpression::PathValue {
        node_variables,
        relationship_variables,
    } = expression
    {
        return Some(
            node_variables
                .iter()
                .chain(relationship_variables)
                .any(|variable| variable_references_outside_scope(variable, scope)),
        );
    }
    if let Some(variable) = scalar_expression_variable_reference(expression) {
        return Some(variable_references_outside_scope(variable, scope));
    }
    if let Some(operand) = scalar_expression_unary_operand(expression) {
        return Some(scalar_expression_references_outside_scope(operand, scope));
    }
    if let ScalarExpression::Temporal(temporal) = expression {
        return Some(temporal_expression_references_outside_scope(
            temporal, scope,
        ));
    }
    None
}

fn count_subquery_scalar_expression_references_outside_scope(
    pattern: &CountSubqueryPattern,
    distinct_target: Option<&ScalarExpression>,
    scope: &BTreeSet<String>,
) -> bool {
    distinct_target.map_or_else(
        || count_subquery_pattern_references_outside_scope(pattern, scope),
        |target| collect_subquery_references_outside_scope(pattern, target, scope),
    )
}

fn scalar_expression_variable_reference(expression: &ScalarExpression) -> Option<&str> {
    match expression {
        ScalarExpression::Property(property) => Some(property.variable.as_str()),
        ScalarExpression::UndirectedEndpointProperty { relationship, .. }
        | ScalarExpression::UndirectedEndpointKey { relationship, .. }
        | ScalarExpression::UndirectedEndpointElementId { relationship, .. }
        | ScalarExpression::UndirectedEndpointLabels { relationship, .. }
        | ScalarExpression::UndirectedEndpointPropertyKeys { relationship, .. } => {
            Some(relationship.as_str())
        }
        ScalarExpression::Key { variable }
        | ScalarExpression::ElementId { variable }
        | ScalarExpression::GraphIdentity { variable }
        | ScalarExpression::GraphPresence { variable }
        | ScalarExpression::NodeLabels { variable, .. }
        | ScalarExpression::PropertyKeys { variable }
        | ScalarExpression::RelationshipType { variable, .. } => Some(variable.as_str()),
        _ => None,
    }
}

fn scalar_expression_unary_operand(expression: &ScalarExpression) -> Option<&ScalarExpression> {
    match expression {
        ScalarExpression::ToString { expression }
        | ScalarExpression::ToInteger { expression }
        | ScalarExpression::ToFloat { expression }
        | ScalarExpression::ToBoolean { expression }
        | ScalarExpression::ToStringOrNull { expression }
        | ScalarExpression::ToIntegerOrNull { expression }
        | ScalarExpression::ToFloatOrNull { expression }
        | ScalarExpression::ToBooleanOrNull { expression }
        | ScalarExpression::ToLower { expression }
        | ScalarExpression::ToUpper { expression }
        | ScalarExpression::Trim { expression }
        | ScalarExpression::LTrim { expression }
        | ScalarExpression::RTrim { expression }
        | ScalarExpression::CharacterLength { expression }
        | ScalarExpression::Reverse { expression }
        | ScalarExpression::Abs { expression }
        | ScalarExpression::Ceil { expression }
        | ScalarExpression::Floor { expression }
        | ScalarExpression::Sqrt { expression }
        | ScalarExpression::Sign { expression }
        | ScalarExpression::Exp { expression }
        | ScalarExpression::Log { expression }
        | ScalarExpression::Log10 { expression }
        | ScalarExpression::Sin { expression }
        | ScalarExpression::Cos { expression }
        | ScalarExpression::Tan { expression }
        | ScalarExpression::Cot { expression }
        | ScalarExpression::Asin { expression }
        | ScalarExpression::Acos { expression }
        | ScalarExpression::Atan { expression }
        | ScalarExpression::Degrees { expression }
        | ScalarExpression::Radians { expression }
        | ScalarExpression::IsNaN { expression }
        | ScalarExpression::Negate { expression } => Some(expression),
        _ => None,
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "This exhaustive scalar IR walker keeps scope recursion total over every structural variant."
)]
fn structural_scalar_expression_references_outside_scope(
    expression: &ScalarExpression,
    scope: &BTreeSet<String>,
) -> bool {
    match expression {
        ScalarExpression::PresenceGated {
            presence_variable,
            expression,
        } => {
            variable_references_outside_scope(presence_variable, scope)
                || scalar_expression_references_outside_scope(expression, scope)
        }
        ScalarExpression::Coalesce { expressions } => expressions
            .iter()
            .any(|expression| scalar_expression_references_outside_scope(expression, scope)),
        ScalarExpression::NullIf { expression, value } => {
            scalar_expression_references_outside_scope(expression, scope)
                || scalar_expression_references_outside_scope(value, scope)
        }
        ScalarExpression::Round { expression, places } => {
            scalar_expression_references_outside_scope(expression, scope)
                || places
                    .as_deref()
                    .is_some_and(|places| scalar_expression_references_outside_scope(places, scope))
        }
        ScalarExpression::Left { expression, count }
        | ScalarExpression::Right { expression, count } => {
            scalar_expression_references_outside_scope(expression, scope)
                || scalar_expression_references_outside_scope(count, scope)
        }
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
        } => {
            scalar_expression_references_outside_scope(expression, scope)
                || scalar_expression_references_outside_scope(pattern, scope)
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
            scalar_expression_references_outside_scope(expression, scope)
                || scalar_expression_references_outside_scope(length, scope)
                || scalar_expression_references_outside_scope(fill, scope)
        }
        ScalarExpression::Replace {
            expression,
            search,
            replacement,
        } => {
            scalar_expression_references_outside_scope(expression, scope)
                || scalar_expression_references_outside_scope(search, scope)
                || scalar_expression_references_outside_scope(replacement, scope)
        }
        ScalarExpression::Substring {
            expression,
            start,
            length,
        } => {
            scalar_expression_references_outside_scope(expression, scope)
                || scalar_expression_references_outside_scope(start, scope)
                || length
                    .as_deref()
                    .is_some_and(|length| scalar_expression_references_outside_scope(length, scope))
        }
        ScalarExpression::Arithmetic { left, right, .. }
        | ScalarExpression::ListConcat { left, right }
        | ScalarExpression::Atan2 { y: left, x: right } => {
            scalar_expression_references_outside_scope(left, scope)
                || scalar_expression_references_outside_scope(right, scope)
        }
        ScalarExpression::ListIndex { list, .. } => {
            scalar_expression_references_outside_scope(list, scope)
        }
        ScalarExpression::Case {
            alternatives,
            else_expression,
        } => {
            alternatives.iter().any(|alternative| {
                predicate_expression_references_outside_scope(&alternative.when, scope)
                    || scalar_expression_references_outside_scope(&alternative.then, scope)
            }) || else_expression.as_deref().is_some_and(|else_expression| {
                scalar_expression_references_outside_scope(else_expression, scope)
            })
        }
        _ => unreachable!("non-structural scalar expressions handled before scope recursion"),
    }
}

fn temporal_expression_references_outside_scope(
    expression: &TemporalExpr,
    scope: &BTreeSet<String>,
) -> bool {
    match expression {
        TemporalExpr::MakeDate { year, month, day } => {
            scalar_expression_references_outside_scope(year, scope)
                || scalar_expression_references_outside_scope(month, scope)
                || scalar_expression_references_outside_scope(day, scope)
        }
        TemporalExpr::DateFromString { text }
        | TemporalExpr::LocalDateTimeFromString { text }
        | TemporalExpr::ZonedDateTimeFromString { text, .. }
        | TemporalExpr::LocalTimeFromString { text } => {
            scalar_expression_references_outside_scope(text, scope)
        }
        TemporalExpr::MakeLocalDateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            millisecond,
            microsecond,
            nanosecond,
        } => [
            year,
            month,
            day,
            hour,
            minute,
            second,
            millisecond,
            microsecond,
            nanosecond,
        ]
        .iter()
        .any(|expression| scalar_expression_references_outside_scope(expression, scope)),
        TemporalExpr::MakeZonedDateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            millisecond,
            microsecond,
            nanosecond,
            ..
        } => [
            year,
            month,
            day,
            hour,
            minute,
            second,
            millisecond,
            microsecond,
            nanosecond,
        ]
        .iter()
        .any(|expression| scalar_expression_references_outside_scope(expression, scope)),
        TemporalExpr::MakeLocalTime {
            hour,
            minute,
            second,
            millisecond,
            microsecond,
            nanosecond,
        } => [hour, minute, second, millisecond, microsecond, nanosecond]
            .iter()
            .any(|expression| scalar_expression_references_outside_scope(expression, scope)),
        TemporalExpr::MakeDuration { .. } => false,
        TemporalExpr::DurationInUnits { start, end, .. } => {
            scalar_expression_references_outside_scope(start, scope)
                || scalar_expression_references_outside_scope(end, scope)
        }
        TemporalExpr::Component { expression, .. }
        | TemporalExpr::ZonedDateTimeAccessor { expression, .. } => {
            scalar_expression_references_outside_scope(expression, scope)
        }
    }
}

fn variable_references_outside_scope(variable: &str, scope: &BTreeSet<String>) -> bool {
    !scope.contains(variable)
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
    /// Order by `COUNT(*)` without requiring the count to be projected.
    CountAll,
    /// Order by an aggregate expression without requiring it to be projected.
    Aggregate {
        /// Aggregate function.
        function: AggregateFunction,
        /// Value to aggregate.
        target: AggregateTarget,
        /// Whether the aggregate applies distinct semantics.
        distinct: bool,
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
    /// A graph-free row source produced by `UNWIND <list> AS variable`.
    Unwind(GraphUnwind),
    /// A dynamic `UNWIND` row source feeding a final graph plan.
    UnwindPipeline(GraphUnwindPipeline),
    /// A staged chain of graph plans where non-terminal stages export row keys
    /// consumed by the final plan.
    Staged(GraphStagedQuery),
    /// A staged aggregate list that is expanded with `UNWIND` before the final
    /// graph plan consumes the row values.
    StagedUnwind(Box<GraphStagedUnwindQuery>),
    /// A top-level set union of graph query plans.
    Union(GraphUnion),
}

/// Graph-free row source that expands a list expression into rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphUnwind {
    /// Optional single-row input stage that produces list-valued aliases.
    pub input: Option<GraphUnwindInput>,
    /// List expression rendered as the row source.
    pub list: ScalarExpression,
    /// Known non-null element type of the `UNWIND` source.
    pub element_type: LiteralListElementType,
    /// Variable bound by the `UNWIND` clause.
    pub variable: String,
    /// Terminal projections over the bound `UNWIND` value.
    pub projections: Vec<GraphUnwindProjection>,
}

impl GraphUnwind {
    /// Returns the tabular output names rendered for this row source.
    #[must_use]
    pub fn projection_output_names(&self) -> Vec<String> {
        self.projections
            .iter()
            .map(GraphUnwindProjection::output_name)
            .collect()
    }
}

/// Single-row input stage for a dynamic `UNWIND` source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphUnwindInput {
    /// List-valued aliases produced before the `UNWIND`.
    pub projections: Vec<GraphUnwindInputProjection>,
}

/// Projection produced by a dynamic `UNWIND` input stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphUnwindInputProjection {
    /// List-valued expression.
    pub expression: ScalarExpression,
    /// Alias visible to the `UNWIND` source expression.
    pub alias: String,
    /// Known non-null element type of the list value.
    pub element_type: LiteralListElementType,
}

/// Projection over a graph-free `UNWIND` row source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphUnwindProjection {
    /// Project the bound `UNWIND` variable.
    Variable {
        /// Output alias.
        alias: String,
    },
}

impl GraphUnwindProjection {
    /// Returns the tabular output name rendered for this projection.
    #[must_use]
    pub fn output_name(&self) -> String {
        match self {
            Self::Variable { alias } => alias.clone(),
        }
    }
}

/// Dynamic `UNWIND` row source feeding a final graph plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphUnwindPipeline {
    /// Row source that exports the `UNWIND` variable.
    pub unwind: GraphUnwind,
    /// Final graph plan that consumes the unwound value as a staged scalar.
    pub final_plan: GraphPlan,
}

/// Staged aggregate list expansion feeding a final graph plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphStagedUnwindQuery {
    /// Aggregate stage that exports the collected list plus any carried keys.
    pub stage: GraphStage,
    /// `UNWIND` expansion over one aggregate list export.
    pub unwind: GraphStagedUnwind,
    /// Final stage rendered against carried and unwound stage columns.
    pub final_plan: GraphPlan,
}

/// A staged `UNWIND` expansion over an aggregate list export.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphStagedUnwind {
    /// Aggregate alias exported by the first stage and expanded into rows.
    pub source_alias: String,
    /// Variable bound by the `UNWIND` clause.
    pub variable: String,
    /// Binding kind produced for the unwound value.
    pub binding: GraphStagedUnwindBinding,
}

/// Binding kind produced by a staged `UNWIND` expansion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphStagedUnwindBinding {
    /// The unwound value is a scalar stage column.
    Scalar {
        /// Known non-null scalar type of the unwound value.
        element_type: LiteralListElementType,
    },
    /// The unwound value is a graph node key stage column.
    NodeKey {
        /// Label of the collected graph node values.
        label: String,
    },
}

/// Staged graph query with one or more non-terminal row-source stages and a
/// final graph plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphStagedQuery {
    /// Non-terminal stages rendered as CTEs in order.
    pub stages: Vec<GraphStage>,
    /// Final stage rendered against exported stage columns.
    pub final_plan: GraphPlan,
}

/// One non-terminal staged graph plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphStage {
    /// Stage-local graph plan.
    pub plan: GraphPlan,
    /// Key columns exported by the stage for later stages.
    pub exports: Vec<GraphStageExport>,
}

/// Column exported by a graph stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphStageExport {
    /// Graph variable key carried across the stage boundary.
    NodeKey {
        /// Graph variable carried across the stage boundary.
        variable: String,
        /// Output column name containing the variable's key.
        column: String,
    },
    /// Relationship variable key carried across the stage boundary.
    RelationshipKey {
        /// Relationship variable carried across the stage boundary.
        variable: String,
        /// Output column name containing the variable's key.
        column: String,
    },
    /// Aggregate scalar value carried across the stage boundary.
    AggregateValue {
        /// Aggregate alias visible to later stages.
        alias: String,
        /// Output column name containing the aggregate value.
        column: String,
    },
    /// Scalar value carried across the stage boundary.
    ScalarValue {
        /// Scalar alias visible to later stages.
        alias: String,
        /// Output column name containing the scalar value.
        source: String,
    },
}

impl GraphStageExport {
    /// Output column name exported by the stage.
    #[must_use]
    pub fn column(&self) -> &str {
        match self {
            Self::NodeKey { column, .. }
            | Self::RelationshipKey { column, .. }
            | Self::AggregateValue { column, .. } => column,
            Self::ScalarValue { source, .. } => source,
        }
    }
}

/// Top-level `UNION` / `UNION ALL` over graph plans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphUnion {
    /// Initial branch before the first `UNION` operator.
    pub first: GraphPlan,
    /// Subsequent branches and their leading union operator.
    pub branches: Vec<GraphUnionBranch>,
    /// Whether an empty branch-union result should be preserved as one null row.
    pub preserve_empty_result_with_null_row: bool,
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
