//! openCypher→IR compile-orchestration hub: parses (via `decypher`) and lowers
//! Coral's read-only openCypher subset into the shared graph-plan IR. Owns the
//! `compile_cypher` / `compile_cypher_query` entry points (per-graph and
//! parameter-bound variants) yielding a `GraphPlan` / `GraphQuery`; the
//! `CypherCompileContext` — a source-derived bundle of span-keyed lookup tables
//! (variable-only function args, comprehension/reduce/UNWIND sources,
//! compact-EXISTS queries, ORDER BY null placements), bound parameters, and the
//! optional graph `Declaration` — threaded read-only through nearly every
//! compile fn alongside `PredicateCompileMode` (Graph vs CASE-WHEN, carrying
//! `&GraphPlan` + `&CypherCompileState`); static UNWIND and pattern-alternative
//! expansion; pattern/clause/statement compilation (the `&mut GraphPlan`
//! mutation core); and EXISTS/COUNT/COLLECT-subquery lowering. The scalar
//! expression-lowering middle lives in `expression` as one strongly-connected
//! component, mutually recursive through the dispatch spine
//! (`compile_scalar_expression_in_mode` /
//! `compile_core_scalar_function_expression` + the single/two/three-argument
//! compilers): a follow-able recursive-descent engine, not decomposable without a
//! trait/callback redesign. Read-side counterpart to the GraphQL frontend.
//! Submodules: `cst_recovery` (CST recovery + pre-parse normalization),
//! `expression` (scalar expression/function lowering), `functions` (function
//! classification), `static_eval` (pure literal folding), `reference_validation`
//! (context-free reference checks), `pattern` (MATCH graph-pattern lowering),
//! `projection` (RETURN/WITH projection, ordering, and row modifiers), `staged`
//! (WITH staged-query planning),
//! `variable_rename` (IR rename visitors), `scalar_builders` (pure IR
//! construction), `predicate` (WHERE predicate/comparison lowering), `subquery`
//! (scoped subquery lowering).

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};

use chrono::{LocalResult, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone};
use chrono_tz::Tz;
use decypher::ast::clause::{
    Match, Order, ProjectionItem, Return, SortDirection, SortItem, Unwind, With,
};
use decypher::ast::expr::{
    BinaryOperator as CypherBinaryOperator, CaseExpression,
    ComparisonOperator as CypherComparisonOperator, ExistsExpression, Expression, FilterExpression,
    FunctionInvocation, ListComprehension, ListLiteral, Literal as CypherLiteral, MapLiteral,
    NumberLiteral, Parameter as CypherParameter, StringLiteral, UnaryOperator,
};
use decypher::ast::names::{SymbolicName, Variable};
use decypher::ast::pattern::{
    LabelExpression, NodePattern as CypherNodePattern, PatternElement, PatternElementChain,
    PatternPart, Quantifier, RangeLiteral, RelationshipDetail,
    RelationshipDirection as CypherRelationshipDirection,
    RelationshipPattern as CypherRelationshipPattern,
};
use decypher::ast::query::{
    MultiPartQuery, MultiPartQueryPart, Query, QueryBody, ReadingClause, RegularQuery,
    SinglePartBody, SinglePartQuery, SingleQuery, SingleQueryKind,
};
use decypher::ast::visit::{self, VisitMut};
use ordered_float::OrderedFloat;
use regex::Regex;

use super::declaration::{Declaration, Relationship as DeclaredRelationship, TableRef};
use super::diagnostic::Diagnostic;
use super::diagnostic_codes;
use super::ir::{
    AggregateFunction, AggregateTarget, ArithmeticOperator, ComparisonOperator,
    CountSubqueryPattern, Direction, ElementIdPredicate, ExistsPatternPredicate, GraphPlan,
    GraphQuery, GraphStage, GraphStageExport, GraphStagedQuery, GraphStagedUnwind,
    GraphStagedUnwindBinding, GraphStagedUnwindQuery, GraphUnion, GraphUnionBranch,
    GraphUnionOuterProjection, GraphUnionOuterProjectionItem, GraphUnwind, GraphUnwindInput,
    GraphUnwindInputProjection, GraphUnwindPipeline, GraphUnwindProjection, KeyPredicate, Literal,
    LiteralListElementType, NodePattern, NullOrder, OptionalMatchScope, OrderDirection,
    OrderExpression, OrderKey, PredicateExpression, PredicateRhs, PresencePredicate, Projection,
    ProjectionPredicate, ProjectionPredicateExpression, ProjectionPredicateRhs,
    PropertyKeyMembershipPredicate, PropertyPredicate, PropertyRef, RelationshipPattern,
    ScalarCaseAlternative, ScalarExpression, ScalarPredicate, ScalarPredicateRhs,
    TemporalComponentUnit, TemporalDurationUnit, TemporalExpr, TemporalKind,
    UndirectedRelationshipEndpoint, ZonedDateTimeAccessor,
};
use crate::{CatalogInfo, CoreError};

mod cst_recovery;
mod expression;
mod functions;
mod optional;
mod pattern;
mod predicate;
mod projection;
mod reference_validation;
mod scalar_builders;
mod staged;
mod static_eval;
mod subquery;
mod temporal;
mod unwind;
mod variable_rename;

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Cypher CST recovery helpers are split into a child module while preserving parent call sites."
)]
use self::cst_recovery::*;

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Cypher scalar-expression lowering helpers are split into a child module while preserving parent and sibling call sites."
)]
use self::expression::*;

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Cypher function classifiers are split into a child module while preserving parent call sites."
)]
use self::functions::*;
use self::optional::{
    OptionalMatchStart, append_static_optional_product_identity_projections,
    attach_optional_match_scope, existing_nodes_are_all_optional,
    is_pure_independent_optional_product_plan, is_pure_leading_optional_plan,
    optional_graph_variable_presence_variable, pattern_part_can_start_leading_optional_match,
    pattern_part_is_single_fixed_relationship, pattern_part_is_single_node,
};

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Cypher pattern lowering helpers are split into a child module while preserving parent and sibling call sites."
)]
use self::pattern::*;

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Cypher predicate lowering helpers are split into a child module while preserving parent and sibling call sites."
)]
use self::predicate::*;

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Cypher projection helpers are split into a child module while preserving parent and sibling call sites."
)]
pub(crate) use self::projection::*;
use self::subquery::{
    compile_collect_subquery_count_scalar_expression, compile_collect_subquery_projection,
    compile_collect_subquery_scalar_expression, compile_count_subquery_projection,
    compile_count_subquery_scalar_expression, compile_exists_pattern_predicate,
    compile_exists_predicate, compile_pattern_comprehension_count_scalar_expression,
    compile_pattern_comprehension_projection, compile_pattern_comprehension_scalar_expression,
};

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Pure scalar-expression builders are split into a child module while preserving parent call sites."
)]
use self::scalar_builders::*;
use self::staged::{compile_staged_single_query, staged_scalar_alias_final_target_unlabeled};
use self::variable_rename::{
    rename_graph_plan_variables, rename_hidden_graph_variables, rename_path_binding_variables,
    rename_projection_variables,
};

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Cypher reference-validation helpers are split into a child module while preserving parent call sites."
)]
use self::reference_validation::*;

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Cypher static literal folding helpers are split into a child module while preserving parent call sites."
)]
use self::static_eval::*;

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Cypher temporal lowering helpers are split into a child module while preserving parent call sites."
)]
use self::temporal::*;

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Cypher UNWIND helpers are split into a child module while preserving parent call sites."
)]
use self::unwind::*;

const MAX_PATTERN_ALTERNATIVE_BRANCHES: usize = 64;
const MAX_STATIC_UNWIND_BRANCHES: usize = 64;
const MAX_STATIC_RANGE_LENGTH: usize = 4096;
const MAX_STATIC_SPLIT_PARTS: usize = 4096;
const MAX_FIXED_RELATIONSHIP_LENGTH: usize = 8;
const INTERNAL_GRAPH_IDENTITY_FUNCTION: &str = "__coral_graph_identity";
const INTERNAL_GRAPH_PRESENCE_FUNCTION: &str = "__coral_graph_presence";
const INTERNAL_STATIC_RANGE_FUNCTION: &str = "__coral_static_range";
const INTERNAL_STRING_CONTAINS_FUNCTION: &str = "__coral_string_contains";
const INTERNAL_STRING_STARTS_WITH_FUNCTION: &str = "__coral_string_starts_with";
const INTERNAL_STRING_ENDS_WITH_FUNCTION: &str = "__coral_string_ends_with";

#[derive(Debug, Clone)]
enum StaticLabelTypeAlternativeSite {
    SinglePart {
        reading_clause_index: usize,
        pattern_part_index: usize,
        target: PatternAlternativeTarget,
        alternatives: Vec<LabelTypeAlternative>,
    },
    MultiPart {
        query_part: MultiPartAlternativePart,
        reading_clause_index: usize,
        pattern_part_index: usize,
        target: PatternAlternativeTarget,
        alternatives: Vec<LabelTypeAlternative>,
    },
}

#[derive(Debug, Clone, Copy)]
enum MultiPartAlternativePart {
    Part(usize),
    FinalPart,
}

#[derive(Debug, Clone, Copy)]
enum PatternAlternativeTarget {
    StartNode,
    ChainNode(usize),
    Relationship(usize),
    RelationshipMapping(usize),
}

#[derive(Debug, Clone)]
enum LabelTypeAlternative {
    NodeLabels(Vec<LabelExpression>),
    RelationshipType(LabelExpression),
    RelationshipMapping {
        left_label: LabelExpression,
        relationship_type: LabelExpression,
        right_label: LabelExpression,
    },
}

type ReadingClauseLabelTypeAlternativeSite = (
    usize,
    usize,
    PatternAlternativeTarget,
    Vec<LabelTypeAlternative>,
);

type MatchLabelTypeAlternativeSite = (usize, PatternAlternativeTarget, Vec<LabelTypeAlternative>);

#[derive(Debug, Clone)]
enum BoundedRelationshipRangeSite {
    SinglePart {
        reading_clause_index: usize,
        pattern_part_index: usize,
        chain_index: usize,
        target: RelationshipRangeTarget,
        alternatives: Vec<BoundedRelationshipRangeAlternative>,
    },
    MultiPart {
        query_part: MultiPartAlternativePart,
        reading_clause_index: usize,
        pattern_part_index: usize,
        chain_index: usize,
        target: RelationshipRangeTarget,
        alternatives: Vec<BoundedRelationshipRangeAlternative>,
    },
}

#[derive(Debug, Clone, Copy)]
struct BoundedRelationshipRangeAlternative {
    length: usize,
    force_empty: bool,
}

#[derive(Debug, Clone, Copy)]
enum RelationshipRangeTarget {
    DetailRange,
    Quantifier,
}

type BoundedRelationshipRangeSiteInfo = (
    usize,
    usize,
    usize,
    RelationshipRangeTarget,
    Vec<BoundedRelationshipRangeAlternative>,
);
type MatchBoundedRelationshipRangeSiteInfo = (
    usize,
    usize,
    RelationshipRangeTarget,
    Vec<BoundedRelationshipRangeAlternative>,
);

#[derive(Debug, Clone)]
struct ExpandedSingleQuery {
    query: SingleQuery,
    force_empty: bool,
    required_presences: BTreeSet<String>,
}

#[derive(Debug, Clone, Copy)]
enum RelationshipEndpoint {
    Start,
    End,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GraphValueRef {
    variable: String,
    presence_variable: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SameLabelUndirectedEndpointRef {
    relationship: String,
    endpoint: UndirectedRelationshipEndpoint,
    label: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MetadataListRef {
    Labels {
        value: GraphValueRef,
        label: String,
    },
    Keys {
        value: GraphValueRef,
    },
    UndirectedEndpointLabels {
        value: SameLabelUndirectedEndpointRef,
    },
    UndirectedEndpointKeys {
        value: SameLabelUndirectedEndpointRef,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataListValue {
    presence_variable: Option<String>,
    literals: Vec<Literal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StaticListValue {
    presence_variable: Option<String>,
    literals: Vec<Literal>,
    element_type: Option<LiteralListElementType>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CollectionFilterCall {
    variable: String,
    collection_source: String,
    has_predicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ListComprehensionSource {
    variable: String,
    collection_source: String,
    filter_source: Option<String>,
    has_map: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PatternComprehensionSource {
    collect_query_source: String,
    count_query_source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StaticReduceSource {
    accumulator_variable: String,
    initial_source: String,
    item_variable: String,
    collection_source: String,
    expression_source: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StaticListFunctionKind {
    Filter,
    Extract,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StaticListFunctionSource {
    kind: StaticListFunctionKind,
    variable: String,
    collection_source: String,
    filter_source: Option<String>,
    map_source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InlinePropertyValueSource {
    source: String,
    end: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StaticBooleanOutcome {
    True,
    False,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StaticListElementFamily {
    String,
    Numeric,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StaticListOrderingOutcome {
    Known(Ordering),
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StaticListQuantifier {
    All,
    Any,
    None,
    Single,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StaticListCastTarget {
    String,
    Integer,
    Float,
    Boolean,
}

impl StaticListCastTarget {
    fn function_name(self) -> &'static str {
        match self {
            Self::String => "toStringList",
            Self::Integer => "toIntegerList",
            Self::Float => "toFloatList",
            Self::Boolean => "toBooleanList",
        }
    }

    fn element_type(self) -> LiteralListElementType {
        match self {
            Self::String => LiteralListElementType::String,
            Self::Integer => LiteralListElementType::Integer,
            Self::Float => LiteralListElementType::Float,
            Self::Boolean => LiteralListElementType::Boolean,
        }
    }
}

#[derive(Clone, Copy)]
struct StaticFilterEvaluation<'a> {
    variable: &'a str,
    item: &'a Literal,
    accumulator_variable: Option<&'a str>,
    accumulator: Option<&'a Literal>,
    mode: PredicateCompileMode<'a>,
    context: &'a CypherCompileContext,
}

impl<'a> StaticFilterEvaluation<'a> {
    fn literal_for_variable(self, variable: &str) -> Option<&'a Literal> {
        if variable == self.variable {
            return Some(self.item);
        }
        if self
            .accumulator_variable
            .is_some_and(|accumulator| accumulator == variable)
        {
            return self.accumulator;
        }
        None
    }

    fn expected_variable_message(self) -> String {
        match self.accumulator_variable {
            Some(accumulator) => {
                format!(
                    "the item variable '{}' or accumulator variable '{}'",
                    self.variable, accumulator
                )
            }
            None => format!("the item variable '{}'", self.variable),
        }
    }
}

#[derive(Clone, Copy)]
struct StaticListComprehensionEvaluation<'a> {
    variable: &'a str,
    filter: Option<&'a Expression>,
    filter_context: &'a CypherCompileContext,
    map: Option<&'a Expression>,
    map_context: &'a CypherCompileContext,
    mode: PredicateCompileMode<'a>,
}

#[derive(Debug, Clone)]
pub(crate) struct PathBinding {
    length: usize,
    node_variables: Vec<String>,
    relationship_variables: Vec<String>,
    optional: bool,
    presence_gate: Option<PathPresenceGate>,
    zero_hop_endpoint_introduced: bool,
    uses_relationship_range_syntax: bool,
}

#[derive(Debug, Clone)]
enum PathPresenceGate {
    Variable(String),
    Predicate(PredicateExpression),
}

#[derive(Debug, Clone)]
struct VariableFunctionArgument {
    variable: String,
    index: usize,
    /// Total source argument count, or 0 when recovered by legacy child fallback.
    count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FunctionArgumentSources {
    arguments: Vec<String>,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct CypherCompileState {
    path_variables: BTreeMap<String, PathBinding>,
    relationship_element_path_variables: BTreeSet<String>,
    hidden_graph_variables: BTreeSet<String>,
    out_of_scope_graph_names: BTreeSet<String>,
    scalar_aliases: Vec<Projection>,
    list_alias_element_types: BTreeMap<String, LiteralListElementType>,
}

#[derive(Debug, Default)]
pub(crate) struct CypherCompileContext {
    source: String,
    variable_function_arguments: BTreeMap<(usize, usize), VariableFunctionArgument>,
    function_argument_sources: BTreeMap<(usize, usize), FunctionArgumentSources>,
    collection_filter_calls: BTreeMap<(usize, usize), CollectionFilterCall>,
    list_comprehension_sources: BTreeMap<(usize, usize), ListComprehensionSource>,
    pattern_comprehension_sources: BTreeMap<(usize, usize), PatternComprehensionSource>,
    static_reduce_sources: BTreeMap<(usize, usize), StaticReduceSource>,
    static_list_function_sources: BTreeMap<(usize, usize), StaticListFunctionSource>,
    unwind_expression_sources: BTreeMap<(usize, usize), String>,
    unwind_variables: BTreeMap<(usize, usize), String>,
    inline_property_value_sources: BTreeMap<usize, InlinePropertyValueSource>,
    compact_exists_pattern_queries: BTreeMap<(usize, usize), String>,
    order_null_placements: BTreeMap<(usize, usize), NullOrder>,
    parameters: BTreeMap<String, CypherParameterValue>,
    graph: Option<Declaration>,
    catalog: Option<CatalogInfo>,
}

impl CypherCompileContext {
    fn from_source_with_parameters_and_graph(
        cypher: &str,
        parameters: BTreeMap<String, CypherParameterValue>,
        graph: Option<Declaration>,
        catalog: Option<&CatalogInfo>,
        order_null_placements: BTreeMap<(usize, usize), NullOrder>,
    ) -> Self {
        Self {
            source: cypher.to_string(),
            variable_function_arguments: collect_variable_function_arguments(cypher),
            function_argument_sources: collect_function_argument_sources(cypher),
            collection_filter_calls: collect_collection_filter_calls(cypher),
            list_comprehension_sources: collect_list_comprehension_sources(cypher),
            pattern_comprehension_sources: collect_pattern_comprehension_sources(cypher),
            static_reduce_sources: collect_static_reduce_sources(cypher),
            static_list_function_sources: collect_static_list_function_sources(cypher),
            unwind_expression_sources: collect_unwind_expression_sources(cypher),
            unwind_variables: collect_unwind_variables(cypher),
            inline_property_value_sources: collect_inline_property_value_sources(cypher),
            compact_exists_pattern_queries: collect_compact_exists_pattern_queries(cypher),
            order_null_placements,
            parameters,
            graph,
            catalog: catalog.cloned(),
        }
    }

    fn function_source_text(&self, function: &FunctionInvocation) -> Option<String> {
        self.source
            .get(function.span.start..function.span.end)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(ToString::to_string)
    }

    fn variable_function_argument(&self, function: &FunctionInvocation) -> Option<&str> {
        self.variable_function_argument_info(function)
            .map(|argument| argument.variable.as_str())
    }

    fn variable_function_argument_info(
        &self,
        function: &FunctionInvocation,
    ) -> Option<&VariableFunctionArgument> {
        self.variable_function_arguments
            .get(&(function.span.start, function.span.end))
    }

    fn function_argument_sources(
        &self,
        function: &FunctionInvocation,
    ) -> Option<&FunctionArgumentSources> {
        self.function_argument_sources
            .get(&(function.span.start, function.span.end))
    }

    fn collection_filter_call(
        &self,
        function: &FunctionInvocation,
    ) -> Option<&CollectionFilterCall> {
        self.collection_filter_calls
            .get(&(function.span.start, function.span.end))
    }

    fn list_comprehension_source(
        &self,
        comprehension: &ListComprehension,
    ) -> Option<&ListComprehensionSource> {
        self.list_comprehension_sources
            .get(&(comprehension.span.start, comprehension.span.end))
    }

    fn pattern_comprehension_source(
        &self,
        comprehension: &decypher::ast::expr::PatternComprehension,
    ) -> Option<&PatternComprehensionSource> {
        self.pattern_comprehension_sources
            .get(&(comprehension.span.start, comprehension.span.end))
    }

    fn static_reduce_source(&self, function: &FunctionInvocation) -> Option<&StaticReduceSource> {
        self.static_reduce_sources
            .get(&(function.span.start, function.span.end))
    }

    fn static_list_function_source(
        &self,
        function: &FunctionInvocation,
    ) -> Option<&StaticListFunctionSource> {
        self.static_list_function_sources
            .get(&(function.span.start, function.span.end))
    }

    fn unwind_expression_source(&self, unwind: &Unwind) -> Option<&str> {
        self.unwind_expression_sources
            .get(&(unwind.span.start, unwind.span.end))
            .map(String::as_str)
    }

    fn unwind_variable(&self, unwind: &Unwind) -> Option<&str> {
        self.unwind_variables
            .get(&(unwind.span.start, unwind.span.end))
            .map(String::as_str)
    }

    fn truncated_inline_property_value_source(
        &self,
        expression: &Expression,
    ) -> Option<&InlinePropertyValueSource> {
        let span = expression_span(expression)?;
        self.inline_property_value_sources
            .get(&span.start)
            .filter(|source| source.end > span.end)
    }

    fn compact_exists_pattern_query(&self, exists: &ExistsExpression) -> Option<&str> {
        self.compact_exists_pattern_queries
            .get(&(exists.span.start, exists.span.end))
            .map(String::as_str)
    }

    fn order_null_placement(&self, item: &SortItem) -> Option<NullOrder> {
        let span = expression_span(&item.expression)?;
        self.order_null_placements
            .get(&(span.start, span.end))
            .copied()
    }

    fn parameter_value(
        &self,
        parameter: &CypherParameter,
        path: impl Into<String>,
    ) -> Result<&CypherParameterValue, CoreError> {
        let path = path.into();
        let name = parameter.name.name.as_str();
        self.parameters.get(name).ok_or_else(|| {
            Diagnostic::new(
                diagnostic_codes::MISSING_PARAMETER,
                path,
                format!("Cypher parameter '${name}' was not provided"),
            )
            .into_core_error()
        })
    }

    fn graph_declaration(&self, path: impl Into<String>) -> Result<&Declaration, CoreError> {
        let path = path.into();
        self.graph.as_ref().ok_or_else(|| {
            unsupported(
                path,
                "graph-variable expansion requires a graph declaration so Coral can expand mapped properties",
            )
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) enum PredicateCompileMode<'a> {
    Graph {
        plan: &'a GraphPlan,
        path_state: Option<&'a CypherCompileState>,
    },
    CaseWhen {
        plan: Option<&'a GraphPlan>,
    },
}

impl<'a> PredicateCompileMode<'a> {
    fn graph_plan(self) -> Option<&'a GraphPlan> {
        match self {
            Self::Graph { plan, .. } => Some(plan),
            Self::CaseWhen { plan } => plan,
        }
    }

    fn path_state(self) -> Option<&'a CypherCompileState> {
        match self {
            Self::Graph { path_state, .. } => path_state,
            Self::CaseWhen { .. } => None,
        }
    }

    fn scalar_alias_state(self) -> Option<&'a CypherCompileState> {
        self.path_state()
    }

    fn unsupported_predicate_message(self) -> &'static str {
        match self {
            Self::Graph { .. } => {
                "WHERE only supports graph property, id(), elementId(), labels(), keys(), exists(property), isEmpty(scalar), contains(scalar, scalar), startsWith(scalar, scalar), endsWith(scalar, scalar), and supported scalar predicates combined with AND, OR, XOR, and NOT"
            }
            Self::CaseWhen { .. } => {
                "CASE WHEN predicates support property/scalar comparisons, static graph metadata predicates including labels()/keys() list predicates and indexes, IN literal lists, null checks, exists(property), isEmpty(scalar), contains(scalar, scalar), startsWith(scalar, scalar), endsWith(scalar, scalar), boolean literals, and AND/OR/XOR/NOT"
            }
        }
    }

    fn unsupported_comparison_message(self) -> &'static str {
        match self {
            Self::Graph { .. } => {
                "comparisons must include at least one variable.property, id(variable), elementId(variable), type(relationship), or supported scalar expression operand"
            }
            Self::CaseWhen { .. } => {
                "CASE WHEN comparisons must include at least one variable.property, type(relationship), or supported scalar expression operand"
            }
        }
    }

    fn unsupported_in_message(self) -> &'static str {
        match self {
            Self::Graph { .. } => {
                "IN predicates require variable.property, id(variable), elementId(variable), type(relationship), supported scalar expression, '<label>' IN labels(node), or '<key>' IN keys(variable)"
            }
            Self::CaseWhen { .. } => {
                "CASE WHEN IN predicates require variable.property, type(relationship), supported scalar expression, '<label>' IN labels(node), or '<key>' IN keys(variable)"
            }
        }
    }

    fn unsupported_null_message(self) -> &'static str {
        match self {
            Self::Graph { .. } => {
                "IS NULL predicates require a graph variable, variable.property, id(variable), elementId(variable), type(relationship), or supported scalar expression"
            }
            Self::CaseWhen { .. } => {
                "CASE WHEN null checks require a graph variable, variable.property, id(variable), elementId(variable), or supported scalar expression operands"
            }
        }
    }

    fn graph_metadata_plan(self) -> Option<&'a GraphPlan> {
        match self {
            Self::Graph { plan, .. } => Some(plan),
            Self::CaseWhen { .. } => None,
        }
    }

    fn static_metadata_plan(self) -> Option<&'a GraphPlan> {
        match self {
            Self::Graph { plan, .. } => Some(plan),
            Self::CaseWhen { plan } => plan,
        }
    }
}

/// Runtime value that can be bound to a Cypher parameter in the supported subset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CypherParameterValue {
    /// Scalar literal parameter, usable where a literal expression is accepted.
    Literal(Literal),
    /// Scalar-list parameter, usable as the right-hand side of `IN`.
    List(Vec<Literal>),
}

/// Parses and compiles the Coral-supported read-only Cypher subset into a shared graph plan.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed or uses
/// Cypher/GQL features outside Coral's current read-only virtual graph subset.
pub fn compile_cypher(cypher: &str) -> Result<GraphPlan, CoreError> {
    compile_cypher_with_parameters(cypher, &BTreeMap::new())
}

/// Parses and compiles Cypher with typed parameter values into a shared graph plan.
///
/// Parameter values are bound before SQL lowering and only in positions where
/// the same literal or literal-list value is already supported by the read-only
/// Cypher subset.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed, uses
/// unsupported Cypher/GQL features, references a missing parameter, or binds a
/// list parameter where a scalar literal is required.
pub fn compile_cypher_with_parameters(
    cypher: &str,
    parameters: &BTreeMap<String, CypherParameterValue>,
) -> Result<GraphPlan, CoreError> {
    match compile_cypher_query_with_parameters(cypher, parameters)? {
        GraphQuery::Plan(plan) => Ok(plan),
        GraphQuery::Unwind(_) | GraphQuery::UnwindPipeline(_) => Err(unsupported(
            "query.unwind",
            "compile_cypher returns a single graph plan; use compile_cypher_query for UNWIND row-source queries",
        )),
        GraphQuery::Staged(_) | GraphQuery::StagedUnwind(_) => Err(unsupported(
            "query.staged",
            "compile_cypher returns a single graph plan; use compile_cypher_query for staged queries",
        )),
        GraphQuery::Union(_) => Err(unsupported(
            "query.union",
            "compile_cypher returns a single graph plan; use compile_cypher_query for UNION queries",
        )),
    }
}

/// Parses and compiles Cypher against a graph declaration into a shared graph plan.
///
/// Declaration-aware compilation enables syntax such as `RETURN *` that must
/// expand mapped graph variables into concrete tabular projections.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed, uses
/// unsupported Cypher/GQL features, or references graph metadata that cannot be
/// resolved from the declaration.
pub fn compile_cypher_for_graph(graph: &Declaration, cypher: &str) -> Result<GraphPlan, CoreError> {
    compile_cypher_for_graph_with_parameters(graph, cypher, &BTreeMap::new())
}

/// Parses and compiles Cypher with typed parameter values against a graph declaration.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed, uses
/// unsupported Cypher/GQL features, references a missing parameter, binds a
/// parameter value in an unsupported position, or uses graph metadata that
/// cannot be resolved from the declaration.
pub fn compile_cypher_for_graph_with_parameters(
    graph: &Declaration,
    cypher: &str,
    parameters: &BTreeMap<String, CypherParameterValue>,
) -> Result<GraphPlan, CoreError> {
    match compile_cypher_query_for_graph_with_parameters(graph, cypher, parameters)? {
        GraphQuery::Plan(plan) => Ok(plan),
        GraphQuery::Unwind(_) | GraphQuery::UnwindPipeline(_) => Err(unsupported(
            "query.unwind",
            "compile_cypher_for_graph returns a single graph plan; use compile_cypher_query_for_graph for UNWIND row-source queries",
        )),
        GraphQuery::Staged(_) | GraphQuery::StagedUnwind(_) => Err(unsupported(
            "query.staged",
            "compile_cypher_for_graph returns a single graph plan; use compile_cypher_query_for_graph for staged queries",
        )),
        GraphQuery::Union(_) => Err(unsupported(
            "query.union",
            "compile_cypher_for_graph returns a single graph plan; use compile_cypher_query_for_graph for UNION queries",
        )),
    }
}

/// Parses and compiles Cypher into a read-only virtual graph query.
///
/// This accepts the same single-query subset as [`compile_cypher`] plus
/// top-level `UNION` / `UNION ALL` composition between supported branch queries.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed or uses
/// Cypher/GQL features outside Coral's current read-only virtual graph subset.
pub fn compile_cypher_query(cypher: &str) -> Result<GraphQuery, CoreError> {
    compile_cypher_query_with_parameters(cypher, &BTreeMap::new())
}

/// Parses and compiles Cypher into a read-only virtual graph query using graph metadata.
///
/// Declaration-aware compilation enables syntax such as `RETURN *` that must
/// expand mapped graph variables into concrete tabular projections.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed or uses
/// Cypher/GQL features outside Coral's current read-only virtual graph subset.
pub fn compile_cypher_query_for_graph(
    graph: &Declaration,
    cypher: &str,
) -> Result<GraphQuery, CoreError> {
    compile_cypher_query_for_graph_with_parameters(graph, cypher, &BTreeMap::new())
}

/// Parses and compiles Cypher with typed parameter values into a read-only graph query.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed, uses
/// unsupported Cypher/GQL features, references a missing parameter, or binds a
/// parameter value in an unsupported position.
pub fn compile_cypher_query_with_parameters(
    cypher: &str,
    parameters: &BTreeMap<String, CypherParameterValue>,
) -> Result<GraphQuery, CoreError> {
    compile_cypher_query_with_optional_graph(cypher, parameters, None, None)
}

/// Parses and compiles Cypher with typed parameter values into a read-only graph query
/// using graph metadata.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed, uses
/// unsupported Cypher/GQL features, references a missing parameter, binds a
/// parameter value in an unsupported position, or uses graph metadata that
/// cannot be resolved from the declaration.
pub fn compile_cypher_query_for_graph_with_parameters(
    graph: &Declaration,
    cypher: &str,
    parameters: &BTreeMap<String, CypherParameterValue>,
) -> Result<GraphQuery, CoreError> {
    let query = compile_cypher_query_with_optional_graph(cypher, parameters, Some(graph), None)?;
    graph.validate_graph_query(&query)?;
    Ok(query)
}

pub(crate) fn compile_cypher_query_for_graph_with_parameters_and_catalog(
    graph: &Declaration,
    cypher: &str,
    parameters: &BTreeMap<String, CypherParameterValue>,
    catalog: &CatalogInfo,
) -> Result<GraphQuery, CoreError> {
    compile_cypher_query_with_optional_graph(cypher, parameters, Some(graph), Some(catalog))
}

/// Translates Cypher against a virtual graph and runtime catalog into DataFusion SQL.
///
/// This is the catalog-bound companion to [`compile_cypher_query_for_graph`]
/// for callers that already own a catalog snapshot and an SQL execution path.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the Cypher text is empty, cannot be
/// parsed, uses unsupported Cypher/GQL features, references a missing
/// parameter, or cannot be lowered against the supplied graph/catalog.
pub fn translate_cypher_query_for_graph_with_parameters_and_catalog(
    graph: &Declaration,
    cypher: &str,
    parameters: &BTreeMap<String, CypherParameterValue>,
    catalog: &CatalogInfo,
) -> Result<super::SqlTranslation, CoreError> {
    if cypher.trim().is_empty() {
        return Err(CoreError::InvalidInput(
            "Cypher query must not be empty".to_string(),
        ));
    }

    let query = compile_cypher_query_for_graph_with_parameters_and_catalog(
        graph, cypher, parameters, catalog,
    )?;
    graph.lower_graph_query_against_catalog(&query, catalog)
}

fn compile_cypher_query_with_optional_graph(
    cypher: &str,
    parameters: &BTreeMap<String, CypherParameterValue>,
    graph: Option<&Declaration>,
    catalog: Option<&CatalogInfo>,
) -> Result<GraphQuery, CoreError> {
    let count_normalized = normalize_compact_count_subqueries(cypher);
    let range_normalized = normalize_static_range_functions(count_normalized.as_ref());
    let string_predicate_function_normalized =
        normalize_string_predicate_functions(range_normalized.as_ref());
    let null_normalized =
        normalize_order_null_placements(string_predicate_function_normalized.as_ref());
    let cypher = null_normalized.cypher.as_ref();
    let query = decypher::parse(cypher).map_err(|error| {
        Diagnostic::new(
            diagnostic_codes::CYPHER_PARSE_ERROR,
            "query",
            error.to_string(),
        )
        .into_core_error()
    })?;
    let order_null_placements =
        collect_order_null_placements_for_query(&query, &null_normalized.placements)?;
    let context = CypherCompileContext::from_source_with_parameters_and_graph(
        cypher,
        parameters.clone(),
        graph.cloned(),
        catalog,
        order_null_placements,
    );
    compile_query(&query, &context)
}

fn compile_query(query: &Query, context: &CypherCompileContext) -> Result<GraphQuery, CoreError> {
    if query.statements.len() != 1 {
        return Err(unsupported(
            "query",
            "only a single Cypher statement is supported",
        ));
    }
    let statement = query
        .statements
        .first()
        .ok_or_else(|| unsupported("query", "Cypher query must contain a statement"))?;

    match statement {
        QueryBody::SingleQuery(single_query) => {
            compile_single_query_as_graph_query(single_query, context, "query")
        }
        QueryBody::Regular(regular_query) => compile_regular_query(regular_query, context),
        _ => Err(unsupported(
            "query",
            "only read-only MATCH queries and UNION queries are supported",
        )),
    }
}

fn compile_single_query_as_graph_query(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<GraphQuery, CoreError> {
    let path = path.into();
    if let Some(query) =
        compile_single_query_row_source_before_expansion(single_query, context, &path)?
    {
        return Ok(query);
    }
    let contains_static_unwind = single_query_contains_unwind(single_query);
    let scalar_alias_final_target_unlabeled =
        staged_scalar_alias_final_target_unlabeled(single_query);
    let mut variants = expand_single_query_static_branches(single_query, context, &path)?;
    if variants.len() == 1 {
        let variant = variants
            .first()
            .ok_or_else(|| CoreError::internal("Cypher query expansion produced no variants"))?;
        if !scalar_alias_final_target_unlabeled
            && let Some(query) = compile_staged_single_query(&variant.query, context)?
        {
            if variant.force_empty {
                return Err(unsupported(
                    path,
                    "empty static expansions with staged query planning are not supported yet",
                ));
            }
            return Ok(query);
        }
        let plan = compile_expanded_single_query_plan(variant, context)?;
        return Ok(GraphQuery::Plan(plan));
    }

    validate_pattern_alternative_expansion_supported(single_query, &path, context)?;
    let outer_projection_plan =
        analyze_static_alternative_outer_projection(single_query, &path, context)?;
    let hidden_order_plan = analyze_static_alternative_hidden_order(
        single_query,
        outer_projection_plan.as_ref(),
        context,
        &path,
    )?;
    let hidden_order_plans = compile_static_branch_hidden_order_plans(
        &variants,
        hidden_order_plan.as_ref(),
        outer_projection_plan.as_ref(),
        contains_static_unwind,
        context,
        &path,
    )?;
    for (variant, hidden_order_plan) in variants.iter_mut().zip(hidden_order_plans.iter()) {
        apply_static_alternative_outer_projection_rewrite(
            &mut variant.query,
            outer_projection_plan.as_ref(),
            &path,
        )?;
        apply_static_alternative_hidden_order_rewrite(
            &mut variant.query,
            hidden_order_plan.as_ref(),
            &path,
        )?;
        clear_final_return_outer_modifiers(&mut variant.query, &path)?;
    }
    let plans = variants
        .iter()
        .map(|variant| compile_expanded_single_query_plan(variant, context))
        .collect::<Result<Vec<_>, CoreError>>()?;
    let mut plans = plans;
    rewrite_missing_branch_properties_as_null(&mut plans, context)?;
    let projection_names = plans
        .first()
        .map(GraphPlan::projection_output_names)
        .ok_or_else(|| CoreError::internal("Cypher query expansion produced no graph plans"))?;
    let outer_projection = compile_static_alternative_outer_projection(
        outer_projection_plan.as_ref(),
        &projection_names,
    )?;
    let outer_projection = compile_static_alternative_hidden_order_outer_projection(
        outer_projection,
        hidden_order_plan.as_ref(),
        &projection_names,
        final_return_clause(single_query, &path)?.items.len(),
    )?;
    let projection_names = outer_projection.as_ref().map_or_else(
        || projection_names.clone(),
        GraphUnionOuterProjection::output_names,
    );
    let order_by = compile_static_alternative_outer_order_by(
        single_query,
        &projection_names,
        hidden_order_plan.as_ref(),
        context,
        &path,
    )?;
    let (skip, limit) = compile_static_alternative_outer_skip_limit(single_query, context, &path)?;
    let distinct = final_return_clause(single_query, &path)?.distinct;
    graph_query_from_alternative_plans(
        plans,
        outer_projection,
        distinct,
        order_by,
        skip,
        limit,
        context,
    )
}

fn compile_expanded_single_query_plan(
    variant: &ExpandedSingleQuery,
    context: &CypherCompileContext,
) -> Result<GraphPlan, CoreError> {
    let mut plan = compile_single_query(&variant.query, context)?;
    apply_required_presence_predicates(&mut plan, &variant.required_presences);
    if variant.force_empty {
        force_empty_plan(&mut plan);
    }
    Ok(plan)
}

fn compile_static_branch_hidden_order_plans(
    variants: &[ExpandedSingleQuery],
    hidden_order_plan: Option<&StaticAlternativeHiddenOrderPlan>,
    outer_projection_plan: Option<&StaticAlternativeOuterProjectionPlan>,
    contains_static_unwind: bool,
    context: &CypherCompileContext,
    path: &str,
) -> Result<Vec<Option<StaticAlternativeHiddenOrderPlan>>, CoreError> {
    if hidden_order_plan.is_none() {
        return Ok(vec![None; variants.len()]);
    }

    if !contains_static_unwind {
        return Ok(vec![hidden_order_plan.cloned(); variants.len()]);
    }

    let hidden_order_plans = variants
        .iter()
        .enumerate()
        .map(|(index, variant)| {
            let branch_path = format!("{path}.static_branches[{index}]");
            analyze_static_alternative_hidden_order(
                &variant.query,
                outer_projection_plan,
                context,
                branch_path.as_str(),
            )
        })
        .collect::<Result<Vec<_>, CoreError>>()?;
    validate_static_branch_hidden_order_alignment(hidden_order_plan, &hidden_order_plans)?;
    Ok(hidden_order_plans)
}

fn validate_static_branch_hidden_order_alignment(
    expected: Option<&StaticAlternativeHiddenOrderPlan>,
    actual: &[Option<StaticAlternativeHiddenOrderPlan>],
) -> Result<(), CoreError> {
    let Some(expected) = expected else {
        return Ok(());
    };

    for plan in actual {
        let Some(plan) = plan else {
            return Err(CoreError::internal(
                "static branch hidden ORDER BY plan disappeared after expansion",
            ));
        };
        if plan.items.len() != expected.items.len()
            || plan
                .items
                .iter()
                .zip(expected.items.iter())
                .any(|(left, right)| {
                    left.order_index != right.order_index || left.alias != right.alias
                })
        {
            return Err(CoreError::internal(
                "static branch hidden ORDER BY plans were not aligned after expansion",
            ));
        }
    }
    Ok(())
}

fn expand_single_query_static_branches(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
    path: &str,
) -> Result<Vec<ExpandedSingleQuery>, CoreError> {
    let unwind_variants = expand_single_query_static_unwinds(single_query, context, path)?;
    let mut expanded = Vec::new();
    for variant in unwind_variants {
        let pattern_variants = expand_single_query_pattern_alternatives(&variant.query, context)?;
        for pattern_variant in pattern_variants {
            if expanded.len() >= MAX_PATTERN_ALTERNATIVE_BRANCHES {
                return Err(unsupported(
                    path,
                    format!(
                        "static branch expansion produced more than {MAX_PATTERN_ALTERNATIVE_BRANCHES} branches; simplify the query or split it explicitly"
                    ),
                ));
            }
            expanded.push(ExpandedSingleQuery {
                query: pattern_variant.query,
                force_empty: variant.force_empty || pattern_variant.force_empty,
                required_presences: variant.required_presences.clone(),
            });
        }
    }
    Ok(expanded)
}

fn expression_uses_graph_metadata_list(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => expression_uses_graph_metadata_list(inner),
        Expression::FunctionCall(function) => {
            is_keys_function(function)
                || function
                    .arguments
                    .iter()
                    .any(expression_uses_graph_metadata_list)
        }
        Expression::ListSlice {
            list, start, end, ..
        } => {
            expression_uses_graph_metadata_list(list)
                || start
                    .as_deref()
                    .is_some_and(expression_uses_graph_metadata_list)
                || end
                    .as_deref()
                    .is_some_and(expression_uses_graph_metadata_list)
        }
        Expression::ListIndex { list, index, .. } => {
            expression_uses_graph_metadata_list(list) || expression_uses_graph_metadata_list(index)
        }
        Expression::ListComprehension(comprehension) => {
            comprehension
                .filter
                .as_deref()
                .is_some_and(expression_uses_graph_metadata_list)
                || comprehension
                    .map
                    .as_ref()
                    .is_some_and(expression_uses_graph_metadata_list)
        }
        Expression::BinaryOp { lhs, rhs, .. } | Expression::In { lhs, rhs, .. } => {
            expression_uses_graph_metadata_list(lhs) || expression_uses_graph_metadata_list(rhs)
        }
        Expression::Comparison { lhs, operators, .. } => {
            expression_uses_graph_metadata_list(lhs)
                || operators
                    .iter()
                    .any(|(_, rhs)| expression_uses_graph_metadata_list(rhs))
        }
        Expression::UnaryOp { operand, .. } | Expression::IsNull { operand, .. } => {
            expression_uses_graph_metadata_list(operand)
        }
        Expression::Case(case) => {
            case.scrutinee
                .as_deref()
                .is_some_and(expression_uses_graph_metadata_list)
                || case.alternatives.iter().any(|alternative| {
                    expression_uses_graph_metadata_list(&alternative.when)
                        || expression_uses_graph_metadata_list(&alternative.then)
                })
                || case
                    .default
                    .as_deref()
                    .is_some_and(expression_uses_graph_metadata_list)
        }
        Expression::PropertyLookup { base, .. } => expression_uses_graph_metadata_list(base),
        Expression::Literal(literal) => literal_uses_graph_metadata_list(literal),
        Expression::All(filter)
        | Expression::Any(filter)
        | Expression::None(filter)
        | Expression::Single(filter) => {
            expression_uses_graph_metadata_list(&filter.collection)
                || filter
                    .predicate
                    .as_deref()
                    .is_some_and(expression_uses_graph_metadata_list)
        }
        Expression::PatternComprehension(comprehension) => {
            comprehension
                .where_clause
                .as_ref()
                .is_some_and(expression_uses_graph_metadata_list)
                || expression_uses_graph_metadata_list(&comprehension.map)
        }
        Expression::Variable(_)
        | Expression::Parameter(_)
        | Expression::CountStar { .. }
        | Expression::NodeLabels { .. }
        | Expression::Pattern(_)
        | Expression::Exists(_)
        | Expression::CountSubquery(_)
        | Expression::CollectSubquery(_)
        | Expression::MapProjection(_) => false,
    }
}

fn literal_uses_graph_metadata_list(literal: &CypherLiteral) -> bool {
    match literal {
        CypherLiteral::List(list) => list
            .elements
            .iter()
            .any(expression_uses_graph_metadata_list),
        CypherLiteral::Map(map) => map
            .entries
            .iter()
            .any(|(_, value)| expression_uses_graph_metadata_list(value)),
        CypherLiteral::Number(_)
        | CypherLiteral::String(_)
        | CypherLiteral::Boolean(_)
        | CypherLiteral::Null => false,
    }
}

fn compile_optional_static_folded_case_list_value(
    case: &CaseExpression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
    non_foldable_message: &'static str,
) -> Result<Option<StaticListValue>, CoreError> {
    let path = path.into();
    let Some(parts) = compile_optional_static_list_case_parts(
        case,
        path.clone(),
        PredicateCompileMode::CaseWhen { plan },
        context,
    )?
    else {
        return Ok(None);
    };
    for (index, (when, result)) in parts.alternatives.into_iter().enumerate() {
        match when {
            PredicateExpression::Boolean(true) => {
                return Ok(Some(static_folded_case_result_value(
                    result,
                    parts.element_type,
                )));
            }
            PredicateExpression::Boolean(false) => {}
            _ => {
                return Err(unsupported(
                    format!("{path}.alternatives[{index}].when"),
                    non_foldable_message,
                ));
            }
        }
    }
    match parts.default {
        Some(result) => Ok(Some(static_folded_case_result_value(
            result,
            parts.element_type,
        ))),
        None => Ok(Some(StaticListValue {
            presence_variable: None,
            literals: Vec::new(),
            element_type: parts.element_type,
        })),
    }
}

fn static_folded_case_result_value(
    result: StaticListCaseResult,
    element_type: Option<LiteralListElementType>,
) -> StaticListValue {
    match result {
        StaticListCaseResult::Null => StaticListValue {
            presence_variable: None,
            literals: Vec::new(),
            element_type,
        },
        StaticListCaseResult::List(value) => match element_type {
            Some(element_type) => with_static_list_element_type(value, element_type),
            None => value,
        },
        StaticListCaseResult::Coalesce(coalesce) => {
            let Some(element_type) = element_type else {
                return StaticListValue {
                    presence_variable: None,
                    literals: Vec::new(),
                    element_type: None,
                };
            };
            for argument in coalesce.arguments {
                let StaticListCoalesceArgument::List(value) = argument else {
                    continue;
                };
                return with_static_list_element_type(value, element_type);
            }
            StaticListValue {
                presence_variable: None,
                literals: Vec::new(),
                element_type: Some(element_type),
            }
        }
    }
}

fn with_projects_variable(with: &With, variable: &str) -> bool {
    with.items
        .iter()
        .any(|item| return_item_projection_name(item) == variable)
}

fn reading_clause_binds_variable(clause: &ReadingClause, variable: &str) -> bool {
    match clause {
        ReadingClause::Match(match_clause) => match_clause_bound_variables(match_clause)
            .iter()
            .any(|candidate| candidate == variable),
        ReadingClause::Unwind(unwind) => variable_name(&unwind.variable) == variable,
        ReadingClause::InQueryCall(_)
        | ReadingClause::CallSubquery(_)
        | ReadingClause::LoadCsv(_) => false,
    }
}

fn match_clause_bound_variables(match_clause: &Match) -> BTreeSet<String> {
    let mut variables = BTreeSet::new();
    for part in &match_clause.pattern.parts {
        pattern_part_bound_variables(part, &mut variables);
    }
    variables
}

fn pattern_part_bound_variables(pattern_part: &PatternPart, variables: &mut BTreeSet<String>) {
    if let Some(variable) = pattern_part.variable.as_ref() {
        variables.insert(variable_name(variable));
    }
    pattern_element_bound_variables(&pattern_part.anonymous.element, variables);
}

fn pattern_element_bound_variables(element: &PatternElement, variables: &mut BTreeSet<String>) {
    match element {
        PatternElement::Path { start, chains } => {
            node_pattern_bound_variables(start, variables);
            for chain in chains {
                relationship_pattern_bound_variables(&chain.relationship, variables);
                node_pattern_bound_variables(&chain.node, variables);
            }
        }
        PatternElement::Parenthesized(inner) => pattern_element_bound_variables(inner, variables),
        PatternElement::Quantified { element, .. } => {
            pattern_element_bound_variables(element, variables);
        }
    }
}

fn pattern_element_path(
    element: &PatternElement,
) -> Option<(&CypherNodePattern, &[PatternElementChain])> {
    match element {
        PatternElement::Path { start, chains } => Some((start, chains.as_slice())),
        PatternElement::Parenthesized(inner) => pattern_element_path(inner),
        PatternElement::Quantified {
            element,
            quantifier,
            ..
        } if is_exact_one_path_quantifier(quantifier) => pattern_element_path(element),
        PatternElement::Quantified { .. } => None,
    }
}

fn pattern_element_path_mut(
    element: &mut PatternElement,
) -> Option<(&mut CypherNodePattern, &mut Vec<PatternElementChain>)> {
    match element {
        PatternElement::Path { start, chains } => Some((start, chains)),
        PatternElement::Parenthesized(inner) => pattern_element_path_mut(inner),
        PatternElement::Quantified {
            element,
            quantifier,
            ..
        } if is_exact_one_path_quantifier(quantifier) => pattern_element_path_mut(element),
        PatternElement::Quantified { .. } => None,
    }
}

fn is_exact_one_path_quantifier(quantifier: &Quantifier) -> bool {
    quantifier.start == Some(1) && quantifier.end == Some(1)
}

fn node_pattern_bound_variables(pattern: &CypherNodePattern, variables: &mut BTreeSet<String>) {
    if let Some(variable) = pattern.variable.as_ref() {
        variables.insert(variable_name(variable));
    }
}

fn relationship_pattern_bound_variables(
    pattern: &CypherRelationshipPattern,
    variables: &mut BTreeSet<String>,
) {
    if let Some(variable) = pattern
        .detail
        .as_ref()
        .and_then(|detail| detail.variable.as_ref())
    {
        variables.insert(variable_name(variable));
    }
}

fn cypher_literal_expression(literal: &Literal, span: decypher::error::Span) -> Expression {
    Expression::Literal(match literal {
        Literal::String(value) => CypherLiteral::String(StringLiteral {
            value: value.clone(),
            span,
            raw: None,
        }),
        Literal::Integer(value) => CypherLiteral::Number(NumberLiteral::Integer(*value)),
        Literal::Float(value) => CypherLiteral::Number(NumberLiteral::Float(value.into_inner())),
        Literal::Boolean(value) => CypherLiteral::Boolean(*value),
        Literal::Null => CypherLiteral::Null,
        Literal::List(values) => CypherLiteral::List(ListLiteral {
            elements: values
                .iter()
                .map(|value| cypher_literal_expression(value, span))
                .collect(),
            span,
        }),
    })
}

fn force_empty_plan(plan: &mut GraphPlan) {
    append_predicate_expression(PredicateExpression::Boolean(false), plan);
}

fn apply_required_presence_predicates(plan: &mut GraphPlan, variables: &BTreeSet<String>) {
    for variable in variables {
        append_predicate_expression(
            PredicateExpression::Presence(PresencePredicate {
                variable: variable.clone(),
                operator: ComparisonOperator::NotEqual,
            }),
            plan,
        );
    }
}

fn graph_query_from_alternative_plans(
    mut plans: Vec<GraphPlan>,
    mut outer_projection: Option<GraphUnionOuterProjection>,
    distinct: bool,
    order_by: Vec<OrderKey>,
    skip: Option<u64>,
    limit: Option<u64>,
    context: &CypherCompileContext,
) -> Result<GraphQuery, CoreError> {
    if plans.is_empty() {
        return Err(CoreError::internal(
            "Cypher query expansion produced no graph plans",
        ));
    }
    if plans.len() == 1 {
        let first = plans.remove(0);
        return Ok(GraphQuery::Plan(first));
    }
    let visible_projection_names = plans
        .first()
        .ok_or_else(|| CoreError::internal("Cypher query expansion produced no graph plans"))?
        .projection_output_names();
    let collapse_static_optional_product_rows =
        plans.iter().all(is_pure_independent_optional_product_plan)
            && append_static_optional_product_identity_projections(&mut plans, context)?;
    if collapse_static_optional_product_rows && outer_projection.is_none() {
        outer_projection = Some(GraphUnionOuterProjection {
            items: visible_projection_names
                .into_iter()
                .map(|name| GraphUnionOuterProjectionItem::Column { name })
                .collect(),
            group_by: Vec::new(),
        });
    }
    let first = plans.remove(0);
    let preserve_empty_result_with_null_row = std::iter::once(&first)
        .chain(plans.iter())
        .all(is_pure_leading_optional_plan);
    Ok(GraphQuery::Union(GraphUnion {
        first,
        branches: plans
            .into_iter()
            .map(|plan| GraphUnionBranch {
                all: !collapse_static_optional_product_rows,
                plan,
            })
            .collect(),
        preserve_empty_result_with_null_row,
        outer_projection,
        distinct,
        order_by,
        skip,
        limit,
    }))
}

fn rewrite_missing_branch_properties_as_null(
    plans: &mut [GraphPlan],
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    let Some(graph) = context.graph.as_ref() else {
        return Ok(());
    };
    for plan in plans {
        let nodes = plan
            .nodes
            .iter()
            .map(|node| (node.variable.clone(), node.label.clone()))
            .collect::<BTreeMap<_, _>>();
        let relationships = plan.relationships.clone();
        rewrite_missing_branch_scalar_expressions_as_null(plan, graph, &nodes, &relationships)?;
        rewrite_missing_branch_property_predicates_as_null(plan, graph, &nodes, &relationships)?;
        for projection in &mut plan.projections {
            let (property, alias) = match projection {
                Projection::Property { property, alias } => {
                    let alias = alias
                        .clone()
                        .unwrap_or_else(|| format!("{}_{}", property.variable, property.property));
                    (property.clone(), alias)
                }
                _ => continue,
            };
            if !branch_property_is_missing(graph, &nodes, &relationships, &property) {
                continue;
            }
            *projection = Projection::Literal {
                literal: Literal::Null,
                alias,
            };
        }
    }
    Ok(())
}

fn rewrite_missing_branch_scalar_expressions_as_null(
    plan: &mut GraphPlan,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<(), CoreError> {
    for projection in &mut plan.projections {
        match projection {
            Projection::Expression { expression, .. } => {
                rewrite_missing_branch_scalar_expression_as_null(
                    expression,
                    graph,
                    nodes,
                    relationships,
                )?;
            }
            Projection::Aggregate { target, .. } => {
                rewrite_missing_branch_aggregate_target_as_null(
                    target,
                    graph,
                    nodes,
                    relationships,
                )?;
            }
            Projection::Property { .. }
            | Projection::Key { .. }
            | Projection::ElementId { .. }
            | Projection::RelationshipType { .. }
            | Projection::NodeLabels { .. }
            | Projection::PropertyKeys { .. }
            | Projection::Literal { .. }
            | Projection::LiteralList { .. }
            | Projection::CountAll { .. } => {}
        }
    }

    for order_key in &mut plan.order_by {
        if let OrderExpression::Scalar(expression) = &mut order_key.expression {
            rewrite_missing_branch_scalar_expression_as_null(
                expression,
                graph,
                nodes,
                relationships,
            )?;
        }
    }
    Ok(())
}

fn rewrite_missing_branch_aggregate_target_as_null(
    target: &mut AggregateTarget,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<(), CoreError> {
    match target {
        AggregateTarget::Expression(expression) => {
            rewrite_missing_branch_scalar_expression_as_null(
                expression,
                graph,
                nodes,
                relationships,
            )?;
        }
        AggregateTarget::Property(_)
        | AggregateTarget::PresenceGatedProperty { .. }
        | AggregateTarget::VariableKey { .. }
        | AggregateTarget::PresenceGatedVariableKey { .. } => {}
    }
    Ok(())
}

fn rewrite_missing_branch_property_predicates_as_null(
    plan: &mut GraphPlan,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<(), CoreError> {
    if let Some(predicate) = plan.predicate.take() {
        plan.predicate = Some(rewrite_missing_branch_property_predicate_expression(
            predicate,
            graph,
            nodes,
            relationships,
        )?);
    }

    let mut retained = Vec::with_capacity(plan.predicates.len());
    let predicates = std::mem::take(&mut plan.predicates);
    for predicate in predicates {
        match rewrite_missing_branch_property_predicate(predicate, graph, nodes, relationships)? {
            BranchPropertyPredicateRewrite::Keep(predicate) => retained.push(predicate),
            BranchPropertyPredicateRewrite::Rewrite(expression) => {
                append_predicate_expression(expression, plan);
            }
        }
    }
    plan.predicates = retained;

    for optional_match in &mut plan.optional_matches {
        if let Some(predicate) = optional_match.predicate.take() {
            optional_match.predicate = Some(rewrite_missing_branch_property_predicate_expression(
                predicate,
                graph,
                nodes,
                relationships,
            )?);
        }
    }
    Ok(())
}

fn rewrite_missing_branch_property_predicate_expression(
    expression: PredicateExpression,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<PredicateExpression, CoreError> {
    match expression {
        PredicateExpression::Comparison(predicate) => {
            match rewrite_missing_branch_property_predicate(predicate, graph, nodes, relationships)?
            {
                BranchPropertyPredicateRewrite::Keep(predicate) => {
                    Ok(PredicateExpression::Comparison(predicate))
                }
                BranchPropertyPredicateRewrite::Rewrite(expression) => Ok(expression),
            }
        }
        PredicateExpression::ScalarComparison(predicate) => {
            rewrite_missing_branch_scalar_predicate(predicate, graph, nodes, relationships)
        }
        PredicateExpression::And { left, right } => Ok(PredicateExpression::And {
            left: Box::new(rewrite_missing_branch_property_predicate_expression(
                *left,
                graph,
                nodes,
                relationships,
            )?),
            right: Box::new(rewrite_missing_branch_property_predicate_expression(
                *right,
                graph,
                nodes,
                relationships,
            )?),
        }),
        PredicateExpression::Or { left, right } => Ok(PredicateExpression::Or {
            left: Box::new(rewrite_missing_branch_property_predicate_expression(
                *left,
                graph,
                nodes,
                relationships,
            )?),
            right: Box::new(rewrite_missing_branch_property_predicate_expression(
                *right,
                graph,
                nodes,
                relationships,
            )?),
        }),
        PredicateExpression::Xor { left, right } => Ok(PredicateExpression::Xor {
            left: Box::new(rewrite_missing_branch_property_predicate_expression(
                *left,
                graph,
                nodes,
                relationships,
            )?),
            right: Box::new(rewrite_missing_branch_property_predicate_expression(
                *right,
                graph,
                nodes,
                relationships,
            )?),
        }),
        PredicateExpression::Not { expression } => Ok(PredicateExpression::Not {
            expression: Box::new(rewrite_missing_branch_property_predicate_expression(
                *expression,
                graph,
                nodes,
                relationships,
            )?),
        }),
        PredicateExpression::ExistsPattern(mut predicate) => {
            rewrite_missing_branch_exists_pattern_as_null(
                &mut predicate,
                graph,
                nodes,
                relationships,
            )?;
            Ok(PredicateExpression::ExistsPattern(predicate))
        }
        _ => Ok(expression),
    }
}

fn rewrite_missing_branch_scalar_predicate(
    mut predicate: ScalarPredicate,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<PredicateExpression, CoreError> {
    let lhs_was_missing = rewrite_missing_branch_scalar_expression_as_null(
        &mut predicate.lhs,
        graph,
        nodes,
        relationships,
    )?;
    let rhs_was_missing = rewrite_missing_branch_scalar_predicate_rhs_as_null(
        &mut predicate.rhs,
        graph,
        nodes,
        relationships,
    )?;
    if rhs_was_missing
        || (lhs_was_missing
            && scalar_predicate_rhs_is_literal_null(&predicate.rhs)
            && (matches!(
                predicate.operator,
                ComparisonOperator::Equal | ComparisonOperator::NotEqual
            ) || is_range_comparison_operator(predicate.operator)))
    {
        return Ok(unknown_boolean_predicate());
    }
    Ok(PredicateExpression::ScalarComparison(predicate))
}

fn rewrite_missing_branch_scalar_predicate_rhs_as_null(
    rhs: &mut ScalarPredicateRhs,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<bool, CoreError> {
    match rhs {
        ScalarPredicateRhs::Expression(expression) => {
            rewrite_missing_branch_scalar_expression_as_null(
                expression,
                graph,
                nodes,
                relationships,
            )
        }
        ScalarPredicateRhs::List(_) => Ok(false),
    }
}

fn scalar_predicate_rhs_is_literal_null(rhs: &ScalarPredicateRhs) -> bool {
    matches!(
        rhs,
        ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null))
    )
}

fn rewrite_missing_branch_scalar_expression_as_null(
    expression: &mut ScalarExpression,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<bool, CoreError> {
    if let ScalarExpression::Property(property) = expression
        && branch_property_is_missing(graph, nodes, relationships, property)
    {
        *expression = ScalarExpression::Literal(Literal::Null);
        return Ok(true);
    }

    if let Some(expression) = unary_scalar_expression_operand_mut(expression) {
        rewrite_missing_branch_scalar_expression_as_null(expression, graph, nodes, relationships)?;
        return Ok(false);
    }

    rewrite_nested_missing_branch_scalar_expressions_as_null(
        expression,
        graph,
        nodes,
        relationships,
    )?;
    Ok(false)
}

fn rewrite_nested_missing_branch_scalar_expressions_as_null(
    expression: &mut ScalarExpression,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<(), CoreError> {
    if rewrite_missing_branch_scalar_leaf_as_null(expression, graph, nodes, relationships)? {
        return Ok(());
    }
    rewrite_missing_branch_compound_scalar_expression_as_null(
        expression,
        graph,
        nodes,
        relationships,
    )
}

fn rewrite_missing_branch_scalar_leaf_as_null(
    expression: &mut ScalarExpression,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<bool, CoreError> {
    match expression {
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
        | ScalarExpression::NodeLabels { .. }
        | ScalarExpression::PropertyKeys { .. }
        | ScalarExpression::RelationshipType { .. } => Ok(true),
        ScalarExpression::CountSubquery {
            pattern,
            distinct_target,
        } => {
            rewrite_missing_branch_count_subquery_as_null(pattern, graph, nodes, relationships)?;
            if let Some(target) = distinct_target {
                rewrite_missing_branch_scalar_expression_as_null(
                    target,
                    graph,
                    nodes,
                    relationships,
                )?;
            }
            Ok(true)
        }
        ScalarExpression::Predicate(predicate) => {
            **predicate = rewrite_missing_branch_property_predicate_expression(
                (**predicate).clone(),
                graph,
                nodes,
                relationships,
            )?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn rewrite_missing_branch_compound_scalar_expression_as_null(
    expression: &mut ScalarExpression,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<(), CoreError> {
    if rewrite_missing_branch_primary_compound_scalar_expression_as_null(
        expression,
        graph,
        nodes,
        relationships,
    )? {
        return Ok(());
    }
    rewrite_missing_branch_secondary_compound_scalar_expression_as_null(
        expression,
        graph,
        nodes,
        relationships,
    )
}

fn rewrite_missing_branch_primary_compound_scalar_expression_as_null(
    expression: &mut ScalarExpression,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<bool, CoreError> {
    match expression {
        ScalarExpression::PresenceGated { expression, .. } => {
            rewrite_missing_branch_scalar_expression_as_null(
                expression,
                graph,
                nodes,
                relationships,
            )?;
            Ok(true)
        }
        ScalarExpression::Coalesce { expressions } => {
            rewrite_missing_branch_scalar_expression_list_as_null(
                expressions,
                graph,
                nodes,
                relationships,
            )?;
            Ok(true)
        }
        ScalarExpression::NullIf { expression, value } => {
            rewrite_missing_branch_scalar_expression_as_null(
                expression,
                graph,
                nodes,
                relationships,
            )?;
            rewrite_missing_branch_scalar_expression_as_null(value, graph, nodes, relationships)?;
            Ok(true)
        }
        ScalarExpression::Round { expression, places } => {
            rewrite_missing_branch_scalar_expression_as_null(
                expression,
                graph,
                nodes,
                relationships,
            )?;
            if let Some(places) = places {
                rewrite_missing_branch_scalar_expression_as_null(
                    places,
                    graph,
                    nodes,
                    relationships,
                )?;
            }
            Ok(true)
        }
        ScalarExpression::Left { expression, count }
        | ScalarExpression::Right { expression, count } => {
            rewrite_missing_branch_scalar_expression_as_null(
                expression,
                graph,
                nodes,
                relationships,
            )?;
            rewrite_missing_branch_scalar_expression_as_null(count, graph, nodes, relationships)?;
            Ok(true)
        }
        ScalarExpression::StringContains {
            expression,
            pattern: operand,
        }
        | ScalarExpression::StringStartsWith {
            expression,
            pattern: operand,
        }
        | ScalarExpression::StringEndsWith {
            expression,
            pattern: operand,
        } => {
            rewrite_missing_branch_scalar_expression_as_null(
                expression,
                graph,
                nodes,
                relationships,
            )?;
            rewrite_missing_branch_scalar_expression_as_null(operand, graph, nodes, relationships)?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn rewrite_missing_branch_secondary_compound_scalar_expression_as_null(
    expression: &mut ScalarExpression,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<(), CoreError> {
    match expression {
        ScalarExpression::Replace {
            expression,
            search,
            replacement,
        } => {
            rewrite_missing_branch_scalar_expression_as_null(
                expression,
                graph,
                nodes,
                relationships,
            )?;
            rewrite_missing_branch_scalar_expression_as_null(search, graph, nodes, relationships)?;
            rewrite_missing_branch_scalar_expression_as_null(
                replacement,
                graph,
                nodes,
                relationships,
            )?;
        }
        ScalarExpression::Substring {
            expression,
            start,
            length,
        } => {
            rewrite_missing_branch_scalar_expression_as_null(
                expression,
                graph,
                nodes,
                relationships,
            )?;
            rewrite_missing_branch_scalar_expression_as_null(start, graph, nodes, relationships)?;
            if let Some(length) = length {
                rewrite_missing_branch_scalar_expression_as_null(
                    length,
                    graph,
                    nodes,
                    relationships,
                )?;
            }
        }
        ScalarExpression::Arithmetic { left, right, .. } => {
            rewrite_missing_branch_scalar_expression_as_null(left, graph, nodes, relationships)?;
            rewrite_missing_branch_scalar_expression_as_null(right, graph, nodes, relationships)?;
        }
        ScalarExpression::Atan2 { y, x } => {
            rewrite_missing_branch_scalar_expression_as_null(y, graph, nodes, relationships)?;
            rewrite_missing_branch_scalar_expression_as_null(x, graph, nodes, relationships)?;
        }
        ScalarExpression::Case {
            alternatives,
            else_expression,
        } => rewrite_missing_branch_case_expression_as_null(
            alternatives,
            else_expression.as_deref_mut(),
            graph,
            nodes,
            relationships,
        )?,
        _ => unreachable!("scalar leaves and unary expressions handled before compound rewrite"),
    }
    Ok(())
}

fn rewrite_missing_branch_count_subquery_as_null(
    pattern: &mut CountSubqueryPattern,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<(), CoreError> {
    match pattern {
        CountSubqueryPattern::Relationships(pattern) => {
            rewrite_missing_branch_exists_pattern_as_null(pattern, graph, nodes, relationships)?;
        }
        CountSubqueryPattern::Nodes {
            nodes: local_nodes,
            predicates,
            predicate,
        } => {
            let scoped_nodes = scoped_branch_nodes(nodes, local_nodes);
            let scoped_relationships = relationships.to_vec();
            rewrite_missing_branch_property_predicate_list_as_null(
                predicates,
                predicate,
                graph,
                &scoped_nodes,
                &scoped_relationships,
            )?;
        }
    }
    Ok(())
}

fn rewrite_missing_branch_exists_pattern_as_null(
    pattern: &mut ExistsPatternPredicate,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<(), CoreError> {
    let scoped_nodes = scoped_branch_nodes(nodes, &pattern.nodes);
    let scoped_relationships = scoped_branch_relationships(relationships, &pattern.relationships);
    rewrite_missing_branch_property_predicate_list_as_null(
        &mut pattern.predicates,
        &mut pattern.predicate,
        graph,
        &scoped_nodes,
        &scoped_relationships,
    )
}

fn scoped_branch_nodes(
    nodes: &BTreeMap<String, String>,
    local_nodes: &[NodePattern],
) -> BTreeMap<String, String> {
    let mut scoped_nodes = nodes.clone();
    for node in local_nodes {
        scoped_nodes.insert(node.variable.clone(), node.label.clone());
    }
    scoped_nodes
}

fn scoped_branch_relationships(
    relationships: &[RelationshipPattern],
    local_relationships: &[RelationshipPattern],
) -> Vec<RelationshipPattern> {
    let mut scoped_relationships = local_relationships.to_vec();
    scoped_relationships.extend_from_slice(relationships);
    scoped_relationships
}

fn rewrite_missing_branch_property_predicate_list_as_null(
    predicates: &mut Vec<PropertyPredicate>,
    predicate: &mut Option<Box<PredicateExpression>>,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<(), CoreError> {
    let mut retained = Vec::with_capacity(predicates.len());
    for property_predicate in std::mem::take(predicates) {
        match rewrite_missing_branch_property_predicate(
            property_predicate,
            graph,
            nodes,
            relationships,
        )? {
            BranchPropertyPredicateRewrite::Keep(property_predicate) => {
                retained.push(property_predicate);
            }
            BranchPropertyPredicateRewrite::Rewrite(expression) => {
                append_scoped_predicate_expression(expression, predicate);
            }
        }
    }
    *predicates = retained;

    if let Some(existing) = predicate.take() {
        *predicate = Some(Box::new(
            rewrite_missing_branch_property_predicate_expression(
                *existing,
                graph,
                nodes,
                relationships,
            )?,
        ));
    }
    Ok(())
}

fn append_scoped_predicate_expression(
    expression: PredicateExpression,
    predicate: &mut Option<Box<PredicateExpression>>,
) {
    let combined = match predicate.take() {
        Some(existing) => PredicateExpression::And {
            left: existing,
            right: Box::new(expression),
        },
        None => expression,
    };
    *predicate = Some(Box::new(combined));
}

fn rewrite_missing_branch_scalar_expression_list_as_null(
    expressions: &mut [ScalarExpression],
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<(), CoreError> {
    for expression in expressions {
        rewrite_missing_branch_scalar_expression_as_null(expression, graph, nodes, relationships)?;
    }
    Ok(())
}

fn rewrite_missing_branch_case_expression_as_null(
    alternatives: &mut [ScalarCaseAlternative],
    else_expression: Option<&mut ScalarExpression>,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<(), CoreError> {
    for alternative in alternatives {
        let original = std::mem::replace(&mut alternative.when, PredicateExpression::Boolean(true));
        alternative.when = rewrite_missing_branch_property_predicate_expression(
            original,
            graph,
            nodes,
            relationships,
        )?;
        rewrite_missing_branch_scalar_expression_as_null(
            &mut alternative.then,
            graph,
            nodes,
            relationships,
        )?;
    }
    if let Some(else_expression) = else_expression {
        rewrite_missing_branch_scalar_expression_as_null(
            else_expression,
            graph,
            nodes,
            relationships,
        )?;
    }
    Ok(())
}

enum BranchPropertyPredicateRewrite {
    Keep(PropertyPredicate),
    Rewrite(PredicateExpression),
}

fn rewrite_missing_branch_property_predicate(
    predicate: PropertyPredicate,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<BranchPropertyPredicateRewrite, CoreError> {
    if branch_property_is_missing(graph, nodes, relationships, &predicate.property) {
        return Ok(BranchPropertyPredicateRewrite::Rewrite(
            missing_branch_property_predicate_expression(predicate, graph, nodes, relationships)?,
        ));
    }
    if branch_predicate_rhs_is_missing_property(&predicate.rhs, graph, nodes, relationships) {
        return Ok(BranchPropertyPredicateRewrite::Rewrite(
            unknown_boolean_predicate(),
        ));
    }
    Ok(BranchPropertyPredicateRewrite::Keep(predicate))
}

fn missing_branch_property_predicate_expression(
    predicate: PropertyPredicate,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<PredicateExpression, CoreError> {
    let rhs = branch_predicate_rhs_as_scalar_rhs(predicate.rhs, graph, nodes, relationships)?;
    if rhs.was_missing_property
        || (rhs.is_null_literal && is_range_comparison_operator(predicate.operator))
    {
        return Ok(unknown_boolean_predicate());
    }
    Ok(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: ScalarExpression::Literal(Literal::Null),
        operator: predicate.operator,
        rhs: rhs.value,
    }))
}

struct BranchScalarPredicateRhs {
    value: ScalarPredicateRhs,
    is_null_literal: bool,
    was_missing_property: bool,
}

fn branch_predicate_rhs_as_scalar_rhs(
    rhs: PredicateRhs,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> Result<BranchScalarPredicateRhs, CoreError> {
    match rhs {
        PredicateRhs::Literal(literal) => {
            let is_null = matches!(literal, Literal::Null);
            Ok(BranchScalarPredicateRhs {
                value: ScalarPredicateRhs::Expression(ScalarExpression::Literal(literal)),
                is_null_literal: is_null,
                was_missing_property: false,
            })
        }
        PredicateRhs::TemporalCoercion { .. } => Err(CoreError::internal(
            "static branch rewrite cannot preserve temporal predicate coercion",
        )),
        PredicateRhs::TemporalCoercionList(_) => Err(CoreError::internal(
            "static branch rewrite cannot preserve temporal predicate list coercion",
        )),
        PredicateRhs::Property(property)
            if branch_property_is_missing(graph, nodes, relationships, &property) =>
        {
            Ok(BranchScalarPredicateRhs {
                value: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null)),
                is_null_literal: true,
                was_missing_property: true,
            })
        }
        PredicateRhs::Property(property) => Ok(BranchScalarPredicateRhs {
            value: ScalarPredicateRhs::Expression(ScalarExpression::Property(property)),
            is_null_literal: false,
            was_missing_property: false,
        }),
        PredicateRhs::Key { variable } => Ok(BranchScalarPredicateRhs {
            value: ScalarPredicateRhs::Expression(ScalarExpression::Key { variable }),
            is_null_literal: false,
            was_missing_property: false,
        }),
        PredicateRhs::ElementId { variable } => Ok(BranchScalarPredicateRhs {
            value: ScalarPredicateRhs::Expression(ScalarExpression::ElementId { variable }),
            is_null_literal: false,
            was_missing_property: false,
        }),
        PredicateRhs::List(literals) => Ok(BranchScalarPredicateRhs {
            value: ScalarPredicateRhs::List(literals),
            is_null_literal: false,
            was_missing_property: false,
        }),
    }
}

fn branch_predicate_rhs_is_missing_property(
    rhs: &PredicateRhs,
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
) -> bool {
    match rhs {
        PredicateRhs::Property(property) => {
            branch_property_is_missing(graph, nodes, relationships, property)
        }
        PredicateRhs::Literal(_)
        | PredicateRhs::TemporalCoercion { .. }
        | PredicateRhs::TemporalCoercionList(_)
        | PredicateRhs::Key { .. }
        | PredicateRhs::ElementId { .. }
        | PredicateRhs::List(_) => false,
    }
}

fn branch_property_is_missing(
    graph: &Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
    property: &PropertyRef,
) -> bool {
    if let Some(label) = nodes.get(&property.variable)
        && let Some(node) = graph.nodes.iter().find(|node| node.label == *label)
    {
        return node.column_for_property(&property.property).is_none();
    }
    if let Some(relationship) =
        branch_relationship_for_property(graph, nodes, relationships, property)
    {
        return relationship
            .column_for_property(&property.property)
            .is_none();
    }
    false
}

fn branch_relationship_for_property<'a>(
    graph: &'a Declaration,
    nodes: &BTreeMap<String, String>,
    relationships: &[RelationshipPattern],
    property: &PropertyRef,
) -> Option<&'a DeclaredRelationship> {
    let relationship = relationships.iter().find(|relationship| {
        relationship.variable.as_deref() == Some(property.variable.as_str())
    })?;
    let left_label = nodes.get(&relationship.left)?;
    let right_label = nodes.get(&relationship.right)?;
    branch_relationship_declaration(graph, relationship, left_label, right_label)
}

fn branch_relationship_declaration<'a>(
    graph: &'a Declaration,
    relationship: &RelationshipPattern,
    left_label: &str,
    right_label: &str,
) -> Option<&'a DeclaredRelationship> {
    let mut matches = graph.relationships.iter().filter(|candidate| {
        candidate.relationship_type == relationship.relationship_type
            && branch_relationship_matches_direction(
                candidate,
                relationship.direction,
                left_label,
                right_label,
            )
    });
    let relationship = matches.next()?;
    if matches.next().is_some() {
        return None;
    }
    Some(relationship)
}

fn branch_relationship_matches_direction(
    candidate: &DeclaredRelationship,
    direction: Direction,
    left_label: &str,
    right_label: &str,
) -> bool {
    match direction {
        Direction::Outgoing => {
            candidate.from.label == left_label && candidate.to.label == right_label
        }
        Direction::Incoming => {
            candidate.from.label == right_label && candidate.to.label == left_label
        }
        Direction::Undirected => {
            (candidate.from.label == left_label && candidate.to.label == right_label)
                || (candidate.from.label == right_label && candidate.to.label == left_label)
        }
    }
}

fn is_range_comparison_operator(operator: ComparisonOperator) -> bool {
    matches!(
        operator,
        ComparisonOperator::GreaterThan
            | ComparisonOperator::GreaterThanOrEqual
            | ComparisonOperator::LessThan
            | ComparisonOperator::LessThanOrEqual
    )
}

fn compile_single_query(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
) -> Result<GraphPlan, CoreError> {
    match &single_query.kind {
        SingleQueryKind::SinglePart(single_part) => compile_single_part(single_part, context),
        SingleQueryKind::MultiPart(multi_part) => compile_multi_part(multi_part, context),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExplicitUnionMode {
    All,
    Distinct,
    Mixed,
}

fn explicit_union_mode(query: &RegularQuery) -> ExplicitUnionMode {
    if query.unions.iter().all(|union| union.all) {
        return ExplicitUnionMode::All;
    }
    if query.unions.iter().all(|union| !union.all) {
        return ExplicitUnionMode::Distinct;
    }
    ExplicitUnionMode::Mixed
}

fn compile_regular_query(
    query: &RegularQuery,
    context: &CypherCompileContext,
) -> Result<GraphQuery, CoreError> {
    let first_query =
        compile_single_query_as_graph_query(&query.single_query, context, "query.single_query")?;
    if query.unions.is_empty() {
        return Ok(first_query);
    }

    let union_mode = explicit_union_mode(query);
    let mut flattened = Vec::with_capacity(query.unions.len() + 1);
    let mut flattened_static_alternative_union = false;
    append_explicit_union_component(
        first_query,
        None,
        union_mode,
        "query.single_query",
        &mut flattened,
        &mut flattened_static_alternative_union,
    )?;

    for (index, union) in query.unions.iter().enumerate() {
        let component = compile_single_query_as_graph_query(
            &union.single_query,
            context,
            format!("query.unions[{index}].single_query"),
        )?;
        append_explicit_union_component(
            component,
            Some(union.all),
            union_mode,
            format!("query.unions[{index}].single_query"),
            &mut flattened,
            &mut flattened_static_alternative_union,
        )?;
    }

    let use_outer_distinct =
        union_mode == ExplicitUnionMode::Distinct && flattened_static_alternative_union;
    let mut flattened = flattened.into_iter();
    let (first_all, first) = flattened
        .next()
        .ok_or_else(|| CoreError::internal("explicit union produced no graph plans"))?;
    if first_all.is_some() {
        return Err(CoreError::internal(
            "explicit union first graph plan unexpectedly had a union operator",
        ));
    }
    let expected_projection_names = projection_names(&first);
    let mut branches = Vec::new();
    for (index, (all, plan)) in flattened.enumerate() {
        let leading_all = all.ok_or_else(|| {
            CoreError::internal("explicit union branch graph plan had no union operator")
        })?;
        let projection_names = projection_names(&plan);
        if projection_names != expected_projection_names {
            return Err(unsupported(
                format!("query.union_branches[{index}].return"),
                format!(
                    "UNION branch projections must match the first branch; expected [{}], got [{}]",
                    expected_projection_names.join(", "),
                    projection_names.join(", ")
                ),
            ));
        }
        branches.push(GraphUnionBranch {
            all: if use_outer_distinct {
                true
            } else {
                leading_all
            },
            plan,
        });
    }

    Ok(GraphQuery::Union(GraphUnion {
        first,
        branches,
        preserve_empty_result_with_null_row: false,
        outer_projection: None,
        distinct: use_outer_distinct,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    }))
}

fn projection_names(plan: &GraphPlan) -> Vec<String> {
    plan.projection_output_names()
}

fn append_explicit_union_component(
    component: GraphQuery,
    leading_all: Option<bool>,
    union_mode: ExplicitUnionMode,
    path: impl Into<String>,
    output: &mut Vec<(Option<bool>, GraphPlan)>,
    flattened_static_alternative_union: &mut bool,
) -> Result<(), CoreError> {
    let path = path.into();
    match component {
        GraphQuery::Plan(plan) => {
            output.push((leading_all, plan));
            Ok(())
        }
        GraphQuery::Unwind(_) | GraphQuery::UnwindPipeline(_) => Err(unsupported(
            path,
            "UNWIND row-source queries with UNION require row-source union planning and are not supported yet",
        )),
        GraphQuery::Staged(_) | GraphQuery::StagedUnwind(_) => Err(unsupported(
            path,
            "staged queries with UNION require staged subquery planning and are not supported yet",
        )),
        GraphQuery::Union(union) => {
            if union_mode == ExplicitUnionMode::Mixed {
                return Err(unsupported(
                    path,
                    "pattern alternatives can be combined with uniform top-level UNION ALL or UNION; mixed UNION and UNION ALL requires nested union grouping",
                ));
            }
            if union.outer_projection.is_some()
                || !union.order_by.is_empty()
                || union.skip.is_some()
                || union.limit.is_some()
            {
                return Err(unsupported(
                    path,
                    "pattern alternatives with branch-level ORDER BY, SKIP, LIMIT, or aggregate outer projections require nested union grouping",
                ));
            }
            if union.distinct && union_mode != ExplicitUnionMode::Distinct {
                return Err(unsupported(
                    path,
                    "pattern alternatives with branch-level DISTINCT can only be flattened into uniform top-level UNION distinct",
                ));
            }
            *flattened_static_alternative_union = true;
            output.push((leading_all, union.first));
            for branch in union.branches {
                if !branch.all {
                    return Err(CoreError::internal(
                        "pattern alternative expansion produced a non-UNION ALL branch",
                    ));
                }
                output.push((Some(true), branch.plan));
            }
            Ok(())
        }
    }
}

fn expand_single_query_pattern_alternatives(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
) -> Result<Vec<ExpandedSingleQuery>, CoreError> {
    let mut expanded = vec![ExpandedSingleQuery {
        query: single_query.clone(),
        force_empty: false,
        required_presences: BTreeSet::new(),
    }];
    loop {
        let mut progressed = false;
        let mut next = Vec::with_capacity(expanded.len());
        for expanded_query in expanded {
            if let Some(site) =
                first_static_label_type_alternative_site(&expanded_query.query, context)?
            {
                progressed = true;
                let alternatives = match &site {
                    StaticLabelTypeAlternativeSite::SinglePart { alternatives, .. }
                    | StaticLabelTypeAlternativeSite::MultiPart { alternatives, .. } => {
                        alternatives.clone()
                    }
                };
                for alternative in alternatives {
                    if next.len() >= MAX_PATTERN_ALTERNATIVE_BRANCHES {
                        return Err(unsupported(
                            "query.pattern",
                            format!(
                                "pattern alternatives expand to more than {MAX_PATTERN_ALTERNATIVE_BRANCHES} branches; simplify the pattern or split the query explicitly"
                            ),
                        ));
                    }
                    let mut variant = expanded_query.query.clone();
                    apply_static_label_type_alternative(&mut variant, &site, alternative)?;
                    next.push(ExpandedSingleQuery {
                        query: variant,
                        force_empty: expanded_query.force_empty,
                        required_presences: expanded_query.required_presences.clone(),
                    });
                }
                continue;
            }

            if let Some(site) =
                first_bounded_relationship_range_site(&expanded_query.query, context)?
            {
                progressed = true;
                let alternatives = match &site {
                    BoundedRelationshipRangeSite::SinglePart { alternatives, .. }
                    | BoundedRelationshipRangeSite::MultiPart { alternatives, .. } => {
                        alternatives.clone()
                    }
                };
                for alternative in alternatives {
                    if next.len() >= MAX_PATTERN_ALTERNATIVE_BRANCHES {
                        return Err(unsupported(
                            "query.pattern",
                            format!(
                                "pattern alternatives expand to more than {MAX_PATTERN_ALTERNATIVE_BRANCHES} branches; simplify the pattern or split the query explicitly"
                            ),
                        ));
                    }
                    let mut variant = expanded_query.query.clone();
                    apply_bounded_relationship_range_alternative(
                        &mut variant,
                        &site,
                        alternative.length,
                    )?;
                    next.push(ExpandedSingleQuery {
                        query: variant,
                        force_empty: expanded_query.force_empty || alternative.force_empty,
                        required_presences: expanded_query.required_presences.clone(),
                    });
                }
                continue;
            }

            next.push(expanded_query);
        }
        expanded = next;
        if !progressed {
            return Ok(expanded);
        }
    }
}

fn first_static_label_type_alternative_site(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
) -> Result<Option<StaticLabelTypeAlternativeSite>, CoreError> {
    match &single_query.kind {
        SingleQueryKind::SinglePart(single_part) => {
            first_single_part_static_label_type_alternative_site(single_part, context)
        }
        SingleQueryKind::MultiPart(multi_part) => {
            first_multi_part_static_label_type_alternative_site(multi_part, context)
        }
    }
}

fn first_single_part_static_label_type_alternative_site(
    single_part: &SinglePartQuery,
    context: &CypherCompileContext,
) -> Result<Option<StaticLabelTypeAlternativeSite>, CoreError> {
    Ok(first_reading_clause_static_label_type_alternative_site(
        &single_part.reading_clauses,
        context,
        true,
    )?
    .map(
        |(reading_clause_index, pattern_part_index, target, alternatives)| {
            StaticLabelTypeAlternativeSite::SinglePart {
                reading_clause_index,
                pattern_part_index,
                target,
                alternatives,
            }
        },
    ))
}

fn first_multi_part_static_label_type_alternative_site(
    multi_part: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<StaticLabelTypeAlternativeSite>, CoreError> {
    for (part_index, part) in multi_part.parts.iter().enumerate() {
        if let Some((reading_clause_index, pattern_part_index, target, alternatives)) =
            first_reading_clause_static_label_type_alternative_site(
                &part.reading_clauses,
                context,
                false,
            )?
        {
            return Ok(Some(StaticLabelTypeAlternativeSite::MultiPart {
                query_part: MultiPartAlternativePart::Part(part_index),
                reading_clause_index,
                pattern_part_index,
                target,
                alternatives,
            }));
        }
    }
    Ok(first_reading_clause_static_label_type_alternative_site(
        &multi_part.final_part.reading_clauses,
        context,
        false,
    )?
    .map(
        |(reading_clause_index, pattern_part_index, target, alternatives)| {
            StaticLabelTypeAlternativeSite::MultiPart {
                query_part: MultiPartAlternativePart::FinalPart,
                reading_clause_index,
                pattern_part_index,
                target,
                alternatives,
            }
        },
    ))
}

fn first_reading_clause_static_label_type_alternative_site(
    reading_clauses: &[ReadingClause],
    context: &CypherCompileContext,
    allow_relationship_mapping_alternatives: bool,
) -> Result<Option<ReadingClauseLabelTypeAlternativeSite>, CoreError> {
    for (reading_clause_index, clause) in reading_clauses.iter().enumerate() {
        let ReadingClause::Match(match_clause) = clause else {
            continue;
        };
        let allow_relationship_mapping_alternatives =
            allow_relationship_mapping_alternatives && reading_clause_index == 0;
        if let Some((pattern_part_index, target, alternatives)) =
            first_match_static_label_type_alternative_site(
                match_clause,
                context,
                allow_relationship_mapping_alternatives,
            )?
        {
            return Ok(Some((
                reading_clause_index,
                pattern_part_index,
                target,
                alternatives,
            )));
        }
    }
    Ok(None)
}

fn first_match_static_label_type_alternative_site(
    match_clause: &Match,
    context: &CypherCompileContext,
    allow_relationship_mapping_alternatives: bool,
) -> Result<Option<MatchLabelTypeAlternativeSite>, CoreError> {
    for (part_index, pattern_part) in match_clause.pattern.parts.iter().enumerate() {
        let Some((start, chains)) = pattern_element_path(&pattern_part.anonymous.element) else {
            continue;
        };
        if let Some(alternatives) =
            graph_declared_standalone_node_label_alternatives(start, chains, context)
        {
            return Ok(Some((
                part_index,
                PatternAlternativeTarget::StartNode,
                alternatives,
            )));
        }
        let raw_alternatives = label_expression_list_alternatives(
            &start.labels,
            "query.pattern.start.labels",
            context,
        )?;
        if raw_alternatives.len() > 1 {
            let alternatives = deduplicate_node_label_alternatives(raw_alternatives);
            return Ok(Some((
                part_index,
                PatternAlternativeTarget::StartNode,
                alternatives
                    .into_iter()
                    .map(LabelTypeAlternative::NodeLabels)
                    .collect(),
            )));
        }
        for (chain_index, chain) in chains.iter().enumerate() {
            if let Some(types) = chain
                .relationship
                .detail
                .as_ref()
                .and_then(|detail| detail.types.as_ref())
            {
                let raw_alternatives = label_expression_alternatives(
                    types,
                    format!("query.pattern.relationships[{chain_index}].types"),
                    context,
                )?;
                if raw_alternatives.len() > 1 {
                    let alternatives = deduplicate_relationship_type_alternatives(raw_alternatives);
                    return Ok(Some((
                        part_index,
                        PatternAlternativeTarget::Relationship(chain_index),
                        alternatives
                            .into_iter()
                            .map(LabelTypeAlternative::RelationshipType)
                            .collect(),
                    )));
                }
            }
            let raw_alternatives = label_expression_list_alternatives(
                &chain.node.labels,
                format!("query.pattern.nodes[{}].labels", chain_index + 1),
                context,
            )?;
            if raw_alternatives.len() > 1 {
                let alternatives = deduplicate_node_label_alternatives(raw_alternatives);
                return Ok(Some((
                    part_index,
                    PatternAlternativeTarget::ChainNode(chain_index),
                    alternatives
                        .into_iter()
                        .map(LabelTypeAlternative::NodeLabels)
                        .collect(),
                )));
            }
            if allow_relationship_mapping_alternatives
                && match_clause.optional
                && match_clause.pattern.parts.len() == 1
                && pattern_part_is_single_fixed_relationship(pattern_part)
                && let Some(alternatives) = graph_declared_relationship_mapping_alternatives(
                    start,
                    chains,
                    chain_index,
                    context,
                )?
            {
                return Ok(Some((
                    part_index,
                    PatternAlternativeTarget::RelationshipMapping(chain_index),
                    alternatives,
                )));
            }
        }
    }
    Ok(None)
}

fn graph_declared_standalone_node_label_alternatives(
    start: &CypherNodePattern,
    chains: &[PatternElementChain],
    context: &CypherCompileContext,
) -> Option<Vec<LabelTypeAlternative>> {
    if !start.labels.is_empty() || !chains.is_empty() {
        return None;
    }
    let graph = context.graph.as_ref()?;
    if graph.nodes.is_empty() {
        return None;
    }
    Some(
        graph
            .nodes
            .iter()
            .map(|node| {
                LabelTypeAlternative::NodeLabels(vec![LabelExpression::Static(SymbolicName {
                    name: node.label.clone(),
                    span: start.span,
                })])
            })
            .collect(),
    )
}

fn graph_declared_relationship_mapping_alternatives(
    start: &CypherNodePattern,
    chains: &[PatternElementChain],
    chain_index: usize,
    context: &CypherCompileContext,
) -> Result<Option<Vec<LabelTypeAlternative>>, CoreError> {
    let Some(graph) = context.graph.as_ref() else {
        return Ok(None);
    };
    let Some(chain) = chains.get(chain_index) else {
        return Ok(None);
    };
    let relationship_path = format!("query.pattern.relationships[{chain_index}]");
    if relationship_fixed_length(&chain.relationship, &relationship_path).unwrap_or(0) != 1 {
        return Ok(None);
    }

    let left_node = if chain_index == 0 {
        start
    } else {
        &chains
            .get(chain_index - 1)
            .ok_or_else(|| CoreError::internal("relationship alternative left node missing"))?
            .node
    };
    let left_label = optional_single_compile_time_label(
        &left_node.labels,
        format!("query.pattern.nodes[{chain_index}].labels"),
        context,
    )?;
    let right_label = optional_single_compile_time_label(
        &chain.node.labels,
        format!("query.pattern.nodes[{}].labels", chain_index + 1),
        context,
    )?;
    let relationship_type = chain
        .relationship
        .detail
        .as_ref()
        .and_then(|detail| detail.types.as_ref())
        .map(|types| {
            single_compile_time_label(
                std::slice::from_ref(types),
                format!("{relationship_path}.types"),
                context,
            )
        })
        .transpose()?;

    let direction = cypher_relationship_direction(chain.relationship.direction);
    let mut seen = BTreeSet::new();
    let mut alternatives = Vec::new();
    for relationship in &graph.relationships {
        if relationship_type
            .as_ref()
            .is_some_and(|expected| expected != &relationship.relationship_type)
        {
            continue;
        }
        for (candidate_left, candidate_right) in
            relationship_label_pairs_for_direction(relationship, direction)
        {
            if left_label
                .as_ref()
                .is_some_and(|expected| expected != &candidate_left)
                || right_label
                    .as_ref()
                    .is_some_and(|expected| expected != &candidate_right)
            {
                continue;
            }
            if !seen.insert((
                candidate_left.clone(),
                relationship.relationship_type.clone(),
                candidate_right.clone(),
            )) {
                continue;
            }
            alternatives.push(LabelTypeAlternative::RelationshipMapping {
                left_label: static_label_expression(candidate_left, left_node.span),
                relationship_type: static_label_expression(
                    relationship.relationship_type.clone(),
                    chain.relationship.span,
                ),
                right_label: static_label_expression(candidate_right, chain.node.span),
            });
        }
    }

    Ok((alternatives.len() > 1).then_some(alternatives))
}

fn cypher_relationship_direction(direction: CypherRelationshipDirection) -> Direction {
    match direction {
        CypherRelationshipDirection::Right => Direction::Outgoing,
        CypherRelationshipDirection::Left => Direction::Incoming,
        CypherRelationshipDirection::Both | CypherRelationshipDirection::Undirected => {
            Direction::Undirected
        }
    }
}

fn static_label_expression(name: String, span: decypher::error::Span) -> LabelExpression {
    LabelExpression::Static(SymbolicName { name, span })
}

fn deduplicate_node_label_alternatives(
    alternatives: Vec<Vec<LabelExpression>>,
) -> Vec<Vec<LabelExpression>> {
    let mut seen = BTreeSet::new();
    alternatives
        .into_iter()
        .filter(|alternative| {
            let Ok(label) = single_static_label(alternative, "query.pattern.alternative") else {
                return true;
            };
            seen.insert(label)
        })
        .collect()
}

fn deduplicate_relationship_type_alternatives(
    alternatives: Vec<LabelExpression>,
) -> Vec<LabelExpression> {
    let mut seen = BTreeSet::new();
    alternatives
        .into_iter()
        .filter(|alternative| {
            let Ok(relationship_type) = single_static_label(
                std::slice::from_ref(alternative),
                "query.pattern.alternative",
            ) else {
                return true;
            };
            seen.insert(relationship_type)
        })
        .collect()
}

fn label_expression_list_alternatives(
    labels: &[LabelExpression],
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Vec<Vec<LabelExpression>>, CoreError> {
    let path = path.into();
    let mut variants = vec![Vec::new()];
    for (index, label) in labels.iter().enumerate() {
        let label_alternatives =
            label_expression_alternatives(label, format!("{path}[{index}]"), context)?;
        let mut next = Vec::with_capacity(variants.len() * label_alternatives.len());
        for variant in &variants {
            for label_alternative in &label_alternatives {
                let mut next_variant = variant.clone();
                next_variant.push(label_alternative.clone());
                next.push(next_variant);
            }
        }
        variants = next;
    }
    Ok(variants)
}

fn label_expression_alternatives(
    expression: &LabelExpression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Vec<LabelExpression>, CoreError> {
    let path = path.into();
    match expression {
        LabelExpression::Or { lhs, rhs, .. } => {
            let lhs = label_expression_alternatives(lhs, format!("{path}.lhs"), context)?;
            let rhs = label_expression_alternatives(rhs, format!("{path}.rhs"), context)?;
            Ok(lhs.into_iter().chain(rhs).collect())
        }
        LabelExpression::And { lhs, rhs, span } => {
            let lhs_alternatives =
                label_expression_alternatives(lhs, format!("{path}.lhs"), context)?;
            let rhs_alternatives =
                label_expression_alternatives(rhs, format!("{path}.rhs"), context)?;
            let mut alternatives =
                Vec::with_capacity(lhs_alternatives.len() * rhs_alternatives.len());
            for lhs_alternative in &lhs_alternatives {
                for rhs_alternative in &rhs_alternatives {
                    alternatives.push(LabelExpression::And {
                        lhs: Box::new(lhs_alternative.clone()),
                        rhs: Box::new(rhs_alternative.clone()),
                        span: *span,
                    });
                }
            }
            Ok(alternatives)
        }
        LabelExpression::Group { inner, .. } => label_expression_alternatives(inner, path, context),
        LabelExpression::Static(_) => Ok(vec![expression.clone()]),
        LabelExpression::Dynamic { expression, span } => {
            let names = compile_dynamic_label_expressions(expression, path, context)?;
            Ok(names
                .into_iter()
                .map(|name| LabelExpression::Static(SymbolicName { name, span: *span }))
                .collect())
        }
        LabelExpression::Not { .. } => Ok(vec![resolve_compile_time_label_expression(
            expression, path, context,
        )?]),
    }
}

fn resolve_compile_time_label_expression(
    expression: &LabelExpression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<LabelExpression, CoreError> {
    let path = path.into();
    match expression {
        LabelExpression::Static(_) => Ok(expression.clone()),
        LabelExpression::Dynamic { expression, span } => {
            let name = compile_dynamic_label_expression(expression, path, context)?;
            Ok(LabelExpression::Static(SymbolicName { name, span: *span }))
        }
        LabelExpression::Or { lhs, rhs, span } => Ok(LabelExpression::Or {
            lhs: Box::new(resolve_compile_time_label_expression(
                lhs,
                format!("{path}.lhs"),
                context,
            )?),
            rhs: Box::new(resolve_compile_time_label_expression(
                rhs,
                format!("{path}.rhs"),
                context,
            )?),
            span: *span,
        }),
        LabelExpression::And { lhs, rhs, span } => Ok(LabelExpression::And {
            lhs: Box::new(resolve_compile_time_label_expression(
                lhs,
                format!("{path}.lhs"),
                context,
            )?),
            rhs: Box::new(resolve_compile_time_label_expression(
                rhs,
                format!("{path}.rhs"),
                context,
            )?),
            span: *span,
        }),
        LabelExpression::Not { inner, span } => Ok(LabelExpression::Not {
            inner: Box::new(resolve_compile_time_label_expression(
                inner,
                format!("{path}.inner"),
                context,
            )?),
            span: *span,
        }),
        LabelExpression::Group { inner, span } => Ok(LabelExpression::Group {
            inner: Box::new(resolve_compile_time_label_expression(inner, path, context)?),
            span: *span,
        }),
    }
}

fn apply_static_label_type_alternative(
    single_query: &mut SingleQuery,
    site: &StaticLabelTypeAlternativeSite,
    alternative: LabelTypeAlternative,
) -> Result<(), CoreError> {
    match site {
        StaticLabelTypeAlternativeSite::SinglePart {
            reading_clause_index,
            pattern_part_index,
            target,
            ..
        } => {
            let SingleQueryKind::SinglePart(single_part) = &mut single_query.kind else {
                return Err(CoreError::internal(
                    "single-part label/type alternative site applied to multi-part query",
                ));
            };
            apply_reading_clause_static_label_type_alternative(
                &mut single_part.reading_clauses,
                *reading_clause_index,
                *pattern_part_index,
                *target,
                alternative,
            )
        }
        StaticLabelTypeAlternativeSite::MultiPart {
            query_part,
            reading_clause_index,
            pattern_part_index,
            target,
            ..
        } => {
            let SingleQueryKind::MultiPart(multi_part) = &mut single_query.kind else {
                return Err(CoreError::internal(
                    "multi-part label/type alternative site applied to single-part query",
                ));
            };
            let reading_clauses = match query_part {
                MultiPartAlternativePart::Part(index) => multi_part
                    .parts
                    .get_mut(*index)
                    .map(|part| &mut part.reading_clauses),
                MultiPartAlternativePart::FinalPart => {
                    Some(&mut multi_part.final_part.reading_clauses)
                }
            }
            .ok_or_else(|| CoreError::internal("multi-part alternative site is out of bounds"))?;
            apply_reading_clause_static_label_type_alternative(
                reading_clauses,
                *reading_clause_index,
                *pattern_part_index,
                *target,
                alternative,
            )
        }
    }
}

fn apply_reading_clause_static_label_type_alternative(
    reading_clauses: &mut [ReadingClause],
    reading_clause_index: usize,
    pattern_part_index: usize,
    target: PatternAlternativeTarget,
    alternative: LabelTypeAlternative,
) -> Result<(), CoreError> {
    let ReadingClause::Match(match_clause) = reading_clauses
        .get_mut(reading_clause_index)
        .ok_or_else(|| {
            CoreError::internal("label/type alternative reading clause is out of bounds")
        })?
    else {
        return Err(CoreError::internal(
            "label/type alternative site did not point at a MATCH clause",
        ));
    };
    let pattern_part = match_clause
        .pattern
        .parts
        .get_mut(pattern_part_index)
        .ok_or_else(|| {
            CoreError::internal("label/type alternative pattern part is out of bounds")
        })?;
    let Some((start, chains)) = pattern_element_path_mut(&mut pattern_part.anonymous.element)
    else {
        return Err(CoreError::internal(
            "label/type alternative site did not point at a path pattern",
        ));
    };
    match (target, alternative) {
        (PatternAlternativeTarget::StartNode, LabelTypeAlternative::NodeLabels(labels)) => {
            start.labels = labels;
            Ok(())
        }
        (PatternAlternativeTarget::ChainNode(index), LabelTypeAlternative::NodeLabels(labels)) => {
            let chain = chains
                .get_mut(index)
                .ok_or_else(|| CoreError::internal("node alternative chain is out of bounds"))?;
            chain.node.labels = labels;
            Ok(())
        }
        (
            PatternAlternativeTarget::Relationship(index),
            LabelTypeAlternative::RelationshipType(relationship_type),
        ) => {
            let chain = chains.get_mut(index).ok_or_else(|| {
                CoreError::internal("relationship alternative chain is out of bounds")
            })?;
            let detail =
                chain.relationship.detail.as_mut().ok_or_else(|| {
                    CoreError::internal("relationship alternative detail is missing")
                })?;
            detail.types = Some(relationship_type);
            Ok(())
        }
        (
            PatternAlternativeTarget::RelationshipMapping(index),
            LabelTypeAlternative::RelationshipMapping {
                left_label,
                relationship_type,
                right_label,
            },
        ) => {
            apply_path_node_label_alternative(start, chains, index, left_label)?;
            apply_path_node_label_alternative(start, chains, index + 1, right_label)?;
            let chain = chains.get_mut(index).ok_or_else(|| {
                CoreError::internal("relationship mapping alternative chain is out of bounds")
            })?;
            let span = chain.relationship.span;
            let detail = chain.relationship.detail.get_or_insert(RelationshipDetail {
                variable: None,
                types: None,
                range: None,
                properties: None,
                span,
            });
            detail.types = Some(relationship_type);
            Ok(())
        }
        _ => Err(CoreError::internal(
            "label/type alternative site and replacement kind did not match",
        )),
    }
}

fn apply_path_node_label_alternative(
    start: &mut CypherNodePattern,
    chains: &mut [PatternElementChain],
    position: usize,
    label: LabelExpression,
) -> Result<(), CoreError> {
    if position == 0 {
        start.labels = vec![label];
        return Ok(());
    }
    let chain = chains.get_mut(position - 1).ok_or_else(|| {
        CoreError::internal("relationship mapping node position is out of bounds")
    })?;
    chain.node.labels = vec![label];
    Ok(())
}

fn first_bounded_relationship_range_site(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
) -> Result<Option<BoundedRelationshipRangeSite>, CoreError> {
    match &single_query.kind {
        SingleQueryKind::SinglePart(single_part) => {
            first_single_part_bounded_relationship_range_site(single_part, context)
        }
        SingleQueryKind::MultiPart(multi_part) => {
            first_multi_part_bounded_relationship_range_site(multi_part, context)
        }
    }
}

fn first_single_part_bounded_relationship_range_site(
    single_part: &SinglePartQuery,
    context: &CypherCompileContext,
) -> Result<Option<BoundedRelationshipRangeSite>, CoreError> {
    first_reading_clause_bounded_relationship_range_site(&single_part.reading_clauses, context).map(
        |site| {
            site.map(
                |(reading_clause_index, pattern_part_index, chain_index, target, alternatives)| {
                    BoundedRelationshipRangeSite::SinglePart {
                        reading_clause_index,
                        pattern_part_index,
                        chain_index,
                        target,
                        alternatives,
                    }
                },
            )
        },
    )
}

fn first_multi_part_bounded_relationship_range_site(
    multi_part: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<BoundedRelationshipRangeSite>, CoreError> {
    for (part_index, part) in multi_part.parts.iter().enumerate() {
        if let Some((reading_clause_index, pattern_part_index, chain_index, target, alternatives)) =
            first_reading_clause_bounded_relationship_range_site(&part.reading_clauses, context)?
        {
            return Ok(Some(BoundedRelationshipRangeSite::MultiPart {
                query_part: MultiPartAlternativePart::Part(part_index),
                reading_clause_index,
                pattern_part_index,
                chain_index,
                target,
                alternatives,
            }));
        }
    }
    first_reading_clause_bounded_relationship_range_site(
        &multi_part.final_part.reading_clauses,
        context,
    )
    .map(|site| {
        site.map(
            |(reading_clause_index, pattern_part_index, chain_index, target, alternatives)| {
                BoundedRelationshipRangeSite::MultiPart {
                    query_part: MultiPartAlternativePart::FinalPart,
                    reading_clause_index,
                    pattern_part_index,
                    chain_index,
                    target,
                    alternatives,
                }
            },
        )
    })
}

fn first_reading_clause_bounded_relationship_range_site(
    reading_clauses: &[ReadingClause],
    context: &CypherCompileContext,
) -> Result<Option<BoundedRelationshipRangeSiteInfo>, CoreError> {
    for (reading_clause_index, clause) in reading_clauses.iter().enumerate() {
        let ReadingClause::Match(match_clause) = clause else {
            continue;
        };
        if let Some((pattern_part_index, chain_index, target, alternatives)) =
            first_match_bounded_relationship_range_site(match_clause, context)?
        {
            if match_clause.optional {
                return Err(unsupported(
                    format!(
                        "match.reading_clauses[{reading_clause_index}].pattern.parts[{pattern_part_index}].relationships[{chain_index}]"
                    ),
                    "OPTIONAL MATCH with bounded variable-length relationship ranges is not supported yet because branch expansion would duplicate unmatched null rows",
                ));
            }
            return Ok(Some((
                reading_clause_index,
                pattern_part_index,
                chain_index,
                target,
                alternatives,
            )));
        }
    }
    Ok(None)
}

fn first_match_bounded_relationship_range_site(
    match_clause: &Match,
    context: &CypherCompileContext,
) -> Result<Option<MatchBoundedRelationshipRangeSiteInfo>, CoreError> {
    for (part_index, pattern_part) in match_clause.pattern.parts.iter().enumerate() {
        let Some((start, chains)) = pattern_element_path(&pattern_part.anonymous.element) else {
            continue;
        };
        for (chain_index, chain) in chains.iter().enumerate() {
            if let Some((target, alternatives)) = bounded_relationship_range_alternatives(
                &chain.relationship,
                format!("match.pattern.parts[{part_index}].relationships[{chain_index}]"),
            )? {
                let alternatives = filter_bounded_relationship_range_alternatives(
                    start,
                    chains,
                    chain_index,
                    &chain.relationship,
                    alternatives,
                    context,
                    format!("match.pattern.parts[{part_index}].relationships[{chain_index}]"),
                )?;
                return Ok(Some((part_index, chain_index, target, alternatives)));
            }
        }
    }
    Ok(None)
}

fn bounded_relationship_range_alternatives(
    pattern: &CypherRelationshipPattern,
    path: impl Into<String>,
) -> Result<Option<(RelationshipRangeTarget, Vec<usize>)>, CoreError> {
    let path = path.into();
    let detail_range = pattern
        .detail
        .as_ref()
        .and_then(|detail| detail.range.as_ref());
    if detail_range.is_some() && pattern.quantifier.is_some() {
        return Err(unsupported(
            path,
            "relationship patterns cannot combine a variable-length range and a GQL quantifier",
        ));
    }
    if let Some(range) = detail_range {
        return bounded_range_alternatives(
            range.start,
            range.end,
            format!("{path}.range"),
            "variable-length relationship ranges require finite non-negative bounds such as *0..3 or *1..3; unbounded ranges are not supported yet",
        )
        .map(|alternatives| alternatives.map(|alternatives| (RelationshipRangeTarget::DetailRange, alternatives)));
    }
    if let Some(quantifier) = pattern.quantifier.as_ref() {
        return bounded_range_alternatives(
            quantifier.start,
            quantifier.end,
            format!("{path}.quantifier"),
            "relationship quantifiers require finite non-negative bounds such as {0,3} or {1,3}; unbounded quantifiers are not supported yet",
        )
        .map(|alternatives| alternatives.map(|alternatives| (RelationshipRangeTarget::Quantifier, alternatives)));
    }
    Ok(None)
}

fn bounded_range_alternatives(
    start: Option<i64>,
    end: Option<i64>,
    path: impl Into<String>,
    message: &'static str,
) -> Result<Option<Vec<usize>>, CoreError> {
    let path = path.into();
    let (Some(start), Some(end)) = (start, end) else {
        return Err(unsupported(path, message));
    };
    if start == end {
        return Ok(None);
    }
    if start < 0 || end < 0 || start > end {
        return Err(unsupported(path, message));
    }
    let start = usize::try_from(start).map_err(|error| {
        unsupported(
            path.clone(),
            format!("bounded relationship range lower bound is out of range: {error}"),
        )
    })?;
    let end = usize::try_from(end).map_err(|error| {
        unsupported(
            path.clone(),
            format!("bounded relationship range upper bound is out of range: {error}"),
        )
    })?;
    if end > MAX_FIXED_RELATIONSHIP_LENGTH {
        return Err(unsupported(
            path,
            format!(
                "bounded relationship range upper bound {end} exceeds Coral's current maximum of {MAX_FIXED_RELATIONSHIP_LENGTH} hops"
            ),
        ));
    }
    Ok(Some((start..=end).collect()))
}

fn filter_bounded_relationship_range_alternatives(
    start: &CypherNodePattern,
    chains: &[PatternElementChain],
    chain_index: usize,
    relationship: &CypherRelationshipPattern,
    alternatives: Vec<usize>,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<Vec<BoundedRelationshipRangeAlternative>, CoreError> {
    let path = path.into();
    let Some(graph) = context.graph.as_ref() else {
        return Ok(normal_bounded_relationship_range_alternatives(alternatives));
    };
    let (Some(start_label), Some(end_label)) =
        bounded_relationship_endpoint_labels(start, chains, chain_index, &path, context)?
    else {
        return Ok(normal_bounded_relationship_range_alternatives(alternatives));
    };
    let Some(relationship_type) = bounded_relationship_type(relationship, &path, context)? else {
        return Ok(normal_bounded_relationship_range_alternatives(alternatives));
    };
    let direction = relationship_pattern_direction(relationship.direction);
    let adjacency = fixed_length_label_adjacency(graph, &relationship_type, direction);
    let mut feasible = Vec::new();
    for length in alternatives {
        if feasible_fixed_relationship_length(
            &relationship_type,
            &adjacency,
            &start_label,
            &end_label,
            length,
            &path,
        )? {
            feasible.push(length);
        }
    }
    if feasible.is_empty() {
        return Ok(vec![BoundedRelationshipRangeAlternative {
            length: 0,
            force_empty: true,
        }]);
    }
    Ok(normal_bounded_relationship_range_alternatives(feasible))
}

fn normal_bounded_relationship_range_alternatives(
    lengths: Vec<usize>,
) -> Vec<BoundedRelationshipRangeAlternative> {
    lengths
        .into_iter()
        .map(|length| BoundedRelationshipRangeAlternative {
            length,
            force_empty: false,
        })
        .collect()
}

fn bounded_relationship_endpoint_labels(
    start: &CypherNodePattern,
    chains: &[PatternElementChain],
    chain_index: usize,
    path: &str,
    context: &CypherCompileContext,
) -> Result<(Option<String>, Option<String>), CoreError> {
    let left = if chain_index == 0 {
        start
    } else {
        &chains
            .get(chain_index - 1)
            .ok_or_else(|| CoreError::internal("bounded range chain start is out of bounds"))?
            .node
    };
    let right = &chains
        .get(chain_index)
        .ok_or_else(|| CoreError::internal("bounded range chain end is out of bounds"))?
        .node;
    Ok((
        optional_single_compile_time_label(&left.labels, format!("{path}.start.labels"), context)?,
        optional_single_compile_time_label(&right.labels, format!("{path}.end.labels"), context)?,
    ))
}

fn bounded_relationship_type(
    relationship: &CypherRelationshipPattern,
    path: &str,
    context: &CypherCompileContext,
) -> Result<Option<String>, CoreError> {
    relationship
        .detail
        .as_ref()
        .and_then(|detail| detail.types.as_ref())
        .map(|relationship_type| {
            single_compile_time_label(
                std::slice::from_ref(relationship_type),
                format!("{path}.types"),
                context,
            )
        })
        .transpose()
}

fn relationship_pattern_direction(direction: CypherRelationshipDirection) -> Direction {
    match direction {
        CypherRelationshipDirection::Right => Direction::Outgoing,
        CypherRelationshipDirection::Left => Direction::Incoming,
        CypherRelationshipDirection::Both | CypherRelationshipDirection::Undirected => {
            Direction::Undirected
        }
    }
}

fn feasible_fixed_relationship_length(
    relationship_type: &str,
    adjacency: &BTreeMap<String, BTreeSet<String>>,
    start_label: &str,
    end_label: &str,
    length: usize,
    path: &str,
) -> Result<bool, CoreError> {
    if length == 0 {
        return Ok(start_label == end_label);
    }
    let sequences =
        fixed_length_label_sequences_with_adjacency(adjacency, start_label, end_label, length);
    match sequences.len() {
        0 => Ok(false),
        1 => Ok(true),
        count => Err(unsupported(
            path,
            format!(
                "bounded relationship range found at least {count} possible {length}-hop '{relationship_type}' label paths from {start_label} to {end_label}; use explicit intermediate nodes to disambiguate"
            ),
        )),
    }
}

fn apply_bounded_relationship_range_alternative(
    single_query: &mut SingleQuery,
    site: &BoundedRelationshipRangeSite,
    length: usize,
) -> Result<(), CoreError> {
    match site {
        BoundedRelationshipRangeSite::SinglePart {
            reading_clause_index,
            pattern_part_index,
            chain_index,
            target,
            ..
        } => {
            let SingleQueryKind::SinglePart(single_part) = &mut single_query.kind else {
                return Err(CoreError::internal(
                    "single-part bounded range site applied to multi-part query",
                ));
            };
            apply_reading_clause_bounded_relationship_range_alternative(
                &mut single_part.reading_clauses,
                *reading_clause_index,
                *pattern_part_index,
                *chain_index,
                *target,
                length,
            )
        }
        BoundedRelationshipRangeSite::MultiPart {
            query_part,
            reading_clause_index,
            pattern_part_index,
            chain_index,
            target,
            ..
        } => {
            let SingleQueryKind::MultiPart(multi_part) = &mut single_query.kind else {
                return Err(CoreError::internal(
                    "multi-part bounded range site applied to single-part query",
                ));
            };
            let reading_clauses = match query_part {
                MultiPartAlternativePart::Part(index) => multi_part
                    .parts
                    .get_mut(*index)
                    .map(|part| &mut part.reading_clauses),
                MultiPartAlternativePart::FinalPart => {
                    Some(&mut multi_part.final_part.reading_clauses)
                }
            }
            .ok_or_else(|| CoreError::internal("multi-part bounded range site is out of bounds"))?;
            apply_reading_clause_bounded_relationship_range_alternative(
                reading_clauses,
                *reading_clause_index,
                *pattern_part_index,
                *chain_index,
                *target,
                length,
            )
        }
    }
}

fn apply_reading_clause_bounded_relationship_range_alternative(
    reading_clauses: &mut [ReadingClause],
    reading_clause_index: usize,
    pattern_part_index: usize,
    chain_index: usize,
    target: RelationshipRangeTarget,
    length: usize,
) -> Result<(), CoreError> {
    let ReadingClause::Match(match_clause) = reading_clauses
        .get_mut(reading_clause_index)
        .ok_or_else(|| CoreError::internal("bounded range reading clause is out of bounds"))?
    else {
        return Err(CoreError::internal(
            "bounded range site did not point at a MATCH clause",
        ));
    };
    let pattern_part = match_clause
        .pattern
        .parts
        .get_mut(pattern_part_index)
        .ok_or_else(|| CoreError::internal("bounded range pattern part is out of bounds"))?;
    let Some((_, chains)) = pattern_element_path_mut(&mut pattern_part.anonymous.element) else {
        return Err(CoreError::internal(
            "bounded range site did not point at a path pattern",
        ));
    };
    let chain = chains
        .get_mut(chain_index)
        .ok_or_else(|| CoreError::internal("bounded range chain is out of bounds"))?;
    set_exact_relationship_range(&mut chain.relationship, target, length)
}

fn set_exact_relationship_range(
    relationship: &mut CypherRelationshipPattern,
    target: RelationshipRangeTarget,
    length: usize,
) -> Result<(), CoreError> {
    let length = i64::try_from(length)
        .map_err(|error| CoreError::internal(format!("range length out of range: {error}")))?;
    match target {
        RelationshipRangeTarget::DetailRange => {
            let detail = relationship
                .detail
                .as_mut()
                .ok_or_else(|| CoreError::internal("bounded range relationship detail missing"))?;
            let range = detail
                .range
                .as_mut()
                .ok_or_else(|| CoreError::internal("bounded relationship range missing"))?;
            set_exact_range_literal(range, length);
            Ok(())
        }
        RelationshipRangeTarget::Quantifier => {
            let quantifier = relationship
                .quantifier
                .as_mut()
                .ok_or_else(|| CoreError::internal("bounded relationship quantifier missing"))?;
            set_exact_quantifier(quantifier, length);
            Ok(())
        }
    }
}

fn set_exact_range_literal(range: &mut RangeLiteral, length: i64) {
    range.start = Some(length);
    range.end = Some(length);
}

fn set_exact_quantifier(quantifier: &mut Quantifier, length: i64) {
    quantifier.start = Some(length);
    quantifier.end = Some(length);
}

fn validate_pattern_alternative_expansion_supported(
    single_query: &SingleQuery,
    path: &str,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    match &single_query.kind {
        SingleQueryKind::SinglePart(single_part) => {
            validate_single_part_pattern_alternative_expansion_supported(single_part, path, context)
        }
        SingleQueryKind::MultiPart(multi_part) => {
            validate_multi_part_pattern_alternative_expansion_supported(multi_part, path, context)
        }
    }
}

fn validate_single_part_pattern_alternative_expansion_supported(
    single_part: &SinglePartQuery,
    path: &str,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    let return_clause = return_clause_from_single_part(single_part, path)?;
    validate_return_allows_pattern_alternative_expansion(return_clause, path, context)
}

fn validate_multi_part_pattern_alternative_expansion_supported(
    multi_part: &MultiPartQuery,
    path: &str,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    for (index, part) in multi_part.parts.iter().enumerate() {
        if !part.updating_clauses.is_empty() {
            return Err(unsupported(
                format!("{path}.parts[{index}].updating_clauses"),
                "write clauses are not supported by Coral virtual graphs",
            ));
        }
        validate_with_allows_pattern_alternative_expansion(
            &part.with,
            &format!("{path}.parts[{index}].with"),
        )?;
    }
    validate_single_part_pattern_alternative_expansion_supported(
        &multi_part.final_part,
        &format!("{path}.final_part"),
        context,
    )
}

fn validate_with_allows_pattern_alternative_expansion(
    with: &With,
    path: &str,
) -> Result<(), CoreError> {
    if with.distinct {
        return Err(unsupported(
            format!("{path}.distinct"),
            "pattern alternatives with WITH DISTINCT require staged query planning and are not supported yet",
        ));
    }
    if with.order.is_some() || with.skip.is_some() || with.limit.is_some() {
        return Err(unsupported(
            path,
            "pattern alternatives with WITH ORDER BY, SKIP, or LIMIT require staged query planning and are not supported yet",
        ));
    }
    for (index, item) in with.items.iter().enumerate() {
        if expression_contains_aggregate(&item.expression) {
            return Err(unsupported(
                format!("{path}.items[{index}].expression"),
                "pattern alternatives with aggregate WITH projections require staged query planning and are not supported yet",
            ));
        }
    }
    Ok(())
}

fn validate_return_allows_pattern_alternative_expansion(
    return_clause: &Return,
    path: &str,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    for (index, item) in return_clause.items.iter().enumerate() {
        if count_star_item_alias(item).is_none()
            && compile_static_alternative_outer_aggregate_item(item, index, path, context)?
                .is_none()
            && expression_contains_aggregate(&item.expression)
        {
            return Err(unsupported(
                format!("{path}.return.items[{index}].expression"),
                "pattern alternatives with property or non-count aggregate RETURN projections require staged query planning and are not supported yet",
            ));
        }
    }
    Ok(())
}

fn expression_contains_aggregate(expression: &Expression) -> bool {
    match expression {
        Expression::CountStar { .. } => true,
        Expression::FunctionCall(function) => {
            is_aggregate_function_call(function)
                || function.arguments.iter().any(expression_contains_aggregate)
        }
        Expression::Literal(literal) => literal_contains_aggregate(literal),
        Expression::PropertyLookup { base, .. }
        | Expression::IsNull { operand: base, .. }
        | Expression::UnaryOp { operand: base, .. }
        | Expression::Parenthesized(base) => expression_contains_aggregate(base),
        Expression::NodeLabels { base, labels, .. } => {
            expression_contains_aggregate(base)
                || labels.iter().any(label_expression_contains_aggregate)
        }
        Expression::BinaryOp { lhs, rhs, .. } | Expression::In { lhs, rhs, .. } => {
            expression_contains_aggregate(lhs) || expression_contains_aggregate(rhs)
        }
        Expression::Comparison { lhs, operators, .. } => {
            expression_contains_aggregate(lhs)
                || operators
                    .iter()
                    .any(|(_, rhs)| expression_contains_aggregate(rhs))
        }
        Expression::ListIndex { list, index, .. } => {
            expression_contains_aggregate(list) || expression_contains_aggregate(index)
        }
        Expression::ListSlice {
            list, start, end, ..
        } => {
            expression_contains_aggregate(list)
                || start.as_deref().is_some_and(expression_contains_aggregate)
                || end.as_deref().is_some_and(expression_contains_aggregate)
        }
        Expression::Case(case) => case_contains_aggregate(case),
        Expression::ListComprehension(comprehension) => {
            list_comprehension_contains_aggregate(comprehension)
        }
        Expression::PatternComprehension(comprehension) => {
            pattern_comprehension_contains_aggregate(comprehension)
        }
        Expression::All(filter)
        | Expression::Any(filter)
        | Expression::None(filter)
        | Expression::Single(filter) => filter_expression_contains_aggregate(filter),
        Expression::Exists(exists) => exists_expression_contains_aggregate(exists),
        Expression::MapProjection(map) => map_projection_contains_aggregate(map),
        Expression::CountSubquery(_)
        | Expression::CollectSubquery(_)
        | Expression::Variable(_)
        | Expression::Parameter(_)
        | Expression::Pattern(_) => false,
    }
}

fn literal_contains_aggregate(literal: &CypherLiteral) -> bool {
    match literal {
        CypherLiteral::List(list) => list.elements.iter().any(expression_contains_aggregate),
        CypherLiteral::Map(map) => map
            .entries
            .iter()
            .any(|(_, value)| expression_contains_aggregate(value)),
        CypherLiteral::Number(_)
        | CypherLiteral::String(_)
        | CypherLiteral::Boolean(_)
        | CypherLiteral::Null => false,
    }
}

fn case_contains_aggregate(case: &CaseExpression) -> bool {
    case.scrutinee
        .as_deref()
        .is_some_and(expression_contains_aggregate)
        || case.alternatives.iter().any(|alternative| {
            expression_contains_aggregate(&alternative.when)
                || expression_contains_aggregate(&alternative.then)
        })
        || case
            .default
            .as_deref()
            .is_some_and(expression_contains_aggregate)
}

fn list_comprehension_contains_aggregate(
    comprehension: &decypher::ast::expr::ListComprehension,
) -> bool {
    comprehension
        .filter
        .as_deref()
        .is_some_and(expression_contains_aggregate)
        || comprehension
            .map
            .as_ref()
            .is_some_and(expression_contains_aggregate)
}

fn pattern_comprehension_contains_aggregate(
    comprehension: &decypher::ast::expr::PatternComprehension,
) -> bool {
    comprehension
        .where_clause
        .as_ref()
        .is_some_and(expression_contains_aggregate)
        || expression_contains_aggregate(&comprehension.map)
}

fn filter_expression_contains_aggregate(filter: &decypher::ast::expr::FilterExpression) -> bool {
    expression_contains_aggregate(&filter.collection)
        || filter
            .predicate
            .as_deref()
            .is_some_and(expression_contains_aggregate)
}

fn exists_expression_contains_aggregate(exists: &decypher::ast::expr::ExistsExpression) -> bool {
    match exists.inner.as_ref() {
        decypher::ast::expr::ExistsInner::Pattern(_, predicate) => predicate
            .as_deref()
            .is_some_and(expression_contains_aggregate),
        decypher::ast::expr::ExistsInner::RegularQuery(_) => true,
    }
}

fn map_projection_contains_aggregate(map: &decypher::ast::expr::MapProjection) -> bool {
    map.items.iter().any(|item| match item {
        decypher::ast::expr::MapProjectionItem::Literal { value, .. } => {
            expression_contains_aggregate(value)
        }
        decypher::ast::expr::MapProjectionItem::AllProperties { .. }
        | decypher::ast::expr::MapProjectionItem::PropertyLookup { .. } => false,
    })
}

fn label_expression_contains_aggregate(expression: &LabelExpression) -> bool {
    match expression {
        LabelExpression::Dynamic {
            expression: dynamic,
            ..
        } => expression_contains_aggregate(dynamic),
        LabelExpression::Or { lhs, rhs, .. } | LabelExpression::And { lhs, rhs, .. } => {
            label_expression_contains_aggregate(lhs) || label_expression_contains_aggregate(rhs)
        }
        LabelExpression::Not { inner, .. } | LabelExpression::Group { inner, .. } => {
            label_expression_contains_aggregate(inner)
        }
        LabelExpression::Static(_) => false,
    }
}

fn compile_single_part(
    query: &SinglePartQuery,
    context: &CypherCompileContext,
) -> Result<GraphPlan, CoreError> {
    let return_clause = return_clause_from_single_part(query, "query")?;

    let mut plan = GraphPlan::default();
    let mut state = compile_state_for_single_part(query, context);
    compile_reading_clauses_into(
        &query.reading_clauses,
        "match",
        &mut plan,
        &mut state,
        context,
    )?;
    compile_return(return_clause, &mut plan, &state, context)?;
    reject_ignored_path_variable_references(&plan, &state, "return")?;
    Ok(plan)
}

fn compile_state_for_single_part(
    query: &SinglePartQuery,
    context: &CypherCompileContext,
) -> CypherCompileState {
    let mut state = CypherCompileState::default();
    collect_relationship_element_path_variables_in_single_part(
        query,
        context,
        &mut state.relationship_element_path_variables,
    );
    state
}

fn compile_state_for_multi_part(
    query: &MultiPartQuery,
    context: &CypherCompileContext,
) -> CypherCompileState {
    let mut state = CypherCompileState::default();
    let mut path_variables_in_scope = BTreeSet::new();
    for part in &query.parts {
        let declared_path_variables =
            declared_path_variables_in_reading_clauses(&part.reading_clauses);
        let available_path_variables = path_variables_in_scope
            .union(&declared_path_variables)
            .cloned()
            .collect::<BTreeSet<_>>();
        collect_relationship_element_path_variables_in_reading_clauses(
            &part.reading_clauses,
            context,
            &mut state.relationship_element_path_variables,
        );
        collect_relationship_element_path_variables_in_with(
            &part.with,
            context,
            &mut state.relationship_element_path_variables,
            &available_path_variables,
        );
        path_variables_in_scope =
            carried_path_variables_after_with(&part.with, &available_path_variables);
    }
    collect_relationship_element_path_variables_in_single_part_with_scope(
        &query.final_part,
        context,
        &mut state.relationship_element_path_variables,
        &path_variables_in_scope,
    );
    state
}

fn collect_relationship_element_path_variables_in_single_part(
    query: &SinglePartQuery,
    context: &CypherCompileContext,
    variables: &mut BTreeSet<String>,
) {
    collect_relationship_element_path_variables_in_single_part_with_scope(
        query,
        context,
        variables,
        &BTreeSet::new(),
    );
}

fn collect_relationship_element_path_variables_in_single_part_with_scope(
    query: &SinglePartQuery,
    context: &CypherCompileContext,
    variables: &mut BTreeSet<String>,
    path_variables_in_scope: &BTreeSet<String>,
) {
    let declared_path_variables =
        declared_path_variables_in_reading_clauses(&query.reading_clauses);
    let available_path_variables = path_variables_in_scope
        .union(&declared_path_variables)
        .cloned()
        .collect::<BTreeSet<_>>();
    collect_relationship_element_path_variables_in_reading_clauses(
        &query.reading_clauses,
        context,
        variables,
    );
    if let SinglePartBody::Return(return_clause) = &query.body {
        collect_relationship_element_path_variables_in_return(
            return_clause,
            context,
            variables,
            &available_path_variables,
        );
    }
}

fn declared_path_variables_in_reading_clauses(clauses: &[ReadingClause]) -> BTreeSet<String> {
    let mut variables = BTreeSet::new();
    for clause in clauses {
        if let ReadingClause::Match(match_clause) = clause {
            for part in &match_clause.pattern.parts {
                if let Some(variable) = part.variable.as_ref() {
                    variables.insert(variable_name(variable));
                }
            }
        }
    }
    variables
}

fn collect_relationship_element_path_variables_in_reading_clauses(
    clauses: &[ReadingClause],
    context: &CypherCompileContext,
    variables: &mut BTreeSet<String>,
) {
    for clause in clauses {
        if let ReadingClause::Match(match_clause) = clause
            && let Some(where_clause) = &match_clause.where_clause
        {
            collect_relationship_element_path_variables_in_expression(
                where_clause,
                context,
                variables,
            );
        }
    }
}

fn collect_relationship_element_path_variables_in_with(
    with: &With,
    context: &CypherCompileContext,
    variables: &mut BTreeSet<String>,
    available_path_variables: &BTreeSet<String>,
) {
    for item in &with.items {
        if let Some(variable) = expression_variable_name(&item.expression)
            && available_path_variables.contains(&variable)
        {
            variables.insert(variable);
        }
        collect_relationship_element_path_variables_in_expression(
            &item.expression,
            context,
            variables,
        );
    }
    if let Some(where_clause) = &with.where_clause {
        collect_relationship_element_path_variables_in_expression(where_clause, context, variables);
    }
    collect_relationship_element_path_variables_in_order_by(
        with.order.as_ref(),
        context,
        variables,
    );
    if let Some(skip) = &with.skip {
        collect_relationship_element_path_variables_in_expression(skip, context, variables);
    }
    if let Some(limit) = &with.limit {
        collect_relationship_element_path_variables_in_expression(limit, context, variables);
    }
}

fn collect_relationship_element_path_variables_in_return(
    return_clause: &Return,
    context: &CypherCompileContext,
    variables: &mut BTreeSet<String>,
    available_path_variables: &BTreeSet<String>,
) {
    for item in &return_clause.items {
        if let Some(variable) = expression_variable_name(&item.expression)
            && available_path_variables.contains(&variable)
        {
            variables.insert(variable);
        }
        collect_relationship_element_path_variables_in_expression(
            &item.expression,
            context,
            variables,
        );
    }
    collect_relationship_element_path_variables_in_order_by(
        return_clause.order.as_ref(),
        context,
        variables,
    );
    if let Some(skip) = &return_clause.skip {
        collect_relationship_element_path_variables_in_expression(skip, context, variables);
    }
    if let Some(limit) = &return_clause.limit {
        collect_relationship_element_path_variables_in_expression(limit, context, variables);
    }
}

fn carried_path_variables_after_with(
    with: &With,
    available_path_variables: &BTreeSet<String>,
) -> BTreeSet<String> {
    if with.star {
        return available_path_variables.clone();
    }
    let mut carried = BTreeSet::new();
    for item in &with.items {
        let Some(input) = expression_variable_name(&item.expression) else {
            continue;
        };
        if !available_path_variables.contains(&input) {
            continue;
        }
        let output = item
            .alias
            .as_ref()
            .map_or_else(|| input.clone(), variable_name);
        carried.insert(output);
    }
    carried
}

fn collect_relationship_element_path_variables_in_order_by(
    order: Option<&Order>,
    context: &CypherCompileContext,
    variables: &mut BTreeSet<String>,
) {
    let Some(order) = order else {
        return;
    };
    for item in &order.items {
        collect_relationship_element_path_variables_in_expression(
            &item.expression,
            context,
            variables,
        );
    }
}

fn collect_relationship_element_path_variables_in_expression(
    expression: &Expression,
    context: &CypherCompileContext,
    variables: &mut BTreeSet<String>,
) {
    match expression {
        Expression::Parenthesized(inner) => {
            collect_relationship_element_path_variables_in_expression(inner, context, variables);
        }
        Expression::UnaryOp { operand, .. } | Expression::IsNull { operand, .. } => {
            collect_relationship_element_path_variables_in_expression(operand, context, variables);
        }
        Expression::BinaryOp { lhs, rhs, .. } | Expression::In { lhs, rhs, .. } => {
            collect_relationship_element_path_variables_in_expression(lhs, context, variables);
            collect_relationship_element_path_variables_in_expression(rhs, context, variables);
        }
        Expression::Comparison { lhs, operators, .. } => {
            collect_relationship_element_path_variables_in_expression(lhs, context, variables);
            for (_, rhs) in operators {
                collect_relationship_element_path_variables_in_expression(rhs, context, variables);
            }
        }
        Expression::ListIndex { list, index, .. } => {
            collect_relationship_element_path_variables_in_expression(list, context, variables);
            collect_relationship_element_path_variables_in_expression(index, context, variables);
        }
        Expression::ListSlice {
            list, start, end, ..
        } => {
            collect_relationship_element_path_variables_in_expression(list, context, variables);
            if let Some(start) = start.as_deref() {
                collect_relationship_element_path_variables_in_expression(
                    start, context, variables,
                );
            }
            if let Some(end) = end.as_deref() {
                collect_relationship_element_path_variables_in_expression(end, context, variables);
            }
        }
        Expression::Case(case) => {
            collect_relationship_element_path_variables_in_case(case, context, variables);
        }
        Expression::FunctionCall(function) => {
            collect_relationship_element_path_variables_in_function(function, context, variables);
        }
        Expression::ListComprehension(comprehension) => {
            if let Some(filter) = comprehension.filter.as_deref() {
                collect_relationship_element_path_variables_in_expression(
                    filter, context, variables,
                );
            }
            if let Some(map) = comprehension.map.as_ref() {
                collect_relationship_element_path_variables_in_expression(map, context, variables);
            }
        }
        Expression::PatternComprehension(comprehension) => {
            if let Some(where_clause) = comprehension.where_clause.as_ref() {
                collect_relationship_element_path_variables_in_expression(
                    where_clause,
                    context,
                    variables,
                );
            }
            collect_relationship_element_path_variables_in_expression(
                &comprehension.map,
                context,
                variables,
            );
        }
        Expression::All(filter)
        | Expression::Any(filter)
        | Expression::None(filter)
        | Expression::Single(filter) => {
            collect_relationship_element_path_variables_in_expression(
                &filter.collection,
                context,
                variables,
            );
            if let Some(predicate) = filter.predicate.as_deref() {
                collect_relationship_element_path_variables_in_expression(
                    predicate, context, variables,
                );
            }
        }
        Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Parameter(_)
        | Expression::CountStar { .. }
        | Expression::PropertyLookup { .. }
        | Expression::NodeLabels { .. }
        | Expression::Pattern(_)
        | Expression::Exists(_)
        | Expression::CountSubquery(_)
        | Expression::CollectSubquery(_)
        | Expression::MapProjection(_) => {}
    }
}

fn collect_relationship_element_path_variables_in_case(
    case: &CaseExpression,
    context: &CypherCompileContext,
    variables: &mut BTreeSet<String>,
) {
    if let Some(scrutinee) = case.scrutinee.as_deref() {
        collect_relationship_element_path_variables_in_expression(scrutinee, context, variables);
    }
    for alternative in &case.alternatives {
        collect_relationship_element_path_variables_in_expression(
            &alternative.when,
            context,
            variables,
        );
        collect_relationship_element_path_variables_in_expression(
            &alternative.then,
            context,
            variables,
        );
    }
    if let Some(default) = case.default.as_deref() {
        collect_relationship_element_path_variables_in_expression(default, context, variables);
    }
}

fn collect_relationship_element_path_variables_in_function(
    function: &FunctionInvocation,
    context: &CypherCompileContext,
    variables: &mut BTreeSet<String>,
) {
    if is_size_function(function) {
        for argument in &function.arguments {
            if expression_relationships_path_variable(argument, context).is_none() {
                collect_relationship_element_path_variables_in_expression(
                    argument, context, variables,
                );
            }
        }
        return;
    }

    if is_relationships_function(function)
        && let Some(variable) = function_relationships_path_variable(function, context)
    {
        variables.insert(variable);
    }
    for argument in &function.arguments {
        collect_relationship_element_path_variables_in_expression(argument, context, variables);
    }
}

fn expression_relationships_path_variable(
    expression: &Expression,
    context: &CypherCompileContext,
) -> Option<String> {
    let Expression::FunctionCall(function) = expression else {
        return None;
    };
    function_relationships_path_variable(function, context)
}

fn function_relationships_path_variable(
    function: &FunctionInvocation,
    context: &CypherCompileContext,
) -> Option<String> {
    if !is_relationships_function(function) {
        return None;
    }
    context
        .variable_function_argument(function)
        .map(str::to_string)
        .or_else(|| match function.arguments.as_slice() {
            [Expression::Variable(variable)] => Some(variable_name(variable)),
            _ => None,
        })
}

fn compile_multi_part(
    query: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<GraphPlan, CoreError> {
    if let Some(plan) = compile_terminal_with_projection(query, context)? {
        return Ok(plan);
    }
    if let Some(plan) = compile_terminal_with_graph_modifiers(query, context)? {
        return Ok(plan);
    }
    compile_transparent_multi_part(query, context)
}

fn compile_transparent_multi_part(
    query: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<GraphPlan, CoreError> {
    let mut plan = GraphPlan::default();
    let mut state = compile_state_for_multi_part(query, context);
    for (index, part) in query.parts.iter().enumerate() {
        compile_transparent_multi_part_part(part, index, &mut plan, &mut state, context)?;
    }

    match query.final_part.reading_clauses.as_slice() {
        [] => {}
        clauses => {
            compile_reading_clauses_into(
                clauses,
                "final_part.match",
                &mut plan,
                &mut state,
                context,
            )?;
        }
    }
    let return_clause = return_clause_from_single_part(&query.final_part, "final_part")?;
    compile_return(return_clause, &mut plan, &state, context)?;
    reject_ignored_path_variable_references(&plan, &state, "final_part.return")?;
    Ok(plan)
}

fn compile_transparent_multi_part_part(
    part: &MultiPartQueryPart,
    index: usize,
    plan: &mut GraphPlan,
    state: &mut CypherCompileState,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    if !part.updating_clauses.is_empty() {
        return Err(unsupported(
            format!("parts[{index}].updating_clauses"),
            "write clauses are not supported by Coral virtual graphs",
        ));
    }
    compile_reading_clauses_into(
        &part.reading_clauses,
        format!("parts[{index}].match"),
        plan,
        state,
        context,
    )?;
    if let Some(predicate) = validate_transparent_with(
        &part.with,
        plan,
        state,
        format!("parts[{index}].with"),
        context,
    )? {
        append_predicate_expression(predicate, plan);
    }
    Ok(())
}

fn validate_transparent_with(
    with: &With,
    plan: &mut GraphPlan,
    state: &mut CypherCompileState,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    if with.distinct {
        return Err(unsupported(
            format!("{path}.distinct"),
            "WITH DISTINCT requires staged query planning and is not supported yet",
        ));
    }
    if with.order.is_some() || with.skip.is_some() || with.limit.is_some() {
        return Err(unsupported(
            path.clone(),
            "WITH ORDER BY, SKIP, and LIMIT require staged query planning and are not supported yet",
        ));
    }
    apply_transparent_with_scope(with, plan, state, path, context)
}

fn apply_transparent_with_scope(
    with: &With,
    plan: &mut GraphPlan,
    state: &mut CypherCompileState,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    if with.star {
        return apply_transparent_with_star_scope(with, plan, state, path, context);
    }
    if with.items.is_empty() {
        return Err(unsupported(
            format!("{path}.items"),
            "WITH must carry every currently bound variable in this transparent subset",
        ));
    }

    let scope = compile_transparent_with_items(with, plan, state, &path, context)?;
    reject_explicit_with_where_path_variable_references(with, state, &scope, &path, context)?;
    let visible = visible_graph_variables(plan, state);
    let dropped_variables = visible
        .difference(&scope.carried_inputs)
        .cloned()
        .collect::<Vec<_>>();
    let mut next_path_variables = carried_transparent_with_path_variables(state, &scope)?;
    let mut hidden_renames = BTreeMap::new();
    let mut renames = scope.renames;
    for variable in &dropped_variables {
        let hidden = fresh_hidden_graph_variable(plan, state, variable);
        renames.insert(variable.clone(), hidden.clone());
        hidden_renames.insert(hidden.clone(), hidden);
    }
    let mut next_scalar_aliases = scope.scalar_aliases;
    if renames.iter().any(|(from, to)| from != to) {
        rename_graph_plan_variables(plan, &renames);
        rename_hidden_graph_variables(state, &renames);
        for projection in &mut next_scalar_aliases {
            rename_projection_variables(projection, &renames);
        }
        for binding in next_path_variables.values_mut() {
            rename_path_binding_variables(binding, &renames);
        }
    }
    state
        .hidden_graph_variables
        .extend(hidden_renames.into_values());
    state.out_of_scope_graph_names.extend(dropped_variables);
    for variable in scope.carried_graph_outputs {
        state.out_of_scope_graph_names.remove(&variable);
    }
    state.scalar_aliases = next_scalar_aliases;
    state.path_variables = next_path_variables;

    let predicate = compile_transparent_with_where(with, plan, Some(state), path.clone(), context)?;
    reject_ignored_path_variable_references(plan, state, &path)?;
    if let Some(predicate) = predicate.as_ref() {
        reject_ignored_path_variable_references_in_predicate(
            predicate,
            state,
            format!("{path}.where"),
        )?;
    }
    Ok(predicate)
}

fn apply_transparent_with_star_scope(
    with: &With,
    plan: &GraphPlan,
    state: &mut CypherCompileState,
    path: String,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    let aliases = compile_transparent_with_star_scalar_aliases(with, plan, state, &path, context)?;
    state.scalar_aliases.extend(aliases);
    compile_transparent_with_where(with, plan, Some(state), path, context)
}

#[derive(Default)]
struct TransparentWithScopePlan {
    carried_inputs: BTreeSet<String>,
    carried_outputs: BTreeSet<String>,
    carried_graph_outputs: BTreeSet<String>,
    carried_path_inputs: BTreeSet<String>,
    scalar_aliases: Vec<Projection>,
    path_renames: BTreeMap<String, String>,
    renames: BTreeMap<String, String>,
}

fn compile_transparent_with_items(
    with: &With,
    plan: &GraphPlan,
    state: &CypherCompileState,
    path: &str,
    context: &CypherCompileContext,
) -> Result<TransparentWithScopePlan, CoreError> {
    let visible = visible_graph_variables(plan, state);
    let mut scope = TransparentWithScopePlan::default();
    for (index, item) in with.items.iter().enumerate() {
        if compile_transparent_with_variable_item(item, index, path, state, &visible, &mut scope)? {
            continue;
        }
        let projection = compile_transparent_with_scalar_alias(
            item,
            format!("{path}.items[{index}]"),
            plan,
            state,
            context,
        )?;
        push_transparent_with_scalar_alias(&mut scope, projection, path, index)?;
    }
    Ok(scope)
}

fn compile_transparent_with_star_scalar_aliases(
    with: &With,
    plan: &GraphPlan,
    state: &CypherCompileState,
    path: &str,
    context: &CypherCompileContext,
) -> Result<Vec<Projection>, CoreError> {
    let visible = visible_graph_variables(plan, state);
    let mut outputs = transparent_with_star_output_names(plan, state);
    let mut aliases = Vec::new();

    for (index, item) in with.items.iter().enumerate() {
        if let Some(variable) = expression_variable_name(&item.expression) {
            if visible.contains(&variable) {
                return Err(unsupported(
                    format!("{path}.items[{index}].expression"),
                    "WITH * plus explicit graph-variable aliases requires graph-value aliasing and is not supported yet",
                ));
            }
            if state.path_variables.contains_key(&variable) {
                return Err(unsupported(
                    format!("{path}.items[{index}].expression"),
                    "WITH * plus explicit path-variable aliases requires path-value materialization across query parts and is not supported yet",
                ));
            }
        }
        let projection = compile_transparent_with_star_scalar_alias(
            item,
            format!("{path}.items[{index}]"),
            plan,
            state,
            context,
        )?;
        let output = projection.output_name();
        if !outputs.insert(output.clone()) {
            return Err(unsupported(
                format!("{path}.items[{index}].alias"),
                format!("WITH * output variable '{output}' is already in scope"),
            ));
        }
        aliases.push(projection);
    }
    Ok(aliases)
}

fn transparent_with_star_output_names(
    plan: &GraphPlan,
    state: &CypherCompileState,
) -> BTreeSet<String> {
    let mut outputs = visible_graph_variables(plan, state);
    outputs.extend(state.path_variables.keys().cloned());
    outputs.extend(state.scalar_aliases.iter().map(Projection::output_name));
    outputs
}

fn compile_transparent_with_star_scalar_alias(
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    if let Some(input) = expression_variable_name(&item.expression)
        && let Some(projection) = scalar_alias_projection(state, &input)
    {
        let Some(alias) = item.alias.as_ref().map(validate_variable).transpose()? else {
            return Err(unsupported(
                format!("{path}.alias"),
                "WITH * explicit scalar alias copies require a new alias",
            ));
        };
        let mut projection = projection.clone();
        set_projection_output_alias(&mut projection, alias);
        return Ok(projection);
    }

    compile_transparent_with_scalar_alias(item, path, plan, state, context)
}

fn reject_explicit_with_where_path_variable_references(
    with: &With,
    state: &CypherCompileState,
    scope: &TransparentWithScopePlan,
    path: &str,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    let Some(where_clause) = &with.where_clause else {
        return Ok(());
    };
    if state.path_variables.is_empty() {
        return Ok(());
    }

    let mut variables = BTreeSet::new();
    expression_variables(where_clause, &mut variables);
    recovered_function_argument_variables(where_clause, context, &mut variables);
    if let Some(variable) = variables.iter().find(|variable| {
        state.path_variables.contains_key(variable.as_str())
            && !scope.carried_outputs.contains(variable.as_str())
    }) {
        return Err(unsupported(
            format!("{path}.where"),
            format!(
                "path variable '{variable}' is not in scope after WITH because Coral does not materialize path values yet"
            ),
        ));
    }
    Ok(())
}

fn recovered_function_argument_variables(
    expression: &Expression,
    context: &CypherCompileContext,
    variables: &mut BTreeSet<String>,
) {
    match expression {
        Expression::Parenthesized(inner) => {
            recovered_function_argument_variables(inner, context, variables);
        }
        Expression::UnaryOp { operand, .. } | Expression::IsNull { operand, .. } => {
            recovered_function_argument_variables(operand, context, variables);
        }
        Expression::BinaryOp { lhs, rhs, .. } | Expression::In { lhs, rhs, .. } => {
            recovered_function_argument_variables(lhs, context, variables);
            recovered_function_argument_variables(rhs, context, variables);
        }
        Expression::Comparison { lhs, operators, .. } => {
            recovered_function_argument_variables(lhs, context, variables);
            for (_, rhs) in operators {
                recovered_function_argument_variables(rhs, context, variables);
            }
        }
        Expression::ListIndex { list, index, .. } => {
            recovered_function_argument_variables(list, context, variables);
            recovered_function_argument_variables(index, context, variables);
        }
        Expression::ListSlice {
            list, start, end, ..
        } => {
            recovered_function_argument_variables(list, context, variables);
            if let Some(start) = start.as_deref() {
                recovered_function_argument_variables(start, context, variables);
            }
            if let Some(end) = end.as_deref() {
                recovered_function_argument_variables(end, context, variables);
            }
        }
        Expression::Case(case) => {
            if let Some(scrutinee) = case.scrutinee.as_deref() {
                recovered_function_argument_variables(scrutinee, context, variables);
            }
            for alternative in &case.alternatives {
                recovered_function_argument_variables(&alternative.when, context, variables);
                recovered_function_argument_variables(&alternative.then, context, variables);
            }
            if let Some(default) = case.default.as_deref() {
                recovered_function_argument_variables(default, context, variables);
            }
        }
        Expression::FunctionCall(function) => {
            if let Some(variable) = context.variable_function_argument(function) {
                variables.insert(variable.to_string());
            }
            for argument in &function.arguments {
                recovered_function_argument_variables(argument, context, variables);
            }
        }
        Expression::ListComprehension(comprehension) => {
            if let Some(filter) = comprehension.filter.as_deref() {
                recovered_function_argument_variables(filter, context, variables);
            }
            if let Some(map) = comprehension.map.as_ref() {
                recovered_function_argument_variables(map, context, variables);
            }
        }
        Expression::PatternComprehension(comprehension) => {
            if let Some(where_clause) = comprehension.where_clause.as_ref() {
                recovered_function_argument_variables(where_clause, context, variables);
            }
            recovered_function_argument_variables(&comprehension.map, context, variables);
        }
        Expression::All(filter)
        | Expression::Any(filter)
        | Expression::None(filter)
        | Expression::Single(filter) => {
            recovered_function_argument_variables(&filter.collection, context, variables);
            if let Some(predicate) = filter.predicate.as_deref() {
                recovered_function_argument_variables(predicate, context, variables);
            }
        }
        Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Parameter(_)
        | Expression::CountStar { .. }
        | Expression::PropertyLookup { .. }
        | Expression::NodeLabels { .. }
        | Expression::Pattern(_)
        | Expression::Exists(_)
        | Expression::CountSubquery(_)
        | Expression::CollectSubquery(_)
        | Expression::MapProjection(_) => {}
    }
}

fn compile_transparent_with_variable_item(
    item: &ProjectionItem,
    index: usize,
    path: &str,
    state: &CypherCompileState,
    visible: &BTreeSet<String>,
    scope: &mut TransparentWithScopePlan,
) -> Result<bool, CoreError> {
    let Expression::Variable(variable) = &item.expression else {
        return Ok(false);
    };
    let input = variable_name(variable);
    if visible.contains(&input) {
        push_transparent_with_graph_variable(item, index, path, input, scope)?;
        return Ok(true);
    }
    if state.path_variables.contains_key(&input) {
        push_transparent_with_path_variable(item, index, path, input, scope)?;
        return Ok(true);
    }
    if let Some(projection) = scalar_alias_projection(state, &input) {
        let output = item
            .alias
            .as_ref()
            .map(validate_variable)
            .transpose()?
            .unwrap_or_else(|| input.clone());
        push_transparent_with_output_name(scope, &output, path, index)?;
        let mut projection = projection.clone();
        set_projection_output_alias(&mut projection, output);
        scope.scalar_aliases.push(projection);
        return Ok(true);
    }
    Err(unsupported(
        format!("{path}.items[{index}].expression"),
        format!(
            "WITH can only carry visible graph variables or scalar aliases; '{input}' is not in scope"
        ),
    ))
}

fn push_transparent_with_graph_variable(
    item: &ProjectionItem,
    index: usize,
    path: &str,
    input: String,
    scope: &mut TransparentWithScopePlan,
) -> Result<(), CoreError> {
    let output = item
        .alias
        .as_ref()
        .map(validate_variable)
        .transpose()?
        .unwrap_or_else(|| input.clone());
    if !scope.carried_inputs.insert(input.clone()) {
        return Err(unsupported(
            format!("{path}.items[{index}].expression"),
            format!("WITH carries graph variable '{input}' more than once"),
        ));
    }
    push_transparent_with_output_name(scope, &output, path, index)?;
    scope.carried_graph_outputs.insert(output.clone());
    scope.renames.insert(input, output);
    Ok(())
}

fn push_transparent_with_path_variable(
    item: &ProjectionItem,
    index: usize,
    path: &str,
    input: String,
    scope: &mut TransparentWithScopePlan,
) -> Result<(), CoreError> {
    let output = item
        .alias
        .as_ref()
        .map(validate_variable)
        .transpose()?
        .unwrap_or_else(|| input.clone());
    if !scope.carried_path_inputs.insert(input.clone()) {
        return Err(unsupported(
            format!("{path}.items[{index}].expression"),
            format!("WITH carries path variable '{input}' more than once"),
        ));
    }
    push_transparent_with_output_name(scope, &output, path, index)?;
    scope.path_renames.insert(input, output);
    Ok(())
}

fn carried_transparent_with_path_variables(
    state: &CypherCompileState,
    scope: &TransparentWithScopePlan,
) -> Result<BTreeMap<String, PathBinding>, CoreError> {
    let mut path_variables = BTreeMap::new();
    for (input, output) in &scope.path_renames {
        let binding = state.path_variables.get(input).ok_or_else(|| {
            CoreError::internal("transparent WITH path variable was not in source scope")
        })?;
        path_variables.insert(output.clone(), binding.clone());
    }
    Ok(path_variables)
}

fn push_transparent_with_scalar_alias(
    scope: &mut TransparentWithScopePlan,
    projection: Projection,
    path: &str,
    index: usize,
) -> Result<(), CoreError> {
    let output = projection.output_name();
    push_transparent_with_output_name(scope, &output, path, index)?;
    scope.scalar_aliases.push(projection);
    Ok(())
}

fn push_transparent_with_output_name(
    scope: &mut TransparentWithScopePlan,
    output: &str,
    path: &str,
    index: usize,
) -> Result<(), CoreError> {
    if !scope.carried_outputs.insert(output.to_string()) {
        return Err(unsupported(
            format!("{path}.items[{index}].alias"),
            format!("WITH output variable '{output}' is defined more than once"),
        ));
    }
    Ok(())
}

fn compile_transparent_with_scalar_alias(
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let Some(alias) = item.alias.as_ref().map(validate_variable).transpose()? else {
        return Err(unsupported(
            format!("{path}.alias"),
            "non-terminal WITH scalar aliases require explicit aliases",
        ));
    };
    if expression_contains_aggregate(&item.expression) {
        return Err(unsupported(
            format!("{path}.expression"),
            "aggregate WITH aliases require staged query planning and are not supported before another MATCH",
        ));
    }
    if expression_contains_subquery(&item.expression) {
        return Err(unsupported(
            format!("{path}.expression"),
            "subquery WITH aliases require staged query planning and are not supported before another MATCH",
        ));
    }
    let mut projection = compile_projection(item, path.clone(), context, plan, state)?;
    set_projection_output_alias(&mut projection, alias);
    if projection.is_aggregate() {
        return Err(unsupported(
            format!("{path}.expression"),
            "aggregate WITH aliases require staged query planning and are not supported before another MATCH",
        ));
    }
    if projection_contains_correlated_subquery(&projection) {
        return Err(unsupported(
            format!("{path}.expression"),
            "subquery WITH aliases require staged query planning and are not supported before another MATCH",
        ));
    }
    reject_ignored_path_variable_references_in_projection(&projection, state, path)?;
    Ok(projection)
}

fn compile_transparent_with_where(
    with: &With,
    plan: &GraphPlan,
    path_state: Option<&CypherCompileState>,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    with.where_clause
        .as_ref()
        .map(|where_clause| {
            compile_predicate_expression_with_path_state(
                where_clause,
                format!("{path}.where"),
                plan,
                path_state,
                context,
            )
        })
        .transpose()
}

fn bound_graph_variables(plan: &GraphPlan) -> BTreeSet<String> {
    plan.nodes
        .iter()
        .map(|node| node.variable.clone())
        .chain(
            plan.relationships
                .iter()
                .filter_map(|relationship| relationship.variable.clone()),
        )
        .collect()
}

fn visible_graph_variables(plan: &GraphPlan, state: &CypherCompileState) -> BTreeSet<String> {
    bound_graph_variables(plan)
        .difference(&state.hidden_graph_variables)
        .cloned()
        .collect()
}

fn unary_scalar_expression_operand_mut(
    expression: &mut ScalarExpression,
) -> Option<&mut ScalarExpression> {
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
        | ScalarExpression::Temporal(
            TemporalExpr::DateFromString { text: expression }
            | TemporalExpr::LocalDateTimeFromString { text: expression }
            | TemporalExpr::ZonedDateTimeFromString {
                text: expression, ..
            }
            | TemporalExpr::LocalTimeFromString { text: expression }
            | TemporalExpr::Component { expression, .. }
            | TemporalExpr::ZonedDateTimeAccessor { expression, .. },
        )
        | ScalarExpression::Negate { expression } => Some(expression),
        _ => None,
    }
}

type NamedScalarOperand<'a> = (&'static str, &'a ScalarExpression);

fn path_variable_scalar_pair_operands(
    expression: &ScalarExpression,
) -> Option<(NamedScalarOperand<'_>, NamedScalarOperand<'_>)> {
    match expression {
        ScalarExpression::NullIf { expression, value } => {
            Some((("expression", expression), ("value", value)))
        }
        ScalarExpression::Left { expression, count }
        | ScalarExpression::Right { expression, count } => {
            Some((("expression", expression), ("count", count)))
        }
        ScalarExpression::StringIndices {
            expression,
            pattern: operand,
        }
        | ScalarExpression::StringContains {
            expression,
            pattern: operand,
        }
        | ScalarExpression::StringStartsWith {
            expression,
            pattern: operand,
        }
        | ScalarExpression::StringEndsWith {
            expression,
            pattern: operand,
        } => Some((("expression", expression), ("pattern", operand))),
        ScalarExpression::Arithmetic { left, right, .. } => {
            Some((("left", left), ("right", right)))
        }
        ScalarExpression::Atan2 { y, x } => Some((("y", y), ("x", x))),
        _ => None,
    }
}

fn path_variable_scalar_triple_operands(
    expression: &ScalarExpression,
) -> Option<(
    NamedScalarOperand<'_>,
    NamedScalarOperand<'_>,
    NamedScalarOperand<'_>,
)> {
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
        } => Some((
            ("expression", expression),
            ("length", length),
            ("fill", fill),
        )),
        ScalarExpression::Replace {
            expression,
            search,
            replacement,
        } => Some((
            ("expression", expression),
            ("search", search),
            ("replacement", replacement),
        )),
        ScalarExpression::Temporal(TemporalExpr::MakeDate { year, month, day }) => {
            Some((("year", year), ("month", month), ("day", day)))
        }
        _ => None,
    }
}

fn unary_scalar_expression_operand(expression: &ScalarExpression) -> Option<&ScalarExpression> {
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
        | ScalarExpression::Temporal(
            TemporalExpr::DateFromString { text: expression }
            | TemporalExpr::LocalDateTimeFromString { text: expression }
            | TemporalExpr::ZonedDateTimeFromString {
                text: expression, ..
            }
            | TemporalExpr::LocalTimeFromString { text: expression }
            | TemporalExpr::Component { expression, .. }
            | TemporalExpr::ZonedDateTimeAccessor { expression, .. },
        )
        | ScalarExpression::Negate { expression } => Some(expression),
        _ => None,
    }
}

fn mark_graph_variable_in_scope(state: &mut CypherCompileState, variable: &str) {
    state.out_of_scope_graph_names.remove(variable);
}

fn compile_key_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    if let Some(value) = compile_optional_same_label_undirected_endpoint_function_argument(
        function,
        format!("{path}.arguments"),
        plan,
        context,
    )? {
        return Ok(same_label_undirected_endpoint_key_scalar_expression(value));
    }
    let value = compile_id_graph_value_ref(function, path, plan, context)?;
    Ok(graph_value_key_scalar_expression(value))
}

fn compile_element_id_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    if let Some(value) = compile_optional_same_label_undirected_endpoint_function_argument(
        function,
        format!("{path}.arguments"),
        plan,
        context,
    )? {
        return Ok(same_label_undirected_endpoint_element_id_scalar_expression(
            value,
        ));
    }
    let value = compile_element_id_graph_value_ref(function, path, plan, context)?;
    Ok(graph_value_element_id_scalar_expression(value))
}

fn compile_id_graph_value_ref(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<GraphValueRef, CoreError> {
    let path = path.into();
    let value = compile_single_graph_value_function_argument_ref(
        function,
        format!("{path}.arguments"),
        "id() supports exactly one graph variable argument",
        plan,
        context,
    )?;
    if !plan_uses_variable(plan, &value.variable) {
        return Err(unsupported(
            format!("{path}.arguments[0]"),
            format!(
                "id() argument '{}' is not a bound graph variable",
                value.variable
            ),
        ));
    }
    Ok(value)
}

fn compile_element_id_graph_value_ref(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<GraphValueRef, CoreError> {
    let path = path.into();
    let value = compile_single_graph_value_function_argument_ref(
        function,
        format!("{path}.arguments"),
        "elementId() supports exactly one graph variable argument",
        plan,
        context,
    )?;
    if !plan_uses_variable(plan, &value.variable) {
        return Err(unsupported(
            format!("{path}.arguments[0]"),
            format!(
                "elementId() argument '{}' is not a bound graph variable",
                value.variable
            ),
        ));
    }
    Ok(value)
}

fn metadata_list_value_scalar_expression(
    value: MetadataListValue,
    _plan: &GraphPlan,
) -> ScalarExpression {
    presence_gate_scalar_expression(
        value.presence_variable,
        ScalarExpression::TypedLiteralList {
            literals: value.literals,
            element_type: LiteralListElementType::String,
        },
    )
}

fn graph_value_metadata_presence_variable(
    value: &GraphValueRef,
    plan: &GraphPlan,
) -> Result<Option<String>, CoreError> {
    match value.presence_variable.clone() {
        Some(variable) => Ok(Some(variable)),
        None => optional_graph_variable_presence_variable(plan, &value.variable),
    }
}

fn compile_optional_static_list_value(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<StaticListValue>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_value(inner, path, plan, context)
        }
        Expression::ListSlice {
            list, start, end, ..
        } => compile_optional_static_list_slice_value(
            list,
            start.as_deref(),
            end.as_deref(),
            path,
            plan,
            context,
        ),
        Expression::ListComprehension(comprehension) => {
            compile_optional_static_list_comprehension_value(comprehension, path, plan, context)
        }
        Expression::BinaryOp {
            op: CypherBinaryOperator::Add,
            lhs,
            rhs,
            ..
        } => compile_optional_static_list_concat_value(lhs, rhs, path, plan, context),
        Expression::FunctionCall(function) if is_internal_static_range_function(function) => {
            compile_static_range_list_value(function, path, context).map(Some)
        }
        Expression::FunctionCall(function) if is_split_function(function) => {
            compile_static_split_list_value(function, path, context).map(Some)
        }
        Expression::FunctionCall(function)
            if is_filter_function(function) || is_extract_function(function) =>
        {
            compile_static_legacy_list_function_value(function, path, plan, context).map(Some)
        }
        expression @ Expression::FunctionCall(function) if is_keys_function(function) => {
            if let Some(value) = compile_optional_static_map_keys_list_value(function) {
                return Ok(Some(value));
            }
            if let Some(plan) = plan
                && let Some(value) =
                    compile_optional_metadata_list_value(expression, path.clone(), plan, context)?
            {
                return Ok(Some(StaticListValue {
                    presence_variable: value.presence_variable.clone(),
                    literals: value.literals,
                    element_type: Some(LiteralListElementType::String),
                }));
            }
            Ok(None)
        }
        Expression::FunctionCall(function) if is_static_list_cast_function(function) => {
            compile_static_list_cast_value(function, path, plan, context).map(Some)
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            compile_optional_static_list_coalesce_value(function, path, plan, context)
        }
        Expression::FunctionCall(function) if is_reverse_function(function) => {
            compile_optional_static_list_reverse_value(function, path, plan, context)
        }
        Expression::FunctionCall(function) if is_tail_function(function) => {
            compile_optional_static_list_tail_value(function, path, plan, context)
        }
        expression => {
            if let Some(plan) = plan
                && let Some(value) =
                    compile_optional_metadata_list_value(expression, path.clone(), plan, context)?
            {
                return Ok(Some(StaticListValue {
                    presence_variable: value.presence_variable.clone(),
                    literals: value.literals,
                    element_type: Some(LiteralListElementType::String),
                }));
            }
            match expression {
                Expression::Literal(CypherLiteral::List(_)) | Expression::ListSlice { .. } => {
                    let literals = compile_literal_list(expression, path, context)?;
                    Ok(Some(StaticListValue {
                        presence_variable: None,
                        element_type: infer_literal_list_element_type(&literals),
                        literals,
                    }))
                }
                Expression::Parameter(parameter) => {
                    match context.parameter_value(parameter, path)? {
                        CypherParameterValue::List(literals) => Ok(Some(StaticListValue {
                            presence_variable: None,
                            element_type: infer_literal_list_element_type(literals),
                            literals: literals.clone(),
                        })),
                        CypherParameterValue::Literal(_) => Ok(None),
                    }
                }
                _ => Ok(None),
            }
        }
    }
}

fn compile_optional_static_list_comprehension_value(
    comprehension: &ListComprehension,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<StaticListValue>, CoreError> {
    let path = path.into();
    let source = context
        .list_comprehension_source(comprehension)
        .ok_or_else(|| {
            unsupported(
                path.clone(),
                "list comprehensions require a recoverable `variable IN collection` source",
            )
        })?;
    let variable = variable_name(&comprehension.variable);
    if source.variable != variable {
        return Err(unsupported(
            path,
            "list comprehension variable recovery did not match the parsed AST",
        ));
    }
    let map = if source.has_map {
        Some(comprehension.map.as_ref().ok_or_else(|| {
            unsupported(
                format!("{path}.map"),
                "mapped static list comprehensions require a recoverable map expression",
            )
        })?)
    } else {
        None
    };
    let Some(collection) = compile_static_list_value_source(
        &source.collection_source,
        format!("{path}.collection"),
        plan,
        context,
    )?
    else {
        return Err(unsupported(
            format!("{path}.collection"),
            "static list comprehensions require a literal list, list parameter, static split(...), range(...), tail(...), or static labels()/keys() metadata list",
        ));
    };
    let recovered_filter =
        recover_static_list_comprehension_filter(comprehension, source, &path, context)?;
    let filter = comprehension
        .filter
        .as_deref()
        .or_else(|| recovered_filter.as_ref().map(|(filter, _)| filter));
    let filter_context = recovered_filter
        .as_ref()
        .map_or(context, |(_, filter_context)| filter_context);

    let evaluation = StaticListComprehensionEvaluation {
        variable: &variable,
        filter,
        filter_context,
        map,
        map_context: context,
        mode: PredicateCompileMode::CaseWhen { plan },
    };
    evaluate_static_list_comprehension_value(collection, path, evaluation).map(Some)
}

fn compile_static_legacy_list_function_value(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<StaticListValue, CoreError> {
    let path = path.into();
    let function_name = qualified_function_name(function);
    let source = context
        .static_list_function_source(function)
        .ok_or_else(|| {
            unsupported(
                format!("{path}.arguments"),
                match function_name.to_ascii_lowercase().as_str() {
                    "filter" => "filter() requires `item IN collection WHERE predicate`",
                    "extract" => "extract() requires `item IN collection | expression`",
                    _ => "legacy static list function source could not be recovered",
                },
            )
        })?;
    let expected_kind = if is_filter_function(function) {
        StaticListFunctionKind::Filter
    } else if is_extract_function(function) {
        StaticListFunctionKind::Extract
    } else {
        return Err(CoreError::internal(format!(
            "unexpected legacy static list function '{function_name}'"
        )));
    };
    if source.kind != expected_kind {
        return Err(CoreError::internal(format!(
            "recovered legacy static list function kind did not match '{function_name}'"
        )));
    }
    let Some(collection) = compile_static_list_value_source(
        &source.collection_source,
        format!("{path}.collection"),
        plan,
        context,
    )?
    else {
        return Err(unsupported(
            format!("{path}.collection"),
            format!(
                "{function_name}() requires a literal list, list parameter, static split(...), range(...), tail(...), extract(...), filter(...), or static labels()/keys() metadata list",
            ),
        ));
    };

    let filter_fragment = source
        .filter_source
        .as_deref()
        .map(|filter_source| {
            parse_cypher_expression_fragment(filter_source, format!("{path}.filter"), context)
        })
        .transpose()?;
    let map_fragment = source
        .map_source
        .as_deref()
        .map(|map_source| {
            parse_cypher_expression_fragment(map_source, format!("{path}.map"), context)
        })
        .transpose()?;
    let filter = filter_fragment.as_ref().map(|(filter, _)| filter);
    let filter_context = filter_fragment
        .as_ref()
        .map_or(context, |(_, filter_context)| filter_context);
    let map = map_fragment.as_ref().map(|(map, _)| map);
    let map_context = map_fragment
        .as_ref()
        .map_or(context, |(_, map_context)| map_context);

    let evaluation = StaticListComprehensionEvaluation {
        variable: &source.variable,
        filter,
        filter_context,
        map,
        map_context,
        mode: PredicateCompileMode::CaseWhen { plan },
    };
    evaluate_static_list_comprehension_value(collection, path, evaluation)
}

fn compile_optional_static_list_comprehension_scalar_expression(
    comprehension: &ListComprehension,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let source = context
        .list_comprehension_source(comprehension)
        .ok_or_else(|| {
            unsupported(
                path.clone(),
                "list comprehensions require a recoverable `variable IN collection` source",
            )
        })?;
    let variable = variable_name(&comprehension.variable);
    if source.variable != variable {
        return Err(unsupported(
            path,
            "list comprehension variable recovery did not match the parsed AST",
        ));
    }
    let map = if source.has_map {
        Some(comprehension.map.as_ref().ok_or_else(|| {
            unsupported(
                format!("{path}.map"),
                "mapped static list comprehensions require a recoverable map expression",
            )
        })?)
    } else {
        None
    };
    let recovered_filter =
        recover_static_list_comprehension_filter(comprehension, source, &path, context)?;
    let filter = comprehension
        .filter
        .as_deref()
        .or_else(|| recovered_filter.as_ref().map(|(filter, _)| filter));
    let filter_context = recovered_filter
        .as_ref()
        .map_or(context, |(_, filter_context)| filter_context);
    let (collection_expression, collection_context) = parse_cypher_expression_fragment(
        &source.collection_source,
        format!("{path}.collection"),
        context,
    )?;
    let evaluation = StaticListComprehensionEvaluation {
        variable: &variable,
        filter,
        filter_context,
        map,
        map_context: context,
        mode: PredicateCompileMode::CaseWhen { plan },
    };
    compile_optional_static_list_comprehension_source_scalar_expression(
        &collection_expression,
        path,
        evaluation,
        &collection_context,
    )
}

fn compile_optional_static_list_comprehension_source_scalar_expression(
    collection: &Expression,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
    collection_context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match collection {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_comprehension_source_scalar_expression(
                inner,
                path,
                evaluation,
                collection_context,
            )
        }
        Expression::Case(case) => {
            compile_optional_static_list_case_comprehension_scalar_expression(
                case,
                path,
                evaluation,
                collection_context,
            )
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            compile_optional_static_list_coalesce_comprehension_scalar_expression(
                function,
                path,
                evaluation,
                collection_context,
            )
        }
        Expression::ListSlice {
            list, start, end, ..
        } => compile_optional_static_list_slice_comprehension_scalar_expression(
            list,
            start.as_deref(),
            end.as_deref(),
            path,
            evaluation,
            collection_context,
        ),
        _ => Ok(None),
    }
}

fn compile_optional_static_list_case_comprehension_scalar_expression(
    case: &CaseExpression,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
    collection_context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some(parts) = compile_optional_static_list_case_parts(
        case,
        format!("{path}.collection"),
        evaluation.mode,
        collection_context,
    )?
    else {
        return Ok(None);
    };
    let mut element_type = None;
    let alternatives = parts
        .alternatives
        .into_iter()
        .enumerate()
        .map(|(index, (when, result))| {
            let result = static_list_case_result_comprehension_result(
                result,
                format!("{path}.collection.alternatives[{index}].then"),
                evaluation,
            )?;
            element_type = merge_static_list_case_result_element_type(
                element_type,
                &result,
                &format!("{path}.collection.alternatives[{index}].then"),
            )?;
            Ok((when, result))
        })
        .collect::<Result<Vec<_>, CoreError>>()?;
    let default = parts
        .default
        .map(|result| {
            let result = static_list_case_result_comprehension_result(
                result,
                format!("{path}.collection.default"),
                evaluation,
            )?;
            element_type = merge_static_list_case_result_element_type(
                element_type,
                &result,
                &format!("{path}.collection.default"),
            )?;
            Ok::<StaticListCaseResult, CoreError>(result)
        })
        .transpose()?;
    let parts = StaticListCaseParts {
        alternatives,
        default,
        element_type,
    };
    let element_type = require_static_list_case_element_type(&parts, path)?;
    Ok(Some(ScalarExpression::Case {
        alternatives: parts
            .alternatives
            .into_iter()
            .map(|(when, result)| ScalarCaseAlternative {
                when,
                then: static_list_case_result_scalar_expression(result, element_type),
            })
            .collect(),
        else_expression: parts.default.map(|result| {
            Box::new(static_list_case_result_scalar_expression(
                result,
                element_type,
            ))
        }),
    }))
}

fn compile_optional_static_list_coalesce_comprehension_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
    collection_context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
        function,
        format!("{path}.collection"),
        evaluation.mode.static_metadata_plan(),
        collection_context,
    )?
    else {
        return Ok(None);
    };
    let coalesce =
        static_list_coalesce_comprehension_arguments(coalesce, path.clone(), evaluation)?;
    let element_type = require_static_list_coalesce_element_type(&coalesce, path)?;
    Ok(Some(static_list_case_result_scalar_expression(
        StaticListCaseResult::Coalesce(coalesce),
        element_type,
    )))
}

fn compile_optional_static_list_slice_comprehension_scalar_expression(
    list: &Expression,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
    collection_context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let bounds = StaticListSliceBounds { start, end };
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_slice_comprehension_scalar_expression(
                inner,
                start,
                end,
                path,
                evaluation,
                collection_context,
            )
        }
        Expression::Case(case) => {
            compile_optional_static_list_case_slice_comprehension_scalar_expression(
                case,
                bounds,
                path,
                evaluation,
                collection_context,
            )
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            compile_optional_static_list_coalesce_slice_comprehension_scalar_expression(
                function,
                bounds,
                path,
                evaluation,
                collection_context,
            )
        }
        _ => Ok(None),
    }
}

fn compile_optional_static_list_case_slice_comprehension_scalar_expression(
    case: &CaseExpression,
    bounds: StaticListSliceBounds<'_>,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
    collection_context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some(parts) = compile_optional_static_list_case_parts(
        case,
        format!("{path}.collection.list"),
        evaluation.mode,
        collection_context,
    )?
    else {
        return Ok(None);
    };
    let mut element_type = None;
    let alternatives = parts
        .alternatives
        .into_iter()
        .enumerate()
        .map(|(index, (when, result))| {
            let result = static_list_case_result_slice_comprehension_result(
                result,
                bounds,
                format!("{path}.collection.alternatives[{index}].then"),
                evaluation,
                collection_context,
            )?;
            element_type = merge_static_list_case_result_element_type(
                element_type,
                &result,
                &format!("{path}.collection.alternatives[{index}].then"),
            )?;
            Ok((when, result))
        })
        .collect::<Result<Vec<_>, CoreError>>()?;
    let default = compile_optional_static_list_case_slice_comprehension_default(
        parts.default,
        bounds,
        &path,
        evaluation,
        collection_context,
        &mut element_type,
    )?;
    let parts = StaticListCaseParts {
        alternatives,
        default,
        element_type,
    };
    let element_type = require_static_list_case_element_type(&parts, path)?;
    Ok(Some(ScalarExpression::Case {
        alternatives: parts
            .alternatives
            .into_iter()
            .map(|(when, result)| ScalarCaseAlternative {
                when,
                then: static_list_case_result_scalar_expression(result, element_type),
            })
            .collect(),
        else_expression: parts.default.map(|result| {
            Box::new(static_list_case_result_scalar_expression(
                result,
                element_type,
            ))
        }),
    }))
}

fn compile_optional_static_list_case_slice_comprehension_default(
    default: Option<StaticListCaseResult>,
    bounds: StaticListSliceBounds<'_>,
    path: &str,
    evaluation: StaticListComprehensionEvaluation<'_>,
    collection_context: &CypherCompileContext,
    element_type: &mut Option<LiteralListElementType>,
) -> Result<Option<StaticListCaseResult>, CoreError> {
    default
        .map(|result| {
            let result = static_list_case_result_slice_comprehension_result(
                result,
                bounds,
                format!("{path}.collection.default"),
                evaluation,
                collection_context,
            )?;
            *element_type = merge_static_list_case_result_element_type(
                *element_type,
                &result,
                &format!("{path}.collection.default"),
            )?;
            Ok::<StaticListCaseResult, CoreError>(result)
        })
        .transpose()
}

fn compile_optional_static_list_coalesce_slice_comprehension_scalar_expression(
    function: &FunctionInvocation,
    bounds: StaticListSliceBounds<'_>,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
    collection_context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
        function,
        format!("{path}.collection.list"),
        evaluation.mode.static_metadata_plan(),
        collection_context,
    )?
    else {
        return Ok(None);
    };
    let coalesce = static_list_coalesce_slice_comprehension_arguments(
        coalesce,
        bounds,
        path.clone(),
        evaluation,
        collection_context,
    )?;
    let element_type = require_static_list_coalesce_element_type(&coalesce, path)?;
    Ok(Some(static_list_case_result_scalar_expression(
        StaticListCaseResult::Coalesce(coalesce),
        element_type,
    )))
}

fn static_list_case_result_comprehension_result(
    result: StaticListCaseResult,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
) -> Result<StaticListCaseResult, CoreError> {
    let path = path.into();
    match result {
        StaticListCaseResult::Null => Ok(StaticListCaseResult::Null),
        StaticListCaseResult::List(value) => Ok(StaticListCaseResult::List(
            evaluate_static_list_comprehension_value(value, path, evaluation)?,
        )),
        StaticListCaseResult::Coalesce(coalesce) => Ok(StaticListCaseResult::Coalesce(
            static_list_coalesce_comprehension_arguments(coalesce, path, evaluation)?,
        )),
    }
}

fn static_list_case_result_slice_comprehension_result(
    result: StaticListCaseResult,
    bounds: StaticListSliceBounds<'_>,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
    context: &CypherCompileContext,
) -> Result<StaticListCaseResult, CoreError> {
    let path = path.into();
    match result {
        StaticListCaseResult::Null => Ok(StaticListCaseResult::Null),
        StaticListCaseResult::List(value) => {
            let value = slice_static_list_value(
                value,
                bounds.start,
                bounds.end,
                format!("{path}.slice"),
                context,
            )?;
            Ok(StaticListCaseResult::List(
                evaluate_static_list_comprehension_value(value, path, evaluation)?,
            ))
        }
        StaticListCaseResult::Coalesce(coalesce) => Ok(StaticListCaseResult::Coalesce(
            static_list_coalesce_slice_comprehension_arguments(
                coalesce, bounds, path, evaluation, context,
            )?,
        )),
    }
}

fn static_list_coalesce_comprehension_arguments(
    coalesce: StaticListCoalesceArguments,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
) -> Result<StaticListCoalesceArguments, CoreError> {
    let path = path.into();
    let mut element_type = None;
    let arguments = coalesce
        .arguments
        .into_iter()
        .enumerate()
        .map(|(index, argument)| match argument {
            StaticListCoalesceArgument::Null => Ok(StaticListCoalesceArgument::Null),
            StaticListCoalesceArgument::List(value) => {
                let value = evaluate_static_list_comprehension_value(
                    value,
                    format!("{path}.arguments[{index}]"),
                    evaluation,
                )?;
                element_type = merge_static_list_coalesce_element_types(
                    element_type,
                    value.element_type,
                    &format!("{path}.arguments[{index}]"),
                )?;
                Ok(StaticListCoalesceArgument::List(value))
            }
        })
        .collect::<Result<Vec<_>, CoreError>>()?;
    Ok(StaticListCoalesceArguments {
        arguments,
        element_type,
    })
}

fn static_list_coalesce_slice_comprehension_arguments(
    coalesce: StaticListCoalesceArguments,
    bounds: StaticListSliceBounds<'_>,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
    context: &CypherCompileContext,
) -> Result<StaticListCoalesceArguments, CoreError> {
    let path = path.into();
    let mut element_type = None;
    let arguments = coalesce
        .arguments
        .into_iter()
        .enumerate()
        .map(|(index, argument)| match argument {
            StaticListCoalesceArgument::Null => Ok(StaticListCoalesceArgument::Null),
            StaticListCoalesceArgument::List(value) => {
                let value = slice_static_list_value(
                    value,
                    bounds.start,
                    bounds.end,
                    format!("{path}.arguments[{index}].slice"),
                    context,
                )?;
                let value = evaluate_static_list_comprehension_value(
                    value,
                    format!("{path}.arguments[{index}]"),
                    evaluation,
                )?;
                element_type = merge_static_list_coalesce_element_types(
                    element_type,
                    value.element_type,
                    &format!("{path}.arguments[{index}]"),
                )?;
                Ok(StaticListCoalesceArgument::List(value))
            }
        })
        .collect::<Result<Vec<_>, CoreError>>()?;
    Ok(StaticListCoalesceArguments {
        arguments,
        element_type,
    })
}

fn evaluate_static_list_comprehension_value(
    collection: StaticListValue,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
) -> Result<StaticListValue, CoreError> {
    let path = path.into();
    let source_element_type = collection.element_type;
    let literals = collection
        .literals
        .iter()
        .enumerate()
        .filter_map(|(index, item)| {
            let filter_evaluation = StaticFilterEvaluation {
                variable: evaluation.variable,
                item,
                accumulator_variable: None,
                accumulator: None,
                mode: evaluation.mode,
                context: evaluation.filter_context,
            };
            let map_evaluation = StaticFilterEvaluation {
                variable: evaluation.variable,
                item,
                accumulator_variable: None,
                accumulator: None,
                mode: evaluation.mode,
                context: evaluation.map_context,
            };
            let outcome = match evaluation.filter {
                Some(filter) => evaluate_static_filter_predicate_expression(
                    filter,
                    filter_evaluation,
                    format!("{path}.filter[{index}]"),
                ),
                None => Ok(StaticBooleanOutcome::True),
            };
            match outcome {
                Ok(StaticBooleanOutcome::True) => {
                    Some(evaluate_static_list_comprehension_map_expression(
                        evaluation.map,
                        map_evaluation,
                        format!("{path}.map[{index}]"),
                    ))
                }
                Ok(StaticBooleanOutcome::False | StaticBooleanOutcome::Unknown) => None,
                Err(error) => Some(Err(error)),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let element_type = match infer_literal_list_element_type(&literals) {
        Some(element_type) => Some(element_type),
        None => static_list_comprehension_output_element_type(
            evaluation.map,
            evaluation.variable,
            source_element_type,
            evaluation.map_context,
        )?,
    };
    Ok(StaticListValue {
        presence_variable: collection.presence_variable,
        literals,
        element_type,
    })
}

fn compile_static_range_list_value(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<StaticListValue, CoreError> {
    let path = path.into();
    let (start_argument, end_argument, step_argument) = match function.arguments.as_slice() {
        [start, end] => (start, end, None),
        [start, end, step] => (start, end, Some(step)),
        _ => {
            return Err(unsupported(
                format!("{path}.arguments"),
                "range() supports start, end, and optional step integer arguments",
            ));
        }
    };

    let start =
        compile_static_range_integer_argument(start_argument, format!("{path}.start"), context)?;
    let end = compile_static_range_integer_argument(end_argument, format!("{path}.end"), context)?;
    let step = if let Some(step) = step_argument {
        compile_static_range_integer_argument(step, format!("{path}.step"), context)?
    } else {
        1
    };
    if step == 0 {
        return Err(unsupported(
            format!("{path}.step"),
            "range() step must not be zero",
        ));
    }

    let mut literals = Vec::new();
    let mut current = start;
    let should_continue = |current: i64| {
        if step > 0 {
            current <= end
        } else {
            current >= end
        }
    };
    while should_continue(current) {
        if literals.len() >= MAX_STATIC_RANGE_LENGTH {
            return Err(unsupported(
                path.clone(),
                format!(
                    "static range() expands to more than {MAX_STATIC_RANGE_LENGTH} values; use a smaller range or split the query explicitly"
                ),
            ));
        }
        literals.push(Literal::Integer(current));
        if current == end {
            break;
        }
        current = current.checked_add(step).ok_or_else(|| {
            unsupported(
                path.clone(),
                "range() integer expansion overflowed i64 bounds",
            )
        })?;
    }

    Ok(StaticListValue {
        presence_variable: None,
        literals,
        element_type: Some(LiteralListElementType::Integer),
    })
}

fn compile_static_range_integer_argument(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<i64, CoreError> {
    let path = path.into();
    match compile_literal(expression, path.clone(), context)? {
        Literal::Integer(value) => Ok(value),
        _ => Err(unsupported(
            path,
            "range() arguments must be integer literals",
        )),
    }
}

fn compile_static_split_list_value(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<StaticListValue, CoreError> {
    let path = path.into();
    let [source, delimiter] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "split() supports exactly two string arguments",
        ));
    };

    let source = compile_static_split_string_argument(source, format!("{path}.source"), context)?;
    let delimiter =
        compile_static_split_string_argument(delimiter, format!("{path}.delimiter"), context)?;
    if delimiter.is_empty() {
        return Err(unsupported(
            format!("{path}.delimiter"),
            "static split() requires a non-empty delimiter",
        ));
    }

    let mut literals = Vec::new();
    for part in source.split(delimiter.as_str()) {
        if literals.len() >= MAX_STATIC_SPLIT_PARTS {
            return Err(unsupported(
                path.clone(),
                format!(
                    "static split() expands to more than {MAX_STATIC_SPLIT_PARTS} values; use a smaller string or split the query explicitly"
                ),
            ));
        }
        literals.push(Literal::String(part.to_string()));
    }

    Ok(StaticListValue {
        presence_variable: None,
        literals,
        element_type: Some(LiteralListElementType::String),
    })
}

fn compile_static_split_string_argument(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_static_split_string_argument(inner, path, context)
        }
        Expression::Literal(CypherLiteral::String(value)) => Ok(value.value.clone()),
        Expression::Parameter(parameter) => match context
            .parameter_value(parameter, path.clone())?
        {
            CypherParameterValue::Literal(Literal::String(value)) => Ok(value.clone()),
            CypherParameterValue::Literal(_) | CypherParameterValue::List(_) => Err(unsupported(
                path,
                "split() arguments must be string literals or scalar string parameters",
            )),
        },
        _ => Err(unsupported(
            path,
            "split() arguments must be string literals or scalar string parameters",
        )),
    }
}

fn compile_optional_static_map_keys_list_value(
    function: &FunctionInvocation,
) -> Option<StaticListValue> {
    let [argument] = function.arguments.as_slice() else {
        return None;
    };
    let Expression::Literal(CypherLiteral::Map(map)) = argument else {
        return None;
    };
    Some(StaticListValue {
        presence_variable: None,
        literals: map
            .entries
            .iter()
            .map(|(key, _)| Literal::String(key.name.name.clone()))
            .collect(),
        element_type: Some(LiteralListElementType::String),
    })
}

fn compile_static_list_cast_value(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<StaticListValue, CoreError> {
    let path = path.into();
    let target = static_list_cast_function(function).ok_or_else(|| {
        unsupported(
            path.clone(),
            format!(
                "function '{}' is not a static list cast function",
                qualified_function_name(function)
            ),
        )
    })?;
    let function_name = target.function_name();
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!("{function_name}() requires exactly one list argument"),
        ));
    };
    let Some(value) = compile_optional_static_list_value(
        argument,
        format!("{path}.arguments[0]"),
        plan,
        context,
    )?
    else {
        return Err(unsupported(
            format!("{path}.arguments[0]"),
            format!(
                "{function_name}() requires a literal list, list parameter, static split(...), range(...), tail(...), or static labels()/keys() metadata list"
            ),
        ));
    };

    let literals = value
        .literals
        .iter()
        .enumerate()
        .map(|(index, literal)| {
            cast_static_list_literal(literal, target, format!("{path}.arguments[0][{index}]"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(StaticListValue {
        presence_variable: value.presence_variable,
        literals,
        element_type: Some(target.element_type()),
    })
}

fn cast_static_list_literal(
    literal: &Literal,
    target: StaticListCastTarget,
    path: impl Into<String>,
) -> Result<Literal, CoreError> {
    match target {
        StaticListCastTarget::String => Ok(cast_static_literal_to_string_or_null(literal)),
        StaticListCastTarget::Integer => cast_static_literal_to_integer_or_null(literal, path),
        StaticListCastTarget::Float => Ok(cast_static_literal_to_float_or_null(literal)),
        StaticListCastTarget::Boolean => Ok(cast_static_literal_to_boolean_or_null(literal)),
    }
}

fn cast_static_literal_to_string_or_null(literal: &Literal) -> Literal {
    match literal {
        Literal::String(value) => Literal::String(value.clone()),
        Literal::Integer(value) => Literal::String(value.to_string()),
        Literal::Float(value) => Literal::String(value.into_inner().to_string()),
        Literal::Boolean(value) => Literal::String(value.to_string()),
        Literal::Null | Literal::List(_) => Literal::Null,
    }
}

fn cast_static_literal_to_integer_or_null(
    literal: &Literal,
    path: impl Into<String>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    match literal {
        Literal::Integer(value) => Ok(Literal::Integer(*value)),
        Literal::Float(value) => {
            let Some(value) = finite_f64_to_i64_or_null(value.into_inner(), path)? else {
                return Ok(Literal::Null);
            };
            Ok(Literal::Integer(value))
        }
        Literal::String(value) => Ok(value
            .trim()
            .parse::<i64>()
            .map_or(Literal::Null, Literal::Integer)),
        Literal::Boolean(value) => Ok(Literal::Integer(i64::from(*value))),
        Literal::Null | Literal::List(_) => Ok(Literal::Null),
    }
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    reason = "static Cypher toIntegerList() folding intentionally truncates finite floats after range checks"
)]
fn finite_f64_to_i64_or_null(value: f64, path: String) -> Result<Option<i64>, CoreError> {
    if !value.is_finite() {
        return Ok(None);
    }
    let truncated = value.trunc();
    if truncated < i64::MIN as f64 || truncated > i64::MAX as f64 {
        return Err(unsupported(
            path,
            "toIntegerList() static float conversion overflowed i64 bounds",
        ));
    }
    Ok(Some(truncated as i64))
}

fn cast_static_literal_to_float_or_null(literal: &Literal) -> Literal {
    let value = match literal {
        Literal::Integer(value) => Some(StaticNumericLiteral::Integer(*value).as_f64()),
        Literal::Float(value) => Some(value.into_inner()),
        Literal::String(value) => value
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite()),
        Literal::Boolean(_) | Literal::Null | Literal::List(_) => None,
    };
    match value {
        Some(value) => Literal::Float(OrderedFloat(value)),
        None => Literal::Null,
    }
}

fn cast_static_literal_to_boolean_or_null(literal: &Literal) -> Literal {
    let value = match literal {
        Literal::Boolean(value) => Some(*value),
        Literal::String(value) if value.trim().eq_ignore_ascii_case("true") => Some(true),
        Literal::String(value) if value.trim().eq_ignore_ascii_case("false") => Some(false),
        Literal::Integer(value) => Some(*value != 0),
        Literal::Float(_) | Literal::String(_) | Literal::Null | Literal::List(_) => None,
    };
    match value {
        Some(value) => Literal::Boolean(value),
        None => Literal::Null,
    }
}

fn recover_static_list_comprehension_filter(
    comprehension: &ListComprehension,
    source: &ListComprehensionSource,
    path: &str,
    context: &CypherCompileContext,
) -> Result<Option<(Expression, CypherCompileContext)>, CoreError> {
    if comprehension.filter.is_some() {
        return Ok(None);
    }
    source
        .filter_source
        .as_deref()
        .map(|filter_source| {
            parse_cypher_expression_fragment(filter_source, format!("{path}.filter"), context)
        })
        .transpose()
}

fn evaluate_static_list_comprehension_map_expression(
    map: Option<&Expression>,
    evaluation: StaticFilterEvaluation<'_>,
    path: impl Into<String>,
) -> Result<Literal, CoreError> {
    let Some(map) = map else {
        return Ok(evaluation.item.clone());
    };
    evaluate_static_map_expression(map, evaluation, path)
}

fn evaluate_static_map_expression(
    expression: &Expression,
    evaluation: StaticFilterEvaluation<'_>,
    path: impl Into<String>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => evaluate_static_map_expression(inner, evaluation, path),
        Expression::Variable(variable_ref) => {
            let variable = variable_name(variable_ref);
            evaluation
                .literal_for_variable(&variable)
                .cloned()
                .ok_or_else(|| {
                    unsupported(
                        path,
                        format!(
                            "static map variable '{variable}' is not {}",
                            evaluation.expected_variable_message()
                        ),
                    )
                })
        }
        Expression::Literal(_) | Expression::Parameter(_) => {
            compile_literal(expression, path, evaluation.context)
        }
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            operand,
            ..
        } => evaluate_static_map_numeric_negation(operand, evaluation, path),
        Expression::UnaryOp {
            op: UnaryOperator::Not,
            ..
        }
        | Expression::Comparison { .. }
        | Expression::In { .. }
        | Expression::IsNull { .. }
        | Expression::BinaryOp {
            op: CypherBinaryOperator::And | CypherBinaryOperator::Or | CypherBinaryOperator::Xor,
            ..
        } => evaluate_static_filter_predicate_expression(expression, evaluation, path)
            .map(static_boolean_outcome_literal),
        Expression::FunctionCall(function) if is_empty_function(function) => {
            evaluate_static_filter_is_empty(function, evaluation, path)
                .map(static_boolean_outcome_literal)
        }
        Expression::BinaryOp { op, lhs, rhs, .. } => {
            evaluate_static_map_arithmetic(*op, lhs, rhs, evaluation, path)
        }
        Expression::FunctionCall(function) => {
            evaluate_static_map_function(function, path, evaluation)
        }
        _ => Err(unsupported(
            path,
            "static list comprehension map expressions support the item variable, scalar literals, scalar parameters, arithmetic, predicate expressions, coalesce(), nullIf(), size()/char_length(), plain and nullable scalar casts, abs(), ceil()/ceiling(), floor(), round(), sqrt(), sign(), exp(), log()/ln(), log10(), pow()/power(), pi(), e(), sin(), cos(), tan(), cot(), asin(), acos(), atan(), atan2(), degrees(), radians(), haversin(), isNaN(), toLower()/lower(), toUpper()/upper(), trim()/btrim(), lTrim(), rTrim(), replace(), substring(), left(), right(), and reverse()",
        )),
    }
}

fn evaluate_static_map_numeric_negation(
    operand: &Expression,
    evaluation: StaticFilterEvaluation<'_>,
    path: impl Into<String>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    match evaluate_static_map_expression(operand, evaluation, format!("{path}.operand"))? {
        Literal::Integer(value) => value
            .checked_neg()
            .map(Literal::Integer)
            .ok_or_else(|| unsupported(path, "static numeric map negation overflowed i64")),
        Literal::Float(value) => Ok(Literal::Float(OrderedFloat(-value.into_inner()))),
        Literal::Null => Ok(Literal::Null),
        _ => Err(unsupported(
            path,
            "static numeric map negation requires numeric operands",
        )),
    }
}

fn evaluate_static_map_arithmetic(
    operator: CypherBinaryOperator,
    lhs: &Expression,
    rhs: &Expression,
    evaluation: StaticFilterEvaluation<'_>,
    path: impl Into<String>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let operator = compile_arithmetic_operator(operator, format!("{path}.operator"))?;
    let lhs = evaluate_static_map_expression(lhs, evaluation, format!("{path}.lhs"))?;
    let rhs = evaluate_static_map_expression(rhs, evaluation, format!("{path}.rhs"))?;
    evaluate_static_literal_arithmetic(&lhs, operator, &rhs, path)
}

#[derive(Debug, Clone, Copy)]
enum StaticNumericLiteral {
    Integer(i64),
    Float(f64),
}

impl StaticNumericLiteral {
    fn from_literal(literal: &Literal, path: impl Into<String>) -> Result<Option<Self>, CoreError> {
        match literal {
            Literal::Integer(value) => Ok(Some(Self::Integer(*value))),
            Literal::Float(value) => Ok(Some(Self::Float(value.into_inner()))),
            Literal::Null => Ok(None),
            _ => Err(unsupported(
                path,
                "static numeric map expressions require numeric operands",
            )),
        }
    }

    fn is_integer(self) -> bool {
        matches!(self, Self::Integer(_))
    }

    fn as_i64(self) -> i64 {
        match self {
            Self::Integer(value) => value,
            Self::Float(_) => unreachable!("float numeric literal has no i64 value"),
        }
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "static numeric map folding follows SQL numeric promotion for mixed/inexact arithmetic"
    )]
    fn as_f64(self) -> f64 {
        match self {
            Self::Integer(value) => value as f64,
            Self::Float(value) => value,
        }
    }
}

fn static_boolean_outcome_literal(outcome: StaticBooleanOutcome) -> Literal {
    match outcome {
        StaticBooleanOutcome::True => Literal::Boolean(true),
        StaticBooleanOutcome::False => Literal::Boolean(false),
        StaticBooleanOutcome::Unknown => Literal::Null,
    }
}

fn evaluate_static_map_function(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    if let Some(literal) = evaluate_static_map_null_or_length_function(function, &path, evaluation)?
    {
        return Ok(literal);
    }
    if let Some(literal) = evaluate_static_map_cast_function(function, &path, evaluation)? {
        return Ok(literal);
    }
    if let Some(literal) = evaluate_static_map_numeric_function(function, &path, evaluation)? {
        return Ok(literal);
    }
    if is_is_nan_function(function) {
        return evaluate_static_map_is_nan(function, &path, evaluation);
    }
    if let Some(literal) = evaluate_static_map_string_function(function, &path, evaluation)? {
        return Ok(literal);
    }
    Err(unsupported(
        path,
        format!(
            "function '{}' is not supported in static list comprehension map expressions",
            qualified_function_name(function)
        ),
    ))
}

fn evaluate_static_map_null_or_length_function(
    function: &FunctionInvocation,
    path: &str,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Option<Literal>, CoreError> {
    if is_coalesce_function(function) {
        return evaluate_static_map_coalesce(function, path.to_string(), evaluation).map(Some);
    }
    if is_null_if_function(function) {
        return evaluate_static_map_null_if(function, path.to_string(), evaluation).map(Some);
    }
    if is_character_length_function(function) {
        return evaluate_static_map_character_length(function, path.to_string(), evaluation)
            .map(Some);
    }
    Ok(None)
}

fn evaluate_static_map_cast_function(
    function: &FunctionInvocation,
    path: &str,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Option<Literal>, CoreError> {
    if is_to_string_function(function) || is_to_string_or_null_function(function) {
        return evaluate_static_map_to_string(function, path.to_string(), evaluation).map(Some);
    }
    if is_to_integer_function(function) {
        return evaluate_static_map_to_integer(function, path.to_string(), evaluation).map(Some);
    }
    if is_to_integer_or_null_function(function) {
        return evaluate_static_map_to_integer(function, path.to_string(), evaluation).map(Some);
    }
    if is_to_float_function(function) {
        return evaluate_static_map_to_float(function, path.to_string(), evaluation).map(Some);
    }
    if is_to_float_or_null_function(function) {
        return evaluate_static_map_to_float(function, path.to_string(), evaluation).map(Some);
    }
    if is_to_boolean_function(function) {
        return evaluate_static_map_to_boolean(function, path.to_string(), evaluation).map(Some);
    }
    if is_to_boolean_or_null_function(function) {
        return evaluate_static_map_to_boolean(function, path.to_string(), evaluation).map(Some);
    }
    Ok(None)
}

fn evaluate_static_map_numeric_function(
    function: &FunctionInvocation,
    path: &str,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Option<Literal>, CoreError> {
    if is_abs_function(function) {
        return evaluate_static_map_abs(function, path.to_string(), evaluation).map(Some);
    }
    if is_ceil_function(function) {
        return evaluate_static_map_unary_numeric_float_function(
            function,
            path.to_string(),
            evaluation,
            "ceil",
            f64::ceil,
        )
        .map(Some);
    }
    if is_floor_function(function) {
        return evaluate_static_map_unary_numeric_float_function(
            function,
            path.to_string(),
            evaluation,
            "floor",
            f64::floor,
        )
        .map(Some);
    }
    if is_round_function(function) {
        return evaluate_static_map_round(function, path.to_string(), evaluation).map(Some);
    }
    if is_sqrt_function(function) {
        return evaluate_static_map_sqrt(function, path.to_string(), evaluation).map(Some);
    }
    if is_sign_function(function) {
        return evaluate_static_map_sign(function, path.to_string(), evaluation).map(Some);
    }
    if is_exp_function(function) {
        return evaluate_static_map_unary_numeric_float_function(
            function,
            path.to_string(),
            evaluation,
            "exp",
            f64::exp,
        )
        .map(Some);
    }
    if is_log_function(function) {
        let function_name = qualified_function_name(function);
        return evaluate_static_map_unary_numeric_float_function(
            function,
            path.to_string(),
            evaluation,
            &function_name,
            f64::ln,
        )
        .map(Some);
    }
    if is_log10_function(function) {
        return evaluate_static_map_unary_numeric_float_function(
            function,
            path.to_string(),
            evaluation,
            "log10",
            f64::log10,
        )
        .map(Some);
    }
    if is_power_function(function) {
        return evaluate_static_map_power(function, path.to_string(), evaluation).map(Some);
    }
    if is_pi_function(function) {
        return evaluate_static_map_constant_function(
            function,
            path.to_string(),
            evaluation,
            "pi",
            std::f64::consts::PI,
        )
        .map(Some);
    }
    if is_e_function(function) {
        return evaluate_static_map_constant_function(
            function,
            path.to_string(),
            evaluation,
            "e",
            std::f64::consts::E,
        )
        .map(Some);
    }
    evaluate_static_map_trigonometric_function(function, path, evaluation)
}

fn evaluate_static_map_trigonometric_function(
    function: &FunctionInvocation,
    path: &str,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Option<Literal>, CoreError> {
    if let Some(literal) =
        evaluate_static_map_unary_trigonometric_function(function, path, evaluation)?
    {
        return Ok(Some(literal));
    }
    evaluate_static_map_angle_function(function, path, evaluation)
}

fn evaluate_static_map_unary_trigonometric_function(
    function: &FunctionInvocation,
    path: &str,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Option<Literal>, CoreError> {
    if is_sin_function(function) {
        return evaluate_static_map_unary_numeric_float_function(
            function,
            path.to_string(),
            evaluation,
            "sin",
            f64::sin,
        )
        .map(Some);
    }
    if is_cos_function(function) {
        return evaluate_static_map_unary_numeric_float_function(
            function,
            path.to_string(),
            evaluation,
            "cos",
            f64::cos,
        )
        .map(Some);
    }
    if is_tan_function(function) {
        return evaluate_static_map_unary_numeric_float_function(
            function,
            path.to_string(),
            evaluation,
            "tan",
            f64::tan,
        )
        .map(Some);
    }
    if is_cot_function(function) {
        return evaluate_static_map_unary_numeric_float_function(
            function,
            path.to_string(),
            evaluation,
            "cot",
            |value| 1.0 / value.tan(),
        )
        .map(Some);
    }
    if is_asin_function(function) {
        return evaluate_static_map_unary_numeric_float_function(
            function,
            path.to_string(),
            evaluation,
            "asin",
            f64::asin,
        )
        .map(Some);
    }
    if is_acos_function(function) {
        return evaluate_static_map_unary_numeric_float_function(
            function,
            path.to_string(),
            evaluation,
            "acos",
            f64::acos,
        )
        .map(Some);
    }
    if is_atan_function(function) {
        return evaluate_static_map_unary_numeric_float_function(
            function,
            path.to_string(),
            evaluation,
            "atan",
            f64::atan,
        )
        .map(Some);
    }
    Ok(None)
}

fn evaluate_static_map_angle_function(
    function: &FunctionInvocation,
    path: &str,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Option<Literal>, CoreError> {
    if is_atan2_function(function) {
        return evaluate_static_map_atan2(function, path.to_string(), evaluation).map(Some);
    }
    if is_degrees_function(function) {
        return evaluate_static_map_unary_numeric_float_function(
            function,
            path.to_string(),
            evaluation,
            "degrees",
            f64::to_degrees,
        )
        .map(Some);
    }
    if is_radians_function(function) {
        return evaluate_static_map_unary_numeric_float_function(
            function,
            path.to_string(),
            evaluation,
            "radians",
            f64::to_radians,
        )
        .map(Some);
    }
    if is_haversin_function(function) {
        return evaluate_static_map_unary_numeric_float_function(
            function,
            path.to_string(),
            evaluation,
            "haversin",
            |value| (1.0 - value.cos()) / 2.0,
        )
        .map(Some);
    }
    Ok(None)
}

fn evaluate_static_map_string_function(
    function: &FunctionInvocation,
    path: &str,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Option<Literal>, CoreError> {
    if is_to_string_function(function) {
        return evaluate_static_map_to_string(function, path.to_string(), evaluation).map(Some);
    }
    if is_to_lower_function(function) {
        return evaluate_static_map_unary_string_function(
            function,
            path.to_string(),
            evaluation,
            "toLower",
            str::to_lowercase,
        )
        .map(Some);
    }
    if is_to_upper_function(function) {
        return evaluate_static_map_unary_string_function(
            function,
            path.to_string(),
            evaluation,
            "toUpper",
            str::to_uppercase,
        )
        .map(Some);
    }
    evaluate_static_map_more_string_function(function, path, evaluation)
}

fn evaluate_static_map_more_string_function(
    function: &FunctionInvocation,
    path: &str,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Option<Literal>, CoreError> {
    if is_trim_function(function) {
        return evaluate_static_map_unary_string_function(
            function,
            path.to_string(),
            evaluation,
            "trim",
            |value| value.trim().to_string(),
        )
        .map(Some);
    }
    if is_ltrim_function(function) {
        return evaluate_static_map_unary_string_function(
            function,
            path.to_string(),
            evaluation,
            "lTrim",
            |value| value.trim_start().to_string(),
        )
        .map(Some);
    }
    if is_rtrim_function(function) {
        return evaluate_static_map_unary_string_function(
            function,
            path.to_string(),
            evaluation,
            "rTrim",
            |value| value.trim_end().to_string(),
        )
        .map(Some);
    }
    evaluate_static_map_composite_string_function(function, path, evaluation)
}

fn evaluate_static_map_composite_string_function(
    function: &FunctionInvocation,
    path: &str,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Option<Literal>, CoreError> {
    if is_replace_function(function) {
        return evaluate_static_map_replace(function, path.to_string(), evaluation).map(Some);
    }
    if is_substring_function(function) {
        return evaluate_static_map_substring(function, path.to_string(), evaluation).map(Some);
    }
    if is_left_function(function) {
        return evaluate_static_map_left(function, path.to_string(), evaluation).map(Some);
    }
    if is_right_function(function) {
        return evaluate_static_map_right(function, path.to_string(), evaluation).map(Some);
    }
    if is_reverse_function(function) {
        return evaluate_static_map_unary_string_function(
            function,
            path.to_string(),
            evaluation,
            "reverse",
            |value| value.chars().rev().collect(),
        )
        .map(Some);
    }
    Ok(None)
}

fn evaluate_static_map_function_arguments(
    function: &FunctionInvocation,
    path: &str,
    evaluation: StaticFilterEvaluation<'_>,
    function_name: &str,
) -> Result<Vec<Literal>, CoreError> {
    if let Some(argument_sources) = evaluation.context.function_argument_sources(function) {
        return evaluate_static_map_function_argument_sources(
            argument_sources,
            path,
            evaluation,
            function_name,
        );
    }

    let variable_argument = evaluation.context.variable_function_argument_info(function);
    if let Some(argument) = variable_argument {
        if evaluation
            .literal_for_variable(argument.variable.as_str())
            .is_none()
        {
            return Err(unsupported(
                format!("{path}.arguments"),
                format!(
                    "{function_name}() argument '{}' is not {}",
                    argument.variable,
                    evaluation.expected_variable_message()
                ),
            ));
        }
        if argument.count != 0
            && (argument.count != function.arguments.len() + 1 || argument.index >= argument.count)
        {
            return Err(unsupported(
                format!("{path}.arguments"),
                format!("{function_name}() arguments could not be recovered from the parsed AST"),
            ));
        }
    }

    let mut literals = function
        .arguments
        .iter()
        .enumerate()
        .map(|(index, argument)| {
            let logical_index = match variable_argument {
                Some(variable_argument) if index >= variable_argument.index => index + 1,
                _ => index,
            };
            evaluate_static_map_expression(
                argument,
                evaluation,
                format!("{path}.arguments[{logical_index}]"),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(argument) = variable_argument {
        let literal = evaluation
            .literal_for_variable(argument.variable.as_str())
            .ok_or_else(|| CoreError::internal("validated static function variable was missing"))?;
        literals.insert(argument.index, literal.clone());
    }
    Ok(literals)
}

fn evaluate_static_map_function_argument_sources(
    argument_sources: &FunctionArgumentSources,
    path: &str,
    evaluation: StaticFilterEvaluation<'_>,
    function_name: &str,
) -> Result<Vec<Literal>, CoreError> {
    argument_sources
        .arguments
        .iter()
        .enumerate()
        .map(|(index, source)| {
            let (expression, fragment_context) = parse_cypher_expression_fragment(
                source,
                format!("{path}.arguments[{index}]"),
                evaluation.context,
            )?;
            evaluate_static_map_expression(
                &expression,
                StaticFilterEvaluation {
                    context: &fragment_context,
                    ..evaluation
                },
                format!("{path}.arguments[{index}]"),
            )
            .map_err(|error| {
                if parse_collection_filter_variable(source)
                    .is_some_and(|variable| evaluation.literal_for_variable(&variable).is_none())
                {
                    unsupported(
                        format!("{path}.arguments[{index}]"),
                        format!(
                            "{function_name}() argument '{}' is not {}",
                            source,
                            evaluation.expected_variable_message()
                        ),
                    )
                } else {
                    error
                }
            })
        })
        .collect()
}

fn evaluate_static_map_coalesce(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let arguments =
        evaluate_static_map_function_arguments(function, &path, evaluation, "coalesce")?;
    if arguments.len() < 2 {
        return Err(unsupported(
            format!("{path}.arguments"),
            "coalesce() in static list comprehension maps requires at least two arguments",
        ));
    }
    Ok(arguments
        .into_iter()
        .find(|literal| !matches!(literal, Literal::Null))
        .unwrap_or(Literal::Null))
}

fn evaluate_static_map_null_if(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let arguments = evaluate_static_map_function_arguments(function, &path, evaluation, "nullIf")?;
    let [expression, value] = arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "nullIf() in static list comprehension maps requires exactly two arguments",
        ));
    };
    if matches!(expression, Literal::Null) {
        return Ok(Literal::Null);
    }
    if matches!(value, Literal::Null) {
        return Ok(expression.clone());
    }
    if evaluate_literal_comparison(expression, ComparisonOperator::Equal, value, path)? {
        Ok(Literal::Null)
    } else {
        Ok(expression.clone())
    }
}

fn evaluate_static_map_character_length(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let function_name = qualified_function_name(function);
    let literal = evaluate_static_map_single_function_argument(
        function,
        path.clone(),
        evaluation,
        &function_name,
    )?;
    let Some(value) = static_map_string_argument(&literal, &function_name, path.clone())? else {
        return Ok(Literal::Null);
    };
    let length = i64::try_from(value.chars().count()).map_err(|error| {
        CoreError::internal(format!("static string length overflowed i64: {error}"))
    })?;
    Ok(Literal::Integer(length))
}

fn evaluate_static_map_abs(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let literal =
        evaluate_static_map_single_function_argument(function, path.clone(), evaluation, "abs")?;
    let Some(value) = StaticNumericLiteral::from_literal(&literal, format!("{path}.arguments[0]"))?
    else {
        return Ok(Literal::Null);
    };
    match value {
        StaticNumericLiteral::Integer(value) => {
            value.checked_abs().map(Literal::Integer).ok_or_else(|| {
                unsupported(
                    path,
                    "abs() in static list comprehension maps overflowed i64",
                )
            })
        }
        StaticNumericLiteral::Float(value) => Ok(Literal::Float(OrderedFloat(value.abs()))),
    }
}

fn evaluate_static_map_unary_numeric_float_function(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
    function_name: &str,
    transform: impl FnOnce(f64) -> f64,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let literal = evaluate_static_map_single_function_argument(
        function,
        path.clone(),
        evaluation,
        function_name,
    )?;
    let Some(value) = StaticNumericLiteral::from_literal(&literal, format!("{path}.arguments[0]"))?
    else {
        return Ok(Literal::Null);
    };
    let value = transform(value.as_f64());
    if !value.is_finite() {
        return Err(unsupported(
            path,
            format!(
                "{function_name}() in static list comprehension maps produced a non-finite float"
            ),
        ));
    }
    Ok(Literal::Float(OrderedFloat(value)))
}

fn evaluate_static_map_round(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let arguments = evaluate_static_map_function_arguments(function, &path, evaluation, "round")?;
    let (value, places) = match arguments.as_slice() {
        [value] => (value, None),
        [value, places] => (value, Some(places)),
        _ => {
            return Err(unsupported(
                format!("{path}.arguments"),
                "round() in static list comprehension maps requires exactly one or two arguments",
            ));
        }
    };
    let Some(value) = StaticNumericLiteral::from_literal(value, format!("{path}.arguments[0]"))?
    else {
        return Ok(Literal::Null);
    };
    let places = match places {
        Some(Literal::Integer(places)) => Some(i32::try_from(*places).map_err(|error| {
            unsupported(
                format!("{path}.arguments[1]"),
                format!("round() precision argument is out of range: {error}"),
            )
        })?),
        Some(Literal::Null) | None => None,
        Some(_) => {
            return Err(unsupported(
                format!("{path}.arguments[1]"),
                "round() in static list comprehension maps requires integer precision arguments",
            ));
        }
    };
    let value = match places {
        Some(places) => round_static_float(value.as_f64(), places, path.clone())?,
        None => value.as_f64().round(),
    };
    if !value.is_finite() {
        return Err(unsupported(
            path,
            "round() in static list comprehension maps produced a non-finite float",
        ));
    }
    Ok(Literal::Float(OrderedFloat(value)))
}

fn round_static_float(value: f64, places: i32, path: impl Into<String>) -> Result<f64, CoreError> {
    let path = path.into();
    let scale = 10_f64.powi(places);
    if !scale.is_finite() || scale == 0.0 {
        return Err(unsupported(
            path,
            "round() precision argument is out of supported range",
        ));
    }
    Ok((value * scale).round() / scale)
}

fn evaluate_static_map_sqrt(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let literal =
        evaluate_static_map_single_function_argument(function, path.clone(), evaluation, "sqrt")?;
    let Some(value) = StaticNumericLiteral::from_literal(&literal, format!("{path}.arguments[0]"))?
    else {
        return Ok(Literal::Null);
    };
    let value = value.as_f64();
    if value < 0.0 {
        return Err(unsupported(
            path,
            "sqrt() in static list comprehension maps requires non-negative numeric arguments",
        ));
    }
    let value = value.sqrt();
    if !value.is_finite() {
        return Err(unsupported(
            path,
            "sqrt() in static list comprehension maps produced a non-finite float",
        ));
    }
    Ok(Literal::Float(OrderedFloat(value)))
}

fn evaluate_static_map_sign(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let literal =
        evaluate_static_map_single_function_argument(function, path.clone(), evaluation, "sign")?;
    let Some(value) = StaticNumericLiteral::from_literal(&literal, format!("{path}.arguments[0]"))?
    else {
        return Ok(Literal::Null);
    };
    let sign = match value {
        StaticNumericLiteral::Integer(value) => value.signum(),
        StaticNumericLiteral::Float(value) => {
            if value > 0.0 {
                1
            } else if value < 0.0 {
                -1
            } else {
                0
            }
        }
    };
    Ok(Literal::Integer(sign))
}

fn evaluate_static_map_is_nan(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let literal =
        evaluate_static_map_single_function_argument(function, path.clone(), evaluation, "isNaN")?;
    let Some(value) = StaticNumericLiteral::from_literal(&literal, format!("{path}.arguments[0]"))?
    else {
        return Ok(Literal::Null);
    };
    Ok(Literal::Boolean(value.as_f64().is_nan()))
}

fn evaluate_static_map_power(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let function_name = qualified_function_name(function);
    let arguments =
        evaluate_static_map_function_arguments(function, &path, evaluation, &function_name)?;
    let [base, exponent] = arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!(
                "{function_name}() in static list comprehension maps requires exactly two arguments"
            ),
        ));
    };
    evaluate_static_literal_arithmetic(base, ArithmeticOperator::Power, exponent, path)
}

fn evaluate_static_map_atan2(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let arguments = evaluate_static_map_function_arguments(function, &path, evaluation, "atan2")?;
    let [y, x] = arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "atan2() in static list comprehension maps requires exactly two arguments",
        ));
    };
    let Some(y) = StaticNumericLiteral::from_literal(y, format!("{path}.arguments[0]"))? else {
        return Ok(Literal::Null);
    };
    let Some(x) = StaticNumericLiteral::from_literal(x, format!("{path}.arguments[1]"))? else {
        return Ok(Literal::Null);
    };
    let value = y.as_f64().atan2(x.as_f64());
    if !value.is_finite() {
        return Err(unsupported(
            path,
            "atan2() in static list comprehension maps produced a non-finite float",
        ));
    }
    Ok(Literal::Float(OrderedFloat(value)))
}

fn evaluate_static_map_constant_function(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
    function_name: &str,
    value: f64,
) -> Result<Literal, CoreError> {
    let path = path.into();
    if function.arguments.is_empty()
        && evaluation
            .context
            .variable_function_argument_info(function)
            .is_none()
    {
        return Ok(Literal::Float(OrderedFloat(value)));
    }
    Err(unsupported(
        format!("{path}.arguments"),
        format!(
            "{function_name}() in static list comprehension maps requires exactly zero arguments"
        ),
    ))
}

fn evaluate_static_map_to_string(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let literal =
        evaluate_static_map_single_function_argument(function, path, evaluation, "toString")?;
    Ok(match literal {
        Literal::String(value) => Literal::String(value),
        Literal::Integer(value) => Literal::String(value.to_string()),
        Literal::Float(value) => Literal::String(value.into_inner().to_string()),
        Literal::Boolean(value) => Literal::String(value.to_string()),
        Literal::Null | Literal::List(_) => Literal::Null,
    })
}

fn evaluate_static_map_to_integer(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let function_name = qualified_function_name(function);
    let literal = evaluate_static_map_single_function_argument(
        function,
        path.clone(),
        evaluation,
        &function_name,
    )?;
    match literal {
        Literal::Integer(value) => Ok(Literal::Integer(value)),
        Literal::String(value) => match value.trim().parse::<i64>() {
            Ok(value) => Ok(Literal::Integer(value)),
            Err(_) => Ok(Literal::Null),
        },
        _ => Ok(Literal::Null),
    }
}

fn evaluate_static_map_to_float(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let function_name = qualified_function_name(function);
    let literal = evaluate_static_map_single_function_argument(
        function,
        path.clone(),
        evaluation,
        &function_name,
    )?;
    let value = match literal {
        Literal::Integer(value) => Some(StaticNumericLiteral::Integer(value).as_f64()),
        Literal::Float(value) => Some(value.into_inner()),
        Literal::String(value) => value
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite()),
        Literal::Null => return Ok(Literal::Null),
        Literal::Boolean(_) | Literal::List(_) => None,
    };
    match value {
        Some(value) => Ok(Literal::Float(OrderedFloat(value))),
        None => Ok(Literal::Null),
    }
}

fn evaluate_static_map_to_boolean(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let function_name = qualified_function_name(function);
    let literal = evaluate_static_map_single_function_argument(
        function,
        path.clone(),
        evaluation,
        &function_name,
    )?;
    let value = match literal {
        Literal::Boolean(value) => Some(value),
        Literal::String(value) if value.trim().eq_ignore_ascii_case("true") => Some(true),
        Literal::String(value) if value.trim().eq_ignore_ascii_case("false") => Some(false),
        Literal::Null => return Ok(Literal::Null),
        Literal::Integer(_) | Literal::Float(_) | Literal::String(_) | Literal::List(_) => None,
    };
    match value {
        Some(value) => Ok(Literal::Boolean(value)),
        None => Ok(Literal::Null),
    }
}

fn evaluate_static_map_unary_string_function(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
    function_name: &str,
    transform: impl FnOnce(&str) -> String,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let literal = evaluate_static_map_single_function_argument(
        function,
        path.clone(),
        evaluation,
        function_name,
    )?;
    let Some(value) = static_map_string_argument(&literal, function_name, path)? else {
        return Ok(Literal::Null);
    };
    Ok(Literal::String(transform(&value)))
}

fn evaluate_static_map_single_function_argument(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
    function_name: &str,
) -> Result<Literal, CoreError> {
    let path = path.into();
    if let Some(argument) = evaluation.context.variable_function_argument_info(function) {
        if argument.variable != evaluation.variable {
            return Err(unsupported(
                format!("{path}.arguments"),
                format!(
                    "{function_name}() argument '{}' is not the item variable '{}'",
                    argument.variable, evaluation.variable
                ),
            ));
        }
        if argument.index != 0
            || (argument.count != 0 && argument.count != 1)
            || !function.arguments.is_empty()
        {
            return Err(unsupported(
                format!("{path}.arguments"),
                format!(
                    "{function_name}() in static list comprehension maps requires exactly one argument"
                ),
            ));
        }
        return Ok(evaluation.item.clone());
    }

    match function.arguments.as_slice() {
        [argument] => {
            evaluate_static_map_expression(argument, evaluation, format!("{path}.arguments[0]"))
        }
        _ => Err(unsupported(
            format!("{path}.arguments"),
            format!(
                "{function_name}() in static list comprehension maps requires exactly one argument"
            ),
        )),
    }
}

fn evaluate_static_map_replace(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let arguments = evaluate_static_map_function_arguments(function, &path, evaluation, "replace")?;
    let [expression, search, replacement] = arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "replace() in static list comprehension maps requires exactly three arguments",
        ));
    };
    let Some(expression) = static_map_string_argument(expression, "replace", path.clone())? else {
        return Ok(Literal::Null);
    };
    let Some(search) = static_map_string_argument(search, "replace", path.clone())? else {
        return Ok(Literal::Null);
    };
    let Some(replacement) = static_map_string_argument(replacement, "replace", path)? else {
        return Ok(Literal::Null);
    };
    Ok(Literal::String(expression.replace(&search, &replacement)))
}

fn evaluate_static_map_substring(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let arguments =
        evaluate_static_map_function_arguments(function, &path, evaluation, "substring")?;
    let (expression, start, length) = match arguments.as_slice() {
        [expression, start] => (expression, start, None),
        [expression, start, length] => (expression, start, Some(length)),
        _ => {
            return Err(unsupported(
                format!("{path}.arguments"),
                "substring() in static list comprehension maps requires exactly two or three arguments",
            ));
        }
    };
    let Some(expression) = static_map_string_argument(expression, "substring", path.clone())?
    else {
        return Ok(Literal::Null);
    };
    let Some(start) = static_map_non_negative_integer_argument(start, "substring", path.clone())?
    else {
        return Ok(Literal::Null);
    };
    let length = match length {
        Some(length) => {
            match static_map_non_negative_integer_argument(length, "substring", path)? {
                Some(length) => Some(length),
                None => return Ok(Literal::Null),
            }
        }
        None => None,
    };
    Ok(Literal::String(static_substring(
        &expression,
        start,
        length,
    )))
}

fn evaluate_static_map_left(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let (expression, count) =
        evaluate_static_map_two_argument_string_function(function, path, evaluation, "left")?;
    let Some(expression) = expression else {
        return Ok(Literal::Null);
    };
    let Some(count) = count else {
        return Ok(Literal::Null);
    };
    Ok(Literal::String(expression.chars().take(count).collect()))
}

fn evaluate_static_map_right(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
) -> Result<Literal, CoreError> {
    let (expression, count) =
        evaluate_static_map_two_argument_string_function(function, path, evaluation, "right")?;
    let Some(expression) = expression else {
        return Ok(Literal::Null);
    };
    let Some(count) = count else {
        return Ok(Literal::Null);
    };
    let character_count = expression.chars().count();
    Ok(Literal::String(
        expression
            .chars()
            .skip(character_count.saturating_sub(count))
            .collect(),
    ))
}

fn evaluate_static_map_two_argument_string_function(
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticFilterEvaluation<'_>,
    function_name: &str,
) -> Result<(Option<String>, Option<usize>), CoreError> {
    let path = path.into();
    let arguments =
        evaluate_static_map_function_arguments(function, &path, evaluation, function_name)?;
    let [expression, count] = arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!(
                "{function_name}() in static list comprehension maps requires exactly two arguments"
            ),
        ));
    };
    Ok((
        static_map_string_argument(expression, function_name, path.clone())?,
        static_map_non_negative_integer_argument(count, function_name, path)?,
    ))
}

fn static_substring(value: &str, start: usize, length: Option<usize>) -> String {
    let characters = value.chars().skip(start);
    match length {
        Some(length) => characters.take(length).collect(),
        None => characters.collect(),
    }
}

fn static_map_string_argument(
    literal: &Literal,
    function_name: &str,
    path: impl Into<String>,
) -> Result<Option<String>, CoreError> {
    match literal {
        Literal::String(value) => Ok(Some(value.clone())),
        Literal::Null => Ok(None),
        _ => Err(unsupported(
            path,
            format!(
                "{function_name}() in static list comprehension maps requires string arguments"
            ),
        )),
    }
}

fn static_map_non_negative_integer_argument(
    literal: &Literal,
    function_name: &str,
    path: impl Into<String>,
) -> Result<Option<usize>, CoreError> {
    let path = path.into();
    match literal {
        Literal::Integer(value) if *value >= 0 => {
            usize::try_from(*value).map(Some).map_err(|error| {
                unsupported(
                    path,
                    format!("{function_name}() integer argument is out of range: {error}"),
                )
            })
        }
        Literal::Integer(_) => Err(unsupported(
            path,
            format!(
                "{function_name}() in static list comprehension maps requires non-negative integer arguments"
            ),
        )),
        Literal::Null => Ok(None),
        _ => Err(unsupported(
            path,
            format!(
                "{function_name}() in static list comprehension maps requires integer arguments"
            ),
        )),
    }
}

fn static_list_comprehension_output_element_type(
    map: Option<&Expression>,
    variable: &str,
    source_element_type: Option<LiteralListElementType>,
    context: &CypherCompileContext,
) -> Result<Option<LiteralListElementType>, CoreError> {
    let Some(map) = map else {
        return Ok(source_element_type);
    };
    static_list_comprehension_map_element_type(map, variable, source_element_type, context)
}

fn static_list_comprehension_map_element_type(
    expression: &Expression,
    variable: &str,
    source_element_type: Option<LiteralListElementType>,
    context: &CypherCompileContext,
) -> Result<Option<LiteralListElementType>, CoreError> {
    match expression {
        Expression::Parenthesized(inner) => static_list_comprehension_map_element_type(
            inner,
            variable,
            source_element_type,
            context,
        ),
        Expression::Variable(variable_ref) if variable_name(variable_ref) == variable => {
            Ok(source_element_type)
        }
        Expression::Literal(_) => Ok(literal_list_element_kind(&compile_literal(
            expression,
            "list_comprehension.map",
            context,
        )?)),
        Expression::Parameter(parameter) => {
            match context.parameter_value(parameter, "list_comprehension.map")? {
                CypherParameterValue::Literal(literal) => Ok(literal_list_element_kind(literal)),
                CypherParameterValue::List(_) => Ok(None),
            }
        }
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            ..
        } => match source_element_type {
            Some(LiteralListElementType::Integer | LiteralListElementType::Float) => {
                Ok(source_element_type)
            }
            _ => Ok(None),
        },
        Expression::UnaryOp {
            op: UnaryOperator::Not,
            ..
        }
        | Expression::Comparison { .. }
        | Expression::In { .. }
        | Expression::IsNull { .. }
        | Expression::BinaryOp {
            op: CypherBinaryOperator::And | CypherBinaryOperator::Or | CypherBinaryOperator::Xor,
            ..
        } => Ok(Some(LiteralListElementType::Boolean)),
        Expression::BinaryOp { op, lhs, rhs, .. } => static_map_arithmetic_element_type(
            *op,
            lhs,
            rhs,
            variable,
            source_element_type,
            context,
        ),
        Expression::FunctionCall(function) => {
            static_map_function_element_type(function, variable, source_element_type, context)
        }
        _ => Ok(None),
    }
}

fn static_map_function_element_type(
    function: &FunctionInvocation,
    variable: &str,
    source_element_type: Option<LiteralListElementType>,
    context: &CypherCompileContext,
) -> Result<Option<LiteralListElementType>, CoreError> {
    if is_coalesce_function(function) {
        return static_map_coalesce_element_type(function, variable, source_element_type, context);
    }
    if is_null_if_function(function) {
        return static_map_null_if_element_type(function, variable, source_element_type, context);
    }
    if is_character_length_function(function) {
        return Ok(Some(LiteralListElementType::Integer));
    }
    if is_empty_function(function) {
        return Ok(Some(LiteralListElementType::Boolean));
    }
    if let Some(element_type) =
        static_map_cast_function_element_type(function, variable, source_element_type, context)?
    {
        return Ok(Some(element_type));
    }
    if let Some(element_type) =
        static_map_numeric_function_element_type(function, variable, source_element_type, context)?
    {
        return Ok(Some(element_type));
    }
    if static_map_string_function_returns_string(function) {
        return Ok(Some(LiteralListElementType::String));
    }
    Ok(None)
}

fn static_map_cast_function_element_type(
    function: &FunctionInvocation,
    variable: &str,
    source_element_type: Option<LiteralListElementType>,
    context: &CypherCompileContext,
) -> Result<Option<LiteralListElementType>, CoreError> {
    let element_type = if is_to_string_function(function) || is_to_string_or_null_function(function)
    {
        LiteralListElementType::String
    } else if is_to_integer_function(function) || is_to_integer_or_null_function(function) {
        LiteralListElementType::Integer
    } else if is_to_float_function(function) || is_to_float_or_null_function(function) {
        LiteralListElementType::Float
    } else if is_to_boolean_function(function) || is_to_boolean_or_null_function(function) {
        LiteralListElementType::Boolean
    } else {
        return Ok(None);
    };
    static_map_single_function_argument_element_type(
        function,
        variable,
        source_element_type,
        context,
        &qualified_function_name(function),
    )?;
    Ok(Some(element_type))
}

fn static_map_numeric_function_element_type(
    function: &FunctionInvocation,
    variable: &str,
    source_element_type: Option<LiteralListElementType>,
    context: &CypherCompileContext,
) -> Result<Option<LiteralListElementType>, CoreError> {
    if is_abs_function(function) {
        return static_map_abs_element_type(function, variable, source_element_type, context);
    }
    if is_ceil_function(function) {
        return static_map_unary_numeric_float_element_type(
            function,
            variable,
            source_element_type,
            context,
            "ceil",
        );
    }
    if is_floor_function(function) {
        return static_map_unary_numeric_float_element_type(
            function,
            variable,
            source_element_type,
            context,
            "floor",
        );
    }
    if is_round_function(function) {
        return static_map_round_element_type(function, variable, source_element_type, context);
    }
    if is_sqrt_function(function) {
        return static_map_sqrt_element_type(function, variable, source_element_type, context);
    }
    if is_sign_function(function) {
        return static_map_sign_element_type(function, variable, source_element_type, context);
    }
    Ok(None)
}

fn static_map_string_function_returns_string(function: &FunctionInvocation) -> bool {
    is_to_lower_function(function)
        || is_to_upper_function(function)
        || is_trim_function(function)
        || is_ltrim_function(function)
        || is_rtrim_function(function)
        || is_replace_function(function)
        || is_substring_function(function)
        || is_left_function(function)
        || is_right_function(function)
        || is_reverse_function(function)
}

fn static_map_function_argument_element_types(
    function: &FunctionInvocation,
    variable: &str,
    source_element_type: Option<LiteralListElementType>,
    context: &CypherCompileContext,
) -> Result<Vec<Option<LiteralListElementType>>, CoreError> {
    if let Some(argument_sources) = context.function_argument_sources(function) {
        return static_map_function_argument_source_element_types(
            argument_sources,
            variable,
            source_element_type,
            context,
        );
    }

    let variable_argument = context.variable_function_argument_info(function);
    if let Some(argument) = variable_argument {
        if argument.variable != variable {
            return Err(unsupported(
                "list_comprehension.map.arguments",
                format!(
                    "static list comprehension map argument '{}' is not the item variable '{}'",
                    argument.variable, variable
                ),
            ));
        }
        if argument.count != 0
            && (argument.count != function.arguments.len() + 1 || argument.index >= argument.count)
        {
            return Err(unsupported(
                "list_comprehension.map.arguments",
                "static list comprehension map arguments could not be recovered from the parsed AST",
            ));
        }
    }

    let mut element_types = function
        .arguments
        .iter()
        .map(|argument| {
            static_list_comprehension_map_element_type(
                argument,
                variable,
                source_element_type,
                context,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(argument) = variable_argument {
        element_types.insert(argument.index, source_element_type);
    }
    Ok(element_types)
}

fn static_map_function_argument_source_element_types(
    argument_sources: &FunctionArgumentSources,
    variable: &str,
    source_element_type: Option<LiteralListElementType>,
    context: &CypherCompileContext,
) -> Result<Vec<Option<LiteralListElementType>>, CoreError> {
    argument_sources
        .arguments
        .iter()
        .enumerate()
        .map(|(index, source)| {
            let (expression, fragment_context) = parse_cypher_expression_fragment(
                source,
                format!("list_comprehension.map.arguments[{index}]"),
                context,
            )?;
            static_list_comprehension_map_element_type(
                &expression,
                variable,
                source_element_type,
                &fragment_context,
            )
        })
        .collect()
}

fn static_map_single_function_argument_element_type(
    function: &FunctionInvocation,
    variable: &str,
    source_element_type: Option<LiteralListElementType>,
    context: &CypherCompileContext,
    function_name: &str,
) -> Result<Option<LiteralListElementType>, CoreError> {
    let element_types = static_map_function_argument_element_types(
        function,
        variable,
        source_element_type,
        context,
    )?;
    let [argument] = element_types.as_slice() else {
        return Err(unsupported(
            "list_comprehension.map.arguments",
            format!(
                "{function_name}() in static list comprehension maps requires exactly one argument"
            ),
        ));
    };
    Ok(*argument)
}

fn static_map_coalesce_element_type(
    function: &FunctionInvocation,
    variable: &str,
    source_element_type: Option<LiteralListElementType>,
    context: &CypherCompileContext,
) -> Result<Option<LiteralListElementType>, CoreError> {
    let element_types = static_map_function_argument_element_types(
        function,
        variable,
        source_element_type,
        context,
    )?;
    if element_types.len() < 2 {
        return Err(unsupported(
            "list_comprehension.map.arguments",
            "coalesce() in static list comprehension maps requires at least two arguments",
        ));
    }
    let mut expected = None;
    for element_type in element_types {
        expected =
            merge_static_map_element_types(expected, element_type, "coalesce() static map result")?;
    }
    Ok(expected)
}

fn static_map_null_if_element_type(
    function: &FunctionInvocation,
    variable: &str,
    source_element_type: Option<LiteralListElementType>,
    context: &CypherCompileContext,
) -> Result<Option<LiteralListElementType>, CoreError> {
    let element_types = static_map_function_argument_element_types(
        function,
        variable,
        source_element_type,
        context,
    )?;
    let [expression, _] = element_types.as_slice() else {
        return Err(unsupported(
            "list_comprehension.map.arguments",
            "nullIf() in static list comprehension maps requires exactly two arguments",
        ));
    };
    Ok(*expression)
}

fn static_map_unary_numeric_float_element_type(
    function: &FunctionInvocation,
    variable: &str,
    source_element_type: Option<LiteralListElementType>,
    context: &CypherCompileContext,
    function_name: &str,
) -> Result<Option<LiteralListElementType>, CoreError> {
    let argument = static_map_single_function_argument_element_type(
        function,
        variable,
        source_element_type,
        context,
        function_name,
    )?;
    Ok(match argument {
        Some(LiteralListElementType::Integer | LiteralListElementType::Float) => {
            Some(LiteralListElementType::Float)
        }
        _ => None,
    })
}

fn static_map_round_element_type(
    function: &FunctionInvocation,
    variable: &str,
    source_element_type: Option<LiteralListElementType>,
    context: &CypherCompileContext,
) -> Result<Option<LiteralListElementType>, CoreError> {
    let element_types = static_map_function_argument_element_types(
        function,
        variable,
        source_element_type,
        context,
    )?;
    match element_types.as_slice() {
        [Some(LiteralListElementType::Integer | LiteralListElementType::Float)] => {
            Ok(Some(LiteralListElementType::Float))
        }
        [
            Some(LiteralListElementType::Integer | LiteralListElementType::Float),
            Some(LiteralListElementType::Integer) | None,
        ] => Ok(Some(LiteralListElementType::Float)),
        [
            _,
            Some(LiteralListElementType::String | LiteralListElementType::Boolean),
        ] => Err(unsupported(
            "list_comprehension.map.arguments[1]",
            "round() in static list comprehension maps requires integer precision arguments",
        )),
        [_] | [_, _] => Ok(None),
        _ => Err(unsupported(
            "list_comprehension.map.arguments",
            "round() in static list comprehension maps requires exactly one or two arguments",
        )),
    }
}

fn static_map_abs_element_type(
    function: &FunctionInvocation,
    variable: &str,
    source_element_type: Option<LiteralListElementType>,
    context: &CypherCompileContext,
) -> Result<Option<LiteralListElementType>, CoreError> {
    let argument = static_map_single_function_argument_element_type(
        function,
        variable,
        source_element_type,
        context,
        "abs",
    )?;
    Ok(match argument {
        Some(LiteralListElementType::Integer) => Some(LiteralListElementType::Integer),
        Some(LiteralListElementType::Float) => Some(LiteralListElementType::Float),
        _ => None,
    })
}

fn static_map_sqrt_element_type(
    function: &FunctionInvocation,
    variable: &str,
    source_element_type: Option<LiteralListElementType>,
    context: &CypherCompileContext,
) -> Result<Option<LiteralListElementType>, CoreError> {
    let argument = static_map_single_function_argument_element_type(
        function,
        variable,
        source_element_type,
        context,
        "sqrt",
    )?;
    Ok(match argument {
        Some(LiteralListElementType::Integer | LiteralListElementType::Float) => {
            Some(LiteralListElementType::Float)
        }
        _ => None,
    })
}

fn static_map_sign_element_type(
    function: &FunctionInvocation,
    variable: &str,
    source_element_type: Option<LiteralListElementType>,
    context: &CypherCompileContext,
) -> Result<Option<LiteralListElementType>, CoreError> {
    let argument = static_map_single_function_argument_element_type(
        function,
        variable,
        source_element_type,
        context,
        "sign",
    )?;
    Ok(match argument {
        Some(LiteralListElementType::Integer | LiteralListElementType::Float) => {
            Some(LiteralListElementType::Integer)
        }
        _ => None,
    })
}

fn merge_static_map_element_types(
    lhs: Option<LiteralListElementType>,
    rhs: Option<LiteralListElementType>,
    description: &str,
) -> Result<Option<LiteralListElementType>, CoreError> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) if lhs != rhs => Err(unsupported(
            "list_comprehension.map",
            format!("{description} requires compatible non-null element types"),
        )),
        (Some(element_type), _) | (_, Some(element_type)) => Ok(Some(element_type)),
        (None, None) => Ok(None),
    }
}

fn static_map_arithmetic_element_type(
    operator: CypherBinaryOperator,
    lhs: &Expression,
    rhs: &Expression,
    variable: &str,
    source_element_type: Option<LiteralListElementType>,
    context: &CypherCompileContext,
) -> Result<Option<LiteralListElementType>, CoreError> {
    let operator = compile_arithmetic_operator(operator, "list_comprehension.map.operator")?;
    if matches!(
        operator,
        ArithmeticOperator::Divide | ArithmeticOperator::Power
    ) {
        return Ok(Some(LiteralListElementType::Float));
    }
    let left =
        static_list_comprehension_map_element_type(lhs, variable, source_element_type, context)?;
    let right =
        static_list_comprehension_map_element_type(rhs, variable, source_element_type, context)?;
    Ok(match (left, right) {
        (Some(LiteralListElementType::Float), _) | (_, Some(LiteralListElementType::Float)) => {
            Some(LiteralListElementType::Float)
        }
        (Some(LiteralListElementType::Integer), Some(LiteralListElementType::Integer)) => {
            Some(LiteralListElementType::Integer)
        }
        _ => None,
    })
}

fn compile_optional_static_list_slice_value(
    list: &Expression,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<StaticListValue>, CoreError> {
    let path = path.into();
    let Some(value) =
        compile_optional_static_list_value(list, format!("{path}.list"), plan, context)?
    else {
        return Ok(None);
    };
    slice_static_list_value(value, start, end, path, context).map(Some)
}

fn slice_static_list_value(
    mut value: StaticListValue,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<StaticListValue, CoreError> {
    value.literals = compile_list_slice_literals(
        &value.literals,
        start,
        end,
        path,
        context,
        "static list slice bounds require integer literals or scalar integer parameters",
    )?;
    Ok(value)
}

fn compile_optional_static_list_reverse_value(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<StaticListValue>, CoreError> {
    let path = path.into();
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "reverse() requires exactly one argument",
        ));
    };
    let Some(value) = compile_optional_static_list_value(
        argument,
        format!("{path}.arguments[0]"),
        plan,
        context,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(reverse_static_list_value(
        value,
        format!("{path}.arguments[0]"),
    )?))
}

#[derive(Debug)]
enum StaticListCoalesceArgument {
    Null,
    List(StaticListValue),
}

#[derive(Debug)]
struct StaticListCoalesceArguments {
    arguments: Vec<StaticListCoalesceArgument>,
    element_type: Option<LiteralListElementType>,
}

fn compile_optional_static_list_coalesce_arguments(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<StaticListCoalesceArguments>, CoreError> {
    let path = path.into();
    if function.arguments.len() < 2 {
        return Err(unsupported(
            format!("{path}.arguments"),
            "coalesce() requires at least two arguments",
        ));
    }

    let mut arguments = Vec::with_capacity(function.arguments.len());
    let mut element_type = None;
    let mut saw_list = false;
    let mut saw_non_list = false;

    for (index, argument) in function.arguments.iter().enumerate() {
        let argument_path = format!("{path}.arguments[{index}]");
        if let Some(value) =
            compile_optional_static_list_value(argument, argument_path.clone(), plan, context)?
        {
            element_type = merge_static_list_coalesce_element_types(
                element_type,
                value.element_type,
                &argument_path,
            )?;
            saw_list = true;
            arguments.push(StaticListCoalesceArgument::List(value));
        } else if is_static_null_expression(argument, context)? {
            arguments.push(StaticListCoalesceArgument::Null);
        } else {
            saw_non_list = true;
        }
    }

    if !saw_list {
        return Ok(None);
    }
    if saw_non_list {
        return Err(unsupported(
            format!("{path}.arguments"),
            "list-valued coalesce() requires every non-null argument to be a static list",
        ));
    }
    Ok(Some(StaticListCoalesceArguments {
        arguments,
        element_type,
    }))
}

fn require_static_list_coalesce_element_type(
    coalesce: &StaticListCoalesceArguments,
    path: impl Into<String>,
) -> Result<LiteralListElementType, CoreError> {
    coalesce.element_type.ok_or_else(|| {
        unsupported(
            format!("{}.arguments", path.into()),
            "list-valued coalesce() requires at least one non-null list element type",
        )
    })
}

fn is_static_null_expression(
    expression: &Expression,
    context: &CypherCompileContext,
) -> Result<bool, CoreError> {
    match expression {
        Expression::Parenthesized(inner) => is_static_null_expression(inner, context),
        Expression::Literal(CypherLiteral::Null) => Ok(true),
        Expression::Parameter(parameter) => Ok(matches!(
            context.parameter_value(parameter, "coalesce.arguments")?,
            CypherParameterValue::Literal(Literal::Null)
        )),
        _ => Ok(false),
    }
}

fn compile_optional_static_list_coalesce_value(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<StaticListValue>, CoreError> {
    let path = path.into();
    let Some(coalesce) =
        compile_optional_static_list_coalesce_arguments(function, path.clone(), plan, context)?
    else {
        return Ok(None);
    };
    let element_type = require_static_list_coalesce_element_type(&coalesce, path)?;

    let mut first_list = None;
    let mut first_presence = None;
    for argument in coalesce.arguments {
        let StaticListCoalesceArgument::List(value) = argument else {
            continue;
        };
        let value = with_static_list_element_type(value, element_type);
        if first_list.is_none() {
            if value.presence_variable.is_none() {
                return Ok(Some(value));
            }
            first_presence.clone_from(&value.presence_variable);
            first_list = Some(value);
            continue;
        }
        if value.presence_variable != first_presence {
            return Ok(None);
        }
    }
    Ok(first_list)
}

fn compile_optional_static_list_coalesce_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some(coalesce) =
        compile_optional_static_list_coalesce_arguments(function, path.clone(), plan, context)?
    else {
        return Ok(None);
    };
    let element_type = require_static_list_coalesce_element_type(&coalesce, path)?;

    let expressions = coalesce
        .arguments
        .into_iter()
        .map(|argument| match argument {
            StaticListCoalesceArgument::Null => ScalarExpression::Literal(Literal::Null),
            StaticListCoalesceArgument::List(value) => {
                static_list_value_scalar_expression_with_element_type(value, element_type)
            }
        })
        .collect::<Vec<_>>();
    Ok(Some(ScalarExpression::Coalesce { expressions }))
}

#[derive(Debug)]
enum StaticListCaseResult {
    Null,
    List(StaticListValue),
    Coalesce(StaticListCoalesceArguments),
}

fn compile_optional_static_list_case_result(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<StaticListCaseResult>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_case_result(inner, path, plan, context)
        }
        expression if is_static_null_expression(expression, context)? => {
            Ok(Some(StaticListCaseResult::Null))
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => Ok(
            compile_optional_static_list_coalesce_arguments(function, path, plan, context)?
                .map(StaticListCaseResult::Coalesce),
        ),
        expression => Ok(
            compile_optional_static_list_value(expression, path, plan, context)?
                .map(StaticListCaseResult::List),
        ),
    }
}

fn merge_static_list_case_result_element_type(
    lhs: Option<LiteralListElementType>,
    result: &StaticListCaseResult,
    path: &str,
) -> Result<Option<LiteralListElementType>, CoreError> {
    let rhs = match result {
        StaticListCaseResult::Null => None,
        StaticListCaseResult::List(value) => value.element_type,
        StaticListCaseResult::Coalesce(coalesce) => coalesce.element_type,
    };
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) if lhs != rhs => Err(unsupported(
            path,
            "list-valued CASE result branches require compatible non-null list element types",
        )),
        (Some(element_type), _) | (_, Some(element_type)) => Ok(Some(element_type)),
        (None, None) => Ok(None),
    }
}

fn compile_optional_static_list_case_length_scalar_expression(
    case: &CaseExpression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let Some(parts) = compile_optional_static_list_case_parts(case, path, mode, context)? else {
        return Ok(None);
    };
    Ok(Some(ScalarExpression::Case {
        alternatives: parts
            .alternatives
            .into_iter()
            .map(|(when, result)| {
                Ok(ScalarCaseAlternative {
                    when,
                    then: static_list_case_result_length_scalar_expression(result)?,
                })
            })
            .collect::<Result<Vec<_>, CoreError>>()?,
        else_expression: parts
            .default
            .map(static_list_case_result_length_scalar_expression)
            .transpose()?
            .map(Box::new),
    }))
}

fn compile_optional_static_list_case_endpoint_scalar_expression(
    case: &CaseExpression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
    endpoint: ListEndpoint,
) -> Result<Option<ScalarExpression>, CoreError> {
    let Some(parts) = compile_optional_static_list_case_parts(case, path, mode, context)? else {
        return Ok(None);
    };
    Ok(Some(ScalarExpression::Case {
        alternatives: parts
            .alternatives
            .into_iter()
            .map(|(when, result)| ScalarCaseAlternative {
                when,
                then: static_list_case_result_endpoint_scalar_expression(result, endpoint),
            })
            .collect(),
        else_expression: parts
            .default
            .map(|result| static_list_case_result_endpoint_scalar_expression(result, endpoint))
            .map(Box::new),
    }))
}

fn compile_optional_static_list_case_is_empty_scalar_expression(
    case: &CaseExpression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let Some(parts) = compile_optional_static_list_case_parts(case, path, mode, context)? else {
        return Ok(None);
    };
    Ok(Some(ScalarExpression::Case {
        alternatives: parts
            .alternatives
            .into_iter()
            .map(|(when, result)| ScalarCaseAlternative {
                when,
                then: static_list_case_result_is_empty_scalar_expression(result),
            })
            .collect(),
        else_expression: parts
            .default
            .map(static_list_case_result_is_empty_scalar_expression)
            .map(Box::new),
    }))
}

fn compile_optional_static_list_coalesce_length_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some(coalesce) =
        compile_optional_static_list_coalesce_arguments(function, path, plan, context)?
    else {
        return Ok(None);
    };
    static_list_coalesce_length_scalar_expression(coalesce).map(Some)
}

fn compile_optional_static_list_tail_value(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<StaticListValue>, CoreError> {
    let path = path.into();
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "tail() requires exactly one list argument",
        ));
    };
    let Some(value) = compile_optional_static_list_value(
        argument,
        format!("{path}.arguments[0]"),
        plan,
        context,
    )?
    else {
        return Err(unsupported(
            format!("{path}.arguments[0]"),
            "tail() requires a literal list, list parameter, static split(...), range(...), or static labels()/keys() metadata list",
        ));
    };
    Ok(Some(tail_static_list_value(
        value,
        format!("{path}.arguments[0]"),
    )?))
}

fn reverse_static_list_value(
    value: StaticListValue,
    path: impl Into<String>,
) -> Result<StaticListValue, CoreError> {
    let path = path.into();
    let Some(element_type) = value.element_type else {
        return Err(unsupported(
            path,
            "reverse() requires a list with a known non-null element type",
        ));
    };
    let mut literals = value.literals;
    literals.reverse();
    Ok(StaticListValue {
        presence_variable: value.presence_variable,
        literals,
        element_type: Some(element_type),
    })
}

fn compile_optional_static_list_concat_value(
    lhs: &Expression,
    rhs: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<StaticListValue>, CoreError> {
    let path = path.into();
    let lhs_value = compile_optional_static_list_value(lhs, format!("{path}.lhs"), plan, context)?;
    let rhs_value = compile_optional_static_list_value(rhs, format!("{path}.rhs"), plan, context)?;
    match (lhs_value, rhs_value) {
        (Some(lhs_value), Some(rhs_value)) => {
            Ok(Some(concat_static_list_values(lhs_value, rhs_value, path)?))
        }
        (Some(lhs_value), None) => {
            let Some(rhs_literal) =
                compile_optional_static_list_concat_literal(rhs, format!("{path}.rhs"), context)?
            else {
                return Err(unsupported(
                    path,
                    "static list concatenation with a list left operand requires the right operand to be a literal list, list parameter, static split(...), range(...), tail(...), static labels()/keys() metadata list, scalar literal, or scalar parameter",
                ));
            };
            Ok(Some(append_literal_to_static_list_value(
                lhs_value,
                rhs_literal,
                path,
            )?))
        }
        (None, Some(rhs_value)) => {
            let Some(lhs_literal) =
                compile_optional_static_list_concat_literal(lhs, format!("{path}.lhs"), context)?
            else {
                return Err(unsupported(
                    path,
                    "static list concatenation with a list right operand requires the left operand to be a literal list, list parameter, static split(...), range(...), tail(...), static labels()/keys() metadata list, scalar literal, or scalar parameter",
                ));
            };
            Ok(Some(prepend_literal_to_static_list_value(
                rhs_value,
                lhs_literal,
                path,
            )?))
        }
        (None, None) => Ok(None),
    }
}

fn concat_static_list_values(
    lhs: StaticListValue,
    rhs: StaticListValue,
    path: impl Into<String>,
) -> Result<StaticListValue, CoreError> {
    let path = path.into();
    validate_static_list_concat_operand(&lhs, format!("{path}.lhs"))?;
    validate_static_list_concat_operand(&rhs, format!("{path}.rhs"))?;
    let presence_variable =
        merge_static_list_presence_variables(lhs.presence_variable, rhs.presence_variable, &path)?;
    let element_type = merge_static_list_element_types(lhs.element_type, rhs.element_type, &path)?;
    let mut literals = lhs.literals;
    literals.extend(rhs.literals);
    Ok(StaticListValue {
        presence_variable,
        literals,
        element_type,
    })
}

fn compile_optional_static_list_concat_literal(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Option<Literal>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_concat_literal(inner, path, context)
        }
        expression if is_literal_expression(expression) => {
            compile_literal(expression, path, context).map(Some)
        }
        _ => Ok(None),
    }
}

fn append_literal_to_static_list_value(
    mut value: StaticListValue,
    literal: Literal,
    path: impl Into<String>,
) -> Result<StaticListValue, CoreError> {
    let path = path.into();
    validate_static_list_concat_operand(&value, format!("{path}.lhs"))?;
    value.element_type =
        merge_static_list_element_type_with_literal(value.element_type, &literal, &path)?;
    value.literals.push(literal);
    Ok(value)
}

fn prepend_literal_to_static_list_value(
    mut value: StaticListValue,
    literal: Literal,
    path: impl Into<String>,
) -> Result<StaticListValue, CoreError> {
    let path = path.into();
    validate_static_list_concat_operand(&value, format!("{path}.rhs"))?;
    value.element_type =
        merge_static_list_element_type_with_literal(value.element_type, &literal, &path)?;
    value.literals.insert(0, literal);
    Ok(value)
}

fn validate_static_list_concat_operand(
    value: &StaticListValue,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    if value.element_type.is_none()
        && infer_literal_list_element_type(&value.literals).is_none()
        && value
            .literals
            .iter()
            .any(|literal| literal_list_element_kind(literal).is_some())
    {
        return Err(unsupported(
            path,
            "static list concatenation requires each operand to have a single non-null element type",
        ));
    }
    Ok(())
}

fn merge_static_list_element_type_with_literal(
    list_element_type: Option<LiteralListElementType>,
    literal: &Literal,
    path: &str,
) -> Result<Option<LiteralListElementType>, CoreError> {
    let literal_type = literal_list_element_kind(literal);
    match (list_element_type, literal_type) {
        (Some(list_element_type), Some(literal_type)) if list_element_type != literal_type => {
            Err(unsupported(
                path,
                "static list concatenation requires compatible non-null element types",
            ))
        }
        (Some(element_type), _) | (_, Some(element_type)) => Ok(Some(element_type)),
        (None, None) => Ok(None),
    }
}

fn merge_static_list_presence_variables(
    lhs: Option<String>,
    rhs: Option<String>,
    path: &str,
) -> Result<Option<String>, CoreError> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) if lhs != rhs => Err(unsupported(
            path,
            "static list concatenation across different optional graph bindings is not supported yet",
        )),
        (Some(variable), _) | (_, Some(variable)) => Ok(Some(variable)),
        (None, None) => Ok(None),
    }
}

fn merge_static_list_element_types(
    lhs: Option<LiteralListElementType>,
    rhs: Option<LiteralListElementType>,
    path: &str,
) -> Result<Option<LiteralListElementType>, CoreError> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) if lhs != rhs => Err(unsupported(
            path,
            "static list concatenation requires compatible non-null element types",
        )),
        (Some(element_type), _) | (_, Some(element_type)) => Ok(Some(element_type)),
        (None, None) => Ok(None),
    }
}

fn merge_static_list_coalesce_element_types(
    lhs: Option<LiteralListElementType>,
    rhs: Option<LiteralListElementType>,
    path: &str,
) -> Result<Option<LiteralListElementType>, CoreError> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) if lhs != rhs => Err(unsupported(
            path,
            "list-valued coalesce() requires compatible non-null list element types",
        )),
        (Some(element_type), _) | (_, Some(element_type)) => Ok(Some(element_type)),
        (None, None) => Ok(None),
    }
}

fn with_static_list_element_type(
    mut value: StaticListValue,
    element_type: LiteralListElementType,
) -> StaticListValue {
    if value.element_type.is_none() {
        value.element_type = Some(element_type);
    }
    value
}

fn infer_literal_list_element_type(literals: &[Literal]) -> Option<LiteralListElementType> {
    let mut expected = None;
    for literal in literals {
        let Some(kind) = literal_list_element_kind(literal) else {
            continue;
        };
        match expected {
            Some(expected) if expected != kind => return None,
            Some(_) => {}
            None => expected = Some(kind),
        }
    }
    expected
}

fn static_list_value_scalar_expression(
    value: StaticListValue,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let Some(element_type) = value.element_type else {
        return Err(unsupported(
            path,
            "static list expressions require a known non-null element type",
        ));
    };
    Ok(presence_gate_scalar_expression(
        value.presence_variable,
        ScalarExpression::TypedLiteralList {
            literals: value.literals,
            element_type,
        },
    ))
}

fn compile_optional_static_list_case_slice_scalar_expression(
    list: &Expression,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_case_slice_scalar_expression(
                inner, start, end, path, plan, context,
            )
        }
        Expression::Case(case) => {
            let Some(parts) = compile_optional_static_list_case_parts(
                case,
                format!("{path}.list"),
                PredicateCompileMode::CaseWhen { plan },
                context,
            )?
            else {
                return Ok(None);
            };
            let element_type = require_static_list_case_element_type(&parts, path.clone())?;
            Ok(Some(ScalarExpression::Case {
                alternatives: parts
                    .alternatives
                    .into_iter()
                    .enumerate()
                    .map(|(alternative_index, (when, result))| {
                        Ok(ScalarCaseAlternative {
                            when,
                            then: static_list_case_result_slice_scalar_expression(
                                result,
                                start,
                                end,
                                format!("{path}.list.alternatives[{alternative_index}].then"),
                                element_type,
                                context,
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>, CoreError>>()?,
                else_expression: parts
                    .default
                    .map(|result| {
                        static_list_case_result_slice_scalar_expression(
                            result,
                            start,
                            end,
                            format!("{path}.list.default"),
                            element_type,
                            context,
                        )
                        .map(Box::new)
                    })
                    .transpose()?,
            }))
        }
        _ => Ok(None),
    }
}

fn compile_optional_static_list_coalesce_slice_scalar_expression(
    list: &Expression,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_coalesce_slice_scalar_expression(
                inner, start, end, path, plan, context,
            )
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
                function,
                format!("{path}.list"),
                plan,
                context,
            )?
            else {
                return Ok(None);
            };
            static_list_coalesce_slice_scalar_expression(coalesce, start, end, path, context)
                .map(Some)
        }
        _ => Ok(None),
    }
}

fn static_list_case_result_slice_scalar_expression(
    result: StaticListCaseResult,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    element_type: LiteralListElementType,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match result {
        StaticListCaseResult::Null => Ok(ScalarExpression::Literal(Literal::Null)),
        StaticListCaseResult::List(value) => {
            static_list_value_slice_scalar_expression_with_element_type(
                value,
                start,
                end,
                path,
                element_type,
                context,
            )
        }
        StaticListCaseResult::Coalesce(coalesce) => {
            static_list_coalesce_slice_scalar_expression_with_element_type(
                coalesce,
                start,
                end,
                path,
                element_type,
                context,
            )
        }
    }
}

fn static_list_coalesce_slice_scalar_expression(
    coalesce: StaticListCoalesceArguments,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let element_type = require_static_list_coalesce_element_type(&coalesce, path.clone())?;
    static_list_coalesce_slice_scalar_expression_with_element_type(
        coalesce,
        start,
        end,
        path,
        element_type,
        context,
    )
}

fn static_list_coalesce_slice_scalar_expression_with_element_type(
    coalesce: StaticListCoalesceArguments,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    element_type: LiteralListElementType,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let mut expression = ScalarExpression::Literal(Literal::Null);
    for (argument_index, argument) in coalesce.arguments.into_iter().enumerate().rev() {
        let StaticListCoalesceArgument::List(mut value) = argument else {
            continue;
        };
        let presence_variable = value.presence_variable.take();
        let sliced = static_list_value_slice_scalar_expression_with_element_type(
            value,
            start,
            end,
            format!("{path}.arguments[{argument_index}]"),
            element_type,
            context,
        )?;
        expression = match presence_variable {
            Some(variable) => ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: PredicateExpression::Presence(PresencePredicate {
                        variable,
                        operator: ComparisonOperator::NotEqual,
                    }),
                    then: sliced,
                }],
                else_expression: Some(Box::new(expression)),
            },
            None => sliced,
        };
    }
    Ok(expression)
}

fn static_list_value_slice_scalar_expression_with_element_type(
    value: StaticListValue,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    element_type: LiteralListElementType,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let value = slice_static_list_value(value, start, end, path, context)?;
    Ok(static_list_value_scalar_expression_with_element_type(
        value,
        element_type,
    ))
}

fn compile_optional_static_list_case_slice_length_scalar_expression(
    list: &Expression,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_case_slice_length_scalar_expression(
                inner, start, end, path, mode, context,
            )
        }
        Expression::Case(case) => {
            let Some(parts) = compile_optional_static_list_case_parts(
                case,
                format!("{path}.list"),
                mode,
                context,
            )?
            else {
                return Ok(None);
            };
            Ok(Some(ScalarExpression::Case {
                alternatives: parts
                    .alternatives
                    .into_iter()
                    .enumerate()
                    .map(|(alternative_index, (when, result))| {
                        Ok(ScalarCaseAlternative {
                            when,
                            then: static_list_case_result_slice_length_scalar_expression(
                                result,
                                start,
                                end,
                                format!("{path}.list.alternatives[{alternative_index}].then"),
                                context,
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>, CoreError>>()?,
                else_expression: parts
                    .default
                    .map(|result| {
                        static_list_case_result_slice_length_scalar_expression(
                            result,
                            start,
                            end,
                            format!("{path}.list.default"),
                            context,
                        )
                        .map(Box::new)
                    })
                    .transpose()?,
            }))
        }
        _ => Ok(None),
    }
}

fn compile_optional_static_list_coalesce_slice_length_scalar_expression(
    list: &Expression,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_coalesce_slice_length_scalar_expression(
                inner, start, end, path, plan, context,
            )
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
                function,
                format!("{path}.list"),
                plan,
                context,
            )?
            else {
                return Ok(None);
            };
            static_list_coalesce_slice_length_scalar_expression(coalesce, start, end, path, context)
                .map(Some)
        }
        _ => Ok(None),
    }
}

fn static_list_case_result_slice_length_scalar_expression(
    result: StaticListCaseResult,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    match result {
        StaticListCaseResult::Null => Ok(ScalarExpression::Literal(Literal::Null)),
        StaticListCaseResult::List(value) => {
            static_list_value_slice_length_scalar_expression(value, start, end, path, context)
        }
        StaticListCaseResult::Coalesce(coalesce) => {
            static_list_coalesce_slice_length_scalar_expression(coalesce, start, end, path, context)
        }
    }
}

fn static_list_coalesce_slice_length_scalar_expression(
    coalesce: StaticListCoalesceArguments,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let mut expression = ScalarExpression::Literal(Literal::Null);
    for (argument_index, argument) in coalesce.arguments.into_iter().enumerate().rev() {
        let StaticListCoalesceArgument::List(mut value) = argument else {
            continue;
        };
        let presence_variable = value.presence_variable.take();
        let length = static_list_value_slice_length_scalar_expression(
            value,
            start,
            end,
            format!("{path}.arguments[{argument_index}]"),
            context,
        )?;
        expression = match presence_variable {
            Some(variable) => ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: PredicateExpression::Presence(PresencePredicate {
                        variable,
                        operator: ComparisonOperator::NotEqual,
                    }),
                    then: length,
                }],
                else_expression: Some(Box::new(expression)),
            },
            None => length,
        };
    }
    Ok(expression)
}

fn static_list_value_slice_length_scalar_expression(
    value: StaticListValue,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let value = slice_static_list_value(value, start, end, path, context)?;
    static_list_length_scalar_expression(value)
}

fn compile_optional_static_list_case_slice_endpoint_scalar_expression(
    list: &Expression,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
    endpoint: ListEndpoint,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_case_slice_endpoint_scalar_expression(
                inner, start, end, path, mode, context, endpoint,
            )
        }
        Expression::Case(case) => {
            let Some(parts) = compile_optional_static_list_case_parts(
                case,
                format!("{path}.list"),
                mode,
                context,
            )?
            else {
                return Ok(None);
            };
            Ok(Some(ScalarExpression::Case {
                alternatives: parts
                    .alternatives
                    .into_iter()
                    .enumerate()
                    .map(|(alternative_index, (when, result))| {
                        Ok(ScalarCaseAlternative {
                            when,
                            then: static_list_case_result_slice_endpoint_scalar_expression(
                                result,
                                start,
                                end,
                                format!("{path}.list.alternatives[{alternative_index}].then"),
                                context,
                                endpoint,
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>, CoreError>>()?,
                else_expression: parts
                    .default
                    .map(|result| {
                        static_list_case_result_slice_endpoint_scalar_expression(
                            result,
                            start,
                            end,
                            format!("{path}.list.default"),
                            context,
                            endpoint,
                        )
                        .map(Box::new)
                    })
                    .transpose()?,
            }))
        }
        _ => Ok(None),
    }
}

fn compile_optional_static_list_coalesce_slice_endpoint_scalar_expression(
    list: &Expression,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
    endpoint: ListEndpoint,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_coalesce_slice_endpoint_scalar_expression(
                inner, start, end, path, plan, context, endpoint,
            )
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
                function,
                format!("{path}.list"),
                plan,
                context,
            )?
            else {
                return Ok(None);
            };
            Ok(Some(static_list_coalesce_slice_endpoint_scalar_expression(
                coalesce, start, end, path, context, endpoint,
            )?))
        }
        _ => Ok(None),
    }
}

fn static_list_case_result_slice_endpoint_scalar_expression(
    result: StaticListCaseResult,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    context: &CypherCompileContext,
    endpoint: ListEndpoint,
) -> Result<ScalarExpression, CoreError> {
    match result {
        StaticListCaseResult::Null => Ok(ScalarExpression::Literal(Literal::Null)),
        StaticListCaseResult::List(value) => static_list_value_slice_endpoint_scalar_expression(
            value, start, end, path, context, endpoint,
        ),
        StaticListCaseResult::Coalesce(coalesce) => {
            static_list_coalesce_slice_endpoint_scalar_expression(
                coalesce, start, end, path, context, endpoint,
            )
        }
    }
}

fn static_list_coalesce_slice_endpoint_scalar_expression(
    coalesce: StaticListCoalesceArguments,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    context: &CypherCompileContext,
    endpoint: ListEndpoint,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let mut expression = ScalarExpression::Literal(Literal::Null);
    for (argument_index, argument) in coalesce.arguments.into_iter().enumerate().rev() {
        let StaticListCoalesceArgument::List(mut value) = argument else {
            continue;
        };
        let presence_variable = value.presence_variable.take();
        let endpoint_expression = static_list_value_slice_endpoint_scalar_expression(
            value,
            start,
            end,
            format!("{path}.arguments[{argument_index}]"),
            context,
            endpoint,
        )?;
        expression = match presence_variable {
            Some(variable) => ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: PredicateExpression::Presence(PresencePredicate {
                        variable,
                        operator: ComparisonOperator::NotEqual,
                    }),
                    then: endpoint_expression,
                }],
                else_expression: Some(Box::new(expression)),
            },
            None => endpoint_expression,
        };
    }
    Ok(expression)
}

fn static_list_value_slice_endpoint_scalar_expression(
    value: StaticListValue,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    context: &CypherCompileContext,
    endpoint: ListEndpoint,
) -> Result<ScalarExpression, CoreError> {
    let value = slice_static_list_value(value, start, end, path, context)?;
    Ok(static_list_value_endpoint_scalar_expression(
        value, endpoint,
    ))
}

fn compile_optional_static_list_case_slice_is_empty_scalar_expression(
    list: &Expression,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_case_slice_is_empty_scalar_expression(
                inner, start, end, path, mode, context,
            )
        }
        Expression::Case(case) => {
            let Some(parts) = compile_optional_static_list_case_parts(
                case,
                format!("{path}.list"),
                mode,
                context,
            )?
            else {
                return Ok(None);
            };
            Ok(Some(ScalarExpression::Case {
                alternatives: parts
                    .alternatives
                    .into_iter()
                    .enumerate()
                    .map(|(alternative_index, (when, result))| {
                        Ok(ScalarCaseAlternative {
                            when,
                            then: static_list_case_result_slice_is_empty_scalar_expression(
                                result,
                                start,
                                end,
                                format!("{path}.list.alternatives[{alternative_index}].then"),
                                context,
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>, CoreError>>()?,
                else_expression: parts
                    .default
                    .map(|result| {
                        static_list_case_result_slice_is_empty_scalar_expression(
                            result,
                            start,
                            end,
                            format!("{path}.list.default"),
                            context,
                        )
                        .map(Box::new)
                    })
                    .transpose()?,
            }))
        }
        _ => Ok(None),
    }
}

fn compile_optional_static_list_coalesce_slice_is_empty_scalar_expression(
    list: &Expression,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_coalesce_slice_is_empty_scalar_expression(
                inner, start, end, path, plan, context,
            )
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
                function,
                format!("{path}.list"),
                plan,
                context,
            )?
            else {
                return Ok(None);
            };
            Ok(Some(static_list_coalesce_slice_is_empty_scalar_expression(
                coalesce, start, end, path, context,
            )?))
        }
        _ => Ok(None),
    }
}

fn static_list_case_result_slice_is_empty_scalar_expression(
    result: StaticListCaseResult,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    match result {
        StaticListCaseResult::Null => Ok(ScalarExpression::Literal(Literal::Null)),
        StaticListCaseResult::List(value) => Ok(
            static_list_value_slice_is_empty_scalar_expression(value, start, end, path, context)?,
        ),
        StaticListCaseResult::Coalesce(coalesce) => {
            static_list_coalesce_slice_is_empty_scalar_expression(
                coalesce, start, end, path, context,
            )
        }
    }
}

fn static_list_coalesce_slice_is_empty_scalar_expression(
    coalesce: StaticListCoalesceArguments,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let mut expression = ScalarExpression::Literal(Literal::Null);
    for (argument_index, argument) in coalesce.arguments.into_iter().enumerate().rev() {
        let StaticListCoalesceArgument::List(mut value) = argument else {
            continue;
        };
        let presence_variable = value.presence_variable.take();
        let is_empty = static_list_value_slice_is_empty_scalar_expression(
            value,
            start,
            end,
            format!("{path}.arguments[{argument_index}]"),
            context,
        )?;
        expression = match presence_variable {
            Some(variable) => ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: PredicateExpression::Presence(PresencePredicate {
                        variable,
                        operator: ComparisonOperator::NotEqual,
                    }),
                    then: is_empty,
                }],
                else_expression: Some(Box::new(expression)),
            },
            None => is_empty,
        };
    }
    Ok(expression)
}

fn static_list_value_slice_is_empty_scalar_expression(
    value: StaticListValue,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let value = slice_static_list_value(value, start, end, path, context)?;
    Ok(static_list_is_empty_scalar_expression(value))
}

fn compile_optional_static_list_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    if let Expression::ListComprehension(comprehension) = expression
        && let Some(expression) = compile_optional_static_list_comprehension_scalar_expression(
            comprehension,
            path.clone(),
            plan,
            context,
        )?
    {
        return Ok(Some(expression));
    }
    if let Expression::ListSlice {
        list, start, end, ..
    } = expression
    {
        if let Some(expression) = compile_optional_static_list_case_slice_scalar_expression(
            list,
            start.as_deref(),
            end.as_deref(),
            path.clone(),
            plan,
            context,
        )? {
            return Ok(Some(expression));
        }
        if let Some(expression) = compile_optional_static_list_coalesce_slice_scalar_expression(
            list,
            start.as_deref(),
            end.as_deref(),
            path.clone(),
            plan,
            context,
        )? {
            return Ok(Some(expression));
        }
    }
    if let Expression::FunctionCall(function) = expression
        && is_coalesce_function(function)
    {
        return compile_optional_static_list_coalesce_scalar_expression(
            function, path, plan, context,
        );
    }
    let Some(value) = compile_optional_static_list_value(expression, path.clone(), plan, context)?
    else {
        return Ok(None);
    };
    Ok(Some(static_list_value_scalar_expression(value, path)?))
}

fn static_list_tail_expression(
    value: StaticListValue,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let value = tail_static_list_value(value, path.clone())?;
    static_list_value_scalar_expression(value, path)
}

fn tail_static_list_value(
    value: StaticListValue,
    path: impl Into<String>,
) -> Result<StaticListValue, CoreError> {
    let path = path.into();
    let Some(element_type) = value.element_type else {
        return Err(unsupported(
            path,
            "tail() requires a list with a known non-null element type",
        ));
    };
    let literals = value.literals.into_iter().skip(1).collect::<Vec<_>>();
    Ok(StaticListValue {
        presence_variable: value.presence_variable,
        literals,
        element_type: Some(element_type),
    })
}

fn compile_relationship_type_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let variable = compile_single_variable_function_argument(
        function,
        format!("{path}.arguments"),
        "type() supports exactly one relationship variable argument",
        context,
    )?;
    let relationship = plan
        .relationships
        .iter()
        .find(|relationship| relationship.variable.as_deref() == Some(variable.as_str()))
        .ok_or_else(|| {
            unsupported(
                format!("{path}.arguments[0]"),
                format!("type() argument '{variable}' is not a named relationship variable"),
            )
        })?;
    Ok(ScalarExpression::RelationshipType {
        variable,
        relationship_type: relationship.relationship_type.clone(),
    })
}

fn validate_variable(variable: &Variable) -> Result<String, CoreError> {
    let name = variable_name(variable);
    if is_internal_graph_variable(&name) {
        return Err(unsupported(
            "variable",
            "variables beginning with __coral_ are reserved for virtual graph planning",
        ));
    }
    Ok(name)
}

fn is_internal_graph_variable(variable: &str) -> bool {
    variable.starts_with("__coral_")
}

fn fresh_internal_node_variable(plan: &GraphPlan, part_index: usize, node_index: usize) -> String {
    fresh_internal_node_variable_avoiding(plan, part_index, node_index, "")
}

fn fresh_internal_node_variable_avoiding(
    plan: &GraphPlan,
    part_index: usize,
    node_index: usize,
    avoid: &str,
) -> String {
    let mut suffix = 0;
    loop {
        let candidate = if suffix == 0 {
            format!("__coral_node_{part_index}_{node_index}")
        } else {
            format!("__coral_node_{part_index}_{node_index}_{suffix}")
        };
        if candidate != avoid && !plan_uses_variable(plan, &candidate) {
            return candidate;
        }
        suffix += 1;
    }
}

fn fresh_internal_relationship_variable(
    plan: &GraphPlan,
    next_node_variable: &str,
    index: usize,
) -> String {
    let mut suffix = 0;
    loop {
        let candidate = if suffix == 0 {
            format!("__coral_rel_{index}")
        } else {
            format!("__coral_rel_{index}_{suffix}")
        };
        if !plan_uses_variable(plan, &candidate) && next_node_variable != candidate {
            return candidate;
        }
        suffix += 1;
    }
}

fn fresh_hidden_graph_variable(
    plan: &GraphPlan,
    state: &CypherCompileState,
    variable: &str,
) -> String {
    let mut suffix = 0;
    loop {
        let candidate = if suffix == 0 {
            format!("__coral_hidden_{variable}")
        } else {
            format!("__coral_hidden_{variable}_{suffix}")
        };
        if !plan_uses_variable(plan, &candidate)
            && !state.hidden_graph_variables.contains(&candidate)
        {
            return candidate;
        }
        suffix += 1;
    }
}

fn plan_uses_variable(plan: &GraphPlan, candidate: &str) -> bool {
    plan.nodes.iter().any(|node| node.variable == candidate)
        || plan
            .relationships
            .iter()
            .any(|relationship| relationship.variable.as_deref() == Some(candidate))
}

fn single_static_label(
    labels: &[LabelExpression],
    path: impl Into<String>,
) -> Result<String, CoreError> {
    single_resolved_label(labels, path, LabelExpressionResolver::StaticOnly)
}

fn single_compile_time_label(
    labels: &[LabelExpression],
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    single_resolved_label(
        labels,
        path,
        LabelExpressionResolver::CompileTimeDynamic { context },
    )
}

fn single_resolved_label(
    labels: &[LabelExpression],
    path: impl Into<String>,
    resolver: LabelExpressionResolver<'_>,
) -> Result<String, CoreError> {
    let path = path.into();
    if labels.is_empty() {
        return Err(unsupported(
            path,
            "exactly one positive static label or relationship type is required",
        ));
    }

    let mut required = BTreeSet::new();
    let mut forbidden = BTreeSet::new();
    for (index, label) in labels.iter().enumerate() {
        collect_label_requirements(
            label,
            &mut required,
            &mut forbidden,
            format!("{path}[{index}]"),
            resolver,
        )?;
    }

    let mut required_labels = required.iter();
    let Some(label) = required_labels.next() else {
        return Err(unsupported(
            path,
            "node and relationship patterns require exactly one positive static label or relationship type",
        ));
    };
    if required_labels.next().is_some() {
        return Err(unsupported(
            path,
            "node and relationship patterns require exactly one positive static label or relationship type",
        ));
    }
    if forbidden.contains(label) {
        return Err(unsupported(
            path,
            "contradictory label expressions cannot be represented by one Coral mapping",
        ));
    }
    for (index, expression) in labels.iter().enumerate() {
        if !evaluate_label_expression(expression, label, format!("{path}[{index}]"), resolver)? {
            return Err(unsupported(
                path,
                "contradictory label expressions cannot be represented by one Coral mapping",
            ));
        }
    }
    Ok((*label).clone())
}

fn collect_label_requirements(
    expression: &LabelExpression,
    required: &mut BTreeSet<String>,
    forbidden: &mut BTreeSet<String>,
    path: impl Into<String>,
    resolver: LabelExpressionResolver<'_>,
) -> Result<(), CoreError> {
    let path = path.into();
    match expression {
        LabelExpression::Static(name) => {
            required.insert(name.name.clone());
            Ok(())
        }
        LabelExpression::Dynamic { expression, .. } => {
            required.insert(resolver.resolve_dynamic(expression, path)?);
            Ok(())
        }
        LabelExpression::Or { .. } => Err(unsupported(
            path,
            "label/type alternatives require union planning and are not supported yet",
        )),
        LabelExpression::And { lhs, rhs, .. } => {
            collect_label_requirements(lhs, required, forbidden, format!("{path}.lhs"), resolver)?;
            collect_label_requirements(rhs, required, forbidden, format!("{path}.rhs"), resolver)
        }
        LabelExpression::Not { inner, .. } => {
            collect_label_exclusion(inner, forbidden, format!("{path}.inner"), resolver)
        }
        LabelExpression::Group { inner, .. } => {
            collect_label_requirements(inner, required, forbidden, path, resolver)
        }
    }
}

fn collect_label_exclusion(
    expression: &LabelExpression,
    forbidden: &mut BTreeSet<String>,
    path: impl Into<String>,
    resolver: LabelExpressionResolver<'_>,
) -> Result<(), CoreError> {
    let path = path.into();
    match expression {
        LabelExpression::Static(name) => {
            forbidden.insert(name.name.clone());
            Ok(())
        }
        LabelExpression::Dynamic { expression, .. } => {
            forbidden.insert(resolver.resolve_dynamic(expression, path)?);
            Ok(())
        }
        LabelExpression::Group { inner, .. } => {
            collect_label_exclusion(inner, forbidden, path, resolver)
        }
        LabelExpression::And { lhs, rhs, .. } | LabelExpression::Or { lhs, rhs, .. } => {
            validate_label_expression(lhs, format!("{path}.lhs"), resolver)?;
            validate_label_expression(rhs, format!("{path}.rhs"), resolver)
        }
        LabelExpression::Not { inner, .. } => {
            validate_label_expression(inner, format!("{path}.inner"), resolver)
        }
    }
}

fn validate_label_expression(
    expression: &LabelExpression,
    path: impl Into<String>,
    resolver: LabelExpressionResolver<'_>,
) -> Result<(), CoreError> {
    let path = path.into();
    match expression {
        LabelExpression::Static(_) => Ok(()),
        LabelExpression::Dynamic { expression, .. } => {
            resolver.resolve_dynamic(expression, path).map(|_| ())
        }
        LabelExpression::Group { inner, .. } => validate_label_expression(inner, path, resolver),
        LabelExpression::And { lhs, rhs, .. } | LabelExpression::Or { lhs, rhs, .. } => {
            validate_label_expression(lhs, format!("{path}.lhs"), resolver)?;
            validate_label_expression(rhs, format!("{path}.rhs"), resolver)
        }
        LabelExpression::Not { inner, .. } => {
            validate_label_expression(inner, format!("{path}.inner"), resolver)
        }
    }
}

fn optional_single_compile_time_label(
    labels: &[LabelExpression],
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Option<String>, CoreError> {
    if labels.is_empty() {
        return Ok(None);
    }
    single_compile_time_label(labels, path, context).map(Some)
}

fn variable_name(variable: &Variable) -> String {
    variable.name.name.clone()
}

fn unsupported(path: impl Into<String>, message: impl Into<String>) -> CoreError {
    Diagnostic::new(diagnostic_codes::UNSUPPORTED_CYPHER, path, message).into_core_error()
}

#[path = "cypher_tests.rs"]
#[cfg(test)]
mod tests;
