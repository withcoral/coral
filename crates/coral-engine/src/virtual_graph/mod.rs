//! Virtual graph declarations, logical plans, and SQL lowering.
//!
//! This module is intentionally small at first: frontend parsers such as Cypher
//! and GraphQL should compile into [`GraphPlan`], and only the SQL lowerer knows
//! how to turn graph semantics into relational `DataFusion` SQL.

mod cypher;
mod declaration;
mod diagnostic;
mod diagnostic_codes;
mod execution;
mod graphql;
mod graphql_aggregate;
mod graphql_schema;
mod ir;
mod sql;
mod validation;

pub use cypher::{
    CypherParameterValue, compile_cypher, compile_cypher_for_graph,
    compile_cypher_for_graph_with_parameters, compile_cypher_query, compile_cypher_query_for_graph,
    compile_cypher_query_for_graph_with_parameters, compile_cypher_query_with_parameters,
    compile_cypher_with_parameters,
};
pub use declaration::{Declaration, Endpoint, Node, Relationship, TableRef};
pub use diagnostic::Diagnostic;
pub use execution::{GraphExecution, GraphQueryPlan};
pub use graphql::{
    GraphqlCapability, GraphqlCapabilitySurface, GraphqlRejectionPath, GraphqlVariableValue,
    compile_graphql, compile_graphql_for_graph, compile_graphql_for_graph_with_operation_name,
    compile_graphql_for_graph_with_variables,
    compile_graphql_for_graph_with_variables_and_operation_name,
    compile_graphql_with_operation_name, compile_graphql_with_variables,
    compile_graphql_with_variables_and_operation_name, graphql_read_capability_surface,
};
pub use graphql_schema::graphql_schema_sdl_for_graph;
pub use ir::{
    AggregateFunction, AggregateTarget, ComparisonOperator, Direction, ElementIdPredicate,
    GraphPlan, GraphQuery, GraphStage, GraphStageExport, GraphStagedQuery, GraphUnion,
    GraphUnionBranch, GraphUnionOuterProjection, GraphUnionOuterProjectionItem, KeyPredicate,
    Literal, NodePattern, NullOrder, OptionalMatchScope, OrderDirection, OrderExpression, OrderKey,
    PredicateExpression, PredicateRhs, PresencePredicate, Projection, ProjectionPredicate,
    ProjectionPredicateExpression, ProjectionPredicateRhs, PropertyPredicate, PropertyRef,
    RelationshipPattern, UndirectedRelationshipEndpoint,
};
pub use sql::SqlTranslation;
