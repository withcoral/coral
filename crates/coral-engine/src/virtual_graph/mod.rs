//! Virtual graph declarations, logical plans, and SQL lowering.
//!
//! This module is intentionally small at first: frontend parsers such as Cypher
//! and GraphQL should compile into [`GraphPlan`], and only the SQL lowerer knows
//! how to turn graph semantics into relational `DataFusion` SQL.

mod cypher;
mod declaration;
mod diagnostic;
mod execution;
mod ir;
mod sql;

pub use cypher::compile_cypher;
pub use declaration::{Declaration, Endpoint, Node, Relationship, TableRef};
pub use diagnostic::Diagnostic;
pub use execution::{GraphExecution, GraphQueryPlan};
pub use ir::{
    ComparisonOperator, Direction, GraphPlan, Literal, NodePattern, OrderDirection, OrderKey,
    Projection, PropertyPredicate, PropertyRef, RelationshipPattern,
};
pub use sql::SqlTranslation;
