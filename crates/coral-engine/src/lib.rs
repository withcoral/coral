//! Data-plane query engine for Coral.
//!
//! `coral-engine` is the federated `DataFusion` engine for Coral. It owns
//! backend-specific source adapters, backend compilation into executable
//! providers, runtime assembly, and `SQL` execution over app-provided managed
//! sources.
//!
//! # Primary Entry Points
//!
//! - [`CoralQuery`] performs high-level query operations.
//! - [`contracts`] contains the reviewable app-to-query seam types and
//!   transport-neutral error contract.
//! - `backends::mod` defines the internal plugin seam that keeps common runtime
//!   orchestration backend-blind.
//!
//! # Crate Relationships
//!
//! - `coral-app` is the management plane and supplies selected [`QuerySource`]
//!   values plus credential providers.
//! - `coral-spec` owns source-spec parsing, validation, and normalized
//!   declarative source models consumed by this engine.
//!
//! # Example
//!
//! ```no_run
//! use std::collections::BTreeMap;
//!
//! use coral_engine::{CoralQuery, QueryRuntimeConfig, QuerySource};
//! use coral_spec::parse_source_manifest_yaml;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//!
//! # let source_spec = parse_source_manifest_yaml(
//! #     "name: demo\nversion: 0.1.0\ndsl_version: 3\nbackend: file\ntables: []",
//! # )?;
//! # let sources = vec![QuerySource::new(
//! #     source_spec,
//! #     BTreeMap::new(),
//! #     BTreeMap::new(),
//! # )];
//! # async fn demo(
//! #     sources: &[QuerySource],
//! # ) -> Result<(), Box<dyn std::error::Error>> {
//! let _ = CoralQuery::list_tables(sources, QueryRuntimeConfig::default(), None, None).await?;
//! # Ok(())
//! # }
//! # Ok(())
//! # }
//! ```
#![cfg_attr(
    test,
    allow(
        unused_crate_dependencies,
        reason = "wiremock is only used by the integration test target in this crate's dev-dependencies."
    )
)]

use std::collections::BTreeMap;

mod backends;
mod composition;
pub mod contracts;
mod runtime;
pub mod virtual_graph;

pub use backends::mcp::discover_tool_catalog as discover_mcp_tool_catalog;
pub use composition::{
    EngineExtensions, QueryResultObserver, QueryResultObserverError, RequestAuthenticator,
    RequestAuthenticatorError, SourceDecorator, SourceDecoratorError, SourceFailurePolicy,
    SourceInputResolutionContext, SourceInputResolver, SourceInputResolverError, SourceTables,
};
pub use contracts::{
    CatalogInfo, ColumnInfo, CoreError, DependentJoinConfig, DependentJoinSourceConfig,
    DescribeTableInfo, EffectiveDependentJoinConfig, MemorySize, QueryExecution,
    QueryExecutionProvenance, QueryMemoryConfig, QueryPlan, QueryRuntimeConfig,
    QueryRuntimeContext, QuerySource, QueryTableFunctionUsage, QueryTableUsage, QueryTestFailure,
    QueryTestResult, QueryTestSuccess, RuntimeSourceComponent, RuntimeSourcePackage,
    SourceValidationReport, StatusCode, StructuredQueryError, TableFunctionArgumentInfo,
    TableFunctionInfo, TableFunctionResultColumnInfo, TableInfo,
};
pub use virtual_graph::{
    AggregateFunction as GraphAggregateFunction, AggregateTarget as GraphAggregateTarget,
    ComparisonOperator, CypherParameterValue as GraphCypherParameterValue,
    Declaration as GraphDeclaration, Diagnostic as GraphDiagnostic, Direction as GraphDirection,
    GraphExecution, GraphPlan, GraphQuery, GraphQueryPlan, GraphUnion as GraphQueryUnion,
    GraphUnionBranch as GraphQueryUnionBranch,
    GraphUnionOuterProjection as GraphQueryUnionOuterProjection,
    GraphUnionOuterProjectionItem as GraphQueryUnionOuterProjectionItem,
    GraphqlVariableValue as GraphGraphqlVariableValue, Literal as GraphLiteral, NodePattern,
    NullOrder as GraphNullOrder, OrderDirection as GraphOrderDirection,
    OrderExpression as GraphOrderExpression, OrderKey as GraphOrderKey,
    PredicateExpression as GraphPredicateExpression, PredicateRhs as GraphPredicateRhs,
    Projection as GraphProjection, ProjectionPredicate as GraphProjectionPredicate,
    ProjectionPredicateExpression as GraphProjectionPredicateExpression,
    ProjectionPredicateRhs as GraphProjectionPredicateRhs,
    PropertyPredicate as GraphPropertyPredicate, PropertyRef as GraphPropertyRef,
    RelationshipPattern, SqlTranslation as GraphSqlTranslation, compile_cypher,
    compile_cypher_for_graph, compile_cypher_for_graph_with_parameters, compile_cypher_query,
    compile_cypher_query_for_graph, compile_cypher_query_for_graph_with_parameters,
    compile_cypher_query_with_parameters, compile_cypher_with_parameters, compile_graphql,
    compile_graphql_for_graph, compile_graphql_for_graph_with_operation_name,
    compile_graphql_for_graph_with_variables,
    compile_graphql_for_graph_with_variables_and_operation_name,
    compile_graphql_with_operation_name, compile_graphql_with_variables,
    compile_graphql_with_variables_and_operation_name, graphql_schema_sdl_for_graph,
};

/// High-level query operations for the local query engine.
pub struct CoralQuery;

impl CoralQuery {
    /// Lists queryable tables from the provided source set.
    ///
    /// When `schema_filter` is present, only tables for that visible `SQL`
    /// schema are returned.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if credential resolution fails, if any validated
    /// source spec cannot be compiled, or if the underlying query runtime
    /// cannot be built.
    pub async fn list_tables(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        schema_filter: Option<&str>,
        table_filter: Option<&str>,
    ) -> Result<Vec<TableInfo>, CoreError> {
        Ok(runtime::query::build_runtime(sources, runtime)
            .await?
            .list_tables(schema_filter, table_filter))
    }

    /// Lists queryable catalog metadata from the provided source set.
    ///
    /// When `schema_filter` is present, only catalog items for that visible
    /// `SQL` schema are returned. Tables and source-scoped table functions are
    /// collected from one runtime build so callers see one consistent catalog
    /// snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if credential resolution fails, if any validated
    /// source spec cannot be compiled, or if the underlying query runtime
    /// cannot be built.
    pub async fn list_catalog(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        schema_filter: Option<&str>,
    ) -> Result<CatalogInfo, CoreError> {
        Ok(runtime::query::build_runtime(sources, runtime)
            .await?
            .catalog_info(schema_filter))
    }

    /// Describes one table or returns lightweight table metadata for missing-table help.
    ///
    /// This builds the runtime once, clones only the matched table on exact
    /// hits, and clones lightweight table metadata when the table is missing.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if credential resolution fails, if any validated
    /// source spec cannot be compiled, or if the underlying query runtime
    /// cannot be built.
    pub async fn describe_table(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        schema_name: &str,
        table_name: &str,
    ) -> Result<DescribeTableInfo, CoreError> {
        Ok(runtime::query::build_runtime(sources, runtime)
            .await?
            .describe_table(schema_name, table_name))
    }

    /// Executes one `SQL` statement over the provided source set.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if the SQL is empty, if source compilation fails,
    /// or if the runtime cannot execute the statement.
    pub async fn execute_sql(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        sql: &str,
    ) -> Result<QueryExecution, CoreError> {
        if sql.trim().is_empty() {
            return Err(CoreError::InvalidInput("SQL must not be empty".to_string()));
        }

        runtime::query::build_runtime(sources, runtime)
            .await?
            .execute_sql(sql)
            .await
    }

    /// Explains one `SQL` statement with logical and physical plan renderings.
    ///
    /// The explanation is built against the provided source set and current
    /// runtime state. It does not execute the SQL statement.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if the SQL is empty, if source compilation fails,
    /// or if the query engine cannot explain the statement.
    pub async fn explain_sql(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        sql: &str,
    ) -> Result<QueryPlan, CoreError> {
        if sql.trim().is_empty() {
            return Err(CoreError::InvalidInput("SQL must not be empty".to_string()));
        }

        runtime::query::build_runtime(sources, runtime)
            .await?
            .explain_sql(sql)
            .await
    }

    /// Executes one virtual graph plan over the provided source set.
    ///
    /// The graph plan is lowered to `DataFusion` SQL and then executed through
    /// the same runtime path as [`Self::execute_sql`]. The returned wrapper
    /// preserves the translated SQL and virtual graph diagnostics for callers
    /// that need to display or audit the generated relational query.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if graph lowering fails, source compilation fails,
    /// or the generated SQL cannot execute.
    pub async fn execute_graph_plan(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        graph: &GraphDeclaration,
        plan: &GraphPlan,
    ) -> Result<GraphExecution, CoreError> {
        let query_runtime = runtime::query::build_runtime(sources, runtime).await?;
        let catalog = query_runtime.catalog_info(None);
        graph.validate_graph_plan_against_catalog(plan, &catalog)?;
        let translation = graph.lower_graph_plan(plan)?;
        let execution = query_runtime.execute_sql(translation.sql()).await?;
        Ok(GraphExecution::new(translation, execution))
    }

    /// Explains one virtual graph plan over the provided source set.
    ///
    /// The graph plan is lowered to `DataFusion` SQL and then explained through
    /// the same runtime path as [`Self::explain_sql`].
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if graph lowering fails, source compilation fails,
    /// or the generated SQL cannot be planned.
    pub async fn explain_graph_plan(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        graph: &GraphDeclaration,
        plan: &GraphPlan,
    ) -> Result<GraphQueryPlan, CoreError> {
        let query_runtime = runtime::query::build_runtime(sources, runtime).await?;
        let catalog = query_runtime.catalog_info(None);
        graph.validate_graph_plan_against_catalog(plan, &catalog)?;
        let translation = graph.lower_graph_plan(plan)?;
        let query_plan = query_runtime.explain_sql(translation.sql()).await?;
        Ok(GraphQueryPlan::new(translation, query_plan))
    }

    /// Executes one read-only virtual graph query over the provided source set.
    ///
    /// This is the query-level companion to [`Self::execute_graph_plan`] and
    /// supports top-level composition such as `UNION` / `UNION ALL`.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if graph lowering fails, source compilation fails,
    /// or the generated SQL cannot execute.
    pub async fn execute_graph_query(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        graph: &GraphDeclaration,
        query: &GraphQuery,
    ) -> Result<GraphExecution, CoreError> {
        let query_runtime = runtime::query::build_runtime(sources, runtime).await?;
        let catalog = query_runtime.catalog_info(None);
        graph.validate_graph_query_against_catalog(query, &catalog)?;
        let translation = graph.lower_graph_query(query)?;
        let execution = query_runtime.execute_sql(translation.sql()).await?;
        Ok(GraphExecution::new(translation, execution))
    }

    /// Explains one read-only virtual graph query over the provided source set.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if graph lowering fails, source compilation fails,
    /// or the generated SQL cannot be planned.
    pub async fn explain_graph_query(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        graph: &GraphDeclaration,
        query: &GraphQuery,
    ) -> Result<GraphQueryPlan, CoreError> {
        let query_runtime = runtime::query::build_runtime(sources, runtime).await?;
        let catalog = query_runtime.catalog_info(None);
        graph.validate_graph_query_against_catalog(query, &catalog)?;
        let translation = graph.lower_graph_query(query)?;
        let query_plan = query_runtime.explain_sql(translation.sql()).await?;
        Ok(GraphQueryPlan::new(translation, query_plan))
    }

    /// Executes one supported read-only Cypher query over a virtual graph declaration.
    ///
    /// The Cypher text is parsed and compiled into Coral's shared graph query,
    /// then lowered to `DataFusion` SQL and executed through the normal SQL
    /// runtime. The returned wrapper preserves the translated SQL.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if parsing or subset validation fails, graph
    /// lowering fails, source compilation fails, or the generated SQL cannot
    /// execute.
    pub async fn execute_cypher(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        graph: &GraphDeclaration,
        cypher: &str,
    ) -> Result<GraphExecution, CoreError> {
        if cypher.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "Cypher query must not be empty".to_string(),
            ));
        }

        let query = compile_cypher_query_for_graph(graph, cypher)?;
        Self::execute_graph_query(sources, runtime, graph, &query).await
    }

    /// Executes one supported read-only Cypher query with typed parameters.
    ///
    /// Parameters are bound into Coral's shared graph query before SQL lowering.
    /// Scalar parameters can be used anywhere the supported subset accepts a
    /// literal; list parameters can be used as `IN` right-hand sides.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if parsing or subset validation fails, a required
    /// parameter is missing, a parameter value is used in an unsupported
    /// position, graph lowering fails, source compilation fails, or the
    /// generated SQL cannot execute.
    pub async fn execute_cypher_with_parameters(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        graph: &GraphDeclaration,
        cypher: &str,
        parameters: &BTreeMap<String, GraphCypherParameterValue>,
    ) -> Result<GraphExecution, CoreError> {
        if cypher.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "Cypher query must not be empty".to_string(),
            ));
        }

        let query = compile_cypher_query_for_graph_with_parameters(graph, cypher, parameters)?;
        Self::execute_graph_query(sources, runtime, graph, &query).await
    }

    /// Explains one supported read-only Cypher query over a virtual graph declaration.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if parsing or subset validation fails, graph
    /// lowering fails, source compilation fails, or the generated SQL cannot be
    /// planned.
    pub async fn explain_cypher(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        graph: &GraphDeclaration,
        cypher: &str,
    ) -> Result<GraphQueryPlan, CoreError> {
        if cypher.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "Cypher query must not be empty".to_string(),
            ));
        }

        let query = compile_cypher_query_for_graph(graph, cypher)?;
        Self::explain_graph_query(sources, runtime, graph, &query).await
    }

    /// Explains one supported read-only Cypher query with typed parameters.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if parsing or subset validation fails, a required
    /// parameter is missing, a parameter value is used in an unsupported
    /// position, graph lowering fails, source compilation fails, or the
    /// generated SQL cannot be planned.
    pub async fn explain_cypher_with_parameters(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        graph: &GraphDeclaration,
        cypher: &str,
        parameters: &BTreeMap<String, GraphCypherParameterValue>,
    ) -> Result<GraphQueryPlan, CoreError> {
        if cypher.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "Cypher query must not be empty".to_string(),
            ));
        }

        let query = compile_cypher_query_for_graph_with_parameters(graph, cypher, parameters)?;
        Self::explain_graph_query(sources, runtime, graph, &query).await
    }

    /// Executes one supported read-only GraphQL virtual graph query.
    ///
    /// The GraphQL text is parsed and compiled into Coral's shared graph plan,
    /// then lowered to `DataFusion` SQL and executed through the normal SQL
    /// runtime. The returned wrapper preserves the translated SQL.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if parsing or subset validation fails, graph
    /// lowering fails, source compilation fails, or the generated SQL cannot
    /// execute.
    pub async fn execute_graphql(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        graph: &GraphDeclaration,
        graphql: &str,
    ) -> Result<GraphExecution, CoreError> {
        if graphql.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "GraphQL query must not be empty".to_string(),
            ));
        }

        let plan = compile_graphql_for_graph(graph, graphql)?;
        Self::execute_graph_plan(sources, runtime, graph, &plan).await
    }

    /// Executes one supported read-only GraphQL query with typed variables.
    ///
    /// Variables are bound into Coral's shared graph plan before SQL lowering.
    /// Scalar variables can be used anywhere the supported GraphQL subset
    /// accepts scalar literals or enum-like names; list variables can be used
    /// as `in` right-hand sides; object variables can be used as supported
    /// `where`, nested `where`, `relationshipWhere`, and `orderBy` objects.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if parsing or subset validation fails, a required
    /// variable is missing, a variable value is used in an unsupported
    /// position, graph lowering fails, source compilation fails, or the
    /// generated SQL cannot execute.
    pub async fn execute_graphql_with_variables(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        graph: &GraphDeclaration,
        graphql: &str,
        variables: &BTreeMap<String, GraphGraphqlVariableValue>,
    ) -> Result<GraphExecution, CoreError> {
        if graphql.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "GraphQL query must not be empty".to_string(),
            ));
        }

        let plan = compile_graphql_for_graph_with_variables(graph, graphql, variables)?;
        Self::execute_graph_plan(sources, runtime, graph, &plan).await
    }

    /// Executes one named operation from a supported read-only GraphQL document.
    ///
    /// Use this when a client sends multiple query operations and selects one
    /// with `operationName`.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if parsing or subset validation fails, the named
    /// operation is missing or not a query, graph lowering fails, source
    /// compilation fails, or the generated SQL cannot execute.
    pub async fn execute_graphql_with_operation_name(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        graph: &GraphDeclaration,
        graphql: &str,
        operation_name: &str,
    ) -> Result<GraphExecution, CoreError> {
        if graphql.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "GraphQL query must not be empty".to_string(),
            ));
        }

        let plan = compile_graphql_for_graph_with_operation_name(graph, graphql, operation_name)?;
        Self::execute_graph_plan(sources, runtime, graph, &plan).await
    }

    /// Executes one named operation from a supported read-only GraphQL document
    /// with typed variables.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if parsing or subset validation fails, the named
    /// operation is missing or not a query, a required selected-operation
    /// variable is missing, a variable value is used in an unsupported
    /// position, graph lowering fails, source compilation fails, or the
    /// generated SQL cannot execute.
    pub async fn execute_graphql_with_variables_and_operation_name(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        graph: &GraphDeclaration,
        graphql: &str,
        variables: &BTreeMap<String, GraphGraphqlVariableValue>,
        operation_name: &str,
    ) -> Result<GraphExecution, CoreError> {
        if graphql.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "GraphQL query must not be empty".to_string(),
            ));
        }

        let plan = compile_graphql_for_graph_with_variables_and_operation_name(
            graph,
            graphql,
            variables,
            operation_name,
        )?;
        Self::execute_graph_plan(sources, runtime, graph, &plan).await
    }

    /// Explains one supported read-only GraphQL virtual graph query.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if parsing or subset validation fails, graph
    /// lowering fails, source compilation fails, or the generated SQL cannot be
    /// planned.
    pub async fn explain_graphql(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        graph: &GraphDeclaration,
        graphql: &str,
    ) -> Result<GraphQueryPlan, CoreError> {
        if graphql.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "GraphQL query must not be empty".to_string(),
            ));
        }

        let plan = compile_graphql_for_graph(graph, graphql)?;
        Self::explain_graph_plan(sources, runtime, graph, &plan).await
    }

    /// Explains one supported read-only GraphQL query with typed variables.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if parsing or subset validation fails, a required
    /// variable is missing, a variable value is used in an unsupported
    /// position, graph lowering fails, source compilation fails, or the
    /// generated SQL cannot be planned.
    pub async fn explain_graphql_with_variables(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        graph: &GraphDeclaration,
        graphql: &str,
        variables: &BTreeMap<String, GraphGraphqlVariableValue>,
    ) -> Result<GraphQueryPlan, CoreError> {
        if graphql.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "GraphQL query must not be empty".to_string(),
            ));
        }

        let plan = compile_graphql_for_graph_with_variables(graph, graphql, variables)?;
        Self::explain_graph_plan(sources, runtime, graph, &plan).await
    }

    /// Explains one named operation from a supported read-only GraphQL document.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if parsing or subset validation fails, the named
    /// operation is missing or not a query, graph lowering fails, source
    /// compilation fails, or the generated SQL cannot be planned.
    pub async fn explain_graphql_with_operation_name(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        graph: &GraphDeclaration,
        graphql: &str,
        operation_name: &str,
    ) -> Result<GraphQueryPlan, CoreError> {
        if graphql.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "GraphQL query must not be empty".to_string(),
            ));
        }

        let plan = compile_graphql_for_graph_with_operation_name(graph, graphql, operation_name)?;
        Self::explain_graph_plan(sources, runtime, graph, &plan).await
    }

    /// Explains one named operation from a supported read-only GraphQL document
    /// with typed variables.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if parsing or subset validation fails, the named
    /// operation is missing or not a query, a required selected-operation
    /// variable is missing, a variable value is used in an unsupported
    /// position, graph lowering fails, source compilation fails, or the
    /// generated SQL cannot be planned.
    pub async fn explain_graphql_with_variables_and_operation_name(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        graph: &GraphDeclaration,
        graphql: &str,
        variables: &BTreeMap<String, GraphGraphqlVariableValue>,
        operation_name: &str,
    ) -> Result<GraphQueryPlan, CoreError> {
        if graphql.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "GraphQL query must not be empty".to_string(),
            ));
        }

        let plan = compile_graphql_for_graph_with_variables_and_operation_name(
            graph,
            graphql,
            variables,
            operation_name,
        )?;
        Self::explain_graph_plan(sources, runtime, graph, &plan).await
    }

    /// Validates that a single source can be initialized and queried.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if runtime construction fails or if the source
    /// cannot be registered or enumerated successfully.
    pub async fn test_source(
        source: &QuerySource,
        runtime: QueryRuntimeConfig,
    ) -> Result<Vec<TableInfo>, CoreError> {
        Ok(Self::validate_source(source, runtime, &[]).await?.tables)
    }

    /// Validates one source and then executes any declared validation queries.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if the source cannot be initialized or enumerated.
    /// Query-test failures are reported in the returned outcome instead of
    /// failing the whole call.
    pub async fn validate_source(
        source: &QuerySource,
        runtime: QueryRuntimeConfig,
        test_queries: &[String],
    ) -> Result<SourceValidationReport, CoreError> {
        let query_runtime =
            runtime::query::build_runtime(std::slice::from_ref(source), runtime).await?;
        let source_name = source.source_name();
        let schema_names = source.schema_names();
        let catalog = query_runtime.catalog_info_for_schemas(&schema_names);
        if catalog.tables.is_empty() && catalog.table_functions.is_empty() {
            if let Some(failure) = query_runtime.registration_failure(source_name) {
                return Err(CoreError::FailedPrecondition(failure.detail.clone()));
            }
            for schema_name in schema_names {
                if let Some(failure) = query_runtime.registration_failure(schema_name) {
                    return Err(CoreError::FailedPrecondition(failure.detail.clone()));
                }
            }
            return Err(CoreError::FailedPrecondition(format!(
                "source '{source_name}' did not become queryable during validation"
            )));
        }

        let mut query_tests = Vec::with_capacity(test_queries.len());
        for sql in test_queries {
            match query_runtime.execute_sql(sql).await {
                Ok(execution) => query_tests.push(QueryTestResult::success(
                    sql.clone(),
                    execution.row_count() as u64,
                )),
                Err(error) => {
                    let error_message = match &error {
                        CoreError::InvalidInput(detail) if is_non_read_only_sql_error(detail) => {
                            "test query must be read-only SQL".to_string()
                        }
                        _ => error.to_string(),
                    };
                    query_tests.push(QueryTestResult::failure(sql.clone(), error_message));
                }
            }
        }

        Ok(SourceValidationReport::new(
            catalog.tables,
            catalog.table_functions,
            query_tests,
        ))
    }
}

fn is_non_read_only_sql_error(detail: &str) -> bool {
    detail.starts_with("DDL not supported")
        || detail.starts_with("DML not supported")
        || detail.starts_with("Statement not supported")
}
