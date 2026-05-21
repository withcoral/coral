use std::collections::HashMap;

use datafusion::common::DataFusionError;
use thiserror::Error;

use crate::contracts::StructuredQueryError;
use crate::{CoreError, StatusCode};

const BINDING_LIMIT_REASON: &str = "DEPENDENT_JOIN_BINDING_LIMIT_EXCEEDED";
const RESOLVER_ROW_LIMIT_REASON: &str = "DEPENDENT_JOIN_RESOLVER_ROW_LIMIT_EXCEEDED";
const ROWS_PER_BINDING_LIMIT_REASON: &str = "DEPENDENT_JOIN_ROWS_PER_BINDING_LIMIT_EXCEEDED";
const RESOLVER_ROWS_PER_BINDING_LIMIT_REASON: &str =
    "DEPENDENT_JOIN_RESOLVER_ROWS_PER_BINDING_LIMIT_EXCEEDED";

#[derive(Debug, Clone, Copy)]
pub(crate) struct ResolverRowsExceeded<'a> {
    pub(crate) source_schema: &'a str,
    pub(crate) table: &'a str,
    pub(crate) observed: usize,
    pub(crate) cap: usize,
}

#[derive(Debug, Error)]
pub(crate) enum DependentJoinError {
    #[error(
        "Your query produced {observed} distinct combinations of join-key values for {source_schema}.{table} (matching on {}), but Coral is configured to push at most {cap} such combinations into the upstream API.",
        binding_filters.join(", ")
    )]
    Cardinality {
        source_schema: String,
        table: String,
        observed: usize,
        cap: usize,
        binding_filters: Vec<String>,
    },

    #[error(
        "The side of the join that supplies keys for {source_schema}.{table} produced {observed} rows, but Coral is configured to inspect at most {cap} rows before deciding how to query the upstream API."
    )]
    ResolverRows {
        source_schema: String,
        table: String,
        observed: usize,
        cap: usize,
    },

    #[error(
        "The upstream API for {source_schema}.{table} returned {observed} rows for one join-key combination, but Coral is configured to accept at most {cap} rows per upstream request."
    )]
    RowsPerBinding {
        source_schema: String,
        table: String,
        observed: usize,
        cap: usize,
    },

    #[error(
        "One join-key combination for {source_schema}.{table} matched {observed} rows on the key-supplying side of the join, but Coral is configured to allow at most {cap} rows for one combination."
    )]
    ResolverRowsPerBinding {
        source_schema: String,
        table: String,
        observed: usize,
        cap: usize,
    },
}

impl DependentJoinError {
    pub(crate) fn into_datafusion(self) -> DataFusionError {
        DataFusionError::External(Box::new(self))
    }

    pub(crate) fn to_core_error(&self) -> CoreError {
        let (reason, summary, detail, hint, metadata) = match self {
            DependentJoinError::Cardinality {
                source_schema,
                table,
                observed,
                cap,
                binding_filters,
            } => {
                let mut metadata = limit_metadata(source_schema, table, *observed, *cap);
                metadata.insert("binding_filters".to_string(), binding_filters.join(","));
                (
                    BINDING_LIMIT_REASON,
                    format!("Too many join-key combinations for {source_schema}.{table}"),
                    self.to_string(),
                    Some(format!(
                        "Narrow the WHERE clause on the other side of the join so fewer distinct \
                         join keys are produced, or ask your Coral operator to raise the \
                         dependent-join binding limit. Matching columns: {}.",
                        binding_filters.join(", ")
                    )),
                    metadata,
                )
            }
            DependentJoinError::ResolverRows {
                source_schema,
                table,
                observed,
                cap,
            } => (
                RESOLVER_ROW_LIMIT_REASON,
                format!("Too many key-supplying rows for {source_schema}.{table}"),
                self.to_string(),
                Some(
                    "Narrow the WHERE clause on the key-supplying side of the join, or ask your \
                     Coral operator to raise the dependent-join resolver-row limit."
                        .to_string(),
                ),
                limit_metadata(source_schema, table, *observed, *cap),
            ),
            DependentJoinError::RowsPerBinding {
                source_schema,
                table,
                observed,
                cap,
            } => (
                ROWS_PER_BINDING_LIMIT_REASON,
                format!("Too many upstream rows for one {source_schema}.{table} join key"),
                self.to_string(),
                Some(
                    "The upstream API returned more rows for one filter combination than Coral \
                     expected. Check that the source manifest filters identify the intended API \
                     route, or ask your Coral operator to raise the per-request row limit."
                        .to_string(),
                ),
                limit_metadata(source_schema, table, *observed, *cap),
            ),
            DependentJoinError::ResolverRowsPerBinding {
                source_schema,
                table,
                observed,
                cap,
            } => (
                RESOLVER_ROWS_PER_BINDING_LIMIT_REASON,
                format!("One {source_schema}.{table} join key matched too many input rows"),
                self.to_string(),
                Some(
                    "Reduce duplicate join-key rows on the key-supplying side of the join, for \
                     example with DISTINCT or a narrower WHERE clause, or ask your Coral operator \
                     to raise the per-key resolver-row limit."
                        .to_string(),
                ),
                limit_metadata(source_schema, table, *observed, *cap),
            ),
        };

        CoreError::QueryFailure(Box::new(StructuredQueryError::new(
            reason,
            summary,
            detail,
            hint,
            false,
            StatusCode::FailedPrecondition,
            metadata,
        )))
    }
}

fn limit_metadata(
    source_schema: &str,
    table: &str,
    observed: usize,
    cap: usize,
) -> HashMap<String, String> {
    HashMap::from([
        ("source".to_string(), source_schema.to_string()),
        ("table".to_string(), table.to_string()),
        ("observed".to_string(), observed.to_string()),
        ("limit".to_string(), cap.to_string()),
    ])
}

pub(crate) fn resolver_rows_exceeded(error: &DataFusionError) -> Option<ResolverRowsExceeded<'_>> {
    let DataFusionError::External(inner) = error.find_root() else {
        return None;
    };
    let error = inner.downcast_ref::<DependentJoinError>()?;
    match error {
        DependentJoinError::ResolverRows {
            source_schema,
            table,
            observed,
            cap,
        } => Some(ResolverRowsExceeded {
            source_schema,
            table,
            observed: *observed,
            cap: *cap,
        }),
        DependentJoinError::Cardinality { .. }
        | DependentJoinError::RowsPerBinding { .. }
        | DependentJoinError::ResolverRowsPerBinding { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_structured_limit_error(
        error: &DependentJoinError,
        expected_reason: &str,
        expected_observed: &str,
        expected_limit: &str,
    ) -> HashMap<String, String> {
        let CoreError::QueryFailure(query_error) = error.to_core_error() else {
            panic!("expected structured query failure");
        };

        assert_eq!(query_error.reason(), expected_reason);
        assert_eq!(query_error.status(), StatusCode::FailedPrecondition);
        assert!(query_error.hint().is_some());
        assert_eq!(
            query_error.metadata().get("source").map(String::as_str),
            Some("github")
        );
        assert_eq!(
            query_error.metadata().get("table").map(String::as_str),
            Some("pull_requests")
        );
        assert_eq!(
            query_error.metadata().get("observed").map(String::as_str),
            Some(expected_observed)
        );
        assert_eq!(
            query_error.metadata().get("limit").map(String::as_str),
            Some(expected_limit)
        );

        query_error.metadata().clone()
    }

    #[test]
    fn cardinality_error_maps_to_structured_query_failure() {
        let metadata = assert_structured_limit_error(
            &DependentJoinError::Cardinality {
                source_schema: "github".to_string(),
                table: "pull_requests".to_string(),
                observed: 501,
                cap: 500,
                binding_filters: vec!["owner".to_string(), "repo".to_string()],
            },
            BINDING_LIMIT_REASON,
            "501",
            "500",
        );

        assert_eq!(
            metadata.get("binding_filters").map(String::as_str),
            Some("owner,repo")
        );
    }

    #[test]
    fn resolver_rows_error_maps_to_structured_query_failure() {
        assert_structured_limit_error(
            &DependentJoinError::ResolverRows {
                source_schema: "github".to_string(),
                table: "pull_requests".to_string(),
                observed: 10001,
                cap: 10000,
            },
            RESOLVER_ROW_LIMIT_REASON,
            "10001",
            "10000",
        );
    }

    #[test]
    fn rows_per_binding_error_maps_to_structured_query_failure() {
        assert_structured_limit_error(
            &DependentJoinError::RowsPerBinding {
                source_schema: "github".to_string(),
                table: "pull_requests".to_string(),
                observed: 1001,
                cap: 1000,
            },
            ROWS_PER_BINDING_LIMIT_REASON,
            "1001",
            "1000",
        );
    }

    #[test]
    fn resolver_rows_per_binding_error_maps_to_structured_query_failure() {
        assert_structured_limit_error(
            &DependentJoinError::ResolverRowsPerBinding {
                source_schema: "github".to_string(),
                table: "pull_requests".to_string(),
                observed: 1001,
                cap: 1000,
            },
            RESOLVER_ROWS_PER_BINDING_LIMIT_REASON,
            "1001",
            "1000",
        );
    }
}
