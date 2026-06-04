//! Implements the gRPC `QueryService`.

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use coral_api::v1::query_service_server::QueryService as QueryServiceApi;
use coral_api::v1::{
    ExecuteSqlRequest, ExecuteSqlResponse, ExplainSqlRequest, ExplainSqlResponse,
    QueryPlan as QueryPlanProto, SqlNamedParameters, SqlParameterValue as ProtoSqlParameterValue,
    SqlPositionalParameters, execute_sql_request, sql_parameter_value,
};
use coral_engine::{SqlParameterValue, SqlParameters};
use std::collections::BTreeMap;
use tonic::{Request, Response, Status};

use crate::bootstrap::core_status;
use crate::query::manager::QueryManager;
use crate::transport::{grpc_span, instrument_grpc, query_status, workspace_name_from_proto};

#[derive(Clone)]
pub(crate) struct QueryService {
    queries: QueryManager,
}

impl QueryService {
    pub(crate) fn new(query_manager: QueryManager) -> Self {
        Self {
            queries: query_manager,
        }
    }
}

#[tonic::async_trait]
impl QueryServiceApi for QueryService {
    async fn execute_sql(
        &self,
        request: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ExecuteSqlResponse>, Status> {
        let span = grpc_span(&request);
        let queries = self.queries.clone();
        Box::pin(instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            let params = sql_parameters_from_proto(inner.parameters)?;
            let execution = queries
                .execute_sql(&workspace_name, &inner.sql, params.as_ref())
                .await
                .map_err(query_status)?;
            let response = ExecuteSqlResponse {
                arrow_ipc_stream: encode_arrow_ipc_stream(
                    execution.arrow_schema(),
                    execution.batches(),
                )
                .map_err(coral_engine::CoreError::from)
                .map_err(core_status)?,
                row_count: i64::try_from(execution.row_count()).unwrap_or(i64::MAX),
            };
            Ok(Response::new(response))
        }))
        .await
    }

    async fn explain_sql(
        &self,
        request: Request<ExplainSqlRequest>,
    ) -> Result<Response<ExplainSqlResponse>, Status> {
        let span = grpc_span(&request);
        let queries = self.queries.clone();
        Box::pin(instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            let plan = queries
                .explain_sql(&workspace_name, &inner.sql)
                .await
                .map_err(query_status)?;
            Ok(Response::new(ExplainSqlResponse {
                plan: Some(query_plan_to_proto(&plan)),
            }))
        }))
        .await
    }
}

fn sql_parameters_from_proto(
    params: Option<execute_sql_request::Parameters>,
) -> Result<Option<SqlParameters>, Status> {
    params
        .map(|params| match params {
            execute_sql_request::Parameters::PositionalParams(params) => {
                positional_sql_parameters_from_proto(params).map(SqlParameters::Positional)
            }
            execute_sql_request::Parameters::NamedParams(params) => {
                named_sql_parameters_from_proto(params).map(SqlParameters::Named)
            }
        })
        .transpose()
}

fn positional_sql_parameters_from_proto(
    params: SqlPositionalParameters,
) -> Result<Vec<SqlParameterValue>, Status> {
    params
        .values
        .into_iter()
        .map(sql_parameter_value_from_proto)
        .collect()
}

fn named_sql_parameters_from_proto(
    params: SqlNamedParameters,
) -> Result<BTreeMap<String, SqlParameterValue>, Status> {
    params
        .values
        .into_iter()
        .map(|(name, value)| {
            if name.is_empty() {
                return Err(Status::invalid_argument(
                    "named SQL parameter names must not be empty",
                ));
            }
            if name.starts_with('$') {
                return Err(Status::invalid_argument(
                    "named SQL parameter names must not include the leading '$'",
                ));
            }
            sql_parameter_value_from_proto(value).map(|value| (name, value))
        })
        .collect()
}

fn sql_parameter_value_from_proto(
    value: ProtoSqlParameterValue,
) -> Result<SqlParameterValue, Status> {
    match value.kind {
        Some(sql_parameter_value::Kind::NullValue(_)) => Ok(SqlParameterValue::Null),
        Some(sql_parameter_value::Kind::BoolValue(value)) => Ok(SqlParameterValue::Boolean(value)),
        Some(sql_parameter_value::Kind::Int64Value(value)) => Ok(SqlParameterValue::Int64(value)),
        Some(sql_parameter_value::Kind::Float64Value(value)) => {
            if value.is_finite() {
                Ok(SqlParameterValue::Float64(value))
            } else {
                Err(Status::invalid_argument(
                    "float64 SQL parameters must be finite",
                ))
            }
        }
        Some(sql_parameter_value::Kind::StringValue(value)) => Ok(SqlParameterValue::Utf8(value)),
        None => Err(Status::invalid_argument(
            "SQL parameter value must specify a kind",
        )),
    }
}

fn query_plan_to_proto(plan: &coral_engine::QueryPlan) -> QueryPlanProto {
    QueryPlanProto {
        unoptimized_logical_plan: plan.unoptimized_logical_plan().to_string(),
        optimized_logical_plan: plan.optimized_logical_plan().to_string(),
        physical_plan: plan.physical_plan().to_string(),
    }
}

fn encode_arrow_ipc_stream(
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    Ok(bytes)
}
