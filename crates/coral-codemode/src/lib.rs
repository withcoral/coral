//! Transport-neutral codemode execution for Coral.
//!
//! The first codemode is Monty-backed Python with explicit read-only host
//! functions for SQL and catalog discovery. MCP and CLI adapters should call
//! this crate rather than owning execution behavior themselves.

mod catalog;
mod values;

use std::collections::BTreeSet;
use std::time::Duration;

use catalog::{describe_table_value, list_catalog_value, list_columns_value, search_catalog_value};
use coral_api::v1::{
    CatalogItemKind as ProtoCatalogItemKind, DescribeTableRequest, ExecuteSqlRequest,
    ListCatalogRequest, ListColumnsRequest, PaginationRequest, SearchCatalogRequest,
};
use coral_client::{
    CatalogClient, QueryClient, batches_to_json_rows_json_safe_numbers,
    decode_execute_sql_response, default_workspace,
};
use monty::{
    DictPairs, ExtFunctionResult, FunctionCall, JsonMontyObject, LimitedTracker, MontyObject,
    MontyRun, NameLookupResult, PrintWriter, ResourceLimits, RunProgress,
};
use monty_type_checking::{SourceFile, type_check};
use serde::Serialize;
use serde_json::Value;
use tokio::time::{Instant, timeout};
use tonic::Request;

/// Default Monty execution timeout used by CLI and MCP adapters.
pub const DEFAULT_TIMEOUT_MS: u64 = 60_000;
/// Minimum accepted Monty execution timeout.
pub const MIN_TIMEOUT_MS: u64 = 100;
/// Maximum accepted Monty execution timeout.
pub const MAX_TIMEOUT_MS: u64 = 60_000;
/// Default number of allowed `sql()` host-function calls.
pub const DEFAULT_MAX_SQL_CALLS: usize = 20;
/// Maximum number of allowed `sql()` host-function calls.
pub const MAX_SQL_CALLS: usize = 100;

const SCRIPT_NAME: &str = "coral_codemode.py";
const TYPE_STUBS_NAME: &str = "coral_codemode_stubs.pyi";
const DEFAULT_MAX_MEMORY_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_MAX_ALLOCATIONS: usize = 200_000;

const SQL_FUNCTION_NAME: &str = "sql";
const LIST_CATALOG_FUNCTION_NAME: &str = "list_catalog";
const SEARCH_CATALOG_FUNCTION_NAME: &str = "search_catalog";
const DESCRIBE_TABLE_FUNCTION_NAME: &str = "describe_table";
const LIST_COLUMNS_FUNCTION_NAME: &str = "list_columns";

const SQL_FUNCTION_DOC: &str =
    "Execute one read-only SQL query against Coral sources and return result rows.";
const LIST_CATALOG_FUNCTION_DOC: &str = "List Coral catalog items.";
const SEARCH_CATALOG_FUNCTION_DOC: &str = "Search Coral catalog metadata with a Rust regex.";
const DESCRIBE_TABLE_FUNCTION_DOC: &str = "Describe one Coral table.";
const LIST_COLUMNS_FUNCTION_DOC: &str = "List columns for one Coral table.";

const TYPE_STUBS: &str = r#"
from typing import Any

def sql(query: str) -> list[dict[str, Any]]:
    """Execute one read-only SQL query against Coral sources and return result rows."""
    ...

def list_catalog(schema: Any = None, kind: Any = None, limit: int = 50, offset: int = 0) -> dict[str, Any]:
    """List Coral catalog items."""
    ...

def search_catalog(pattern: str, schema: Any = None, kind: Any = None, ignore_case: bool = True, limit: int = 20, offset: int = 0) -> dict[str, Any]:
    """Search Coral catalog metadata with a Rust regex."""
    ...

def describe_table(schema: str, table: str) -> dict[str, Any]:
    """Describe one Coral table."""
    ...

def list_columns(schema: str, table: str, pattern: Any = None, ignore_case: bool = True, required_only: bool = False, limit: int = 50, offset: int = 0) -> dict[str, Any]:
    """List columns for one Coral table."""
    ...
"#;

#[derive(Clone)]
/// Coral clients available to codemode host functions.
pub struct CodemodeClients {
    /// SQL query client used by `sql()`.
    pub query: QueryClient,
    /// Catalog client used by catalog helper functions.
    pub catalog: CatalogClient,
}

impl CodemodeClients {
    #[must_use]
    /// Builds codemode clients from existing Coral gRPC clients.
    pub fn new(query: QueryClient, catalog: CatalogClient) -> Self {
        Self { query, catalog }
    }
}

/// Request for one Monty-backed Python codemode execution.
#[derive(Clone, Debug)]
pub struct PythonCodemodeRequest {
    /// Python code to execute.
    pub code: String,
    /// Whether to run Monty's static type checker before execution.
    pub type_check: bool,
    /// Maximum execution time in milliseconds.
    pub timeout_ms: u64,
    /// Maximum number of `sql()` host-function calls.
    pub max_sql_calls: usize,
}

impl PythonCodemodeRequest {
    #[must_use]
    /// Builds a request with conservative default execution limits.
    pub fn new(code: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            type_check: true,
            timeout_ms: DEFAULT_TIMEOUT_MS,
            max_sql_calls: DEFAULT_MAX_SQL_CALLS,
        }
    }
}

/// Result produced by one codemode run.
#[derive(Debug, Serialize)]
pub struct CodemodeResult {
    /// Natural JSON representation of the final Monty Python value.
    pub output: Value,
    /// Text captured from Python `print()` calls.
    pub stdout: String,
    /// Number of Coral SQL queries executed through `sql()`.
    pub query_count: usize,
}

/// Error returned by codemode execution.
#[derive(Debug, thiserror::Error)]
pub enum CodemodeError {
    /// Invalid user code or host-function arguments.
    #[error("{0}")]
    Invalid(String),
    /// Internal execution or serialization failure.
    #[error("codemode internal error: {0}")]
    Internal(String),
    /// SQL query failure.
    #[error("query failed: {0}")]
    Query(tonic::Status),
    /// Catalog lookup failure.
    #[error("catalog lookup failed: {0}")]
    Catalog(tonic::Status),
}

impl CodemodeError {
    #[must_use]
    /// Converts the codemode error into a gRPC status for MCP rendering.
    pub fn into_status(self) -> tonic::Status {
        match self {
            Self::Invalid(message) => tonic::Status::invalid_argument(message),
            Self::Internal(message) => tonic::Status::internal(message),
            Self::Query(status) | Self::Catalog(status) => status,
        }
    }

    #[must_use]
    /// Returns a structured status when this error originated from Coral RPCs.
    pub fn status(&self) -> Option<&tonic::Status> {
        match self {
            Self::Query(status) | Self::Catalog(status) => Some(status),
            Self::Invalid(_) | Self::Internal(_) => None,
        }
    }
}

struct CodemodeState {
    clients: CodemodeClients,
    query_count: usize,
    max_sql_calls: usize,
    deadline: Instant,
}

impl CodemodeState {
    fn new(clients: CodemodeClients, max_sql_calls: usize, timeout_ms: u64) -> Self {
        Self {
            clients,
            query_count: 0,
            max_sql_calls,
            deadline: Instant::now() + Duration::from_millis(timeout_ms),
        }
    }

    fn remaining_time(&self) -> Result<Duration, CodemodeError> {
        self.deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| CodemodeError::Invalid("codemode timeout exceeded".to_string()))
    }

    async fn query_rows(&mut self, sql: &str) -> Result<Vec<Value>, CodemodeError> {
        if self.query_count >= self.max_sql_calls {
            return Err(CodemodeError::Invalid(format!(
                "codemode exceeded max_sql_calls ({})",
                self.max_sql_calls
            )));
        }
        self.query_count = self.query_count.saturating_add(1);

        let mut query_client = self.clients.query.clone();
        let response = timeout(
            self.remaining_time()?,
            query_client.execute_sql(Request::new(ExecuteSqlRequest {
                workspace: Some(default_workspace()),
                sql: sql.to_string(),
            })),
        )
        .await
        .map_err(|_elapsed| CodemodeError::Invalid("codemode timeout exceeded".to_string()))?
        .map_err(CodemodeError::Query)?
        .into_inner();

        let result = decode_execute_sql_response(&response)
            .map_err(|error| CodemodeError::Internal(error.to_string()))?;
        batches_to_json_rows_json_safe_numbers(result.batches())
            .map_err(|error| CodemodeError::Internal(error.to_string()))
    }

    async fn list_catalog(&self, arguments: &CallArguments<'_>) -> Result<Value, CodemodeError> {
        let schema = arguments.optional_string("schema")?;
        let kind = arguments.optional_catalog_kind("kind")?;
        let limit = arguments.optional_u32("limit", 50, 1, 200)?;
        let offset = arguments.optional_u32("offset", 0, 0, u32::MAX)?;
        let mut catalog_client = self.clients.catalog.clone();
        let response = timeout(
            self.remaining_time()?,
            catalog_client.list_catalog(Request::new(ListCatalogRequest {
                workspace: Some(default_workspace()),
                schema_name: schema.unwrap_or_default(),
                kind: catalog_item_kind(kind) as i32,
                pagination: Some(PaginationRequest { limit, offset }),
            })),
        )
        .await
        .map_err(|_elapsed| CodemodeError::Invalid("codemode timeout exceeded".to_string()))?
        .map_err(CodemodeError::Catalog)?
        .into_inner();
        Ok(list_catalog_value(&response))
    }

    async fn search_catalog(&self, arguments: &CallArguments<'_>) -> Result<Value, CodemodeError> {
        let pattern = arguments.required_string("pattern")?;
        let schema = arguments.optional_string("schema")?;
        let kind = arguments.optional_catalog_kind("kind")?;
        let ignore_case = arguments.optional_bool("ignore_case", true)?;
        let limit = arguments.optional_u32("limit", 20, 1, 100)?;
        let offset = arguments.optional_u32("offset", 0, 0, u32::MAX)?;
        let mut catalog_client = self.clients.catalog.clone();
        let response = timeout(
            self.remaining_time()?,
            catalog_client.search_catalog(Request::new(SearchCatalogRequest {
                workspace: Some(default_workspace()),
                pattern,
                ignore_case,
                schema_name: schema.unwrap_or_default(),
                kind: catalog_item_kind(kind) as i32,
                pagination: Some(PaginationRequest { limit, offset }),
            })),
        )
        .await
        .map_err(|_elapsed| CodemodeError::Invalid("codemode timeout exceeded".to_string()))?
        .map_err(CodemodeError::Catalog)?
        .into_inner();
        Ok(search_catalog_value(&response))
    }

    async fn describe_table(&self, arguments: &CallArguments<'_>) -> Result<Value, CodemodeError> {
        let schema = arguments.required_string("schema")?;
        let table = arguments.required_string("table")?;
        self.describe_table_value(&schema, &table).await
    }

    async fn describe_table_value(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<Value, CodemodeError> {
        let mut catalog_client = self.clients.catalog.clone();
        let response = timeout(
            self.remaining_time()?,
            catalog_client.describe_table(Request::new(DescribeTableRequest {
                workspace: Some(default_workspace()),
                schema_name: schema.to_string(),
                table_name: table.to_string(),
            })),
        )
        .await
        .map_err(|_elapsed| CodemodeError::Invalid("codemode timeout exceeded".to_string()))?
        .map_err(CodemodeError::Catalog)?
        .into_inner();
        Ok(describe_table_value(schema, table, &response))
    }

    async fn list_columns(&self, arguments: &CallArguments<'_>) -> Result<Value, CodemodeError> {
        let schema = arguments.required_string("schema")?;
        let table = arguments.required_string("table")?;
        let pattern = arguments.optional_non_empty_string("pattern")?;
        let ignore_case = arguments.optional_bool("ignore_case", true)?;
        let required_only = arguments.optional_bool("required_only", false)?;
        let limit = arguments.optional_u32("limit", 50, 1, 200)?;
        let offset = arguments.optional_u32("offset", 0, 0, u32::MAX)?;
        let mut catalog_client = self.clients.catalog.clone();
        match timeout(
            self.remaining_time()?,
            catalog_client.list_columns(Request::new(ListColumnsRequest {
                workspace: Some(default_workspace()),
                schema_name: schema.clone(),
                table_name: table.clone(),
                pattern,
                ignore_case,
                required_only,
                pagination: Some(PaginationRequest { limit, offset }),
            })),
        )
        .await
        .map_err(|_elapsed| CodemodeError::Invalid("codemode timeout exceeded".to_string()))?
        {
            Ok(response) => Ok(list_columns_value(&schema, &table, &response.into_inner())),
            Err(status) if status.code() == tonic::Code::NotFound => {
                self.describe_table_value(&schema, &table).await
            }
            Err(status) => Err(CodemodeError::Catalog(status)),
        }
    }
}

/// Executes one Monty-backed Python codemode request.
///
/// # Errors
///
/// Returns [`CodemodeError`] when code is invalid, resource limits are exceeded,
/// or a Coral SQL/catalog host call fails.
pub async fn run_python(
    clients: CodemodeClients,
    request: PythonCodemodeRequest,
) -> Result<CodemodeResult, CodemodeError> {
    validate_request(&request)?;
    if request.type_check {
        type_check_code(&request.code)?;
    }

    let runner = MontyRun::new(request.code, SCRIPT_NAME, Vec::new())
        .map_err(|error| monty_codemode_error(&error))?;
    let limits = ResourceLimits::new()
        .max_duration(Duration::from_millis(request.timeout_ms))
        .max_memory(DEFAULT_MAX_MEMORY_BYTES)
        .max_allocations(DEFAULT_MAX_ALLOCATIONS);
    let mut stdout = String::new();
    let mut state = CodemodeState::new(clients, request.max_sql_calls, request.timeout_ms);
    let mut progress = runner
        .start(
            Vec::new(),
            LimitedTracker::new(limits),
            PrintWriter::CollectString(&mut stdout),
        )
        .map_err(|error| monty_codemode_error(&error))?;

    loop {
        progress = match progress {
            RunProgress::Complete(value) => {
                return Ok(CodemodeResult {
                    output: monty_to_json(&value)?,
                    stdout,
                    query_count: state.query_count,
                });
            }
            RunProgress::NameLookup(lookup) => {
                let result = host_function_doc(&lookup.name).map_or(
                    NameLookupResult::Undefined,
                    |docstring| {
                        NameLookupResult::Value(MontyObject::Function {
                            name: lookup.name.clone(),
                            docstring: Some(docstring.to_string()),
                        })
                    },
                );
                lookup
                    .resume(result, PrintWriter::CollectString(&mut stdout))
                    .map_err(|error| monty_codemode_error(&error))?
            }
            RunProgress::FunctionCall(call) => {
                resume_function_call(&mut state, call, &mut stdout).await?
            }
            RunProgress::OsCall(call) => {
                let error = call.function_call.on_no_handler();
                call.resume(error, PrintWriter::CollectString(&mut stdout))
                    .map_err(|error| monty_codemode_error(&error))?
            }
            RunProgress::ResolveFutures(pending) => {
                return Err(CodemodeError::Invalid(format!(
                    "codemode async external futures are not supported; pending call ids: {:?}",
                    pending.pending_call_ids()
                )));
            }
        };
    }
}

async fn resume_function_call(
    state: &mut CodemodeState,
    call: FunctionCall<LimitedTracker>,
    stdout: &mut String,
) -> Result<RunProgress<LimitedTracker>, CodemodeError> {
    match call.function_name.as_str() {
        SQL_FUNCTION_NAME => {
            let arguments = CallArguments::new(
                SQL_FUNCTION_NAME,
                &[SQL_ARGUMENT_QUERY],
                &call.args,
                &call.kwargs,
            )?;
            let sql = arguments.required_string(SQL_ARGUMENT_QUERY)?;
            let rows = state.query_rows(&sql).await?;
            let result = json_to_monty(Value::Array(rows))?;
            resume_call(call, result, stdout)
        }
        LIST_CATALOG_FUNCTION_NAME => {
            let arguments = CallArguments::new(
                LIST_CATALOG_FUNCTION_NAME,
                &LIST_CATALOG_ARGUMENTS,
                &call.args,
                &call.kwargs,
            )?;
            let result = json_to_monty(state.list_catalog(&arguments).await?)?;
            resume_call(call, result, stdout)
        }
        SEARCH_CATALOG_FUNCTION_NAME => {
            let arguments = CallArguments::new(
                SEARCH_CATALOG_FUNCTION_NAME,
                &SEARCH_CATALOG_ARGUMENTS,
                &call.args,
                &call.kwargs,
            )?;
            let result = json_to_monty(state.search_catalog(&arguments).await?)?;
            resume_call(call, result, stdout)
        }
        DESCRIBE_TABLE_FUNCTION_NAME => {
            let arguments = CallArguments::new(
                DESCRIBE_TABLE_FUNCTION_NAME,
                &DESCRIBE_TABLE_ARGUMENTS,
                &call.args,
                &call.kwargs,
            )?;
            let result = json_to_monty(state.describe_table(&arguments).await?)?;
            resume_call(call, result, stdout)
        }
        LIST_COLUMNS_FUNCTION_NAME => {
            let arguments = CallArguments::new(
                LIST_COLUMNS_FUNCTION_NAME,
                &LIST_COLUMNS_ARGUMENTS,
                &call.args,
                &call.kwargs,
            )?;
            let result = json_to_monty(state.list_columns(&arguments).await?)?;
            resume_call(call, result, stdout)
        }
        _ => {
            let function_name = call.function_name.clone();
            call.resume(
                ExtFunctionResult::NotFound(function_name),
                PrintWriter::CollectString(stdout),
            )
            .map_err(|error| monty_codemode_error(&error))
        }
    }
}

fn resume_call(
    call: FunctionCall<LimitedTracker>,
    result: MontyObject,
    stdout: &mut String,
) -> Result<RunProgress<LimitedTracker>, CodemodeError> {
    call.resume(result, PrintWriter::CollectString(stdout))
        .map_err(|error| monty_codemode_error(&error))
}

fn validate_request(request: &PythonCodemodeRequest) -> Result<(), CodemodeError> {
    if request.code.trim().is_empty() {
        return Err(CodemodeError::Invalid(
            "codemode code must not be empty".to_string(),
        ));
    }
    if !(MIN_TIMEOUT_MS..=MAX_TIMEOUT_MS).contains(&request.timeout_ms) {
        return Err(CodemodeError::Invalid(format!(
            "timeout_ms must be between {MIN_TIMEOUT_MS} and {MAX_TIMEOUT_MS}"
        )));
    }
    if request.max_sql_calls > MAX_SQL_CALLS {
        return Err(CodemodeError::Invalid(format!(
            "max_sql_calls must be between 0 and {MAX_SQL_CALLS}"
        )));
    }
    Ok(())
}

fn type_check_code(code: &str) -> Result<(), CodemodeError> {
    let stubs = SourceFile::new(TYPE_STUBS, TYPE_STUBS_NAME);
    match type_check(&SourceFile::new(code, SCRIPT_NAME), Some(&stubs)) {
        Ok(None) => Ok(()),
        Ok(Some(diagnostics)) => Err(CodemodeError::Invalid(format!(
            "Monty type check failed:\n{diagnostics}"
        ))),
        Err(error) => Err(CodemodeError::Internal(format!(
            "Monty type checker failed: {error}"
        ))),
    }
}

fn host_function_doc(name: &str) -> Option<&'static str> {
    match name {
        SQL_FUNCTION_NAME => Some(SQL_FUNCTION_DOC),
        LIST_CATALOG_FUNCTION_NAME => Some(LIST_CATALOG_FUNCTION_DOC),
        SEARCH_CATALOG_FUNCTION_NAME => Some(SEARCH_CATALOG_FUNCTION_DOC),
        DESCRIBE_TABLE_FUNCTION_NAME => Some(DESCRIBE_TABLE_FUNCTION_DOC),
        LIST_COLUMNS_FUNCTION_NAME => Some(LIST_COLUMNS_FUNCTION_DOC),
        _ => None,
    }
}

const SQL_ARGUMENT_QUERY: &str = "query";
const LIST_CATALOG_ARGUMENTS: [&str; 4] = ["schema", "kind", "limit", "offset"];
const SEARCH_CATALOG_ARGUMENTS: [&str; 6] = [
    "pattern",
    "schema",
    "kind",
    "ignore_case",
    "limit",
    "offset",
];
const DESCRIBE_TABLE_ARGUMENTS: [&str; 2] = ["schema", "table"];
const LIST_COLUMNS_ARGUMENTS: [&str; 7] = [
    "schema",
    "table",
    "pattern",
    "ignore_case",
    "required_only",
    "limit",
    "offset",
];

struct CallArguments<'a> {
    function_name: &'static str,
    positional_names: &'static [&'static str],
    positional: &'a [MontyObject],
    keyword: Vec<(&'static str, &'a MontyObject)>,
}

impl<'a> CallArguments<'a> {
    fn new(
        function_name: &'static str,
        positional_names: &'static [&'static str],
        positional: &'a [MontyObject],
        keyword: &'a [(MontyObject, MontyObject)],
    ) -> Result<Self, CodemodeError> {
        if positional.len() > positional_names.len() {
            return Err(CodemodeError::Invalid(format!(
                "{function_name}() expected at most {} positional arguments, got {}",
                positional_names.len(),
                positional.len()
            )));
        }

        let mut seen = BTreeSet::new();
        for name in positional_names.iter().take(positional.len()) {
            let inserted = seen.insert(*name);
            debug_assert!(inserted, "argument names are unique");
        }

        let mut parsed_keyword = Vec::with_capacity(keyword.len());
        for (key, value) in keyword {
            let MontyObject::String(key) = key else {
                return Err(CodemodeError::Invalid(format!(
                    "{function_name}() keyword argument names must be strings"
                )));
            };
            let Some(name) = positional_names
                .iter()
                .copied()
                .find(|name| key.as_str() == *name)
            else {
                return Err(CodemodeError::Invalid(format!(
                    "{function_name}() got unexpected keyword argument '{key}'"
                )));
            };
            if !seen.insert(name) {
                return Err(CodemodeError::Invalid(format!(
                    "{function_name}() got multiple values for argument '{key}'"
                )));
            }
            parsed_keyword.push((name, value));
        }

        Ok(Self {
            function_name,
            positional_names,
            positional,
            keyword: parsed_keyword,
        })
    }

    fn required_string(&self, name: &'static str) -> Result<String, CodemodeError> {
        let value = self.value(name).ok_or_else(|| {
            CodemodeError::Invalid(format!("{}() missing {name} argument", self.function_name))
        })?;
        required_string(value, &format!("{}() {name}", self.function_name))
    }

    fn optional_string(&self, name: &'static str) -> Result<Option<String>, CodemodeError> {
        let Some(value) = self.value(name) else {
            return Ok(None);
        };
        optional_string(value, &format!("{}() {name}", self.function_name))
    }

    fn optional_non_empty_string(
        &self,
        name: &'static str,
    ) -> Result<Option<String>, CodemodeError> {
        let Some(value) = self.value(name) else {
            return Ok(None);
        };
        optional_non_empty_string(value, &format!("{}() {name}", self.function_name))
    }

    fn optional_bool(&self, name: &'static str, default: bool) -> Result<bool, CodemodeError> {
        let Some(value) = self.value(name) else {
            return Ok(default);
        };
        let MontyObject::Bool(value) = value else {
            return Err(CodemodeError::Invalid(format!(
                "{}() {name} must be a boolean",
                self.function_name
            )));
        };
        Ok(*value)
    }

    fn optional_u32(
        &self,
        name: &'static str,
        default: u32,
        min: u32,
        max: u32,
    ) -> Result<u32, CodemodeError> {
        let Some(value) = self.value(name) else {
            return Ok(default);
        };
        let MontyObject::Int(value) = value else {
            return Err(CodemodeError::Invalid(format!(
                "{}() {name} must be an integer",
                self.function_name
            )));
        };
        if *value < i64::from(min) || *value > i64::from(max) {
            return Err(CodemodeError::Invalid(format!(
                "{}() {name} must be between {min} and {max}",
                self.function_name
            )));
        }
        u32::try_from(*value).map_err(|_error| {
            CodemodeError::Invalid(format!(
                "{}() {name} must be between {min} and {max}",
                self.function_name
            ))
        })
    }

    fn optional_catalog_kind(
        &self,
        name: &'static str,
    ) -> Result<Option<CatalogKind>, CodemodeError> {
        let Some(kind) = self.optional_string(name)? else {
            return Ok(None);
        };
        match kind.as_str() {
            "table" => Ok(Some(CatalogKind::Table)),
            "table_function" => Ok(Some(CatalogKind::TableFunction)),
            _ => Err(CodemodeError::Invalid(format!(
                "{}() {name} must be 'table' or 'table_function'",
                self.function_name
            ))),
        }
    }

    fn value(&self, name: &'static str) -> Option<&'a MontyObject> {
        self.positional_names
            .iter()
            .position(|candidate| *candidate == name)
            .and_then(|idx| self.positional.get(idx))
            .or_else(|| {
                self.keyword
                    .iter()
                    .find_map(|(key, value)| (*key == name).then_some(*value))
            })
    }
}

#[derive(Clone, Copy)]
enum CatalogKind {
    Table,
    TableFunction,
}

fn catalog_item_kind(kind: Option<CatalogKind>) -> ProtoCatalogItemKind {
    match kind {
        None => ProtoCatalogItemKind::Unspecified,
        Some(CatalogKind::Table) => ProtoCatalogItemKind::Table,
        Some(CatalogKind::TableFunction) => ProtoCatalogItemKind::TableFunction,
    }
}

fn required_string(value: &MontyObject, context: &str) -> Result<String, CodemodeError> {
    let MontyObject::String(value) = value else {
        return Err(CodemodeError::Invalid(format!(
            "{context} must be a string"
        )));
    };
    let value = value.trim();
    if value.is_empty() {
        return Err(CodemodeError::Invalid(format!(
            "{context} must not be empty"
        )));
    }
    Ok(value.to_string())
}

fn optional_string(value: &MontyObject, context: &str) -> Result<Option<String>, CodemodeError> {
    if matches!(value, MontyObject::None) {
        return Ok(None);
    }
    let MontyObject::String(value) = value else {
        return Err(CodemodeError::Invalid(format!(
            "{context} must be a string"
        )));
    };
    let value = value.trim();
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value.to_string()))
    }
}

fn optional_non_empty_string(
    value: &MontyObject,
    context: &str,
) -> Result<Option<String>, CodemodeError> {
    if matches!(value, MontyObject::None) {
        return Ok(None);
    }
    let MontyObject::String(value) = value else {
        return Err(CodemodeError::Invalid(format!(
            "{context} must be a string"
        )));
    };
    let value = value.trim();
    if value.is_empty() {
        Err(CodemodeError::Invalid(format!(
            "{context} must not be empty"
        )))
    } else {
        Ok(Some(value.to_string()))
    }
}

fn monty_to_json(value: &MontyObject) -> Result<Value, CodemodeError> {
    serde_json::to_value(JsonMontyObject(value))
        .map_err(|error| CodemodeError::Internal(error.to_string()))
}

fn json_to_monty(value: Value) -> Result<MontyObject, CodemodeError> {
    match value {
        Value::Null => Ok(MontyObject::None),
        Value::Bool(value) => Ok(MontyObject::Bool(value)),
        Value::Number(value) => Ok(number_to_monty(&value)),
        Value::String(value) => Ok(MontyObject::String(value)),
        Value::Array(values) => values
            .into_iter()
            .map(json_to_monty)
            .collect::<Result<Vec<_>, _>>()
            .map(MontyObject::List),
        Value::Object(map) => map
            .into_iter()
            .map(|(key, value)| Ok((MontyObject::String(key), json_to_monty(value)?)))
            .collect::<Result<Vec<_>, CodemodeError>>()
            .map(DictPairs::from)
            .map(MontyObject::Dict),
    }
}

fn number_to_monty(value: &serde_json::Number) -> MontyObject {
    if let Some(value) = value.as_i64() {
        MontyObject::Int(value)
    } else if let Some(value) = value.as_f64() {
        MontyObject::Float(value)
    } else {
        MontyObject::String(value.to_string())
    }
}

fn monty_codemode_error(error: &monty::MontyException) -> CodemodeError {
    CodemodeError::Invalid(error.to_string())
}
