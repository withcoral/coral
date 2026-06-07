use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::{SqlError, SqlResult};

/// Validate that a SQL string contains exactly one read-only query statement.
///
/// # Errors
///
/// Returns [`SqlError::InvalidInput`] when the statement is not a read-only
/// query.
pub fn validate_read_only_sql(sql: &str) -> SqlResult<()> {
    if first_sql_keyword(sql).is_some_and(|keyword| keyword.eq_ignore_ascii_case("COPY")) {
        return Err(SqlError::InvalidInput(
            "DML not supported: COPY".to_string(),
        ));
    }
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|error| SqlError::InvalidInput(format!("invalid SQL: {error}")))?;
    let [statement] = statements.as_slice() else {
        return Err(SqlError::InvalidInput(
            "exactly one SQL statement is supported".to_string(),
        ));
    };
    validate_read_only_statement(statement)
}

fn validate_read_only_statement(statement: &Statement) -> SqlResult<()> {
    match statement {
        Statement::Query(_)
        | Statement::ExplainTable { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowFunctions { .. } => Ok(()),
        Statement::Explain { statement, .. } => validate_read_only_statement(statement),
        Statement::Copy { .. } | Statement::CopyIntoSnowflake { .. } => Err(
            SqlError::InvalidInput("DML not supported: COPY".to_string()),
        ),
        Statement::Insert(_) => Err(SqlError::InvalidInput(
            "DML not supported: INSERT".to_string(),
        )),
        Statement::Update { .. } => Err(SqlError::InvalidInput(
            "DML not supported: UPDATE".to_string(),
        )),
        Statement::Delete(_) => Err(SqlError::InvalidInput(
            "DML not supported: DELETE".to_string(),
        )),
        Statement::Truncate(_) => Err(SqlError::InvalidInput(
            "DML not supported: TRUNCATE".to_string(),
        )),
        Statement::CreateTable(_)
        | Statement::CreateView(_)
        | Statement::CreateIndex(_)
        | Statement::CreateVirtualTable { .. }
        | Statement::CreateRole(_)
        | Statement::CreateSecret { .. }
        | Statement::CreateServer(_)
        | Statement::CreatePolicy(_)
        | Statement::CreateConnector(_)
        | Statement::Drop { .. }
        | Statement::DropFunction(_)
        | Statement::DropProcedure { .. }
        | Statement::DropDomain(_)
        | Statement::DropExtension(_)
        | Statement::DropPolicy(_)
        | Statement::DropSecret { .. }
        | Statement::DropTrigger(_)
        | Statement::DropConnector { .. }
        | Statement::AlterTable(_)
        | Statement::AlterSchema(_)
        | Statement::AlterIndex { .. }
        | Statement::AlterView { .. }
        | Statement::AlterType(_)
        | Statement::AlterRole { .. }
        | Statement::AlterPolicy(_)
        | Statement::AlterConnector { .. } => {
            Err(SqlError::InvalidInput("DDL not supported".to_string()))
        }
        Statement::Set(_) => Err(SqlError::InvalidInput(
            "Statement not supported: SET".to_string(),
        )),
        _ => Err(SqlError::InvalidInput(
            "Statement not supported".to_string(),
        )),
    }
}

fn first_sql_keyword(sql: &str) -> Option<&str> {
    sql.trim_start()
        .split(|ch: char| ch.is_whitespace() || ch == ';' || ch == '(')
        .find(|part| !part.is_empty())
}
