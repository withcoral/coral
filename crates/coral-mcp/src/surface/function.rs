use coral_api::v1::{Function, FunctionArgument, TableFunctionResultColumn, function};
use coral_client::{format_schema_table_equivalent, format_sql_identifier};
use rmcp::{
    ErrorData,
    model::{Tool, ToolAnnotations},
};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::{Map, Value};

use super::{
    arguments::{optional_bool_argument, required_string_argument},
    schema::{tool_input_schema, tool_output_schema},
    tool_names::ToolName,
};

const DEFAULT_REPLACE_EXISTING: bool = false;

#[derive(JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct AddFunctionArguments {
    #[schemars(
        length(min = 1),
        pattern(r"^[a-z_][a-z0-9_]*$"),
        description = "Lowercase SQL schema where the table function will be published."
    )]
    pub(crate) schema: String,
    #[schemars(
        length(min = 1),
        pattern(r"^[a-z_][a-z0-9_]*$"),
        description = "Lowercase function name."
    )]
    pub(crate) name: String,
    #[schemars(length(min = 1), description = "What the reusable function returns.")]
    pub(crate) description: String,
    #[schemars(
        length(min = 1),
        description = "One read-only Coral SQL query without function frontmatter. Use $name placeholders for scalar values that callers must supply. Add an explicit cast when SQL context does not determine a placeholder's type. Placeholders cannot replace SQL identifiers."
    )]
    pub(crate) sql: String,
    #[schemars(
        default = "default_replace_existing",
        description = "Replace a function with the same name. Defaults to false; when false, an existing function is left unchanged."
    )]
    pub(crate) replace_existing: bool,
}

fn default_replace_existing() -> bool {
    DEFAULT_REPLACE_EXISTING
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct FunctionAddedValue<'a> {
    schema_name: &'a str,
    function_name: &'a str,
    description: &'a str,
    arguments: Vec<FunctionArgumentValue<'a>>,
    result_columns: Vec<FunctionResultColumnValue<'a>>,
    sql_reference: String,
    sql_call_example: String,
    replaced: bool,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct FunctionArgumentValue<'a> {
    name: &'a str,
    data_type: &'a str,
}

impl<'a> From<&'a FunctionArgument> for FunctionArgumentValue<'a> {
    fn from(argument: &'a FunctionArgument) -> Self {
        Self {
            name: &argument.name,
            data_type: &argument.data_type,
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct FunctionResultColumnValue<'a> {
    column_name: &'a str,
    data_type: &'a str,
    is_nullable: bool,
    description: &'a str,
}

impl<'a> From<&'a TableFunctionResultColumn> for FunctionResultColumnValue<'a> {
    fn from(column: &'a TableFunctionResultColumn) -> Self {
        Self {
            column_name: &column.name,
            data_type: &column.data_type,
            is_nullable: column.nullable,
            description: &column.description,
        }
    }
}

pub(crate) fn add_function_tool() -> Tool {
    Tool::new(
        ToolName::AddFunction.as_str(),
        "Create a reusable table function in the current Coral workspace from one read-only SQL query. Use this when a SQL pattern developed during the task is likely to be useful again. Values written as $placeholders become required named arguments. Coral validates the function before persisting it.",
        tool_input_schema::<AddFunctionArguments>(),
    )
    .with_raw_output_schema(tool_output_schema::<FunctionAddedValue<'static>>())
    .with_annotations(
        ToolAnnotations::with_title("Add Function")
            .read_only(false)
            .destructive(true)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn add_function_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<AddFunctionArguments, ErrorData> {
    Ok(AddFunctionArguments {
        schema: required_string_argument(arguments, "schema")?,
        name: required_string_argument(arguments, "name")?,
        description: required_string_argument(arguments, "description")?,
        sql: required_string_argument(arguments, "sql")?,
        replace_existing: optional_bool_argument(
            arguments,
            "replace_existing",
            DEFAULT_REPLACE_EXISTING,
        )?,
    })
}

pub(crate) fn render_function_artifact(
    arguments: &AddFunctionArguments,
) -> Result<String, serde_json::Error> {
    let name = comment_safe_yaml_string(&arguments.name)?;
    let schema = comment_safe_yaml_string(&arguments.schema)?;
    let description = comment_safe_yaml_string(&arguments.description)?;
    let sql = arguments.sql.trim();

    Ok(format!(
        r"/*
name: {name}
schema: {schema}
description: {description}
*/

{sql}
"
    ))
}

fn comment_safe_yaml_string(value: &str) -> Result<String, serde_json::Error> {
    let encoded = serde_json::to_string(value)?;
    Ok(encoded.replace("*/", "*\\u002f"))
}

pub(crate) fn function_added_value(
    function: &Function,
    replaced: bool,
) -> Result<Value, tonic::Status> {
    let ready = match function.runtime.as_ref() {
        Some(function::Runtime::Ready(ready)) => ready,
        Some(function::Runtime::Invalid(_)) => {
            return Err(tonic::Status::internal(
                "add function response returned an invalid function",
            ));
        }
        None => {
            return Err(tonic::Status::internal(
                "add function response missing runtime status",
            ));
        }
    };
    let table_function = ready
        .table_function
        .as_ref()
        .ok_or_else(|| tonic::Status::internal("add function response missing publish target"))?;
    let sql_reference =
        format_schema_table_equivalent(&table_function.schema_name, &table_function.name);
    let arguments = ready
        .arguments
        .iter()
        .map(|argument| format!("{} => '<value>'", format_sql_identifier(&argument.name)))
        .collect::<Vec<_>>()
        .join(", ");
    serde_json::to_value(FunctionAddedValue {
        schema_name: &table_function.schema_name,
        function_name: &table_function.name,
        description: &ready.description,
        arguments: ready
            .arguments
            .iter()
            .map(FunctionArgumentValue::from)
            .collect(),
        result_columns: ready
            .result_columns
            .iter()
            .map(FunctionResultColumnValue::from)
            .collect(),
        sql_call_example: format!("{sql_reference}({arguments})"),
        sql_reference,
        replaced,
    })
    .map_err(|error| tonic::Status::internal(error.to_string()))
}

#[cfg(test)]
mod tests {
    use coral_spec::parse_function_sql;

    use super::{AddFunctionArguments, render_function_artifact};

    fn arguments(description: &str) -> AddFunctionArguments {
        AddFunctionArguments {
            schema: "functions".to_string(),
            name: "open_prs".to_string(),
            description: description.to_string(),
            sql: "  select cast($owner as VARCHAR) as owner  ".to_string(),
            replace_existing: false,
        }
    }

    #[test]
    fn render_function_artifact_uses_the_canonical_layout() {
        let artifact = render_function_artifact(&arguments("Open pull requests"))
            .expect("function artifact should render");

        assert_eq!(
            artifact,
            r#"/*
name: "open_prs"
schema: "functions"
description: "Open pull requests"
*/

select cast($owner as VARCHAR) as owner
"#
        );
    }

    #[test]
    fn render_function_artifact_round_trips_comment_sensitive_metadata() {
        let description = "Owner's */ queue\nwith \\ paths";
        let artifact = render_function_artifact(&arguments(description))
            .expect("function artifact should render");
        let function = parse_function_sql(&artifact).expect("rendered artifact should parse");

        assert_eq!(function.name(), "open_prs");
        assert_eq!(function.schema(), "functions");
        assert_eq!(function.description(), description);
        assert_eq!(
            function.implementation().coral_sql.query,
            "select cast($owner as VARCHAR) as owner"
        );
    }
}
