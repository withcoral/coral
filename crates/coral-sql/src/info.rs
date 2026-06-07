use coral_exports::SqlProjectionV1;

use crate::metadata::{
    ColumnInfo, TableFunctionArgumentInfo, TableFunctionInfo, TableFunctionResultColumnInfo,
    TableInfo,
};
use crate::runtime::SqlRuntimeBinding;

pub(crate) fn table_info_from_binding(binding: &SqlRuntimeBinding) -> Option<TableInfo> {
    let (schema_name, table_name) = sql_reference_parts(&binding.binding.sql_reference)?;
    Some(TableInfo {
        schema_name,
        table_name,
        description: binding.capability.display.description.clone(),
        guide: String::new(),
        columns: columns_from_projection(&binding.binding.projection),
        required_filters: binding
            .binding
            .projection
            .inputs
            .iter()
            .filter(|input| input.required)
            .map(|input| input.name.clone())
            .collect(),
    })
}

pub(crate) fn table_function_info_from_binding(
    binding: &SqlRuntimeBinding,
) -> Option<TableFunctionInfo> {
    let (schema_name, function_name) = sql_reference_parts(&binding.binding.sql_reference)?;
    Some(TableFunctionInfo {
        schema_name,
        function_name,
        description: binding.capability.display.description.clone(),
        arguments: binding
            .binding
            .projection
            .inputs
            .iter()
            .map(|input| TableFunctionArgumentInfo {
                name: input.name.clone(),
                required: input.required,
                values: Vec::new(),
            })
            .collect(),
        result_columns: binding
            .binding
            .projection
            .columns
            .iter()
            .map(|column| TableFunctionResultColumnInfo {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
                description: column.description.clone(),
            })
            .collect(),
    })
}

fn columns_from_projection(projection: &SqlProjectionV1) -> Vec<ColumnInfo> {
    let mut columns = projection
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| ColumnInfo {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
            is_virtual: false,
            is_required_filter: false,
            description: column.description.clone(),
            ordinal_position: u32::try_from(index).unwrap_or(u32::MAX),
        })
        .collect::<Vec<_>>();
    let output_count = columns.len();
    columns.extend(
        projection
            .inputs
            .iter()
            .enumerate()
            .map(|(index, input)| ColumnInfo {
                name: input.name.clone(),
                data_type: input.data_type.clone(),
                nullable: !input.required,
                is_virtual: true,
                is_required_filter: input.required,
                description: String::new(),
                ordinal_position: u32::try_from(output_count + index).unwrap_or(u32::MAX),
            }),
    );
    columns
}

pub(crate) fn sql_reference_parts(reference: &str) -> Option<(String, String)> {
    let (schema, table) = reference.split_once('.')?;
    if schema.trim().is_empty() || table.trim().is_empty() || table.contains('.') {
        return None;
    }
    Some((schema.to_string(), table.to_string()))
}

pub(crate) fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}
