use std::collections::HashSet;

use serde_json::Value;

use crate::v4::ir::{IrExecutionAttachment, IrInputLocation, IrOperation};
use crate::{
    ColumnSpec, ExprSpec, FilterMode, FilterSpec, FunctionArgBinding, ManifestDataType,
    ParsedTemplate, RequestSpec, Result, TableFunctionArgSpec,
};

use super::model::{Projection, SqlInputExposure};
use super::pagination::{pagination_owns_input, pagination_query_param_names};

pub fn projection_filter_specs(projection: &Projection) -> Vec<FilterSpec> {
    let pagination_query_params = pagination_query_param_names(&projection.pagination);
    projection
        .inputs
        .iter()
        .filter(|input| input.sql_exposure == SqlInputExposure::Filter)
        .filter(|input| !pagination_owns_input(input, &pagination_query_params))
        .map(|input| FilterSpec {
            name: input.name.clone(),
            data_type: manifest_data_type_name(input.data_type).to_string(),
            required: input.required,
            mode: FilterMode::Equality,
            description: input.description.clone(),
        })
        .collect()
}

pub fn projection_arg_specs(projection: &Projection) -> Vec<TableFunctionArgSpec> {
    let pagination_query_params = pagination_query_param_names(&projection.pagination);
    projection
        .inputs
        .iter()
        .filter(|input| input.sql_exposure == SqlInputExposure::FunctionArg)
        .filter(|input| !pagination_owns_input(input, &pagination_query_params))
        .map(|input| TableFunctionArgSpec {
            name: input.name.clone(),
            required: input.required,
            values: Vec::new(),
            bind: FunctionArgBinding {
                arg: input.name.clone(),
            },
        })
        .collect()
}

pub fn projection_column_specs(projection: &Projection) -> Vec<ColumnSpec> {
    let pagination_query_params = pagination_query_param_names(&projection.pagination);
    let mut columns = projection
        .columns
        .iter()
        .map(|column| ColumnSpec {
            name: column.name.clone(),
            data_type: manifest_data_type_name(column.data_type).to_string(),
            nullable: column.nullable,
            r#virtual: false,
            description: column.description.clone(),
            expr: Some(ExprSpec::Path {
                path: column.source_path.clone(),
            }),
        })
        .collect::<Vec<_>>();
    let existing = columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<HashSet<_>>();
    columns.extend(
        projection
            .inputs
            .iter()
            .filter(|input| input.sql_exposure == SqlInputExposure::Filter)
            .filter(|input| !pagination_owns_input(input, &pagination_query_params))
            .filter(|input| !existing.contains(&input.name))
            .map(|input| ColumnSpec {
                name: input.name.clone(),
                data_type: manifest_data_type_name(input.data_type).to_string(),
                nullable: !input.required,
                r#virtual: true,
                description: input.description.clone(),
                expr: Some(ExprSpec::FromFilter {
                    key: input.name.clone(),
                }),
            }),
    );
    columns
}

pub fn manifest_data_type_name(data_type: ManifestDataType) -> &'static str {
    match data_type {
        ManifestDataType::Utf8 => "Utf8",
        ManifestDataType::Int64 => "Int64",
        ManifestDataType::Boolean => "Boolean",
        ManifestDataType::Float64 => "Float64",
        ManifestDataType::Timestamp => "Timestamp",
        ManifestDataType::Json => "Json",
    }
}

pub fn request_spec_for_projection(
    projection: &Projection,
    operation: &IrOperation,
) -> Result<RequestSpec> {
    let IrExecutionAttachment::Rest(rest) = &operation.execution;
    let pagination_query_params = pagination_query_param_names(&projection.pagination);
    let mut path = rest.path_template.clone();
    for input in &projection.inputs {
        if input.source_location == IrInputLocation::Path {
            let replacement = match input.sql_exposure {
                SqlInputExposure::Filter => format!("{{{{filter.{}}}}}", input.name),
                SqlInputExposure::FunctionArg => format!("{{{{arg.{}}}}}", input.name),
                SqlInputExposure::Internal => continue,
            };
            path = path.replace(&format!("{{{}}}", input.wire_name), &replacement);
        }
    }
    let query = projection
        .inputs
        .iter()
        .filter(|input| input.source_location == IrInputLocation::Query)
        .filter(|input| !pagination_owns_input(input, &pagination_query_params))
        .filter_map(|input| {
            let value = match input.sql_exposure {
                SqlInputExposure::Filter => crate::ValueSourceSpec::Filter {
                    key: input.name.clone(),
                    default: input
                        .default_value
                        .as_ref()
                        .map(|value| Value::String(value.clone())),
                },
                SqlInputExposure::FunctionArg => crate::ValueSourceSpec::Arg {
                    key: input.name.clone(),
                    default: input
                        .default_value
                        .as_ref()
                        .map(|value| Value::String(value.clone())),
                },
                SqlInputExposure::Internal => return None,
            };
            Some(crate::QueryParamSpec {
                name: input.wire_name.clone(),
                value,
            })
        })
        .collect();
    Ok(RequestSpec {
        method: crate::HttpMethod::GET,
        path: ParsedTemplate::parse(&path)?,
        query,
        body: crate::BodySpec::default(),
        headers: Vec::new(),
    })
}
