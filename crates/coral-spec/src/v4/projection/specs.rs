use std::collections::HashSet;

use serde_json::Value;

use crate::{
    BodySpec, ColumnSpec, ExprSpec, FilterMode, FilterSpec, FunctionArgBinding,
    HttpMethod as RequestHttpMethod, ParsedTemplate, QueryParamSpec, RequestSpec, Result,
    TableFunctionArgSpec, ValueSourceSpec,
};

use super::super::ir::{IrExecutionAttachment, IrOperation, OpenApiParameterLocation};
use super::model::{Projection, SqlInputExposure};
use super::types::manifest_data_type_name;

pub fn projection_filter_specs(projection: &Projection) -> Vec<FilterSpec> {
    projection
        .inputs
        .iter()
        .filter(|input| input.sql_exposure == SqlInputExposure::Filter)
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
    projection
        .inputs
        .iter()
        .filter(|input| input.sql_exposure == SqlInputExposure::FunctionArg)
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

pub fn request_spec_for_projection(
    projection: &Projection,
    operation: &IrOperation,
) -> Result<RequestSpec> {
    let IrExecutionAttachment::Rest(rest) = &operation.execution;
    let mut path = rest.path_template.clone();
    for input in &projection.inputs {
        if input.source_location == OpenApiParameterLocation::Path {
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
        .filter(|input| input.source_location == OpenApiParameterLocation::Query)
        .map(|input| QueryParamSpec {
            name: input.wire_name.clone(),
            value: match input.sql_exposure {
                SqlInputExposure::Filter => ValueSourceSpec::Filter {
                    key: input.name.clone(),
                    default: input
                        .default_value
                        .as_ref()
                        .map(|value| Value::String(value.clone())),
                },
                SqlInputExposure::FunctionArg => ValueSourceSpec::Arg {
                    key: input.name.clone(),
                    default: input
                        .default_value
                        .as_ref()
                        .map(|value| Value::String(value.clone())),
                },
                SqlInputExposure::Internal => ValueSourceSpec::Literal { value: Value::Null },
            },
        })
        .collect();
    Ok(RequestSpec {
        method: RequestHttpMethod::GET,
        path: ParsedTemplate::parse(&path)?,
        query,
        body: BodySpec::default(),
        headers: Vec::new(),
    })
}
