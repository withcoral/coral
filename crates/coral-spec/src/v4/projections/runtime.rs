use std::collections::HashSet;

use crate::v4::ir::{IrExecutionAttachment, IrInputLocation, IrOperation};
use crate::{
    ColumnSpec, DeclaredDefaultValue, ExprSpec, FilterMode, FilterSpec, FunctionArgBinding,
    ParsedTemplate, RequestSpec, Result, TableFunctionArgSpec,
};

use super::model::{Projection, SqlInputExposure};

pub fn projection_filter_specs(projection: &Projection) -> Vec<FilterSpec> {
    projection
        .inputs
        .iter()
        .filter(|input| input.sql_exposure == SqlInputExposure::Filter)
        .map(|input| FilterSpec {
            name: input.name.clone(),
            data_type: input.data_type,
            required: input.required,
            mode: FilterMode::Equality,
            description: input.description.clone(),
            lookup_key: input.lookup_key,
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
            data_type: input.data_type,
            required: input.required,
            values: Vec::new(),
            default: input.default_value.clone(),
            bind: FunctionArgBinding {
                arg: input.name.clone(),
            },
        })
        .collect()
}

pub fn mcp_projection_arg_specs(projection: &Projection) -> Vec<TableFunctionArgSpec> {
    projection
        .inputs
        .iter()
        .filter(|input| input.sql_exposure == SqlInputExposure::FunctionArg)
        .map(|input| TableFunctionArgSpec {
            name: input.name.clone(),
            data_type: input.data_type,
            required: input.required,
            values: Vec::new(),
            default: input.default_value.clone(),
            bind: FunctionArgBinding {
                arg: input.wire_name.clone(),
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
            data_type: column.data_type,
            nullable: column.nullable,
            r#virtual: false,
            description: column.description.clone(),
            expr: Some(ExprSpec::Path {
                path: column.source_path.clone(),
            }),
            do_not_index: column.do_not_index,
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
                data_type: input.data_type,
                nullable: !input.required,
                r#virtual: true,
                description: input.description.clone(),
                expr: Some(ExprSpec::FromFilter {
                    key: input.name.clone(),
                }),
                do_not_index: false,
            }),
    );
    columns
}

pub fn request_spec_for_projection(
    projection: &Projection,
    operation: &IrOperation,
) -> Result<RequestSpec> {
    let IrExecutionAttachment::Rest(rest) = &operation.execution else {
        return Err(crate::ManifestError::validation(format!(
            "projection '{}' is not backed by a REST operation",
            projection.name
        )));
    };
    let mut path = rest.path_template.clone();
    for input in &projection.inputs {
        if input.source_location == IrInputLocation::Path {
            let replacement = match input.sql_exposure {
                SqlInputExposure::Filter => path_template_token(
                    "filter",
                    &input.name,
                    usable_text_default(input.default_value.as_ref()),
                ),
                SqlInputExposure::FunctionArg => path_template_token(
                    "arg",
                    &input.name,
                    usable_text_default(input.default_value.as_ref()),
                ),
                SqlInputExposure::Internal => continue,
            };
            path = path.replace(&format!("{{{}}}", input.wire_name), &replacement);
        }
    }
    let query = projection
        .inputs
        .iter()
        .filter(|input| input.source_location == IrInputLocation::Query)
        .filter_map(|input| {
            let value = match input.sql_exposure {
                SqlInputExposure::Filter => crate::ValueSourceSpec::Filter {
                    key: input.name.clone(),
                    default: usable_text_default(input.default_value.as_ref())
                        .map(|value| value.value().clone()),
                },
                SqlInputExposure::FunctionArg => crate::ValueSourceSpec::Arg {
                    key: input.name.clone(),
                    default: usable_text_default(input.default_value.as_ref())
                        .map(|value| value.value().clone()),
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

fn usable_text_default(default: Option<&DeclaredDefaultValue>) -> Option<&DeclaredDefaultValue> {
    default.filter(|default| !default.value().is_null())
}

fn path_template_token(
    namespace: &str,
    key: &str,
    default: Option<&DeclaredDefaultValue>,
) -> String {
    default.map_or_else(
        || format!("{{{{{namespace}.{key}}}}}"),
        |default| {
            let rendered_default = match default.value() {
                serde_json::Value::String(value) => value.clone(),
                value => value.to_string(),
            };
            let encoded_default = encode_path_segment_default(&rendered_default);
            format!("{{{{{namespace}.{key}|{encoded_default}}}}}")
        },
    )
}

fn encode_path_segment_default(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    let is_dot_segment = matches!(value, "." | "..");
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'~' => {
                encoded.push(byte as char);
            }
            b'.' if is_dot_segment => encoded.push_str("%252E"),
            _ => push_percent_encoded(&mut encoded, byte),
        }
    }
    encoded
}

fn push_percent_encoded(output: &mut String, byte: u8) {
    output.push('%');
    output.push(hex_digit(byte >> 4));
    output.push(hex_digit(byte & 0x0f));
}

fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => char::from(b'0' + nibble),
        10..=15 => char::from(b'A' + (nibble - 10)),
        _ => unreachable!("hex nibble must be in 0..=15"),
    }
}
