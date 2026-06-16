use std::collections::BTreeSet;

use serde_json::{Map, Value};

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{
    IrEntityCandidate, IrOperationOutput, OutputCardinality, RestResponseAttachment,
};
use crate::{
    ResponseBodyFormat, ResponseSpec, XmlFieldSpec, XmlJsonSpec, XmlListSpec, XmlObjectSpec,
    XmlResponseSpec, XmlScalarSpec, XmlScalarType, XmlValueSpec,
};

use super::import::OpenApiImporter;

pub(super) struct ImportedResponseVariant {
    pub(super) name_suffix: String,
    pub(super) output: IrOperationOutput,
    pub(super) response: RestResponseAttachment,
    pub(super) entity: Option<IrEntityCandidate>,
}

#[derive(Debug, Clone)]
struct ClassifiedResponse {
    name_suffix: String,
    cardinality: OutputCardinality,
    row_path: Vec<String>,
    row_schema: Value,
    entity_name: Option<String>,
    xml: Option<XmlResponseSpec>,
}

#[derive(Debug, Clone)]
struct XmlCollectionCandidate {
    name: String,
    item_schema: Value,
    entity_name: Option<String>,
}

impl OpenApiImporter<'_> {
    #[expect(
        clippy::too_many_lines,
        reason = "Response import has to keep JSON and XML variant assembly close to preserve response-selection behavior."
    )]
    pub(super) fn import_response_variants(
        &mut self,
        path: &str,
        operation: &Map<String, Value>,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Vec<ImportedResponseVariant> {
        let Some(selected) = self.select_success_response(
            operation.get("responses").and_then(Value::as_object),
            operation_id,
            diagnostics,
        ) else {
            let response = ResponseSpec::default();
            return vec![ImportedResponseVariant {
                name_suffix: "response".to_string(),
                output: IrOperationOutput {
                    cardinality: OutputCardinality::None,
                    type_ref: "none".to_string(),
                    row_path: Vec::new(),
                },
                response: RestResponseAttachment {
                    status_code: 204,
                    media_type: "application/json".to_string(),
                    response,
                },
                entity: None,
            }];
        };

        let Some(resolved) = self.resolve_ref(&selected.schema, operation_id, diagnostics) else {
            diagnostics.push(Diagnostic::warning(
                "OPENAPI_RESPONSE_SCHEMA_UNRESOLVED",
                format!("operation '{operation_id}' response schema could not be resolved"),
                self.surface.id.clone(),
                Some(operation_id.to_string()),
            ));
            return vec![ImportedResponseVariant {
                name_suffix: "response".to_string(),
                output: IrOperationOutput {
                    cardinality: OutputCardinality::Unknown,
                    type_ref: "json".to_string(),
                    row_path: Vec::new(),
                },
                response: RestResponseAttachment {
                    status_code: selected.status_code,
                    media_type: selected.media_type,
                    response: ResponseSpec {
                        format: selected.format,
                        ..ResponseSpec::default()
                    },
                },
                entity: None,
            }];
        };

        let classified = match selected.format {
            ResponseBodyFormat::Json | ResponseBodyFormat::JsonEachRow => {
                let (cardinality, row_path, row_schema, entity_name) =
                    classify_response_schema(path, &resolved);
                vec![ClassifiedResponse {
                    name_suffix: entity_name
                        .clone()
                        .unwrap_or_else(|| entity_name_from_path(path)),
                    cardinality,
                    row_path,
                    row_schema,
                    entity_name,
                    xml: None,
                }]
            }
            ResponseBodyFormat::Xml => {
                let root_name = response_root_name(
                    path,
                    &selected.schema,
                    &resolved,
                    selected.schema_name.as_deref(),
                );
                let xml = self.build_xml_response_spec(
                    &selected.schema,
                    root_name.clone(),
                    operation_id,
                    diagnostics,
                );
                self.classify_xml_response_variants(
                    path,
                    &selected.schema,
                    &root_name,
                    xml,
                    operation_id,
                    diagnostics,
                )
            }
        };

        classified
            .into_iter()
            .map(|classified| {
                let name_suffix = classified.name_suffix;
                let type_ref = self
                    .import_schema(
                        &classified.row_schema,
                        &format!("{operation_id}_{name_suffix}_row"),
                        operation_id,
                        diagnostics,
                    )
                    .unwrap_or_else(|| "json".to_string());
                let response = ResponseSpec {
                    format: selected.format,
                    xml: classified.xml,
                    rows_path: classified.row_path.clone(),
                    ..ResponseSpec::default()
                };
                let entity = (classified.cardinality != OutputCardinality::None
                    && classified.cardinality != OutputCardinality::Unknown)
                    .then(|| IrEntityCandidate {
                        name: classified
                            .entity_name
                            .unwrap_or_else(|| entity_name_from_path(path)),
                        type_ref: type_ref.clone(),
                        identity_fields: vec!["id".to_string()],
                    });
                ImportedResponseVariant {
                    name_suffix,
                    output: IrOperationOutput {
                        cardinality: classified.cardinality,
                        type_ref,
                        row_path: classified.row_path,
                    },
                    response: RestResponseAttachment {
                        status_code: selected.status_code,
                        media_type: selected.media_type.clone(),
                        response,
                    },
                    entity,
                }
            })
            .collect()
    }

    fn select_success_response(
        &self,
        responses: Option<&Map<String, Value>>,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<SelectedResponse> {
        let responses = responses?;
        let mut candidates = Vec::new();
        for (status, response) in responses {
            let Some(status) = success_response_status(status) else {
                continue;
            };
            let Some(response) = self.resolve_ref(response, operation_id, diagnostics) else {
                continue;
            };
            let Some(content) = response.get("content").and_then(Value::as_object) else {
                continue;
            };
            for (media_type, media) in content {
                let Some((format, media_rank)) = response_media_format(media_type) else {
                    continue;
                };
                candidates.push(SelectedResponse {
                    status_code: status.representative_status_code(),
                    status_rank: status.preference_rank(),
                    media_type: media_type.clone(),
                    media_rank,
                    format,
                    schema: media.get("schema").cloned().unwrap_or(Value::Null),
                    schema_name: media
                        .get("schema")
                        .and_then(Value::as_object)
                        .and_then(|schema| schema.get("$ref"))
                        .and_then(Value::as_str)
                        .map(entity_name_from_ref),
                });
            }
        }
        candidates.into_iter().min_by_key(|candidate| {
            (
                candidate.media_rank,
                candidate.status_rank,
                candidate.status_code,
            )
        })
    }

    #[expect(
        clippy::too_many_lines,
        reason = "XML row classification has several conservative schema branches that are clearer kept together."
    )]
    fn classify_xml_response_variants(
        &self,
        path: &str,
        schema: &Value,
        root_name: &str,
        xml: Option<XmlResponseSpec>,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Vec<ClassifiedResponse> {
        let Some(effective) = self.effective_schema(schema, operation_id, diagnostics) else {
            return vec![ClassifiedResponse {
                name_suffix: entity_name_from_path(path),
                cardinality: OutputCardinality::Unknown,
                row_path: Vec::new(),
                row_schema: schema.clone(),
                entity_name: None,
                xml,
            }];
        };
        if effective == Value::Null {
            return vec![ClassifiedResponse {
                name_suffix: "response".to_string(),
                cardinality: OutputCardinality::None,
                row_path: Vec::new(),
                row_schema: Value::Null,
                entity_name: None,
                xml,
            }];
        }
        if schema_type(&effective) == "array" {
            let item = effective.get("items").cloned().unwrap_or(Value::Null);
            return vec![ClassifiedResponse {
                name_suffix: item
                    .get("$ref")
                    .and_then(Value::as_str)
                    .map_or_else(|| root_name.to_string(), entity_name_from_ref),
                cardinality: OutputCardinality::List,
                row_path: vec![root_name.to_string()],
                row_schema: item.clone(),
                entity_name: item
                    .get("$ref")
                    .and_then(Value::as_str)
                    .map(entity_name_from_ref),
                xml,
            }];
        }
        if schema_type(&effective) == "object" {
            let collections = self.xml_collection_properties(&effective, operation_id, diagnostics);
            if !collections.is_empty() {
                return collections
                    .into_iter()
                    .map(|collection| {
                        let name_suffix = collection
                            .entity_name
                            .clone()
                            .unwrap_or_else(|| collection.name.clone());
                        ClassifiedResponse {
                            name_suffix,
                            cardinality: OutputCardinality::WrappedList,
                            row_path: vec![root_name.to_string(), collection.name],
                            row_schema: collection.item_schema,
                            entity_name: collection.entity_name,
                            xml: xml.clone(),
                        }
                    })
                    .collect();
            }

            if let Some((property_name, property_schema)) =
                self.single_xml_payload_object_property(&effective, operation_id, diagnostics)
            {
                let property_schema = self
                    .effective_schema(&property_schema, operation_id, diagnostics)
                    .unwrap_or(property_schema);
                let collections =
                    self.xml_collection_properties(&property_schema, operation_id, diagnostics);
                if !collections.is_empty() {
                    return collections
                        .into_iter()
                        .map(|collection| {
                            let name_suffix = collection
                                .entity_name
                                .clone()
                                .unwrap_or_else(|| collection.name.clone());
                            ClassifiedResponse {
                                name_suffix,
                                cardinality: OutputCardinality::WrappedList,
                                row_path: vec![
                                    root_name.to_string(),
                                    property_name.clone(),
                                    collection.name,
                                ],
                                row_schema: collection.item_schema,
                                entity_name: collection.entity_name,
                                xml: xml.clone(),
                            }
                        })
                        .collect();
                }
                let entity_name = property_name.clone();
                return vec![ClassifiedResponse {
                    name_suffix: entity_name.clone(),
                    cardinality: OutputCardinality::Singleton,
                    row_path: vec![root_name.to_string(), property_name],
                    row_schema: property_schema.clone(),
                    entity_name: property_schema
                        .get("$ref")
                        .and_then(Value::as_str)
                        .map(entity_name_from_ref)
                        .or(Some(entity_name)),
                    xml,
                }];
            }

            return vec![ClassifiedResponse {
                name_suffix: schema
                    .get("$ref")
                    .and_then(Value::as_str)
                    .map_or_else(|| entity_name_from_path(path), entity_name_from_ref),
                cardinality: OutputCardinality::Singleton,
                row_path: vec![root_name.to_string()],
                row_schema: effective.clone(),
                entity_name: schema
                    .get("$ref")
                    .and_then(Value::as_str)
                    .map(entity_name_from_ref)
                    .or_else(|| Some(entity_name_from_path(path))),
                xml,
            }];
        }
        vec![ClassifiedResponse {
            name_suffix: entity_name_from_path(path),
            cardinality: OutputCardinality::Unknown,
            row_path: Vec::new(),
            row_schema: effective,
            entity_name: None,
            xml,
        }]
    }

    fn xml_collection_properties(
        &self,
        schema: &Value,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Vec<XmlCollectionCandidate> {
        let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
            return Vec::new();
        };
        properties
            .iter()
            .filter(|(name, _)| !is_xml_wrapper_metadata_property(name))
            .filter_map(|(name, property)| {
                let effective = self.effective_schema(property, operation_id, diagnostics)?;
                if schema_type(&effective) != "array" {
                    return None;
                }
                let item_schema = effective.get("items").cloned().unwrap_or(Value::Null);
                let name = property_xml_name(property, &effective, name);
                let entity_name = item_schema
                    .get("$ref")
                    .and_then(Value::as_str)
                    .map(entity_name_from_ref);
                Some(XmlCollectionCandidate {
                    name,
                    item_schema,
                    entity_name,
                })
            })
            .collect()
    }

    fn single_xml_payload_object_property(
        &self,
        schema: &Value,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<(String, Value)> {
        let properties = schema.get("properties").and_then(Value::as_object)?;
        let payloads = properties
            .iter()
            .filter(|(name, _)| !is_xml_wrapper_metadata_property(name))
            .filter_map(|(name, property)| {
                let effective = self.effective_schema(property, operation_id, diagnostics)?;
                (schema_type(&effective) == "object").then(|| {
                    (
                        property_xml_name(property, &effective, name),
                        property.clone(),
                    )
                })
            })
            .collect::<Vec<_>>();
        match payloads.as_slice() {
            [(name, property)] => Some((name.clone(), property.clone())),
            [] | [_, _, ..] => None,
        }
    }

    fn build_xml_response_spec(
        &self,
        schema: &Value,
        root_name: String,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<XmlResponseSpec> {
        let mut seen_refs = BTreeSet::new();
        let root = self.build_xml_value_spec(
            schema,
            &root_name,
            operation_id,
            diagnostics,
            &mut seen_refs,
        )?;
        Some(XmlResponseSpec {
            root_name: Some(root_name),
            root,
        })
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Recursive XML plan construction mirrors OpenAPI schema branches and is easier to audit in one place."
    )]
    fn build_xml_value_spec(
        &self,
        schema: &Value,
        fallback_name: &str,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
        seen_refs: &mut BTreeSet<String>,
    ) -> Option<XmlValueSpec> {
        let reference = schema
            .get("$ref")
            .and_then(Value::as_str)
            .map(str::to_string);
        if let Some(reference) = &reference
            && !seen_refs.insert(reference.clone())
        {
            return Some(XmlValueSpec::Json(XmlJsonSpec {
                xml_name: Some(local_name(fallback_name).to_string()),
            }));
        }

        let effective = self.effective_schema(schema, operation_id, diagnostics)?;
        let xml_name = property_xml_name(schema, &effective, fallback_name);
        let spec = match schema_type(&effective) {
            "array" => {
                let items = effective.get("items").unwrap_or(&Value::Null);
                let wrapped = xml_wrapped(schema)
                    .or_else(|| xml_wrapped(&effective))
                    .unwrap_or(true);
                let item_effective = self
                    .effective_schema(items, operation_id, diagnostics)
                    .unwrap_or_else(|| items.clone());
                let item_xml_name = array_item_xml_name(
                    schema,
                    &effective,
                    items,
                    &item_effective,
                    &xml_name,
                    fallback_name,
                    wrapped,
                );
                let item = self
                    .build_xml_value_spec(
                        items,
                        &item_xml_name,
                        operation_id,
                        diagnostics,
                        seen_refs,
                    )
                    .unwrap_or_else(|| {
                        XmlValueSpec::Json(XmlJsonSpec {
                            xml_name: Some(item_xml_name.clone()),
                        })
                    });
                XmlValueSpec::List(XmlListSpec {
                    xml_name: Some(xml_name),
                    wrapped,
                    item_xml_name: Some(item_xml_name),
                    item: Box::new(item),
                })
            }
            "object" => {
                let fields = effective
                    .get("properties")
                    .and_then(Value::as_object)
                    .map(|properties| {
                        properties
                            .iter()
                            .filter_map(|(name, property)| {
                                let property_effective =
                                    self.effective_schema(property, operation_id, diagnostics)?;
                                let field_xml_name =
                                    property_xml_name(property, &property_effective, name);
                                let attribute =
                                    xml_attribute(property).or_else(|| xml_attribute(&property_effective));
                                let value = self
                                    .build_xml_value_spec(
                                        property,
                                        &field_xml_name,
                                        operation_id,
                                        diagnostics,
                                        seen_refs,
                                    )
                                    .unwrap_or_else(|| {
                                        XmlValueSpec::Json(XmlJsonSpec {
                                            xml_name: Some(field_xml_name.clone()),
                                        })
                                    });
                                if attribute == Some(true) && !matches!(value, XmlValueSpec::Scalar(_))
                                {
                                    diagnostics.push(Diagnostic::warning(
                                        "OPENAPI_XML_ATTRIBUTE_NON_SCALAR",
                                        format!(
                                            "operation '{operation_id}' XML attribute field '{name}' is not scalar"
                                        ),
                                        self.surface.id.clone(),
                                        Some(operation_id.to_string()),
                                    ));
                                }
                                Some(XmlFieldSpec {
                                    name: name.clone(),
                                    xml_name: field_xml_name,
                                    attribute: attribute.unwrap_or(false),
                                    value,
                                })
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                if fields.is_empty()
                    && effective
                        .get("additionalProperties")
                        .is_some_and(|additional| additional.as_bool() != Some(false))
                {
                    XmlValueSpec::Json(XmlJsonSpec {
                        xml_name: Some(xml_name),
                    })
                } else {
                    XmlValueSpec::Object(XmlObjectSpec {
                        xml_name: Some(xml_name),
                        fields,
                    })
                }
            }
            "integer" => XmlValueSpec::Scalar(XmlScalarSpec {
                xml_name: Some(xml_name),
                scalar_type: XmlScalarType::Integer,
            }),
            "number" => XmlValueSpec::Scalar(XmlScalarSpec {
                xml_name: Some(xml_name),
                scalar_type: XmlScalarType::Number,
            }),
            "boolean" => XmlValueSpec::Scalar(XmlScalarSpec {
                xml_name: Some(xml_name),
                scalar_type: XmlScalarType::Boolean,
            }),
            "string" => XmlValueSpec::Scalar(XmlScalarSpec {
                xml_name: Some(xml_name),
                scalar_type: XmlScalarType::String,
            }),
            _ => {
                if effective.get("enum").and_then(Value::as_array).is_some() {
                    XmlValueSpec::Scalar(XmlScalarSpec {
                        xml_name: Some(xml_name),
                        scalar_type: XmlScalarType::String,
                    })
                } else {
                    XmlValueSpec::Json(XmlJsonSpec {
                        xml_name: Some(xml_name),
                    })
                }
            }
        };

        if let Some(reference) = reference {
            seen_refs.remove(&reference);
        }
        Some(spec)
    }
}

#[derive(Debug, Clone)]
struct SelectedResponse {
    status_code: u16,
    status_rank: u16,
    media_type: String,
    media_rank: u8,
    format: ResponseBodyFormat,
    schema: Value,
    schema_name: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum SuccessResponseStatus {
    Numeric(u16),
    Range2xx,
}

impl SuccessResponseStatus {
    fn representative_status_code(self) -> u16 {
        match self {
            Self::Numeric(status_code) => status_code,
            Self::Range2xx => 200,
        }
    }

    fn preference_rank(self) -> u16 {
        match self {
            Self::Numeric(200) => 0,
            Self::Numeric(status_code) => status_code,
            Self::Range2xx => u16::MAX,
        }
    }
}

fn success_response_status(status: &str) -> Option<SuccessResponseStatus> {
    if let Ok(status_code) = status.parse::<u16>() {
        return (200..300)
            .contains(&status_code)
            .then_some(SuccessResponseStatus::Numeric(status_code));
    }
    status
        .eq_ignore_ascii_case("2XX")
        .then_some(SuccessResponseStatus::Range2xx)
}

fn response_media_format(media_type: &str) -> Option<(ResponseBodyFormat, u8)> {
    let normalized = media_type
        .split(';')
        .next()
        .unwrap_or(media_type)
        .trim()
        .to_ascii_lowercase();
    match normalized.as_str() {
        "application/json" => Some((ResponseBodyFormat::Json, 0)),
        "text/xml" | "application/xml" => Some((ResponseBodyFormat::Xml, 2)),
        other if other.ends_with("+json") => Some((ResponseBodyFormat::Json, 1)),
        other if other.ends_with("+xml") || other.ends_with("/xml") => {
            Some((ResponseBodyFormat::Xml, 3))
        }
        _ => None,
    }
}

fn classify_response_schema(
    path: &str,
    schema: &Value,
) -> (OutputCardinality, Vec<String>, Value, Option<String>) {
    if schema == &Value::Null {
        return (OutputCardinality::None, Vec::new(), Value::Null, None);
    }
    if schema.get("type").and_then(Value::as_str) == Some("array") {
        let item = schema.get("items").cloned().unwrap_or(Value::Null);
        return (
            OutputCardinality::List,
            Vec::new(),
            item.clone(),
            item.get("$ref")
                .and_then(Value::as_str)
                .map(entity_name_from_ref),
        );
    }
    if schema
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("object")
        == "object"
    {
        if let Some((property_name, items)) = schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(wrapped_list_property)
        {
            let item = items.get("items").cloned().unwrap_or(Value::Null);
            return (
                OutputCardinality::WrappedList,
                vec![property_name.to_string()],
                item.clone(),
                item.get("$ref")
                    .and_then(Value::as_str)
                    .map(entity_name_from_ref),
            );
        }
        return (
            OutputCardinality::Singleton,
            Vec::new(),
            schema.clone(),
            schema
                .get("$ref")
                .and_then(Value::as_str)
                .map(entity_name_from_ref)
                .or_else(|| Some(entity_name_from_path(path))),
        );
    }
    (OutputCardinality::Unknown, Vec::new(), schema.clone(), None)
}

fn wrapped_list_property(properties: &Map<String, Value>) -> Option<(&str, &Value)> {
    ["items", "data", "results", "rows"]
        .iter()
        .find_map(|name| {
            properties
                .get(*name)
                .filter(|property| property.get("type").and_then(Value::as_str) == Some("array"))
                .map(|property| (*name, property))
        })
        .or_else(|| single_array_payload_property(properties))
}

fn single_array_payload_property(properties: &Map<String, Value>) -> Option<(&str, &Value)> {
    let array_properties = properties
        .iter()
        .filter(|(_, property)| property.get("type").and_then(Value::as_str) == Some("array"))
        .filter(|(name, _)| !is_wrapper_metadata_property(name))
        .collect::<Vec<_>>();
    match array_properties.as_slice() {
        [(name, property)] => Some((name.as_str(), *property)),
        [] | [_, _, ..] => None,
    }
}

fn is_wrapper_metadata_property(name: &str) -> bool {
    matches!(
        name,
        "total_count" | "incomplete_results" | "has_more" | "next" | "previous"
    )
}

fn is_xml_wrapper_metadata_property(name: &str) -> bool {
    let normalized = name.to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "responsemetadata"
            | "response_metadata"
            | "metadata"
            | "total_count"
            | "incomplete_results"
            | "has_more"
            | "next"
            | "previous"
    )
}

fn response_root_name(
    path: &str,
    schema: &Value,
    resolved: &Value,
    schema_name: Option<&str>,
) -> String {
    schema_xml_name(schema)
        .or_else(|| schema_xml_name(resolved))
        .or(schema_name)
        .map(local_name)
        .map_or_else(|| entity_name_from_path(path), ToString::to_string)
}

fn property_xml_name(schema: &Value, effective: &Value, fallback: &str) -> String {
    schema_xml_name(schema)
        .or_else(|| schema_xml_name(effective))
        .map(local_name)
        .map_or_else(|| local_name(fallback).to_string(), ToString::to_string)
}

fn array_item_xml_name(
    schema: &Value,
    effective: &Value,
    items: &Value,
    item_effective: &Value,
    array_xml_name: &str,
    fallback_name: &str,
    wrapped: bool,
) -> String {
    items
        .get("xml")
        .and_then(Value::as_object)
        .and_then(|xml| xml.get("name"))
        .and_then(Value::as_str)
        .or_else(|| schema_xml_name(item_effective))
        .or_else(|| (!wrapped).then_some(array_xml_name))
        .or_else(|| schema_xml_name(schema))
        .or_else(|| schema_xml_name(effective))
        .map(local_name)
        .map_or_else(
            || {
                if wrapped {
                    "member".to_string()
                } else {
                    local_name(fallback_name).to_string()
                }
            },
            ToString::to_string,
        )
}

fn schema_xml_name(schema: &Value) -> Option<&str> {
    schema
        .get("xml")
        .and_then(Value::as_object)
        .and_then(|xml| xml.get("name"))
        .and_then(Value::as_str)
}

fn xml_attribute(schema: &Value) -> Option<bool> {
    schema
        .get("xml")
        .and_then(Value::as_object)
        .and_then(|xml| xml.get("attribute"))
        .and_then(Value::as_bool)
}

fn xml_wrapped(schema: &Value) -> Option<bool> {
    schema
        .get("xml")
        .and_then(Value::as_object)
        .and_then(|xml| xml.get("wrapped"))
        .and_then(Value::as_bool)
}

fn schema_type(schema: &Value) -> &str {
    schema
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_else(|| {
            if schema.get("properties").is_some() || schema.get("additionalProperties").is_some() {
                "object"
            } else if schema.get("items").is_some() {
                "array"
            } else if schema.get("enum").is_some() {
                "string"
            } else {
                "object"
            }
        })
}

fn local_name(name: &str) -> &str {
    name.rsplit_once(':').map_or(name, |(_, local)| local)
}

fn entity_name_from_ref(reference: &str) -> String {
    reference
        .rsplit('/')
        .next()
        .map_or_else(|| "entity".to_string(), |raw| raw.replace(" Response", ""))
}

fn entity_name_from_path(path: &str) -> String {
    path.split('/')
        .rfind(|segment| !segment.is_empty() && !segment.starts_with('{'))
        .unwrap_or("entity")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::response_media_format;
    use crate::ResponseBodyFormat;

    #[test]
    fn response_media_format_accepts_xml_suffixes_and_parameters() {
        assert_eq!(
            response_media_format("application/xml; charset=utf-8"),
            Some((ResponseBodyFormat::Xml, 2))
        );
        assert_eq!(
            response_media_format("application/problem+xml"),
            Some((ResponseBodyFormat::Xml, 3))
        );
    }
}
