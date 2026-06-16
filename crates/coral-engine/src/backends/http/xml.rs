//! XML response parsing and schema-driven normalization.

use std::collections::BTreeMap;
use std::str;

use quick_xml::Reader;
use quick_xml::escape::unescape;
use quick_xml::events::{BytesStart, Event};
use serde_json::{Map, Number, Value};

use coral_spec::{XmlFieldSpec, XmlListSpec, XmlResponseSpec, XmlScalarType, XmlValueSpec};

#[derive(Debug, Clone, PartialEq, Eq)]
struct XmlElement {
    name: String,
    attributes: BTreeMap<String, String>,
    children: Vec<XmlElement>,
    text: String,
}

pub(super) fn decode_xml_response(bytes: &[u8], spec: &XmlResponseSpec) -> Result<Value, String> {
    let root = parse_xml(bytes)?;
    if let Some(expected) = &spec.root_name
        && !name_matches(&root.name, expected)
    {
        return Err(format!(
            "XML response root '{}' did not match expected root '{}'",
            root.name, expected
        ));
    }
    let root_name = spec.root_name.as_deref().unwrap_or(&root.name).to_string();
    let normalized = normalize_value(&root, &spec.root);
    let mut object = Map::new();
    object.insert(root_name, normalized);
    Ok(Value::Object(object))
}

fn parse_xml(bytes: &[u8]) -> Result<XmlElement, String> {
    let mut reader = Reader::from_reader(bytes);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut stack = Vec::new();
    let mut root = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(start)) => {
                stack.push(element_from_start(&start)?);
            }
            Ok(Event::Empty(start)) => {
                let element = element_from_start(&start)?;
                attach_element(element, &mut stack, &mut root)?;
            }
            Ok(Event::End(_end)) => {
                let Some(element) = stack.pop() else {
                    return Err("XML response had an unexpected closing element".to_string());
                };
                attach_element(element, &mut stack, &mut root)?;
            }
            Ok(Event::Text(text)) => {
                let decoded = text
                    .xml_content()
                    .map_err(|error| format!("XML text decode failed: {error}"))?;
                append_text(&mut stack, decoded.as_ref());
            }
            Ok(Event::CData(cdata)) => {
                let decoded = cdata
                    .decode()
                    .map_err(|error| format!("XML CDATA decode failed: {error}"))?;
                append_text(&mut stack, decoded.as_ref());
            }
            Ok(Event::GeneralRef(reference)) => {
                let raw = str::from_utf8(reference.as_ref())
                    .map_err(|error| format!("XML entity reference was not UTF-8: {error}"))?;
                let escaped = format!("&{raw};");
                let decoded = unescape(&escaped)
                    .map_err(|error| format!("XML entity reference decode failed: {error}"))?;
                append_text(&mut stack, decoded.as_ref());
            }
            Ok(Event::Decl(_) | Event::PI(_) | Event::DocType(_) | Event::Comment(_)) => {}
            Ok(Event::Eof) => break,
            Err(error) => return Err(format!("XML response decoding failed: {error}")),
        }
        buf.clear();
    }

    if !stack.is_empty() {
        return Err("XML response ended before all elements were closed".to_string());
    }
    root.ok_or_else(|| "XML response did not contain a document element".to_string())
}

fn element_from_start(start: &BytesStart<'_>) -> Result<XmlElement, String> {
    let name = local_name_bytes(start.name().as_ref())?.to_string();
    let mut attributes = BTreeMap::new();
    for attribute in start.attributes() {
        let attribute =
            attribute.map_err(|error| format!("XML attribute decode failed: {error}"))?;
        let name = local_name_bytes(attribute.key.as_ref())?.to_string();
        let value = attribute
            .unescape_value()
            .map_err(|error| format!("XML attribute value decode failed: {error}"))?
            .into_owned();
        attributes.insert(name, value);
    }
    Ok(XmlElement {
        name,
        attributes,
        children: Vec::new(),
        text: String::new(),
    })
}

fn attach_element(
    element: XmlElement,
    stack: &mut [XmlElement],
    root: &mut Option<XmlElement>,
) -> Result<(), String> {
    if let Some(parent) = stack.last_mut() {
        parent.children.push(element);
        return Ok(());
    }
    if root.replace(element).is_some() {
        return Err("XML response contained multiple document elements".to_string());
    }
    Ok(())
}

fn append_text(stack: &mut [XmlElement], text: &str) {
    let Some(element) = stack.last_mut() else {
        return;
    };
    element.text.push_str(text);
}

fn normalize_value(element: &XmlElement, spec: &XmlValueSpec) -> Value {
    match spec {
        XmlValueSpec::Scalar(scalar) => coerce_scalar(&element.text, scalar.scalar_type),
        XmlValueSpec::Object(object) => {
            if object.fields.is_empty() {
                return generic_element_value(element);
            }
            let mut map = Map::new();
            for field in &object.fields {
                if let Some(value) = normalize_field(element, field) {
                    map.insert(field.name.clone(), value);
                }
            }
            Value::Object(map)
        }
        XmlValueSpec::List(list) => normalize_list_element(element, list),
        XmlValueSpec::Json(_json) => generic_element_value(element),
    }
}

fn normalize_field(element: &XmlElement, field: &XmlFieldSpec) -> Option<Value> {
    if field.attribute {
        return element
            .attributes
            .get(local_name(&field.xml_name))
            .map(|value| coerce_attribute(value, &field.value));
    }
    if let XmlValueSpec::List(list) = &field.value {
        return Some(normalize_list_field(element, &field.xml_name, list));
    }
    element
        .children
        .iter()
        .find(|child| name_matches(&child.name, &field.xml_name))
        .map(|child| normalize_value(child, &field.value))
}

fn normalize_list_field(parent: &XmlElement, field_xml_name: &str, list: &XmlListSpec) -> Value {
    let containers = parent
        .children
        .iter()
        .filter(|child| name_matches(&child.name, field_xml_name))
        .collect::<Vec<_>>();
    if containers.is_empty() {
        return Value::Array(Vec::new());
    }
    if list.wrapped {
        let mut items = Vec::new();
        for container in containers {
            append_wrapped_items(container, list, &mut items);
        }
        Value::Array(items)
    } else {
        Value::Array(
            containers
                .into_iter()
                .map(|container| normalize_value(container, &list.item))
                .collect(),
        )
    }
}

fn normalize_list_element(element: &XmlElement, list: &XmlListSpec) -> Value {
    if list.wrapped {
        let mut items = Vec::new();
        append_wrapped_items(element, list, &mut items);
        Value::Array(items)
    } else {
        Value::Array(vec![normalize_value(element, &list.item)])
    }
}

fn append_wrapped_items(container: &XmlElement, list: &XmlListSpec, items: &mut Vec<Value>) {
    let item_xml_name = list.item_xml_name.as_deref().unwrap_or("member");
    let matching_children = container
        .children
        .iter()
        .filter(|child| name_matches(&child.name, item_xml_name))
        .collect::<Vec<_>>();
    if matching_children.is_empty() {
        if container.children.is_empty() && container.text.trim().is_empty() {
            return;
        }
        items.push(normalize_value(container, &list.item));
        return;
    }
    items.extend(
        matching_children
            .into_iter()
            .map(|child| normalize_value(child, &list.item)),
    );
}

fn coerce_attribute(value: &str, spec: &XmlValueSpec) -> Value {
    match spec {
        XmlValueSpec::Scalar(scalar) => coerce_scalar(value, scalar.scalar_type),
        XmlValueSpec::Object(_) | XmlValueSpec::List(_) | XmlValueSpec::Json(_) => {
            Value::String(value.to_string())
        }
    }
}

fn coerce_scalar(value: &str, scalar_type: XmlScalarType) -> Value {
    let trimmed = value.trim();
    match scalar_type {
        XmlScalarType::String => Value::String(trimmed.to_string()),
        XmlScalarType::Integer => trimmed.parse::<i64>().map_or_else(
            |_error| Value::String(trimmed.to_string()),
            |value| Value::Number(Number::from(value)),
        ),
        XmlScalarType::Number => trimmed
            .parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .map_or_else(|| Value::String(trimmed.to_string()), Value::Number),
        XmlScalarType::Boolean => match trimmed.to_ascii_lowercase().as_str() {
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            _ => Value::String(trimmed.to_string()),
        },
    }
}

fn generic_element_value(element: &XmlElement) -> Value {
    if element.attributes.is_empty() && element.children.is_empty() {
        return Value::String(element.text.trim().to_string());
    }
    let mut map = Map::new();
    for (name, value) in &element.attributes {
        map.insert(name.clone(), Value::String(value.clone()));
    }
    if !element.text.trim().is_empty() {
        map.insert(
            "#text".to_string(),
            Value::String(element.text.trim().to_string()),
        );
    }
    for child in &element.children {
        insert_grouped_child(&mut map, child.name.clone(), generic_element_value(child));
    }
    Value::Object(map)
}

fn insert_grouped_child(map: &mut Map<String, Value>, key: String, value: Value) {
    match map.get_mut(&key) {
        Some(Value::Array(values)) => values.push(value),
        Some(existing) => {
            let previous = std::mem::replace(existing, Value::Null);
            *existing = Value::Array(vec![previous, value]);
        }
        None => {
            map.insert(key, value);
        }
    }
}

fn local_name_bytes(bytes: &[u8]) -> Result<&str, String> {
    let name = str::from_utf8(bytes)
        .map_err(|error| format!("XML element name was not UTF-8: {error}"))?;
    Ok(local_name(name))
}

fn local_name(name: &str) -> &str {
    name.rsplit_once(':').map_or(name, |(_, local)| local)
}

fn name_matches(actual: &str, expected: &str) -> bool {
    local_name(actual) == local_name(expected)
}

#[cfg(test)]
mod tests {
    use coral_spec::{
        XmlFieldSpec, XmlListSpec, XmlObjectSpec, XmlResponseSpec, XmlScalarSpec, XmlScalarType,
        XmlValueSpec,
    };
    use serde_json::json;

    use super::decode_xml_response;

    fn alarm_response_spec() -> XmlResponseSpec {
        XmlResponseSpec {
            root_name: Some("DescribeAlarmsResponse".to_string()),
            root: XmlValueSpec::Object(XmlObjectSpec {
                xml_name: Some("DescribeAlarmsResponse".to_string()),
                fields: vec![XmlFieldSpec {
                    name: "DescribeAlarmsResult".to_string(),
                    xml_name: "DescribeAlarmsResult".to_string(),
                    attribute: false,
                    value: XmlValueSpec::Object(XmlObjectSpec {
                        xml_name: Some("DescribeAlarmsResult".to_string()),
                        fields: vec![XmlFieldSpec {
                            name: "MetricAlarms".to_string(),
                            xml_name: "MetricAlarms".to_string(),
                            attribute: false,
                            value: XmlValueSpec::List(XmlListSpec {
                                xml_name: Some("MetricAlarms".to_string()),
                                wrapped: true,
                                item_xml_name: Some("member".to_string()),
                                item: Box::new(XmlValueSpec::Object(XmlObjectSpec {
                                    xml_name: Some("member".to_string()),
                                    fields: vec![
                                        XmlFieldSpec {
                                            name: "AlarmName".to_string(),
                                            xml_name: "AlarmName".to_string(),
                                            attribute: false,
                                            value: XmlValueSpec::Scalar(XmlScalarSpec {
                                                xml_name: Some("AlarmName".to_string()),
                                                scalar_type: XmlScalarType::String,
                                            }),
                                        },
                                        XmlFieldSpec {
                                            name: "ActionsEnabled".to_string(),
                                            xml_name: "ActionsEnabled".to_string(),
                                            attribute: false,
                                            value: XmlValueSpec::Scalar(XmlScalarSpec {
                                                xml_name: Some("ActionsEnabled".to_string()),
                                                scalar_type: XmlScalarType::Boolean,
                                            }),
                                        },
                                    ],
                                })),
                            }),
                        }],
                    }),
                }],
            }),
        }
    }

    #[test]
    fn normalizes_single_member_as_array() {
        let payload = br"
<DescribeAlarmsResponse>
  <DescribeAlarmsResult>
    <MetricAlarms>
      <member>
        <AlarmName>cpu</AlarmName>
        <ActionsEnabled>true</ActionsEnabled>
      </member>
    </MetricAlarms>
  </DescribeAlarmsResult>
</DescribeAlarmsResponse>
";
        let value = decode_xml_response(payload, &alarm_response_spec()).expect("decode xml");
        assert_eq!(
            value,
            json!({
                "DescribeAlarmsResponse": {
                    "DescribeAlarmsResult": {
                        "MetricAlarms": [{
                            "AlarmName": "cpu",
                            "ActionsEnabled": true
                        }]
                    }
                }
            })
        );
    }

    #[test]
    fn normalizes_multiple_members_as_array() {
        let payload = br"
<DescribeAlarmsResponse>
  <DescribeAlarmsResult>
    <MetricAlarms>
      <member><AlarmName>cpu</AlarmName></member>
      <member><AlarmName>memory</AlarmName></member>
    </MetricAlarms>
  </DescribeAlarmsResult>
</DescribeAlarmsResponse>
";
        let value = decode_xml_response(payload, &alarm_response_spec()).expect("decode xml");
        assert_eq!(
            *value
                .pointer("/DescribeAlarmsResponse/DescribeAlarmsResult/MetricAlarms")
                .expect("metric alarms"),
            json!([
                {"AlarmName": "cpu"},
                {"AlarmName": "memory"}
            ])
        );
    }

    #[test]
    fn matches_namespaces_by_local_name_and_reads_attributes() {
        let spec = XmlResponseSpec {
            root_name: Some("entry".to_string()),
            root: XmlValueSpec::Object(XmlObjectSpec {
                xml_name: Some("entry".to_string()),
                fields: vec![XmlFieldSpec {
                    name: "id".to_string(),
                    xml_name: "id".to_string(),
                    attribute: true,
                    value: XmlValueSpec::Scalar(XmlScalarSpec {
                        xml_name: Some("id".to_string()),
                        scalar_type: XmlScalarType::Integer,
                    }),
                }],
            }),
        };
        let value =
            decode_xml_response(br#"<atom:entry atom:id="42"/>"#, &spec).expect("decode xml");
        assert_eq!(value, json!({"entry": {"id": 42}}));
    }

    #[test]
    fn rejects_malformed_xml() {
        let error = decode_xml_response(br"<root><broken></root>", &alarm_response_spec())
            .expect_err("malformed xml should fail");
        assert!(error.contains("XML response decoding failed"), "{error}");
    }
}
