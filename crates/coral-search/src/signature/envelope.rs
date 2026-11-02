//! A Tool's result: the envelopes, one per declared success, reduced to
//! their `data` types. An HTTP Tool names each one
//! `HttpResult<Data, Status, MediaType>` (see [`coral_catalog::HTTP_RESULT`]);
//! an MCP-backed Tool's envelope is a `{ data; metadata }` object.

use std::collections::HashSet;

use super::render::{Renderer, forms};
use super::tree::{Member, Type};
use crate::hit::CUT_MARKER;

/// Where an MCP-backed Tool without a declared output puts its result.
const MCP_TEXT_RESULT: &str = "// JSON text in data.content[0].text";

/// How a form shows each `data` type.
#[derive(Clone, Copy)]
enum Data {
    /// An object, named or not, as its top-level field names.
    Fields,
    /// A named type by name, an unnamed object as the marker.
    Names,
}

impl Renderer<'_> {
    /// A Tool's result forms: the envelope with the field names of its
    /// declared successes' `data`, then with their names, then with the
    /// marker. An MCP envelope with `unknown` data contributes nothing, and
    /// one without a declared output says where its text is.
    pub(super) fn tool_results(&self, result: &Type) -> Vec<String> {
        let Some(envelopes) = envelopes(result) else {
            return forms([
                self.result(result, &mut HashSet::new()),
                CUT_MARKER.to_string(),
            ]);
        };
        let shortest = format!("Promise<{{ data: {CUT_MARKER} }}>");
        if let [envelope @ Type::Object(_)] = envelopes.as_slice()
            && let Some(data) = response(envelope)
            && let Type::Object(members) = self.resolved(data)
            && let Some((optional, structured)) = member(members, "structuredContent")
        {
            let content = member(members, "content").map_or_else(
                || "unknown".to_string(),
                |(_optional, content)| self.result(content, &mut HashSet::new()),
            );
            if structured == &Type::Plain("unknown".to_string()) {
                return forms([
                    format!("Promise<{{ data: {{ content: {content} }} }}> {MCP_TEXT_RESULT}"),
                    shortest,
                ]);
            }
            let structured_form = |form| {
                format!(
                    "Promise<{{ data: {{ structuredContent{}: {}; content: {content} }} }}>",
                    if optional { "?" } else { "" },
                    self.data(structured, &mut HashSet::new(), form),
                )
            };
            return forms([
                structured_form(Data::Fields),
                structured_form(Data::Names),
                shortest,
            ]);
        }
        forms([
            self.envelope_data(&envelopes, Data::Fields),
            self.envelope_data(&envelopes, Data::Names),
            shortest,
        ])
    }

    fn envelope_data(&self, envelopes: &[&Type], form: Data) -> String {
        let mut data = Vec::<String>::new();
        for envelope in envelopes {
            let Some(value) = response(envelope) else {
                continue;
            };
            // An untyped HTTP success stays in the union; an MCP envelope
            // without a declared output contributes nothing.
            if matches!(envelope, Type::Object(_)) && value == &Type::Plain("unknown".to_string()) {
                continue;
            }
            // Split on the type tree, so the data types of several responses
            // merge without repeats.
            let mut seen = HashSet::new();
            let alternatives = match self.shallow_body(value, &mut seen) {
                Type::Union(parts) => parts.iter().collect::<Vec<_>>(),
                single => vec![single],
            };
            for alternative in alternatives {
                let mut part = self.data(alternative, &mut seen.clone(), form);
                if matches!(alternative, Type::Function { .. }) {
                    part = format!("({part})");
                }
                if !data.contains(&part) {
                    data.push(part);
                }
            }
        }
        if data.is_empty() {
            data.push("unknown".to_string());
        }
        format!("Promise<{{ data: {} }}>", data.join(" | "))
    }

    /// One `data` type in `form`. An object, named or not, and the element
    /// of an array of one, shows its field names when the form shows
    /// fields; any other type is shown as [`Renderer::result`] shows it.
    fn data(&self, type_: &Type, seen: &mut HashSet<String>, form: Data) -> String {
        if matches!(form, Data::Fields)
            && let Some(fields) = self.fields(type_, &mut seen.clone())
        {
            return fields;
        }
        self.result(type_, seen)
    }

    /// The field names of an object, named or not, or of an array's
    /// element; `None` for any other type, or one with no fields.
    fn fields(&self, type_: &Type, seen: &mut HashSet<String>) -> Option<String> {
        match self.resolved_past(type_, seen) {
            Type::Array { element, readonly } => self
                .fields(element, seen)
                .map(|fields| format!("{}{fields}[]", if *readonly { "readonly " } else { "" })),
            Type::Named { name, arguments }
                if matches!(name.as_str(), "Array" | "ReadonlyArray")
                    && self
                        .aliases
                        .is_none_or(|aliases| !aliases.contains_key(name)) =>
            {
                let [element] = arguments.as_slice() else {
                    return None;
                };
                self.fields(element, seen)
                    .map(|fields| format!("{name}<{fields}>"))
            }
            resolved => {
                let mut fields = Vec::new();
                (self.collect_fields(resolved, seen, &mut fields) && !fields.is_empty())
                    .then(|| format!("{{ {} }}", fields.join("; ")))
            }
        }
    }

    /// Adds the field names of an object, or of an intersection of
    /// objects, to `fields`, each once, with `?` when optional. Returns
    /// whether every part was an object; an open record adds nothing.
    fn collect_fields(
        &self,
        type_: &Type,
        seen: &mut HashSet<String>,
        fields: &mut Vec<String>,
    ) -> bool {
        match self.resolved_past(type_, seen) {
            Type::Object(members) => {
                for member in members {
                    if let Member::Property { name, optional, .. } = member
                        && !fields
                            .iter()
                            .any(|field| field.trim_end_matches('?') == name)
                    {
                        fields.push(format!("{name}{}", if *optional { "?" } else { "" }));
                    }
                }
                true
            }
            Type::Intersection(parts) => parts.iter().all(|part| {
                self.is_open_record(part, seen) || self.collect_fields(part, seen, fields)
            }),
            _ => false,
        }
    }
}

/// The envelopes of a Tool's `Promise` result, one per response, or
/// `None` for another result.
pub(super) fn envelopes(result: &Type) -> Option<Vec<&Type>> {
    let Type::Named { name, arguments } = result else {
        return None;
    };
    let [envelopes] = arguments.as_slice() else {
        return None;
    };
    if name != "Promise" {
        return None;
    }
    Some(match envelopes {
        Type::Union(parts) => parts.iter().collect(),
        single => vec![single],
    })
}

/// One declared success's `data` type.
pub(super) fn response(envelope: &Type) -> Option<&Type> {
    match envelope {
        Type::Object(members) => member(members, "data").map(|(_optional, value)| value),
        Type::Named { name, arguments } if name == coral_catalog::HTTP_RESULT => {
            match arguments.as_slice() {
                [data, _status, _media_type] => Some(data),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Whether the property `wanted` is optional, and its type.
fn member<'t>(members: &'t [Member], wanted: &str) -> Option<(bool, &'t Type)> {
    members.iter().find_map(|member| match member {
        Member::Property {
            name,
            optional,
            value,
        } if name == wanted => Some((*optional, value)),
        _ => None,
    })
}
