//! A Tool's result: the envelopes, one per declared success, reduced to
//! their `data` types. An HTTP Tool names each one
//! `HttpResult<Data, Status, MediaType>` (see [`coral_catalog::HTTP_RESULT`]);
//! an MCP-backed Tool's envelope is a `{ data; metadata }` object.

use std::collections::HashSet;

use super::render::Renderer;
use super::tree::{Member, Type};

impl Renderer<'_> {
    /// A Tool's result: the envelope with the `data` types of its
    /// declared successes. An MCP envelope with `unknown` data contributes
    /// nothing.
    pub(super) fn tool_result(&self, result: &Type) -> String {
        let Some(envelopes) = envelopes(result) else {
            return self.result(result, &mut HashSet::new());
        };
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
                let mut part = self.result(alternative, &mut seen.clone());
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
        Type::Object(members) => property(members, "data"),
        Type::Named { name, arguments } if name == coral_catalog::HTTP_RESULT => {
            match arguments.as_slice() {
                [data, _status, _media_type] => Some(data),
                _ => None,
            }
        }
        _ => None,
    }
}

fn property<'t>(members: &'t [Member], wanted: &str) -> Option<&'t Type> {
    members.iter().find_map(|member| match member {
        Member::Property { name, value, .. } if name == wanted => Some(value),
        _ => None,
    })
}
