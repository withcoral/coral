//! An Operation's result: the envelopes, one per response, reduced to the
//! `data` types of the success responses. An HTTP Operation names each typed
//! response `HttpResult<Data, Status, MediaType>` (see
//! [`coral_catalog::HTTP_RESULT`]); an MCP Operation's envelope is a
//! `{ data; metadata }` object without a status.

use std::collections::HashSet;

use super::render::Renderer;
use super::tree::{Member, Type};

impl Renderer<'_> {
    /// An Operation's result: the envelope with the `data` types of its
    /// success responses. An `HttpResult` is a success when its status is
    /// 2xx; any other envelope counts as success unless its data is `unknown`.
    pub(super) fn operation_result(&self, result: &Type) -> String {
        let Some(envelopes) = envelopes(result) else {
            return self.result(result, &mut HashSet::new());
        };
        let mut data = Vec::<String>::new();
        for envelope in envelopes {
            let Some((value, status)) = response(envelope) else {
                continue;
            };
            let success = status.map_or(value != &Type::Plain("unknown".to_string()), |status| {
                status_is_success(status)
            });
            if !success {
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

/// The envelopes of an Operation's `Promise` result, one per response, or
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

/// One response's `data` type and, for an `HttpResult`, its status.
/// `HttpUnknownResult` is never a success, so it yields nothing.
pub(super) fn response(envelope: &Type) -> Option<(&Type, Option<&Type>)> {
    match envelope {
        Type::Object(members) => Some((property(members, "data")?, None)),
        Type::Named { name, arguments } if name == coral_catalog::HTTP_RESULT => {
            match arguments.as_slice() {
                [data, status, _media_type] => Some((data, Some(status))),
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

pub(super) fn status_is_success(status: &Type) -> bool {
    let codes = match status {
        Type::Union(parts) => parts.iter().collect::<Vec<_>>(),
        single => vec![single],
    };
    codes.into_iter().all(|code| {
        matches!(code, Type::Plain(code) if code.parse::<u16>().is_ok_and(|code| (200..300).contains(&code)))
    })
}
