//! The rendering rules: the input inline one level deep, shallow aliases
//! inlined, nested objects as the marker, and other types by name.

use std::collections::{HashMap, HashSet};

use super::Callable;
use super::tree::{Declarations, Member, Type};
use crate::hit::CUT_MARKER;

/// Generic and global names whose meaning the caller knows without describe.
const GLOBAL_NAMES: [&str; 9] = [
    "Array",
    "Function",
    "ReadonlyArray",
    "Record",
    "Promise",
    "Date",
    "Map",
    "Set",
    "Uint8Array",
];

/// The unbounded signature line of the declarations' callable.
pub(super) fn render(declarations: &Declarations, callable: Callable<'_>) -> String {
    let renderer = Renderer {
        aliases: Some(&declarations.aliases),
    };
    let parameters = declarations
        .callable
        .parameters
        .iter()
        .enumerate()
        .map(|(index, parameter)| {
            let value = parameter.value.as_ref().map_or_else(String::new, |value| {
                let shown = if index == 0 {
                    renderer.input(value)
                } else {
                    renderer.member(value, &mut HashSet::new())
                };
                format!(": {shown}")
            });
            format!(
                "{}{}{}{value}",
                if parameter.rest { "..." } else { "" },
                parameter.name,
                if parameter.optional { "?" } else { "" },
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let result = declarations.callable.result.as_ref().map_or_else(
        || "unknown".to_string(),
        |result| match callable {
            Callable::Operation { .. } => renderer.operation_result(result),
            Callable::SavedFunction { .. } => renderer.result(result, &mut HashSet::new()),
        },
    );
    format!("{}({parameters}): {result}", callable.leaf())
}

#[derive(Default)]
pub(super) struct Renderer<'a> {
    pub(super) aliases: Option<&'a HashMap<String, Type>>,
}

impl Renderer<'_> {
    fn alias(&self, name: &str) -> Option<&Type> {
        self.aliases.and_then(|aliases| aliases.get(name))
    }

    /// The body of a shallow alias, followed through aliases of aliases.
    pub(super) fn shallow_body<'t>(
        &'t self,
        type_: &'t Type,
        seen: &mut HashSet<String>,
    ) -> &'t Type {
        if let Type::Named { name, arguments } = type_
            && arguments.is_empty()
            && let Some(body) = self.alias(name)
            && self.is_shallow(type_, &mut seen.clone())
        {
            seen.insert(name.clone());
            return self.shallow_body(body, seen);
        }
        type_
    }

    /// Whether the type holds no object, function, or other named type, so
    /// the line can show it whole.
    fn is_shallow(&self, type_: &Type, seen: &mut HashSet<String>) -> bool {
        match type_ {
            Type::Plain(_) => true,
            Type::Object(_) | Type::Function { .. } | Type::Opaque => false,
            Type::Union(parts) | Type::Intersection(parts) | Type::Tuple(parts) => {
                parts.iter().all(|part| self.is_shallow(part, seen))
            }
            Type::Array { element, .. } | Type::Optional(element) => self.is_shallow(element, seen),
            Type::Named { name, arguments } => {
                // A declared alias shadows the global name and goes through the
                // cycle guard below.
                if GLOBAL_NAMES.contains(&name.as_str()) && self.alias(name).is_none() {
                    return arguments
                        .iter()
                        .all(|argument| self.is_shallow(argument, seen));
                }
                arguments.is_empty()
                    && seen.insert(name.clone())
                    && self
                        .alias(name)
                        .is_some_and(|body| self.is_shallow(body, seen))
            }
        }
    }

    /// The first parameter: an object's members inline one level deep.
    fn input(&self, type_: &Type) -> String {
        let mut seen = HashSet::new();
        let mut resolved = type_;
        while let Type::Named { name, arguments } = resolved
            && arguments.is_empty()
            && seen.insert(name.clone())
            && let Some(body) = self.alias(name)
        {
            resolved = body;
        }
        match resolved {
            Type::Object(members) => self.members(members),
            Type::Intersection(parts)
                if parts.iter().any(|part| matches!(part, Type::Object(_))) =>
            {
                let shown = parts
                    .iter()
                    .filter(|part| !is_open_record(part))
                    .map(|part| match part {
                        Type::Object(members) => self.members(members),
                        other => self.member(other, &mut HashSet::new()),
                    })
                    .collect::<Vec<_>>();
                shown.join(" & ")
            }
            Type::Union(parts) => parts
                .iter()
                .map(|part| match part {
                    Type::Object(members) => self.members(members),
                    other => self.member(other, &mut HashSet::new()),
                })
                .collect::<Vec<_>>()
                .join(" | "),
            other => self.member(other, &mut HashSet::new()),
        }
    }

    fn members(&self, members: &[Member]) -> String {
        if members.is_empty() {
            return "{}".to_string();
        }
        let members = members
            .iter()
            .map(|member| match member {
                Member::Property {
                    name,
                    optional,
                    value,
                } => format!(
                    "{name}{}: {}",
                    if *optional { "?" } else { "" },
                    self.member(value, &mut HashSet::new())
                ),
                Member::Index { key, value } => {
                    format!("[{key}]: {}", self.member(value, &mut HashSet::new()))
                }
                Member::Opaque => CUT_MARKER.to_string(),
            })
            .collect::<Vec<_>>();
        format!("{{ {} }}", members.join("; "))
    }

    /// A type below the input object: a nested object is the marker, a
    /// shallow alias is inlined, any other named type is its name.
    fn member(&self, type_: &Type, seen: &mut HashSet<String>) -> String {
        self.render(type_, seen, &format!("{{{CUT_MARKER}}}"), CUT_MARKER)
    }

    /// A result type: named types by name, an anonymous object as `object`.
    pub(super) fn result(&self, type_: &Type, seen: &mut HashSet<String>) -> String {
        self.render(type_, seen, "object", "unknown")
    }

    /// A type as a key or plain value, with no alias lookups.
    pub(super) fn plain(&self, type_: &Type) -> String {
        self.render(type_, &mut HashSet::new(), "object", "unknown")
    }

    fn render(
        &self,
        type_: &Type,
        seen: &mut HashSet<String>,
        object: &str,
        opaque: &str,
    ) -> String {
        let type_ = self.shallow_body(type_, seen);
        match type_ {
            Type::Plain(text) => text.clone(),
            Type::Opaque => opaque.to_string(),
            Type::Object(_) => object.to_string(),
            Type::Union(parts) => {
                let mut rendered = Vec::<String>::new();
                for part in parts {
                    let function = matches!(
                        self.shallow_body(part, &mut seen.clone()),
                        Type::Function { .. }
                    );
                    let mut part = self.render(part, &mut seen.clone(), object, opaque);
                    if function {
                        part = format!("({part})");
                    }
                    if !rendered.contains(&part) {
                        rendered.push(part);
                    }
                }
                rendered.join(" | ")
            }
            // Each part keeps its name; inline objects share one marker, and an
            // open record adds nothing a reader can use.
            Type::Intersection(parts) => {
                let mut rendered = Vec::<String>::new();
                // A declared `Record` alias shadows the global one, so its part stays.
                let open_record = self.alias("Record").is_none();
                for part in parts
                    .iter()
                    .filter(|part| !(open_record && is_open_record(part)))
                {
                    let part = self.grouped(part, seen, object, opaque);
                    if !rendered.contains(&part) {
                        rendered.push(part);
                    }
                }
                if rendered.is_empty() {
                    return object.to_string();
                }
                rendered.join(" & ")
            }
            Type::Array { element, readonly } => format!(
                "{}{}[]",
                if *readonly { "readonly " } else { "" },
                self.grouped(element, seen, object, opaque)
            ),
            Type::Tuple(elements) => format!(
                "[{}]",
                elements
                    .iter()
                    .map(|element| self.render(element, &mut seen.clone(), object, opaque))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Type::Optional(element) => {
                format!("{}?", self.grouped(element, seen, object, opaque))
            }
            Type::Named { name, arguments } if arguments.is_empty() => name.clone(),
            Type::Named { name, arguments } => format!(
                "{name}<{}>",
                arguments
                    .iter()
                    .map(|argument| self.render(argument, &mut seen.clone(), object, opaque))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Type::Function { parameters, result } => format!(
                "({}) => {}",
                parameters
                    .iter()
                    .map(|parameter| {
                        let value = parameter.value.as_ref().map_or_else(String::new, |value| {
                            format!(
                                ": {}",
                                self.render(value, &mut seen.clone(), object, opaque)
                            )
                        });
                        format!(
                            "{}{}{}{value}",
                            if parameter.rest { "..." } else { "" },
                            parameter.name,
                            if parameter.optional { "?" } else { "" },
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
                self.render(result, &mut seen.clone(), object, opaque)
            ),
        }
    }

    /// A type in operand position, in parentheses when it is a union,
    /// intersection, or function.
    fn grouped(
        &self,
        type_: &Type,
        seen: &mut HashSet<String>,
        object: &str,
        opaque: &str,
    ) -> String {
        let rendered = self.render(type_, &mut seen.clone(), object, opaque);
        let compound = matches!(
            self.shallow_body(type_, &mut seen.clone()),
            Type::Union(_) | Type::Function { .. }
        ) || (matches!(
            self.shallow_body(type_, &mut seen.clone()),
            Type::Intersection(_)
        ) && rendered != object);
        if compound {
            format!("({rendered})")
        } else {
            rendered
        }
    }
}

fn is_open_record(type_: &Type) -> bool {
    matches!(type_, Type::Named { name, arguments }
        if name == "Record"
            && matches!(arguments.as_slice(), [Type::Plain(key), Type::Plain(value)] if key == "string" && value == "unknown"))
}
