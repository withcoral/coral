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
                    renderer.inline(value)
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
            Callable::Tool { .. } => renderer.tool_result(result),
            Callable::SavedFunction { .. } => {
                renderer.saved_function_result(result, &mut HashSet::new())
            }
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

    /// The body of `type_`, followed through every alias.
    fn resolved<'t>(&'t self, type_: &'t Type) -> &'t Type {
        self.resolved_past(type_, &mut HashSet::new())
    }

    /// The body of `type_`, followed through every alias not in `seen`. Each
    /// alias it follows joins `seen`.
    fn resolved_past<'t>(&'t self, type_: &'t Type, seen: &mut HashSet<String>) -> &'t Type {
        let mut resolved = type_;
        while let Type::Named { name, arguments } = resolved
            && arguments.is_empty()
            && seen.insert(name.clone())
            && let Some(body) = self.alias(name)
        {
            resolved = body;
        }
        resolved
    }

    /// The first parameter or a Saved Function's result: an object's members
    /// inline one level deep.
    fn inline(&self, type_: &Type) -> String {
        match self.resolved(type_) {
            Type::Object(members) => self.members(members),
            Type::Intersection(parts) if self.shows_members(type_) => {
                let shown = parts
                    .iter()
                    .filter(|part| !self.is_open_record(part, &HashSet::new()))
                    .map(|part| self.inline_part(part))
                    .collect::<Vec<_>>();
                shown.join(" & ")
            }
            Type::Union(parts) => parts
                .iter()
                .map(|part| self.inline_part(part))
                .collect::<Vec<_>>()
                .join(" | "),
            other => self.member(other, &mut HashSet::new()),
        }
    }

    /// One part of an inlined union or intersection: an object, named or
    /// not, as its members; anything else as a member.
    fn inline_part(&self, part: &Type) -> String {
        match self.resolved(part) {
            Type::Object(members) => self.members(members),
            _ => self.member(part, &mut HashSet::new()),
        }
    }

    /// A Saved Function's result: inside its `Promise`, an object's members
    /// inline one level deep, as in the input, or an array element's. What
    /// the tree cannot read is the marker, and so is a type that resolves to
    /// an alias `seen` already followed: its `Promise` would hold itself
    /// without end.
    fn saved_function_result(&self, type_: &Type, seen: &mut HashSet<String>) -> String {
        match self.resolved_past(type_, seen) {
            Type::Named { name, arguments }
                if arguments.is_empty() && self.alias(name).is_some() =>
            {
                return CUT_MARKER.to_string();
            }
            Type::Named { name, arguments } if self.alias(name).is_none() => {
                match (name.as_str(), arguments.as_slice()) {
                    ("Promise", [value]) => {
                        return format!("Promise<{}>", self.saved_function_result(value, seen));
                    }
                    ("Array" | "ReadonlyArray", [element]) if self.shows_members(element) => {
                        return format!("{name}<{}>", self.inline(element));
                    }
                    _ => {}
                }
            }
            Type::Array { element, readonly } if self.shows_members(element) => {
                let mut shown = self.inline(element);
                if !matches!(self.resolved(element), Type::Object(_)) {
                    shown = format!("({shown})");
                }
                return format!("{}{shown}[]", if *readonly { "readonly " } else { "" });
            }
            _ => {}
        }
        self.inline(type_)
    }

    /// Whether [`Self::inline`] shows members of `type_`: an object, or a
    /// union or intersection with an object part.
    fn shows_members(&self, type_: &Type) -> bool {
        match self.resolved(type_) {
            Type::Object(_) => true,
            Type::Union(parts) | Type::Intersection(parts) => parts
                .iter()
                .any(|part| matches!(self.resolved(part), Type::Object(_))),
            _ => false,
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

    /// A result type: named types by name, an anonymous object as the
    /// marker, so a line that hides one is not complete.
    pub(super) fn result(&self, type_: &Type, seen: &mut HashSet<String>) -> String {
        self.render(type_, seen, &format!("{{{CUT_MARKER}}}"), "unknown")
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
                for part in parts {
                    if self.is_open_record(part, seen) {
                        continue;
                    }
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

    /// Whether `type_`, through shallow aliases, is the global
    /// `Record<string, unknown>`, which adds nothing a reader can use. A
    /// declared `Record` alias shadows the global one.
    fn is_open_record(&self, type_: &Type, seen: &HashSet<String>) -> bool {
        self.alias("Record").is_none()
            && matches!(self.shallow_body(type_, &mut seen.clone()), Type::Named { name, arguments }
                if name == "Record"
                    && matches!(arguments.as_slice(), [Type::Plain(key), Type::Plain(value)] if key == "string" && value == "unknown"))
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
