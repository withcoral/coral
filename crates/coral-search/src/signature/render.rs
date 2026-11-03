//! The rendering rules: the input inline one level deep, required members
//! first and long enums last, shallow aliases inlined, nested objects as the
//! marker, and other types by name. The result comes in forms from the most
//! detailed to the shortest, so the line can keep one whole.

use std::cell::Cell;
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

/// An enum longer than this, in UTF-8 bytes as shown, goes after the other
/// input members of its kind, so a cut drops it before them.
const LONG_ENUM_BYTES: usize = 64;

/// What the line shows of a callable, before it is fitted to its limit.
pub(super) struct Shown {
    /// The name the line calls the callable by.
    pub(super) leaf: String,
    pub(super) parameters: Parameters,
    /// The result, from the most detailed form to the shortest.
    pub(super) results: Vec<String>,
    /// Whether the input names a type that the line does not show.
    pub(super) names_unshown_type: bool,
    /// Whether the callable has other overloads, whose inputs the line does
    /// not show.
    pub(super) overloaded: bool,
}

/// The parameters as one text, and the first one's object members, which a
/// cut drops from the end.
pub(super) struct Parameters {
    pub(super) whole: String,
    /// The text before the first parameter's object, such as `input: `,
    /// and its members in the order the line shows them; `None` when the
    /// first parameter is not one object.
    pub(super) object: Option<(String, Vec<String>)>,
    /// The parameters after the first, each after its `, `.
    pub(super) rest: String,
}

/// What the line shows of the declarations' callable.
pub(super) fn render(declarations: &Declarations, callable: Callable<'_>) -> Shown {
    let input = Renderer {
        aliases: Some(&declarations.aliases),
        input: true,
        ..Renderer::default()
    };
    let mut object = None;
    let mut texts = Vec::new();
    for (index, parameter) in declarations.callable.parameters.iter().enumerate() {
        let mut text = format!(
            "{}{}{}",
            if parameter.rest { "..." } else { "" },
            parameter.name,
            if parameter.optional { "?" } else { "" },
        );
        if let Some(value) = &parameter.value {
            text.push_str(": ");
            if index > 0 {
                text.push_str(&input.member(value, &mut HashSet::new()));
            } else if let Type::Object(members) = input.resolved(value) {
                let members = input.member_texts(members);
                object = Some((text.clone(), members.clone()));
                text.push_str(&braced(&members));
            } else {
                text.push_str(&input.inline(value));
            }
        }
        texts.push(text);
    }
    let rest = texts
        .iter()
        .skip(1)
        .flat_map(|text| [", ", text.as_str()])
        .collect::<String>();
    let results = declarations.callable.result.as_ref().map_or_else(
        || vec!["unknown".to_string()],
        |result| {
            let renderer = Renderer {
                aliases: Some(&declarations.aliases),
                ..Renderer::default()
            };
            match callable {
                Callable::Tool { .. } => renderer.tool_results(result),
                Callable::SavedFunction { .. } => renderer.saved_function_results(result),
            }
        },
    );
    Shown {
        leaf: callable.leaf().to_string(),
        parameters: Parameters {
            whole: texts.join(", "),
            object,
            rest,
        },
        results,
        names_unshown_type: input.named.get(),
        overloaded: declarations.overloaded,
    }
}

/// Members as an object type.
pub(super) fn braced(members: &[String]) -> String {
    if members.is_empty() {
        "{}".to_string()
    } else {
        format!("{{ {} }}", members.join("; "))
    }
}

#[derive(Default)]
pub(super) struct Renderer<'a> {
    pub(super) aliases: Option<&'a HashMap<String, Type>>,
    /// Whether this renders the input: its members go required first and
    /// long enums last, and a type it shows by name is recorded in `named`.
    input: bool,
    /// Whether an object's members show their names only.
    stubs: bool,
    /// Whether the input showed a type by name.
    named: Cell<bool>,
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
    pub(super) fn resolved<'t>(&'t self, type_: &'t Type) -> &'t Type {
        self.resolved_past(type_, &mut HashSet::new())
    }

    /// The body of `type_`, followed through every alias not in `seen`. Each
    /// alias it follows joins `seen`.
    pub(super) fn resolved_past<'t>(
        &'t self,
        type_: &'t Type,
        seen: &mut HashSet<String>,
    ) -> &'t Type {
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

    /// A Saved Function's result forms: its members typed, then their names
    /// alone, then the marker inside its `Promise`.
    fn saved_function_results(&self, result: &Type) -> Vec<String> {
        let stubs = Renderer {
            aliases: self.aliases,
            stubs: true,
            ..Renderer::default()
        };
        let promised = matches!(self.resolved(result), Type::Named { name, .. }
            if name == "Promise" && self.alias(name).is_none());
        forms([
            self.saved_function_result(result, &mut HashSet::new()),
            stubs.saved_function_result(result, &mut HashSet::new()),
            if promised {
                format!("Promise<{CUT_MARKER}>")
            } else {
                CUT_MARKER.to_string()
            },
        ])
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
        braced(&self.member_texts(members))
    }

    /// Each member as the line shows it: in the input, required members
    /// first and long enums last, each group in declaration order; as
    /// stubs, a property's name alone.
    fn member_texts(&self, members: &[Member]) -> Vec<String> {
        let mut shown = members
            .iter()
            .map(|member| match member {
                Member::Property {
                    name,
                    optional,
                    value,
                } => {
                    let optional_mark = if *optional { "?" } else { "" };
                    if self.stubs {
                        return ((false, false), format!("{name}{optional_mark}"));
                    }
                    let shown = self.member(value, &mut HashSet::new());
                    let long_enum = self.input
                        && shown.len() > LONG_ENUM_BYTES
                        && self.is_enum(value, &mut HashSet::new());
                    (
                        (self.input && *optional, long_enum),
                        format!("{name}{optional_mark}: {shown}"),
                    )
                }
                Member::Index { key, value } => (
                    (false, false),
                    format!("[{key}]: {}", self.member(value, &mut HashSet::new())),
                ),
                Member::Opaque => ((false, false), CUT_MARKER.to_string()),
            })
            .collect::<Vec<_>>();
        shown.sort_by_key(|(order, _text)| *order);
        shown.into_iter().map(|(_order, text)| text).collect()
    }

    /// Whether `type_` is a union of literals, or an array of one.
    fn is_enum(&self, type_: &Type, seen: &mut HashSet<String>) -> bool {
        match self.shallow_body(type_, seen) {
            Type::Union(parts) => {
                parts.len() > 1
                    && parts.iter().all(|part| {
                        matches!(self.shallow_body(part, &mut seen.clone()),
                            Type::Plain(text) if is_literal(text) || text == "null")
                    })
            }
            Type::Array { element, .. } => self.is_enum(element, seen),
            Type::Named { name, arguments }
                if matches!(name.as_str(), "Array" | "ReadonlyArray")
                    && self.alias(name).is_none() =>
            {
                matches!(arguments.as_slice(), [element] if self.is_enum(element, seen))
            }
            _ => false,
        }
    }

    /// A type below the input object: a nested object is the marker, a
    /// shallow alias is inlined, any other named type is its name.
    fn member(&self, type_: &Type, seen: &mut HashSet<String>) -> String {
        self.render(type_, seen, &format!("{{{CUT_MARKER}}}"), CUT_MARKER)
    }

    /// A result type: named types by name, an anonymous object as the
    /// marker.
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
            Type::Named { name, arguments } if arguments.is_empty() => {
                self.shows_by_name(name);
                name.clone()
            }
            Type::Named { name, arguments } => {
                self.shows_by_name(name);
                format!(
                    "{name}<{}>",
                    arguments
                        .iter()
                        .map(|argument| self.render(argument, &mut seen.clone(), object, opaque))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
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

    /// Records that the input shows `name` by name: a declared type, or a
    /// name that is not a known global.
    fn shows_by_name(&self, name: &str) {
        if self.input && (self.alias(name).is_some() || !GLOBAL_NAMES.contains(&name)) {
            self.named.set(true);
        }
    }

    /// Whether `type_`, through shallow aliases, is the global
    /// `Record<string, unknown>`, which adds nothing a reader can use. A
    /// declared `Record` alias shadows the global one.
    pub(super) fn is_open_record(&self, type_: &Type, seen: &HashSet<String>) -> bool {
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

/// Whether a plain type is a literal: a string, template, number, or
/// boolean literal, as written.
fn is_literal(text: &str) -> bool {
    text.starts_with(['"', '\'', '`', '-'])
        || text.starts_with(|first: char| first.is_ascii_digit())
        || text == "true"
        || text == "false"
}

/// The distinct forms, in order.
pub(super) fn forms(forms: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut distinct = Vec::<String>::new();
    for form in forms {
        if !distinct.contains(&form) {
            distinct.push(form);
        }
    }
    distinct
}
