//! The abridged declaration `describe` returns: the required parts whole,
//! and the remaining result types fitted to [`DESCRIBE_EXTRA_BYTES`].
//!
//! The inputs, the callable, the result wrapper, and the top level of the
//! success data are always whole, outside the budget. The top level is the
//! type the data is, such as `PullRequestSimple` in `Array<PullRequestSimple>`;
//! inline data is its own top level. The remaining types are placed in
//! breadth-first order from the success data:
//!
//! 1. Each type is placed as a stub, its field names only, if the stub
//!    fits. A type with no fields, or no longer whole than as a stub, is
//!    placed whole instead. A type that does not fit is skipped.
//! 2. Each stub is upgraded to the full type, in the same order, if the
//!    difference fits.
//! 3. The skipped types are listed by name on one line.
//!
//! A declaration whose remaining types all fit whole is returned unchanged.
//! With `types`, the requested types are whole and their own nested types
//! are placed the same way. Stub comments count against the budget; the
//! line of skipped types does not. Search signature lines and Program
//! compilation read the complete stored declaration, never this text.

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet, VecDeque};

use super::envelope::{envelopes, response};
use super::syntax;
use super::tree::{Declarations, Member, Signature, TopLevel, Type};
use crate::{EntityKind, Error};

/// Bytes, in UTF-8, that `describe` spends on result types beyond the parts
/// it always returns whole.
pub(crate) const DESCRIBE_EXTRA_BYTES: usize = 2048;

/// The abridged text `describe` returns for a callable's declaration, or
/// for the `types` of it that the caller names.
///
/// # Errors
///
/// [`Error::UndeclaredTypes`] when `types` names a type the declaration does
/// not declare.
pub(crate) fn abridge(
    declaration: &str,
    kind: EntityKind,
    types: &[String],
) -> Result<String, Error> {
    fit(declaration, kind, types, DESCRIBE_EXTRA_BYTES)
}

fn fit(
    declaration: &str,
    kind: EntityKind,
    types: &[String],
    budget: usize,
) -> Result<String, Error> {
    let Some(declarations) = syntax::read(declaration) else {
        tracing::debug!("a declaration has no readable callable; describe returns it whole");
        return match types {
            [] => Ok(declaration.to_string()),
            _ => Err(Error::UndeclaredTypes {
                undeclared: types.to_vec(),
                declared: Vec::new(),
            }),
        };
    };
    let graph = TypeGraph::new(&declarations);
    let roles = Roles::of(&graph, kind);
    let parts = if types.is_empty() {
        let parts = Parts::of_callable(&graph, &roles);
        if parts.extras_fit_whole(budget) {
            return Ok(declaration.to_string());
        }
        parts
    } else {
        Parts::of_types(&graph, &roles, types)?
    };
    Ok(parts.render(&parts.fit(budget)))
}

/// What `describe` returns: the required texts, always whole and outside
/// the budget, and the extras that share it, in placement order.
struct Parts<'d> {
    required: Vec<&'d str>,
    extras: Vec<Extra<'d>>,
}

/// A type that competes for the budget: its full text, and its stub when
/// the stub is shorter.
struct Extra<'d> {
    name: &'d str,
    full: String,
    stub: Option<String>,
}

/// How an extra is shown.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Form {
    Whole,
    Stub,
    Skipped,
}

impl<'d> Parts<'d> {
    /// A callable's description: its inputs, the callable, the result
    /// wrapper and the top level are required; every other type is an extra.
    fn of_callable(graph: &TypeGraph<'d>, roles: &Roles<'d>) -> Self {
        let untyped = graph.declarations.statements.iter();
        let untyped = untyped.filter(|statement| statement.declares.is_none());
        let mut required = graph.texts(&roles.inputs);
        required.extend(untyped.map(|statement| statement.text.as_str()));
        required.extend(graph.texts(&roles.result));
        Self {
            required,
            extras: graph.extras(&roles.rest),
        }
    }

    /// The requested types: they are required, and their nested types are
    /// extras, except those the callable's description already shows whole.
    fn of_types(graph: &TypeGraph<'d>, roles: &Roles<'d>, types: &[String]) -> Result<Self, Error> {
        let undeclared: Vec<String> = types
            .iter()
            .filter(|name| graph.declared(name).is_none())
            .cloned()
            .collect();
        if !undeclared.is_empty() {
            return Err(Error::UndeclaredTypes {
                undeclared,
                declared: graph.names().map(ToString::to_string).collect(),
            });
        }
        let requested = types.iter().filter_map(|name| graph.declared(name));
        let mut reached = Reached::new(graph);
        let requested = reached.claim(requested);
        reached.claim(roles.inputs.iter().chain(&roles.result).copied());
        let bodies = requested.iter().filter_map(|name| graph.body(name));
        let nested = reached.breadth_first(graph.references(bodies));
        Ok(Self {
            required: graph.texts(&requested),
            extras: graph.extras(&nested),
        })
    }

    fn extras_fit_whole(&self, budget: usize) -> bool {
        self.extras
            .iter()
            .map(|extra| extra.full.len() + 1)
            .sum::<usize>()
            <= budget
    }

    /// Pass 1 places each extra as a stub, or whole when it has no shorter
    /// stub, if it fits. Pass 2 upgrades each stub to whole, in order, if
    /// the difference fits.
    fn fit(&self, budget: usize) -> Vec<Form> {
        let mut used = 0;
        let mut forms = Vec::with_capacity(self.extras.len());
        for extra in &self.extras {
            let form = if extra.stub.is_some() {
                Form::Stub
            } else {
                Form::Whole
            };
            let cost = extra.cost(form);
            if used + cost <= budget {
                used += cost;
                forms.push(form);
            } else {
                forms.push(Form::Skipped);
            }
        }
        for (extra, form) in self.extras.iter().zip(&mut forms) {
            let upgrade = extra.cost(Form::Whole) - extra.cost(*form);
            if *form == Form::Stub && used + upgrade <= budget {
                used += upgrade;
                *form = Form::Whole;
            }
        }
        forms
    }

    /// The required texts, then the placed extras, then one line naming the
    /// skipped extras.
    fn render(&self, forms: &[Form]) -> String {
        let mut lines = self.required.clone();
        let mut skipped = Vec::new();
        for (extra, form) in self.extras.iter().zip(forms) {
            match form {
                Form::Whole => lines.push(&extra.full),
                Form::Stub => lines.extend(extra.stub.as_deref()),
                Form::Skipped => skipped.push(format!("\"{}\"", extra.name)),
            }
        }
        // `types` takes at most DESCRIBE_TYPES_MAX names, so a longer list
        // says to ask for some of them at a time.
        let skipped = match skipped.len() {
            0 => None,
            count if count <= crate::DESCRIBE_TYPES_MAX => Some(format!(
                "// Not shown, to save space. To see them, describe this path with types: [{}].",
                skipped.join(", ")
            )),
            _ => Some(format!(
                "// Not shown, to save space: {}. To see them, describe this path with up to {} of these names in types.",
                skipped.join(", "),
                crate::DESCRIBE_TYPES_MAX
            )),
        };
        lines.extend(skipped.as_deref());
        lines.join("\n")
    }
}

impl Extra<'_> {
    /// Bytes the extra takes in `form`, with its line break.
    fn cost(&self, form: Form) -> usize {
        match form {
            Form::Whole => self.full.len() + 1,
            Form::Stub => self.stub.as_ref().map_or(0, |stub| stub.len() + 1),
            Form::Skipped => 0,
        }
    }
}

/// The declared types of a callable, by role: the inputs and the result
/// frame (the wrapper and the top level of the success data) are required;
/// the rest are extras, in placement order.
struct Roles<'d> {
    inputs: Vec<&'d str>,
    result: Vec<&'d str>,
    rest: Vec<&'d str>,
}

impl<'d> Roles<'d> {
    /// Types are claimed in this order, each by its first role: the inputs,
    /// the wrapper, the top level, the types the success data reaches, then
    /// any type nothing reaches.
    fn of(graph: &TypeGraph<'d>, kind: EntityKind) -> Self {
        let callable = &graph.declarations.callable;
        let result = ResultData::of(callable, kind);
        let mut reached = Reached::new(graph);
        let parameters = callable.parameters.iter();
        let inputs = reached.breadth_first(
            graph.references(parameters.filter_map(|parameter| parameter.value.as_ref())),
        );
        let mut frame = reached.claim(result.wrapper.iter().copied());
        // The top level: the types the success data is, such as
        // `PullRequestSimple` in `Array<PullRequestSimple>`.
        let top = reached.claim(result.success.iter().flat_map(|data| outer_names(data)));
        let mut start = graph.references(result.success.iter().copied());
        start.extend(graph.references(top.iter().filter_map(|name| graph.body(name))));
        let mut rest = reached.breadth_first(start);
        rest.extend(reached.claim(graph.names()));
        frame.extend(top);
        Self {
            inputs,
            result: frame,
            rest,
        }
    }
}

/// Where a callable's result types start: the names its wrapper mentions,
/// and the data of its declared successes.
struct ResultData<'d> {
    wrapper: Vec<&'d str>,
    success: Vec<&'d Type>,
}

impl<'d> ResultData<'d> {
    /// A Tool's result is a union of envelopes, each a wrapper around
    /// one response's data (see `signature::envelope`). A Saved Function's
    /// whole result is success data.
    fn of(callable: &'d Signature, kind: EntityKind) -> Self {
        let mut data = Self {
            wrapper: Vec::new(),
            success: Vec::new(),
        };
        let envelopes = match kind {
            EntityKind::ProviderTool => callable.result.as_ref().and_then(envelopes),
            EntityKind::SavedFunction => None,
        };
        let Some(envelopes) = envelopes else {
            data.success.extend(callable.result.as_ref());
            return data;
        };
        for envelope in envelopes {
            let mentioned = all_names(envelope);
            let Some(response_data) = response(envelope) else {
                // An envelope without readable data: whole.
                data.wrapper.extend(mentioned);
                continue;
            };
            let in_data = all_names(response_data);
            data.wrapper
                .extend(mentioned.into_iter().filter(|name| !in_data.contains(name)));
            data.success.push(response_data);
        }
        data
    }
}

/// The types a declaration declares at its top level, by name, and the
/// references between them.
struct TypeGraph<'d> {
    declarations: &'d Declarations,
    /// Every statement that declares each name, in source order: interfaces
    /// of the same name merge.
    statements: HashMap<&'d str, Vec<&'d TopLevel>>,
}

impl<'d> TypeGraph<'d> {
    fn new(declarations: &'d Declarations) -> Self {
        let mut statements: HashMap<&'d str, Vec<&'d TopLevel>> = HashMap::new();
        for statement in &declarations.statements {
            if let Some(declared) = &statement.declares {
                statements
                    .entry(declared.name.as_str())
                    .or_default()
                    .push(statement);
            }
        }
        Self {
            declarations,
            statements,
        }
    }

    /// The declared names in source order.
    fn names(&self) -> impl Iterator<Item = &'d str> + use<'d> {
        let statements = self.declarations.statements.iter();
        statements.filter_map(|statement| Some(statement.declares.as_ref()?.name.as_str()))
    }

    fn declared(&self, name: &str) -> Option<&'d str> {
        self.statements.get_key_value(name).map(|(name, _)| *name)
    }

    fn body(&self, name: &str) -> Option<&'d Type> {
        self.declarations.aliases.get(name)
    }

    /// The declared types that `types` name, in order.
    fn references(&self, types: impl IntoIterator<Item = &'d Type>) -> Vec<&'d str> {
        let names = types.into_iter().flat_map(all_names);
        names.filter_map(|name| self.declared(name)).collect()
    }

    fn texts(&self, names: &[&'d str]) -> Vec<&'d str> {
        let statements = names.iter().filter_map(|name| self.statements.get(name));
        statements
            .flatten()
            .map(|statement| statement.text.as_str())
            .collect()
    }

    fn extras(&self, names: &[&'d str]) -> Vec<Extra<'d>> {
        names.iter().filter_map(|name| self.extra(name)).collect()
    }

    fn extra(&self, name: &'d str) -> Option<Extra<'d>> {
        let statements = self.statements.get(name)?;
        let statement = statements.first()?;
        let full = self.texts(&[name]).join("\n");
        let mut fields = Vec::new();
        self.field_names(name, &mut HashSet::new(), &mut fields);
        let declared = statement.declares.as_ref();
        let parameters = declared.map_or("", |declared| declared.parameters.as_str());
        let stub = format!(
            "// Stubbed to save space: field names only. For field types, describe this path with types: [\"{name}\"].\ntype {name}{parameters} = {{ {} }};",
            fields.join("; ")
        );
        let shorter = !fields.is_empty() && stub.len() < full.len();
        Some(Extra {
            name,
            full,
            stub: shorter.then_some(stub),
        })
    }

    /// The field names of the declared type `name`, in order, through
    /// unions, intersections and the declared types they name, such as
    /// `Base` in `Base & { own: string }`.
    fn field_names(&self, name: &'d str, seen: &mut HashSet<&'d str>, fields: &mut Vec<&'d str>) {
        if !seen.insert(name) {
            return;
        }
        if let Some(body) = self.body(name) {
            self.collect_fields(body, seen, fields);
        }
    }

    fn collect_fields(
        &self,
        type_: &'d Type,
        seen: &mut HashSet<&'d str>,
        fields: &mut Vec<&'d str>,
    ) {
        match type_ {
            Type::Object(members) => {
                for member in members {
                    if let Member::Property { name, .. } = member
                        && !fields.contains(&name.as_str())
                    {
                        fields.push(name);
                    }
                }
            }
            Type::Union(parts) | Type::Intersection(parts) => {
                for part in parts {
                    self.collect_fields(part, seen, fields);
                }
            }
            Type::Named { name, .. } => {
                if let Some(declared) = self.declared(name) {
                    self.field_names(declared, seen, fields);
                }
            }
            _ => {}
        }
    }
}

/// Declared types claimed in the order they are first reached; a type
/// reached again keeps its first claim.
struct Reached<'g, 'd> {
    graph: &'g TypeGraph<'d>,
    claimed: HashSet<&'d str>,
}

impl<'g, 'd> Reached<'g, 'd> {
    fn new(graph: &'g TypeGraph<'d>) -> Self {
        Self {
            graph,
            claimed: HashSet::new(),
        }
    }

    /// The declared names among `names` not yet claimed, which are now.
    fn claim(&mut self, names: impl IntoIterator<Item = &'d str>) -> Vec<&'d str> {
        let declared = names
            .into_iter()
            .filter_map(|name| self.graph.declared(name));
        declared.filter(|name| self.claimed.insert(name)).collect()
    }

    /// The types reached from `start`, breadth first, not yet claimed, which
    /// are now.
    fn breadth_first(&mut self, start: Vec<&'d str>) -> Vec<&'d str> {
        let mut order = Vec::new();
        let mut queue = VecDeque::from(start);
        while let Some(name) = queue.pop_front() {
            if self.claimed.insert(name) {
                order.push(name);
                queue.extend(self.graph.references(self.graph.body(name)));
            }
        }
        order
    }
}

/// Every name a type mentions, in order, including inside inline objects
/// and function types, without looking through aliases.
fn all_names(type_: &Type) -> Vec<&str> {
    let mut names = Vec::new();
    collect_names(type_, true, &mut names);
    names
}

/// The names a type mentions outside its inline objects: the types the
/// data is, such as `PullRequestSimple` in `Array<PullRequestSimple>`.
fn outer_names(type_: &Type) -> Vec<&str> {
    let mut names = Vec::new();
    collect_names(type_, false, &mut names);
    names
}

fn collect_names<'t>(type_: &'t Type, into_objects: bool, names: &mut Vec<&'t str>) {
    match type_ {
        Type::Named { name, arguments } => {
            names.push(name);
            for argument in arguments {
                collect_names(argument, into_objects, names);
            }
        }
        Type::Union(parts) | Type::Intersection(parts) | Type::Tuple(parts) => {
            for part in parts {
                collect_names(part, into_objects, names);
            }
        }
        Type::Array { element, .. } | Type::Optional(element) => {
            collect_names(element, into_objects, names);
        }
        Type::Object(members) if into_objects => {
            for member in members {
                if let Member::Property { value, .. } | Member::Index { value, .. } = member {
                    collect_names(value, into_objects, names);
                }
            }
        }
        Type::Function { parameters, result } if into_objects => {
            for parameter in parameters {
                if let Some(value) = &parameter.value {
                    collect_names(value, into_objects, names);
                }
            }
            collect_names(result, into_objects, names);
        }
        Type::Object(_) | Type::Function { .. } | Type::Plain(_) | Type::Opaque => {}
    }
}
