//! The small type tree a signature line is rendered from: what the line
//! shows of a TypeScript declaration, and nothing more.

use std::collections::HashMap;

/// A type, reduced to what the line shows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Type {
    Union(Vec<Self>),
    Intersection(Vec<Self>),
    Object(Vec<Member>),
    Named {
        name: String,
        arguments: Vec<Self>,
    },
    Array {
        element: Box<Self>,
        readonly: bool,
    },
    Tuple(Vec<Self>),
    /// An optional tuple element: `T?`.
    Optional(Box<Self>),
    /// A keyword or literal type, as written.
    Plain(String),
    Function {
        parameters: Vec<Parameter>,
        result: Box<Self>,
    },
    /// A construct the line does not show.
    Opaque,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Member {
    Property {
        name: String,
        optional: bool,
        value: Type,
    },
    Index {
        key: String,
        value: Type,
    },
    Opaque,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Parameter {
    pub(super) name: String,
    pub(super) optional: bool,
    pub(super) rest: bool,
    pub(super) value: Option<Type>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Signature {
    pub(super) parameters: Vec<Parameter>,
    pub(super) result: Option<Type>,
}

/// The declared type aliases and interfaces, the callable the line shows,
/// and the top-level statements in source order.
pub(super) struct Declarations {
    pub(super) aliases: HashMap<String, Type>,
    pub(super) callable: Signature,
    pub(super) statements: Vec<TopLevel>,
}

/// One top-level statement of a declaration: its text, with the comments
/// before it, and the type it declares, if it declares one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TopLevel {
    pub(super) text: String,
    pub(super) declares: Option<DeclaredType>,
}

/// A type alias or interface a top-level statement declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DeclaredType {
    pub(super) name: String,
    /// Its type parameters as written, such as `<T>`, or empty.
    pub(super) parameters: String,
}
