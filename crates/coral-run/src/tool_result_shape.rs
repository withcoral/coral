//! Tool Result Shapes: the keys and value types of the data that a failed
//! Program received from its Tool Invocations, never the values.
//!
//! A shape is TypeScript-like text. An array shows the union of its elements'
//! shapes, and a key that some of the merged objects lack is optional, since
//! a Program most often crashes on results whose elements differ.
//!
//! Keys show where data lives, so a shape names an object's keys only when
//! they read as field names. An object keyed by data (identifiers, emails,
//! tokens, paths, names a person chose) shows as `Record<string, V>`, `V` the
//! union of its values' shapes, since its keys are values.

use std::collections::HashMap;

use serde_json::Value;

/// Object levels a shape shows before it shows a deeper object as `{…}`.
const MAX_DEPTH: usize = 6;
/// Keys one object shows. The rest are counted.
const KEYS_SHOWN: usize = 16;
/// Distinct keys one object tracks. An object with more is a map.
const KEYS_TRACKED: usize = 256;
/// Bytes of the longest key that reads as a field name. Field names in the
/// APIs Coral calls stay well under it; longer keys are tokens, URLs or text.
const FIELD_NAME_MAX_BYTES: usize = 40;
/// Keys past which an object whose values are all objects, or all arrays, is
/// a map. Fixed groupings of sub-objects stay under a dozen (GitHub's
/// `_links` has 8), while a collection keyed by data passes it quickly.
const MAP_MIN_UNIFORM_KEYS: usize = 12;
/// Text bytes of every Call Path and shape that one failure carries.
pub(crate) const SHAPES_MAX_BYTES: usize = 4096;
/// The latest Tool Results one failure reads.
pub(crate) const RESULTS_READ: u64 = 64;
/// Retained bytes of Tool Results one failure parses, newest first: a few
/// pages of a typical list API, and a few milliseconds of parsing, however
/// large the results the Program received. A result that does not fit what
/// is left is not read.
pub(crate) const RESULTS_READ_MAX_BYTES: u64 = 4 * 1024 * 1024;

/// The shape of the data that one Call Path returned to a failed Program, and
/// how many of its Tool Invocations returned data of that shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolResultShape {
    call_path: String,
    invocations: u64,
    data: String,
    parsed_text: Option<String>,
}

impl ToolResultShape {
    /// The Call Path the Program called.
    #[must_use]
    pub fn call_path(&self) -> &str {
        &self.call_path
    }

    /// How many Tool Invocations of this Call Path returned data of this shape.
    #[must_use]
    pub const fn invocations(&self) -> u64 {
        self.invocations
    }

    /// The shape of the `data` the Program received, in a Tool Result or a
    /// Tool Error.
    #[must_use]
    pub fn data(&self) -> &str {
        &self.data
    }

    /// The shape of the JSON that the result's MCP text content parses to,
    /// when some of it does.
    #[must_use]
    pub fn parsed_text(&self) -> Option<&str> {
        self.parsed_text.as_deref()
    }
}

/// The Tool Result Shapes of one failed Run, in call order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolResultShapes {
    shapes: Vec<ToolResultShape>,
    unsketched: u64,
}

impl ToolResultShapes {
    /// One shape per distinct Call Path and shape, in the order the Program
    /// first received each.
    #[must_use]
    pub fn shapes(&self) -> &[ToolResultShape] {
        &self.shapes
    }

    /// Tool Results the Program received that no shape describes: results
    /// beyond the ones read, those too large to read, and shapes that did
    /// not fit.
    #[must_use]
    pub const fn unsketched(&self) -> u64 {
        self.unsketched
    }

    /// Whether the Program received a Tool Result at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.shapes.is_empty() && self.unsketched == 0
    }
}

/// One Tool Result a Program received, as Coral retained it.
pub(crate) struct ReceivedResult {
    pub(crate) call_path: String,
    pub(crate) tool_result: Value,
}

/// Sketches `received`, in call order, within the failure's byte budget.
/// `unread` counts earlier results that were not read.
pub(crate) fn sketch(received: Vec<ReceivedResult>, unread: u64) -> ToolResultShapes {
    let mut drafts: Vec<Draft> = Vec::new();
    for result in received {
        let draft = Draft::new(result);
        if let Some(same) = drafts.iter_mut().find(|other| other.same_shape(&draft)) {
            same.invocations += 1;
        } else {
            drafts.push(draft);
        }
    }
    let mut unsketched = unread;
    // Shrink the largest shape a level at a time, then drop the earliest.
    while drafts.iter().map(Draft::bytes).sum::<usize>() > SHAPES_MAX_BYTES {
        let largest = drafts
            .iter_mut()
            .filter(|draft| draft.depth > 1)
            .max_by_key(|draft| draft.bytes());
        if let Some(draft) = largest {
            draft.render(draft.depth - 1);
        } else {
            unsketched += drafts.remove(0).invocations;
        }
    }
    ToolResultShapes {
        shapes: drafts
            .into_iter()
            .map(|draft| ToolResultShape {
                call_path: draft.call_path,
                invocations: draft.invocations,
                data: draft.text,
                parsed_text: draft.parsed_text,
            })
            .collect(),
        unsketched,
    }
}

struct Draft {
    call_path: String,
    invocations: u64,
    data: Shape,
    parsed: Option<Shape>,
    depth: usize,
    text: String,
    parsed_text: Option<String>,
}

impl Draft {
    fn new(result: ReceivedResult) -> Self {
        let data = result
            .tool_result
            .get("data")
            .unwrap_or(&result.tool_result);
        let parsed = if result.tool_result.pointer("/metadata/kind") == Some(&Value::from("mcp")) {
            parsed_text(data)
        } else {
            None
        };
        let mut data = Shape::of(data, 0);
        data.collapse_maps();
        let parsed = parsed.map(|mut shape| {
            shape.collapse_maps();
            shape
        });
        let mut draft = Self {
            call_path: result.call_path,
            invocations: 1,
            data,
            parsed,
            depth: MAX_DEPTH,
            text: String::new(),
            parsed_text: None,
        };
        draft.render(MAX_DEPTH);
        draft
    }

    fn render(&mut self, depth: usize) {
        self.depth = depth;
        self.text = self.data.render(0, depth);
        self.parsed_text = self.parsed.as_ref().map(|shape| shape.render(0, depth));
    }

    /// Whether both have one Call Path and render alike. Shapes that merged a
    /// different number of objects render alike when they show the same, so
    /// the text decides, not the counts behind it. Both render at full depth
    /// here, and text that is alike at full depth stays alike when shrunk.
    fn same_shape(&self, other: &Self) -> bool {
        self.call_path == other.call_path
            && self.text == other.text
            && self.parsed_text == other.parsed_text
    }

    fn bytes(&self) -> usize {
        self.call_path.len() + self.text.len() + self.parsed_text.as_ref().map_or(0, String::len)
    }
}

/// The union of the JSON that the text blocks of MCP `content` parse to,
/// when it is an object or an array.
fn parsed_text(data: &Value) -> Option<Shape> {
    let mut parsed: Option<Shape> = None;
    for block in data.get("content")?.as_array()? {
        if block.get("type").and_then(Value::as_str) != Some("text") {
            continue;
        }
        let Some(text) = block.get("text").and_then(Value::as_str) else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<Value>(text) else {
            continue;
        };
        if !(value.is_object() || value.is_array()) {
            continue;
        }
        let shape = Shape::of(&value, 0);
        match &mut parsed {
            Some(union) => union.merge(shape),
            None => parsed = Some(shape),
        }
    }
    parsed
}

/// A union of JSON kinds, at most one of each, with array elements and
/// objects merged.
#[derive(Debug, Clone, Default, PartialEq)]
struct Shape {
    /// The scalar kinds, one [`Scalar`] bit each.
    scalars: u8,
    array: Option<Box<Self>>,
    object: Option<ObjectShape>,
}

/// A scalar JSON kind, in the order a union shows them.
#[derive(Debug, Clone, Copy)]
enum Scalar {
    String,
    Number,
    Boolean,
    Null,
}

impl Scalar {
    const ALL: [Self; 4] = [Self::String, Self::Number, Self::Boolean, Self::Null];

    const fn bit(self) -> u8 {
        1 << self as u8
    }

    const fn name(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Number => "number",
            Self::Boolean => "boolean",
            Self::Null => "null",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Compound {
    Array,
    Object,
}

#[derive(Debug, Clone, PartialEq)]
struct ObjectShape {
    /// How many objects merged into this one.
    seen: u64,
    fields: Vec<Field>,
    index: HashMap<String, usize>,
    /// The union of the values of keys beyond the ones tracked.
    untracked: Option<Box<Shape>>,
    /// The union of the values of an object keyed by data, whose keys a
    /// shape does not show.
    record: Option<Box<Shape>>,
    /// The object lies beyond the deepest level a shape shows.
    deeper: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct Field {
    name: String,
    /// How many of the merged objects have this key.
    present: u64,
    shape: Shape,
}

impl Shape {
    fn of(value: &Value, level: usize) -> Self {
        let mut shape = Self::default();
        match value {
            Value::Null => shape.scalars = Scalar::Null.bit(),
            Value::Bool(_) => shape.scalars = Scalar::Boolean.bit(),
            Value::Number(_) => shape.scalars = Scalar::Number.bit(),
            Value::String(_) => shape.scalars = Scalar::String.bit(),
            Value::Array(items) => {
                let mut elements = Self::default();
                for item in items {
                    elements.merge(Self::of(item, level));
                }
                shape.array = Some(Box::new(elements));
            }
            Value::Object(fields) => shape.object = Some(ObjectShape::of(fields, level)),
        }
        shape
    }

    fn merge(&mut self, other: Self) {
        self.scalars |= other.scalars;
        match (&mut self.array, other.array) {
            (Some(elements), Some(more)) => elements.merge(*more),
            (array @ None, Some(more)) => *array = Some(more),
            (_, None) => {}
        }
        match (&mut self.object, other.object) {
            (Some(object), Some(more)) => object.merge(more),
            (object @ None, Some(more)) => *object = Some(more),
            (_, None) => {}
        }
    }

    fn render(&self, level: usize, depth: usize) -> String {
        let mut parts = Vec::new();
        if let Some(object) = &self.object {
            parts.push(object.render(level, depth));
        }
        if let Some(elements) = &self.array {
            parts.push(if elements.is_never() {
                "[]".to_string()
            } else {
                format!("Array<{}>", elements.render(level, depth))
            });
        }
        for scalar in Scalar::ALL {
            if self.scalars & scalar.bit() != 0 {
                parts.push(scalar.name().to_string());
            }
        }
        if parts.is_empty() {
            "never".to_string()
        } else {
            parts.join(" | ")
        }
    }

    fn is_never(&self) -> bool {
        *self == Self::default()
    }

    /// Turns every object keyed by data into a record of its values' union.
    fn collapse_maps(&mut self) {
        if let Some(elements) = &mut self.array {
            elements.collapse_maps();
        }
        if let Some(object) = &mut self.object {
            object.collapse_maps();
        }
    }

    /// The one compound kind of a shape that is only an object or only an
    /// array.
    const fn sole_compound_kind(&self) -> Option<Compound> {
        match (self.scalars, &self.array, &self.object) {
            (0, Some(_), None) => Some(Compound::Array),
            (0, None, Some(_)) => Some(Compound::Object),
            _ => None,
        }
    }
}

impl ObjectShape {
    fn of(fields: &serde_json::Map<String, Value>, level: usize) -> Self {
        let mut object = Self {
            seen: 1,
            fields: Vec::new(),
            index: HashMap::new(),
            untracked: None,
            record: None,
            deeper: level >= MAX_DEPTH && !fields.is_empty(),
        };
        if object.deeper {
            return object;
        }
        for (name, value) in fields {
            object.add(Field {
                name: name.clone(),
                present: 1,
                shape: Shape::of(value, level + 1),
            });
        }
        object
    }

    fn add(&mut self, field: Field) {
        if let Some(known) = self
            .index
            .get(&field.name)
            .and_then(|&position| self.fields.get_mut(position))
        {
            known.present += field.present;
            known.shape.merge(field.shape);
        } else if self.fields.len() < KEYS_TRACKED {
            self.index.insert(field.name.clone(), self.fields.len());
            self.fields.push(field);
        } else {
            merge_into(&mut self.untracked, field.shape);
        }
    }

    fn merge(&mut self, other: Self) {
        self.seen += other.seen;
        self.deeper |= other.deeper;
        if let Some(untracked) = other.untracked {
            merge_into(&mut self.untracked, *untracked);
        }
        if let Some(record) = other.record {
            merge_into(&mut self.record, *record);
        }
        for field in other.fields {
            self.add(field);
        }
    }

    /// Whether the keys are data rather than field names: one of them does
    /// not read as a field name, there are more than are tracked, or there
    /// are many and every value is an object, or every value an array.
    fn is_map(&self) -> bool {
        if self.untracked.is_some() || self.fields.iter().any(|field| !is_field_name(&field.name)) {
            return true;
        }
        let mut kinds = self
            .fields
            .iter()
            .map(|field| field.shape.sole_compound_kind());
        self.fields.len() >= MAP_MIN_UNIFORM_KEYS
            && kinds
                .next()
                .flatten()
                .is_some_and(|first| kinds.all(|kind| kind == Some(first)))
    }

    fn collapse_maps(&mut self) {
        if self.is_map() {
            self.index.clear();
            for field in std::mem::take(&mut self.fields) {
                merge_into(&mut self.record, field.shape);
            }
            if let Some(untracked) = self.untracked.take() {
                merge_into(&mut self.record, *untracked);
            }
        }
        if let Some(record) = &mut self.record {
            record.collapse_maps();
        }
        for field in &mut self.fields {
            field.shape.collapse_maps();
        }
    }

    fn render(&self, level: usize, depth: usize) -> String {
        if self.fields.is_empty() && self.record.is_none() && !self.deeper {
            return "{}".to_string();
        }
        if self.deeper || level >= depth {
            return "{…}".to_string();
        }
        if let Some(values) = &self.record {
            return format!("Record<string, {}>", values.render(level + 1, depth));
        }
        let mut members: Vec<String> = self
            .fields
            .iter()
            .take(KEYS_SHOWN)
            .map(|field| {
                format!(
                    "{}{}: {}",
                    key(&field.name),
                    if field.present < self.seen { "?" } else { "" },
                    field.shape.render(level + 1, depth)
                )
            })
            .collect();
        let more = self.fields.len().saturating_sub(KEYS_SHOWN);
        if more > 0 {
            let keys = if more == 1 { "key" } else { "keys" };
            members.push(format!("… {more} more {keys}"));
        }
        format!("{{ {} }}", members.join("; "))
    }
}

fn merge_into(union: &mut Option<Box<Shape>>, shape: Shape) {
    match union {
        Some(union) => union.merge(shape),
        None => *union = Some(Box::new(shape)),
    }
}

/// Whether a key reads as a field name an API chose rather than as data: at
/// most [`FIELD_NAME_MAX_BYTES`] of ASCII letters, digits, `_`, `-` and `$`,
/// starting with a letter, `_` or `$`, and not shaped like an ID, a hash or
/// a token. Whitespace, `@`, `/`, `:`, `.` and other characters mark names a
/// person chose, emails, paths, URLs and hosts.
fn is_field_name(name: &str) -> bool {
    let separator = |byte: u8| matches!(byte, b'_' | b'-' | b'$');
    let bytes = name.as_bytes();
    let Some(&first) = bytes.first() else {
        return false;
    };
    if bytes.len() > FIELD_NAME_MAX_BYTES
        || !(first.is_ascii_alphabetic() || first == b'_' || first == b'$')
        || !bytes
            .iter()
            .all(|&byte| byte.is_ascii_alphanumeric() || separator(byte))
    {
        return false;
    }
    let alphanumeric = bytes
        .iter()
        .filter(|byte| byte.is_ascii_alphanumeric())
        .count();
    let digits = bytes.iter().filter(|byte| byte.is_ascii_digit()).count();
    // Mostly digits: `user_98765432`.
    if digits * 2 > alphanumeric {
        return false;
    }
    // Hex with a digit: a hash, or a UUID that starts with a letter.
    if alphanumeric >= 8
        && digits > 0
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() || separator(*byte))
    {
        return false;
    }
    // Letters and digits that alternate three times or more within one word
    // read as a token (`U024BE7LH`, `4eC39Hq`), while field names carry at
    // most a number or two (`sha256`, `x509Certificate`, `line2`).
    !bytes.split(|&byte| separator(byte)).any(|word| {
        word.windows(2)
            .filter(
                |pair| matches!(pair, [one, next] if one.is_ascii_digit() != next.is_ascii_digit()),
            )
            .count()
            >= 3
    })
}

/// A key as TypeScript writes it: bare when it is an identifier, quoted
/// otherwise.
fn key(name: &str) -> String {
    if name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$')
    {
        name.to_string()
    } else {
        serde_json::to_string(name).unwrap_or_else(|_never| "\"\"".to_string())
    }
}

#[cfg(test)]
mod tests;
