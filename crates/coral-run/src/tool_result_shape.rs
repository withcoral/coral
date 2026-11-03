//! Tool Result Shapes: the keys and value types of the data that a failed
//! Program received from its Tool Invocations, never the values.
//!
//! A shape is TypeScript-like text. An array shows the union of its elements'
//! shapes, and a key that some of the merged objects lack is optional, since
//! a Program most often crashes on results whose elements differ.

use std::collections::HashMap;

use serde_json::Value;

/// Object levels a shape shows before it shows a deeper object as `{…}`.
const MAX_DEPTH: usize = 6;
/// Keys one object shows. The rest are counted, since some objects are maps
/// keyed by data.
const KEYS_SHOWN: usize = 16;
/// Distinct keys one object tracks, so a large map stays bounded.
const KEYS_TRACKED: usize = 256;
/// Bytes of one key name a shape shows.
const KEY_NAME_MAX_BYTES: usize = 48;
/// Text bytes of every Call Path and shape that one failure carries.
pub(crate) const SHAPES_MAX_BYTES: usize = 4096;
/// The latest Tool Results one failure reads.
pub(crate) const RESULTS_READ: u64 = 64;

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

    /// Tool Results the Program received that no shape describes: earlier
    /// results beyond the ones read, and shapes that did not fit.
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
        let mut draft = Self {
            call_path: result.call_path,
            invocations: 1,
            data: Shape::of(data, 0),
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

    fn same_shape(&self, other: &Self) -> bool {
        self.call_path == other.call_path && self.data == other.data && self.parsed == other.parsed
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

#[derive(Debug, Clone, PartialEq)]
struct ObjectShape {
    /// How many objects merged into this one.
    seen: u64,
    fields: Vec<Field>,
    index: HashMap<String, usize>,
    /// Keys beyond the ones tracked were seen.
    overflow: bool,
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
}

impl ObjectShape {
    fn of(fields: &serde_json::Map<String, Value>, level: usize) -> Self {
        let mut object = Self {
            seen: 1,
            fields: Vec::new(),
            index: HashMap::new(),
            overflow: false,
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
            self.overflow = true;
        }
    }

    fn merge(&mut self, other: Self) {
        self.seen += other.seen;
        self.overflow |= other.overflow;
        self.deeper |= other.deeper;
        for field in other.fields {
            self.add(field);
        }
    }

    fn render(&self, level: usize, depth: usize) -> String {
        if self.fields.is_empty() && !self.overflow && !self.deeper {
            return "{}".to_string();
        }
        if self.deeper || level >= depth {
            return "{…}".to_string();
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
        if more > 0 || self.overflow {
            let plus = if self.overflow { "+" } else { "" };
            let keys = if more == 1 && !self.overflow {
                "key"
            } else {
                "keys"
            };
            members.push(format!("… {more}{plus} more {keys}"));
        }
        format!("{{ {} }}", members.join("; "))
    }
}

/// A key as TypeScript writes it: bare when it is an identifier, quoted
/// otherwise, and shortened when it is long.
fn key(name: &str) -> String {
    let mut end = name.len().min(KEY_NAME_MAX_BYTES);
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    let shown = name.get(..end).unwrap_or_default();
    let identifier = shown
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_' || first == '$')
        && shown
            .chars()
            .all(|next| next.is_ascii_alphanumeric() || next == '_' || next == '$');
    let mut written = if identifier {
        shown.to_string()
    } else {
        serde_json::to_string(shown).unwrap_or_else(|_never| "\"\"".to_string())
    };
    if end < name.len() {
        written.push('…');
    }
    written
}

#[cfg(test)]
mod tests;
