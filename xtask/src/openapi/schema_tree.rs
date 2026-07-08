//! JSON-schema reconstruction for OpenAPI export.
//!
//! A v3 manifest never states the provider's response shape directly; it
//! declares typed columns whose expressions *address into* the raw response
//! rows. This module rebuilds the implied JSON shape: each column expression
//! contributes the fields it reads to a [`SchemaTree`], and the merged tree
//! renders as one OpenAPI schema object. The merge rules (structure beats
//! scalar, conflicting scalars widen to the empty schema) live here so the
//! converter never reasons about shape conflicts.

use coral_spec::{ColumnSpec, ExprSpec, ManifestDataType, TimestampInput};
use serde_json::{Value, json};

/// A mergeable JSON-schema fragment addressed by response paths.
///
/// Leaves are ready-made OpenAPI schema objects; interior nodes are objects
/// or arrays inferred from the paths and expressions inserted into the tree.
/// Children keep insertion order, matching column declaration order.
#[derive(Debug, Default)]
pub(crate) struct SchemaTree {
    root: Option<Node>,
}

#[derive(Debug)]
enum Node {
    Leaf(Value),
    Object(Vec<(String, Node)>),
    Array(Box<Node>),
}

impl SchemaTree {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// A tree whose root is the given ready-made schema object.
    pub(crate) fn leaf(schema: Value) -> Self {
        Self {
            root: Some(Node::Leaf(schema)),
        }
    }

    /// A tree whose root is an array of the given item tree.
    pub(crate) fn array_of(items: Self) -> Self {
        Self {
            root: Some(Node::Array(Box::new(
                items.root.unwrap_or(Node::Leaf(json!({}))),
            ))),
        }
    }

    /// Returns true when nothing has been inserted.
    pub(crate) fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    /// Merge `subtree` into this tree at `path`, creating object nodes for
    /// each path segment. An empty path merges at the root; an empty subtree
    /// is a no-op.
    pub(crate) fn insert(&mut self, path: &[String], subtree: Self) {
        let Some(mut node) = subtree.root else {
            return;
        };
        for segment in path.iter().rev() {
            node = Node::Object(vec![(segment.clone(), node)]);
        }
        self.root = Some(match self.root.take() {
            Some(existing) => merge(existing, node),
            None => node,
        });
    }

    /// Detach and return the direct child object property named `name`.
    ///
    /// Used for `dict_entries` tables, whose synthesized `_key`/`_value`
    /// columns do not correspond to raw response fields. Returns `None`
    /// when the root is not an object or has no such property.
    pub(crate) fn remove_child(&mut self, name: &str) -> Option<Self> {
        let Some(Node::Object(children)) = &mut self.root else {
            return None;
        };
        let index = children.iter().position(|(key, _)| key == name)?;
        let (_, child) = children.remove(index);
        if children.is_empty() {
            self.root = None;
        }
        Some(Self { root: Some(child) })
    }

    /// Render the tree as one OpenAPI schema object; empty trees render as
    /// the unconstrained schema `{}`.
    pub(crate) fn into_schema(self) -> Value {
        self.root.map_or_else(|| json!({}), render)
    }
}

fn render(node: Node) -> Value {
    match node {
        Node::Leaf(schema) => schema,
        Node::Object(children) => {
            let properties: serde_json::Map<String, Value> = children
                .into_iter()
                .map(|(name, child)| (name, render(child)))
                .collect();
            json!({"type": "object", "properties": properties})
        }
        Node::Array(items) => json!({"type": "array", "items": render(*items)}),
    }
}

/// Merge two nodes describing the same location. Structure wins over
/// scalars (a column may read `labels` as raw JSON text while another
/// addresses `labels[].name`); conflicting shapes widen to `{}`.
fn merge(left: Node, right: Node) -> Node {
    match (left, right) {
        (Node::Object(mut left_children), Node::Object(right_children)) => {
            for (name, right_child) in right_children {
                match left_children
                    .iter()
                    .position(|(existing, _)| *existing == name)
                {
                    Some(index) => {
                        let (name, left_child) = left_children.remove(index);
                        left_children.insert(index, (name, merge(left_child, right_child)));
                    }
                    None => left_children.push((name, right_child)),
                }
            }
            Node::Object(left_children)
        }
        (Node::Array(left_items), Node::Array(right_items)) => {
            Node::Array(Box::new(merge(*left_items, *right_items)))
        }
        (Node::Leaf(left_schema), Node::Leaf(right_schema)) => {
            Node::Leaf(merge_leaf_schemas(left_schema, right_schema))
        }
        // Structure beats a scalar leaf: keep whichever side is composite.
        (composite @ (Node::Object(_) | Node::Array(_)), Node::Leaf(_))
        | (Node::Leaf(_), composite @ (Node::Object(_) | Node::Array(_))) => composite,
        // Object vs array is a genuine shape conflict: widen to `{}`.
        (Node::Object(_), Node::Array(_)) | (Node::Array(_), Node::Object(_)) => {
            Node::Leaf(json!({}))
        }
    }
}

fn merge_leaf_schemas(left: Value, right: Value) -> Value {
    if is_unconstrained(&left) {
        return right;
    }
    if right == left || is_unconstrained(&right) {
        return left;
    }
    // Same declared type: keep the first (richer descriptions arrive first
    // because columns contribute in declaration order). Different types:
    // widen to the unconstrained schema.
    if left.get("type").is_some() && left.get("type") == right.get("type") {
        left
    } else {
        json!({})
    }
}

fn is_unconstrained(schema: &Value) -> bool {
    schema.as_object().is_some_and(serde_json::Map::is_empty)
}

/// The OpenAPI schema object for one manifest scalar type.
pub(crate) fn scalar_schema(data_type: ManifestDataType) -> Value {
    match data_type {
        ManifestDataType::Utf8 => json!({"type": "string"}),
        ManifestDataType::Int64 => json!({"type": "integer", "format": "int64"}),
        ManifestDataType::Float64 => json!({"type": "number", "format": "double"}),
        ManifestDataType::Boolean => json!({"type": "boolean"}),
        ManifestDataType::Timestamp => json!({"type": "string", "format": "date-time"}),
        ManifestDataType::Json => json!({}),
    }
}

/// Rebuild the raw response-row schema implied by a table's declared
/// columns. Virtual columns and expressions that echo request values
/// (`from_filter`, `from_arg`, literals) contribute nothing because they do
/// not read the response payload.
pub(crate) fn row_schema_tree(columns: &[ColumnSpec]) -> SchemaTree {
    let mut tree = SchemaTree::new();
    for column in columns {
        if column.r#virtual {
            continue;
        }
        let mut leaf = scalar_schema(column.data_type);
        if let Some(leaf_object) = leaf.as_object_mut() {
            if column.nullable && !leaf_object.is_empty() {
                leaf_object.insert("nullable".to_string(), json!(true));
            }
            if !column.description.is_empty() {
                leaf_object.insert("description".to_string(), json!(column.description));
            }
        }
        contribute_expr(&mut tree, &column.resolved_expr(), leaf);
    }
    tree
}

/// Add the raw fields read by `expr` to `tree`. `leaf` is the schema of the
/// value the expression ultimately produces; transforms substitute the
/// schema their input must have (for example `base64_decode` reads a
/// string regardless of the column type).
fn contribute_expr(tree: &mut SchemaTree, expr: &ExprSpec, leaf: Value) {
    match expr {
        ExprSpec::Path { path } => tree.insert(path, SchemaTree::leaf(leaf)),
        ExprSpec::Coalesce { exprs } => {
            for sub_expr in exprs {
                contribute_expr(tree, sub_expr, leaf.clone());
            }
        }
        ExprSpec::JoinArray { path, .. } => {
            tree.insert(path, SchemaTree::array_of(SchemaTree::leaf(json!({}))));
        }
        ExprSpec::JoinArrayPath {
            path, item_path, ..
        } => {
            let mut item = SchemaTree::new();
            item.insert(item_path, SchemaTree::leaf(json!({})));
            tree.insert(path, SchemaTree::array_of(item));
        }
        ExprSpec::FirstArrayItemPath { path, item_path } => {
            let mut item = SchemaTree::new();
            item.insert(item_path, SchemaTree::leaf(leaf));
            tree.insert(path, SchemaTree::array_of(item));
        }
        ExprSpec::TagValue {
            path,
            key_field,
            value_field,
            ..
        }
        | ExprSpec::JoinTagValues {
            path,
            key_field,
            value_field,
            ..
        } => {
            let mut item = SchemaTree::new();
            item.insert(
                std::slice::from_ref(key_field),
                SchemaTree::leaf(json!({"type": "string"})),
            );
            item.insert(
                std::slice::from_ref(value_field),
                SchemaTree::leaf(json!({})),
            );
            tree.insert(path, SchemaTree::array_of(item));
        }
        ExprSpec::ObjectFilterPath {
            path,
            filter_key,
            item_path,
        } => {
            let mut item = SchemaTree::new();
            item.insert(
                std::slice::from_ref(filter_key),
                SchemaTree::leaf(json!({"type": "string"})),
            );
            item.insert(item_path, SchemaTree::leaf(leaf));
            tree.insert(path, SchemaTree::array_of(item));
        }
        ExprSpec::IfPresent { check, .. } => contribute_expr(tree, check, json!({})),
        ExprSpec::FormatTimestamp { expr, input } => {
            let input_leaf = match input {
                TimestampInput::Seconds | TimestampInput::Milliseconds => json!({"type": "number"}),
                TimestampInput::Iso8601 => json!({"type": "string"}),
            };
            contribute_expr(tree, expr, input_leaf);
        }
        ExprSpec::Base64Decode { expr } | ExprSpec::Replace { expr, .. } => {
            contribute_expr(tree, expr, json!({"type": "string"}));
        }
        ExprSpec::Template { values, .. } => {
            for value_expr in values.values() {
                contribute_expr(tree, value_expr, json!({}));
            }
        }
        // These read request state or constants, not the response payload.
        ExprSpec::FromFilter { .. }
        | ExprSpec::FromArg { .. }
        | ExprSpec::Literal { .. }
        | ExprSpec::Null
        | ExprSpec::CurrentRow => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn utf8_column(name: &str, path: &[&str]) -> ColumnSpec {
        column(name, ManifestDataType::Utf8, path)
    }

    fn column(name: &str, data_type: ManifestDataType, path: &[&str]) -> ColumnSpec {
        serde_json::from_value(json!({
            "name": name,
            "type": data_type.as_manifest_str(),
            "nullable": false,
            "expr": {"kind": "path", "path": path},
        }))
        .expect("column spec")
    }

    #[test]
    fn nested_paths_reconstruct_objects() {
        let columns = vec![
            utf8_column("login", &["user", "login"]),
            column("id", ManifestDataType::Int64, &["user", "id"]),
            utf8_column("title", &["title"]),
        ];
        assert_eq!(
            row_schema_tree(&columns).into_schema(),
            json!({
                "type": "object",
                "properties": {
                    "user": {
                        "type": "object",
                        "properties": {
                            "login": {"type": "string"},
                            "id": {"type": "integer", "format": "int64"},
                        }
                    },
                    "title": {"type": "string"},
                }
            })
        );
    }

    #[test]
    fn structure_wins_over_raw_json_scalar() {
        let raw: ColumnSpec = serde_json::from_value(json!({
            "name": "labels",
            "type": "Json",
            "expr": {"kind": "path", "path": ["labels"]},
        }))
        .expect("column spec");
        let joined: ColumnSpec = serde_json::from_value(json!({
            "name": "label_names",
            "type": "Utf8",
            "expr": {"kind": "join_array_path", "path": ["labels"], "item_path": ["name"]},
        }))
        .expect("column spec");

        assert_eq!(
            row_schema_tree(&[raw, joined]).into_schema(),
            json!({
                "type": "object",
                "properties": {
                    "labels": {
                        "type": "array",
                        "items": {"type": "object", "properties": {"name": {}}}
                    }
                }
            })
        );
    }

    #[test]
    fn conflicting_scalar_types_widen_to_unconstrained() {
        let columns = vec![
            column("count", ManifestDataType::Int64, &["count"]),
            utf8_column("count_text", &["count"]),
        ];
        assert_eq!(
            row_schema_tree(&columns).into_schema(),
            json!({"type": "object", "properties": {"count": {}}})
        );
    }

    #[test]
    fn virtual_and_filter_echo_columns_contribute_nothing() {
        let echo: ColumnSpec = serde_json::from_value(json!({
            "name": "org",
            "type": "Utf8",
            "expr": {"kind": "from_filter", "key": "org"},
        }))
        .expect("column spec");
        let synthetic: ColumnSpec = serde_json::from_value(json!({
            "name": "synthetic",
            "type": "Utf8",
            "virtual": true,
        }))
        .expect("column spec");
        assert!(row_schema_tree(&[echo, synthetic]).is_empty());
    }

    #[test]
    fn remove_child_detaches_dict_key() {
        let mut tree = row_schema_tree(&[
            utf8_column("key", &["_key"]),
            utf8_column("status", &["_value"]),
        ]);
        assert!(tree.remove_child("_key").is_some());
        let value = tree.remove_child("_value").expect("value child");
        assert!(tree.is_empty());
        assert_eq!(value.into_schema(), json!({"type": "string"}));
    }
}
