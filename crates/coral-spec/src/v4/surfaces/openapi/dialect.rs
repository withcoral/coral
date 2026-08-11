use serde_json::Value;

/// The rules that differ between the `OpenAPI` versions the importer supports.
///
/// The traversal itself is shared: response selection, pagination detection,
/// row-path inference, and `allOf` folding all read a 3.1 document exactly as
/// they read a 3.0 one. Only the few keywords the versions genuinely disagree
/// about route through here, which keeps each version's rules readable as one
/// implementation instead of as `match` arms spread across the traversal.
pub(super) trait OpenApiDialect {
    /// Whether a schema admits `null`.
    fn schema_nullable(&self, schema: &Value) -> bool;
}
