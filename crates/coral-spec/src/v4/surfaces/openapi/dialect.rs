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

    /// The values a schema's `const` constrains it to, if this version has that
    /// keyword and the schema uses it.
    fn const_enum_values(&self, schema: &Value) -> Option<Vec<String>>;

    /// Whether keywords written beside a `$ref` constrain the schema it
    /// resolves to, or are ignored in its favour.
    ///
    /// The versions genuinely disagree: 3.0 defines a Reference Object as an
    /// object whose other members are ignored, while 3.1 follows 2020-12, where
    /// `$ref` is one keyword among the rest and every sibling still applies.
    fn ref_siblings_apply(&self) -> bool;

    /// What to warn about a schema reaching for a keyword this version removed.
    ///
    /// Such a keyword is not an error — it is ignored, exactly as an unknown
    /// annotation would be — but ignoring it silently changes what the schema
    /// means, and the author is unlikely to have intended that.
    fn removed_keyword_warning(&self, schema: &Value) -> Option<String>;
}
