//! Canonical structured query error contract.

use std::collections::HashMap;

use datafusion::common::utils::quote_identifier;

use super::catalog::TableInfo;
use super::error::StatusCode;

/// Wire-stable `reason` code for unknown-column errors.
pub(crate) const UNKNOWN_COLUMN_REASON: &str = "UNKNOWN_COLUMN";

/// Wire-stable `reason` code for table-not-found errors.
pub(crate) const TABLE_NOT_FOUND_REASON: &str = "TABLE_NOT_FOUND";

/// Minimum `strsim::normalized_levenshtein` score for a "did you mean?"
/// suggestion to surface.
///
/// Matches the value `datafusion-common` uses in its own `FieldNotFound`
/// Display impl (`filter(|s| normalized_levenshtein(s, field_name) >= 0.5)`),
/// so a user sees the same forgiveness whether the error routes through
/// our structured enrichment path or `DataFusion`'s raw error text. Change
/// this only if we have data justifying divergence from that baseline.
const DID_YOU_MEAN_SIMILARITY: f64 = 0.5;

/// Structure-preserving column reference used by `unknown_column`.
///
/// Keeping the qualifier components separate from the bare name means the
/// hint builder can render a `"player.id"`-style identifier (literal dot
/// inside the name) without any downstream logic having to guess whether a
/// given `.` is a qualifier separator or part of the identifier.
#[derive(Debug, Clone)]
pub(crate) struct ColumnParts {
    /// 0, 1, 2, or 3 qualifier segments (alias / schema.table /
    /// catalog.schema.table) — each stored as a bare identifier.
    pub relation: Vec<String>,
    /// The bare column name, exactly as registered in the schema
    /// (case-preserving, may contain embedded dots).
    pub name: String,
}

/// Structure-preserving table reference used by `table_not_found`.
///
/// `DataFusion` formats missing table references as dotted strings, which loses
/// the distinction between `schema.table` and a quoted identifier containing a
/// literal dot. Keeping the parsed object-name parts separate lets the table
/// hint code recover the user's intent without reparsing by raw `.` splits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableRefParts {
    /// Object-name parts in order: table, schema.table, or
    /// catalog.schema.table. Each element is a bare identifier value.
    pub parts: Vec<String>,
}

impl TableRefParts {
    pub(crate) fn new(parts: Vec<String>) -> Self {
        Self { parts }
    }
}

impl ColumnParts {
    /// Renders the reference as SQL, quoting each component individually.
    fn quoted(&self) -> String {
        let mut out = String::new();
        for part in &self.relation {
            out.push_str(&quote_identifier(part));
            out.push('.');
        }
        out.push_str(&quote_identifier(&self.name));
        out
    }

    /// Joins all components with dots, lowercased. Used for
    /// structure-preserving case-insensitive equality: two references with
    /// the same dotted form AND the same number of components match.
    fn joined_lower(&self) -> String {
        let mut out = String::new();
        for part in &self.relation {
            out.push_str(&part.to_lowercase());
            out.push('.');
        }
        out.push_str(&self.name.to_lowercase());
        out
    }

    /// Renders without quoting — for the user-facing summary line where
    /// readability matters more than round-trip SQL safety.
    fn flat_display(&self) -> String {
        let mut out = String::new();
        for part in &self.relation {
            out.push_str(part);
            out.push('.');
        }
        out.push_str(&self.name);
        out
    }
}

/// Structured query failure with first-class semantic fields.
#[derive(Debug, Clone)]
pub struct StructuredQueryError {
    reason: String,
    summary: String,
    detail: String,
    hint: Option<String>,
    retryable: bool,
    status: StatusCode,
    metadata: HashMap<String, String>,
}

impl StructuredQueryError {
    /// Builds a structured error from its parts.
    pub(crate) fn new(
        reason: impl Into<String>,
        summary: impl Into<String>,
        detail: impl Into<String>,
        hint: Option<String>,
        retryable: bool,
        status: StatusCode,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            reason: reason.into(),
            summary: summary.into(),
            detail: detail.into(),
            hint,
            retryable,
            status,
            metadata,
        }
    }

    /// Builds a structured `UNKNOWN_COLUMN` error from a missing column and
    /// its in-scope candidates. Callers pass both the missing reference and
    /// each candidate as a `ColumnParts` (qualifier segments plus bare
    /// name) so dotted identifiers round-trip without ambiguity.
    pub(crate) fn unknown_column(missing: &ColumnParts, valid_columns: &[ColumnParts]) -> Self {
        let hint = unknown_column_hint(missing, valid_columns);

        let mut metadata = HashMap::new();
        metadata.insert("column".to_string(), missing.flat_display());

        let display_missing = missing.flat_display();
        let detail = if valid_columns.is_empty() {
            format!("No column matching `{display_missing}` is in scope.")
        } else {
            let preview: Vec<String> = valid_columns
                .iter()
                .take(10)
                .map(ColumnParts::flat_display)
                .collect();
            format!(
                "No column `{display_missing}` is in scope. Valid columns include: {}.",
                preview.join(", ")
            )
        };

        Self::new(
            UNKNOWN_COLUMN_REASON,
            format!("No column named `{display_missing}`"),
            detail,
            hint,
            false,
            StatusCode::InvalidArgument,
            metadata,
        )
    }

    /// Builds a structured `TABLE_NOT_FOUND` error.
    ///
    /// `missing_ref` preserves the SQL object-name components for the missing
    /// table. `known_tables` is consulted to (a) distinguish `DataFusion`'s
    /// synthetic `public` schema from a real user source also named `public`,
    /// and (b) recover a correct `(schema, table)` split when the source
    /// name itself contains a dot.
    pub(crate) fn table_not_found(missing_ref: &TableRefParts, known_tables: &[TableInfo]) -> Self {
        let parsed = parse_table_ref(missing_ref, known_tables);

        let (schema, table) = match &parsed {
            ParsedTableRef::Unqualified { table } => (None, table.clone()),
            ParsedTableRef::Qualified { schema, table } => (Some(schema.clone()), table.clone()),
        };

        let display_ref = match &schema {
            Some(schema) => format!("{schema}.{table}"),
            None => table.clone(),
        };
        let hint = table_not_found_hint(schema.as_deref(), &table, known_tables);

        let mut metadata = HashMap::new();
        if let Some(schema) = &schema {
            metadata.insert("schema".to_string(), schema.clone());
        }
        metadata.insert("table".to_string(), table.clone());

        let detail = match schema.as_deref() {
            Some(schema) => format!("No table `{table}` exists in schema `{schema}`."),
            None => format!("No table `{table}` exists in any registered schema."),
        };

        Self::new(
            TABLE_NOT_FOUND_REASON,
            format!("Table `{display_ref}` not found"),
            detail,
            hint,
            false,
            StatusCode::NotFound,
            metadata,
        )
    }

    /// Machine-readable error reason (e.g. `"MISSING_REQUIRED_FILTER"`).
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }

    /// One-line error summary.
    #[must_use]
    pub fn summary(&self) -> &str {
        &self.summary
    }

    /// Longer explanation (may be empty).
    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }

    /// Actionable recovery guidance.
    #[must_use]
    pub fn hint(&self) -> Option<&str> {
        self.hint.as_deref()
    }

    /// Whether the error is transient.
    #[must_use]
    pub fn retryable(&self) -> bool {
        self.retryable
    }

    /// Transport-neutral status code.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Additional key-value metadata.
    #[must_use]
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }
}

impl std::fmt::Display for StructuredQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.summary)?;
        if !self.detail.is_empty() {
            write!(f, "\n{}", self.detail)?;
        }
        if let Some(hint) = &self.hint {
            write!(f, "\nHint: {hint}")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Hint builders
// ---------------------------------------------------------------------------

/// Hint template for a case-only mismatch (`Master` vs `master`, `playerID`
/// vs `playerid`). Keeping this in one place so the wording stays
/// consistent across column and table hints.
fn case_sensitive_hint(suggestion: &str) -> String {
    format!("Unquoted identifiers are lowercased. Try `{suggestion}`.")
}

/// Hint template for a Levenshtein "closest match" suggestion.
fn did_you_mean_hint(suggestion: &str) -> String {
    format!("Did you mean `{suggestion}`?")
}

fn unknown_column_hint(missing: &ColumnParts, valid_columns: &[ColumnParts]) -> Option<String> {
    // Prefer a full-reference case-insensitive hit: same qualifier shape and
    // same name, differing only in case. Reproduces the user's qualifier in
    // the suggestion (alias vs resolved schema.table), preserving the
    // literal identifier inside the bare name — so a declared `"player.id"`
    // column survives round-trip.
    let missing_key = missing.joined_lower();
    if let Some(exact) = valid_columns
        .iter()
        .find(|candidate| candidate.joined_lower() == missing_key)
    {
        return Some(case_sensitive_hint(&exact.quoted()));
    }

    // Dotted-column case: the declared column name itself contains a dot
    // (e.g. `"player.id"`). A user who writes `SELECT player.id FROM …`
    // unquoted reaches us as `relation=[player], name=id`; the candidate
    // has `relation=[schema, table], name="player.id"`. The user's full
    // flat form (`player.id`) is what the candidate's bare name actually
    // is — match on that so the hint quotes the real column.
    if let Some(exact) = valid_columns
        .iter()
        .find(|candidate| candidate.name.to_lowercase() == missing_key)
    {
        return Some(case_sensitive_hint(&exact.quoted()));
    }

    // Bare-name case-insensitive fallback: the user supplied a different
    // qualifier shape than the valid-columns list does (e.g. unqualified
    // typo against a schema.table.column candidate). Comparing only the
    // bare name still picks up a capitalization-only difference.
    let missing_name_lower = missing.name.to_lowercase();
    if let Some(exact) = valid_columns
        .iter()
        .find(|candidate| candidate.name.to_lowercase() == missing_name_lower)
    {
        return Some(case_sensitive_hint(&exact.quoted()));
    }

    // Levenshtein over bare names for typos. Lowercase both sides: the
    // missing name arrives pre-lowercased from DataFusion's identifier
    // normalization, but candidates keep their schema-declared casing, so
    // a raw comparison against `playerID` would double-count case edits
    // and reject a genuine typo of `playrID` → `playerID`.
    let (best, _score) = valid_columns
        .iter()
        .filter_map(|candidate| {
            let score =
                strsim::normalized_levenshtein(&candidate.name.to_lowercase(), &missing_name_lower);
            (score >= DID_YOU_MEAN_SIMILARITY).then_some((candidate, score))
        })
        .max_by(|left, right| {
            left.1
                .partial_cmp(&right.1)
                .unwrap_or(std::cmp::Ordering::Equal)
        })?;
    Some(did_you_mean_hint(&best.quoted()))
}

fn table_not_found_hint(
    schema: Option<&str>,
    table: &str,
    known_tables: &[TableInfo],
) -> Option<String> {
    let Some(schema) = schema else {
        // Unqualified `FROM X` could not be resolved. Before falling back to
        // the generic catalog pointer, scan every known table across every
        // schema for a close match — if `FROM account` has `stripe.accounts`
        // in the catalog, suggest the schema-qualified name instead of
        // sending the user to `coral.tables`.
        if table.contains('.')
            && let Some(info) = quoted_qualified_table_match(table, known_tables)
        {
            return Some(quoted_qualified_table_hint(table, info));
        }

        let table_lower = table.to_lowercase();
        let best = known_tables
            .iter()
            .filter_map(|info| {
                let score =
                    strsim::normalized_levenshtein(&info.table_name.to_lowercase(), &table_lower);
                (score >= DID_YOU_MEAN_SIMILARITY).then_some((info, score))
            })
            .max_by(|left, right| {
                left.1
                    .partial_cmp(&right.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        if let Some((winner, _score)) = best {
            return Some(did_you_mean_hint(&format_schema_table(winner)));
        }
        return Some(
            "List available tables with `SELECT schema_name, table_name FROM coral.tables`."
                .to_string(),
        );
    };

    let schema_lower = schema.to_lowercase();
    let tables_in_schema: Vec<&TableInfo> = known_tables
        .iter()
        .filter(|info| info.schema_name.to_lowercase() == schema_lower)
        .collect();

    if tables_in_schema.is_empty() {
        // `known_tables` only contains successfully-registered sources, so an
        // empty schema here could mean either "not installed" or "configured
        // but failed to register". Keep the hint transport-neutral — point
        // at the SQL catalog so callers (CLI, MCP, gRPC) each render the
        // action in their native surface; any adapter that wants to
        // prescribe a specific command (e.g. `coral source list`) can
        // enrich the hint at their layer.
        return Some(format!(
            "Schema `{schema}` is not currently registered. \
             Query `SELECT DISTINCT schema_name FROM coral.tables` \
             to see available schemas."
        ));
    }

    let table_lower = table.to_lowercase();
    if let Some(hit) = tables_in_schema
        .iter()
        .find(|info| info.table_name.to_lowercase() == table_lower)
    {
        return Some(case_sensitive_hint(&format_schema_table(hit)));
    }

    let (best, _score) = tables_in_schema
        .iter()
        .filter_map(|info| {
            let score =
                strsim::normalized_levenshtein(&info.table_name.to_lowercase(), &table_lower);
            (score >= DID_YOU_MEAN_SIMILARITY).then_some((info, score))
        })
        .max_by(|left, right| {
            left.1
                .partial_cmp(&right.1)
                .unwrap_or(std::cmp::Ordering::Equal)
        })?;
    Some(did_you_mean_hint(&format_schema_table(best)))
}

fn quoted_qualified_table_match<'a>(
    table: &str,
    known_tables: &'a [TableInfo],
) -> Option<&'a TableInfo> {
    if let Some(exact) = known_tables
        .iter()
        .find(|info| raw_schema_table_name(info) == table)
    {
        return Some(exact);
    }

    let mut matches = known_tables
        .iter()
        .filter(|info| raw_schema_table_name(info).eq_ignore_ascii_case(table));
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}

fn quoted_qualified_table_hint(missing: &str, info: &TableInfo) -> String {
    let reference = format_schema_table(info);
    let fully_quoted_reference = format_schema_table_fully_quoted(info);
    let suggestions = if reference == fully_quoted_reference {
        format!("`{reference}`")
    } else {
        format!("`{reference}` or `{fully_quoted_reference}`")
    };

    format!(
        "`\"{missing}\"` is one quoted identifier, so SQL looks for a table literally named \
         `{missing}`. Use {suggestions} in `FROM`/`JOIN` clauses; do not quote the whole \
         `schema.table` string.",
    )
}

fn raw_schema_table_name(info: &TableInfo) -> String {
    format!("{}.{}", info.schema_name, info.table_name)
}

/// Renders `schema.table` with per-component SQL quoting (dotted source
/// names stay one quoted identifier; case-preserving names are quoted
/// only when they would otherwise round-trip wrong).
fn format_schema_table(info: &TableInfo) -> String {
    format!(
        "{}.{}",
        quote_dotted_identifier(&info.schema_name),
        quote_identifier(&info.table_name)
    )
}

fn format_schema_table_fully_quoted(info: &TableInfo) -> String {
    format!(
        "{}.{}",
        quote_identifier_always(&info.schema_name),
        quote_identifier_always(&info.table_name)
    )
}

// ---------------------------------------------------------------------------
// Table-ref parsing
// ---------------------------------------------------------------------------

/// Either a truly unqualified `FROM X` or a qualified `FROM schema.table`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ParsedTableRef {
    Unqualified { table: String },
    Qualified { schema: String, table: String },
}

/// Recovers the user's intent from a parsed missing table reference.
///
/// Bare `FROM games` surfaces as `datafusion.public.games` (the default
/// catalog plus the synthetic `public` schema), which we classify as
/// unqualified unless a real source named `public` exists in the catalog.
/// For dotted source names, we try increasingly long schema candidates
/// against the registered set so `datafusion."foo.bar".missing` is not
/// silently sliced into `bar` / `missing`.
fn parse_table_ref(reference: &TableRefParts, known_tables: &[TableInfo]) -> ParsedTableRef {
    let parts = reference.parts.as_slice();

    if parts.is_empty() {
        return ParsedTableRef::Unqualified {
            table: String::new(),
        };
    }
    if parts.len() == 1 {
        return ParsedTableRef::Unqualified {
            table: parts[0].clone(),
        };
    }

    // Strip the default catalog prefix if present.
    let body = if parts[0] == "datafusion" {
        &parts[1..]
    } else {
        parts
    };

    match body.len() {
        0 => ParsedTableRef::Unqualified {
            table: String::new(),
        },
        1 => ParsedTableRef::Unqualified {
            table: body[0].clone(),
        },
        _ => {
            // Synthetic `public` schema: `datafusion.public.X` comes from a
            // bare `FROM X`. Treat as unqualified unless a real source named
            // `public` exists (so a user who did register `public` as a
            // schema still gets catalog-aware hints). When the table name
            // itself contains a dot (manifest permits), the body is longer
            // than two — `datafusion.public.player.stats` for a table named
            // `player.stats` — and we collapse everything after `public`
            // back into the unqualified bare name.
            if body.len() >= 2
                && body[0] == "public"
                && !schema_is_registered("public", known_tables)
            {
                return ParsedTableRef::Unqualified {
                    table: join_ref_parts(&body[1..]),
                };
            }

            // Pick the longest contiguous prefix of `body` that matches a
            // registered schema (case-insensitive). This recovers dotted
            // source names like `"foo.bar"` from their exploded form.
            for schema_len in (1..body.len()).rev() {
                let candidate_schema = join_ref_parts(&body[..schema_len]);
                if schema_is_registered(&candidate_schema, known_tables) {
                    return ParsedTableRef::Qualified {
                        schema: candidate_schema,
                        table: join_ref_parts(&body[schema_len..]),
                    };
                }
            }

            // No known schema matched any prefix. Fall back to "everything
            // but the last dot is the schema" — keeps dotted source names
            // intact even when the source itself is missing from the
            // catalog (so the remediation hint still names the right
            // install target).
            let last = body.len() - 1;
            ParsedTableRef::Qualified {
                schema: join_ref_parts(&body[..last]),
                table: body[last].clone(),
            }
        }
    }
}

fn join_ref_parts(parts: &[String]) -> String {
    parts.join(".")
}

fn schema_is_registered(candidate: &str, known_tables: &[TableInfo]) -> bool {
    let lowered = candidate.to_lowercase();
    known_tables
        .iter()
        .any(|info| info.schema_name.to_lowercase() == lowered)
}

// ---------------------------------------------------------------------------
// Identifier helpers
// ---------------------------------------------------------------------------

/// Like `quote_identifier` but accepts identifiers that themselves contain
/// dots. A literal source name `foo.bar` quotes as `"foo.bar"`, not as
/// `foo.bar` (which would read as a 2-component path).
fn quote_dotted_identifier(ident: &str) -> String {
    if ident.contains('.') {
        let escaped = ident.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        quote_identifier(ident).into_owned()
    }
}

fn quote_identifier_always(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(schema: &str, name: &str) -> TableInfo {
        TableInfo {
            schema_name: schema.to_string(),
            table_name: name.to_string(),
            description: String::new(),
            columns: vec![],
            required_filters: vec![],
        }
    }

    fn cp(relation: &[&str], name: &str) -> ColumnParts {
        ColumnParts {
            relation: relation.iter().map(ToString::to_string).collect(),
            name: name.to_string(),
        }
    }

    fn tr(parts: &[&str]) -> TableRefParts {
        TableRefParts::new(parts.iter().map(ToString::to_string).collect())
    }

    #[test]
    fn reason_consts_match_wire_contract() {
        // Guard against an accidental rename: these literal values are the
        // wire contract. Changing them is a breaking change for any consumer
        // that pattern-matches on reason codes.
        assert_eq!(UNKNOWN_COLUMN_REASON, "UNKNOWN_COLUMN");
        assert_eq!(TABLE_NOT_FOUND_REASON, "TABLE_NOT_FOUND");
    }

    #[test]
    fn unknown_column_case_insensitive_match_quotes_preserved_name() {
        let valid = vec![cp(&["g"], "playerID"), cp(&["m"], "playerID")];
        let err = StructuredQueryError::unknown_column(&cp(&["g"], "playerid"), &valid);

        assert_eq!(err.reason(), UNKNOWN_COLUMN_REASON);
        assert_eq!(err.status(), StatusCode::InvalidArgument);
        let hint = err.hint().expect("hint should be present");
        assert!(
            hint.contains("g.\"playerID\""),
            "expected case-preserving quoted hint, got: {hint}"
        );
    }

    #[test]
    fn unknown_column_preserves_dotted_column_name_in_hint() {
        // Literal-dot column (`player.id`) referenced as an unquoted
        // 4-part path; hint must re-quote the dotted bare name.
        let valid = vec![cp(&["demo", "users"], "player.id")];
        let missing = cp(&["demo", "users", "player"], "id");
        let err = StructuredQueryError::unknown_column(&missing, &valid);

        let hint = err.hint().expect("hint should be present");
        assert!(
            hint.contains("demo.users.\"player.id\""),
            "hint must quote the literal-dot name, got: {hint}"
        );
        assert!(
            !hint.contains("demo.users.player.id"),
            "hint must not render the unquoted 4-part form, got: {hint}"
        );
    }

    #[test]
    fn unknown_column_matches_dotted_name_against_unqualified_reference() {
        // User's bare `player.id` (parsed as relation=[player], name=id)
        // matches the registered `hockey.master."player.id"` only after
        // we join the user's relation+name to compare against the
        // candidate's dotted bare name.
        let valid = vec![cp(&["hockey", "master"], "player.id")];
        let missing = cp(&["player"], "id");
        let err = StructuredQueryError::unknown_column(&missing, &valid);

        let hint = err.hint().expect("hint should be present");
        assert!(
            hint.contains("hockey.master.\"player.id\""),
            "hint must suggest the fully-qualified quoted form, got: {hint}"
        );
    }

    #[test]
    fn unknown_column_levenshtein_suggests_closest() {
        let valid = vec![cp(&[], "user_login"), cp(&[], "title")];
        let err = StructuredQueryError::unknown_column(&cp(&[], "user_llogin"), &valid);

        let hint = err.hint().expect("hint should be present");
        assert!(hint.contains("user_login"), "got: {hint}");
    }

    #[test]
    fn unknown_column_no_candidates_has_no_hint() {
        let err = StructuredQueryError::unknown_column(&cp(&[], "anything"), &[]);
        assert!(err.hint().is_none());
    }

    #[test]
    fn unknown_column_too_distant_omits_hint() {
        let valid = vec![cp(&[], "zzzzz")];
        let err = StructuredQueryError::unknown_column(&cp(&[], "playerID"), &valid);
        assert!(err.hint().is_none());
    }

    #[test]
    fn table_not_found_missing_schema_points_at_coral_tables_catalog() {
        let tables = vec![table("github", "issues")];
        let err = StructuredQueryError::table_not_found(
            &tr(&["datafusion", "hockey", "master"]),
            &tables,
        );

        assert_eq!(err.reason(), TABLE_NOT_FOUND_REASON);
        assert_eq!(err.status(), StatusCode::NotFound);
        let hint = err.hint().expect("hint should be present");
        assert!(hint.contains("coral.tables"), "got: {hint}");
        assert!(
            !hint.contains("coral source"),
            "hint must stay transport-neutral (no CLI-specific commands), got: {hint}"
        );
    }

    #[test]
    fn table_not_found_case_insensitive_match_quotes_preserved_name() {
        let tables = vec![table("hockey", "Master")];
        let err = StructuredQueryError::table_not_found(
            &tr(&["datafusion", "hockey", "master"]),
            &tables,
        );

        let hint = err.hint().expect("hint should be present");
        assert!(
            hint.contains("hockey.\"Master\""),
            "expected case-preserving quoted hint, got: {hint}"
        );
    }

    #[test]
    fn table_not_found_levenshtein_suggests_similar_table() {
        let tables = vec![
            table("hockey", "games"),
            table("hockey", "players"),
            table("hockey", "teams"),
        ];
        let err =
            StructuredQueryError::table_not_found(&tr(&["datafusion", "hockey", "game"]), &tables);

        let hint = err.hint().expect("hint should be present");
        assert!(hint.contains("hockey.games"), "got: {hint}");
    }

    #[test]
    fn table_not_found_strips_datafusion_catalog_prefix_from_display() {
        let tables = vec![table("hockey", "Master")];
        let err = StructuredQueryError::table_not_found(
            &tr(&["datafusion", "hockey", "master"]),
            &tables,
        );

        assert!(
            err.summary().contains("`hockey.master`"),
            "summary should strip catalog prefix, got: {}",
            err.summary()
        );
        assert_eq!(
            err.metadata().get("schema").map(String::as_str),
            Some("hockey")
        );
        assert_eq!(
            err.metadata().get("table").map(String::as_str),
            Some("master")
        );
    }

    #[test]
    fn table_not_found_synthetic_public_treated_as_unqualified() {
        // `FROM games` surfaces as `datafusion.public.games`; with no
        // user-registered `public` source, collapse to unqualified and
        // prefer a close cross-schema match over the catalog pointer.
        let tables = vec![table("hockey", "games")];
        let err =
            StructuredQueryError::table_not_found(&tr(&["datafusion", "public", "games"]), &tables);

        assert_eq!(err.metadata().get("schema"), None);
        assert_eq!(
            err.metadata().get("table").map(String::as_str),
            Some("games")
        );
        assert!(
            err.summary().contains("`games`"),
            "summary should show the bare table, got: {}",
            err.summary()
        );
        let hint = err.hint().expect("hint should be present");
        assert!(
            hint.contains("hockey.games"),
            "unqualified miss with a close match should suggest the schema-qualified form, got: {hint}"
        );
    }

    #[test]
    fn table_not_found_synthetic_public_preserves_dotted_table_name() {
        // `FROM "player.stats"` → `datafusion.public.player.stats` (4
        // parts); the public shortcut must keep everything after `public`
        // as the bare name, not split on the last dot.
        let tables = vec![table("hockey", "player.stats")];
        let err = StructuredQueryError::table_not_found(
            &tr(&["datafusion", "public", "player.stats"]),
            &tables,
        );

        assert_eq!(err.metadata().get("schema"), None);
        assert_eq!(
            err.metadata().get("table").map(String::as_str),
            Some("player.stats")
        );
        assert!(
            err.summary().contains("`player.stats`"),
            "summary should show the dotted bare table, got: {}",
            err.summary()
        );
        let hint = err.hint().expect("hint should be present");
        assert!(
            hint.contains("hockey.\"player.stats\""),
            "hint should quote dotted table name, got: {hint}"
        );
    }

    #[test]
    fn table_not_found_quoted_qualified_name_suggests_sql_reference() {
        // `FROM "github.pulls"` reaches the planner as a single bare
        // identifier under the synthetic `public` schema. When that flat
        // string exactly matches a visible `schema_name.table_name`, point
        // at the qualified SQL form before typo-based fallback can fire.
        let tables = vec![table("github", "pulls")];
        let err = StructuredQueryError::table_not_found(
            &tr(&["datafusion", "public", "github.pulls"]),
            &tables,
        );

        assert_eq!(err.metadata().get("schema"), None);
        assert_eq!(
            err.metadata().get("table").map(String::as_str),
            Some("github.pulls")
        );
        let hint = err.hint().expect("hint should be present");
        assert!(
            hint.contains("`\"github.pulls\"` is one quoted identifier"),
            "hint should explain whole-reference quoting, got: {hint}"
        );
        assert!(
            hint.contains("`github.pulls`"),
            "hint should suggest the SQL-safe unquoted qualified reference, got: {hint}"
        );
        assert!(
            hint.contains("`\"github\".\"pulls\"`"),
            "hint should show per-identifier quoting as the alternative, got: {hint}"
        );
        assert!(
            hint.contains("do not quote the whole `schema.table` string"),
            "hint should explicitly reject whole-reference quoting, got: {hint}"
        );
    }

    #[test]
    fn table_not_found_quoted_qualified_name_matches_unique_case_insensitive_reference() {
        let tables = vec![table("GitHub", "Pulls")];
        let err = StructuredQueryError::table_not_found(
            &tr(&["datafusion", "public", "github.pulls"]),
            &tables,
        );

        assert_eq!(err.metadata().get("schema"), None);
        assert_eq!(
            err.metadata().get("table").map(String::as_str),
            Some("github.pulls")
        );
        let hint = err.hint().expect("hint should be present");
        assert!(
            hint.contains("`\"github.pulls\"` is one quoted identifier"),
            "hint should explain whole-reference quoting, got: {hint}"
        );
        assert!(
            hint.contains("`\"GitHub\".\"Pulls\"`"),
            "hint should suggest the case-preserved table reference, got: {hint}"
        );
        assert!(
            !hint.contains("or `\"GitHub\".\"Pulls\"`"),
            "hint should not duplicate equivalent suggestions, got: {hint}"
        );
    }

    #[test]
    fn table_not_found_quoted_qualified_name_ignores_ambiguous_case_insensitive_reference() {
        let tables = vec![table("GitHub", "Pulls"), table("github", "PULLS")];
        let err = StructuredQueryError::table_not_found(
            &tr(&["datafusion", "public", "github.pulls"]),
            &tables,
        );

        let hint = err.hint().expect("hint should be present");
        assert!(
            !hint.contains("`\"github.pulls\"` is one quoted identifier"),
            "ambiguous case-insensitive matches should not pick an arbitrary table, got: {hint}"
        );
    }

    #[test]
    fn table_not_found_real_public_schema_beats_synthetic_shortcut() {
        // If a user genuinely registered a source named `public`, don't
        // collapse the 2-part body — resolve against the catalog.
        let tables = vec![table("public", "Reports")];
        let err = StructuredQueryError::table_not_found(
            &tr(&["datafusion", "public", "reports"]),
            &tables,
        );

        assert_eq!(
            err.metadata().get("schema").map(String::as_str),
            Some("public")
        );
        let hint = err.hint().expect("hint should be present");
        assert!(hint.contains("public.\"Reports\""), "got: {hint}");
    }

    #[test]
    fn table_not_found_preserves_dotted_source_name() {
        // Source name `foo.bar` explodes to `datafusion.foo.bar.items`;
        // parser must recover schema=`foo.bar`, table=`items`.
        let tables = vec![table("foo.bar", "Items")];
        let err = StructuredQueryError::table_not_found(
            &tr(&["datafusion", "foo.bar", "items"]),
            &tables,
        );

        assert_eq!(
            err.metadata().get("schema").map(String::as_str),
            Some("foo.bar")
        );
        assert_eq!(
            err.metadata().get("table").map(String::as_str),
            Some("items")
        );
        let hint = err.hint().expect("hint should be present");
        assert!(
            hint.contains("\"foo.bar\".\"Items\""),
            "hint should quote dotted schema as one identifier, got: {hint}"
        );
    }

    #[test]
    fn table_not_found_dotted_source_without_matching_table_still_routes_schema() {
        // Dotted source + wrong table: schema/metadata must still name
        // the real source even when the hint path finds no close match.
        let tables = vec![table("foo.bar", "Items")];
        let err = StructuredQueryError::table_not_found(
            &tr(&["datafusion", "foo.bar", "missing"]),
            &tables,
        );

        assert_eq!(
            err.metadata().get("schema").map(String::as_str),
            Some("foo.bar")
        );
        assert_eq!(
            err.metadata().get("table").map(String::as_str),
            Some("missing")
        );
    }

    #[test]
    fn table_not_found_unqualified_falls_back_to_catalog_pointer() {
        // Defensive guard for bare unqualified refs (DataFusion normally
        // emits 3-part paths); catalog entry is distant so cross-schema
        // Levenshtein doesn't fire and the generic pointer surfaces.
        let tables = vec![table("hockey", "zzzzzzzzz")];
        let err = StructuredQueryError::table_not_found(&tr(&["games"]), &tables);

        let hint = err.hint().expect("hint should be present");
        assert!(hint.contains("coral.tables"), "got: {hint}");
    }

    #[test]
    fn table_not_found_unqualified_cross_schema_levenshtein_suggests_match() {
        // Bare `FROM account` against `stripe.accounts`: hint prefers
        // the schema-qualified match over the catalog pointer; metadata
        // stays unqualified so it reflects what the user wrote.
        let tables = vec![table("stripe", "accounts"), table("github", "issues")];
        let err = StructuredQueryError::table_not_found(
            &tr(&["datafusion", "public", "account"]),
            &tables,
        );

        let hint = err.hint().expect("hint should be present");
        assert!(
            hint.contains("stripe.accounts"),
            "unqualified miss should suggest the closest schema-qualified name, got: {hint}"
        );
        assert_eq!(err.metadata().get("schema"), None);
        assert_eq!(
            err.metadata().get("table").map(String::as_str),
            Some("account")
        );
    }

    #[test]
    fn table_not_found_unqualified_no_close_match_falls_back_to_catalog_pointer() {
        // Unqualified miss with no close catalog entry: fall back to
        // the generic catalog pointer.
        let tables = vec![table("stripe", "subscriptions")];
        let err = StructuredQueryError::table_not_found(
            &tr(&["datafusion", "public", "account"]),
            &tables,
        );

        let hint = err.hint().expect("hint should be present");
        assert!(
            hint.contains("coral.tables"),
            "no-match unqualified case should fall back to catalog pointer, got: {hint}"
        );
    }

    #[test]
    fn quote_dotted_identifier_wraps_names_with_embedded_dots() {
        assert_eq!(quote_dotted_identifier("foo.bar"), "\"foo.bar\"");
        assert_eq!(quote_dotted_identifier("hockey"), "hockey");
        assert_eq!(quote_dotted_identifier("Master"), "\"Master\"");
    }

    #[test]
    fn quote_identifier_always_escapes_embedded_quotes() {
        assert_eq!(quote_identifier_always("github"), "\"github\"");
        assert_eq!(quote_identifier_always("git\"hub"), "\"git\"\"hub\"");
    }

    #[test]
    fn column_parts_quoted_escapes_each_component() {
        assert_eq!(cp(&["g"], "playerID").quoted(), "g.\"playerID\"");
        assert_eq!(cp(&[], "playerID").quoted(), "\"playerID\"");
        assert_eq!(
            cp(&["hockey", "master"], "playerID").quoted(),
            "hockey.master.\"playerID\""
        );
        // Literal dot inside the bare column name stays inside one set of
        // quotes — without this, the hint reads as a 4-component reference.
        assert_eq!(
            cp(&["demo", "users"], "player.id").quoted(),
            "demo.users.\"player.id\""
        );
    }
}
