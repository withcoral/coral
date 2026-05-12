//! MDX rendering for the bundled-sources index.
//!
//! Produces the exact byte-for-byte contents written to
//! `docs/reference/bundled-sources.mdx`. The single page contains the
//! at-a-glance source table followed by one deep-linkable sub-section per
//! source surfacing its declared inputs and hints.

use std::fmt::Write as _;

use coral_spec::{ManifestInputSpec, ValidatedSourceManifest};

/// Render the `bundled-sources.mdx` index page.
pub(crate) fn index_page(manifests: &[ValidatedSourceManifest]) -> String {
    let mut out = String::new();
    out.push_str(INDEX_FRONTMATTER);
    out.push_str("{/* AUTO-GENERATED — DO NOT EDIT. Run `make docs-generate` to update. */}\n\n");
    out.push_str(INDEX_INTRO);

    // At-a-glance table.
    out.push_str("\n## Bundled data sources\n\n");
    out.push_str("| Source | Backend | Description |\n");
    out.push_str("| --- | --- | --- |\n");
    for manifest in manifests {
        let name = manifest.schema_name();
        let description = manifest.description();
        let description = if description.is_empty() {
            format!("Coral bundled source: {name}")
        } else {
            // Table rows can't contain raw newlines (they terminate the row)
            // or literal `|` (the cell delimiter). Collapse both so multi-line
            // block-scalar descriptions render cleanly in one cell.
            escape_mdx(&flatten_for_table_cell(description))
        };
        writeln!(
            out,
            "| [{name}](#{name}) | `{}` | {description} |",
            backend_label(manifest),
        )
        .expect("writing to String is infallible");
    }

    out.push_str(INDEX_TYPES);
    out.push_str(INDEX_UPGRADING);

    // Per-source setup sub-sections. Each source gets an h3 so Mintlify
    // auto-generates an anchor, allowing deep links like
    // `/reference/bundled-sources#slack`.
    if !manifests.is_empty() {
        out.push_str("\n## Configure a source\n\n");
        out.push_str(
            "Each source has its own set of interactive inputs — API tokens, base URLs, or\n\
             other per-install configuration.\n",
        );
        for manifest in manifests {
            render_source_section(&mut out, manifest);
        }
    }

    out.push_str(INDEX_OUTRO);
    out
}

fn render_source_section(out: &mut String, manifest: &ValidatedSourceManifest) {
    let name = manifest.schema_name();
    writeln!(out, "\n### `{name}`").expect("writing to String is infallible");
    out.push('\n');

    let inputs = manifest.declared_inputs();
    if inputs.is_empty() {
        out.push_str("No configuration required.\n");
        return;
    }

    for (idx, input) in inputs.iter().enumerate() {
        if idx > 0 {
            out.push('\n');
        }
        render_input_block(out, input);
    }
}

fn render_input_block(out: &mut String, input: &ManifestInputSpec) {
    let requirement = if input.required {
        "required"
    } else {
        "optional"
    };

    write!(out, "`{}` ({requirement})", input.key).expect("writing to String is infallible");
    if input.default_value.is_empty() {
        out.push('\n');
    } else {
        // `<br />` gives a soft line break so the default sits visually
        // right under the key without starting a new paragraph. Trailing-
        // whitespace line breaks are fragile because editors strip them.
        writeln!(out, "<br />").expect("writing to String is infallible");
        writeln!(out, "default `{}`", input.default_value)
            .expect("writing to String is infallible");
    }

    if let Some(hint) = input.hint.as_deref() {
        let hint = hint.trim();
        if !hint.is_empty() {
            out.push('\n');
            out.push_str(&escape_mdx(hint));
            out.push('\n');
        }
    }
}

fn backend_label(manifest: &ValidatedSourceManifest) -> &'static str {
    if manifest.as_http().is_some() {
        "http"
    } else if manifest.as_parquet().is_some() {
        "parquet"
    } else if manifest.as_jsonl().is_some() {
        "jsonl"
    } else {
        // ValidatedSourceManifest covers all three backends; unreachable in
        // practice but we avoid `unreachable!` to keep the generator robust.
        "unknown"
    }
}

/// Collapse internal whitespace for safe rendering inside a markdown table
/// cell: newlines become spaces and literal `|` is escaped.
fn flatten_for_table_cell(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut prev_space = false;
    for ch in input.trim().chars() {
        match ch {
            '\n' | '\r' | '\t' | ' ' => {
                if !prev_space {
                    out.push(' ');
                    prev_space = true;
                }
            }
            '|' => {
                out.push_str("\\|");
                prev_space = false;
            }
            other => {
                out.push(other);
                prev_space = false;
            }
        }
    }
    out
}

/// Escape MDX-hostile characters (`{`, `}`, `<`, `>`) in authored prose
/// while preserving markdown constructs whose contents the author wrote
/// as literal text.
///
/// Hints and descriptions are freeform markdown. We use `pulldown-cmark`
/// to identify byte ranges that should pass through unchanged:
///
/// - inline code spans (`` `foo` ``),
/// - fenced and indented code blocks,
/// - autolinks (`<https://…>` and `<mailto:…>`),
/// - inline link destinations (the `(…)` portion of `[text](url)`).
///
/// Everything else — ordinary prose, link text, emphasis, raw HTML —
/// goes through character-level `{`/`}`/`<`/`>` escaping. Raw HTML is
/// intentionally escaped: authors of Coral hints write `<placeholder>`
/// as literal text, not as a JSX tag, so `\<placeholder\>` is what we
/// want Mintlify to render.
pub(crate) fn escape_mdx(input: &str) -> String {
    use pulldown_cmark::{Event, LinkType, Options, Parser, Tag, TagEnd};

    let mut out = String::with_capacity(input.len());
    // `cursor` marks the byte offset in `input` up to which we've already
    // emitted output. Anything from `cursor` to the next passthrough
    // range's start must still be escaped as prose.
    let mut cursor: usize = 0;
    let mut code_block_start: Option<usize> = None;
    let mut inline_link_start: Option<usize> = None;

    for (event, range) in Parser::new_ext(input, Options::empty()).into_offset_iter() {
        match event {
            // Inline code spans and autolinks are one-event passthroughs:
            // emit the whole event range verbatim.
            Event::Code(_)
            | Event::Start(Tag::Link {
                link_type: LinkType::Autolink | LinkType::Email,
                ..
            }) => {
                emit_passthrough(input, &mut cursor, range, &mut out);
            }
            Event::Start(Tag::CodeBlock(_)) => code_block_start = Some(range.start),
            Event::End(TagEnd::CodeBlock) => {
                if let Some(start) = code_block_start.take() {
                    emit_passthrough(input, &mut cursor, start..range.end, &mut out);
                }
            }
            Event::Start(Tag::Link { .. }) => inline_link_start = Some(range.start),
            Event::End(TagEnd::Link) => {
                if let Some(start) = inline_link_start.take() {
                    // Inline link: pass through just the destination, the
                    // `](…)` segment. The text before `](` (which may
                    // contain placeholders like `<host>`) still gets the
                    // prose treatment.
                    if let Some(dest) = inline_link_dest(input, start..range.end) {
                        emit_passthrough(input, &mut cursor, dest, &mut out);
                    }
                }
            }
            _ => {}
        }
    }
    escape_prose_into(
        input
            .get(cursor..)
            .expect("pulldown-cmark cursor is a valid UTF-8 boundary"),
        &mut out,
    );
    out
}

/// Emit `input[cursor..range.start]` as escaped prose, then
/// `input[range]` verbatim, and advance `cursor` past the range.
fn emit_passthrough(
    input: &str,
    cursor: &mut usize,
    range: std::ops::Range<usize>,
    out: &mut String,
) {
    if *cursor < range.start {
        escape_prose_into(
            input
                .get(*cursor..range.start)
                .expect("pulldown-cmark range starts on a valid UTF-8 boundary"),
            out,
        );
    }
    out.push_str(
        input
            .get(range.start..range.end)
            .expect("pulldown-cmark range is a valid UTF-8 span"),
    );
    *cursor = range.end;
}

/// Find the byte range of the destination in an inline link — the `](…)`
/// tail, inclusive of the brackets. Returns `None` for reference-style
/// links (`[text][ref]` etc.) where no `](` appears in the link span.
fn inline_link_dest(input: &str, link: std::ops::Range<usize>) -> Option<std::ops::Range<usize>> {
    let slice = input.get(link.clone())?;
    let bracket_paren = slice.find("](")?;
    Some((link.start + bracket_paren)..link.end)
}

fn escape_prose_into(slice: &str, out: &mut String) {
    for ch in slice.chars() {
        match ch {
            '{' => out.push_str("\\{"),
            '}' => out.push_str("\\}"),
            '<' => out.push_str("\\<"),
            '>' => out.push_str("\\>"),
            other => out.push(other),
        }
    }
}

const INDEX_FRONTMATTER: &str =
    "---\ntitle: \"Bundled sources\"\ndescription: \"Data sources that ship with Coral.\"\n---\n\n";

const INDEX_INTRO: &str = concat!(
    "Coral supports connecting to some data sources out of the box. These bundled specs live in [sources/core](https://github.com/withcoral/coral/tree/main/sources/core).<br />\n",
    "If the source you need is not available, you can extend Coral by [writing a custom source spec](/guides/write-a-custom-source).\n",
    "\n",
    "<Tip>\n",
    "  Run `coral source discover` to see the bundled sources available in your\n",
    "  build.\n",
    "</Tip>\n",
);

const INDEX_TYPES: &str = concat!(
    "\n## Supported data source types\n\n",
    "Supported sources fall into two categories.\n\n",
    "- **HTTP API** — Coral translates SQL queries into paginated HTTP requests against a provider's REST API.\n",
    "- **File-backed** — Coral reads local Parquet or JSONL files directly.\n",
);

const INDEX_UPGRADING: &str = concat!(
    "\n## Upgrading bundled sources\n\n",
    "To update bundled sources, upgrade the Coral binary. Coral resolves each bundled manifest ",
    "from the current binary at validate or query time, so spec fixes and newly required inputs ",
    "are picked up automatically, you don't need to remove and re-add the source. Your configured ",
    "variables and secrets stay in local state across upgrades.\n",
);

const INDEX_OUTRO: &str = concat!(
    "\n## Don't see what you need?\n\n",
    "The bundled set is growing. If your data source is not listed, ",
    "[write a custom source](/guides/write-a-custom-source), or reach out to us via ",
    "[Discord](https://discord.gg/h9aun8KpFF) or [GitHub](https://github.com/withcoral/coral/issues).\n",
);

#[cfg(test)]
mod tests {
    use super::{escape_mdx, index_page};
    use coral_spec::parse_source_manifest_yaml;

    const SAMPLE_MANIFEST: &str = r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
description: A small demo source used in snapshot tests
inputs:
  DEMO_API_BASE:
    kind: variable
    default: https://api.example.com
    hint: |
      For self-hosted deploys, use https://<host>/api/v3.
      Use the `admin` account's `token` value.
  DEMO_TOKEN:
    kind: secret
    hint: Create an API token in Settings → Tokens
base_url: "{{input.DEMO_API_BASE}}"
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: template
      template: Bearer {{input.DEMO_TOKEN}}
tables:
  - name: widgets
    description: All the widgets
    request:
      method: GET
      path: /widgets
    response:
      rows_path:
        - widgets
    columns:
      - name: id
        type: Utf8
        nullable: false
        description: Widget identifier
        expr:
          kind: path
          path: [id]
"#;

    const NO_INPUTS_MANIFEST: &str = r"
name: minimal
version: 0.1.0
dsl_version: 3
backend: http
base_url: https://api.example.com
tables:
  - name: pings
    description: Ping events
    request:
      method: GET
      path: /ping
    response:
      rows_path: []
    columns:
      - name: id
        type: Utf8
        nullable: false
        description: Ping id
        expr:
          kind: path
          path: [id]
";

    #[test]
    fn escape_mdx_escapes_angle_and_brace_in_prose() {
        let input = "See https://<host>/api/v3 and the {workspace} placeholder.";
        let escaped = escape_mdx(input);
        assert_eq!(
            escaped,
            "See https://\\<host\\>/api/v3 and the \\{workspace\\} placeholder."
        );
    }

    #[test]
    fn escape_mdx_preserves_inline_code_and_links() {
        let input = "Use `{{input.X}}` to reference input [X](https://x.example).";
        assert_eq!(
            escape_mdx(input),
            "Use `{{input.X}}` to reference input [X](https://x.example)."
        );
    }

    #[test]
    fn escape_mdx_preserves_fenced_code_blocks_verbatim() {
        let input = "See the config:\n\n\
                     ```yaml\n\
                     token: <secret>\n\
                     key: {{input.KEY}}\n\
                     ```\n\n\
                     After <host>.";
        let expected = "See the config:\n\n\
                        ```yaml\n\
                        token: <secret>\n\
                        key: {{input.KEY}}\n\
                        ```\n\n\
                        After \\<host\\>.";
        assert_eq!(escape_mdx(input), expected);
    }

    #[test]
    fn escape_mdx_accepts_indented_fence_markers() {
        // CommonMark allows up to 3 leading spaces before a fence marker.
        let input = "Prose.\n\n   ```\n<raw>\n   ```\n\nAfter <host>.";
        let expected = "Prose.\n\n   ```\n<raw>\n   ```\n\nAfter \\<host\\>.";
        assert_eq!(escape_mdx(input), expected);
    }

    #[test]
    fn escape_mdx_preserves_autolinks_verbatim() {
        let input = "See <https://docs.example.com> and <https://other.example>.";
        assert_eq!(
            escape_mdx(input),
            "See <https://docs.example.com> and <https://other.example>."
        );
    }

    #[test]
    fn escape_mdx_preserves_inline_link_destinations_verbatim() {
        let input = "Use [doc](https://example.com/<token>?q={x}) for setup.";
        assert_eq!(
            escape_mdx(input),
            "Use [doc](https://example.com/<token>?q={x}) for setup."
        );
    }

    #[test]
    fn escape_mdx_escapes_link_text_while_preserving_destination() {
        // Link text still goes through prose escaping (so placeholder-style
        // `<host>` in link text gets escaped); the destination in the
        // parentheses is emitted verbatim.
        let input = "See [the <host> guide](https://example.com/<token>).";
        assert_eq!(
            escape_mdx(input),
            "See [the \\<host\\> guide](https://example.com/<token>)."
        );
    }

    #[test]
    fn escape_mdx_preserves_indented_code_blocks() {
        // Four-space indent is an indented code block in CommonMark.
        let input = "Example:\n\n    token: <secret>\n    key: {var}\n\nAfter <host>.";
        let expected = "Example:\n\n    token: <secret>\n    key: {var}\n\nAfter \\<host\\>.";
        assert_eq!(escape_mdx(input), expected);
    }

    #[test]
    fn index_page_renders_table_and_accordions() {
        let demo = parse_source_manifest_yaml(SAMPLE_MANIFEST).expect("parse demo");
        let minimal = parse_source_manifest_yaml(NO_INPUTS_MANIFEST).expect("parse minimal");
        insta::assert_snapshot!("index_page_renders_rows", index_page(&[demo, minimal]));
    }
}
