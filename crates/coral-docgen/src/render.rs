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
        let _ = writeln!(
            out,
            "| [`{name}`](#{name}) | `{}` | {description} |",
            backend_label(manifest),
        );
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
    let _ = writeln!(out, "\n### `{name}`");
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

    let _ = write!(out, "`{}` ({requirement})", input.key);
    if input.default_value.is_empty() {
        out.push('\n');
    } else {
        // `<br />` gives a soft line break so the default sits visually
        // right under the key without starting a new paragraph. Trailing-
        // whitespace line breaks are fragile because editors strip them.
        let _ = writeln!(out, "<br />");
        let _ = writeln!(out, "default `{}`", input.default_value);
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

/// Escape MDX-hostile characters (`{`, `}`, `<`, `>`) in plain prose without
/// disturbing markdown code spans or fenced code blocks.
///
/// Content inside backtick code spans and ```...``` fences is emitted
/// verbatim because MDX does not interpret JSX inside code.
pub(crate) fn escape_mdx(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    let mut in_fence = false;
    // Track whether we're at the start of a line for fence detection.
    let mut at_line_start = true;

    while let Some(ch) = chars.next() {
        // Detect a ``` fence opening/closing at the start of a line.
        if at_line_start && ch == '`' {
            let mut backticks = 1;
            while matches!(chars.peek(), Some('`')) {
                chars.next();
                backticks += 1;
            }
            for _ in 0..backticks {
                out.push('`');
            }
            if backticks >= 3 {
                in_fence = !in_fence;
            }
            at_line_start = false;
            continue;
        }

        if in_fence {
            out.push(ch);
            at_line_start = ch == '\n';
            continue;
        }

        match ch {
            '`' => {
                // Inline code span: copy verbatim until the matching backtick
                // or end of line.
                out.push('`');
                let mut closed_by_newline = false;
                for next in chars.by_ref() {
                    out.push(next);
                    if next == '`' {
                        break;
                    }
                    if next == '\n' {
                        closed_by_newline = true;
                        break;
                    }
                }
                at_line_start = closed_by_newline;
                continue;
            }
            '{' => out.push_str("\\{"),
            '}' => out.push_str("\\}"),
            '<' => out.push_str("\\<"),
            '>' => out.push_str("\\>"),
            '\n' => {
                out.push('\n');
                at_line_start = true;
                continue;
            }
            other => out.push(other),
        }
        at_line_start = false;
    }
    out
}

const INDEX_FRONTMATTER: &str =
    "---\ntitle: \"Bundled sources\"\ndescription: \"Data sources that ship with Coral.\"\n---\n\n";

const INDEX_INTRO: &str = concat!(
    "Coral supports connecting to some data sources out of the box.<br />\n",
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
    fn escape_mdx_preserves_fenced_code_blocks() {
        let input = "Intro.\n\n```yaml\nkey: <placeholder>\nother: {var}\n```\n\nAfter <host>.";
        assert_eq!(
            escape_mdx(input),
            "Intro.\n\n```yaml\nkey: <placeholder>\nother: {var}\n```\n\nAfter \\<host\\>.",
        );
    }

    #[test]
    fn index_page_renders_table_and_accordions() {
        let demo = parse_source_manifest_yaml(SAMPLE_MANIFEST).expect("parse demo");
        let minimal = parse_source_manifest_yaml(NO_INPUTS_MANIFEST).expect("parse minimal");
        insta::assert_snapshot!("index_page_renders_rows", index_page(&[demo, minimal]));
    }
}
