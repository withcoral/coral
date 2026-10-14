use super::{Callable, SIGNATURE_LINE_MAX_BYTES, signature_line};

const HTTP_RESULT_ALIASES: &str = "type HttpResult<Data, Status, MediaType> = { data: Data; metadata: { readonly kind: \"http\"; readonly status: Status; readonly mediaType: MediaType; readonly decodeDiagnostics: readonly [] } };\n\
     type HttpUnknownResult = { data: unknown; metadata: { readonly kind: \"http\"; readonly status: number; readonly decodeDiagnostics: readonly [{ readonly code: string }] } };\n";

/// The aliases an HTTP Operation's result needs, and the result.
fn http_result(branches: &[(&str, &str)]) -> (&'static str, String) {
    let mut envelopes = branches
        .iter()
        .map(|(data, status)| format!("HttpResult<{data}, {status}, \"application/json\">"))
        .collect::<Vec<_>>();
    envelopes.push("HttpUnknownResult".to_string());
    (
        HTTP_RESULT_ALIASES,
        format!("Promise<{}>", envelopes.join(" | ")),
    )
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one table holds every declaration shape the parser reads"
)]
fn signature_lines_inline_the_input_one_level_and_name_the_rest() {
    let list_issues = {
        let (aliases, result) = http_result(&[("Array<Issue>", "200"), ("Error", "404")]);
        format!(
            "{aliases}type GetParameter0Input = string;\n\
             type GetParameter2Input = \"open\" | \"closed\" | \"all\";\n\
             type GetParameter3Input = number;\n\
             type Issue = ({{ \"id\"?: number; \"labels\"?: Array<Label>; \"title\"?: string }} & Record<string, unknown>);\n\
             type Label = ({{ \"color\"?: string; \"name\": string }} & Record<string, unknown>);\n\
             namespace issues {{ function list(input: {{ \"owner\": GetParameter0Input; \"per_page\"?: GetParameter3Input; \"state\"?: GetParameter2Input }}): {result}; }}"
        )
    };
    let create_issue = {
        let (aliases, result) = http_result(&[
            ("Issue", "201"),
            ("Issue | IssueDraft", "200 | 202"),
            ("Problem", "number"),
        ]);
        format!(
            "{aliases}type BodyInput = ({{ \"labels\"?: Array<LabelInput>; \"meta\"?: ({{ \"x\"?: string }} & Record<string, unknown>); \"title\": string }} & Record<string, unknown>);\n\
             type Parameter0Input = string;\n\
             function createIssue(input: {{ \"body\": BodyInput; \"owner\": Parameter0Input }}): {result};"
        )
    };
    let nested_mcp = "type McpSafeMeta = Record<string, unknown>;\n\
         function forecast(input: ({ \"city\": string; \"options\"?: ({ \"units\"?: \"c\" | \"f\" } & Record<string, unknown>); \"days\"?: Array<number> } & Record<string, unknown>)): Promise<{ data: { content: Array<McpContentBlock>; structuredContent?: unknown; isError?: boolean }; metadata: { kind: \"mcp\"; _meta?: McpSafeMeta } }>;";
    let status_ranges = {
        let (aliases, result) = http_result(&[
            ("Array<Name>", "HttpStatus2xx"),
            ("Problem", "HttpStatus4xx"),
            ("Outage", "HttpStatus5xx"),
        ]);
        format!(
            "{aliases}type HttpStatus2xx = number & {{ readonly __statusRange: \"2xx\" }};\n\
             type HttpStatus4xx = number & {{ readonly __statusRange: \"4xx\" }};\n\
             type HttpStatus5xx = number & {{ readonly __statusRange: \"5xx\" }};\n\
             function names(): {result};"
        )
    };
    let argumentless = {
        let (aliases, result) = http_result(&[("string", "200")]);
        format!("{aliases}function ping(): {result};")
    };
    let fallback = "function raw(input: unknown): Promise<{ data: unknown; metadata: { readonly kind: \"http\" } }>;";
    let named_input = "interface Input {\n    value: number;\n    nested: {\n        a: string;\n    };\n    tags?: string[];\n}\nexport default function run(input: Input): number;\nexport {};\n";
    let anonymous =
        "export default function (input: {\n    q: string;\n}): {\n    n: number;\n};\n";
    let arrow = "declare const g: (input: {\n    q: string;\n}) => number;\nexport default g;\n";
    let alias_union = "type Mode = 'a' | 'b';\ntype Modes = Mode[];\ninterface Out { x: number }\nexport default function f(input: { mode: Mode; modes?: Modes; limit?: number }): Promise<Out[]>;";
    let recursive = "type Tree = Tree[] | string;\nexport default function walk(input: { tree: Tree }): number;";
    let unreadable = "export default something weird";
    let keyed_record = "export default function keyed(input: { label?: string } & Record<\"id\", unknown>): number;";
    let beyond_members = "// emitted by the compiler\nexport default function beyond(input: { keys: { [K in \"a\" | \"b\"]: number }; pick: Pick<Input, \"a\">; kind: string extends infer T ? T : never; label: `v${number}` }): number;";
    let shadowed_global = "type Array = Array[] | string;\nexport default function shadow(input: { list: Array }): number;";
    let escaped_literal = "function quoted(): Promise<{ data: \"x\\\"|y\" | \"z\"; metadata: { readonly status: 200 } }>;";
    let markers = "export default function markers(input: { pair: [number, number?]; cb?: ((value?: number) => number) | null; sum: (...values: number[]) => number }): number;";

    for (declaration, callable, expected) in [
        (
            list_issues.as_str(),
            Callable::Operation { leaf: "list" },
            r#"list(input: { owner: string; per_page?: number; state?: "open" | "closed" | "all" }): Promise<{ data: Array<Issue> }>"#,
        ),
        (
            create_issue.as_str(),
            Callable::Operation {
                leaf: "createIssue",
            },
            "createIssue(input: { body: BodyInput; owner: string }): Promise<{ data: Issue | IssueDraft }>",
        ),
        (
            nested_mcp,
            Callable::Operation { leaf: "forecast" },
            "forecast(input: { city: string; options?: {…}; days?: Array<number> }): Promise<{ data: object }>",
        ),
        (
            status_ranges.as_str(),
            Callable::Operation { leaf: "names" },
            "names(): Promise<{ data: Array<Name> }>",
        ),
        (
            argumentless.as_str(),
            Callable::Operation { leaf: "ping" },
            "ping(): Promise<{ data: string }>",
        ),
        (
            fallback,
            Callable::Operation { leaf: "raw" },
            "raw(input: unknown): Promise<{ data: unknown }>",
        ),
        (
            named_input,
            Callable::SavedFunction { leaf: "run" },
            "run(input: { value: number; nested: {…}; tags?: string[] }): number",
        ),
        (
            anonymous,
            Callable::SavedFunction { leaf: "count" },
            "count(input: { q: string }): object",
        ),
        (
            arrow,
            Callable::SavedFunction { leaf: "measure" },
            "measure(input: { q: string }): number",
        ),
        (
            alias_union,
            Callable::SavedFunction { leaf: "f" },
            "f(input: { mode: \"a\" | \"b\"; modes?: (\"a\" | \"b\")[]; limit?: number }): Promise<Out[]>",
        ),
        (
            recursive,
            Callable::SavedFunction { leaf: "walk" },
            "walk(input: { tree: Tree }): number",
        ),
        (
            unreadable,
            Callable::SavedFunction { leaf: "strange" },
            "strange(…)",
        ),
        (
            keyed_record,
            Callable::SavedFunction { leaf: "keyed" },
            "keyed(input: { label?: string } & Record<\"id\", unknown>): number",
        ),
        (
            beyond_members,
            Callable::SavedFunction { leaf: "beyond" },
            "beyond(input: { keys: …; pick: Pick<Input, \"a\">; kind: …; label: … }): number",
        ),
        (
            shadowed_global,
            Callable::SavedFunction { leaf: "shadow" },
            "shadow(input: { list: Array }): number",
        ),
        (
            escaped_literal,
            Callable::Operation { leaf: "quoted" },
            r#"quoted(): Promise<{ data: "x\"|y" | "z" }>"#,
        ),
        (
            markers,
            Callable::SavedFunction { leaf: "markers" },
            "markers(input: { pair: [number, number?]; cb?: ((value?: number) => number) | null; sum: (...values: number[]) => number }): number",
        ),
    ] {
        assert_eq!(
            signature_line(declaration, callable),
            expected,
            "{declaration}"
        );
    }
}

#[test]
fn a_long_signature_line_is_cut_at_a_word_boundary_with_a_marker() {
    let members = (0..80)
        .map(|index| format!("\"field{index}\": string"))
        .collect::<Vec<_>>()
        .join("; ");
    let declaration =
        format!("function wide(input: {{ {members} }}): Promise<{{ data: unknown }}>;");

    let line = signature_line(&declaration, Callable::Operation { leaf: "wide" });

    assert!(line.len() <= SIGNATURE_LINE_MAX_BYTES, "{}", line.len());
    assert!(line.ends_with('…'), "{line}");
    let kept = line.trim_end_matches('…');
    assert!(
        kept.ends_with(';') || kept.ends_with(':'),
        "cut inside a word: {line}"
    );
    assert!(line.starts_with("wide(input: { field0: string; "));
}
