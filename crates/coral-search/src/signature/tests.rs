use super::{Callable, SIGNATURE_LINE_MAX_BYTES, SignatureLine, signature_line};

const HTTP_RESULT_ALIASES: &str = "type HttpResult<Data, Status, MediaType> = { status: Status; data: Data; metadata: { readonly kind: \"http\"; readonly mediaType: MediaType; readonly decodeDiagnostics: readonly { readonly code: string }[] } };\n";

/// The aliases an HTTP Tool's result needs, and the result: one
/// envelope per declared success.
fn http_result(branches: &[(&str, &str)]) -> (&'static str, String) {
    let envelopes = branches
        .iter()
        .map(|(data, status)| format!("HttpResult<{data}, {status}, \"application/json\">"))
        .collect::<Vec<_>>();
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
        let (aliases, result) = http_result(&[("Array<Issue>", "200")]);
        format!(
            "{aliases}type GetParameter0Input = string;\n\
             type GetParameter2Input = \"open\" | \"closed\" | \"all\";\n\
             type GetParameter3Input = number;\n\
             type Issue = {{ \"id\"?: number; \"labels\"?: Array<Label>; \"title\"?: string }};\n\
             type Label = {{ \"color\"?: string; \"name\": string }};\n\
             export default function (input: {{ \"owner\": GetParameter0Input; \"per_page\"?: GetParameter3Input; \"state\"?: GetParameter2Input }}): {result};"
        )
    };
    let create_issue = {
        let (aliases, result) = http_result(&[("Issue", "201"), ("IssueDraft", "202")]);
        format!(
            "{aliases}type BodyInput = {{ \"labels\"?: Array<LabelInput>; \"meta\"?: {{ \"x\"?: string }}; \"title\": string }};\n\
             type Parameter0Input = string;\n\
             export default function (input: {{ \"body\": BodyInput; \"owner\": Parameter0Input }}): {result};"
        )
    };
    let nested_mcp = "type McpSafeMeta = Record<string, unknown>;\n\
         export default function (input: { \"city\": string; \"options\"?: { \"units\"?: \"c\" | \"f\" }; \"days\"?: Array<number> }): Promise<{ data: { content: Array<McpContentBlock>; structuredContent?: unknown; isError?: false }; metadata: { kind: \"mcp\"; _meta?: McpSafeMeta } }>;";
    let status_range = {
        let (aliases, result) = http_result(&[("Array<Name>", "HttpStatus2xx")]);
        format!(
            "{aliases}const HttpStatusRangeBrand: unique symbol;\n\
             type HttpStatus2xx = number & {{ readonly [HttpStatusRangeBrand]: \"2xx\" }};\n\
             export default function (): {result};"
        )
    };
    let named_with_inline = {
        let (aliases, result) =
            http_result(&[("(Issue & { \"triage\"?: { \"type\"?: string } })", "200")]);
        format!(
            "{aliases}type Issue = {{ \"id\": number }};\nexport default function (): {result};"
        )
    };
    let inline_only = {
        let (aliases, result) = http_result(&[
            ("({ \"a\"?: string } & { \"b\"?: string })", "200"),
            ("(Record<string, unknown> & Record<string, unknown>)", "201"),
        ]);
        format!("{aliases}export default function (): {result};")
    };
    let typed_and_untyped = {
        let (aliases, result) = http_result(&[("Thing", "200"), ("unknown", "202")]);
        format!(
            "{aliases}type Thing = {{ \"id\": string }};\nexport default function (): {result};"
        )
    };
    let shadowed_record = {
        let (aliases, result) = http_result(&[("(Thing & Record<string, unknown>)", "200")]);
        format!(
            "{aliases}type Record<K, V> = {{ \"key\": K; \"value\": V }};\ntype Thing = {{ \"id\": string }};\nexport default function (): {result};"
        )
    };
    let aliased_open_record = {
        let (aliases, result) = http_result(&[("(Thing & Extra)", "200")]);
        format!(
            "{aliases}type Extra = Record<string, unknown>;\ntype Thing = {{ \"id\": string }};\nexport default function (): {result};"
        )
    };
    let shadowed_record_input = "type Record<K, V> = { \"key\": K; \"value\": V };\nexport default function (input: { \"q\": string } & Record<string, unknown>): number;";
    let default_only = {
        let (aliases, result) = http_result(&[("Thing", "number")]);
        format!(
            "{aliases}type Thing = {{ \"id\": string }};\nexport default function (): {result};"
        )
    };
    let argumentless = {
        let (aliases, result) = http_result(&[("string", "200")]);
        format!("{aliases}export default function (): {result};")
    };
    let fallback = "function raw(input: unknown): Promise<{ data: unknown; metadata: { readonly kind: \"http\" } }>;";
    let named_input = "interface Input {\n    value: number;\n    nested: {\n        a: string;\n    };\n    tags?: string[];\n}\nexport default function run(input: Input): number;\nexport {};\n";
    let anonymous =
        "export default function (input: {\n    q: string;\n}): {\n    n: number;\n};\n";
    let inferred = "export default function pullRequestDetails(input: {\n    repo: string;\n    number: number;\n}): Promise<{\n    title: string;\n    merged_at: string | null;\n    base: {\n        ref: string;\n    };\n    linked: string[];\n}>;\n";
    let listed = "export default function openPrs(input: {\n    repo: string;\n}): Promise<{\n    number: number;\n    author: {\n        login: string;\n    };\n}[]>;\n";
    let generic_array = "export default function openIssuesByLabel(input: {\n    label: string;\n}): Promise<Array<{\n    number: number;\n    title: string;\n}>>;\n";
    let readonly_array = "export default function labels(): ReadonlyArray<{ name: string }>;";
    let named_union = "interface Row { id: number }\nexport default function find(input: { id: number }): Promise<Row | undefined>;";
    let named_intersection = "interface Base { id: number }\nexport default function tagged(): Promise<Base & { tag: string }>;";
    let aliased_promise =
        "type Result = Promise<{ title: string }>;\nexport default function aliased(): Result;";
    let union_elements = "interface Row { id: number }\nexport default function rows(): Promise<(Row | undefined)[]>;";
    let union_generic =
        "export default function events(): Array<{ kind: \"a\" } | { kind: \"b\" }>;";
    let mapped_result =
        "export default function mapped(): Promise<{ [K in \"a\" | \"b\"]: number }>;";
    let arrow = "declare const g: (input: {\n    q: string;\n}) => number;\nexport default g;\n";
    let alias_union = "type Mode = 'a' | 'b';\ntype Modes = Mode[];\ninterface Out { x: number }\nexport default function f(input: { mode: Mode; modes?: Modes; limit?: number }): Promise<Out[]>;";
    let promised_cycle = "type Result = Promise<Result>;\nexport default function loop(): Result;";
    let promised_chain = "type Outer = Inner;\ntype Inner = Promise<Outer>;\nexport default function chain(): Outer;";
    let promised_through_alias = "type Result = Promise<Next>;\ntype Next = Result;\nexport default function relay(): Result;";
    let recursive = "type Tree = Tree[] | string;\nexport default function walk(input: { tree: Tree }): number;";
    let unreadable = "export default something weird";
    let keyed_record = "export default function keyed(input: { label?: string } & Record<\"id\", unknown>): number;";
    let beyond_members = "// emitted by the compiler\nexport default function beyond(input: { keys: { [K in \"a\" | \"b\"]: number }; pick: Pick<Input, \"a\">; kind: string extends infer T ? T : never; label: `v${number}` }): number;";
    let shadowed_global = "type Array = Array[] | string;\nexport default function shadow(input: { list: Array }): number;";
    let escaped_literal = "function quoted(): Promise<{ data: \"x\\\"|y\" | \"z\"; metadata: { readonly status: 200 } }>;";
    let lone_surrogate = "export default function surrogate(input: { \"\\uD800\": number; mode: \"\\uD800\" | \"a\" }): number;";
    let markers = "export default function markers(input: { pair: [number, number?]; cb?: ((value?: number) => number) | null; sum: (...values: number[]) => number }): number;";

    for (declaration, callable, expected, complete) in [
        (
            list_issues.as_str(),
            Callable::Tool { leaf: "list" },
            r#"list(input: { owner: string; per_page?: number; state?: "open" | "closed" | "all" }): Promise<{ data: Array<{ id?; labels?; title? }> }>"#,
            true,
        ),
        (
            create_issue.as_str(),
            Callable::Tool {
                leaf: "createIssue",
            },
            "createIssue(input: { body: BodyInput; owner: string }): Promise<{ data: Issue | IssueDraft }>",
            false,
        ),
        (
            nested_mcp,
            Callable::Tool { leaf: "forecast" },
            "forecast(input: { city: string; options?: {…}; days?: Array<number> }): Promise<{ data: { content: Array<McpContentBlock> } }> // JSON text in data.content[0].text",
            false,
        ),
        (
            status_range.as_str(),
            Callable::Tool { leaf: "names" },
            "names(): Promise<{ data: Array<Name> }>",
            true,
        ),
        (
            named_with_inline.as_str(),
            Callable::Tool { leaf: "update" },
            "update(): Promise<{ data: { id; triage? } }>",
            true,
        ),
        (
            inline_only.as_str(),
            Callable::Tool { leaf: "merged" },
            "merged(): Promise<{ data: { a?; b? } | {…} }>",
            false,
        ),
        (
            typed_and_untyped.as_str(),
            Callable::Tool { leaf: "accept" },
            "accept(): Promise<{ data: { id } | unknown }>",
            true,
        ),
        (
            shadowed_record.as_str(),
            Callable::Tool { leaf: "pair" },
            "pair(): Promise<{ data: Thing & Record<string, unknown> }>",
            true,
        ),
        (
            aliased_open_record.as_str(),
            Callable::Tool { leaf: "extra" },
            "extra(): Promise<{ data: { id } }>",
            true,
        ),
        (
            shadowed_record_input,
            Callable::SavedFunction { leaf: "lookup" },
            "lookup(input: { q: string } & Record<string, unknown>): number",
            false,
        ),
        (
            default_only.as_str(),
            Callable::Tool { leaf: "thing" },
            "thing(): Promise<{ data: { id } }>",
            true,
        ),
        (
            argumentless.as_str(),
            Callable::Tool { leaf: "delete" },
            "delete(): Promise<{ data: string }>",
            true,
        ),
        (
            fallback,
            Callable::Tool { leaf: "raw" },
            "raw(input: unknown): Promise<{ data: unknown }>",
            true,
        ),
        (
            named_input,
            Callable::SavedFunction { leaf: "run" },
            "run(input: { value: number; nested: {…}; tags?: string[] }): number",
            false,
        ),
        (
            anonymous,
            Callable::SavedFunction { leaf: "count" },
            "count(input: { q: string }): { n: number }",
            true,
        ),
        (
            inferred,
            Callable::SavedFunction {
                leaf: "pullRequestDetails",
            },
            "pullRequestDetails(input: { repo: string; number: number }): Promise<{ title: string; merged_at: string | null; base: {…}; linked: string[] }>",
            false,
        ),
        (
            listed,
            Callable::SavedFunction { leaf: "openPrs" },
            "openPrs(input: { repo: string }): Promise<{ number: number; author: {…} }[]>",
            false,
        ),
        (
            generic_array,
            Callable::SavedFunction {
                leaf: "openIssuesByLabel",
            },
            "openIssuesByLabel(input: { label: string }): Promise<Array<{ number: number; title: string }>>",
            true,
        ),
        (
            readonly_array,
            Callable::SavedFunction { leaf: "labels" },
            "labels(): ReadonlyArray<{ name: string }>",
            true,
        ),
        (
            named_union,
            Callable::SavedFunction { leaf: "find" },
            "find(input: { id: number }): Promise<{ id: number } | undefined>",
            true,
        ),
        (
            named_intersection,
            Callable::SavedFunction { leaf: "tagged" },
            "tagged(): Promise<{ id: number } & { tag: string }>",
            true,
        ),
        (
            aliased_promise,
            Callable::SavedFunction { leaf: "aliased" },
            "aliased(): Promise<{ title: string }>",
            true,
        ),
        (
            union_elements,
            Callable::SavedFunction { leaf: "rows" },
            "rows(): Promise<({ id: number } | undefined)[]>",
            true,
        ),
        (
            union_generic,
            Callable::SavedFunction { leaf: "events" },
            r#"events(): Array<{ kind: "a" } | { kind: "b" }>"#,
            true,
        ),
        (
            mapped_result,
            Callable::SavedFunction { leaf: "mapped" },
            "mapped(): Promise<…>",
            false,
        ),
        (
            arrow,
            Callable::SavedFunction { leaf: "measure" },
            "measure(input: { q: string }): number",
            true,
        ),
        (
            alias_union,
            Callable::SavedFunction { leaf: "f" },
            r#"f(input: { mode: "a" | "b"; modes?: ("a" | "b")[]; limit?: number }): Promise<{ x: number }[]>"#,
            true,
        ),
        (
            promised_cycle,
            Callable::SavedFunction { leaf: "loop" },
            "loop(): Promise<…>",
            false,
        ),
        (
            promised_chain,
            Callable::SavedFunction { leaf: "chain" },
            "chain(): Promise<…>",
            false,
        ),
        (
            promised_through_alias,
            Callable::SavedFunction { leaf: "relay" },
            "relay(): Promise<…>",
            false,
        ),
        (
            recursive,
            Callable::SavedFunction { leaf: "walk" },
            "walk(input: { tree: Tree }): number",
            false,
        ),
        (
            unreadable,
            Callable::SavedFunction { leaf: "strange" },
            "strange(…)",
            false,
        ),
        (
            keyed_record,
            Callable::SavedFunction { leaf: "keyed" },
            r#"keyed(input: { label?: string } & Record<"id", unknown>): number"#,
            true,
        ),
        (
            beyond_members,
            Callable::SavedFunction { leaf: "beyond" },
            r#"beyond(input: { keys: …; pick: Pick<Input, "a">; kind: …; label: … }): number"#,
            false,
        ),
        (
            shadowed_global,
            Callable::SavedFunction { leaf: "shadow" },
            "shadow(input: { list: Array }): number",
            false,
        ),
        (
            escaped_literal,
            Callable::Tool { leaf: "quoted" },
            r#"quoted(): Promise<{ data: "x\"|y" | "z" }>"#,
            true,
        ),
        (
            lone_surrogate,
            Callable::SavedFunction { leaf: "surrogate" },
            r#"surrogate(input: { …; mode: … | "a" }): number"#,
            false,
        ),
        (
            markers,
            Callable::SavedFunction { leaf: "markers" },
            "markers(input: { pair: [number, number?]; sum: (...values: number[]) => number; cb?: ((value?: number) => number) | null }): number",
            true,
        ),
    ] {
        assert_eq!(
            signature_line(declaration, callable),
            SignatureLine {
                text: expected.to_string(),
                complete,
            },
            "{declaration}"
        );
    }
}

/// An MCP-backed Tool's declaration as the catalog projects it, with
/// `output` as its declared output, or `unknown` when it declares none.
fn mcp_tool(input: &str, output: &str) -> String {
    format!(
        "type McpSafeMeta = Record<string, unknown>;\n\
         export default function (input: {input}): Promise<{{ data: {{ content: Array<McpContentBlock>; structuredContent?: {output}; isError?: false }}; metadata: {{ kind: \"mcp\"; _meta?: McpSafeMeta }} }}>;"
    )
}

/// A string-literal union of `values`.
fn literals(values: &[&str]) -> String {
    values
        .iter()
        .map(|value| format!("\"{value}\""))
        .collect::<Vec<_>>()
        .join(" | ")
}

#[test]
fn a_long_enum_gives_way_so_an_mcp_listing_still_shows_its_query_and_text_result() {
    // Shaped like an issue tracker's MCP `list_issues`: a long `fields` enum
    // declared first once pushed every other input, and the result, past
    // the cut.
    let fields = literals(&[
        "id",
        "identifier",
        "title",
        "description",
        "priority",
        "estimate",
        "state",
        "assignee",
        "creator",
        "team",
        "project",
        "cycle",
        "labels",
        "parent",
        "children",
        "createdAt",
        "updatedAt",
        "startedAt",
        "completedAt",
        "canceledAt",
        "dueDate",
        "url",
        "branchName",
        "attachments",
        "comments",
        "subscribers",
        "relations",
        "reactions",
    ]);
    let declaration = mcp_tool(
        &format!(
            "{{ \"fields\"?: Array<{fields}>; \"query\"?: string; \"team\"?: string; \"assignee\"?: string; \"state\"?: string; \"limit\"?: number; \"includeArchived\"?: boolean; \"orderBy\"?: \"createdAt\" | \"updatedAt\" }}"
        ),
        "unknown",
    );

    assert_eq!(
        signature_line(&declaration, Callable::Tool { leaf: "listIssues" }),
        SignatureLine {
            text: r#"listIssues(input: { query?: string; team?: string; assignee?: string; state?: string; limit?: number; includeArchived?: boolean; orderBy?: "createdAt" | "updatedAt"; … }): Promise<{ data: { content: Array<McpContentBlock> } }> // JSON text in data.content[0].text"#.to_string(),
            complete: false,
        }
    );
}

#[test]
fn required_inputs_come_first_and_long_enums_last() {
    let long = literals(&[
        "created",
        "updated",
        "popularity",
        "long-running",
        "comments",
        "reactions",
    ]);
    let declaration = mcp_tool(
        &format!(
            "{{ \"fields\"?: Array<{long}>; \"sort\"?: \"asc\" | \"desc\"; \"kind\": {long}; \"page\"?: number; \"owner\": string }}"
        ),
        "{ \"total\": number }",
    );

    assert_eq!(
        signature_line(&declaration, Callable::Tool { leaf: "list" }),
        SignatureLine {
            text: format!(
                "list(input: {{ owner: string; kind: {long}; sort?: \"asc\" | \"desc\"; page?: number; fields?: Array<{long}> }}): Promise<{{ data: {{ structuredContent?: {{ total }}; content: Array<McpContentBlock> }} }}>"
            ),
            complete: true,
        },
        "required members come first, each group's long enums after its others, \
         and a declared output shows its field names"
    );
}

#[test]
fn a_long_input_gives_way_to_the_result_member_by_member() {
    let members = (0..80)
        .map(|index| format!("\"field{index}\": string"))
        .collect::<Vec<_>>()
        .join("; ");
    let issue = "type Issue = { \"id\": number; \"title\": string; \"state\"?: string };\n";
    let wide = |data: &str| {
        let (aliases, result) = http_result(&[(data, "200")]);
        format!("{aliases}{issue}export default function (input: {{ {members} }}): {result};")
    };

    let line = signature_line(&wide("Array<Issue>"), Callable::Tool { leaf: "wide" });

    assert!(
        line.text.len() <= SIGNATURE_LINE_MAX_BYTES,
        "{}",
        line.text.len()
    );
    assert!(
        line.text
            .starts_with("wide(input: { field0: string; field1: string; "),
        "{}",
        line.text
    );
    assert!(
        line.text
            .ends_with("; … }): Promise<{ data: Array<{ id; title; state? }> }>"),
        "whole members, then the marker, then the whole result with its fields: {}",
        line.text
    );
    assert!(
        !line.complete,
        "a line that leaves out inputs is not complete"
    );

    let fields = (0..40)
        .map(|index| format!("\"column{index}\": string"))
        .collect::<Vec<_>>()
        .join("; ");
    let (aliases, result) = http_result(&[("Row", "200")]);
    let row = format!("type Row = {{ {fields} }};\n");
    let line = signature_line(
        &format!("{aliases}{row}export default function (input: {{ {members} }}): {result};"),
        Callable::Tool { leaf: "wide" },
    );
    assert!(
        line.text.ends_with("; … }): Promise<{ data: Row }>"),
        "a result whose fields do not fit beside a long input shows its type name: {}",
        line.text
    );

    let line = signature_line(
        &format!("{aliases}{row}export default function (input: {{ \"id\": string }}): {result};"),
        Callable::Tool { leaf: "row" },
    );
    assert!(
        line.text
            .starts_with("row(input: { id: string }): Promise<{ data: { column0; column1; ")
            && line.text.ends_with("; column39 } }>")
            && line.complete,
        "a short input leaves the result more than its reserve: {}",
        line.text
    );
}

#[test]
fn an_input_that_is_not_one_object_is_cut_at_a_word_boundary() {
    let members = (0..80)
        .map(|index| format!("\"field{index}\": string"))
        .collect::<Vec<_>>()
        .join("; ");
    let declaration = format!(
        "function wide(input: {{ {members} }} | {{ \"id\": string }}): Promise<{{ data: unknown }}>;"
    );

    let line = signature_line(&declaration, Callable::Tool { leaf: "wide" });

    assert!(
        line.text.len() <= SIGNATURE_LINE_MAX_BYTES,
        "{}",
        line.text.len()
    );
    assert!(
        line.text.ends_with("…): Promise<{ data: unknown }>"),
        "{}",
        line.text
    );
    let kept = line.text.trim_end_matches("…): Promise<{ data: unknown }>");
    assert!(
        kept.ends_with(';') || kept.ends_with(':'),
        "cut inside a word: {}",
        line.text
    );
    assert!(line.text.starts_with("wide(input: { field0: string; "));
    assert!(!line.complete);
}
