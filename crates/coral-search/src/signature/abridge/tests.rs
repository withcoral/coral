use super::fit;
use crate::EntityKind;

const USER: &str = r#"type User = { "login": string; "name": string | null; "email": string | null; "company": string | null; "blog": string | null; "location": string | null; "bio": string | null };"#;
const MILESTONE: &str = r#"type Milestone = { "title": string; "creator": User; "labels": Array<Label>; "parent": Issue | null; "due_on": string | null; "closed_at": string | null; "description": string | null; "html_url": string | null; "node_id": string | null };"#;
const LABEL: &str = r#"type Label = { "name": string };"#;
/// Always whole: the input, the callable, the wrapper and the top level.
const WHOLE: &str = r#"type OwnerInput = string;
export default function (input: { "owner": OwnerInput }): Promise<HttpResult<Array<Issue>, 200, "application/json">>;
type HttpResult<Data, Status, MediaType> = { status: Status; data: Data; mediaType: MediaType };
type Issue = { "id": number; "user": User; "milestone": Milestone };"#;

fn stub(name: &str, fields: &str) -> String {
    format!(
        "// Stubbed to save space: field names only. For field types, describe this path with types: [\"{name}\"].\ntype {name} = {{ {fields} }};"
    )
}

fn not_shown(names: &str) -> String {
    format!("// Not shown, to save space. To see them, describe this path with types: [{names}].")
}

/// Each row is one rule. The remaining types of `issues.list` are placed in
/// the order User and Milestone (the top level's), then Label (one level
/// down). Label is no longer whole than as a stub, so it is never stubbed.
#[test]
fn describe_fits_the_remaining_types_to_the_budget() {
    let user_stub = stub("User", "login; name; email; company; blog; location; bio");
    let milestone_stub = stub(
        "Milestone",
        "title; creator; labels; parent; due_on; closed_at; description; html_url; node_id",
    );
    let issues_list = [USER, MILESTONE, LABEL, WHOLE].join("\n");
    let all_whole = [USER, MILESTONE, LABEL]
        .iter()
        .map(|text| text.len() + 1)
        .sum::<usize>();
    let saved_function = "interface Input {\n    owner: string;\n}\ninterface Report {\n    author: Person;\n}\ninterface Person {\n    login: string;\n}\nexport default function weekly(input: Input): Promise<Report>;";
    let mcp = r#"type Day = { "high": number };
type McpSafeMeta = Record<string, unknown>;
function forecast(input: { "city": string }): Promise<{ data: { days: Array<Day> }; metadata: { _meta?: McpSafeMeta } }>;"#;
    let tool = EntityKind::ProviderTool;

    for (story, declaration, kind, types, budget, expected) in [
        (
            "everything fits whole, so the declaration is unchanged",
            issues_list.as_str(),
            tool,
            &[][..],
            all_whole,
            issues_list.clone(),
        ),
        (
            "every type fits as a stub; User's upgrade fits, and then Milestone's does not",
            &issues_list,
            tool,
            &[],
            all_whole - 1,
            [WHOLE, USER, &milestone_stub, LABEL].join("\n"),
        ),
        (
            "what does not fit is listed by name",
            &issues_list,
            tool,
            &[],
            0,
            [WHOLE, &not_shown(r#""User", "Milestone", "Label""#)].join("\n"),
        ),
        (
            "a stub fits where its full type does not",
            &issues_list,
            tool,
            &[],
            user_stub.len() + 1,
            [
                WHOLE,
                &user_stub,
                &not_shown(r#""Milestone", "Label""#),
            ]
            .join("\n"),
        ),
        (
            "requested types are whole, with their nested types placed and the top level not repeated",
            &issues_list,
            tool,
            &["Milestone"],
            user_stub.len() + 1,
            [MILESTONE, &user_stub, &not_shown(r#""Label""#)].join("\n"),
        ),
        (
            "a Saved Function's whole result is its success data",
            saved_function,
            EntityKind::SavedFunction,
            &[],
            0,
            "interface Input {\n    owner: string;\n}\nexport default function weekly(input: Input): Promise<Report>;\ninterface Report {\n    author: Person;\n}\n".to_string()
                + &not_shown(r#""Person""#),
        ),
        (
            "MCP data is inline, so it is its own top level, and its metadata's types are whole",
            mcp,
            tool,
            &[],
            0,
            r#"function forecast(input: { "city": string }): Promise<{ data: { days: Array<Day> }; metadata: { _meta?: McpSafeMeta } }>;
type McpSafeMeta = Record<string, unknown>;
"#
            .to_string()
                + &not_shown(r#""Day""#),
        ),
    ] {
        let types: Vec<String> = types.iter().map(ToString::to_string).collect();
        assert_eq!(
            fit(declaration, kind, &types, budget).unwrap(),
            expected,
            "{story}"
        );
    }
    assert!(milestone_stub.len() < MILESTONE.len() && user_stub.len() < USER.len());
}

/// A stub lists every field a type has, including those of the declared
/// types it is built from, and same-name interfaces merge as in TypeScript.
#[test]
fn stubs_and_requested_types_keep_fields_from_named_parts_and_merged_interfaces() {
    let long = "Array<Record<string, string | number | boolean | null>> | null";
    let intersection = format!(
        "type Base = {{ \"id\": {long}; \"login\": {long} }};\n\
         type Child = Base & {{ \"own\": {long}; \"extra\": {long}; \"more\": {long}; \"most\": {long} }};\n\
         function get(): Promise<{{ data: {{ child: Child }}; metadata: {{}} }}>;"
    );
    let abridged = fit(&intersection, EntityKind::ProviderTool, &[], 200).unwrap();
    assert!(
        abridged.contains("type Child = { id; login; own; extra; more; most };"),
        "{abridged}"
    );

    let merged = "interface Person {\n    login: string;\n}\ninterface Report {\n    author: Person;\n}\ninterface Person {\n    email: string;\n}\nexport default function weekly(): Promise<Report>;\nexport {};";
    assert_eq!(
        fit(
            merged,
            EntityKind::SavedFunction,
            &["Person".to_string()],
            0
        )
        .unwrap(),
        "interface Person {\n    login: string;\n}\ninterface Person {\n    email: string;\n}"
    );
}

/// `types` takes at most 32 names, so a longer list of skipped types asks
/// for some of them at a time instead of suggesting a call that fails.
#[test]
fn more_skipped_types_than_types_accepts_are_requested_some_at_a_time() {
    let count = crate::DESCRIBE_TYPES_MAX + 1;
    let types = (0..count)
        .map(|index| format!("type T{index} = {{ \"v\": string }};"))
        .collect::<Vec<_>>()
        .join("\n");
    let fields: Vec<String> = (0..count)
        .map(|index| format!("t{index}: T{index}"))
        .collect();
    let declaration = format!(
        "{types}\nfunction get(): Promise<{{ data: {{ {} }}; metadata: {{}} }}>;",
        fields.join("; ")
    );

    let abridged = fit(&declaration, EntityKind::ProviderTool, &[], 0).unwrap();

    let line = abridged.lines().last().unwrap();
    assert!(
        line.starts_with("// Not shown, to save space: \"T0\", "),
        "{line}"
    );
    assert!(
        line.ends_with(
            ", \"T32\". To see them, describe this path with up to 32 of these names in types."
        ),
        "{line}"
    );
}
