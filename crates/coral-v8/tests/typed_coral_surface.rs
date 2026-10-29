//! Typing `coral.providers`, `coral.workspace` and `coral.functions` from
//! supplied declarations.

use coral_telemetry as _;
use coral_v8::{
    CallableDeclaration, CompilerDiagnostic, CoralDeclarations, IsolateLimits, ProgramCompiler,
    SavedFunctionCompilation,
};
use coral_value as _;
use deno_core_icudata as _;
#[cfg(unix)]
use rustix as _;
use serde as _;
use serde_json as _;
use sourcemap as _;
use thiserror as _;
use tracing as _;
use v8 as _;

const DECLARATION_MAX_BYTES: usize = 256 * 1024;

const UNKNOWN_TOOL: u64 = 100_001;

const DECLARATION_TOO_LARGE: u64 = 100_002;

const REPOS_GET: &str = r#"type Owner = { "login": string };
type Repo = { "id": number; "full_name": string; "owner": Owner };
type HttpResult<Data, Status, MediaType> = { status: Status; data: Data; metadata: { readonly kind: "http"; readonly mediaType: MediaType } };
export default function (input: { "owner": string; "repo": string }): Promise<HttpResult<Repo, 200, "application/json">>;"#;

const TREE_GET: &str = r#"type Node = { "name": string; "parent"?: Node };
export default function (): Promise<{ status: 200; data: Node }>;"#;

// A reserved word is a valid Tool segment.
const REPOS_DELETE: &str =
    "export default function (input: { \"repo\": string }): Promise<{ status: 204; data: null }>;";

// A result whose shape the Provider does not declare.
const NOTION_SEARCH: &str = "export default function (): Promise<{ status: 200; data: unknown }>;";

// A result type the declaration exports, so an emitted Saved Function
// declaration could name it only by its Call Path module, which it cannot
// resolve.
const EXPORTED_GET: &str = "export type Item = { \"name\": string };\nexport default function (): Promise<{ status: 200; data: Item }>;";

const LIST_PROVIDERS: &str = "export default function (input?: { \"limit\"?: number }): Promise<{ \"providers\": Array<{ \"callRoot\": string }> }>;";

const FORECAST: &str = "interface Forecast {\n    temperatureC: number;\n}\nexport default function forecast(city: string): Forecast;\n";

fn deadline() -> std::time::Instant {
    std::time::Instant::now() + coral_v8::COMPILATION_TIMEOUT
}

fn tool(path: &str, declaration: &str) -> CallableDeclaration {
    CallableDeclaration::new(
        path.split('.').map(str::to_string).collect(),
        declaration.to_string(),
    )
}

fn tools() -> Vec<CallableDeclaration> {
    vec![
        tool("providers.github.repos.get", REPOS_GET),
        tool("providers.github.repos.delete", REPOS_DELETE),
        tool("providers.tree.get", TREE_GET),
        tool("providers.notion.search", NOTION_SEARCH),
        tool("providers.exported.get", EXPORTED_GET),
        tool("workspace.listProviders", LIST_PROVIDERS),
    ]
}

/// Prepares `source` as Saved Function save does: typed by `tools` and
/// `weather.forecast` along the Call Paths the source names.
fn prepare(
    compiler: &ProgramCompiler,
    source: &str,
    tools: Vec<CallableDeclaration>,
    declaration_max_bytes: usize,
) -> Result<SavedFunctionCompilation, coral_v8::Error> {
    let named = compiler.saved_function_call_paths(deadline(), source)?;
    compiler.prepare_typed_saved_function(
        deadline(),
        source,
        &CoralDeclarations::new(declaration_max_bytes)
            .with_tools(named.tools(), tools)
            .with_functions(vec![tool("weather.forecast", FORECAST)]),
    )
}

/// `(code, line, column)` of one diagnostic.
type Located = (u64, Option<u64>, Option<u64>);

fn errors(diagnostics: &[CompilerDiagnostic]) -> Vec<Located> {
    diagnostics
        .iter()
        .map(|diagnostic| (diagnostic.code(), diagnostic.line(), diagnostic.column()))
        .collect()
}

#[test]
fn saved_function_diagnostics_check_calls_against_declarations() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let cases: &[(&str, &str, &[Located])] = &[
        (
            "a wrong input field",
            "export default async function stars() {\n  const r = await coral.providers.github.repos.get({ owner: \"withcoral\", nope: 1 });\n  return r.data.id;\n}",
            &[(2353, Some(2), Some(74))],
        ),
        (
            "a misspelled result field",
            "export default async function stars() {\n  const r = await coral.providers.github.repos.get({ owner: \"withcoral\", repo: \"coral\" });\n  return r.data.fullname;\n}",
            &[(2551, Some(3), Some(17))],
        ),
        (
            "a wrong Saved Function argument",
            "export default function hot() {\n  return coral.functions.weather.forecast(42).temperatureC > 30;\n}",
            &[(2345, Some(2), Some(43))],
        ),
        (
            "a reserved-word Tool segment",
            "export default async function remove() {\n  await coral.providers.github.repos.delete({ repo: \"coral\", nope: 1 });\n}",
            &[(2353, Some(2), Some(62))],
        ),
        (
            "a Call Path that names no Tool the caller can call",
            "export default async function stars() {\n  const r = await coral.providers.github.repos.gett({ owner: 1 });\n  return r.data.anything;\n}",
            &[(UNKNOWN_TOOL, Some(2), Some(19))],
        ),
        (
            "a read of an undeclared result shape",
            "export default async function ids() {\n  const found = await coral.providers.notion.search();\n  return found.data.results;\n}",
            &[(18046, Some(3), Some(10))],
        ),
        (
            "a wrong Built-in Tool input",
            "export default async function namespaces() {\n  const r = await coral.workspace.listProviders({ limit: \"all\" });\n  return r.providers.map((p) => p.callRoot);\n}",
            &[(2322, Some(2), Some(51))],
        ),
        (
            "a Built-in Tool that does not exist",
            "export default async function wipe() {\n  return coral.workspace.deleteEverything();\n}",
            &[(UNKNOWN_TOOL, Some(2), Some(10))],
        ),
        (
            "an alias to a namespace that holds nothing the caller can call",
            "export default async function aliased() {\n  const things = coral.providers.secret.things;\n  return things.list();\n}",
            &[],
        ),
        (
            "an alias to a namespace beside a direct call into it",
            "export default async function aliased() {\n  const repos = coral.providers.github.repos;\n  await coral.providers.github.repos.get({ owner: \"a\", repo: \"b\" });\n  return repos.unseen();\n}",
            &[],
        ),
        (
            "a call through a computed segment of a namespace with nothing declared",
            "export default async function computed(name: string) {\n  return coral.providers.secret.things[name]();\n}",
            &[],
        ),
        (
            "a provider namespace the caller cannot see",
            "export default async function hidden() {\n  return coral.providers.secret.things.list();\n}",
            &[(UNKNOWN_TOOL, Some(2), Some(10))],
        ),
    ];
    for (name, source, expected) in cases {
        let compilation = prepare(&compiler, source, tools(), DECLARATION_MAX_BYTES)
            .unwrap_or_else(|error| panic!("{name}: save is advisory: {error:?}"));
        assert_eq!(
            errors(compilation.diagnostics()),
            *expected,
            "{name}: {:?}",
            compilation.diagnostics()
        );
    }
}

/// The message names the chain as written, whether or not a sibling
/// Tool under the same root resolved.
#[test]
fn unknown_tool_diagnostic_names_the_call_path_only() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    for (name, tools) in [
        ("beside a resolved sibling", tools()),
        ("alone in its root", Vec::new()),
    ] {
        for call_path in [
            "coral.providers.github.repos.gett",
            "coral.workspace.listProviderz",
        ] {
            let compilation = prepare(
                &compiler,
                &format!("export default async function f() {{\n  return {call_path}();\n}}"),
                tools.clone(),
                DECLARATION_MAX_BYTES,
            )
            .expect("advisory save");
            let messages = compilation
                .diagnostics()
                .iter()
                .map(CompilerDiagnostic::message)
                .collect::<Vec<_>>();
            assert_eq!(
                messages,
                [format!("{call_path} names no Tool that you can call.")],
                "{name}: {call_path}"
            );
        }
    }
}

#[test]
fn saved_function_declaration_inlines_a_tool_result_type() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let compilation = prepare(
        &compiler,
        "export default async function repo(owner: string) {\n  const r = await coral.providers.github.repos.get({ owner, repo: \"coral\" });\n  return r.data.owner;\n}",
        tools(),
        DECLARATION_MAX_BYTES,
    )
    .expect("typed save");
    assert!(
        compilation.diagnostics().is_empty(),
        "{:?}",
        compilation.diagnostics()
    );
    assert_eq!(
        compilation.declaration(),
        "export default function repo(owner: string): Promise<{\n    login: string;\n}>;\n"
    );
}

/// Each diagnostic names the Tool by its Call Path.
#[test]
fn an_uninlinable_result_type_saves_the_untyped_declaration() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    for (name, declaration_max_bytes, source, expected_codes, named) in [
        (
            "a recursive result type",
            DECLARATION_MAX_BYTES,
            "export default async function tree() {\n  return (await coral.providers.tree.get()).data;\n}",
            vec![4058],
            "\"coral.providers.tree.get\"",
        ),
        (
            "an oversized declaration",
            64,
            "export default async function repo() {\n  return (await coral.providers.github.repos.get({ owner: \"a\", repo: \"b\" })).data;\n}",
            vec![DECLARATION_TOO_LARGE],
            "exceeds 64 bytes",
        ),
        (
            "an exported result type",
            DECLARATION_MAX_BYTES,
            "export default async function item() {\n  return (await coral.providers.exported.get()).data;\n}",
            vec![2307],
            "'coral.providers.exported.get'",
        ),
    ] {
        let compilation = prepare(&compiler, source, tools(), declaration_max_bytes)
            .unwrap_or_else(|error| panic!("{name}: typing never fails a save: {error:?}"));
        assert!(
            compilation.declaration().contains("Promise<any>"),
            "{name}: {}",
            compilation.declaration()
        );
        assert_eq!(
            compilation
                .diagnostics()
                .iter()
                .map(|diagnostic| (diagnostic.code(), diagnostic.message().contains(named)))
                .collect::<Vec<_>>(),
            expected_codes
                .into_iter()
                .map(|code| (code, true))
                .collect::<Vec<_>>(),
            "{name}: {:?}",
            compilation.diagnostics()
        );
    }
}

#[test]
fn call_path_analysis_names_every_candidate_tool_call_path() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let named = compiler
        .saved_function_call_paths(
            deadline(),
            "export default async function f() {\n  const a = await coral.providers.github.repos.get.call(null, {});\n  const b = coral.providers[\"tree\"].get;\n  const c = coral.providers.weird[\"not-an-identifier\"].x;\n  const d = await coral.workspace.listProviders();\n  return [a, b, c, d, coral.providers, coral.workspace, coral.functions.weather.forecast];\n}",
        )
        .expect("analyze Saved Function");
    assert_eq!(
        named.tools().call_paths(),
        [
            "coral.providers.github.repos",
            "coral.providers.github.repos.get",
            "coral.providers.github.repos.get.call",
            "coral.providers.tree.get",
            "coral.workspace.listProviders",
        ]
    );
    assert_eq!(
        named
            .functions()
            .iter()
            .map(coral_v8::SavedFunctionReference::path)
            .collect::<Vec<_>>(),
        ["weather.forecast"]
    );
}
