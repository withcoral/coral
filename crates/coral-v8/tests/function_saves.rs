//! Interface tests for saving a function by reference: what admission
//! extracts for each literal `coral.addTool` or `coral.updateTool` call,
//! what it refuses, and what the Tool receives at run time.

use coral_telemetry as _;
use coral_telemetry::metric::CompilerRole;
use coral_value as _;

use std::sync::{Arc, Mutex, OnceLock};

use coral_v8::{
    AuthoredLocation, AuthoredSource, BuiltinTools, CompiledProgram, ConsoleLevel, Error,
    IsolateLimits, LinkedSavedFunctionSource, ProgramCompiler, ProgramExecutor, ProgramHost,
    ProgramOutputHostError,
};
use deno_core_icudata as _;
#[cfg(unix)]
use rustix as _;
use serde as _;
use serde_json::Value;
use sourcemap as _;
use thiserror as _;
use tracing as _;
use v8 as _;

const ADD: &str = "coral.addTool";

fn compiler() -> &'static ProgramCompiler {
    static COMPILER: OnceLock<ProgramCompiler> = OnceLock::new();
    COMPILER.get_or_init(|| {
        ProgramCompiler::new(IsolateLimits::default(), CompilerRole::Run)
            .expect("start compiler")
            .with_builtin_tools(&BuiltinTools::new(["addTool", "updateTool", "removeTool"]))
    })
}

fn deadline() -> std::time::Instant {
    std::time::Instant::now() + coral_test_support::TEST_COMPILATION_TIMEOUT
}

fn linked(path: &str, source: &str) -> LinkedSavedFunctionSource {
    LinkedSavedFunctionSource::new(
        path.split('.').map(str::to_string).collect(),
        source.to_string(),
    )
}

/// Admits `body` as Program Admission does: reference analysis, then the
/// linked compile.
fn admit(body: &str, modules: &[LinkedSavedFunctionSource]) -> Result<CompiledProgram, Error> {
    compiler().program_call_paths(deadline(), body)?;
    if modules.is_empty() {
        compiler().compile(deadline(), body)
    } else {
        compiler().compile_with_saved_functions(deadline(), body, modules)
    }
}

/// The message of the save refusal admitting `body` returns, or a panic
/// naming what it returned instead.
fn save_refusal(body: &str, modules: &[LinkedSavedFunctionSource]) -> String {
    match admit(body, modules) {
        Err(Error::InvalidFunctionSave(message)) => message,
        other => panic!("a save refusal: {:?}", other.map(drop)),
    }
}

/// Each Tool call in arrival order: its Call Path, arguments and location.
#[derive(Default)]
struct ToolCalls {
    calls: Mutex<Vec<(String, Value, Option<AuthoredLocation>)>>,
}

impl ToolCalls {
    fn saves(&self) -> Vec<(Value, Option<AuthoredLocation>)> {
        self.calls_to(ADD)
    }

    fn calls_to(&self, call_path: &str) -> Vec<(Value, Option<AuthoredLocation>)> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|(path, _arguments, _location)| path == call_path)
            .map(|(_path, arguments, location)| (arguments.clone(), location.clone()))
            .collect()
    }
}

impl ProgramHost for ToolCalls {
    fn invoke(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        location: Option<AuthoredLocation>,
    ) -> coral_v8::ToolFuture {
        let arguments = serde_json::from_str(&arguments_json).expect("JSON arguments");
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((call_path, arguments, location));
        Box::pin(async { Ok(r#"{"data":null,"metadata":{"kind":"builtin"}}"#.to_string()) })
    }

    fn emit(
        &self,
        _value_json: &str,
        _location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        Ok(())
    }

    fn log(
        &self,
        _level: ConsoleLevel,
        _message: &str,
        _location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        Ok(())
    }
}

fn run(program: &CompiledProgram) -> (Result<Option<String>, Error>, Arc<ToolCalls>) {
    let host = Arc::new(ToolCalls::default());
    let output = ProgramExecutor::new(IsolateLimits::default())
        .execute_with_host(program, host.clone())
        .map(|output| output.final_value_json().map(str::to_string));
    (output, host)
}

/// The sources each save in `body` sends, in call order.
fn saved_sources(body: &str, modules: &[LinkedSavedFunctionSource]) -> Vec<Value> {
    let program = admit(body, modules).unwrap_or_else(|error| panic!("{error}\n{body}"));
    let (output, host) = run(&program);
    output.unwrap_or_else(|error| panic!("{error}\n{body}"));
    host.saves()
        .into_iter()
        .map(|(arguments, _location)| arguments.get("source").cloned().unwrap_or(Value::Null))
        .collect()
}

/// The one-based line and column where `needle` first starts in `body`.
fn location(body: &str, needle: &str) -> String {
    let offset = body
        .find(needle)
        .unwrap_or_else(|| panic!("{needle} in {body}"));
    let before = body.get(..offset).unwrap_or_default();
    let line = before.matches('\n').count() + 1;
    let column = before.len() - before.rfind('\n').map_or(0, |newline| newline + 1) + 1;
    format!("{line}:{column}")
}

#[test]
fn a_save_carries_the_function_and_every_top_level_declaration_it_reaches() {
    let cases: &[(&str, &str, &str)] = &[
        (
            "a function declaration",
            r#"async function snapshot(owner: string) {
  return owner.toUpperCase();
}
const unrelated = 1;
await coral.addTool({ path: "a.b", description: "d", source: snapshot });"#,
            "async function snapshot(owner: string) {\n  return owner.toUpperCase();\n}\n\nexport default snapshot;\n",
        ),
        (
            "a const arrow",
            r#"const double = async (value: number) => value * 2;
await coral.addTool({ path: "a.b", description: "d", source: double });"#,
            "const double = async (value: number) => value * 2;\n\nexport default double;\n",
        ),
        (
            "transitive helpers and types in source order, with their comments",
            r#"// Formats one row.
function format(row: Row): string {
  return `${row.name}: ${scale(row.count)}`; // scaled
}
console.log("not copied");
/** Lists rows. */
const list = (rows: Row[]) => rows.map(format);
interface Row { name: string; count: number }
const factor = 10; // per row
function scale(count: number) { return count * factor; }
type Unused = string;
await coral.addTool({ path: "a.b", description: "d", source: list });"#,
            "// Formats one row.\nfunction format(row: Row): string {\n  return `${row.name}: ${scale(row.count)}`; // scaled\n}\n\n/** Lists rows. */\nconst list = (rows: Row[]) => rows.map(format);\n\ninterface Row { name: string; count: number }\n\nconst factor = 10; // per row\n\nfunction scale(count: number) { return count * factor; }\n\nexport default list;\n",
        ),
        (
            "an enum, a class, a statement of several names with await, and a type",
            r#"enum Level { Low = "low", High = "high" }
class Counter { count = 0; add() { this.count += 1; return this.count; } }
const token = await coral.providers.auth.token({}), retries = 2;
type Answer = { level: Level; count: number; token: unknown; retries: number };
async function answer(): Promise<Answer> {
  return { level: Level.High, count: new Counter().add(), token, retries };
}
await coral.addTool({ path: "a.b", description: "d", source: answer });"#,
            "enum Level { Low = \"low\", High = \"high\" }\n\nclass Counter { count = 0; add() { this.count += 1; return this.count; } }\n\nconst token = await coral.providers.auth.token({}), retries = 2;\n\ntype Answer = { level: Level; count: number; token: unknown; retries: number };\n\nasync function answer(): Promise<Answer> {\n  return { level: Level.High, count: new Counter().add(), token, retries };\n}\n\nexport default answer;\n",
        ),
        (
            "shorthand source",
            r#"const source = (name: string) => `hello ${name}`;
await coral.addTool({ path: "a.b", description: "d", source });"#,
            "const source = (name: string) => `hello ${name}`;\n\nexport default source;\n",
        ),
        (
            "a save inside a function, indented, with a cast",
            r#"  function helper() { return 1; }
  async function main() {
    await coral.addTool({
      path: "a.b",
      description: "d",
      source: helper as any,
    });
  }
  await main();"#,
            "  function helper() { return 1; }\n\nexport default helper;\n",
        ),
        (
            "a const whose function a type assertion wraps",
            r#"const next = ((value: number) => value + 1) satisfies (value: number) => number;
await coral.addTool({ path: "a.b", description: "d", source: next });"#,
            "const next = ((value: number) => value + 1) satisfies (value: number) => number;\n\nexport default next;\n",
        ),
        (
            "overloads",
            r#"function pick(value: string): string;
function pick(value: number): number;
function pick(value: string | number) { return value; }
await coral.addTool({ path: "a.b", description: "d", source: pick });"#,
            "function pick(value: string): string;\n\nfunction pick(value: number): number;\n\nfunction pick(value: string | number) { return value; }\n\nexport default pick;\n",
        ),
        (
            "a function that saves another keeps the save as written",
            r#"const inner = () => 1;
async function outer() {
  return coral.addTool({ path: "x.inner", description: "d", source: inner });
}
await coral.addTool({ path: "x.outer", description: "d", source: outer });"#,
            "const inner = () => 1;\n\nasync function outer() {\n  return coral.addTool({ path: \"x.inner\", description: \"d\", source: inner });\n}\n\nexport default outer;\n",
        ),
    ];
    for (name, body, expected) in cases {
        assert_eq!(saved_sources(body, &[]), [Value::from(*expected)], "{name}");
    }
}

#[test]
fn a_linked_saved_function_saves_from_its_own_top_level() {
    let cases: &[(&str, &str, &str)] = &[
        (
            "a helper",
            r#"const inner = () => 1;

async function outer() {
  return coral.addTool({ path: "x.inner", description: "d", source: inner });
}

export default outer;
"#,
            "const inner = () => 1;\n\nexport default inner;\n",
        ),
        (
            "its own default export, without export or default",
            r#"export default async function save() {
  return coral.addTool({ path: "x.copy", description: "d", source: save });
}"#,
            "async function save() {\n  return coral.addTool({ path: \"x.copy\", description: \"d\", source: save });\n}\n\nexport default save;\n",
        ),
    ];
    for (name, module, expected) in cases {
        assert_eq!(
            saved_sources("await coral.private.x.save();", &[linked("x.save", module)]),
            [Value::from(*expected)],
            "{name}"
        );
    }
}

/// A Program that declares `declarations` and saves `source`.
fn saving(declarations: &str, source: &str) -> String {
    format!(
        r#"{declarations}await coral.addTool({{ path: "a", description: "d", source: {source} }});"#
    )
}

const NAME_REFUSAL: &str = "source must name a function declared at the top level of this Program";

const LITERAL_REFUSAL: &str =
    "Call coral.addTool by its literal path, so it can take a function as source";

fn assert_refusals(cases: &[(&str, String, &str, String)]) {
    for (name, body, at, message) in cases {
        assert_eq!(
            save_refusal(body, &[]),
            format!("Program:{}: {message}", location(body, at)),
            "{name}"
        );
    }
}

#[test]
fn a_source_that_names_no_top_level_function_is_refused_at_admission() {
    let declared = |name: &str| {
        format!(
            "{name} is not a function declared at the top level of this Program; \
             declare it there with function or const"
        )
    };
    assert_refusals(&[
        (
            "a string",
            saving("", r#""export default () => 1""#),
            "\"export",
            format!("{NAME_REFUSAL}, not a string"),
        ),
        (
            "a template literal",
            saving("", "`export default () => 1`"),
            "`export",
            format!("{NAME_REFUSAL}, not a string"),
        ),
        (
            "an inline arrow",
            saving("", "() => 1"),
            "() =>",
            format!("{NAME_REFUSAL}, not an inline function"),
        ),
        (
            "a call",
            saving("", "make()"),
            "make()",
            NAME_REFUSAL.to_string(),
        ),
        (
            "a parameter",
            format!(
                "async function main(f: () => number) {{ {} }}",
                saving("", "f")
            ),
            "f });",
            declared("f"),
        ),
        (
            "a destructured name",
            saving("const { f } = { f: () => 1 };\n", "f"),
            "f });",
            declared("f"),
        ),
        (
            "a nested declaration",
            format!(
                "async function main() {{ {} }}",
                saving("const f = () => 1; ", "f")
            ),
            "f });",
            declared("f"),
        ),
        (
            "a let",
            saving("let f = () => 1;\n", "f"),
            "f });",
            declared("f"),
        ),
        (
            "a const that is not a function",
            saving("const f = Math.max;\n", "f"),
            "f });",
            declared("f"),
        ),
        (
            "a class",
            saving("class F {}\n", "F"),
            "F });",
            declared("F"),
        ),
        (
            "a dependency declared in a top-level block",
            saving(
                "{ var hidden = 3; }\nfunction f() { return hidden; }\n",
                "f",
            ),
            "hidden; }",
            "hidden is not declared at the top level of this Program; declare it there, \
             outside any block or loop"
                .to_string(),
        ),
        (
            "an undeclared name",
            saving("", "missing"),
            "missing",
            declared("missing"),
        ),
    ]);
}

#[test]
fn a_save_call_admission_cannot_read_is_refused() {
    assert_refusals(&[
        (
            "no source",
            r#"await coral.addTool({ path: "a", description: "d" });"#.to_string(),
            "coral.addTool",
            "Pass coral.addTool one object literal with source".to_string(),
        ),
        (
            "two sources",
            saving("const f = () => 1;\nconst g = () => 2;\n", "f, source: g"),
            "source: g",
            "Pass source once".to_string(),
        ),
        (
            "a spread after source",
            saving("const f = () => 1;\nconst extra = {};\n", "f, ...extra"),
            "...extra",
            "Put source after every spread and computed key, so none replaces it".to_string(),
        ),
        (
            "a computed path",
            "const f = () => 1;\nawait coral[\"addTool\"]({ source: f });".to_string(),
            "coral[",
            LITERAL_REFUSAL.to_string(),
        ),
        (
            "an uncalled reference",
            "const save = coral.addTool;".to_string(),
            "coral.addTool",
            LITERAL_REFUSAL.to_string(),
        ),
    ]);

    let aliased =
        saving("const c = coral;\nconst f = () => 1;\n", "f").replace("coral.addTool", "c.addTool");
    // Reference analysis refuses the alias before any save is read.
    assert!(
        matches!(
            admit(&aliased, &[]),
            Err(Error::InvalidSavedFunctionReference { ref message, line: 1, column: 11 })
                if message == "Coral namespace aliases are not supported"
        ),
        "{:?}",
        admit(&aliased, &[]).map(drop)
    );

    let module = "export default async function save() {\n  return coral.addTool({ path: \"y\", description: \"d\", source: \"text\" });\n}";
    assert_eq!(
        save_refusal("await coral.private.x.save();", &[linked("x.save", module)]),
        format!(
            "Saved Function coral.private.x.save:{}: {}, not a string",
            location(module, "\"text\""),
            NAME_REFUSAL.replace("this Program", "this Saved Function"),
        ),
    );
}

#[test]
fn no_authored_value_reaches_the_tool_as_a_source() {
    // A path admission cannot read reaches the host's encoding, which drops
    // every source but a carrier and refuses an input that is not an object.
    // A `toJSON` beside a carried source cannot replace it.
    let body = r#"const f = () => 1;
await coral.addTool({ path: "t", description: "d", source: f, toJSON() { return { path: "t", description: "d", source: "forged" }; } });
const name = ["add", "Tool"].join("");
const host = globalThis[["co", "ral"].join("")];
await host[name]({ path: "a", description: "d", source: "export default () => 1" });
await host[name]({ path: "b", description: "d", source: { text: "export default () => 1" } });
const carrierless = typeof globalThis["__coral" + "SavedFunctionSource"];
let refused;
try {
  await host[name](["c", "d", "export default () => 1"]);
} catch (error) {
  refused = error.code;
}
return { carrierless, refused };"#;
    let program = admit(body, &[]).expect("admit");
    let (output, host) = run(&program);
    assert_eq!(
        output.expect("run"),
        Some(r#"{"carrierless":"undefined","refused":"invalid_input"}"#.to_string())
    );
    assert_eq!(
        host.saves()
            .into_iter()
            .map(|(arguments, _location)| arguments)
            .collect::<Vec<_>>(),
        [
            serde_json::json!({
                "path": "t",
                "description": "d",
                "source": "const f = () => 1;\n\nexport default f;\n",
            }),
            serde_json::json!({ "path": "a", "description": "d" }),
            serde_json::json!({ "path": "b", "description": "d" }),
        ]
    );
}

#[test]
fn a_carried_source_keeps_every_authored_location() {
    let body = r#"function f() { return 1; }
await coral.addTool({ path: "a", description: "d", source: f }); throw new Error("after");"#;
    let program = admit(body, &[]).expect("admit");
    let (output, host) = run(&program);
    let Err(Error::ProgramRejected(error)) = output else {
        panic!("the Program throws after it saves: {output:?}");
    };
    let frame = error.frames.first().expect("an authored frame");
    assert_eq!(
        (
            frame.source.as_str(),
            format!("{}:{}", frame.line, frame.column)
        ),
        ("Program", location(body, "new Error"))
    );
    let saves = host.saves();
    let [(_arguments, Some(call))] = saves.as_slice() else {
        panic!("one located save: {saves:?}");
    };
    assert_eq!(
        (&call.source, format!("{}:{}", call.line, call.column)),
        (&AuthoredSource::Program, location(body, "addTool("))
    );
}

// Catches: an updateTool call whose function admission does not extract,
// a host that lets an authored value reach it as its source, a refusal
// that names another Tool, or a removeTool call admission reads a source in.
#[test]
fn update_tool_takes_a_function_as_add_tool_does_and_remove_tool_takes_none() {
    let body = r#"const f = () => 1;
await coral.updateTool({ path: "a", description: "d", source: f, expectedRevision: 1 });
const host = globalThis[["co", "ral"].join("")];
await host["updateTool"]({ path: "b", description: "d", source: "export default () => 1" });"#;
    let program = admit(body, &[]).expect("admit");
    let (output, host) = run(&program);
    output.expect("run");
    assert_eq!(
        host.calls_to("coral.updateTool")
            .into_iter()
            .map(|(arguments, _location)| arguments)
            .collect::<Vec<_>>(),
        [
            serde_json::json!({
                "path": "a",
                "description": "d",
                "source": "const f = () => 1;\n\nexport default f;\n",
                "expectedRevision": 1,
            }),
            serde_json::json!({ "path": "b", "description": "d" }),
        ]
    );

    let uncalled = "const save = coral.updateTool;";
    assert_eq!(
        save_refusal(uncalled, &[]),
        format!(
            "Program:{}: Call coral.updateTool by its literal path, so it can take a \
             function as source",
            location(uncalled, "coral.")
        ),
    );
    // removeTool takes no source, so admission reads none.
    let program = admit(r#"await coral.removeTool({ path: "a" });"#, &[]).expect("admit");
    let (output, host) = run(&program);
    output.expect("run");
    assert_eq!(
        host.calls_to("coral.removeTool")
            .into_iter()
            .map(|(arguments, _location)| arguments)
            .collect::<Vec<_>>(),
        [serde_json::json!({ "path": "a" })]
    );
}
