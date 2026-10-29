//! Interface tests for isolated Program compilation and execution.

use coral_value as _;

use std::sync::{Arc, Mutex};

use coral_v8::{
    AuthoredLocation, AuthoredSource, CompiledProgram, ConsoleLevel, Error, IsolateLimits,
    LinkedSavedFunctionSource, ProgramCompiler, ProgramExecutionControl, ProgramExecutor,
    ProgramHost, ProgramOutputHostError, SavedFunctionReference, TYPESCRIPT_PROFILE, ToolHostError,
};
use deno_core_icudata as _;
use serde as _;
use serde_json as _;
use sourcemap as _;
use thiserror as _;
use v8 as _;

fn compilation_deadline() -> std::time::Instant {
    std::time::Instant::now() + coral_v8::COMPILATION_TIMEOUT
}

fn linked_source(path: &[&str], source: &str) -> LinkedSavedFunctionSource {
    LinkedSavedFunctionSource::new(
        path.iter().map(|segment| (*segment).to_string()).collect(),
        source.to_string(),
    )
}

fn reference_paths(references: &[SavedFunctionReference]) -> Vec<&str> {
    references
        .iter()
        .map(SavedFunctionReference::path)
        .collect()
}

#[test]
fn typescript_program_executes_as_javascript_in_v8() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let program = compiler
        .compile(
            compilation_deadline(),
            r#"
                enum Direction { Up = "up", Down = "down" }
                namespace MathHelpers {
                    export function double(value: number): number {
                        return value * 2;
                    }
                }
                class BoxedValue {
                    constructor(public readonly value: number) {}
                }

                const direction: Direction = Direction.Up;
                const result = await Promise.resolve(
                    new BoxedValue(MathHelpers.double(21)),
                );
                if (direction !== Direction.Up || result.value !== 42) {
                    throw new Error("TypeScript Program produced the wrong result");
                }
            "#,
        )
        .expect("compile TypeScript");

    ProgramExecutor::new(IsolateLimits::default())
        .execute(&program)
        .expect("execute emitted JavaScript");
}

#[test]
fn execution_control_terminates_noncooperative_javascript() {
    let program = ProgramCompiler::new(IsolateLimits::default())
        .expect("start compiler")
        .compile(compilation_deadline(), "while (true) {}")
        .expect("compile infinite Program");
    let control = ProgramExecutionControl::new();
    let execution_control = control.clone();
    let execution = std::thread::spawn(move || {
        ProgramExecutor::new(IsolateLimits::default()).execute_with_host_control(
            &program,
            Arc::new(RejectingToolHost),
            &execution_control,
        )
    });
    std::thread::sleep(std::time::Duration::from_millis(50));

    let _isolate_was_ready = control.terminate();
    let result = execution.join().expect("join Program execution");
    assert!(
        matches!(result, Err(Error::ProgramHostTerminated)),
        "unexpected termination result: {result:?}"
    );
}

#[test]
fn execution_distinguishes_no_final_value_from_explicit_null() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let no_value = compiler
        .compile(compilation_deadline(), "")
        .expect("compile empty body");
    let explicit_null = compiler
        .compile(compilation_deadline(), "return null;")
        .expect("compile explicit null");
    let structured = compiler
        .compile(
            compilation_deadline(),
            r#"return { 1: "one", answer: 42, labels: ["coral"] };"#,
        )
        .expect("compile structured value");
    let executor = ProgramExecutor::new(IsolateLimits::default());

    assert_eq!(
        executor
            .execute(&no_value)
            .expect("execute body with no value")
            .final_value_json(),
        None
    );
    assert_eq!(
        executor
            .execute(&explicit_null)
            .expect("execute explicit null")
            .final_value_json(),
        Some("null")
    );
    assert_eq!(
        executor
            .execute(&structured)
            .expect("execute structured value")
            .final_value_json(),
        Some(r#"{"1":"one","answer":42,"labels":["coral"]}"#)
    );
}

#[test]
fn final_values_follow_native_json_serialization() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let cases = [
        ("non-finite number", "return Number.NaN;", "null"),
        (
            "nested undefined",
            "return { omitted: undefined, kept: 42 };",
            r#"{"kept":42}"#,
        ),
        ("nested function", "return [() => 42];", "[null]"),
        ("sparse array", "return Array(1);", "[null]"),
        (
            "Date conversion",
            "return new Date(0);",
            r#""1970-01-01T00:00:00.000Z""#,
        ),
        (
            "getter",
            "return { get answer() { return 42; } };",
            r#"{"answer":42}"#,
        ),
        (
            "custom toJSON",
            "return { toJSON() { return { projected: 42 }; } };",
            r#"{"projected":42}"#,
        ),
        (
            "Map conversion",
            "return new Map([[\"answer\", 42]]);",
            "{}",
        ),
    ];
    let executor = ProgramExecutor::new(IsolateLimits::default());

    for (case, source, expected) in cases {
        let program = compiler
            .compile(compilation_deadline(), source)
            .expect("compile JSON serialization probe");
        let output = executor.execute(&program).expect("serialize Final Value");
        assert_eq!(output.final_value_json(), Some(expected), "{case}");
    }
}

#[test]
fn final_value_serialization_rejects_native_json_failures() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let invalid_values = [
        (
            "cyclic object",
            "const value = {}; value.self = value; return value;",
            Some("circular"),
        ),
        ("BigInt", "return 42n;", Some("BigInt")),
        ("root function", "return () => 42;", None),
    ];
    let executor = ProgramExecutor::new(IsolateLimits::default());

    for (case, source, expected_message) in invalid_values {
        let program = compiler
            .compile(compilation_deadline(), source)
            .expect("compile invalid value probe");
        let error = executor
            .execute(&program)
            .expect_err("native JSON failure must reject the Final Value");
        match (expected_message, error) {
            (Some(message), Error::ProgramOutputRejected(error)) => {
                assert_eq!(error.name, "TypeError", "{case}: {error}");
                assert!(error.message.contains(message), "{case}: {error}");
            }
            (None, Error::InvalidProgramValue(_)) => {}
            (_, error) => panic!("{case} returned the wrong error variant: {error}"),
        }
    }
}

#[test]
fn program_cannot_replace_the_pinned_json_serializer() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let program = compiler
        .compile(
            compilation_deadline(),
            r#"
                JSON.stringify = () => '"fabricated"';
                return { answer: 42 };
            "#,
        )
        .expect("compile serializer mutation");

    assert_eq!(
        ProgramExecutor::new(IsolateLimits::default())
            .execute(&program)
            .expect("serialize with captured intrinsic")
            .final_value_json(),
        Some(r#"{"answer":42}"#)
    );
}

#[test]
fn profile_accepts_javascript_and_exposes_the_bounded_ecmascript_surface() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let program = compiler
        .compile(
            compilation_deadline(),
            r#"
                function* numbers() { yield 1; yield 2; }
                class Total {
                    value = 0;
                    add(value) { this.value += value; }
                }

                const total = new Total();
                for (const value of numbers()) total.add(value);
                const mapped = new Map([["answer", 42]]);
                const selected = new Set(["coral"]);
                const settled = await Promise.resolve(true);
                const escaped = RegExp.escape("a+b");

                const actual = {
                    total: total.value,
                    mapped: mapped.get("answer"),
                    selected: selected.has("coral"),
                    matched: /reef/u.test("reef"),
                    year: new Date("2026-01-01T00:00:00Z").getUTCFullYear(),
                    formatted: new Intl.NumberFormat("en-US").format(1234),
                    bigint: Number(0x28n + 0b10n),
                    symbol: Symbol.keyFor(Symbol.for("coral")),
                    settled,
                    regexpEscape: new RegExp(`^${escaped}$`).test("a+b") &&
                        !new RegExp(`^${escaped}$`).test("aaab"),
                    setUnion: [...new Set([1, 2]).union(new Set([2, 3]))],
                    iteratorHelpers: Iterator.from([1, 2, 3])
                        .map((value) => value * 2)
                        .toArray(),
                    promiseTry: await Promise.try(() => 42),
                };
                if (
                    actual.total !== 3 ||
                    actual.mapped !== 42 ||
                    actual.selected !== true ||
                    actual.matched !== true ||
                    actual.year !== 2026 ||
                    actual.formatted !== "1,234" ||
                    actual.bigint !== 42 ||
                    actual.symbol !== "coral" ||
                    actual.settled !== true ||
                    actual.regexpEscape !== true ||
                    JSON.stringify(actual.setUnion) !== "[1,2,3]" ||
                    JSON.stringify(actual.iteratorHelpers) !== "[2,4,6]" ||
                    actual.promiseTry !== 42
                ) {
                    throw new Error("ES2025 runtime surface produced the wrong result");
                }
            "#,
        )
        .expect("compile JavaScript as TypeScript");

    assert_eq!(program.profile(), TYPESCRIPT_PROFILE);
    // The source map names only the Program, never a path or its source text.
    let source_map: serde_json::Value =
        serde_json::from_str(program.source_map()).expect("parse source map");
    assert_eq!(
        source_map.get("sources"),
        Some(&serde_json::json!(["Program"]))
    );
    assert!(source_map.get("sourceRoot").is_none());
    assert!(source_map.get("sourcesContent").is_none());

    ProgramExecutor::new(IsolateLimits::default())
        .execute(&program)
        .expect("execute JavaScript");
}

#[test]
fn program_syntax_diagnostics_use_authored_locations_without_a_semantic_gate() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let error = compiler
        .compile(compilation_deadline(), "\nconst answer: = 42;")
        .expect_err("syntax failure must block admission");
    let Error::Compilation { diagnostics } = error else {
        panic!("syntax failure returned {error}");
    };
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code() == 1110
                && diagnostic.line() == Some(2)
                && diagnostic.column() == Some(15))
    );

    compiler
        .compile(
            compilation_deadline(),
            "const answer: number = 'runtime value'; return answer;",
        )
        .expect("ordinary semantic diagnostics must not gate one-time admission");
}

#[test]
fn saved_function_reference_analysis_is_static_and_runtime_only() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let paths = compiler
        .saved_function_paths(compilation_deadline(),
            r"
                type Erased = typeof coral.functions.types.only;
                interface ErasedHeritage extends coral.functions.types.interfaceOnly {}
                declare class AmbientHeritage extends coral.functions.types.ambientOnly {}
                class RuntimeHeritage extends coral.functions.models.base {}
                const local = (coral: { functions: { local: { value(): number } } }) =>
                    coral.functions.local.value();
                const object = { coral: 1 };
                void object.coral;
                const first = coral.functions.alpha.one;
                return [first, (coral.functions.beta.two as () => number)(), RuntimeHeritage, local];
            ",
        )
        .expect("analyze literal Program references");
    assert_eq!(
        reference_paths(&paths),
        ["alpha.one", "beta.two", "models.base"]
    );
    assert!(
        matches!(
            paths.as_slice(),
            [alpha, beta, model]
                if (alpha.line(), alpha.column()) == (10, 31)
                    && (beta.line(), beta.column()) == (11, 33)
                    && (model.line(), model.column()) == (5, 47)
        ),
        "unexpected references: {paths:?}"
    );

    let dependencies = compiler
        .saved_function_dependency_paths(
            compilation_deadline(),
            r"
                export default function compose(): number {
                    type Erased = typeof coral.functions.types.only;
                    return coral.functions.alpha.one();
                }
            ",
        )
        .expect("analyze literal Saved Function dependencies");
    assert_eq!(reference_paths(&dependencies), ["alpha.one"]);
    assert!(matches!(
        dependencies.as_slice(),
        [reference] if (reference.line(), reference.column()) == (4, 28)
    ));

    let overload_only = compiler
        .saved_function_dependency_paths(compilation_deadline(),
            "function coral(value: string): void; export default function compose() { return coral.functions.alpha.one(); }",
        )
        .expect("analyze through an erased overload declaration");
    assert_eq!(reference_paths(&overload_only), ["alpha.one"]);
    let erased_overload = compiler
        .saved_function_dependency_paths(compilation_deadline(),
            "function erased(value = coral.functions.types.overloadOnly()): void; export default function compose() { return 42; }",
        )
        .expect("ignore an erased overload declaration");
    assert!(erased_overload.is_empty());

    let shadowed_global = compiler
        .saved_function_paths(compilation_deadline(),
            "const globalThis = { coral: { functions: { local: { value: () => 42 } } } }; return globalThis.coral.functions.local.value();",
        )
        .expect("ignore a local globalThis binding");
    assert!(shadowed_global.is_empty());

    let shadowed = compiler
        .prepare_saved_function(compilation_deadline(),
            "export default function local(coral: { functions: { local: { value(): number } } }): number { return coral.functions.local.value(); }",
        )
        .expect("prepare a callable with a local coral parameter");
    assert!(shadowed.references().is_empty());

    for source in [
        "const key = 'one'; return coral.functions.alpha[key]();",
        "const key = 'one'; return (coral.functions.alpha as any)[key]();",
        "const functions = coral.functions; return functions.alpha.one();",
        "const root = coral; return root.functions.alpha.one();",
        "const key = 'functions'; return coral[key].alpha.one();",
        "return globalThis.coral.functions.alpha.one();",
        "const global = globalThis; return global.coral.functions.alpha.one();",
        "let global = globalThis; return global.coral.functions.alpha.one();",
        "const key = 'coral'; return globalThis[key].functions.alpha.one();",
        "let key = 'functions'; return globalThis.coral[key].alpha.one();",
        "const key = unknownKey; return globalThis[key].functions.alpha.one();",
        "const { coral: globalCoral } = globalThis; return globalCoral.functions.alpha.one();",
        "const { functions } = globalThis.coral; return functions.alpha.one();",
        "const pick = (root: any, key: string) => root[key]; return pick(coral.functions, 'alpha')();",
        "const key = 'base'; class RuntimeHeritage extends coral.functions.models[key] {} return RuntimeHeritage;",
    ] {
        compiler
            .saved_function_paths(compilation_deadline(), source)
            .expect_err("computed or aliased Program reference must be rejected");
    }
}

#[test]
fn reference_analysis_preserves_transparent_wrappers_and_ordinary_globals() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    for source in [
        "return ((coral)).functions.alpha.one();",
        "return (coral as any).functions.alpha.one();",
        "return (((coral as any))).functions.alpha.one();",
    ] {
        let references = compiler
            .saved_function_paths(compilation_deadline(), source)
            .expect("transparent wrappers preserve a literal reference");
        assert_eq!(reference_paths(&references), ["alpha.one"]);
    }

    let source = "type __coralModel = number; const __coralValue: __coralModel = 1; let global = globalThis; return global[\"JSON\"].stringify(__coralValue);";
    let ordinary_globals = compiler
        .saved_function_paths(compilation_deadline(), source)
        .expect("ordinary globals and local compiler-prefix names remain available");
    assert!(ordinary_globals.is_empty());
    compiler
        .compile(compilation_deadline(), source)
        .expect("compile ordinary globals and local compiler-prefix names");
}

#[test]
fn authored_source_cannot_reach_private_linker_bindings() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let error = compiler
        .compile_with_saved_functions(compilation_deadline(),
            "__coralSavedFunctionDefaults[0] = () => (() => 'changed'); return coral.functions.safe.value();",
            &[linked_source(
                &["safe", "value"],
                "export default function value(): string { return 'original'; }",
            )],
        )
        .expect_err("Program source must not reach private linker state");
    assert!(
        matches!(
            &error,
            Error::ForbiddenCapability(message)
                if message == "private compiler bindings are not available at 1:1"
        ),
        "unexpected private-binding error: {error:?}"
    );
    compiler
        .prepare_saved_function(
            compilation_deadline(),
            "export default function value() { return __coralSavedFunctionDefaults[0]; }",
        )
        .expect_err("Saved Function source must not reach private linker state");
    for source in [
        "class Attack extends (__coralSavedFunctionDefaults[0] = Object, Object) {} return Attack;",
        "const captured = { __coralSavedFunctionDefaults }; return captured;",
    ] {
        compiler
            .compile_with_saved_functions(
                compilation_deadline(),
                source,
                &[linked_source(
                    &["safe", "value"],
                    "export default function value(): string { return 'original'; }",
                )],
            )
            .expect_err("indirect source must not reach private linker state");
    }

    let local_private_name = compiler
        .compile_with_saved_functions(compilation_deadline(),
            "const __coralSavedFunctionDefaults = 42; void coral.functions.safe.value; return __coralSavedFunctionDefaults;",
            &[linked_source(
                &["safe", "value"],
                "export default function value(): string { return 'original'; }",
            )],
        )
        .expect("a local binding may use the compiler-private prefix");
    let output = ProgramExecutor::new(IsolateLimits::default())
        .execute(&local_private_name)
        .expect("execute local private-prefix binding");
    assert_eq!(output.final_value_json(), Some("42"));

    let prototype_probe = compiler
        .compile_with_saved_functions(
            compilation_deadline(),
            r#"
                const captured = globalThis.capturedLinkedDefault;
                Reflect.deleteProperty(Array.prototype, "1");
                return typeof captured;
            "#,
            &[
                linked_source(
                    &["attack", "install"],
                    r#"
                        Object.defineProperty(Array.prototype, "1", {
                            configurable: true,
                            set(value) { globalThis.capturedLinkedDefault = value; },
                        });
                        export default function install() {}
                    "#,
                ),
                linked_source(
                    &["safe", "value"],
                    "export default function value(): number { return 42; }",
                ),
            ],
        )
        .expect("compile prototype-pollution probe");
    let output = ProgramExecutor::new(IsolateLimits::default())
        .execute(&prototype_probe)
        .expect("execute prototype-pollution probe");
    assert_eq!(output.final_value_json(), Some(r#""undefined""#));
}

#[test]
fn forbidden_capabilities_block_programs_and_saved_functions() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    for source in [
        "await import('node:fs');",
        "const moduleName = 'node:fs'; await import(moduleName);",
        "return fetch('https://example.test');",
        "return process.env.SECRET;",
        "return eval('40 + 2');",
        "return new Function('return 42')();",
        "return new ArrayBuffer(8);",
        "return new Uint8Array(8);",
        "return globalThis.Uint16Array.from([1]);",
        "return globalThis['DataView'];",
        "//# sourceURL=spoofed.js\nreturn 42;",
    ] {
        compiler
            .compile(compilation_deadline(), source)
            .expect_err("forbidden Program capability must be rejected");
    }
    compiler
        .compile(
            compilation_deadline(),
            "const process = { answer: 42 }; return process.answer;",
        )
        .expect("an authored local with the same name grants no ambient capability");
    compiler
        .compile(
            compilation_deadline(),
            "process: { break process; } return 42;",
        )
        .expect("a label named like an ambient global is ordinary JavaScript");
    compiler
        .compile(
            compilation_deadline(),
            "type BrowserFile = File; return true;",
        )
        .expect("an erased unresolved type is ordinary non-blocking semantic advice");
    compiler
        .prepare_saved_function(compilation_deadline(),
            "type BrowserFile = File; export default function available(): boolean { return true; }",
        )
        .expect("a private erased unresolved type must not block Saved Function authoring");

    for source in [
        "export default async function load() { return import('node:fs'); }",
        "export default function secret() { return process.env.SECRET; }",
        "export default function bytes() { return new ArrayBuffer(8); }",
        "export default function values() { return new Float64Array(8); }",
    ] {
        compiler
            .prepare_saved_function(compilation_deadline(), source)
            .expect_err("forbidden Saved Function capability must be rejected");
    }
}

#[test]
fn authored_comments_cannot_collide_with_linker_source_mapping_boundaries() {
    let program = ProgramCompiler::new(IsolateLimits::default())
        .expect("start compiler")
        .compile_with_saved_functions(compilation_deadline(),
            "return coral.functions.safe.value();",
            &[
                linked_source(
                    &["comments", "marker"],
                    "/*__coral_authored_saved_function_1__*/\n/*__coral_authored_program__*/\nexport default function marker(): number { return 1; }",
                ),
                linked_source(
                    &["safe", "value"],
                    "export default function value(): number { return 42; }",
                ),
            ],
        )
        .expect("authored marker-like comments cannot alter source mappings");
    let output = ProgramExecutor::new(IsolateLimits::default())
        .execute(&program)
        .expect("execute comment-collision probe");
    assert_eq!(output.final_value_json(), Some("42"));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one compact corpus covers callable-rooted declaration projection"
)]
fn saved_function_declaration_fields_follow_the_callable_contract() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let compilation = compiler
        .prepare_saved_function(compilation_deadline(),
            r#"
                interface RecursiveNode {
                    label: string;
                    children: RecursiveNode[];
                }
                type Choice<T extends RecursiveNode> = T | { alternate: string };
                interface ResultValue {
                    status: "ready" | "waiting";
                    "wire-name": unknown;
                    7?: unknown;
                    [key: string]: unknown;
                    [Symbol.iterator](): Iterator<unknown>;
                }
                function inspect(value: Choice<RecursiveNode>, ...flags: string[]): Promise<ResultValue>;
                function inspect<T extends RecursiveNode>(generic: T, mode: "generic"): Promise<ResultValue>;
                function inspect(value: Choice<RecursiveNode>, ...flags: string[]): Promise<ResultValue> {
                    void flags;
                    return Promise.resolve({
                        status: "ready",
                        "wire-name": value,
                        [Symbol.iterator]: function* () {},
                    });
                }
                export default inspect;
            "#,
        )
        .expect("prepare Saved Function");

    let fields = compilation
        .declaration_fields()
        .expect("declaration projection is ready");
    let paths = fields
        .iter()
        .map(coral_v8::SavedFunctionDeclarationField::path)
        .collect::<Vec<_>>();
    assert!(paths.contains(&"input.value.label"));
    assert!(paths.contains(&"input.value.children"));
    assert!(paths.contains(&"input.value.children[]"));
    assert!(paths.contains(&"input.value.alternate"));
    assert!(paths.contains(&"input.flags[]"));
    assert!(paths.contains(&"input.generic.label"));
    assert!(paths.contains(&"result.status"));
    assert!(paths.contains(&"result[\"wire-name\"]"));
    assert!(paths.contains(&"result[7]"));
    assert!(paths.contains(&"result[key]"));
    assert!(!paths.iter().any(|path| path.contains("iterator")));
    assert!(
        !paths.iter().any(|path| path.starts_with("RecursiveNode")),
        "local declarations must be projected only through callable roots"
    );

    let nested_and_rest = compiler
        .prepare_saved_function(
            compilation_deadline(),
            r"
                interface Item { id: string }
                type Items = Array<Item>;
                interface Options { callback: (nested: Item) => Item }
                export default function inspect(options: Options, ...items: Items): Item[] {
                    return items.map(options.callback);
                }
            ",
        )
        .expect("prepare nested-callable and rest Saved Function");
    let nested_and_rest_paths = nested_and_rest
        .declaration_fields()
        .expect("declaration projection is ready")
        .iter()
        .map(coral_v8::SavedFunctionDeclarationField::path)
        .collect::<Vec<_>>();
    assert!(nested_and_rest_paths.contains(&"input.options.callback"));
    assert!(nested_and_rest_paths.contains(&"input.items[].id"));
    assert!(!nested_and_rest_paths.contains(&"input.nested"));
    assert!(!nested_and_rest_paths.contains(&"result.id"));
    assert!(
        !nested_and_rest_paths
            .iter()
            .any(|path| path.contains("[][]"))
    );

    for (case, source) in [
        (
            "tuple rest",
            "interface Item { id: string } export default function inspect(...items: [Item, Item]): number { return items.length; }",
        ),
        (
            "generic constrained rest",
            "interface Item { id: string } export default function inspect<T extends Item[]>(...items: T): number { return items.length; }",
        ),
    ] {
        let projected = compiler
            .prepare_saved_function(compilation_deadline(), source)
            .unwrap_or_else(|error| panic!("prepare {case}: {error}"));
        let paths = projected
            .declaration_fields()
            .unwrap_or_else(|error| panic!("project {case}: {error}"))
            .iter()
            .map(coral_v8::SavedFunctionDeclarationField::path)
            .collect::<Vec<_>>();
        assert!(paths.contains(&"input.items[].id"), "{case}: {paths:?}");
        assert!(
            !paths.iter().any(|path| path.contains("[][]")),
            "{case}: {paths:?}"
        );
    }

    let destructured = compiler
        .prepare_saved_function(compilation_deadline(),
            "export default function inspect({ label }: { label: string }): string { return label; }",
        )
        .expect("prepare destructured Saved Function");
    assert!(
        destructured
            .declaration_fields()
            .expect("declaration projection is ready")
            .iter()
            .any(|field| field.path() == "input[0].label")
    );
}

#[test]
fn saved_function_authoring_types_the_coral_host_root() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let typed = compiler
        .prepare_saved_function(
            compilation_deadline(),
            r"
        export default async function report(id: string): Promise<string> {
            try {
                const issue = await coral.providers.tracker.issues.get({ id });
                const title: string = await coral.functions.reports.title(issue.data);
                await coral.emit(title);
                return title;
            } catch (error) {
                console.warn('report failed', error);
                if (error instanceof coral.ToolError) {
                    if (error.status === 404) return 'missing';
                    const body: unknown = error.data;
                    const metadata: unknown = error.metadata;
                    return error.code;
                }
                throw error;
            }
        }
        ",
        )
        .expect("prepare a Saved Function that uses the coral host root");
    assert_eq!(
        typed
            .diagnostics()
            .iter()
            .map(|diagnostic| (diagnostic.code(), diagnostic.message()))
            .collect::<Vec<_>>(),
        []
    );
    assert_eq!(reference_paths(typed.references()), ["reports.title"]);

    let misused = compiler
        .prepare_saved_function(
            compilation_deadline(),
            "export default function misuse(): number { console.table([]); console.log = () => {}; new coral.ToolError(); return coral.missing; }",
        )
        .expect("host root misuse is advisory");
    for (code, name) in [
        (2339, "'table'"),
        (2540, "'log'"),
        (2673, "ToolError"),
        (2339, "'missing'"),
    ] {
        assert!(
            misused
                .diagnostics()
                .iter()
                .any(|diagnostic| diagnostic.code() == code && diagnostic.message().contains(name)),
            "missing {code} for {name}: {:?}",
            misused.diagnostics()
        );
    }

    let error = compiler
        .prepare_saved_function(
            compilation_deadline(),
            "export default function leak(error: coral.ToolError): string { return error.code; }",
        )
        .expect_err("the public declaration must stay self-contained");
    assert!(
        matches!(&error, Error::Compilation { diagnostics } if !diagnostics.is_empty()),
        "unexpected host-type declaration error: {error:?}"
    );
}

#[test]
fn declaration_projection_uses_resolved_public_instance_types() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let compilation = compiler
        .prepare_saved_function(
            compilation_deadline(),
            r#"
        interface Item {
            /** Contact address. */
            customerEmail: string;
        }
        interface Box<T = Item> { item: T }
        enum Status { Ready = "ready", Waiting = "waiting" }
        type Renamed<T> = { [K in keyof T as `public_${string & K}`]: T[K] };
        class Model {
            private secret: string = "";
            protected hidden: string = "";
            static cache: string = "";
            public name: string = "";
        }
        export default function inspect(
            picked: Pick<Item, "customerEmail">,
            renamed: Renamed<Item>,
            boxed: Box,
            model: Model,
            status: Status,
        ): Promise<Readonly<Item>> { return Promise.resolve({ customerEmail: "" }); }
        "#,
        )
        .expect("prepare resolved types");
    let fields = compilation.declaration_fields().expect("project fields");
    let paths = fields
        .iter()
        .map(coral_v8::SavedFunctionDeclarationField::path)
        .collect::<Vec<_>>();
    for path in [
        "input.picked.customerEmail",
        "input.renamed.public_customerEmail",
        "input.boxed.item.customerEmail",
        "input.model.name",
        "result.customerEmail",
    ] {
        assert!(paths.contains(&path), "missing {path}: {paths:?}");
    }
    for path in [
        "input.model.secret",
        "input.renamed.customerEmail",
        "input.model.hidden",
        "input.model.cache",
    ] {
        assert!(
            !paths.contains(&path),
            "not a public instance field: {path}"
        );
    }
    assert!(
        fields
            .iter()
            .any(|field| field.path() == "input.picked.customerEmail"
                && field.text().contains("Contact address"))
    );
    assert!(
        fields
            .iter()
            .any(|field| field.path() == "input.status" && field.text().contains("waiting"))
    );
}

#[test]
fn tool_paths_are_not_thenables() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let program = compiler.compile(compilation_deadline(),
        "const tool = await Promise.resolve(coral.providers.widgets.create); return await tool({ id: 1 });",
    ).expect("compile path value");
    let host = Arc::new(CapturingToolHost::default());
    ProgramExecutor::new(IsolateLimits::default())
        .execute_with_host(&program, host.clone())
        .expect("resolve and invoke path");
    assert_eq!(
        host.calls.lock().expect("calls").as_slice(),
        &[(
            "coral.providers.widgets.create".to_string(),
            "{\"id\":1}".to_string()
        )]
    );
}

// Catches: a misspelled member such as `coral.emitt` becoming a Tool
// call that fails only when the Run ends, instead of throwing where it is used.
#[test]
fn programs_call_every_call_root_under_coral_by_its_complete_path() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let program = compiler
        .compile(
            compilation_deadline(),
            "await coral.providers.workspace.create({ id: 1 }); await coral.workspace.addProvider(); \
             let typo = 'no error'; try { coral.emitt(1); } catch (error) { typo = error.name; } \
             return [typeof coral.emit, coral.ToolError.name, Object.keys(coral).sort(), \
                     coral.then, typeof coral.billing, typo];",
        )
        .expect("compile Call Root calls");
    let host = Arc::new(CapturingToolHost::default());
    let outcome = ProgramExecutor::new(IsolateLimits::default())
        .execute_with_host(&program, host.clone())
        .expect("invoke both Call Roots");
    assert_eq!(
        host.calls.lock().expect("calls").as_slice(),
        &[
            (
                "coral.providers.workspace.create".to_string(),
                "{\"id\":1}".to_string()
            ),
            ("coral.workspace.addProvider".to_string(), "{}".to_string()),
        ]
    );
    assert_eq!(
        outcome.final_value_json(),
        Some(
            "[\"function\",\"ToolError\",[\"ToolError\",\"emit\",\"providers\",\"workspace\"],\
             null,\"undefined\",\"TypeError\"]"
        )
    );
}

#[test]
fn execution_has_no_forbidden_ambient_capability() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let program = compiler
        .compile(
            compilation_deadline(),
            r#"
                const categories = {
                    node: ["global", "process", "Buffer"],
                    browser: [
                        "window", "document", "navigator", "location",
                        "history", "screen", "performance",
                    ],
                    packages: ["require", "module", "exports", "Deno", "Bun"],
                    filesystem: ["File", "FileReader", "showOpenFilePicker"],
                    environment: ["process", "Deno", "Bun"],
                    timers: [
                        "setTimeout", "clearTimeout", "setInterval",
                        "clearInterval", "setImmediate", "clearImmediate",
                        "queueMicrotask",
                    ],
                    workers: [
                        "Worker", "SharedWorker", "ServiceWorker",
                        "importScripts", "MessageChannel", "BroadcastChannel",
                    ],
                    network: [
                        "fetch", "XMLHttpRequest", "WebSocket", "EventSource",
                    ],
                    credentials: ["crypto", "Credential", "CredentialsContainer"],
                    sharedMemory: ["SharedArrayBuffer", "Atomics"],
                    binaryBuffers: [
                        "ArrayBuffer", "DataView", "Int8Array", "Uint8Array",
                        "Uint8ClampedArray", "Int16Array", "Uint16Array",
                        "Int32Array", "Uint32Array", "Float16Array",
                        "Float32Array", "Float64Array", "BigInt64Array",
                        "BigUint64Array",
                    ],
                    webAssembly: ["WebAssembly"],
                };
                const ambient = Object.fromEntries(
                    Object.entries(categories).map(([category, names]) => [
                        category,
                        Object.fromEntries(names.map((name) => [
                            name,
                            typeof globalThis[name],
                        ])),
                    ]),
                );

                for (const [category, globals] of Object.entries(ambient)) {
                    for (const [name, kind] of Object.entries(globals)) {
                        if (kind !== "undefined") {
                            throw new Error(
                                `forbidden ${category} capability ${name} was present`,
                            );
                        }
                    }
                }
            "#,
        )
        .expect("compile ambient-capability probe");
    ProgramExecutor::new(IsolateLimits::default())
        .execute(&program)
        .expect("execute ambient-capability probe");
}

#[test]
fn runtime_also_disables_dynamic_code_for_previously_accepted_packages() {
    let source_map = ProgramCompiler::new(IsolateLimits::default())
        .expect("start compiler")
        .compile(compilation_deadline(), "return true;")
        .expect("compile source-map fixture")
        .source_map()
        .to_string();
    let program = CompiledProgram::restore(
        r#"
            (async () => {
                for (const attempt of [
                    () => eval("1 + 1"),
                    () => new Function("return 2")(),
                    () => (() => {}).constructor("return 3")(),
                    () => (async () => {}).constructor("return 4")(),
                    () => (function* () {}).constructor("return 5")(),
                ]) {
                    try {
                        attempt();
                        throw new Error("dynamic code was available");
                    } catch (error) {
                        if (!(error instanceof EvalError)) throw error;
                    }
                }
            })();
        "#
        .to_string(),
        source_map,
    );

    ProgramExecutor::new(IsolateLimits::default())
        .execute(&program)
        .expect("runtime must reject dynamic code generation");
}

#[test]
fn runtime_rejects_buffers_in_previously_accepted_packages() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let fixture = compiler
        .compile(compilation_deadline(), "return 42;")
        .expect("compile source-map fixture");
    for source in [
        "new ArrayBuffer(8)",
        "new Uint8Array(8).buffer.constructor",
        "new DataView(new ArrayBuffer(8))",
        "new ArrayBuffer(0, { maxByteLength: 8 })",
    ] {
        let program = CompiledProgram::restore(
            format!("(async () => {{ return {source}; }})();"),
            fixture.source_map().to_string(),
        );
        let error = ProgramExecutor::new(IsolateLimits::default())
            .execute(&program)
            .expect_err("restored package must not gain binary-buffer access");
        let Error::ProgramRejected(diagnostic) = error else {
            panic!("unexpected buffer rejection: {error}");
        };
        assert!(diagnostic.to_string().contains("ReferenceError"));
    }
    assert_eq!(
        ProgramExecutor::new(IsolateLimits::default())
            .execute(&fixture)
            .expect("execute healthy Program after rejection")
            .final_value_json(),
        Some("42")
    );
}

#[test]
fn compiler_and_fresh_execution_isolates_share_no_mutable_state() {
    fn assert_send_and_sync<T: Send + Sync>() {}
    assert_send_and_sync::<ProgramCompiler>();

    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let program = compiler
        .compile(
            compilation_deadline(),
            r#"
                globalThis.programMutation =
                    (globalThis.programMutation ?? 0) + 1;
                if (
                    globalThis.programMutation !== 1 ||
                    typeof globalThis.ts !== "undefined" ||
                    typeof globalThis.__coralCompile !== "undefined" ||
                    typeof globalThis.__coralForbiddenAmbientGlobals !== "undefined"
                ) {
                    throw new Error("mutable isolate state leaked into execution");
                }
            "#,
        )
        .expect("compile state probe");
    let executor = ProgramExecutor::new(IsolateLimits::default());

    executor.execute(&program).expect("first execution");
    executor.execute(&program).expect("second execution");
}

#[test]
fn program_cannot_replace_the_host_observed_entry_promise() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let program = compiler
        .compile(
            compilation_deadline(),
            r#"
                Object.defineProperty(globalThis, "__coralProgram", {
                    configurable: true,
                    get() { return Promise.resolve("fabricated success"); },
                    set(_actualPromise) {},
                });
                throw new Error("real Program rejection");
            "#,
        )
        .expect("compile tampering probe");

    let error = ProgramExecutor::new(IsolateLimits::default())
        .execute(&program)
        .expect_err("the actual entry Promise must reject");

    assert!(matches!(
        error,
        Error::ProgramRejected(message)
            if message.message == "real Program rejection"
                && message.to_string().contains("at Program:7:")
    ));
}

#[derive(Default)]
struct CapturingToolHost {
    calls: Mutex<Vec<(String, String)>>,
}

impl ProgramHost for CapturingToolHost {
    fn invoke(
        self: Arc<Self>,
        call_path: String,
        arguments_json: String,
        _location: Option<AuthoredLocation>,
    ) -> coral_v8::ToolFuture {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((call_path, arguments_json));
        Box::pin(async { Ok(r#"{"data":null,"metadata":{"kind":"http"}}"#.to_string()) })
    }

    fn emit(
        &self,
        _value_json: &str,
        _location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        Err(ProgramOutputHostError::Fatal)
    }

    fn log(
        &self,
        _level: ConsoleLevel,
        _message: &str,
        _location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        Err(ProgramOutputHostError::Fatal)
    }
}

#[test]
fn tool_call_passes_one_logical_argument_object_to_the_host() {
    let program = ProgramCompiler::new(IsolateLimits::default())
        .expect("start compiler")
        .compile(
            compilation_deadline(),
            r#"
                return await coral.providers.widgets.create({
                    id: "widget/1",
                    page: 2,
                    body: { label: "example" },
                });
            "#,
        )
        .expect("compile Tool argument probe");
    let host = Arc::new(CapturingToolHost::default());

    ProgramExecutor::new(IsolateLimits::default())
        .execute_with_host(&program, host.clone())
        .expect("execute Tool argument probe");

    assert_eq!(
        *host.calls.lock().expect("captured Tool calls"),
        vec![(
            "coral.providers.widgets.create".to_string(),
            r#"{"id":"widget/1","page":2,"body":{"label":"example"}}"#.to_string()
        )]
    );
}

struct RejectingToolHost;

impl ProgramHost for RejectingToolHost {
    fn invoke(
        self: Arc<Self>,
        call_path: String,
        _arguments_json: String,
        _location: Option<AuthoredLocation>,
    ) -> coral_v8::ToolFuture {
        Box::pin(async move {
            Err(ToolHostError::Rejected {
                code: "upstream_incomplete",
                message: "durable Tool failure".into(),
                trace_id: call_path
                    .ends_with(".traced")
                    .then(|| "0123456789abcdef0123456789abcdef".to_string()),
            })
        })
    }

    fn emit(
        &self,
        _value_json: &str,
        _location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        Err(ProgramOutputHostError::Fatal)
    }

    fn log(
        &self,
        _level: ConsoleLevel,
        _message: &str,
        _location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        Err(ProgramOutputHostError::Fatal)
    }
}

/// Rejects every call with one complete upstream outcome.
struct UpstreamRejectingHost {
    outcome_json: String,
}

impl UpstreamRejectingHost {
    fn new(outcome: &serde_json::Value) -> Arc<Self> {
        Arc::new(Self {
            outcome_json: outcome.to_string(),
        })
    }
}

impl ProgramHost for UpstreamRejectingHost {
    fn invoke(
        self: Arc<Self>,
        _call_path: String,
        _arguments_json: String,
        _location: Option<AuthoredLocation>,
    ) -> coral_v8::ToolFuture {
        Box::pin(async move {
            Err(ToolHostError::Upstream {
                code: "error_response",
                message: "The upstream service returned an error",
                outcome_json: self.outcome_json.clone(),
            })
        })
    }

    fn emit(
        &self,
        _value_json: &str,
        _location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        Err(ProgramOutputHostError::Fatal)
    }

    fn log(
        &self,
        _level: ConsoleLevel,
        _message: &str,
        _location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        Err(ProgramOutputHostError::Fatal)
    }
}

#[test]
fn an_upstream_tool_error_carries_its_outcome_read_only() {
    let program = ProgramCompiler::new(IsolateLimits::default())
        .expect("compiler")
        .compile(
            compilation_deadline(),
            r#"
            try {
                await coral.providers.weather.getForecast();
                return "unexpected resolution";
            } catch (error) {
                const replaceable = ["status", "data", "metadata"].filter((name) => {
                    const field = Object.getOwnPropertyDescriptor(error, name);
                    return field !== undefined && (field.writable || field.configurable);
                });
                return {
                    isToolError: error instanceof coral.ToolError,
                    code: error.code,
                    status: error.status ?? null,
                    data: error.data,
                    metadata: error.metadata,
                    fields: Object.getOwnPropertyNames(error).sort(),
                    replaceable,
                };
            }
            "#,
        )
        .expect("compile");
    let output = ProgramExecutor::new(IsolateLimits::default())
        .execute_with_host(
            &program,
            UpstreamRejectingHost::new(&serde_json::json!({
                "status": 404,
                "data": {"message": "missing"},
                "metadata": {"kind": "http", "mediaType": "application/json"},
            })),
        )
        .expect("a caught upstream error lets the Program finish");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(output.final_value_json().expect("value"))
            .expect("JSON"),
        serde_json::json!({
            "isToolError": true,
            "code": "error_response",
            "status": 404,
            "data": {"message": "missing"},
            "metadata": {"kind": "http", "mediaType": "application/json"},
            "fields": ["code", "data", "message", "metadata", "stack", "status"],
            "replaceable": [],
        })
    );

    // An MCP outcome has no HTTP status.
    let output = ProgramExecutor::new(IsolateLimits::default())
        .execute_with_host(
            &program,
            UpstreamRejectingHost::new(&serde_json::json!({
                "data": {"content": [], "isError": true},
                "metadata": {"kind": "mcp"},
            })),
        )
        .expect("handled");
    let value: serde_json::Value =
        serde_json::from_str(output.final_value_json().expect("value")).expect("JSON");
    assert_eq!(value.get("status"), Some(&serde_json::Value::Null));
    assert_eq!(
        value.get("fields").expect("fields"),
        &serde_json::json!(["code", "data", "message", "metadata", "stack"])
    );
}

#[test]
fn an_uncaught_error_response_retains_its_status_and_a_bounded_body() {
    let rejected = |data: serde_json::Value, source: &str| {
        let program = ProgramCompiler::new(IsolateLimits::default())
            .expect("compiler")
            .compile(compilation_deadline(), source)
            .expect("compile");
        let error = ProgramExecutor::new(IsolateLimits::default())
            .execute_with_host(
                &program,
                UpstreamRejectingHost::new(&serde_json::json!({
                    "status": 422, "data": data, "metadata": {"kind": "http"},
                })),
            )
            .expect_err("an uncaught upstream error fails the Program");
        let Error::ProgramRejected(error) = error else {
            panic!("{error}");
        };
        error.tool.expect("trusted Tool fields")
    };

    let tool = rejected(
        serde_json::json!({"field": "city"}),
        "return await coral.providers.weather.getForecast();",
    );
    assert_eq!(
        tool,
        coral_v8::ProgramToolError {
            code: "error_response".to_string(),
            trace_id: None,
            status: Some(422),
            body: Some(r#"{"field":"city"}"#.to_string()),
            body_truncated: false,
        }
    );

    // A caught and rethrown error still reaches the uncaught path with the
    // outcome the host retained.
    let tool = rejected(
        serde_json::json!({"field": "city"}),
        r#"try { await coral.providers.weather.getForecast(); }
           catch (error) {
             try { Object.defineProperty(error, "data", {value: "forged"}); } catch {}
             throw error;
           }"#,
    );
    assert_eq!(tool.body.as_deref(), Some(r#"{"field":"city"}"#));

    let tool = rejected(
        serde_json::json!("é".repeat(4096)),
        "return await coral.providers.weather.getForecast();",
    );
    let body = tool.body.expect("body excerpt");
    assert!(tool.body_truncated);
    assert!(body.len() <= 1024, "{}", body.len());
    assert!(body.starts_with("\"é"));
}

struct FatalToolHost;

impl ProgramHost for FatalToolHost {
    fn invoke(
        self: Arc<Self>,
        _call_path: String,
        _arguments_json: String,
        _location: Option<AuthoredLocation>,
    ) -> coral_v8::ToolFuture {
        Box::pin(async { Err(ToolHostError::Fatal) })
    }

    fn emit(
        &self,
        _value_json: &str,
        _location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        Err(ProgramOutputHostError::Fatal)
    }

    fn log(
        &self,
        _level: ConsoleLevel,
        _message: &str,
        _location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        Err(ProgramOutputHostError::Fatal)
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one table of every authored frame shape"
)]
fn failure_stacks_name_only_authored_locations() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("compiler");
    let fail = linked_source(
        &["debug", "fail"],
        "\ninterface Marker { value: number }\n\nexport default function fail(): never {\n\n    throw new Error('private linked canary');\n}",
    );
    let failure = linked_source(
        &["debug", "failure"],
        "export default function failure(): never { throw new Error('authored dependency canary'); }",
    );
    let caller = linked_source(
        &["debug", "caller"],
        "export default function caller(): never { return coral.functions.debug.failure(); }",
    );
    let fetch = linked_source(
        &["debug", "fetch"],
        "export default async function fetch() {\n    await coral.providers.weather.getForecast();\n}",
    );
    // (Program, its Saved Functions, the failure's message, its authored frames)
    let cases: [(&str, Vec<LinkedSavedFunctionSource>, &str, &[&str]); 10] = [
        (
            "const meaning: number = 42;\nvoid meaning;\nthrow new Error(\"private rejection canary\");",
            vec![],
            "Error: private rejection canary\n",
            &["at Program:3:7"],
        ),
        // Final Value serialization runs authored code too.
        (
            "return {\n    toJSON() {\n        throw new Error(\"private serialization canary\");\n    },\n};",
            vec![],
            "Error: private serialization canary\n",
            &["at Program:3:"],
        ),
        (
            "return coral.functions.debug.fail();",
            vec![fail],
            "Error: private linked canary\n",
            &[
                "at Saved Function coral.functions.debug.fail:6:",
                "at Program:1:",
            ],
        ),
        (
            "return coral.functions.debug.caller();",
            vec![failure, caller],
            "Error: authored dependency canary\n",
            &[
                "at Saved Function coral.functions.debug.failure:1:",
                "at Saved Function coral.functions.debug.caller:1:",
                "at Program:1:",
            ],
        ),
        // Tool rejections settle asynchronously but keep their call sites.
        (
            "\nawait coral.providers.weather.getForecast();",
            vec![],
            "",
            &["at Program:2:"],
        ),
        // Saved Function source without semicolons keeps its frames.
        (
            "return coral.functions.debug.asi();",
            vec![linked_source(
                &["debug", "asi"],
                "export default () => {\n  const reason = 'asi canary'\n  throw new Error(reason)\n}",
            )],
            "Error: asi canary\n",
            &[
                "at Saved Function coral.functions.debug.asi:3:",
                "at Program:1:",
            ],
        ),
        // An error frame inside a Saved Function statement with a trailing
        // comma keeps its line.
        (
            "return coral.functions.debug.comma();",
            vec![linked_source(
                &["debug", "comma"],
                "export default function () {\n  throw new Error('comma canary', {\n    cause: 1,\n  },)\n}",
            )],
            "Error: comma canary\n",
            &[
                "at Saved Function coral.functions.debug.comma:2:",
                "at Program:1:",
            ],
        ),
        // Frames count ECMAScript line terminators, not only LF.
        (
            "const meaning = 1;\r\u{2028}throw new Error(\"terminator canary\");",
            vec![],
            "Error: terminator canary\n",
            &["at Program:3:7"],
        ),
        (
            "Error.stackTraceLimit = 0;\nError.prepareStackTrace = () => 'spoofed';\nawait coral.providers.weather.getForecast();",
            vec![],
            "",
            &["at Program:3:"],
        ),
        (
            "return await coral.functions.debug.fetch();",
            vec![fetch],
            "",
            &[
                "at Saved Function coral.functions.debug.fetch:2:",
                "at Program:1:",
            ],
        ),
    ];
    for (source, functions, message, frames) in cases {
        let program = compiler
            .compile_with_saved_functions(compilation_deadline(), source, &functions)
            .expect(source);
        let error = ProgramExecutor::new(IsolateLimits::default())
            .execute_with_host(&program, Arc::new(RejectingToolHost))
            .expect_err(source);
        let (Error::ProgramRejected(diagnostic) | Error::ProgramOutputRejected(diagnostic)) = error
        else {
            panic!("{source} returned {error}");
        };
        let diagnostic = diagnostic.to_string();
        assert!(diagnostic.starts_with(message), "{diagnostic}");
        for frame in frames {
            assert!(diagnostic.contains(frame), "missing {frame}: {diagnostic}");
        }
        for private in ["program.js", "program.ts", "coral_", "async"] {
            assert!(!diagnostic.contains(private), "{diagnostic}");
        }
    }
}

#[test]
fn rejected_tool_host_failure_is_catchable() {
    let program = ProgramCompiler::new(IsolateLimits::default())
        .expect("start compiler")
        .compile(
            compilation_deadline(),
            r#"
                try {
                    await coral.providers.weather.getForecast();
                    return "unexpected resolution";
                } catch (error) {
                    let constructible = false;
                    try { new coral.ToolError(); constructible = true; } catch {}
                    return {
                        isError: error instanceof Error,
                        isToolError: error instanceof coral.ToolError,
                        name: error.name,
                        code: error.code,
                        message: error.message,
                        traceId: error.traceId,
                        serialized: JSON.stringify(error),
                        fields: Object.getOwnPropertyNames(error).sort(),
                        constructible,
                    };
                }
            "#,
        )
        .expect("compile durable Tool rejection probe");

    let output = ProgramExecutor::new(IsolateLimits::default())
        .execute_with_host(&program, Arc::new(RejectingToolHost))
        .expect("durable host rejection remains catchable");

    assert_eq!(
        serde_json::from_str::<serde_json::Value>(output.final_value_json().expect("value"))
            .expect("JSON"),
        serde_json::json!({
            "isError": true,
            "isToolError": true,
            "name": "ToolError",
            "code": "upstream_incomplete",
            "message": "durable Tool failure",
            "serialized": "{}",
            "fields": ["code", "message", "stack"],
            "constructible": false,
        })
    );
}

#[test]
fn tool_error_preserves_optional_trace_and_resists_program_replacement() {
    let program = ProgramCompiler::new(IsolateLimits::default())
        .expect("compiler")
        .compile(
            compilation_deadline(),
            r#"
        const ToolError = coral.ToolError;
        for (const replace of [
            () => { coral.ToolError = Error; },
            () => { ToolError.prototype.name = "forged"; },
            () => { ToolError.prototype.toJSON = () => "forged"; },
            () => { Object.setPrototypeOf(ToolError, null); },
        ]) { try { replace(); } catch {} }
        globalThis.Error = function() { throw "replaced"; };
        Object.setPrototypeOf = () => { throw "replaced"; };
        Object.defineProperties = () => { throw "replaced"; };
        Object.prototype.get = function () {};
        try { await coral.providers.weather.traced(); }
        catch (error) {
            return {name: error.name, isToolError: error instanceof ToolError,
                code: error.code, traceId: error.traceId, serialized: JSON.stringify(error)};
        }
    "#,
        )
        .expect("compile");
    let output = ProgramExecutor::new(IsolateLimits::default())
        .execute_with_host(&program, Arc::new(RejectingToolHost))
        .expect("handled");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(output.final_value_json().expect("value"))
            .expect("JSON"),
        serde_json::json!({
            "name": "ToolError", "isToolError": true, "code": "upstream_incomplete",
            "traceId": "0123456789abcdef0123456789abcdef", "serialized": "{}"
        })
    );
}

#[test]
fn unhandled_tool_errors_fail_execution_even_without_await() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("compiler");
    for source in [
        "coral.providers.weather.getForecast(); return 42;",
        "return {toJSON() { coral.providers.weather.getForecast(); return 42; }};",
    ] {
        let program = compiler
            .compile(compilation_deadline(), source)
            .expect("compile");
        assert!(
            matches!(
                ProgramExecutor::new(IsolateLimits::default())
                    .execute_with_host(&program, Arc::new(RejectingToolHost)),
                Err(Error::ProgramRejected(_))
            ),
            "{source}"
        );
    }
}

#[test]
fn invalid_tool_host_values_reject_without_invoking_the_host() {
    let program = ProgramCompiler::new(IsolateLimits::default()).expect("compiler").compile(compilation_deadline(), r#"
        const cycle = {}; cycle.self = cycle;
        const calls = [() => coral.providers.widgets.create(undefined),
            () => coral.providers.widgets.create(1n),
            () => coral.providers.widgets.create(cycle),
            () => coral.providers.widgets.create({}, {})];
        for (const call of calls) {
            try { await call(); throw Error("unexpected resolution"); }
            catch (error) {
                if (!(error instanceof coral.ToolError) || error.code !== "invalid_input") throw error;
            }
        }
        return "handled";
    "#).expect("compile invalid values");
    let host = Arc::new(CapturingToolHost::default());
    let result = ProgramExecutor::new(IsolateLimits::default())
        .execute_with_host(&program, host.clone())
        .expect("catch invalid input");
    assert_eq!(result.final_value_json(), Some(r#""handled""#));
    assert!(host.calls.lock().expect("calls").is_empty());
}

#[test]
fn fatal_tool_host_failure_during_final_value_serialization_stays_fatal() {
    let program = ProgramCompiler::new(IsolateLimits::default())
        .expect("start compiler")
        .compile(
            compilation_deadline(),
            r#"
                return {
                    toJSON() {
                        try {
                            coral.providers.weather.getForecast();
                            return "unexpected resolution";
                        } catch {
                            return "unexpected catch";
                        }
                    },
                };
            "#,
        )
        .expect("compile final serialization host failure probe");

    let error = ProgramExecutor::new(IsolateLimits::default())
        .execute_with_host(&program, Arc::new(FatalToolHost))
        .expect_err("fatal host failure during serialization must terminate execution");

    assert!(matches!(error, Error::ProgramHostTerminated));
}

#[test]
fn fatal_tool_host_failure_cannot_be_caught_by_the_program() {
    let program = ProgramCompiler::new(IsolateLimits::default())
        .expect("start compiler")
        .compile(
            compilation_deadline(),
            r#"
                try {
                    await coral.providers.weather.getForecast();
                    return "unexpected resolution";
                } catch {
                    return "unexpected catch";
                }
            "#,
        )
        .expect("compile Tool host failure probe");

    let error = ProgramExecutor::new(IsolateLimits::default())
        .execute_with_host(&program, Arc::new(FatalToolHost))
        .expect_err("fatal host failure must terminate Program execution");

    assert!(matches!(error, Error::ProgramHostTerminated));
}

#[test]
fn source_cannot_escape_the_private_async_wrapper() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let escapes = [
        (
            "two top-level IIFEs",
            r#"
                throw new Error("hidden rejection");
            })();
            (async () => {
            "#,
        ),
        (
            "one top-level comma expression",
            r#"
                throw new Error("hidden rejection");
            })(), (async () => {
            "#,
        ),
    ];

    for (case, source) in escapes {
        let error = compiler
            .compile(compilation_deadline(), source)
            .expect_err("source must not escape the private wrapper");
        assert!(
            matches!(&error, Error::InvalidProgramBody),
            "{case} returned {error}"
        );
    }
}

#[test]
fn a_single_microtask_checkpoint_drains_deep_continuations() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let chains = [
        (
            "sequential awaits",
            r"
                let value = 0;
                for (let step = 0; step < 5000; step += 1) {
                    value = await Promise.resolve(value + 1);
                }
                return value;
            ",
        ),
        (
            "chained then callbacks",
            r"
                let chain = Promise.resolve(0);
                for (let step = 0; step < 5000; step += 1) {
                    chain = chain.then((value) => value + 1);
                }
                return await chain;
            ",
        ),
    ];
    let executor = ProgramExecutor::new(IsolateLimits::default());

    for (case, source) in chains {
        let program = compiler
            .compile(compilation_deadline(), source)
            .expect("compile deep chain");
        let output = executor.execute(&program).expect("execute deep chain");
        assert_eq!(
            output.final_value_json(),
            Some("5000"),
            "{case} did not settle in one checkpoint"
        );
    }
}

#[derive(Default)]
struct ToolGate {
    state: Mutex<(bool, Option<std::task::Waker>)>,
}

impl ToolGate {
    fn finish(&self) {
        let mut state = self.state.lock().expect("gate");
        state.0 = true;
        if let Some(waker) = state.1.take() {
            waker.wake();
        }
    }
}

struct GatedToolHost {
    started: std::sync::mpsc::Sender<Arc<ToolGate>>,
}

impl ProgramHost for GatedToolHost {
    fn invoke(
        self: Arc<Self>,
        _path: String,
        _arguments: String,
        _location: Option<AuthoredLocation>,
    ) -> coral_v8::ToolFuture {
        let gate = Arc::new(ToolGate::default());
        self.started
            .send(gate.clone())
            .expect("report started Tool");
        Box::pin(std::future::poll_fn(move |context| {
            let mut state = gate.state.lock().expect("gate");
            if state.0 {
                std::task::Poll::Ready(Ok("{\"data\":42,\"metadata\":{}}".to_string()))
            } else {
                state.1 = Some(context.waker().clone());
                std::task::Poll::Pending
            }
        }))
    }

    fn emit(
        &self,
        _json: &str,
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

#[test]
fn tools_progress_concurrently_and_unawaited_work_is_drained() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("compiler");
    for source in [
        "const results = await Promise.all([coral.providers.test.first(), coral.providers.test.second()]); return results[0].data + results[1].data;",
        "coral.providers.test.first(); coral.providers.test.second(); return 84;",
        "return { toJSON() { coral.providers.test.first(); coral.providers.test.second(); return 84; } };",
    ] {
        let program = compiler
            .compile(compilation_deadline(), source)
            .expect("compile");
        let (started, calls) = std::sync::mpsc::channel();
        let (completed, completion) = std::sync::mpsc::channel();
        let control = ProgramExecutionControl::new();
        let worker_control = control.clone();
        let worker = std::thread::spawn(move || {
            let result = ProgramExecutor::new(IsolateLimits::default()).execute_with_host_control(
                &program,
                Arc::new(GatedToolHost { started }),
                &worker_control,
            );
            completed.send(result).expect("send completion");
        });
        let first = calls.recv_timeout(std::time::Duration::from_secs(2));
        let second = calls.recv_timeout(std::time::Duration::from_secs(2));
        if first.is_err() || second.is_err() {
            let _installed = control.terminate();
        }
        let first = first.expect("first Tool starts");
        let second = second.expect("second Tool starts before the first completes");
        second.finish();
        assert!(
            completion.try_recv().is_err(),
            "unawaited work must finish before the Run"
        );
        first.finish();
        let output = completion
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("execution settles")
            .expect("Program succeeds");
        worker.join().expect("worker");
        assert_eq!(output.final_value_json(), Some("84"));
    }
}

#[test]
fn cancellation_wakes_execution_waiting_for_a_tool() {
    let program = ProgramCompiler::new(IsolateLimits::default())
        .expect("compiler")
        .compile(compilation_deadline(), "await coral.providers.test.wait();")
        .expect("compile");
    let (started, calls) = std::sync::mpsc::channel();
    let (completed, completion) = std::sync::mpsc::channel();
    let control = ProgramExecutionControl::new();
    let worker_control = control.clone();
    let worker = std::thread::spawn(move || {
        completed
            .send(
                ProgramExecutor::new(IsolateLimits::default()).execute_with_host_control(
                    &program,
                    Arc::new(GatedToolHost { started }),
                    &worker_control,
                ),
            )
            .expect("send completion");
    });
    let _pending = calls
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("Tool started");
    let _installed = control.terminate();
    let result = completion
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("termination wakes executor");
    worker.join().expect("worker");
    assert!(
        matches!(result, Err(Error::ProgramHostTerminated)),
        "{result:?}"
    );
}

#[test]
fn a_program_awaiting_an_absent_event_source_is_pending_rather_than_hanging() {
    let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("start compiler");
    let program = compiler
        .compile(
            compilation_deadline(),
            "await new Promise(() => {}); return 1;",
        )
        .expect("compile never-settling await");

    let error = ProgramExecutor::new(IsolateLimits::default())
        .execute(&program)
        .expect_err("a never-settling Program must not produce a Final Value");
    assert!(matches!(&error, Error::PendingProgram), "{error}");
}

/// Records Program output and Tool calls under the given value limits.
struct RecordingHost {
    limits: coral_value::ValueLimits,
    emitted: Mutex<Vec<String>>,
    logs: Mutex<Vec<(ConsoleLevel, String)>>,
    calls: Mutex<Vec<String>>,
    /// Every host call in arrival order: its kind and authored location.
    locations: Mutex<Vec<(&'static str, Option<AuthoredLocation>)>>,
}

/// Each value crossing to the host may encode to at most this many bytes.
const HOST_BYTES: usize = 128;

impl RecordingHost {
    fn new(limits: coral_value::ValueLimits) -> Arc<Self> {
        Arc::new(Self {
            limits,
            emitted: Mutex::default(),
            logs: Mutex::default(),
            calls: Mutex::default(),
            locations: Mutex::default(),
        })
    }

    fn record(&self, kind: &'static str, location: Option<AuthoredLocation>) {
        self.locations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((kind, location));
    }
}

impl ProgramHost for RecordingHost {
    fn value_limits(&self) -> coral_value::ValueLimits {
        self.limits
    }

    fn invoke(
        self: Arc<Self>,
        _call_path: String,
        arguments_json: String,
        location: Option<AuthoredLocation>,
    ) -> coral_v8::ToolFuture {
        self.record("invoke", location);
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(arguments_json);
        Box::pin(async { Ok(r#"{"data":null,"metadata":{"kind":"http"}}"#.to_string()) })
    }

    fn emit(
        &self,
        value_json: &str,
        location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        self.record("emit", location);
        self.emitted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(value_json.to_string());
        Ok(())
    }

    fn log(
        &self,
        level: ConsoleLevel,
        message: &str,
        location: Option<AuthoredLocation>,
    ) -> Result<(), ProgramOutputHostError> {
        self.record("log", location);
        self.logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((level, message.to_string()));
        Ok(())
    }
}

fn run_with_recording_host(source: &str) -> (Option<String>, Arc<RecordingHost>) {
    run_linked_with_recording_host(source, &[])
}

fn run_linked_with_recording_host(
    source: &str,
    functions: &[LinkedSavedFunctionSource],
) -> (Option<String>, Arc<RecordingHost>) {
    let program = ProgramCompiler::new(IsolateLimits::default())
        .expect("start compiler")
        .compile_with_saved_functions(compilation_deadline(), source, functions)
        .expect("compile output probe");
    let host = RecordingHost::new(
        coral_value::ValueLimits::new(1024, HOST_BYTES, 1024, 1024).expect("limits"),
    );
    let output = ProgramExecutor::new(IsolateLimits::default())
        .execute_with_host(&program, host.clone())
        .expect("execute output probe");
    (output.final_value_json().map(str::to_string), host)
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "table rows compare the optional location each host call receives"
)]
fn at(line: u32, column: u32) -> Option<AuthoredLocation> {
    Some(AuthoredLocation {
        source: AuthoredSource::Program,
        line,
        column,
    })
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "table rows compare the optional location each host call receives"
)]
fn in_saved_function(path: &str, line: u32, column: u32) -> Option<AuthoredLocation> {
    Some(AuthoredLocation {
        source: AuthoredSource::SavedFunction {
            path: path.to_string(),
        },
        line,
        column,
    })
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one table of every call-site shape the location rule covers"
)]
fn host_calls_carry_the_authored_location_of_each_call() {
    let helpers = linked_source(
        &["debug", "fetch"],
        "console.info(\"linked\");\n\nexport default async function fetch(id: number) {\n    return coral.providers.demo.getItem({ id });\n}",
    );
    // (Program, Saved Functions, every host call in arrival order)
    #[expect(clippy::type_complexity, reason = "one readable table of cases")]
    let cases: Vec<(
        &str,
        Vec<LinkedSavedFunctionSource>,
        Vec<(&str, Option<AuthoredLocation>)>,
    )> = vec![
        // One call per line: the column is the called name, after type syntax
        // that TypeScript removes.
        (
            "const label: string = \"x\";\nawait coral.emit({ label });\nconsole.warn(label);\nawait coral.providers.demo.getItem({});",
            vec![],
            vec![
                ("emit", at(2, 13)),
                ("log", at(3, 9)),
                ("invoke", at(4, 28)),
            ],
        ),
        // The same call site in a loop records the same location each time.
        (
            "for (let index: number = 0; index < 3; index += 1) {\n    console.log(index);\n}",
            vec![],
            vec![("log", at(2, 13)), ("log", at(2, 13)), ("log", at(2, 13))],
        ),
        // Parallel calls through a helper record the helper's call site, and
        // each Promise.all member keeps its own invocation.
        (
            "async function one(id: number): Promise<unknown> {\n    return coral.providers.demo.getItem({ id });\n}\nawait Promise.all([one(1), one(2)]);\nawait coral.providers.demo.getItem({}); await coral.providers.demo.getItem({});",
            vec![],
            vec![
                ("invoke", at(2, 33)),
                ("invoke", at(2, 33)),
                ("invoke", at(5, 28)),
                ("invoke", at(5, 68)),
            ],
        ),
        // After an await, the continuation keeps its own call site.
        (
            "await Promise.resolve();\n  coral.emit(1);",
            vec![],
            vec![("emit", at(2, 9))],
        ),
        // UTF-16 code units: the astral character before the call counts as two.
        (
            "const s = \"\u{1F600}\"; console.log(s);",
            vec![],
            vec![("log", at(1, 25))],
        ),
        // A call inside a Saved Function names that Saved Function's source,
        // including its top-level initialization before the Program body.
        (
            "\nawait coral.functions.debug.fetch(7);",
            vec![helpers],
            vec![
                ("log", in_saved_function("debug.fetch", 1, 9)),
                ("invoke", in_saved_function("debug.fetch", 4, 33)),
            ],
        ),
        // Lines follow ECMAScript line terminators: CRLF counts once, and a lone
        // CR, U+2028 or U+2029 ends a line, even inside a string literal.
        (
            "console.log(1);\r\nconsole.log(2);",
            vec![],
            vec![("log", at(1, 9)), ("log", at(2, 9))],
        ),
        (
            "console.log(1);\rconsole.log(2);",
            vec![],
            vec![("log", at(1, 9)), ("log", at(2, 9))],
        ),
        (
            "console.log(1);\u{2028}console.log(2);\u{2029}console.log(3);",
            vec![],
            vec![("log", at(1, 9)), ("log", at(2, 9)), ("log", at(3, 9))],
        ),
        (
            "const s = \"a\u{2028}b\"; console.log(s);\nconsole.log(s);",
            vec![],
            vec![("log", at(2, 13)), ("log", at(3, 9))],
        ),
        (
            "coral.functions.debug.mixed();\r\nconsole.log(0);",
            vec![linked_source(
                &["debug", "mixed"],
                "console.info(1);\rexport default function mixed() {\r\n    console.warn(\"a\u{2029}b\");\r}",
            )],
            vec![
                ("log", in_saved_function("debug.mixed", 1, 9)),
                ("log", in_saved_function("debug.mixed", 3, 13)),
                ("log", at(2, 9)),
            ],
        ),
        // Saved Function lines follow the same terminators. A private line the
        // linker writes after a lone CR, U+2028 or U+2029 adds no line of its own
        // to the authored count.
        (
            "coral.functions.terminators.lone()",
            vec![linked_source(
                &["terminators", "lone"],
                "console.info(1)\rexport default (() => {\u{2028}  console.log(2)\u{2029}})\r",
            )],
            vec![
                ("log", in_saved_function("terminators.lone", 1, 9)),
                ("log", in_saved_function("terminators.lone", 3, 11)),
            ],
        ),
        (
            "coral.functions.terminators.clause()",
            vec![linked_source(
                &["terminators", "clause"],
                "export {}\u{2028}console.info('a\u{2029}b')\u{2029}const run = () => console.log(1)\rexport { run as default }",
            )],
            vec![
                ("log", in_saved_function("terminators.clause", 2, 9)),
                ("log", in_saved_function("terminators.clause", 4, 27)),
            ],
        ),
        // A trailing CR ends the last authored line; nothing maps past it.
        (
            "console.debug(1); console.error(2);\r",
            vec![],
            vec![("log", at(1, 9)), ("log", at(1, 27))],
        ),
        // A binding passed to a builtin records the authored call into the builtin.
        (
            "\n[1].forEach(console.log);",
            vec![],
            vec![("log", at(2, 5))],
        ),
        // Calls inside Saved Function source without semicolons keep their lines.
        (
            "await coral.functions.locations.notify()",
            vec![linked_source(
                &["locations", "notify"],
                "export default async () => {\n  await coral.providers.locationFixture.lookup({})\n  console.warn('accepted-snapshot-notice')\n  return 1\n}",
            )],
            vec![
                ("invoke", in_saved_function("locations.notify", 2, 41)),
                ("log", in_saved_function("locations.notify", 3, 11)),
            ],
        ),
        (
            "coral.functions.locations.named()",
            vec![linked_source(
                &["locations", "named"],
                "const prefix = 'n'\nexport default function named() {\n  console.info(prefix)\n  coral.emit(prefix);;\n}",
            )],
            vec![
                ("log", in_saved_function("locations.named", 3, 11)),
                ("emit", in_saved_function("locations.named", 4, 9)),
            ],
        ),
        (
            "coral.functions.locations.implicit(); coral.functions.locations.guarded(true); coral.functions.locations.boxed()",
            vec![
                linked_source(
                    &["locations", "implicit"],
                    "export default () => console.log('implicit')",
                ),
                linked_source(
                    &["locations", "guarded"],
                    "export default function guarded(flag: boolean) {\n  // a comment\n  if (!flag) return\n  /* block */ console.debug('after')\n}",
                ),
                linked_source(
                    &["locations", "boxed"],
                    "class Box {\n  value = 1\n  read() { return this.value }\n}\nexport default () => { console.log(new Box().read()) }",
                ),
            ],
            vec![
                ("log", in_saved_function("locations.implicit", 1, 30)),
                ("log", in_saved_function("locations.guarded", 4, 23)),
                ("log", in_saved_function("locations.boxed", 5, 32)),
            ],
        ),
        // A template or regular expression earlier on the line cannot shift a
        // later call's column.
        (
            "coral.functions.locations.lexical()",
            vec![linked_source(
                &["locations", "lexical"],
                "type Pair = { a: string, b: number }\nexport default () => {\n  foo(`${1}`,   coral.emit(1))\n  foo(/'/,   coral.emit(2))\n  ;[1].forEach(() => coral.emit(3))\n  console.log('after')\n}\nfunction foo(..._values: unknown[]) {}",
            )],
            vec![
                ("emit", in_saved_function("locations.lexical", 3, 23)),
                ("emit", in_saved_function("locations.lexical", 4, 20)),
                ("emit", in_saved_function("locations.lexical", 5, 28)),
                ("log", in_saved_function("locations.lexical", 6, 11)),
            ],
        ),
        (
            "coral.functions.locations.documented(`${1}`)",
            vec![linked_source(
                &["locations", "documented"],
                "/** Logs one value. */\nexport default function documented(value: string) {\n  console.log(`value ${value}`, /x/g.test(value))\n}",
            )],
            vec![("log", in_saved_function("locations.documented", 3, 11))],
        ),
        // A trailing comma later in a Saved Function statement leaves every
        // earlier call in it mapped.
        (
            "coral.functions.locations.reprinted()",
            vec![linked_source(
                &["locations", "reprinted"],
                "export default async function () {\n  console.log('a', {\n    b: 1,\n  },)\n}",
            )],
            vec![("log", in_saved_function("locations.reprinted", 2, 11))],
        ),
        // Export syntax spread over lines and comments leaves later lines in place.
        (
            "coral.functions.locations.multiline()",
            vec![linked_source(
                &["locations", "multiline"],
                "export /* keyword */ default\n  // the function\n  async () => {\n    console.log('multiline')\n  }",
            )],
            vec![("log", in_saved_function("locations.multiline", 4, 13))],
        ),
        (
            "coral.functions.locations.generated().next()",
            vec![linked_source(
                &["locations", "generated"],
                "export default function* () { console.log('gen'); yield 1 }",
            )],
            vec![("log", in_saved_function("locations.generated", 1, 39))],
        ),
        // A call inside the default expression itself keeps its column.
        (
            "coral.functions.locations.initialized()",
            vec![linked_source(
                &["locations", "initialized"],
                "export default (console.log('init'), () => {})",
            )],
            vec![("log", in_saved_function("locations.initialized", 1, 25))],
        ),
        // A removed export clause still ends the statement before it.
        (
            "coral.functions.locations.clause()",
            vec![linked_source(
                &["locations", "clause"],
                "console.info('first')\nexport {}\n(() => console.log('second'))()\nconst clause = () => {}\nexport { clause as default }",
            )],
            vec![
                ("log", in_saved_function("locations.clause", 1, 9)),
                ("log", in_saved_function("locations.clause", 3, 16)),
            ],
        ),
        (
            "coral.functions.locations.shebang()",
            vec![linked_source(
                &["locations", "shebang"],
                "#!/usr/bin/env coral\n'use strict'\nexport default () => console.log('shebang')",
            )],
            vec![("log", in_saved_function("locations.shebang", 3, 30))],
        ),
        // The private default name cannot collide with an authored binding,
        // even one spelled with an escape.
        (
            "coral.functions.locations.hygiene()",
            vec![linked_source(
                &["locations", "hygiene"],
                "const \\u005f_coralSavedFunctionDefaultExport_1 = 1\nexport default () => console.log(__coralSavedFunctionDefaultExport_1)",
            )],
            vec![("log", in_saved_function("locations.hygiene", 2, 30))],
        ),
        (
            "await coral.functions.locations.agen().next()",
            vec![linked_source(
                &["locations", "agen"],
                "export default async function* () { console.log('agen'); yield 1 }",
            )],
            vec![("log", in_saved_function("locations.agen", 1, 45))],
        ),
        // Several export removals on one line keep the rest of the line in place.
        (
            "coral.functions.locations.removals()",
            vec![linked_source(
                &["locations", "removals"],
                "const one = () => console.log('one'); export {}; export { one as default }; console.info('tail')",
            )],
            vec![
                ("log", in_saved_function("locations.removals", 1, 85)),
                ("log", in_saved_function("locations.removals", 1, 27)),
            ],
        ),
        (
            "coral.functions.locations.template()",
            vec![linked_source(
                &["locations", "template"],
                "export default () => {\r\n  const s = `\u{1F600}\r\n${1}`; console.log(s)\r\n}",
            )],
            vec![("log", in_saved_function("locations.template", 3, 16))],
        ),
        // Repeated and parallel calls inside a Saved Function keep their own
        // call sites.
        (
            "await coral.functions.locations.parallel()",
            vec![linked_source(
                &["locations", "parallel"],
                "export default async () => {\n  for (const id of [1, 2]) console.log(id)\n  await Promise.all([coral.providers.demo.getItem({}), coral.providers.demo.getItem({})])\n}",
            )],
            vec![
                ("log", in_saved_function("locations.parallel", 2, 36)),
                ("log", in_saved_function("locations.parallel", 2, 36)),
                ("invoke", in_saved_function("locations.parallel", 3, 43)),
                ("invoke", in_saved_function("locations.parallel", 3, 77)),
            ],
        ),
        // A call before an inline export that needs private source on the same
        // line keeps its UTF-16 column.
        (
            "coral.functions.locations.inline(); coral.functions.locations.anonymous()",
            vec![
                linked_source(
                    &["locations", "inline"],
                    "const s = '\u{1F600}'; console.log(s); export default () => {}",
                ),
                linked_source(
                    &["locations", "anonymous"],
                    "coral.emit('\u{1F600}'); export default function () {}",
                ),
            ],
            vec![
                ("log", in_saved_function("locations.inline", 1, 25)),
                ("emit", in_saved_function("locations.anonymous", 1, 7)),
            ],
        ),
        // A binding called with no authored frame on the stack has no location.
        (
            "await Promise.resolve(\"x\").then(console.log);\nawait Promise.resolve(2).then(coral.emit);",
            vec![],
            vec![("log", None), ("emit", None)],
        ),
    ];
    let mismatches = cases
        .into_iter()
        .filter_map(|(source, functions, expected)| {
            let (_, host) = run_linked_with_recording_host(source, &functions);
            let locations = host.locations.lock().expect("locations").clone();
            (locations != expected)
                .then(|| format!("{source:?}\n  got {locations:?}\n want {expected:?}"))
        })
        .collect::<Vec<_>>();
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[test]
fn program_emissions_and_console_lines_reach_the_host_in_order() {
    let (_, host) = run_with_recording_host(
        r#"
            await coral.emit({ step: 1 });
            console.log("joined", 1, true, null);
            console.debug("d");
            console.info("i");
            console.warn("w");
            console.error("e");
            await coral.emit([2]);
        "#,
    );
    assert_eq!(
        *host.emitted.lock().expect("emissions"),
        [r#"{"step":1}"#, "[2]"]
    );
    assert_eq!(
        *host.logs.lock().expect("logs"),
        [
            (ConsoleLevel::Info, "joined 1 true null".to_string()),
            (ConsoleLevel::Debug, "d".to_string()),
            (ConsoleLevel::Info, "i".to_string()),
            (ConsoleLevel::Warn, "w".to_string()),
            (ConsoleLevel::Error, "e".to_string()),
        ]
    );
}

#[test]
fn oversized_or_invalid_host_values_reject_without_reaching_the_host() {
    // A JSON string encodes with two quotes; a Tool input also carries `{"body":}`.
    let fits = HOST_BYTES - 2;
    let body_fits = HOST_BYTES - r#"{"body":""}"#.len();
    let (outcomes, host) = run_with_recording_host(&format!(
        r#"
            const outcomes = [];
            const attempt = async (action) => {{
                try {{
                    await action();
                    outcomes.push("accepted");
                }} catch (error) {{
                    outcomes.push(error.code ?? error.name);
                }}
            }};
            await attempt(() => coral.emit("x".repeat({fits})));
            await attempt(() => coral.emit("x".repeat({fits} + 1)));
            await attempt(() => coral.emit(undefined));
            await attempt(() => coral.providers.widgets.create({{ body: "x".repeat({body_fits}) }}));
            await attempt(() => coral.providers.widgets.create({{ body: "x".repeat({body_fits} + 1) }}));
            return outcomes;
        "#
    ));
    assert_eq!(
        outcomes.as_deref(),
        Some(r#"["accepted","RangeError","TypeError","accepted","tool_input_too_large"]"#)
    );
    assert_eq!(host.emitted.lock().expect("emissions").len(), 1);
    assert_eq!(host.calls.lock().expect("calls").len(), 1);
}

/// Runs `source` to its rejection and returns the retained error and the host.
fn rejection(source: &str) -> (coral_v8::ProgramError, Arc<RecordingHost>) {
    let program = ProgramCompiler::new(IsolateLimits::default())
        .expect("start compiler")
        .compile(compilation_deadline(), source)
        .expect(source);
    let host = RecordingHost::new(coral_value::ValueLimits::default());
    let error = ProgramExecutor::new(IsolateLimits::default())
        .execute_with_host(&program, host.clone())
        .expect_err(source);
    let Error::ProgramRejected(error) = error else {
        panic!("{source} returned {error}");
    };
    (*error, host)
}

#[test]
fn program_errors_are_bounded() {
    let (error, _) = rejection("throw 'Address missing';");
    assert_eq!(
        (
            error.name.as_str(),
            error.message.as_str(),
            error.frames.len()
        ),
        ("ThrownValue", "Address missing", 0)
    );

    let (error, _) = rejection(&format!(
        "const e = new Error('{}'); e.name = '{}'; throw e;",
        "m".repeat(2000),
        "N".repeat(100)
    ));
    assert_eq!((error.name.len(), error.message.len()), (64, 1024));
    assert!(error.truncated);

    let (error, _) = rejection("const e = new Error('cycle'); e.cause = e; throw e;");
    assert!(error.cause.expect("cause").circular);

    // An error keeps at most four nested causes.
    let (error, _) = rejection(
        "let e = new Error('root'); for (let i = 0; i < 8; i++) e = new Error('level ' + i, { cause: e }); throw e;",
    );
    let mut depth = 0;
    let mut node = &error;
    while let Some(cause) = &node.cause {
        depth += 1;
        node = cause;
    }
    assert_eq!(depth, 4);
    assert!(node.cause_omitted);

    let (error, _) = rejection(
        "throw new AggregateError(Array.from({ length: 20 }, (_, i) => new Error('failure ' + i)), 'Many failures');",
    );
    assert_eq!((error.errors.len(), error.omitted_errors), (8, 12));

    // Mutual recursion keeps adjacent frames distinct, so only the frame bound applies.
    let (error, _) = rejection(
        "Error.stackTraceLimit = 100;\nfunction ping(n: number): never { return n === 0 ? fail() : pong(n - 1); }\nfunction pong(n: number): never { return ping(n - 1); }\nfunction fail(): never { throw new Error('deep'); }\nping(40);",
    );
    assert_eq!(error.frames.len(), 16);
    assert!(error.truncated);

    // Eight large members exceed the retained error budget, so members are dropped.
    let (error, _) = rejection(&format!(
        "throw new AggregateError(Array.from({{ length: 8 }}, () => new Error('{}')), 'Large');",
        "x".repeat(1000)
    ));
    assert!(error.omitted_errors > 0);
    assert!(serde_json::to_vec(&error).expect("encode").len() <= 8 * 1024);
}

#[test]
fn reading_a_program_error_runs_no_program_code() {
    for source in [
        "const e = new Error('original'); Object.defineProperty(e, 'cause', { get() { coral.emit('getter ran'); return new Error('extra'); } }); throw e;",
        "Error.prepareStackTrace = () => { coral.emit('stack hook ran'); return 'private stack'; }; throw new Error('original');",
        "const e = new Error('original'); const errors: Error[] = []; Object.defineProperty(errors, '0', { get() { coral.emit('index getter ran'); return e; } }); const all = new AggregateError([], 'original'); (all as { errors: Error[] }).errors = errors; throw all;",
    ] {
        let (error, host) = rejection(source);
        assert_eq!(error.message, "original", "{source}");
        assert!(
            host.emitted.lock().expect("emissions").is_empty(),
            "{source}"
        );
    }
}

#[test]
fn private_linker_source_maps_to_no_authored_source() {
    let program = ProgramCompiler::new(IsolateLimits::default())
        .expect("start compiler")
        .compile_with_saved_functions(
            compilation_deadline(),
            "coral.functions.a(); coral.functions.b(); coral.functions.c();",
            &[
                linked_source(&["a"], "export default (() => 1)"),
                linked_source(&["b"], "console.info(0); export default function () {}"),
                linked_source(
                    &["c"],
                    "const c = () => 1\nexport {}\nexport { c as default }",
                ),
            ],
        )
        .expect("compile");
    let source_map =
        sourcemap::SourceMap::from_slice(program.source_map().as_bytes()).expect("source map");
    // The runtime reads the nearest preceding token on the same line.
    let authored = |line: usize, column: usize| {
        let line = u32::try_from(line).expect("line");
        let column = u32::try_from(column).expect("column");
        source_map
            .lookup_token(line, column)
            .filter(|token| token.get_dst_line() == line)
            .and_then(|token| token.get_source().map(str::to_string))
    };
    let javascript = program.javascript();
    // Private source the linker writes: the wrapper, a private default
    // declaration and name, the statement end left by a removed export
    // clause, and each private return.
    let mut found = Vec::new();
    for (line, text) in javascript.lines().enumerate() {
        for marker in [
            "(async () => {",
            "__coralSavedFunctionDefaults[",
            "const __coralSavedFunctionDefaultExport_",
            "__coralSavedFunctionDefaultExport_",
            "return () =>",
        ] {
            for (column, _) in text.match_indices(marker) {
                found.push(marker);
                assert_eq!(authored(line, column), None, "{marker} at {line}:{column}");
            }
        }
        if text.trim() == ";" {
            found.push(";");
            let column = text.find(';').expect("semicolon");
            assert_eq!(authored(line, column), None, "; at {line}:{column}");
        }
    }
    for marker in [
        "(async () => {",
        "__coralSavedFunctionDefaults[",
        "const __coralSavedFunctionDefaultExport_",
        "__coralSavedFunctionDefaultExport_",
        "return () =>",
        ";",
    ] {
        assert!(found.contains(&marker), "no {marker} in {javascript}");
    }
}

#[test]
fn source_maps_name_only_authored_sources_and_embed_no_text() {
    let program = ProgramCompiler::new(IsolateLimits::default())
        .expect("start compiler")
        .compile(compilation_deadline(), "return 1;")
        .expect("compile");
    coral_v8::validate_source_map(program.source_map()).expect("compiled map");
    let compiled: serde_json::Value =
        serde_json::from_str(program.source_map()).expect("source map JSON");
    let with = |key: &str, value: serde_json::Value| {
        let mut map = compiled.clone();
        map.as_object_mut()
            .expect("source map object")
            .insert(key.to_string(), value);
        map.to_string()
    };
    for invalid in [
        "{".to_string(),
        with("file", serde_json::json!("other.js")),
        with("sourceRoot", serde_json::json!("/private")),
        with("sourcesContent", serde_json::json!(["return 1;"])),
        with("sources", serde_json::json!(["program.ts"])),
        with(
            "sources",
            serde_json::json!(["Saved Function coral.functions.a/b"]),
        ),
        with(
            "sources",
            serde_json::json!(["Saved Function coral.functions.a\nb"]),
        ),
        with(
            "sources",
            serde_json::json!([format!(
                "Saved Function coral.functions.{}",
                "a".repeat(257)
            )]),
        ),
    ] {
        assert!(
            matches!(
                coral_v8::validate_source_map(&invalid),
                Err(Error::InvalidSourceMap(_))
            ),
            "{invalid}"
        );
    }
}
