//! Program admission, packaging and Private Tool linking.
use super::*;

#[test]
fn fixed_program_source_limit_accepts_the_boundary_only() {
    let maximum_program = "p".repeat(crate::PROGRAM_SOURCE_MAX_BYTES);
    ProgramSource::parse(&maximum_program).expect("maximum Program source");
    ProgramSource::parse(&format!("{maximum_program}p")).expect_err("oversized Program source");
}

#[test]
fn fixed_run_timeout_limit_accepts_the_boundary_only() {
    RunTimeout::from_millis(900_000).expect("maximum Run timeout");
    let error = RunTimeout::from_millis(900_001).expect_err("oversized Run timeout");
    assert!(matches!(error, Error::InvalidInput(_)));
    assert_eq!(
        error.to_string(),
        "invalid input: Run timeout must be at most 900000 milliseconds"
    );
}

#[tokio::test]
async fn admission_rejects_a_private_tool_change_during_one_source_closure_read() {
    let (engine, release) = ClosureReadGateEngine::new();
    let fixture = TestModules::new(Arc::new(engine.clone())).await;
    let private_tools = PrivateToolModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .expect("start Private Tool compiler");
    let (workspace_id, principal) = fixture
        .user_scope("user:closure-race", "Read one consistent source closure")
        .await;
    let dependency_path = PrivateToolPath::parse("closure.dependency").expect("dependency path");
    let root_path = PrivateToolPath::parse("closure.root").expect("root path");
    private_tools
        .save(
            &private_tool_change(principal.clone(), 30, 'a'),
            &workspace_id,
            PrivateToolSave::new(
                dependency_path.clone(),
                AuthoredToolDescription::parse("Closure dependency").expect("description"),
                AuthoredToolSource::parse("export default () => 'old dependency';")
                    .expect("source"),
            ),
        )
        .await
        .expect("save dependency");
    private_tools
        .save(
            &private_tool_change(principal.clone(), 31, 'b'),
            &workspace_id,
            PrivateToolSave::new(
                root_path.clone(),
                AuthoredToolDescription::parse("Closure root").expect("description"),
                AuthoredToolSource::parse(
                    "const marker = 'root-v1'; export default () => marker + coral.private.closure.dependency();",
                )
                .expect("source"),
            ),
        )
        .await
        .expect("save root");

    let starting_runs = fixture.runs.clone();
    let starting_principal = principal.clone();
    let starting_workspace = workspace_id.clone();
    let start = tokio::spawn(async move {
        starting_runs
            .start(
                &starting_principal,
                &starting_workspace,
                submission("return coral.private.closure.root();"),
            )
            .await
    });
    wait_until(|| engine.reading_root.load(Ordering::SeqCst)).await;

    private_tools
        .save(
            &private_tool_change(principal.clone(), 40, 'c'),
            &workspace_id,
            PrivateToolSave::new(
                root_path,
                AuthoredToolDescription::parse("Changed closure root").expect("description"),
                AuthoredToolSource::parse(
                    "const marker = 'root-v2'; export default () => marker + coral.private.closure.dependency();",
                )
                .expect("source"),
            ),
        )
        .await
        .expect("replace root during closure read");
    private_tools
        .save(
            &private_tool_change(principal, 41, 'd'),
            &workspace_id,
            PrivateToolSave::new(
                dependency_path,
                AuthoredToolDescription::parse("Changed closure dependency").expect("description"),
                AuthoredToolSource::parse("export default () => 'new dependency';")
                    .expect("source"),
            ),
        )
        .await
        .expect("replace dependency during closure read");
    release.send(()).expect("release source-closure read");

    let result = start.await.expect("join admission");
    assert!(
        matches!(
            &result,
            Err(Error::ProgramAdmission(message)) if message.contains("changed during Program admission")
        ),
        "unexpected admission result: {result:?}"
    );
    assert_eq!(fixture.runs.count_runs().await.expect("count Runs"), 0);
}

#[tokio::test]
async fn type_only_private_tool_references_do_not_enter_the_runtime_closure() {
    coral_test_support::database_only!(Sqlite);
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let private_tools = PrivateToolModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .expect("start Private Tool compiler");
    let runs = fixture.live_runs(None);
    let (workspace_id, principal) = fixture
        .user_scope(
            "user:type-only-link",
            "Erase a type-only Private Tool reference",
        )
        .await;
    private_tools
        .save(
            &private_tool_change(principal.clone(), 30, '6'),
            &workspace_id,
            PrivateToolSave::new(
                PrivateToolPath::parse("types.answer").expect("path"),
                AuthoredToolDescription::parse("Return an answer").expect("description"),
                AuthoredToolSource::parse(
                    "export default function answer(): number { type Missing = typeof coral.private.never.loaded; const value: number = 42; return value; }",
                )
                .expect("source"),
            ),
        )
        .await
        .expect("save a function with an erased type reference");

    let accepted = runs
        .start(
            &principal,
            &workspace_id,
            submission("return coral.private.types.answer();"),
        )
        .await
        .expect("accept only the runtime source closure");
    let terminal = wait_for_terminal(&runs, &principal, &workspace_id, accepted.run_id()).await;
    assert!(matches!(
        terminal.terminal_outcome(),
        Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &serde_json::json!(42)
    ));
}

#[tokio::test]
async fn program_calls_one_private_tool_repeatedly_after_top_level_initialization() {
    coral_test_support::database_only!(Sqlite);
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let private_tools = PrivateToolModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .expect("start Private Tool compiler");
    let runs = fixture.live_runs(None);
    let (workspace_id, principal) = fixture
        .user_scope("user:function-runner", "Call Private Tool")
        .await;
    private_tools
        .save(
            &private_tool_change(principal.clone(), 30, '1'),
            &workspace_id,
            PrivateToolSave::new(
                PrivateToolPath::parse("math.factorial").expect("path"),
                AuthoredToolDescription::parse("Calculate a factorial").expect("description"),
                AuthoredToolSource::parse(
                    r"
                        function factorial(value: number): number {
                            return value <= 1 ? 1 : value * factorial(value - 1);
                        }
                        function offset(value: number): number { return value + 1; }
                        export default function calculate({ value }: { value: number }): number {
                            return offset(factorial(value));
                        }
                    ",
                )
                .expect("source"),
            ),
        )
        .await
        .expect("save callable");

    let accepted = runs
        .start(
            &principal,
            &workspace_id,
            submission(
                r"
                    const first = await coral.private.math.factorial({ value: 4 });
                    const second = await coral.private.math.factorial({ value: 3 });
                    return { first, second };
                ",
            ),
        )
        .await
        .expect("accept linked Program");
    let terminal = wait_for_terminal(&runs, &principal, &workspace_id, accepted.run_id()).await;

    assert!(matches!(
        terminal.terminal_outcome(),
        Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &serde_json::json!({"first": 25, "second": 7})
    ));
}

#[tokio::test]
async fn program_links_multiple_transitive_private_tools_into_one_run() {
    coral_test_support::database_only!(Sqlite);
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let private_tools = PrivateToolModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .expect("start Private Tool compiler");
    let runs = fixture.live_runs(None);
    let (workspace_id, principal) = fixture
        .user_scope("user:closure-runner", "Link a Private Tool closure")
        .await;

    for (path, description, source) in [
        (
            "math.increment",
            "Increment one number",
            "export default function increment({ value }: { value: number }): number { return value + 1; }",
        ),
        (
            "math.incrementTwice",
            "Increment one number twice",
            "const incremented = await coral.private.math.increment({ value: 40 }); export default function incrementTwice(): Promise<number> { return coral.private.math.increment({ value: incremented }); }",
        ),
        (
            "math.triple",
            "Triple one number",
            "export default function triple({ value }: { value: number }): number { return value * 3; }",
        ),
    ] {
        private_tools
            .save(
                &private_tool_change(principal.clone(), 30, '1'),
                &workspace_id,
                PrivateToolSave::new(
                    PrivateToolPath::parse(path).expect("path"),
                    AuthoredToolDescription::parse(description).expect("description"),
                    AuthoredToolSource::parse(source).expect("source"),
                ),
            )
            .await
            .expect("save closure member");
    }

    let accepted = runs
        .start(
            &principal,
            &workspace_id,
            submission("const triple = coral.private.math.triple; return [await coral.private.math.incrementTwice(), await triple({ value: 14 })];",
            ),
        )
        .await
        .expect("accept transitive closure");
    let terminal = wait_for_terminal(&runs, &principal, &workspace_id, accepted.run_id()).await;

    assert!(matches!(
        terminal.terminal_outcome(),
        Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &serde_json::json!([42, 42])
    ));
    assert_eq!(runs.count_runs().await.expect("count Runs"), 1);

    private_tools
        .delete(
            &private_tool_change(principal.clone(), 50, '2'),
            &workspace_id,
            &PrivateToolPath::parse("math.increment").expect("dependency path"),
        )
        .await
        .expect("delete transitive dependency");
    assert!(matches!(
        runs.start(
            &principal,
            &workspace_id,
            submission("const triple = coral.private.math.triple; return [await coral.private.math.incrementTwice(), await triple({ value: 14 })];",
            ),
        )
        .await,
        Err(Error::ProgramAdmission(message))
            if message == "Private Tool coral.private.math.incrementTwice:1:27: coral.private.math.increment names no Tool that you can call. Find one with coral.search."
    ));
    assert_eq!(runs.count_runs().await.expect("count Runs"), 1);
}

// Catches: admission that asks the compiler once per linked Tool, so the
// Run Executor link carries a message for each; one that refuses an unknown
// Tool before its one compile; or one that reports an unknown Tool before a
// missing Private Tool.
#[tokio::test]
async fn admission_analyzes_the_program_and_each_closure_level_once_and_compiles_once() {
    use coral_run_executor::testing::{CompilerCalls, CountingCompiler};
    coral_test_support::database_only!(Sqlite);
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let private_tools = PrivateToolModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .expect("start Private Tool compiler");
    let compiler = Arc::new(CountingCompiler::new(Arc::new(
        coral_run_executor::InProcessCompiler::new(crate::IsolateLimits::default())
            .expect("start the Program compiler"),
    )));
    let runs = fixture.live_runs_compiling_through(compiler.clone());
    let (workspace_id, principal) = fixture
        .user_scope("user:counted-admission", "Count compiler calls")
        .await;
    // Two levels below the Program: `increment` is reached twice.
    for (path, source) in [
        (
            "math.increment",
            "export default function increment({ value }: { value: number }): number { return value + 1; }",
        ),
        (
            "math.double",
            "export default function double({ value }: { value: number }): number { return value * 2; }",
        ),
        (
            "math.incrementTwice",
            "export default async function incrementTwice({ value }: { value: number }): Promise<number> { return coral.private.math.increment({ value: await coral.private.math.increment({ value }) }); }",
        ),
        (
            "math.sum",
            "export default async function sum({ value }: { value: number }): Promise<number> { return (await coral.private.math.increment({ value })) + (await coral.private.math.double({ value })); }",
        ),
    ] {
        private_tools
            .save(
                &private_tool_change(principal.clone(), 30, '1'),
                &workspace_id,
                PrivateToolSave::new(
                    PrivateToolPath::parse(path).expect("path"),
                    AuthoredToolDescription::parse("Counted").expect("description"),
                    AuthoredToolSource::parse(source).expect("source"),
                ),
            )
            .await
            .expect("save closure member");
    }

    let links = "return [await coral.private.math.incrementTwice({ value: 1 }), await coral.private.math.sum({ value: 1 })];";
    // The fixture has no such Provider, so the call checked at compile is
    // refused there.
    let unknown = "await coral.providers.nope.get({});";
    let unknown_tool = format!("{unknown}\n{links}");
    let missing_too = format!("{unknown}\nreturn coral.private.math.missing({{}});");
    let cases: [(&str, &[usize], usize, Option<&str>); 4] = [
        ("return 42;", &[1], 1, None),
        (links, &[1, 2, 2], 1, None),
        (
            &unknown_tool,
            &[1, 2, 2],
            1,
            Some("coral.providers.nope.get names no Tool"),
        ),
        (
            &missing_too,
            &[1],
            0,
            Some("coral.private.math.missing names no Tool"),
        ),
    ];
    for (source, analyzed, compiled, refused) in cases {
        let prepared = runs
            .prepare(&principal, &workspace_id, submission(source))
            .await;
        match refused {
            None => {
                prepared.unwrap_or_else(|error| panic!("{source}: {error:?}"));
            }
            // Which refusal wins, not its wording, which `call_paths.rs` owns.
            Some(refusal) => assert!(
                matches!(&prepared, Err(Error::ProgramAdmission(message)) if message.contains(refusal)),
                "{source}: {:?}",
                prepared.map(drop)
            ),
        }
        assert_eq!(
            compiler.take(),
            CompilerCalls {
                analyze: analyzed.to_vec(),
                compile: compiled,
                prepare: 0,
            },
            "{source}"
        );
    }
}

/// Scanning references, rather than compiling 64 functions with V8, keeps the
/// two-second compilation deadline out of this count rule under load.
#[tokio::test]
async fn linked_private_tool_limit_accepts_the_boundary_and_rejects_overflow_atomically() {
    let fixture = TestModules::new(Arc::new(ReferenceScanningEngine)).await;
    let private_tools = PrivateToolModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .expect("start Private Tool compiler");
    let runs = &fixture.runs;
    let (workspace_id, principal) = fixture
        .user_scope("user:closure-limit", "Bound the Private Tool closure")
        .await;

    for index in (0..=crate::LINKED_TOOL_MAX_COUNT).rev() {
        let source = if index == crate::LINKED_TOOL_MAX_COUNT {
            "export default function end(): number { return 1; }".to_string()
        } else {
            format!(
                "export default function next(): number {{ return coral.private.limit.function{:02}(); }}",
                index + 1
            )
        };
        private_tools
            .save(
                &private_tool_change(principal.clone(), 30, '3'),
                &workspace_id,
                PrivateToolSave::new(
                    PrivateToolPath::parse(&format!("limit.function{index:02}")).expect("path"),
                    AuthoredToolDescription::parse("One closure member").expect("description"),
                    AuthoredToolSource::parse(&source).expect("source"),
                ),
            )
            .await
            .expect("save closure member");
    }

    let boundary = runs
        .start(
            &principal,
            &workspace_id,
            submission("return coral.private.limit.function01();"),
        )
        .await
        .expect("accept exactly 64 linked Private Tools");
    let boundary = wait_for_terminal(runs, &principal, &workspace_id, boundary.run_id()).await;
    assert!(matches!(
        boundary.terminal_outcome(),
        Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &serde_json::json!(crate::LINKED_TOOL_MAX_COUNT)
    ));

    assert!(matches!(
        runs.start(
            &principal,
            &workspace_id,
            submission("return coral.private.limit.function00();"),
        )
        .await,
        Err(Error::ProgramAdmission(message))
            if message.contains("may link at most 64 Private and Shared Tools")
    ));
    assert_eq!(runs.count_runs().await.expect("count Runs"), 1);
}

#[tokio::test]
async fn transitive_source_closure_enforces_the_combined_typescript_limit_without_a_run() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let private_tools = PrivateToolModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .expect("start Private Tool compiler");
    let runs = fixture.live_runs(None);
    let (workspace_id, principal) = fixture
        .user_scope("user:closure-bytes", "Bound combined TypeScript")
        .await;
    let large_source_bytes = 60 * 1024;

    for index in (0..9).rev() {
        let implementation = if index == 8 {
            "export default function end(): number { return 1; }".to_string()
        } else {
            format!(
                "export default function next(): number {{ return coral.private.bytes.function{:02}(); }}",
                index + 1
            )
        };
        let padding = "x".repeat(large_source_bytes - implementation.len() - 4);
        let source = format!("/*{padding}*/{implementation}");
        assert_eq!(source.len(), large_source_bytes);
        private_tools
            .save(
                &private_tool_change(principal.clone(), 30, '4'),
                &workspace_id,
                PrivateToolSave::new(
                    PrivateToolPath::parse(&format!("bytes.function{index:02}")).expect("path"),
                    AuthoredToolDescription::parse("One large closure member")
                        .expect("description"),
                    AuthoredToolSource::parse(&source).expect("source"),
                ),
            )
            .await
            .expect("save large closure member");
    }

    assert!(matches!(
        runs.start(
            &principal,
            &workspace_id,
            submission("return coral.private.bytes.function00();"),
        )
        .await,
        Err(Error::PackageTooLarge {
            component: "combined TypeScript source",
            maximum_bytes: crate::COMBINED_TYPESCRIPT_MAX_BYTES,
        })
    ));
    assert_eq!(runs.count_runs().await.expect("count Runs"), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn private_tool_initialization_uses_the_run_surface_once_before_the_program_body() {
    coral_test_support::database_only!(Sqlite);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/forecast"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "temperature": 21
        })))
        .expect(1)
        .mount(&server)
        .await;

    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture.live_runs(None);
    let private_tools = PrivateToolModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .expect("start Private Tool compiler");
    let tool = fixture
        .user_tool_at(
            "user:initialization-runner",
            "Initialize one Private Tool",
            &server.uri(),
        )
        .await;
    private_tools
        .save(
            &private_tool_change(tool.principal.clone(), 40, 'a'),
            &tool.workspace_id,
            PrivateToolSave::new(
                PrivateToolPath::parse("weather.initializedTemperature").expect("path"),
                AuthoredToolDescription::parse("Read the initialized temperature")
                    .expect("description"),
                AuthoredToolSource::parse(
                    r#"
                        const forecast = await coral.providers.weather.getForecast();
                        await coral.emit({ phase: "initialization", temperature: forecast.data.temperature });
                        console.log("Private Tool initialized");
                        const initializedTemperature = forecast.data.temperature;
                        export default function read(): number { return initializedTemperature; }
                    "#,
                )
                .expect("source"),
            ),
        )
        .await
        .expect("save initializing function");

    let accepted = runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission(
                r#"
                    const first = await coral.private.weather.initializedTemperature();
                    const second = await coral.private.weather.initializedTemperature();
                    await coral.emit({ phase: "body" });
                    console.log("Program body");
                    return [first, second];
                "#,
            ),
        )
        .await
        .expect("accept initializing package");
    let terminal = wait_for_terminal(
        &runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;

    assert!(matches!(
        terminal.terminal_outcome(),
        Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &serde_json::json!([21, 21])
    ));
    assert!(matches!(
        terminal.events(),
        [initialized, initialization_log, body, body_log]
            if matches!(initialized.kind(), RunEventKind::ProgramEmitted { value }
                    if value.as_json() == &serde_json::json!({"phase": "initialization", "temperature": 21}))
                && matches!(initialization_log.kind(), RunEventKind::ProgramLogged { level: ProgramLogLevel::Info, message }
                    if message == "Private Tool initialized")
                && matches!(body.kind(), RunEventKind::ProgramEmitted { value }
                    if value.as_json() == &serde_json::json!({"phase": "body"}))
                && matches!(body_log.kind(), RunEventKind::ProgramLogged { level: ProgramLogLevel::Info, message }
                    if message == "Program body")
    ));
}

#[tokio::test]
async fn replacement_and_deletion_do_not_change_an_accepted_run_package() {
    let (engine, release) = SnapshotEngine::new();
    let fixture = TestModules::new(Arc::new(engine.clone())).await;
    let private_tools = PrivateToolModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .expect("start Private Tool compiler");
    let (workspace_id, principal) = fixture
        .user_scope("user:snapshot-runner", "Retain accepted source")
        .await;
    let path = PrivateToolPath::parse("snapshots.value").expect("path");
    let original_source = "export default function value() { return 'original'; }";
    private_tools
        .save(
            &private_tool_change(principal.clone(), 30, '7'),
            &workspace_id,
            PrivateToolSave::new(
                path.clone(),
                AuthoredToolDescription::parse("Original snapshot").expect("description"),
                AuthoredToolSource::parse(original_source).expect("source"),
            ),
        )
        .await
        .expect("save original");

    let accepted = fixture
        .runs
        .start(
            &principal,
            &workspace_id,
            submission("return coral.private.snapshots.value();"),
        )
        .await
        .expect("accept Run with original source");
    wait_until(|| engine.started.load(Ordering::SeqCst)).await;

    private_tools
        .save(
            &private_tool_change(principal.clone(), 40, '8'),
            &workspace_id,
            PrivateToolSave::new(
                path.clone(),
                AuthoredToolDescription::parse("Replacement snapshot").expect("description"),
                AuthoredToolSource::parse(
                    "export default function value() { return 'replacement'; }",
                )
                .expect("source"),
            ),
        )
        .await
        .expect("replace after Run acceptance");
    private_tools
        .delete(
            &private_tool_change(principal.clone(), 50, '9'),
            &workspace_id,
            &path,
        )
        .await
        .expect("delete after Run acceptance");
    let detail = fixture
        .runs
        .inspect_run(
            &principal,
            &workspace_id,
            accepted.run_id(),
            &crate::RunInspection::default(),
        )
        .await
        .expect("inspect retained source");
    assert_eq!(
        detail.submitted_source,
        "return coral.private.snapshots.value();"
    );
    assert_eq!(detail.linked_private_tools.len(), 1);
    let snapshot = detail.linked_private_tools.first().unwrap();
    assert_eq!(snapshot.path, "snapshots.value");
    assert_eq!(snapshot.source, original_source);
    release.send(()).expect("release accepted package");

    let terminal =
        wait_for_terminal(&fixture.runs, &principal, &workspace_id, accepted.run_id()).await;
    assert!(matches!(
        terminal.terminal_outcome(),
        Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &serde_json::json!({"linked_source": original_source})
    ));
    assert!(matches!(
        fixture
            .runs
            .start(
                &principal,
                &workspace_id,
                submission("return coral.private.snapshots.value();"),
            )
            .await,
        Err(Error::ProgramAdmission(message))
            if message == "Program:1:1: coral.private.snapshots.value names no Tool that you can call. Find one with coral.search."
    ));
    assert_eq!(fixture.runs.count_runs().await.expect("count Runs"), 1);
}

#[tokio::test]
async fn admission_rejects_missing_or_computed_private_tool_paths_without_a_run() {
    coral_test_support::database_only!(Sqlite);
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture.live_runs(None);
    let (workspace_id, principal) = fixture
        .user_scope("user:link-rejections", "Reject invalid links")
        .await;

    let missing = runs
        .start(
            &principal,
            &workspace_id,
            submission("\nreturn coral.private.missing.call();"),
        )
        .await;
    assert!(matches!(
        missing,
        Err(Error::ProgramAdmission(message))
            if message == "Program:2:8: coral.private.missing.call names no Tool that you can call. Find one with coral.search."
    ));

    let computed = runs
        .start(
            &principal,
            &workspace_id,
            submission("const name = 'call';\nreturn coral.private.missing[name]();"),
        )
        .await;
    assert!(matches!(
        computed,
        Err(Error::ProgramAdmission(message))
            if message == "Program:2:8: Private Tool paths must be directly named without computed access"
    ));

    for source in [
        "return coral.private[\"computed\"]();",
        "const name = 'call'; return (coral.private.missing as any)[name]();",
        "const functions = coral.private; return functions.missing();",
        "const pick = (root: any, key: string) => root[key]; return pick(coral.private, 'missing')();",
        "return fetch('https://example.test');",
    ] {
        assert!(matches!(
            runs.start(&principal, &workspace_id, submission(source),)
                .await,
            Err(Error::ProgramAdmission(_))
        ));
    }
    assert_eq!(runs.count_runs().await.expect("count rejected Runs"), 0);
}

#[tokio::test]
async fn direct_repeated_calls_follow_a_unicode_private_tool_live_binding() {
    coral_test_support::database_only!(Sqlite);
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let private_tools = PrivateToolModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .expect("start Private Tool compiler");
    let runs = fixture.live_runs(None);
    let (workspace_id, principal) = fixture
        .user_scope("user:live-binding", "Run live binding")
        .await;
    private_tools
        .save(
            &private_tool_change(principal.clone(), 40, 'a'),
            &workspace_id,
            PrivateToolSave::new(
                PrivateToolPath::parse("café.calculer").expect("Unicode path"),
                AuthoredToolDescription::parse("Follow the exported live binding")
                    .expect("description"),
                AuthoredToolSource::parse(
                    "let calculer = () => { calculer = () => 2; return 1; }; export { calculer as default };",
                )
                .expect("source"),
            ),
        )
        .await
        .expect("save Unicode live-binding function");

    let accepted = runs
        .start(
            &principal,
            &workspace_id,
            submission("const first = await coral.private.café.calculer(); const second = await coral.private.café.calculer(); return [first, second];",
            ),
        )
        .await
        .expect("accept Unicode linked Program");
    let terminal = wait_for_terminal(&runs, &principal, &workspace_id, accepted.run_id()).await;
    assert!(matches!(
        terminal.terminal_outcome(),
        Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &serde_json::json!([1, 2])
    ));
}

#[tokio::test]
async fn equivalent_callable_default_export_forms_execute() {
    coral_test_support::database_only!(Sqlite);
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let private_tools = PrivateToolModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .expect("start Private Tool compiler");
    let runs = fixture.live_runs(None);
    let (workspace_id, principal) = fixture
        .user_scope("user:export-forms", "Run export forms")
        .await;
    let forms = [
        "function answer({ value }: { value: number }): number { return value; } export { answer as default };",
        "function answer({ value }: { value: number }): number { return value; } export {}; export { answer as default };",
        "function answer({ value }: { value: number }): number { return value; } export type {}; export { answer as default };",
        "const answer = ({ value }: { value: number }): number => value; export default answer;",
        "let answer = (_input: { value: number }): number => 42; export default answer; answer = () => 0;",
        "export default ({ value }: { value: number }): number => value;",
        "export default async function answer({ value }: { value: number }): Promise<number> { return value; }",
    ];

    for (index, source) in forms.into_iter().enumerate() {
        private_tools
            .save(
                &private_tool_change(
                    principal.clone(),
                    40 + i64::try_from(index).expect("small index"),
                    char::from_digit(u32::try_from(index + 4).expect("small digit"), 16)
                        .expect("hex digit"),
                ),
                &workspace_id,
                PrivateToolSave::new(
                    PrivateToolPath::parse("forms.answer").expect("path"),
                    AuthoredToolDescription::parse("Return the supplied answer")
                        .expect("description"),
                    AuthoredToolSource::parse(source).expect("source"),
                ),
            )
            .await
            .expect("accept equivalent default export");
        let accepted = runs
            .start(
                &principal,
                &workspace_id,
                submission("return await coral.private.forms.answer({ value: 42 });"),
            )
            .await
            .expect("accept linked export form");
        let terminal = wait_for_terminal(&runs, &principal, &workspace_id, accepted.run_id()).await;
        assert!(
            matches!(
                terminal.terminal_outcome(),
                Some(RunTerminalOutcome::Succeeded {
                    final_value: FinalValue::Present(value)
                }) if value.as_json() == &serde_json::json!(42)
            ),
            "export form {index}"
        );
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end admission scenario covers both successful Final Value forms and rejection atomicity"
)]
async fn admission_executes_one_durable_package_and_preserves_final_value_presence() {
    let engine = Arc::new(ImmediateEngine::default());
    let fixture = TestModules::new(engine.clone()).await;
    let (workspace_id, principal) = fixture.user_scope("user:runner", "Run programs").await;

    let absent = fixture
        .runs
        .start(&principal, &workspace_id, submission("absent"))
        .await
        .expect("accept no-value Program");
    assert_eq!(absent.state(), RunState::Running);
    assert!(absent.events().is_empty());
    let absent = wait_for_terminal(&fixture.runs, &principal, &workspace_id, absent.run_id()).await;
    assert!(matches!(
        absent.terminal_outcome(),
        Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Absent
        })
    ));
    let completion_winner = fixture
        .runs
        .cancel(
            &principal,
            &workspace_id,
            absent.run_id(),
            &immediate(None, 100),
        )
        .await
        .expect("completed Run keeps its terminal result");
    assert_eq!(completion_winner.state(), RunState::Succeeded);
    assert!(
        !retained_events(
            &fixture.runs,
            &principal,
            &workspace_id,
            completion_winner.run_id()
        )
        .await
        .iter()
        .any(|event| matches!(event.kind(), RunEventKind::CancellationRequested { .. }))
    );

    let explicit_null = fixture
        .runs
        .start(&principal, &workspace_id, submission("null"))
        .await
        .expect("accept explicit-null Program");
    let explicit_null = wait_for_terminal(
        &fixture.runs,
        &principal,
        &workspace_id,
        explicit_null.run_id(),
    )
    .await;
    assert!(matches!(
        explicit_null.terminal_outcome(),
        Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json().is_null()
    ));
    assert_eq!(engine.executions.load(Ordering::SeqCst), 2);

    let other = organization_user(
        fixture.database.clone(),
        "user:other",
        OrganizationRole::Member,
    )
    .await;
    fixture
        .workspaces
        .set_membership(
            &change(
                principal
                    .authenticated_user()
                    .expect("User Principal")
                    .clone(),
                30,
                '2',
            ),
            &workspace_id,
            &other,
            WorkspaceRole::Member,
        )
        .await
        .expect("add other member");
    let before = fixture.runs.count_runs().await.expect("count Runs");
    assert!(matches!(
        fixture
            .runs
            .start(&principal, &workspace_id, submission("compile-error"),)
            .await,
        Err(Error::ProgramAdmission(_))
    ));
    assert!(matches!(
        fixture
            .runs
            .start(&principal, &workspace_id, submission("oversized-package"),)
            .await,
        Err(Error::PackageTooLarge {
            component: "emitted JavaScript",
            ..
        })
    ));
    assert!(matches!(
        fixture
            .runs
            .start(
                &principal,
                &workspace_id,
                submission("oversized-source-map"),
            )
            .await,
        Err(Error::PackageTooLarge {
            component: "source map",
            ..
        })
    ));
    assert!(matches!(
        fixture
            .runs
            .start(&principal, &workspace_id, submission("invalid-source-map"),)
            .await,
        Err(Error::ProgramAdmission(_))
    ));
    assert_eq!(fixture.runs.count_runs().await.expect("count Runs"), before);
    assert_eq!(engine.executions.load(Ordering::SeqCst), 2);
    ProgramSource::parse(&"x".repeat(crate::PROGRAM_SOURCE_MAX_BYTES + 1))
        .expect_err("oversized source must fail before admission");
}

#[tokio::test]
async fn accepted_typescript_executes_from_its_stored_package() {
    coral_test_support::database_only!(Sqlite);
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture.live_runs(None);
    let (workspace_id, principal) = fixture
        .user_scope("user:typescript-runner", "Execute TypeScript")
        .await;
    let accepted = runs
        .start(
            &principal,
            &workspace_id,
            submission(
                r"
                    const answer: number = await Promise.resolve(21 * 2);
                    return { answer };
                ",
            ),
        )
        .await
        .expect("accept TypeScript Program");
    let terminal = wait_for_terminal(&runs, &principal, &workspace_id, accepted.run_id()).await;

    assert!(matches!(
        terminal.terminal_outcome(),
        Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &serde_json::json!({"answer": 42})
    ));

    let rejected = runs
        .start(
            &principal,
            &workspace_id,
            submission(r#"throw new Error("authored rejection canary");"#),
        )
        .await
        .expect("accept rejecting TypeScript Program");
    let rejected = wait_for_terminal(&runs, &principal, &workspace_id, rejected.run_id()).await;
    let Some(RunTerminalOutcome::Failed(failure)) = rejected.terminal_outcome() else {
        panic!("rejecting Program must fail its Run");
    };
    assert_eq!(failure.code(), RunFailureCode::ProgramRejected);
    assert_eq!(failure.message(), "Error: authored rejection canary");
    let error = failure.error().expect("retained Program error");
    assert_eq!(error.message, "authored rejection canary");
    assert_eq!(error.frames.first().expect("authored location").line, 1);
    assert!(!failure.message().contains("program.js"));

    let native_projection = runs
        .start(&principal, &workspace_id, submission("return Number.NaN;"))
        .await
        .expect("accept Program using native JSON projection");
    let native_projection =
        wait_for_terminal(&runs, &principal, &workspace_id, native_projection.run_id()).await;
    assert!(matches!(
        native_projection.terminal_outcome(),
        Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json().is_null()
    ));

    let non_interoperable_json = runs
        .start(&principal, &workspace_id, submission("return \"\\uD800\";"))
        .await
        .expect("accept Program before Final Value projection");
    let non_interoperable_json = wait_for_terminal(
        &runs,
        &principal,
        &workspace_id,
        non_interoperable_json.run_id(),
    )
    .await;
    assert!(matches!(
        non_interoperable_json.terminal_outcome(),
        Some(RunTerminalOutcome::Failed(failure))
            if failure.code() == RunFailureCode::InvalidProgramValue
    ));
}

#[tokio::test]
async fn compilation_holds_no_write_transaction_and_final_recheck_is_atomic() {
    let (engine, release) = CompileGateEngine::new();
    let fixture = TestModules::new(Arc::new(engine.clone())).await;
    let owner_user = organization_user(
        fixture.database.clone(),
        "user:compile-owner",
        OrganizationRole::Member,
    )
    .await;
    let workspace = fixture
        .workspaces
        .create(
            &change(owner_user.clone(), 10, '5'),
            WorkspaceDisplayName::parse("Compile gate").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let principal = AuthenticatedPrincipal::user(owner_user.clone());
    let runs = fixture.runs.clone();
    let workspace_id = workspace.id().clone();
    let start_principal = principal.clone();
    let start = tokio::spawn(async move {
        runs.start(&start_principal, &workspace_id, submission("compile-gated"))
            .await
    });
    wait_until(|| engine.compiling.load(Ordering::SeqCst)).await;

    fixture
        .workspaces
        .archive(&change(owner_user, 30, '6'), workspace.id())
        .await
        .expect("archive while compilation is blocked");
    release.send(()).expect("release compilation");
    assert!(matches!(
        start.await.expect("join start"),
        Err(Error::Workspace(coral_workspace::Error::WorkspaceArchived))
    ));
    assert_eq!(fixture.runs.count_runs().await.expect("count Runs"), 0);
    assert_eq!(engine.executions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn standalone_acceptance_can_commit_before_launch_without_a_goal() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let user = organization_user(
        fixture.database.clone(),
        "user:standalone",
        OrganizationRole::Member,
    )
    .await;
    let workspace = fixture
        .workspaces
        .create(
            &change(user.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Standalone execution").unwrap(),
        )
        .await
        .unwrap();
    let actor = AuthenticatedPrincipal::user(user);
    let prepared = fixture
        .runs
        .prepare(
            &actor,
            workspace.id(),
            RunSubmission::new(
                ProgramSource::parse("null").unwrap(),
                RunTimeout::from_millis(30_000).unwrap(),
            ),
        )
        .await
        .unwrap();
    let mut transaction = fixture.database.begin().await.unwrap();
    let staged = fixture
        .runs
        .accept_in_transaction(&mut transaction, prepared)
        .await
        .unwrap();
    let id = staged.run_id().clone();
    transaction.commit().await.unwrap();
    let accepted = fixture
        .runs
        .observe(&actor, workspace.id(), &id, &immediate(None, 100))
        .await
        .unwrap();
    assert_eq!(accepted.state(), RunState::Running);
    fixture.runs.launch(staged).await.unwrap();
    let terminal = wait_for_terminal(&fixture.runs, &actor, workspace.id(), &id).await;
    assert_eq!(terminal.state(), RunState::Succeeded);
}

/// Acceptance records no deadline. The start mark records it as the start
/// plus the time limit, so time spent waiting to start does not count.
#[tokio::test]
async fn the_deadline_counts_the_time_limit_from_the_start() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let (workspace_id, principal) = fixture
        .user_scope("user:start-deadline", "Count the deadline from the start")
        .await;
    let prepared = fixture
        .runs
        .prepare(&principal, &workspace_id, submission("null"))
        .await
        .unwrap();
    let mut transaction = fixture.database.begin().await.unwrap();
    let staged = fixture
        .runs
        .accept_in_transaction(&mut transaction, prepared)
        .await
        .unwrap();
    let run_id = staged.run_id().clone();
    transaction.commit().await.unwrap();
    let accepted = fixture.run_record(&workspace_id, &run_id).await;
    assert_eq!(
        accepted.deadline_at_unix_nanos, None,
        "acceptance records none"
    );

    // Less than the lease, which a Run that waits longer has lost.
    let waited = fixture.runs.execution_policy.claim_lease_nanos() / 2;
    fixture.clock.next.fetch_add(waited, Ordering::SeqCst);
    fixture.runs.launch(staged).await.unwrap();
    wait_for_terminal(&fixture.runs, &principal, &workspace_id, &run_id).await;
    let ended = fixture.run_record(&workspace_id, &run_id).await;
    let started_at = ended
        .execution_started_at_unix_nanos
        .expect("the Run started");
    assert!(started_at > accepted.accepted_at_unix_nanos + waited);
    assert_eq!(
        ended.deadline_at_unix_nanos,
        Some(started_at + 60_000_000_000),
        "the start plus the 60 s time limit"
    );
}
