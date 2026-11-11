//! Each Built-in Tool's parser and results, held to the schemas a Program
//! is typed from.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use super::{
    AUTHENTICATION_KINDS, AddProviderResult, BuiltinArguments, BuiltinCredentialRoute, BuiltinTool,
    ConfigurationOutput, GetPolicyResult, GetProviderResult, ListProvidersResult, ListedProvider,
    ListedProviderState, ListedSetup, ListedSetupState, ProviderFacts, ProviderSourceOutput,
    UpdatePolicyResult, UpdateProviderResult, definition,
};
use crate::{CredentialField, ServerSelection};

/// Arguments that use every part of `tool`'s input schema, from its own
/// module.
fn valid_arguments(tool: BuiltinTool) -> Vec<Value> {
    match tool {
        BuiltinTool::AddProvider => super::add_provider::tests::valid_arguments(),
        BuiltinTool::ListProviders => super::list_providers::valid_arguments(),
        BuiltinTool::GetProvider => super::get_provider::tests::valid_arguments(),
        BuiltinTool::GetPolicy => super::get_policy::valid_arguments(),
        BuiltinTool::UpdatePolicy => super::update_policy::valid_arguments(),
        BuiltinTool::UpdateProvider => super::update_provider::tests::valid_arguments(),
        BuiltinTool::Search => super::search::tests::valid_arguments(),
        BuiltinTool::AddCustomTool => super::private_tools::valid_add_arguments(),
        BuiltinTool::UpdateCustomTool => super::private_tools::valid_update_arguments(),
        BuiltinTool::RemoveCustomTool => super::private_tools::valid_remove_arguments(),
    }
}

/// How a value differs from a valid one.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Change {
    /// The whole value replaced, an unknown field in an object, or a member or
    /// element removed, made `null` or given another type. The schema and the
    /// parser must agree on it.
    Structure,
    /// A string replaced by a word no schema lists. Where the schema allows
    /// only listed words, the parser must reject it too; elsewhere a rule
    /// beyond the schema, such as the URL syntax, may reject it.
    Word,
}

/// Every value one change away from `value`, each with how it changed.
fn mutations(value: &Value) -> Vec<(Change, String, Value)> {
    let mut found = Vec::new();
    for replacement in [Value::Null, json!("text"), json!(7), json!([])] {
        found.push((
            Change::Structure,
            format!("{replacement} at the root"),
            replacement,
        ));
    }
    mutate_within(value, "", &mut |change, description, changed| {
        found.push((change, description, changed));
    });
    found
}

/// The replacements for one member or element, each with how it changes it.
fn replacements(member: &Value) -> Vec<(Change, Value)> {
    let mut replacements = vec![(Change::Structure, Value::Null)];
    if member.is_string() {
        replacements.push((Change::Structure, json!(7)));
        replacements.push((Change::Word, json!("unlisted")));
    } else {
        replacements.push((Change::Structure, json!("text")));
    }
    replacements
}

fn mutate_within(value: &Value, path: &str, found: &mut dyn FnMut(Change, String, Value)) {
    match value {
        Value::Object(members) => {
            let mut added = members.clone();
            added.insert("unexpectedField".to_owned(), json!("text"));
            found(
                Change::Structure,
                format!("an unknown field in {path:?}"),
                Value::Object(added),
            );
            for (key, member) in members {
                let mut removed = members.clone();
                removed.remove(key);
                found(
                    Change::Structure,
                    format!("{path}/{key} removed"),
                    Value::Object(removed),
                );
                for (change, replacement) in replacements(member) {
                    let mut replaced = members.clone();
                    replaced.insert(key.clone(), replacement.clone());
                    found(
                        change,
                        format!("{path}/{key} as {replacement}"),
                        Value::Object(replaced),
                    );
                }
                mutate_within(
                    member,
                    &format!("{path}/{key}"),
                    &mut |change, description, changed| {
                        let mut within = members.clone();
                        within.insert(key.clone(), changed);
                        found(change, description, Value::Object(within));
                    },
                );
            }
        }
        Value::Array(elements) => {
            for (index, element) in elements.iter().enumerate() {
                for (change, replacement) in replacements(element) {
                    let replaced = elements
                        .iter()
                        .enumerate()
                        .map(|(at, element)| {
                            if at == index {
                                replacement.clone()
                            } else {
                                element.clone()
                            }
                        })
                        .collect();
                    found(
                        change,
                        format!("{path}/{index} as {replacement}"),
                        Value::Array(replaced),
                    );
                }
            }
        }
        _ => {}
    }
}

fn validator(schema: &Value) -> jsonschema::Validator {
    jsonschema::validator_for(schema).expect("a built-in schema compiles")
}

// Catches: a parser that accepts what the schema forbids, such as a field
// beside a method that takes none or a word the schema does not list, or
// rejects what a typed Program may send.
#[test]
fn each_tool_reads_exactly_the_arguments_its_input_schema_accepts() {
    let mut disagreements = Vec::new();
    for tool in BuiltinTool::ALL {
        let schema = validator(&definition(tool).input);
        for valid in valid_arguments(tool) {
            assert!(schema.is_valid(&valid), "{}: {valid}", tool.key());
            if let Err(problems) = BuiltinArguments::parse(tool, &valid) {
                panic!("{} rejects {valid}: {problems:?}", tool.key());
            }
            for (change, description, arguments) in mutations(&valid) {
                let schema_accepts = schema.is_valid(&arguments);
                let parser_accepts = BuiltinArguments::parse(tool, &arguments).is_ok();
                let disagree = match change {
                    Change::Structure => schema_accepts != parser_accepts,
                    Change::Word => parser_accepts && !schema_accepts,
                };
                if disagree {
                    disagreements.push(format!(
                        "{} with {description}: the schema {} it, the parser {} it",
                        tool.key(),
                        if schema_accepts { "accepts" } else { "rejects" },
                        if parser_accepts { "accepts" } else { "rejects" },
                    ));
                }
            }
        }
    }
    disagreements.sort();
    disagreements.dedup();
    assert!(disagreements.is_empty(), "{}", disagreements.join("\n"));
}

/// Facts for one Provider per method, which together use every route.
fn facts() -> impl Iterator<Item = ProviderFacts> {
    AUTHENTICATION_KINDS
        .into_iter()
        .zip(BuiltinCredentialRoute::ALL.into_iter().cycle())
        .enumerate()
        .map(
            |(index, (authentication, credential_route))| ProviderFacts {
                display_name: format!("Provider {index}"),
                credential_route,
                authentication,
                tools: index,
                account_required: index.is_multiple_of(2),
            },
        )
}

/// A Provider Configuration per configured method, which together use
/// every source, route, state and server choice.
fn configured_providers() -> impl Iterator<Item = GetProviderResult> {
    let sources = [
        ProviderSourceOutput::OpenapiDocument,
        ProviderSourceOutput::OpenapiUrl {
            url: "https://weather.example/openapi.json".to_owned(),
        },
        ProviderSourceOutput::McpUrl {
            url: "https://weather.example/mcp".to_owned(),
        },
    ];
    super::get_provider::tests::configured_methods()
        .into_iter()
        .zip(sources.into_iter().cycle())
        .zip(BuiltinCredentialRoute::ALL.into_iter().cycle())
        .enumerate()
        .map(|(index, ((authentication, source), credential_route))| {
            let server = if index.is_multiple_of(2) {
                ServerSelection::Declared {
                    url: "https://{region}.weather.example".to_owned(),
                    variables: BTreeMap::from([("region".to_owned(), "eu".to_owned())]),
                }
            } else {
                ServerSelection::Custom {
                    url: "https://weather.example".to_owned(),
                }
            };
            GetProviderResult {
                namespace: format!("provider{index}"),
                state: if index.is_multiple_of(2) {
                    ListedProviderState::Active
                } else {
                    ListedProviderState::Disabled
                },
                source,
                credential_route,
                configuration: ConfigurationOutput::new(
                    format!("Provider {index}"),
                    BTreeMap::from([("#/servers".to_owned(), server)]),
                    authentication,
                    BTreeMap::from([("x-trace".to_owned(), "on".to_owned())]),
                    BTreeMap::from([(
                        "token".to_owned(),
                        CredentialField {
                            label: "Token".to_owned(),
                            description: String::new(),
                            sensitive: true,
                        },
                    )]),
                ),
            }
        })
}

/// What each Tool that prepares a Provider Setup returns: the link where a
/// person finishes it, or the Provider a rename changed.
fn setup_tool_results() -> Vec<(BuiltinTool, serde_json::Value)> {
    let mut results = facts()
        .map(|facts| {
            (
                BuiltinTool::UpdateProvider,
                UpdateProviderResult::Published {
                    namespace: "weather".to_owned(),
                    facts,
                }
                .into_tool_result(),
            )
        })
        .collect::<Vec<_>>();
    results.extend([
        (
            BuiltinTool::AddProvider,
            AddProviderResult::NeedsPerson {
                url: "https://coral.example/workspaces/w/providers/new?setup=s".to_owned(),
                namespace: "weather".to_owned(),
            }
            .into_tool_result(),
        ),
        (
            BuiltinTool::UpdateProvider,
            UpdateProviderResult::NeedsPerson {
                url: "https://coral.example/workspaces/w/providers/p/update?setup=s&task=refresh"
                    .to_owned(),
                namespace: "weather".to_owned(),
            }
            .into_tool_result(),
        ),
    ]);
    results
}

/// What `getPolicy` reads for a Provider without rules and for one with a
/// rule of each state, and what `updatePolicy` returns when it sets them.
fn policies() -> impl Iterator<Item = (BuiltinTool, Value)> {
    let states = BTreeMap::from([
        (
            "coral.providers.github.repos.get".to_owned(),
            crate::ToolAccessState::Disabled,
        ),
        (
            "coral.shared.mail.remind".to_owned(),
            crate::ToolAccessState::Enabled,
        ),
        (
            "coral.addProvider".to_owned(),
            crate::ToolAccessState::ApprovalRequired,
        ),
    ]);
    [BTreeMap::new(), states.clone()]
        .into_iter()
        .map(|rules| {
            let read = GetPolicyResult { rules };
            (BuiltinTool::GetPolicy, read.into_tool_result())
        })
        .chain([(
            BuiltinTool::UpdatePolicy,
            UpdatePolicyResult { tools: states }.into_tool_result(),
        )])
}

/// What the Private Tool authoring Tools return.
fn private_tool_results() -> [(BuiltinTool, Value); 3] {
    let saved = super::SavedToolResult {
        call_path: "coral.private.github.pulls.listStale".to_owned(),
        description: "List stale pull requests.".to_owned(),
        declaration: "export default function (): Promise<string[]>;".to_owned(),
        diagnostics: vec![crate::SavedDiagnostic {
            category: crate::SavedDiagnosticCategory::Warning,
            code: 6_133,
            message: "'unused' is declared but its value is never read.".to_owned(),
            line: None,
            column: Some(3),
        }],
        diagnostics_truncated: true,
        revision: 2,
    };
    [
        (
            BuiltinTool::AddCustomTool,
            super::SavedToolResult {
                revision: 1,
                diagnostics: Vec::new(),
                diagnostics_truncated: false,
                ..saved.clone()
            }
            .into_tool_result(),
        ),
        (BuiltinTool::UpdateCustomTool, saved.into_tool_result()),
        (
            BuiltinTool::RemoveCustomTool,
            super::RemovedToolResult {
                call_path: "coral.private.github.pulls.listStale".to_owned(),
            }
            .into_tool_result(),
        ),
    ]
}

// Catches: a result that tells a typed Program something its declaration does not.
#[test]
fn each_result_meets_its_output_schema() {
    let mut results = setup_tool_results();
    results.push((
        BuiltinTool::ListProviders,
        ListProvidersResult {
            providers: facts()
                .enumerate()
                .map(|(index, facts)| {
                    // The first entry stands for the Built-in Provider.
                    let namespace = (index != 0).then(|| format!("provider{index}"));
                    ListedProvider {
                        call_root: namespace.as_ref().map_or_else(
                            || "coral".to_owned(),
                            |namespace| format!("coral.providers.{namespace}"),
                        ),
                        namespace,
                        state: if index.is_multiple_of(2) {
                            ListedProviderState::Active
                        } else {
                            ListedProviderState::Disabled
                        },
                        facts,
                    }
                })
                .collect(),
            setups: [
                (BuiltinCredentialRoute::Fixed, ListedSetupState::Ready),
                (
                    BuiltinCredentialRoute::Caller,
                    ListedSetupState::AwaitingSignIn,
                ),
                (
                    BuiltinCredentialRoute::None,
                    ListedSetupState::AwaitingConfiguration,
                ),
                (
                    BuiltinCredentialRoute::Caller,
                    ListedSetupState::AwaitingDiscovery,
                ),
            ]
            .into_iter()
            .map(|(credential_route, state)| ListedSetup {
                call_root: "coral.providers.weather".to_owned(),
                credential_route,
                state,
                url: "https://coral.example/workspaces/w/providers/new?setup=s".to_owned(),
            })
            .collect(),
        }
        .into_tool_result(),
    ));
    results.push((
        BuiltinTool::ListProviders,
        ListProvidersResult {
            providers: Vec::new(),
            setups: Vec::new(),
        }
        .into_tool_result(),
    ));
    results.extend(
        configured_providers().map(|saved| (BuiltinTool::GetProvider, saved.into_tool_result())),
    );
    results.extend(policies());
    results.extend(
        super::search::tests::results()
            .into_iter()
            .map(|result| (BuiltinTool::Search, result)),
    );
    results.extend(private_tool_results());
    for (tool, result) in results {
        assert_eq!(
            result.get("metadata"),
            Some(&json!({ "kind": "builtin" })),
            "the declaration's metadata"
        );
        let data = result.get("data").expect("a Tool Result has data");
        let schema = validator(&definition(tool).output);
        let errors = schema
            .iter_errors(data)
            .map(|error| error.to_string())
            .collect::<Vec<_>>();
        assert!(errors.is_empty(), "{}: {data}: {errors:?}", tool.key());
    }
}

// Catches: a search output schema that lets a missing path carry more
// suggestions than its description promises and discovery returns.
#[test]
fn a_missing_path_carries_at_most_three_suggestions() {
    let schema = validator(&definition(BuiltinTool::Search).output);
    let missing = super::search::tests::results()
        .into_iter()
        .filter_map(|result| result.get("data").cloned())
        .filter(|data| data.pointer("/missing/0/suggestions").is_some())
        .collect::<Vec<_>>();
    assert!(!missing.is_empty(), "some result names a missing path");
    for data in missing {
        for (count, valid) in [(3, true), (4, false)] {
            let mut suggested = data.clone();
            if let Some(suggestions) = suggested.pointer_mut("/missing/0/suggestions") {
                *suggestions = json!(vec!["coral.providers.notion.retrieveAPage"; count]);
            }
            assert_eq!(schema.is_valid(&suggested), valid, "{count}: {suggested}");
        }
    }
}

// Catches: a save that reaches the Tool without the module admission
// extracts, as one through a computed path does, being read as anything
// but a missing source, or a refusal that names another Tool.
#[test]
fn a_save_without_an_extracted_source_is_refused_naming_source() {
    type Parse = fn(&Value) -> Result<(), Vec<super::ArgumentProblem>>;
    let parsers: [(&str, Parse); 2] = [
        ("addCustomTool", |arguments| {
            super::ToolSaveArguments::parse_add(arguments).map(drop)
        }),
        ("updateCustomTool", |arguments| {
            super::ToolSaveArguments::parse_update(arguments).map(drop)
        }),
    ];
    for (tool, parse) in parsers {
        for arguments in [
            json!({ "path": "a.b", "description": "d" }),
            json!(["a.b", "d", "export default () => 1"]),
            json!("export default () => 1"),
        ] {
            let problems = parse(&arguments).expect_err("no source");
            assert_eq!(
                problems
                    .iter()
                    .map(|problem| (problem.field(), problem.reason()))
                    .collect::<Vec<_>>(),
                [(
                    "source",
                    format!(
                        "pass a function declared at the top level, through a literal \
                         coral.{tool} call"
                    )
                    .as_str()
                )],
                "{tool}: {arguments}"
            );
        }
    }
}

// Catches: an updateCustomTool that accepts a revision no tool can be at, or
// that accepts `null` for an argument to omit.
#[test]
fn update_tool_refuses_a_revision_below_one_naming_expected_revision() {
    let save = json!({ "path": "a.b", "description": "d", "source": "export default 1" });
    for (revision, reason) in [
        (json!(0), "pass a revision of 1 or more"),
        (json!(null), "expected u64"),
    ] {
        let mut arguments = save.clone();
        if let Some(fields) = arguments.as_object_mut() {
            fields.insert("expectedRevision".to_owned(), revision.clone());
        }
        let problems = super::ToolSaveArguments::parse_update(&arguments)
            .map(drop)
            .expect_err("a refused revision");
        assert_eq!(
            problems
                .iter()
                .map(|problem| (problem.field(), problem.reason()))
                .collect::<Vec<_>>(),
            [("expectedRevision", reason)],
            "{revision}"
        );
    }
}

// Catches: a save that loses the default its author declared, reads an
// omitted policy as a state, or accepts a word that is no Tool Access
// State, or `null`, without naming `policy`.
#[test]
fn a_save_reads_its_declared_policy_and_refuses_any_other_word_naming_policy() {
    type Parse = fn(&Value) -> Result<super::ToolSaveArguments, Vec<super::ArgumentProblem>>;
    let parsers: [(&str, Parse); 2] = [
        ("addCustomTool", super::ToolSaveArguments::parse_add),
        ("updateCustomTool", super::ToolSaveArguments::parse_update),
    ];
    let save = json!({ "path": "a.b", "description": "d", "source": "export default 1" });
    for (tool, parse) in parsers {
        for (policy, declared) in [
            (None, Ok(None)),
            (
                Some(json!("enabled")),
                Ok(Some(crate::ToolAccessState::Enabled)),
            ),
            (
                Some(json!("approval_required")),
                Ok(Some(crate::ToolAccessState::ApprovalRequired)),
            ),
            (
                Some(json!("disabled")),
                Ok(Some(crate::ToolAccessState::Disabled)),
            ),
            (Some(json!("ask")), Err("policy")),
            (Some(json!(null)), Err("policy")),
        ] {
            let mut arguments = save.clone();
            if let (Some(fields), Some(policy)) = (arguments.as_object_mut(), policy.clone()) {
                fields.insert("policy".to_owned(), policy);
            }
            let parsed = parse(&arguments)
                .map(|parsed| parsed.policy())
                .map_err(|problems| {
                    problems
                        .iter()
                        .map(|problem| problem.field().to_owned())
                        .collect::<Vec<_>>()
                });
            assert_eq!(
                parsed,
                declared.map_err(|field| vec![field.to_owned()]),
                "{tool}: {policy:?}"
            );
        }
    }
}

// Catches: a Built-in Tool that a Program cannot call as `coral.<key>`,
// because its key has more than one segment or names another member of
// `coral`.
#[test]
fn each_tool_key_is_one_segment_on_coral_that_no_other_member_takes() {
    for tool in BuiltinTool::ALL {
        let key = tool.key();
        assert!(
            crate::provider_namespace_is_valid(key),
            "{key} is not one safe identifier"
        );
        assert!(
            !super::NON_TOOL_ROOT_MEMBERS.contains(&key),
            "{key} is another member of coral"
        );
    }
}
