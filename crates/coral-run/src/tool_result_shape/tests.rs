use serde_json::{Value, json};

use super::*;

fn http(call_path: &str, data: Value) -> ReceivedResult {
    received(call_path, data, json!({"kind": "http"}))
}

fn mcp(call_path: &str, data: Value) -> ReceivedResult {
    received(call_path, data, json!({"kind": "mcp"}))
}

fn received(call_path: &str, data: Value, metadata: Value) -> ReceivedResult {
    ReceivedResult {
        call_path: call_path.to_string(),
        tool_result: Value::Object(serde_json::Map::from_iter([
            ("data".to_string(), data),
            ("metadata".to_string(), metadata),
        ])),
    }
}

/// The one shape of one received result.
fn shape_of(result: ReceivedResult) -> ToolResultShape {
    let sketched = sketch(vec![result], 0);
    assert_eq!(sketched.unsketched(), 0);
    match sketched.shapes() {
        [shape] => shape.clone(),
        other => panic!("one shape expected, got {other:?}"),
    }
}

#[test]
fn shapes_show_keys_and_types_of_data() {
    for (data, expected) in [
        (json!({}), "{}"),
        (json!([]), "[]"),
        (json!(null), "null"),
        (json!("text"), "string"),
        (
            json!({"a": 1, "b": true, "c": null}),
            "{ a: number; b: boolean; c: null }",
        ),
        (json!({"list": [1, 2]}), "{ list: Array<number> }"),
        (json!({"empty": []}), "{ empty: [] }"),
        (json!([[1], ["x"]]), "Array<Array<string | number>>"),
        (
            json!({"content-type": 1, "_id": 2, "$ok_1": 3}),
            r#"{ "content-type": number; _id: number; $ok_1: number }"#,
        ),
    ] {
        assert_eq!(shape_of(http("coral.providers.a.b", data)).data(), expected);
    }
}

#[test]
fn an_array_shows_the_union_of_its_elements_and_keys_some_lack_are_optional() {
    let shape = shape_of(http(
        "coral.providers.notion.postSearch",
        json!({"results": [
            {"object": "page", "id": "p1", "properties": {"Name": {"title": []}}},
            {"object": "data_source", "id": "d1", "title": [{"plain_text": "x"}]},
            {"object": "data_source", "id": "d2", "title": "flat"},
            "loose",
        ]}),
    ));
    assert_eq!(
        shape.data(),
        "{ results: Array<{ object: string; id: string; properties?: { Name: { title: [] } }; title?: Array<{ plain_text: string }> | string } | string> }"
    );
}

#[test]
fn a_union_of_scalars_and_null_shows_each_kind_once() {
    let shape = shape_of(http(
        "coral.providers.a.b",
        json!([{"cursor": "c"}, {"cursor": null}, {"cursor": 3}, {"cursor": "d"}]),
    ));
    assert_eq!(shape.data(), "Array<{ cursor: string | number | null }>");
}

#[test]
fn deep_objects_and_many_keys_are_summarised() {
    let mut deep = json!({"leaf": 1});
    for _ in 0..MAX_DEPTH {
        deep = json!({"next": deep});
    }
    assert_eq!(
        shape_of(http("coral.providers.a.b", deep)).data(),
        "{ next: { next: { next: { next: { next: { next: {…} } } } } } }"
    );

    let map: serde_json::Map<String, Value> = (0..KEYS_SHOWN + 3)
        .map(|index| (format!("key{index:02}"), json!(index)))
        .collect();
    let shown = shape_of(http("coral.providers.a.b", Value::Object(map)))
        .data()
        .to_string();
    assert!(shown.ends_with("key15: number; … 3 more keys }"), "{shown}");
    assert!(!shown.contains("key16"), "{shown}");

    let one_more: serde_json::Map<String, Value> = (0..=KEYS_SHOWN)
        .map(|index| (format!("key{index:02}"), json!(index)))
        .collect();
    let shown = shape_of(http("coral.providers.a.b", Value::Object(one_more)))
        .data()
        .to_string();
    assert!(shown.ends_with("… 1 more key }"), "{shown}");

    // More keys than are tracked make a map, whatever their names.
    let letters = |mut index: usize| {
        let mut name = String::from("key_");
        loop {
            name.push(char::from(b'a' + u8::try_from(index % 26).unwrap()));
            index /= 26;
            if index == 0 {
                return name;
            }
        }
    };
    let mut huge: serde_json::Map<String, Value> = (0..KEYS_TRACKED)
        .map(|index| (letters(index), json!(index)))
        .collect();
    huge.insert("key_beyond".to_string(), json!("text"));
    assert_eq!(
        shape_of(http("coral.providers.a.b", Value::Object(huge))).data(),
        "Record<string, string | number>"
    );
}

#[test]
fn objects_keyed_by_data_show_as_records_without_their_keys() {
    let users = |count: usize| -> Value {
        [
            "ada", "bob", "cy", "dee", "eve", "fay", "gus", "hal", "ivy", "jo", "kai", "lu",
        ]
        .iter()
        .take(count)
        .enumerate()
        .map(|(index, name)| {
            let user = if index == 0 {
                json!({"id": index, "email": "x"})
            } else {
                json!({"id": index})
            };
            ((*name).to_string(), user)
        })
        .collect::<serde_json::Map<_, _>>()
        .into()
    };
    for (data, expected) in [
        // Property names a person chose.
        (
            json!({"object": "page", "properties": {
                "Name": {"id": "title", "type": "title", "title": [{"plain_text": "c0ral"}]},
                "Due date": {"id": "a%3Bc", "type": "date", "date": null},
            }}),
            "{ object: string; properties: Record<string, { id: string; type: string; title?: Array<{ plain_text: string }>; date?: null }> }",
        ),
        (
            json!({"Name": 1, "Statut": 2, "Fällig": 3}),
            "Record<string, number>",
        ),
        // Emails, UUIDs, numeric and hex identifiers, tokens.
        (
            json!({"members": {"ada@example.com": {"role": "admin"}, "bob@example.com": {"role": "member"}}}),
            "{ members: Record<string, { role: string }> }",
        ),
        (
            json!({"a7c9e2d4-5b1f-4c3e-9d8a-6f0b2e1c3d4a": true}),
            "Record<string, boolean>",
        ),
        (
            json!({"3f2b8c1e-0a9d-4e7f-b6c5-1d2e3f4a5b6c": true}),
            "Record<string, boolean>",
        ),
        (
            json!({"cafe1234beef": [1]}),
            "Record<string, Array<number>>",
        ),
        (json!({"12345": "x"}), "Record<string, string>"),
        (json!({"user_98765432": "x"}), "Record<string, string>"),
        (
            json!({"U024BE7LH": {"presence": "away"}}),
            "Record<string, { presence: string }>",
        ),
        (
            json!({"tok_4eC39HqLyjWDarjtT1zdp7dc": null}),
            "Record<string, null>",
        ),
        // Paths, URLs, dotted and long names.
        (json!({"/v1/users": {}}), "Record<string, {}>"),
        (
            json!({"https://example.com/x": 1}),
            "Record<string, number>",
        ),
        (json!({"example.com": 1}), "Record<string, number>"),
        (json!({"urn:team:1": 1}), "Record<string, number>"),
        (json!({ "n".repeat(41): 1 }), "Record<string, number>"),
        (json!({"": 1}), "Record<string, number>"),
        // Many plain names whose values share one kind.
        (
            json!({"by_user": users(12)}),
            "{ by_user: Record<string, { id: number; email?: string }> }",
        ),
    ] {
        let shown = shape_of(http("coral.providers.a.b", data.clone()))
            .data()
            .to_string();
        assert_eq!(shown, expected, "{data}");
    }
    // A few plain names keep their keys.
    assert_eq!(
        shape_of(http("coral.providers.a.b", users(3))).data(),
        "{ ada: { id: number; email: string }; bob: { id: number }; cy: { id: number } }"
    );
}

#[test]
fn ordinary_api_objects_keep_their_field_names() {
    let link = json!({"href": "https://api.example.com/c0ral"});
    let data = json!({
        "id": 1_234,
        "node_id": "I_kwDOABCDEF",
        "title": "c0ral",
        "has_more": false,
        "next_cursor": null,
        "results": [{"object": "page", "created_time": "2026-01-01"}],
        "customfield_10010": null,
        "sha256": "e3b0",
        "x509Certificate": null,
        "content-type": "text/plain",
        "$ref": "#/a",
        "__typename": "Issue",
        "_links": {
            "self": link, "html": link, "issue": link, "comments": link,
            "review_comments": link, "review_comment": link, "commits": link, "statuses": link,
        },
    });
    assert_eq!(
        shape_of(http("coral.providers.github.getPull", data)).data(),
        concat!(
            "{ id: number; node_id: string; title: string; has_more: boolean; next_cursor: null; ",
            "results: Array<{ object: string; created_time: string }>; customfield_10010: null; ",
            "sha256: string; x509Certificate: null; \"content-type\": string; $ref: string; ",
            "__typename: string; _links: { self: { href: string }; html: { href: string }; ",
            "issue: { href: string }; comments: { href: string }; review_comments: { href: string }; ",
            "review_comment: { href: string }; commits: { href: string }; statuses: { href: string } } }",
        )
    );
}

#[test]
fn repeated_calls_with_one_shape_appear_once_in_call_order() {
    let sketched = sketch(
        vec![
            http(
                "coral.providers.slack.history",
                json!({"ok": true, "messages": []}),
            ),
            http("coral.providers.slack.channels", json!({"ok": true})),
            http(
                "coral.providers.slack.history",
                json!({"ok": true, "messages": []}),
            ),
            http(
                "coral.providers.slack.history",
                json!({"ok": false, "error": "not_in_channel"}),
            ),
        ],
        0,
    );
    let shapes: Vec<_> = sketched
        .shapes()
        .iter()
        .map(|shape| (shape.call_path(), shape.invocations(), shape.data()))
        .collect();
    assert_eq!(
        shapes,
        [
            (
                "coral.providers.slack.history",
                2,
                "{ ok: boolean; messages: [] }"
            ),
            ("coral.providers.slack.channels", 1, "{ ok: boolean }"),
            (
                "coral.providers.slack.history",
                1,
                "{ ok: boolean; error: string }"
            ),
        ]
    );
}

#[test]
fn mcp_text_that_is_json_is_also_shaped_as_parsed_text() {
    let shape = shape_of(mcp(
        "coral.providers.linear.listIssues",
        json!({"content": [
            {"type": "text", "text": "{\"issues\":[{\"id\":\"ISS-1\",\"state\":\"open\"}]}"},
            {"type": "text", "text": "not json"},
            {"type": "text", "text": "\"a json string\""},
            {"type": "text", "text": "{\"issues\":[],\"cursor\":\"abc\"}"},
            {"type": "image", "data": "", "mimeType": "image/png"},
        ]}),
    ));
    assert_eq!(
        shape.data(),
        "{ content: Array<{ type: string; text?: string; data?: string; mimeType?: string }> }"
    );
    assert_eq!(
        shape.parsed_text(),
        Some("{ issues: Array<{ id: string; state: string }>; cursor?: string }")
    );
    // HTTP text is the body the Program received, not MCP content.
    assert_eq!(
        shape_of(http(
            "coral.providers.a.b",
            json!({"content": [{"type": "text", "text": "{\"a\":1}"}]})
        ))
        .parsed_text(),
        None
    );
    assert_eq!(
        shape_of(mcp(
            "coral.providers.a.b",
            json!({"content": [{"type": "text", "text": "plain"}]})
        ))
        .parsed_text(),
        None
    );
}

#[test]
fn shapes_fit_the_byte_budget_by_shrinking_the_largest_then_dropping_the_earliest() {
    let wide = |prefix: &str| -> Value {
        let mut nested = json!({"leaf": "x"});
        for level in 0..MAX_DEPTH {
            // Two keys nest, so each level doubles the text.
            let fields: serde_json::Map<String, Value> = (0..KEYS_SHOWN)
                .map(|index| {
                    let value = if index < 2 {
                        nested.clone()
                    } else {
                        json!(index)
                    };
                    (format!("{prefix}{level}_{index:02}_padding"), value)
                })
                .collect();
            nested = Value::Object(fields);
        }
        nested
    };
    let sketched = sketch(
        vec![
            http("coral.providers.a.first", wide("f")),
            http("coral.providers.a.small", json!({"ok": true})),
        ],
        0,
    );
    let total: usize = sketched
        .shapes()
        .iter()
        .map(|shape| shape.call_path().len() + shape.data().len())
        .sum();
    assert!(total <= SHAPES_MAX_BYTES, "{total}");
    assert_eq!(sketched.unsketched(), 0);
    let [first, small] = sketched.shapes() else {
        panic!("both shapes stay: {sketched:?}");
    };
    assert_eq!(small.data(), "{ ok: boolean }");
    assert!(first.data().contains("{…}"), "{}", first.data());

    // Shapes that cannot fit even one level deep go, earliest first.
    let many: Vec<_> = (0..RESULTS_READ)
        .map(|index| http(&format!("coral.providers.a.tool{index}"), wide("w")))
        .collect();
    let sketched = sketch(many, 7);
    let total: usize = sketched
        .shapes()
        .iter()
        .map(|shape| shape.call_path().len() + shape.data().len())
        .sum();
    assert!(total <= SHAPES_MAX_BYTES, "{total}");
    let kept = sketched.shapes().len() as u64;
    assert!(kept > 0 && kept < RESULTS_READ);
    assert_eq!(sketched.unsketched(), 7 + RESULTS_READ - kept);
    assert_eq!(
        sketched.shapes().last().map(ToolResultShape::call_path),
        Some(format!("coral.providers.a.tool{}", RESULTS_READ - 1).as_str()),
        "the latest results stay"
    );
}

#[test]
fn no_value_ever_appears() {
    let canary = "c0ral-canary";
    let data = json!({
        "status": "c0ral-canary-enum",
        "count": 4_815_162_342_u64,
        "ratio": 0.271_828,
        "flag": false,
        "items": [{"kind": "c0ral-canary-kind", "id": 1_234_567}, {"kind": "c0ral-canary-other"}],
        "map": {"key": "c0ral-canary-map"},
        "members": {"c0ral-canary@example.com": {"role": "c0ral-canary-role"}},
        "by_id": {"c0ral-canary-4eC39HqLyj": 1},
    });
    let text = serde_json::to_string(&data).unwrap();
    let sketched = sketch(
        vec![
            http("coral.providers.a.b", data),
            mcp(
                "coral.providers.a.c",
                json!({"content": [{"type": "text", "text": text}], "isError": false}),
            ),
        ],
        0,
    );
    let rendered = format!("{sketched:?}");
    for value in [canary, "4815162342", "0.271828", "1234567", "false"] {
        assert!(!rendered.contains(value), "{value} appeared in {rendered}");
    }
}
