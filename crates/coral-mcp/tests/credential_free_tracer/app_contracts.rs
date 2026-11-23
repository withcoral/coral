//! What the MCP App shows for each result its host hands it. The app's own
//! script runs here as a Program, against a stand-in for the page and the
//! host, so the states are the ones the served HTML decides.

use serde_json::{Value, json};

use super::{RawSession, Tracer};
use coral_mcp::SUPPORTED_PROTOCOL_VERSIONS;

const APP: &str = "ui://coral/required-actions";

/// The page and the host, as far as the app's script touches them. The page
/// starts hidden, as the markup has it, and measures 40 pixels when shown.
const PAGE: &str = r#"
const posted = [];
function node(tag) {
  return {
    tag,
    className: "",
    textContent: "",
    hidden: false,
    children: [],
    append(...items) {
      this.children.push(...items);
    },
    replaceChildren() {
      this.children = [];
    },
    setAttribute() {},
    addEventListener() {},
    get childElementCount() {
      return this.children.filter((child) => typeof child === "object").length;
    },
  };
}
const page = node("main");
page.hidden = true;
const document = {
  body: { scrollHeight: 40 },
  documentElement: { scrollWidth: 320, style: { setProperty() {} }, dataset: {} },
  getElementById: () => page,
  createElement: node,
  createElementNS: (_namespace, tag) => node(tag),
};
const window = {
  parent: {
    postMessage(message) {
      posted.push(message);
    },
  },
  addEventListener(_name, handler) {
    this.deliver = handler;
  },
};
class ResizeObserver {
  observe() {}
}
const setTimeout = () => 0;
"#;

/// Hands the app one host message after another and reads the page after
/// each: whether it is hidden, the height it last reported, the title and
/// the control of each card, and each line of plain text.
const HOST: &str = r#"
function text(item) {
  return typeof item === "string" ? item : item.textContent + item.children.map(text).join("");
}
function find(item, tag) {
  if (typeof item !== "object") return null;
  if (item.tag === tag) return item;
  return item.children.map((child) => find(child, tag)).find(Boolean) || null;
}
function host(method, params) {
  window.deliver({ source: window.parent, data: { jsonrpc: "2.0", method, params } });
  const sizes = posted.filter((message) => message.method === "ui/notifications/size-changed");
  return {
    hidden: page.hidden,
    height: sizes[sizes.length - 1].params.height,
    cards: page.children
      .filter((child) => child.tag === "article")
      .map((card) => [text(find(card, "h1")), text(find(card, "button"))]),
    lines: page.children.filter((child) => child.tag === "p").map(text),
  };
}
const yours = {
  kind: "invocation_approval",
  request_id: "3d5810c7-7e01-46a7-be5c-9a04585dc57f",
  required_actor: "invoking_user",
  tool: "coral.providers.weather.getForecast",
  provider: "Weather",
  deadline_at: "not a time",
  url: "https://reef.example.test/approvals/3d5810c7-7e01-46a7-be5c-9a04585dc57f",
};
const anOwners = { ...yours, required_actor: "workspace_owner" };
const listing = (required_actions) => ({
  structuredContent: { run_id: "a-run", state: "running", required_actions },
});
return {
  hidden_at_first: page.hidden,
  an_entry_for_the_person: host("ui/notifications/tool-result", listing([yours, anOwners])),
  no_entry_for_the_person: host("ui/notifications/tool-result", listing([anOwners])),
  a_failed_call: host("ui/notifications/tool-result", {
    isError: true,
    structuredContent: { code: "run_not_found", message: "Run not found", trace_id: "a-trace" },
  }),
  a_cancelled_call: host("ui/notifications/tool-cancelled", {}),
};
"#;

/// The app shows a card for each entry that names the person and a line
/// when a result lists none. A failed or a cancelled call says nothing about
/// what waits, so the app shows nothing for it and reports no height.
#[tokio::test(flavor = "multi_thread")]
async fn the_app_shows_what_a_result_lists_and_nothing_for_a_call_without_a_result() {
    let tracer = Tracer::start().await;
    let version = SUPPORTED_PROTOCOL_VERSIONS
        .first()
        .expect("a supported version");
    let session = RawSession::open(&tracer.endpoint(), version).await;
    let app = session
        .result("resources/read", json!({"uri": APP}))
        .await
        .expect("the app is readable");
    let html = app
        .pointer("/contents/0/text")
        .and_then(Value::as_str)
        .expect("the app's HTML");
    assert!(
        html.contains(r#"<main id="root" hidden></main>"#),
        "the page starts hidden, as the stand-in has it"
    );
    let script = html
        .split_once("<script>")
        .and_then(|(_, rest)| rest.split_once("</script>"))
        .map(|(script, _)| script)
        .expect("the app's one script");

    let ran = session
        .tool(
            "exec",
            json!({
                "goal": "Run the app's script", "intent": "Read what the app shows",
                // The script is JavaScript, so the compiler does not type it.
                "program": format!("// @ts-nocheck\n{PAGE}\n{script}\n{HOST}"),
            }),
        )
        .await;
    assert_eq!(ran["isError"], false, "{ran}");

    let nothing = json!({"hidden": true, "height": 0, "cards": [], "lines": []});
    assert_eq!(
        ran.pointer("/structuredContent/final_value/value"),
        Some(&json!({
            "hidden_at_first": true,
            "an_entry_for_the_person": {
                "hidden": false, "height": 40,
                "cards": [["Approve a Weather call", "Open in Coral"]], "lines": []
            },
            "no_entry_for_the_person": {
                "hidden": false, "height": 40,
                "cards": [], "lines": ["No request is waiting for you."]
            },
            "a_failed_call": nothing,
            "a_cancelled_call": nothing,
        })),
        "{ran}"
    );
}
