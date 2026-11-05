//! The MCP App that shows a Run's Required Action in a client that renders
//! one: a card with what is asked and a control that opens the page where
//! the person decides. A client without the extension sees only the Run
//! Update, as before.

use rmcp::model::{ExtensionCapabilities, MetaObject, ReadResourceResult, ResourceContents};
use serde_json::json;

/// The extension identifier of MCP Apps.
pub(crate) const EXTENSION: &str = "io.modelcontextprotocol/ui";
/// The one readable resource: the app's HTML.
pub(crate) const RESOURCE_URI: &str = "ui://coral/required-actions";
/// The content type MCP Apps render.
pub(crate) const MIME_TYPE: &str = "text/html;profile=mcp-app";
const HTML: &str = include_str!("app/required-actions.html");

/// The server's declaration of the extension.
pub(crate) fn extensions() -> ExtensionCapabilities {
    let mut extensions = ExtensionCapabilities::new();
    let settings = json!({"mimeTypes": [MIME_TYPE]});
    extensions.insert(
        EXTENSION.to_owned(),
        settings.as_object().cloned().unwrap_or_default(),
    );
    extensions
}

/// Links a tool's result to the app.
pub(crate) fn link_tool(meta: &mut MetaObject) {
    meta.0
        .insert("ui".to_owned(), json!({"resourceUri": RESOURCE_URI}));
}

/// The app itself. It loads nothing from any origin, so the host's
/// restrictive default policy applies.
pub(crate) fn resource() -> ReadResourceResult {
    let ui = json!({"ui": {"prefersBorder": false}});
    ReadResourceResult::new(vec![ResourceContents::TextResourceContents {
        uri: RESOURCE_URI.to_owned(),
        mime_type: Some(MIME_TYPE.to_owned()),
        text: HTML.to_owned(),
        meta: Some(MetaObject(ui.as_object().cloned().unwrap_or_default())),
    }])
}
