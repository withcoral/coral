//! Stand-in for the text the Sub-agent's model reads.

pub(crate) struct ToolText {
    pub(crate) name: &'static str,
    pub(crate) description: &'static str,
    /// The JSON Schema of the tool's arguments, as JSON text.
    pub(crate) parameters: &'static str,
}

const ARGUMENTS: &str = r#"{"type":"object"}"#;

pub(crate) const TOOLS: [ToolText; 3] = [
    ToolText {
        name: "exec",
        description: "Run a Program.",
        parameters: ARGUMENTS,
    },
    ToolText {
        name: "wait",
        description: "Wait for a Run.",
        parameters: ARGUMENTS,
    },
    ToolText {
        name: "cancel",
        description: "Cancel a Run.",
        parameters: ARGUMENTS,
    },
];

pub(crate) fn system_prompt(providers: &[coral_catalog::ListedProvider]) -> String {
    format!(
        "You are a Sub-agent. The workspace has {} providers.",
        providers.len()
    )
}
