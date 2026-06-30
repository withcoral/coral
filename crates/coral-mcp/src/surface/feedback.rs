use rmcp::model::{Tool, ToolAnnotations};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::{Map, Value};

use super::{
    arguments::required_string_argument,
    schema::{tool_input_schema, tool_output_schema},
    tool_names::ToolName,
};

#[derive(JsonSchema)]
pub(crate) struct FeedbackArguments {
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "What you were trying to do."
    )]
    pub(crate) trying_to_do: String,
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "What you already tried."
    )]
    pub(crate) tried: String,
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "Where you got blocked."
    )]
    pub(crate) stuck: String,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct FeedbackStoredValue {
    pub(crate) feedback_id: String,
    pub(crate) created_at: String,
    pub(crate) message: &'static str,
}

pub(crate) fn feedback_tool() -> Tool {
    Tool::new(
        ToolName::Feedback.as_str(),
        "Submit feedback when you are blocked. Coral stores the report locally and uploads an anonymous copy, without user identifiers, to Coral's hosted feedback service to improve Coral's performance.",
        tool_input_schema::<FeedbackArguments>(),
    )
    .with_raw_output_schema(tool_output_schema::<FeedbackStoredValue>())
    .with_annotations(
        ToolAnnotations::with_title("Store Feedback Report")
            .read_only(false)
            .destructive(false)
            .idempotent(false)
            .open_world(true),
    )
}

pub(crate) fn feedback_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<FeedbackArguments, rmcp::ErrorData> {
    Ok(FeedbackArguments {
        trying_to_do: required_string_argument(arguments, "trying_to_do")?,
        tried: required_string_argument(arguments, "tried")?,
        stuck: required_string_argument(arguments, "stuck")?,
    })
}
