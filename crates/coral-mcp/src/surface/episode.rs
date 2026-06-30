use std::sync::Arc;

use coral_api::{CORAL_EPISODE_ID_MAX_LEN, CORAL_EPISODE_INTENT_MAX_CHARS};
use rmcp::{
    ErrorData,
    model::{Tool, ToolAnnotations},
};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::{Map, Value, json};

use super::{
    arguments::required_string_argument,
    schema::{json_schema_value, tool_input_schema, tool_output_schema},
    tool_names::ToolName,
};

const EPISODE_ID_ARGUMENT_DESCRIPTION: &str = "Optional episode id returned by open_episode. Pass it on subsequent Coral tool calls for the same task so Coral can attribute the call to that episode.";
const EPISODE_ID_JSON_SCHEMA_PATTERN: &str = "^[!-~]+$";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, JsonSchema)]
#[serde(transparent)]
#[schemars(transparent, inline)]
pub(crate) struct EpisodeId(
    #[schemars(
        length(min = 1, max = CORAL_EPISODE_ID_MAX_LEN),
        regex(pattern = EPISODE_ID_JSON_SCHEMA_PATTERN)
    )]
    String,
);

impl EpisodeId {
    pub(crate) fn generated(value: String) -> Self {
        debug_assert!(Self::is_valid(&value));
        Self(value)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    fn parse_argument(key: &str, value: &str) -> Result<Self, ErrorData> {
        if value.is_empty() {
            return Err(ErrorData::invalid_params(
                format!("argument '{key}' must not be empty"),
                None,
            ));
        }
        if value.len() > CORAL_EPISODE_ID_MAX_LEN {
            return Err(ErrorData::invalid_params(
                format!("argument '{key}' must be at most {CORAL_EPISODE_ID_MAX_LEN} bytes"),
                None,
            ));
        }
        if !value.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(ErrorData::invalid_params(
                format!("argument '{key}' must be graphic ASCII with no spaces or control bytes"),
                None,
            ));
        }
        Ok(Self(value.to_string()))
    }

    fn is_valid(value: &str) -> bool {
        !value.is_empty()
            && value.len() <= CORAL_EPISODE_ID_MAX_LEN
            && value.bytes().all(|byte| byte.is_ascii_graphic())
    }
}

#[derive(JsonSchema)]
pub(crate) struct OpenEpisodeArguments {
    #[schemars(
        length(min = 1, max = CORAL_EPISODE_INTENT_MAX_CHARS),
        pattern(r"\S"),
        description = "Natural-language description of the task this episode should group."
    )]
    pub(crate) intent: String,
    #[schemars(
        description = "Optional parent episode id when this task is a child of an existing episode."
    )]
    pub(crate) parent_episode_id: Option<EpisodeId>,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct EpisodeOpenedValue {
    pub(crate) episode_id: EpisodeId,
    pub(crate) parent_episode_id: Option<EpisodeId>,
    pub(crate) message: &'static str,
    pub(crate) instructions: &'static str,
}

pub(crate) fn open_episode_tool() -> Tool {
    Tool::new(
        ToolName::OpenEpisode.as_str(),
        "Open a Coral episode for the current task. Call this once at the start of a task, then pass the returned episode_id on subsequent Coral tool calls for that task.",
        tool_input_schema::<OpenEpisodeArguments>(),
    )
    .with_raw_output_schema(tool_output_schema::<EpisodeOpenedValue>())
    .with_annotations(
        ToolAnnotations::with_title("Open Episode")
            .read_only(false)
            .destructive(false)
            .idempotent(false)
            .open_world(false),
    )
}

pub(crate) fn open_episode_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<OpenEpisodeArguments, ErrorData> {
    Ok(OpenEpisodeArguments {
        intent: required_string_argument(arguments, "intent")?,
        parent_episode_id: optional_episode_id_argument(arguments, "parent_episode_id")?,
    })
}

pub(crate) fn optional_episode_id_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<EpisodeId>, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let value = value.as_str().ok_or_else(|| {
        ErrorData::invalid_params(format!("argument '{key}' must be a string"), None)
    })?;
    EpisodeId::parse_argument(key, value).map(Some)
}

pub(crate) fn with_episode_id_argument(mut tool: Tool) -> Tool {
    add_episode_id_property(Arc::make_mut(&mut tool.input_schema));
    tool
}

fn add_episode_id_property(schema: &mut Map<String, Value>) {
    schema
        .entry("properties")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .expect("tool input properties are an object")
        .insert(
            "episode_id".to_string(),
            nullable_episode_id_schema(Some(EPISODE_ID_ARGUMENT_DESCRIPTION)),
        );
}

fn nullable_episode_id_schema(description: Option<&str>) -> Value {
    let mut schema = json_schema_value::<Option<EpisodeId>>();
    if let Some(description) = description {
        schema
            .as_object_mut()
            .expect("nullable episode id schema is an object")
            .insert("description".to_string(), json!(description));
    }
    schema
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, Value};

    use super::{EpisodeId, optional_episode_id_argument};

    #[test]
    fn episode_id_argument_accepts_valid_graphic_ascii() {
        let arguments = Map::from_iter([(
            "episode_id".to_string(),
            Value::String("episode-1".to_string()),
        )]);

        let parsed = optional_episode_id_argument(Some(&arguments), "episode_id")
            .expect("episode id should parse");

        assert_eq!(parsed.as_ref().map(EpisodeId::as_str), Some("episode-1"));
    }
}
