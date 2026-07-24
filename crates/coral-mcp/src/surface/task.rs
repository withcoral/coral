use std::{borrow::Cow, fmt, sync::Arc};

use coral_api::CORAL_TASK_INTENT_MAX_CHARS;
use rmcp::{
    ErrorData,
    model::{Tool, ToolAnnotations},
};
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::{
    arguments::required_string_argument,
    schema::{tool_input_schema, tool_output_schema},
    tool_names::ToolName,
};

const TASK_ID_ARGUMENT_DESCRIPTION: &str = "Task id returned by start_task. Pass it on subsequent Coral tool calls for the same work so Coral can attribute those calls to that task.";
const TOOL_INTENT_ARGUMENT_DESCRIPTION: &str =
    "Natural-language description of why this MCP tool call is needed for the current task.";
const TOOL_INTENT_JSON_SCHEMA_PATTERN: &str = r".*\S.*";
const TASK_ID_LEN: usize = 36;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub(crate) struct TaskId(uuid::Uuid);

impl TaskId {
    pub(crate) fn from_uuid_str(value: &str) -> Result<Self, uuid::Error> {
        uuid::Uuid::parse_str(value).map(Self)
    }

    pub(crate) fn parse_argument(key: &str, value: &str) -> Result<Self, ErrorData> {
        Self::from_uuid_str(value).map_err(|_err| {
            ErrorData::invalid_params(format!("argument '{key}' must be a UUID"), None)
        })
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl JsonSchema for TaskId {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "TaskId".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        Schema::from(task_id_schema_map(None))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TaskStatus {
    Success,
    Failure,
}

pub(crate) struct StartTaskArguments {
    pub(crate) intent: String,
}

#[derive(JsonSchema)]
#[schemars(deny_unknown_fields)]
#[expect(
    dead_code,
    reason = "schema-only struct for the end_task input contract"
)]
pub(crate) struct EndTaskArgumentsSchema {
    #[schemars(description = "Task id returned by start_task.")]
    task_id: TaskId,
    #[schemars(description = "Final task status.")]
    task_status: TaskStatus,
}

pub(crate) struct EndTaskArguments {
    pub(crate) task_id: TaskId,
    pub(crate) task_status: TaskStatus,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct TaskStartedValue {
    pub(crate) task_id: TaskId,
    pub(crate) message: &'static str,
    pub(crate) instructions: &'static str,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct TaskEndedValue {
    pub(crate) task_id: TaskId,
    pub(crate) task_status: TaskStatus,
    pub(crate) note: &'static str,
}

pub(crate) fn start_task_tool() -> Tool {
    Tool::new(
        ToolName::StartTask.as_str(),
        "Start a Coral task for the current unit of work.",
        start_task_input_schema(),
    )
    .with_raw_output_schema(tool_output_schema::<TaskStartedValue>())
    .with_annotations(
        ToolAnnotations::with_title("Start Task")
            .read_only(false)
            .destructive(false)
            .idempotent(false)
            .open_world(false),
    )
}

fn start_task_input_schema() -> Arc<Map<String, Value>> {
    let schema = json!({
        "type": "object",
        "properties": {
            "intent": {
                "type": "string",
                "minLength": 1,
                "maxLength": CORAL_TASK_INTENT_MAX_CHARS,
                "pattern": TOOL_INTENT_JSON_SCHEMA_PATTERN,
                "description": "Natural-language description of the work this task should group."
            }
        },
        "required": ["intent"],
        "additionalProperties": false
    });
    Arc::new(
        schema
            .as_object()
            .expect("start_task input schema is an object")
            .clone(),
    )
}

pub(crate) fn end_task_tool() -> Tool {
    Tool::new(
        ToolName::EndTask.as_str(),
        "End a Coral task with a final status.",
        tool_input_schema::<EndTaskArgumentsSchema>(),
    )
    .with_raw_output_schema(tool_output_schema::<TaskEndedValue>())
    .with_annotations(
        ToolAnnotations::with_title("End Task")
            .read_only(false)
            .destructive(false)
            .idempotent(false)
            .open_world(false),
    )
}

pub(crate) fn start_task_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<StartTaskArguments, ErrorData> {
    reject_unknown_arguments(arguments, &["intent"])?;
    Ok(StartTaskArguments {
        intent: required_tool_intent_argument(arguments, "intent")?,
    })
}

pub(crate) fn end_task_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<EndTaskArguments, ErrorData> {
    reject_unknown_arguments(arguments, &["task_id", "task_status"])?;
    Ok(EndTaskArguments {
        task_id: required_task_id_argument(arguments, "task_id")?,
        task_status: required_task_status_argument(arguments, "task_status")?,
    })
}

pub(crate) fn required_task_id_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<TaskId, ErrorData> {
    let value = required_string_argument(arguments, key)?;
    TaskId::parse_argument(key, &value)
}

fn required_task_status_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<TaskStatus, ErrorData> {
    let value = required_string_argument(arguments, key)?;
    match value.as_str() {
        "success" => Ok(TaskStatus::Success),
        "failure" => Ok(TaskStatus::Failure),
        _ => Err(ErrorData::invalid_params(
            format!("argument '{key}' must be 'success' or 'failure'"),
            None,
        )),
    }
}

fn reject_unknown_arguments(
    arguments: Option<&Map<String, Value>>,
    allowed: &[&str],
) -> Result<(), ErrorData> {
    let Some(arguments) = arguments else {
        return Ok(());
    };
    if let Some(key) = arguments
        .keys()
        .find(|key| !allowed.contains(&key.as_str()))
    {
        return Err(ErrorData::invalid_params(
            format!("unknown argument '{key}'"),
            None,
        ));
    }
    Ok(())
}

pub(crate) fn required_tool_intent_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<String, ErrorData> {
    let intent = required_string_argument(arguments, key)?;
    if intent.chars().count() > CORAL_TASK_INTENT_MAX_CHARS {
        return Err(ErrorData::invalid_params(
            format!("argument '{key}' must be at most {CORAL_TASK_INTENT_MAX_CHARS} characters"),
            None,
        ));
    }
    Ok(intent)
}

pub(crate) fn with_task_context_arguments(mut tool: Tool) -> Tool {
    let schema = Arc::make_mut(&mut tool.input_schema);
    add_task_id_property(schema);
    add_tool_intent_property(schema);
    require_properties(schema, &["task_id", "intent"]);
    tool
}

fn add_task_id_property(schema: &mut Map<String, Value>) {
    schema
        .entry("properties")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .expect("tool input properties are an object")
        .insert(
            "task_id".to_string(),
            task_id_schema(Some(TASK_ID_ARGUMENT_DESCRIPTION)),
        );
}

fn add_tool_intent_property(schema: &mut Map<String, Value>) {
    schema
        .entry("properties")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .expect("tool input properties are an object")
        .insert(
            "intent".to_string(),
            json!({
                "type": "string",
                "minLength": 1,
                "maxLength": CORAL_TASK_INTENT_MAX_CHARS,
                "pattern": TOOL_INTENT_JSON_SCHEMA_PATTERN,
                "description": TOOL_INTENT_ARGUMENT_DESCRIPTION
            }),
        );
}

fn require_properties(schema: &mut Map<String, Value>, properties: &[&str]) {
    let required = schema
        .entry("required")
        .or_insert_with(|| json!([]))
        .as_array_mut()
        .expect("tool input required fields are an array");
    for property in properties {
        if !required
            .iter()
            .any(|value| value.as_str() == Some(property))
        {
            required.push(Value::String((*property).to_string()));
        }
    }
}

fn task_id_schema(description: Option<&str>) -> Value {
    Value::Object(task_id_schema_map(description))
}

fn task_id_schema_map(description: Option<&str>) -> Map<String, Value> {
    let mut schema = Map::from_iter([
        ("type".to_string(), json!("string")),
        ("format".to_string(), json!("uuid")),
        ("minLength".to_string(), json!(TASK_ID_LEN)),
        ("maxLength".to_string(), json!(TASK_ID_LEN)),
    ]);
    if let Some(description) = description {
        schema.insert("description".to_string(), json!(description));
    }
    schema
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, Value, json};

    use super::{TaskStatus, end_task_arguments, required_task_id_argument, start_task_arguments};

    const TASK_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

    #[test]
    fn task_id_argument_accepts_uuid() {
        let arguments = Map::from_iter([("task_id".to_string(), Value::String(TASK_ID.into()))]);

        let parsed =
            required_task_id_argument(Some(&arguments), "task_id").expect("task id should parse");

        assert_eq!(parsed.to_string(), TASK_ID);
    }

    #[test]
    fn task_id_argument_rejects_malformed_uuid() {
        let arguments =
            Map::from_iter([("task_id".to_string(), Value::String("task-1".to_string()))]);

        let error = required_task_id_argument(Some(&arguments), "task_id")
            .expect_err("task id should be rejected");

        assert!(error.to_string().contains("must be a UUID"));
    }

    #[test]
    fn start_task_argument_parses_intent() {
        let parsed = start_task_arguments(Some(&Map::from_iter([(
            "intent".to_string(),
            json!("Investigate renewal risk"),
        )])))
        .expect("start task arguments");

        assert_eq!(parsed.intent, "Investigate renewal risk");
    }

    #[test]
    fn start_task_argument_rejects_overlong_intent() {
        let overlong_intent = "a".repeat(coral_api::CORAL_TASK_INTENT_MAX_CHARS + 1);

        let result = start_task_arguments(Some(&Map::from_iter([(
            "intent".to_string(),
            Value::String(overlong_intent),
        )])));
        let Err(error) = result else {
            panic!("overlong intent should be rejected");
        };

        assert!(error.to_string().contains("must be at most"));
    }

    #[test]
    fn end_task_argument_parses_status() {
        let parsed = end_task_arguments(Some(&Map::from_iter([
            ("task_id".to_string(), json!(TASK_ID)),
            ("task_status".to_string(), json!("success")),
        ])))
        .expect("end task arguments");

        assert_eq!(parsed.task_id.to_string(), TASK_ID);
        assert_eq!(parsed.task_status, TaskStatus::Success);
    }

    #[test]
    fn start_task_schema_requires_only_intent() {
        let tool = super::start_task_tool();
        let schema = Value::Object((*tool.input_schema).clone());
        let compiled = jsonschema::validator_for(&schema).expect("start_task schema compiles");

        assert!(compiled.is_valid(&json!({
            "intent": "Investigate renewal risk"
        })));
        assert!(!compiled.is_valid(&json!({
            "intent": "",
        })));
        assert!(!compiled.is_valid(&json!({
            "intent": " ",
        })));
        assert!(!compiled.is_valid(&json!({
            "intent": "Investigate renewal risk",
            "initialize_session": true
        })));
    }

    #[test]
    fn end_task_schema_rejects_unknown_fields() {
        let tool = super::end_task_tool();
        let schema = Value::Object((*tool.input_schema).clone());
        let compiled = jsonschema::validator_for(&schema).expect("end_task schema compiles");

        assert!(compiled.is_valid(&json!({
            "task_id": TASK_ID,
            "task_status": "success"
        })));
        assert!(!compiled.is_valid(&json!({
            "intent": "Record task completion",
            "task_id": TASK_ID,
            "task_status": "success",
        })));
        assert!(!compiled.is_valid(&json!({
            "task_id": TASK_ID,
            "task_status": "success",
            "extra": true
        })));
    }
}
