//! What an Organization Owner enters, checked once.

use coral_outbound::DestinationPolicy;
use serde_json::{Map, Value};
use url::Url;

use crate::error::{Error, InvalidSetting};

/// The fewest tokens a model's context window may hold.
pub const CONTEXT_WINDOW_MIN: u32 = 48_000;

/// The names of the request args that Coral passes on, each with any value.
///
/// None of them changes who receives the data, so a change to the args needs
/// no new consent of a Workspace Owner. A list of refused names could not
/// promise that: a provider can add a field that forwards data.
pub const REQUEST_ARG_NAMES: [&str; 13] = [
    "chat_template_kwargs",
    "max_completion_tokens",
    "max_output_tokens",
    "max_tokens",
    "output_config",
    "reasoning",
    "reasoning_effort",
    "service_tier",
    "speed",
    "temperature",
    "thinking",
    "top_k",
    "top_p",
];

/// The longest base URL, in bytes.
const BASE_URL_MAX: usize = 2_048;
/// The longest model name, in characters.
const MODEL_MAX: usize = 200;
/// The largest request args, in bytes of JSON as entered.
const REQUEST_ARGS_MAX: usize = 16 * 1024;

/// The wire protocol that a model's endpoint speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelProtocol {
    /// OpenAI-compatible Chat Completions.
    ChatCompletions,
    /// `OpenAI`'s Responses API.
    OpenAiResponses,
    /// Anthropic's Messages API.
    Anthropic,
}

impl ModelProtocol {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::ChatCompletions => "chat_completions",
            Self::OpenAiResponses => "openai_responses",
            Self::Anthropic => "anthropic",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "chat_completions" => Ok(Self::ChatCompletions),
            "openai_responses" => Ok(Self::OpenAiResponses),
            "anthropic" => Ok(Self::Anthropic),
            _ => Err(Error::CorruptData("a model has an unknown protocol")),
        }
    }
}

/// A model's settings but its API key, as a person entered them.
#[derive(Clone, Debug)]
pub struct EnteredSettings {
    /// The wire protocol of the endpoint.
    pub protocol: ModelProtocol,
    /// Where the endpoint is.
    pub base_url: String,
    /// The model's name, as the endpoint knows it.
    pub model: String,
    /// The model's context window in tokens, or `None` to leave it to Coral.
    pub context_window: Option<u32>,
    /// What to merge into each request: a JSON object, or empty for none.
    pub request_args_json: String,
}

/// A model's settings but its API key, once [`parse`](Self::parse) checked them.
#[derive(Clone, Debug, PartialEq)]
pub struct ModelSettings {
    pub(crate) protocol: ModelProtocol,
    pub(crate) base_url: String,
    pub(crate) model: String,
    pub(crate) context_window: Option<u32>,
    pub(crate) request_args: Map<String, Value>,
}

impl ModelSettings {
    /// Checks what a person entered, under the operator's destination policy.
    ///
    /// The check of the destination reads the URL alone. `coral_outbound`
    /// checks each address that it resolves to when a request is sent.
    pub(crate) fn parse(
        policy: DestinationPolicy,
        entered: &EnteredSettings,
    ) -> Result<Self, Error> {
        let model = entered.model.trim();
        if model.is_empty()
            || model.chars().count() > MODEL_MAX
            || model.chars().any(char::is_control)
        {
            return Err(Error::Invalid(InvalidSetting::Model));
        }
        if entered
            .context_window
            .is_some_and(|tokens| tokens < CONTEXT_WINDOW_MIN)
        {
            return Err(Error::Invalid(InvalidSetting::ContextWindow));
        }
        Ok(Self {
            protocol: entered.protocol,
            base_url: parse_base_url(policy, &entered.base_url)?,
            model: model.to_owned(),
            context_window: entered.context_window,
            request_args: parse_request_args(&entered.request_args_json)?,
        })
    }

    /// The wire protocol of the endpoint.
    #[must_use]
    pub const fn protocol(&self) -> ModelProtocol {
        self.protocol
    }

    /// Where the endpoint is, in one spelling, with no slash at its end.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The model's name, as the endpoint knows it.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    /// The model's context window in tokens, when the Owner set one.
    #[must_use]
    pub const fn context_window(&self) -> Option<u32> {
        self.context_window
    }

    /// What Coral merges into each request, by field name.
    #[must_use]
    pub const fn request_args(&self) -> &Map<String, Value> {
        &self.request_args
    }
}

/// The base URL in one spelling, so that two spellings of one destination
/// compare equal.
fn parse_base_url(policy: DestinationPolicy, entered: &str) -> Result<String, Error> {
    let invalid = || Error::Invalid(InvalidSetting::BaseUrl);
    let url = Url::parse(entered.trim())
        .ok()
        .filter(|url| matches!(url.scheme(), "http" | "https") && url.has_host())
        .filter(|url| url.username().is_empty() && url.password().is_none())
        .filter(|url| url.query().is_none() && url.fragment().is_none())
        .ok_or_else(invalid)?;
    let spelled = url.as_str().trim_end_matches('/').to_owned();
    if spelled.len() > BASE_URL_MAX {
        return Err(invalid());
    }
    policy
        .validate(&url)
        .map_err(|_rejected| Error::Invalid(InvalidSetting::Destination))?;
    Ok(spelled)
}

fn parse_request_args(entered: &str) -> Result<Map<String, Value>, Error> {
    let invalid = || Error::Invalid(InvalidSetting::RequestArgs);
    if entered.trim().is_empty() {
        return Ok(Map::new());
    }
    if entered.len() > REQUEST_ARGS_MAX {
        return Err(invalid());
    }
    let args: Map<String, Value> =
        serde_json::from_str(entered).map_err(|_not_an_object| invalid())?;
    if args
        .keys()
        .any(|name| !REQUEST_ARG_NAMES.contains(&name.as_str()))
    {
        return Err(Error::Invalid(InvalidSetting::RequestArgName));
    }
    Ok(args)
}
