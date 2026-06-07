use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ImageDetail {
    High,
    Original,
}

pub const DEFAULT_IMAGE_DETAIL: ImageDetail = ImageDetail::High;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputShapingMetadata {
    pub channel: String,
    pub limit_name: String,
    pub truncated: bool,
    pub spilled: bool,
    pub dropped_items: usize,
    pub observed_items: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configured_items: Option<usize>,
    pub observed_bytes: usize,
    pub dropped_bytes: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configured_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_tokens: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configured_tokens: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_output_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FunctionCallOutputContentItem {
    Text {
        text: String,
    },
    InputImage {
        image_url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<ImageDetail>,
    },
    OutputShaping {
        #[serde(flatten)]
        metadata: OutputShapingMetadata,
    },
}
