use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostic {
    pub message: String,
    pub operation_id: Option<String>,
}

impl Diagnostic {
    pub fn new(message: impl Into<String>, operation_id: Option<String>) -> Self {
        Self {
            message: message.into(),
            operation_id,
        }
    }
}
