use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: String,
    pub severity: DiagnosticSeverity,
    pub message: String,
    pub surface_id: Option<String>,
    pub operation_id: Option<String>,
    pub projection_name: Option<String>,
}

impl Diagnostic {
    pub(crate) fn warning(
        code: &str,
        message: impl Into<String>,
        surface_id: impl Into<String>,
        operation_id: Option<String>,
    ) -> Self {
        Self {
            code: code.to_string(),
            severity: DiagnosticSeverity::Warning,
            message: message.into(),
            surface_id: Some(surface_id.into()),
            operation_id,
            projection_name: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Info,
    Warning,
    Error,
}
