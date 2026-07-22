use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Copy, PartialOrd, Eq, Hash, Ord)]
pub enum DiagnosticCode {
    DiagnosticsUnavailable,
    FingerprintHeaderMismatch,
    FingerprintSurfaceMismatch,
    FingerprintUnavailable,
    ManifestFingerprintMismatch,
    McpInputSchemaCompositionUnsupported,
    McpInputSchemaConflict,
    McpInputSchemaDepthExceeded,
    McpInputSchemaRefNotFound,
    McpInputSchemaRefUnsupported,
    McpInputSchemaRequiredPropertyMissing,
    OpenApiAllOfConflict,
    OpenApiExternalRefUnsupported,
    OpenApiParameterInvalid,
    OpenApiParameterSerializationUnsupported,
    OpenApiRefNotFound,
    OpenApiRequestBodyUnpublished,
    OpenApiResponseSchemeUnresolved,
    OperationMetadataOverrideFailed,
    OperationMetadataProvenanceMismatch,
    OperationMetadataUnavailable,
    ParameterMetadataOverrideFailed,
    ProjectionCatalogProvenanceMismatch,
    ProjectionInputUnsupported,
    ProjectionNameCollisionResolved,
    RawDocumentFingerprintMismatch,
    RawDocumentUnavailable,
    SemanticIrProvenanceMismatch,
    SemanticIrUnavailable,
    SourceLoadFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: DiagnosticCode,
    pub severity: DiagnosticSeverity,
    pub message: String,
    pub operation_id: Option<String>,
}

impl Diagnostic {
    pub(crate) fn warning(
        code: DiagnosticCode,
        message: impl Into<String>,
        operation_id: Option<String>,
    ) -> Self {
        Self {
            code,
            severity: DiagnosticSeverity::Warning,
            message: message.into(),
            operation_id,
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
