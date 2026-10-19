//! Adapter-owned diagnostic projections. Numeric host fields remain exact JSON integers.
use coral_functions::{SavedFunctionDiagnostic, SavedFunctionDiagnosticCategory};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::ToolFailure;

const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Serialize, JsonSchema)]
#[serde(transparent)]
pub(super) struct SafeInteger(#[schemars(range(min = 0, max = MAX_SAFE_INTEGER))] u64);

impl SafeInteger {
    pub(super) fn new(value: u64) -> Result<Self, ToolFailure> {
        if value <= MAX_SAFE_INTEGER {
            Ok(Self(value))
        } else {
            Err(ToolFailure::internal())
        }
    }
}

impl<'de> Deserialize<'de> for SafeInteger {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(|_error| {
            serde::de::Error::custom("Coral result integer exceeds the exact JSON range")
        })
    }
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct Diagnostic {
    category: DiagnosticCategory,
    code: SafeInteger,
    message: String,
    line: Option<SafeInteger>,
    column: Option<SafeInteger>,
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum DiagnosticCategory {
    Message,
    Warning,
    Error,
    Suggestion,
}

pub(super) fn saved_diagnostics(
    diagnostics: &[SavedFunctionDiagnostic],
) -> Result<Vec<Diagnostic>, ToolFailure> {
    diagnostics
        .iter()
        .map(|diagnostic| {
            Ok(Diagnostic {
                category: match diagnostic.category() {
                    SavedFunctionDiagnosticCategory::Message => DiagnosticCategory::Message,
                    SavedFunctionDiagnosticCategory::Warning => DiagnosticCategory::Warning,
                    SavedFunctionDiagnosticCategory::Error => DiagnosticCategory::Error,
                    SavedFunctionDiagnosticCategory::Suggestion => DiagnosticCategory::Suggestion,
                },
                code: SafeInteger::new(diagnostic.code())?,
                message: diagnostic.message().to_owned(),
                line: diagnostic.line().map(SafeInteger::new).transpose()?,
                column: diagnostic.column().map(SafeInteger::new).transpose()?,
            })
        })
        .collect()
}

#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct ProgramError<'a> {
    name: &'a str,
    message: &'a str,
    frames: Vec<Frame<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "OperationError<'a>")]
    operation: Option<OperationError<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Self")]
    cause: Option<Box<Self>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<Self>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    truncated: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    cause_omitted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "SafeInteger")]
    omitted_errors: Option<SafeInteger>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    circular: bool,
}

#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Frame<'a> {
    source: &'a str,
    #[schemars(range(min = 0, max = 4_294_967_295_u64))]
    line: u32,
    #[schemars(range(min = 0, max = 4_294_967_295_u64))]
    column: u32,
}

#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct OperationError<'a> {
    code: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "str")]
    trace_id: Option<&'a str>,
}

impl<'a> TryFrom<&'a coral_run::ProgramError> for ProgramError<'a> {
    type Error = ToolFailure;

    fn try_from(error: &'a coral_run::ProgramError) -> Result<Self, Self::Error> {
        Ok(Self {
            name: &error.name,
            message: &error.message,
            frames: error
                .frames
                .iter()
                .map(|frame| Frame {
                    source: &frame.source,
                    line: frame.line,
                    column: frame.column,
                })
                .collect(),
            operation: error.operation.as_ref().map(|operation| OperationError {
                code: &operation.code,
                trace_id: operation.trace_id.as_deref(),
            }),
            cause: error
                .cause
                .as_deref()
                .map(Self::try_from)
                .transpose()?
                .map(Box::new),
            errors: error
                .errors
                .iter()
                .map(Self::try_from)
                .collect::<Result<_, _>>()?,
            truncated: error.truncated,
            cause_omitted: error.cause_omitted,
            omitted_errors: if error.omitted_errors == 0 {
                None
            } else {
                Some(SafeInteger::new(
                    u64::try_from(error.omitted_errors)
                        .map_err(|_error| ToolFailure::internal())?,
                )?)
            },
            circular: error.circular,
        })
    }
}

pub(super) fn middleware_diagnostics(
    diagnostics: &[coral_middleware::MiddlewareDiagnostic],
) -> Result<Vec<Diagnostic>, ToolFailure> {
    diagnostics
        .iter()
        .map(|diagnostic| {
            Ok(Diagnostic {
                category: match diagnostic.category {
                    coral_middleware::MiddlewareDiagnosticCategory::Warning => {
                        DiagnosticCategory::Warning
                    }
                    coral_middleware::MiddlewareDiagnosticCategory::Error => {
                        DiagnosticCategory::Error
                    }
                    coral_middleware::MiddlewareDiagnosticCategory::Suggestion => {
                        DiagnosticCategory::Suggestion
                    }
                    coral_middleware::MiddlewareDiagnosticCategory::Message => {
                        DiagnosticCategory::Message
                    }
                },
                code: SafeInteger::new(diagnostic.code)?,
                message: diagnostic.message.clone(),
                line: diagnostic.line.map(SafeInteger::new).transpose()?,
                column: diagnostic.column.map(SafeInteger::new).transpose()?,
            })
        })
        .collect()
}
