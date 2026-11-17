//! The one place that builds a `tonic::Status` for the private API.
//!
//! Every status carries a stable reason, which is also its message, and the
//! call's trace id, in a `google.rpc.ErrorInfo` detail. No status carries
//! prose: a client acts on the code and the reason, and an operator finds
//! the rest by the trace id.

use coral_workspace::TraceId;
use prost::Message as _;
use tonic::{Code, Status};

// Standard google.rpc.Status / ErrorInfo wire layout. The one domain error
// text exported is the bounded text of a definite refusal, as metadata.
#[derive(Clone, PartialEq, prost::Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}
#[derive(Clone, PartialEq, prost::Message)]
struct ErrorInfo {
    #[prost(string, tag = "1")]
    reason: String,
    #[prost(string, tag = "2")]
    domain: String,
    #[prost(map = "string, string", tag = "3")]
    metadata: std::collections::HashMap<String, String>,
}

/// A status whose reason, and message, is `reason`.
pub(super) fn safe_status(code: Code, reason: &str) -> Status {
    safe_status_with(code, reason, [])
}

/// A status whose `ErrorInfo` also carries `metadata`: safe values that the
/// client acts on or shows. An error's text is not one, except the bounded
/// text its domain returns for a definite refusal.
pub(super) fn safe_status_with(
    code: Code,
    reason: &str,
    metadata: impl IntoIterator<Item = (&'static str, String)>,
) -> Status {
    let detail = ErrorInfo {
        reason: reason.into(),
        domain: "coral.management".into(),
        metadata: metadata
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .chain([(
                "trace_id".to_owned(),
                TraceId::current_or_new().as_str().to_owned(),
            )])
            .collect(),
    };
    let status = RpcStatus {
        code: code as i32,
        message: reason.into(),
        details: vec![prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.ErrorInfo".into(),
            value: detail.encode_to_vec(),
        }],
    };
    Status::with_details(code, reason, status.encode_to_vec().into())
}

/// The one status for a request the adapter refuses as invalid. It does not
/// say which rule the request broke: the client gets the stable reason alone.
pub(super) fn invalid() -> Status {
    safe_status(Code::InvalidArgument, "INVALID_ARGUMENT")
}

/// The resource is absent, or concealed from the caller.
pub(super) fn not_found() -> Status {
    safe_status(Code::NotFound, "NOT_FOUND")
}

/// The caller may not do this.
pub(super) fn permission_denied() -> Status {
    safe_status(Code::PermissionDenied, "PERMISSION_DENIED")
}

/// The request carries no credential that Coral accepts.
pub(super) fn unauthenticated() -> Status {
    safe_status(Code::Unauthenticated, "UNAUTHENTICATED")
}

/// A dependency failed. A retry can succeed.
pub(super) fn unavailable() -> Status {
    safe_status(Code::Unavailable, "UNAVAILABLE")
}

/// Coral could not build its answer.
pub(super) fn internal() -> Status {
    safe_status(Code::Internal, "INTERNAL")
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    /// Every `tonic::Status` constructor.
    const CONSTRUCTORS: [&str; 24] = [
        "new",
        "from_error",
        "try_from_error",
        "from_header_map",
        "with_details",
        "with_details_and_metadata",
        "with_metadata",
        "ok",
        "cancelled",
        "unknown",
        "invalid_argument",
        "deadline_exceeded",
        "not_found",
        "already_exists",
        "permission_denied",
        "resource_exhausted",
        "failed_precondition",
        "aborted",
        "out_of_range",
        "unimplemented",
        "internal",
        "unavailable",
        "data_loss",
        "unauthenticated",
    ];

    fn rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(directory).expect("a readable source directory") {
            let path = entry.expect("a readable directory entry").path();
            if path.is_dir() {
                rust_files(&path, files);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }

    /// Whether the line names a `Status` constructor, called or passed as a
    /// function, such as `map_err(Status::internal)`.
    fn names_a_constructor(line: &str) -> bool {
        line.match_indices("Status::").any(|(at, path)| {
            let name: String = line
                .get(at + path.len()..)
                .unwrap_or_default()
                .chars()
                .take_while(|character| character.is_ascii_alphanumeric() || *character == '_')
                .collect();
            CONSTRUCTORS.contains(&name.as_str())
        })
    }

    /// Catches: a status built outside this module, which reaches the client
    /// with no reason and no trace id.
    #[test]
    fn only_this_module_builds_a_status() {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = vec![source.join("grpc.rs")];
        rust_files(&source.join("grpc"), &mut files);
        let mut built_elsewhere = Vec::new();
        for file in files {
            if file.ends_with("grpc/status.rs") {
                continue;
            }
            let text = std::fs::read_to_string(&file).expect("a readable source file");
            for (index, line) in text.lines().enumerate() {
                if names_a_constructor(line) {
                    built_elsewhere.push(format!("{}:{}", file.display(), index + 1));
                }
            }
        }
        assert!(
            built_elsewhere.is_empty(),
            "build these statuses with grpc/status.rs: {built_elsewhere:#?}"
        );
        assert!(names_a_constructor("    .map_err(Status::internal)?;"));
        assert!(names_a_constructor("Status::from_error(Box::new(error))"));
        assert!(names_a_constructor("Err(tonic::Status::not_found(\"x\"))"));
        assert!(!names_a_constructor(
            "let code = status.code(); ServingStatus::Serving"
        ));
    }

    /// Catches: a status that reaches the client with no reason to act on or
    /// no trace id to report.
    #[test]
    fn a_status_names_its_reason_and_the_trace() {
        use prost::Message as _;
        let status = super::safe_status(tonic::Code::NotFound, "NOT_FOUND");
        assert_eq!(
            (status.code(), status.message()),
            (tonic::Code::NotFound, "NOT_FOUND")
        );
        let decoded = super::RpcStatus::decode(status.details()).expect("a google.rpc.Status");
        let detail = decoded.details.first().expect("one detail");
        let info =
            super::ErrorInfo::decode(detail.value.as_slice()).expect("a google.rpc.ErrorInfo");
        assert_eq!(info.reason, "NOT_FOUND");
        assert!(
            info.metadata
                .get("trace_id")
                .is_some_and(|trace| !trace.is_empty())
        );
    }
}
