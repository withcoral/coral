//! OpenAPI HTTP request serialization and complete response preservation.

mod request;
mod response;
mod response_headers;

pub use request::ReadyHttpDispatch;
pub use response::{HttpDecodeDiagnostic, HttpOutcome, HttpRedirect, HttpUpstream};
