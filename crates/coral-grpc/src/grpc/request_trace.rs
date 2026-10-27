//! Keeps a gRPC request span open until the call's status is known.

use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::response::Response;
use http_body::{Frame, SizeHint};

const GRPC_STATUS: &str = "grpc-status";

/// Records the call's status on `span` and returns the response. A call that
/// fails before any message carries its status in the response headers. Any
/// other call sends it in trailers after the body, so the span stays open
/// until they arrive.
pub(super) fn until_status(response: Response, span: tracing::Span, path: String) -> Response {
    if let Some(code) = status(response.headers()) {
        coral_telemetry::record_grpc_response(&span, &path, code);
        return response;
    }
    response.map(|body| {
        Body::new(StatusBody {
            body,
            span,
            path,
            recorded: false,
        })
    })
}

fn status(headers: &axum::http::HeaderMap) -> Option<i32> {
    headers.get(GRPC_STATUS)?.to_str().ok()?.parse().ok()
}

/// A response body that records the call's status from its trailers.
struct StatusBody {
    body: Body,
    span: tracing::Span,
    path: String,
    recorded: bool,
}

impl http_body::Body for StatusBody {
    type Data = axum::body::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let frame = Pin::new(&mut this.body).poll_frame(context);
        if let Poll::Ready(Some(Ok(frame))) = &frame
            && let Some(code) = frame.trailers_ref().and_then(status)
            && !this.recorded
        {
            this.recorded = true;
            coral_telemetry::record_grpc_response(&this.span, &this.path, code);
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}
