//! W3C trace context on an outbound request: only the context Coral chose.

use reqwest::header::{HeaderName, HeaderValue};

const TRACEPARENT: HeaderName = HeaderName::from_static("traceparent");
const TRACESTATE: HeaderName = HeaderName::from_static("tracestate");
const BAGGAGE: HeaderName = HeaderName::from_static("baggage");

/// Gives `request` Coral's W3C `traceparent`, or none, in place of any trace
/// context already on it. A `traceparent`, `tracestate` or `baggage` header
/// that came from elsewhere, such as Program arguments, never leaves. A
/// `traceparent` that is not a valid header value is dropped.
///
/// # Errors
///
/// Returns the error of a request that cannot be built, as sending it would.
pub fn with_trace_parent(
    request: reqwest::RequestBuilder,
    traceparent: Option<&str>,
) -> Result<reqwest::RequestBuilder, reqwest::Error> {
    let (client, built) = request.build_split();
    let mut built = built?;
    let headers = built.headers_mut();
    for name in [&TRACEPARENT, &TRACESTATE, &BAGGAGE] {
        headers.remove(name);
    }
    if let Some(value) = traceparent.and_then(|value| HeaderValue::from_str(value).ok()) {
        headers.insert(TRACEPARENT, value);
    }
    Ok(reqwest::RequestBuilder::from_parts(client, built))
}

#[cfg(test)]
mod tests {
    use super::with_trace_parent;
    use crate::{Client, DestinationPolicy, NetworkAccess};

    const TRACEPARENT: &str = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";

    fn headers(traceparent: Option<&str>) -> reqwest::header::HeaderMap {
        let client = Client::new(
            DestinationPolicy::default().with_loopback(NetworkAccess::HttpAndHttps),
            std::time::Duration::from_secs(1),
        )
        .expect("client");
        let request = client
            .get("http://127.0.0.1:1/")
            .expect("loopback destination")
            .header(
                "traceparent",
                "00-ffffffffffffffffffffffffffffffff-ffffffffffffffff-01",
            )
            .header("tracestate", "caller=value")
            .header("baggage", "user=private")
            .header("x-kept", "kept");
        with_trace_parent(request, traceparent)
            .expect("a valid request")
            .build()
            .expect("request")
            .headers()
            .clone()
    }

    /// A request carries the trace context Coral chose and nothing a caller
    /// set: no caller `traceparent`, `tracestate` or `baggage`.
    #[test]
    fn only_coral_trace_context_leaves() {
        for (traceparent, expected) in [(Some(TRACEPARENT), Some(TRACEPARENT)), (None, None)] {
            let headers = headers(traceparent);
            assert_eq!(
                headers
                    .get_all("traceparent")
                    .iter()
                    .map(|value| value.to_str().expect("ASCII"))
                    .collect::<Vec<_>>(),
                expected.into_iter().collect::<Vec<_>>()
            );
            assert!(headers.get("tracestate").is_none());
            assert!(headers.get("baggage").is_none());
            assert_eq!(
                headers.get("x-kept").map(|value| value.as_bytes()),
                Some(&b"kept"[..])
            );
        }
    }
}
