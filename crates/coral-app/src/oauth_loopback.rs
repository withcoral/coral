//! Shared native-app OAuth loopback callback transport.

use std::collections::BTreeMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::time::Duration;

use subtle::ConstantTimeEq as _;
use thiserror::Error;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinError, JoinSet};
use tokio::time::Instant;
use url::{Host, Url};

const MAX_CALLBACK_REQUEST_BYTES: usize = 8 * 1024;
const MAX_CONCURRENT_CONNECTIONS: usize = 8;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(3);

/// A state-authenticated terminal OAuth callback.
pub(crate) enum OAuthCallbackOutcome {
    AuthorizationCode(String),
    ProviderError {
        error: String,
        description: Option<String>,
    },
}

/// Failures owned by the loopback callback transport.
#[derive(Debug, Error)]
pub(crate) enum OAuthLoopbackError {
    #[error("invalid OAuth callback configuration: {0}")]
    InvalidConfiguration(String),
    #[error("OAuth callback listener failed: {0}")]
    Io(#[from] io::Error),
    #[error("OAuth callback listener task failed: {0}")]
    Task(#[from] JoinError),
    #[error("OAuth callback timed out")]
    TimedOut,
}

/// Single-use receiver for a native-app OAuth redirect.
pub(crate) struct OAuthLoopbackReceiver {
    listener: TcpListener,
    expected: Arc<ExpectedCallback>,
    deadline: Instant,
}

struct ExpectedCallback {
    redirect_uri: Url,
    state: String,
}

enum ConnectionResult {
    Terminal(OAuthCallbackOutcome),
    Ignored,
}

enum ParsedCallback {
    Terminal(OAuthCallbackOutcome),
    Ignored(ResponseKind),
}

#[derive(Clone, Copy)]
enum ResponseKind {
    Success,
    ProviderError,
    BadRequest,
    NotFound,
    MethodNotAllowed,
}

impl OAuthLoopbackReceiver {
    pub(crate) fn new(
        listener: TcpListener,
        redirect_uri: Url,
        state: String,
        deadline: Instant,
    ) -> Result<Self, OAuthLoopbackError> {
        validate_configuration(&listener, &redirect_uri, &state)?;
        Ok(Self {
            listener,
            expected: Arc::new(ExpectedCallback {
                redirect_uri,
                state,
            }),
            deadline,
        })
    }

    pub(crate) async fn receive(self) -> Result<OAuthCallbackOutcome, OAuthLoopbackError> {
        let mut connections = JoinSet::new();
        loop {
            if Instant::now() >= self.deadline {
                return Err(OAuthLoopbackError::TimedOut);
            }
            if connections.len() >= MAX_CONCURRENT_CONNECTIONS {
                let joined = tokio::time::timeout_at(self.deadline, connections.join_next())
                    .await
                    .map_err(|_elapsed| OAuthLoopbackError::TimedOut)?;
                if let Some(outcome) = terminal_outcome(joined)? {
                    return Ok(outcome);
                }
                continue;
            }

            tokio::select! {
                biased;
                joined = connections.join_next(), if !connections.is_empty() => {
                    if let Some(outcome) = terminal_outcome(joined)? {
                        return Ok(outcome);
                    }
                }
                accepted = self.listener.accept() => {
                    let (stream, _peer) = accepted?;
                    let expected = Arc::clone(&self.expected);
                    let deadline = std::cmp::min(
                        self.deadline,
                        Instant::now() + CONNECTION_TIMEOUT,
                    );
                    connections.spawn(handle_connection(stream, expected, deadline));
                }
                () = tokio::time::sleep_until(self.deadline) => {
                    return Err(OAuthLoopbackError::TimedOut);
                }
            }
        }
    }
}

fn terminal_outcome(
    joined: Option<Result<ConnectionResult, JoinError>>,
) -> Result<Option<OAuthCallbackOutcome>, OAuthLoopbackError> {
    match joined {
        Some(Ok(ConnectionResult::Terminal(outcome))) => Ok(Some(outcome)),
        Some(Ok(ConnectionResult::Ignored)) | None => Ok(None),
        Some(Err(error)) => Err(error.into()),
    }
}

fn validate_configuration(
    listener: &TcpListener,
    redirect_uri: &Url,
    state: &str,
) -> Result<(), OAuthLoopbackError> {
    if redirect_uri.scheme() != "http"
        || !redirect_uri.username().is_empty()
        || redirect_uri.password().is_some()
        || redirect_uri.fragment().is_some()
        || !url_host_is_loopback(redirect_uri)
    {
        return Err(OAuthLoopbackError::InvalidConfiguration(
            "redirect URI must be an HTTP loopback URL without credentials or a fragment"
                .to_string(),
        ));
    }
    if redirect_uri.query_pairs().any(|(key, _value)| {
        matches!(
            key.as_ref(),
            "state" | "code" | "error" | "error_description"
        )
    }) {
        return Err(OAuthLoopbackError::InvalidConfiguration(
            "redirect URI query must not include OAuth response parameters".to_string(),
        ));
    }
    if state.is_empty() {
        return Err(OAuthLoopbackError::InvalidConfiguration(
            "state must not be empty".to_string(),
        ));
    }
    let address = listener.local_addr()?;
    if !address.ip().is_loopback()
        || redirect_uri.port_or_known_default() != Some(address.port())
        || !redirect_host_matches_listener(redirect_uri, address.ip())
    {
        return Err(OAuthLoopbackError::InvalidConfiguration(
            "redirect URI must identify the bound loopback listener".to_string(),
        ));
    }
    Ok(())
}

async fn handle_connection(
    mut stream: TcpStream,
    expected: Arc<ExpectedCallback>,
    deadline: Instant,
) -> ConnectionResult {
    let parsed = match read_request(&mut stream, deadline).await {
        Ok(request) => parse_request(&request, &expected),
        Err(error) => {
            tracing::debug!(%error, "ignoring unreadable OAuth callback connection");
            ParsedCallback::Ignored(ResponseKind::BadRequest)
        }
    };
    let (kind, terminal) = match parsed {
        ParsedCallback::Terminal(outcome @ OAuthCallbackOutcome::AuthorizationCode(_)) => {
            (ResponseKind::Success, Some(outcome))
        }
        ParsedCallback::Terminal(outcome @ OAuthCallbackOutcome::ProviderError { .. }) => {
            (ResponseKind::ProviderError, Some(outcome))
        }
        ParsedCallback::Ignored(kind) => (kind, None),
    };
    if let Err(error) = write_response(&mut stream, kind).await {
        tracing::debug!(%error, "failed to write OAuth callback response");
    }
    terminal.map_or(ConnectionResult::Ignored, ConnectionResult::Terminal)
}

async fn read_request(stream: &mut TcpStream, deadline: Instant) -> io::Result<Vec<u8>> {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = tokio::time::timeout_at(deadline, stream.read(&mut chunk))
            .await
            .map_err(|_elapsed| {
                io::Error::new(io::ErrorKind::TimedOut, "callback read timed out")
            })??;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "callback request ended before its headers",
            ));
        }
        if request.len().saturating_add(read) > MAX_CALLBACK_REQUEST_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "callback request headers are too large",
            ));
        }
        request.extend(chunk.iter().take(read).copied());
        if let Some(end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            if end + 4 != request.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "callback request included a body",
                ));
            }
            return Ok(request);
        }
    }
}

fn parse_request(raw: &[u8], expected: &ExpectedCallback) -> ParsedCallback {
    parse_request_inner(raw, expected).unwrap_or(ParsedCallback::Ignored(ResponseKind::BadRequest))
}

fn parse_request_inner(raw: &[u8], expected: &ExpectedCallback) -> Result<ParsedCallback, ()> {
    let raw = std::str::from_utf8(raw).map_err(|_error| ())?;
    let headers_end = raw.find("\r\n\r\n").ok_or(())?;
    if headers_end + 4 != raw.len() {
        return Err(());
    }
    let mut lines = raw.get(..headers_end).ok_or(())?.split("\r\n");
    let mut request_line = lines.next().ok_or(())?.split_ascii_whitespace();
    let method = request_line.next().ok_or(())?;
    let target = request_line.next().ok_or(())?;
    let version = request_line.next().ok_or(())?;
    if request_line.next().is_some() || !matches!(version, "HTTP/1.0" | "HTTP/1.1") {
        return Err(());
    }
    if method != "GET" {
        return Ok(ParsedCallback::Ignored(ResponseKind::MethodNotAllowed));
    }
    if !target.starts_with('/') || target.starts_with("//") || target.contains('#') {
        return Err(());
    }

    let headers = parse_headers(lines)?;
    let host = single_header(&headers, "host")?.ok_or(())?;
    if !authority_matches(host, &expected.redirect_uri) {
        return Err(());
    }
    if headers.contains_key("transfer-encoding") {
        return Err(());
    }
    if let Some(length) = single_header(&headers, "content-length")?
        && length.parse::<u64>().map_err(|_error| ())? != 0
    {
        return Err(());
    }

    let callback = Url::parse(&format!("http://callback.invalid{target}")).map_err(|_error| ())?;
    if callback.path() != expected.redirect_uri.path() {
        return Ok(ParsedCallback::Ignored(ResponseKind::NotFound));
    }
    let callback_pairs = callback.query_pairs().into_owned().collect::<Vec<_>>();
    let expected_pairs = expected
        .redirect_uri
        .query_pairs()
        .into_owned()
        .collect::<Vec<_>>();
    if !callback_pairs.starts_with(&expected_pairs) {
        return Err(());
    }
    let params = callback_pairs.into_iter().fold(
        BTreeMap::<String, Vec<String>>::new(),
        |mut params, (key, value)| {
            params.entry(key).or_default().push(value);
            params
        },
    );
    let state = single_param(&params, "state")?.ok_or(())?;
    if state
        .as_bytes()
        .ct_eq(expected.state.as_bytes())
        .unwrap_u8()
        != 1
    {
        return Err(());
    }
    let code = single_param(&params, "code")?;
    let error = single_param(&params, "error")?;
    let description = single_param(&params, "error_description")?;
    match (code, error) {
        (Some(code), None) if !code.is_empty() && description.is_none() => Ok(
            ParsedCallback::Terminal(OAuthCallbackOutcome::AuthorizationCode(code)),
        ),
        (None, Some(error)) if !error.is_empty() => Ok(ParsedCallback::Terminal(
            OAuthCallbackOutcome::ProviderError { error, description },
        )),
        _ => Err(()),
    }
}

fn parse_headers<'a>(
    lines: impl Iterator<Item = &'a str>,
) -> Result<BTreeMap<String, Vec<String>>, ()> {
    let mut headers = BTreeMap::<String, Vec<String>>::new();
    for line in lines {
        if line.starts_with(' ') || line.starts_with('\t') {
            return Err(());
        }
        let (name, value) = line.split_once(':').ok_or(())?;
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
        {
            return Err(());
        }
        headers
            .entry(name.to_ascii_lowercase())
            .or_default()
            .push(value.trim().to_string());
    }
    Ok(headers)
}

fn single_header<'a>(
    headers: &'a BTreeMap<String, Vec<String>>,
    name: &str,
) -> Result<Option<&'a str>, ()> {
    let Some(values) = headers.get(name) else {
        return Ok(None);
    };
    let [value] = values.as_slice() else {
        return Err(());
    };
    Ok(Some(value.as_str()))
}

fn single_param(params: &BTreeMap<String, Vec<String>>, name: &str) -> Result<Option<String>, ()> {
    let Some(values) = params.get(name) else {
        return Ok(None);
    };
    let [value] = values.as_slice() else {
        return Err(());
    };
    Ok(Some(value.clone()))
}

fn authority_matches(value: &str, expected: &Url) -> bool {
    let Ok(actual) = Url::parse(&format!("http://{value}/")) else {
        return false;
    };
    actual.username().is_empty()
        && actual.password().is_none()
        && actual.host() == expected.host()
        && actual.port_or_known_default() == expected.port_or_known_default()
}

fn url_host_is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

fn redirect_host_matches_listener(url: &Url, listener_ip: IpAddr) -> bool {
    match url.host() {
        Some(Host::Domain(domain)) if domain.eq_ignore_ascii_case("localhost") => {
            matches!(
                listener_ip,
                IpAddr::V4(Ipv4Addr::LOCALHOST) | IpAddr::V6(Ipv6Addr::LOCALHOST)
            )
        }
        Some(Host::Ipv4(address)) => listener_ip == address,
        Some(Host::Ipv6(address)) => listener_ip == address,
        Some(Host::Domain(_)) | None => false,
    }
}

async fn write_response(stream: &mut TcpStream, kind: ResponseKind) -> io::Result<()> {
    let (status, body) = match kind {
        ResponseKind::Success => (
            "200 OK",
            "Authorization received. Coral is finishing sign-in in your terminal.",
        ),
        ResponseKind::ProviderError => (
            "400 Bad Request",
            "Authorization was not completed. Return to your terminal for details.",
        ),
        ResponseKind::BadRequest => ("400 Bad Request", "OAuth callback request ignored."),
        ResponseKind::NotFound => ("404 Not Found", "OAuth callback request ignored."),
        ResponseKind::MethodNotAllowed => {
            ("405 Method Not Allowed", "OAuth callback request ignored.")
        }
    };
    let allow = matches!(kind, ResponseKind::MethodNotAllowed).then_some("allow: GET\r\n");
    let page = format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>Coral OAuth</title></head><body><p>{body}</p></body></html>"
    );
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: text/html; charset=utf-8\r\ncontent-length: {}\r\ncache-control: no-store\r\ncontent-security-policy: default-src 'none'; frame-ancestors 'none'; base-uri 'none'\r\nreferrer-policy: no-referrer\r\nx-content-type-options: nosniff\r\nx-frame-options: DENY\r\n{}connection: close\r\n\r\n{page}",
        page.len(),
        allow.unwrap_or_default(),
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expected(uri: &str, state: &str) -> ExpectedCallback {
        ExpectedCallback {
            redirect_uri: Url::parse(uri).expect("redirect URI"),
            state: state.to_string(),
        }
    }

    fn request(target: &str, extra_headers: &str) -> Vec<u8> {
        format!("GET {target} HTTP/1.1\r\nhost: 127.0.0.1:14554\r\n{extra_headers}\r\n")
            .into_bytes()
    }

    #[test]
    fn parser_accepts_only_state_authenticated_terminal_callbacks() {
        let expected = expected("http://127.0.0.1:14554/oauth/callback", "right-state");
        assert!(matches!(
            parse_request(
                &request("/oauth/callback?state=right-state&code=code-1", ""),
                &expected,
            ),
            ParsedCallback::Terminal(OAuthCallbackOutcome::AuthorizationCode(code))
                if code == "code-1"
        ));
        assert!(matches!(
            parse_request(
                &request(
                    "/oauth/callback?state=right-state&error=access_denied&error_description=private",
                    "",
                ),
                &expected,
            ),
            ParsedCallback::Terminal(OAuthCallbackOutcome::ProviderError { error, description })
                if error == "access_denied" && description.as_deref() == Some("private")
        ));
        for target in [
            "/oauth/callback?error=access_denied",
            "/oauth/callback?state=wrong&error=access_denied",
            "/oauth/callback?state=right-state&code=one&code=two",
            "/oauth/callback?state=right-state&code=one&error=access_denied",
            "/oauth/callback?state=right-state&code=one&error_description=orphan",
        ] {
            assert!(
                matches!(
                    parse_request(&request(target, ""), &expected),
                    ParsedCallback::Ignored(ResponseKind::BadRequest)
                ),
                "accepted {target}"
            );
        }
    }

    #[test]
    fn parser_rejects_ambiguous_or_non_origin_http_requests() {
        let expected = expected("http://127.0.0.1:14554/oauth/callback", "state");
        let invalid = [
            "POST /oauth/callback?state=state&code=x HTTP/1.1\r\nhost: 127.0.0.1:14554\r\n\r\n",
            "GET http://127.0.0.1:14554/oauth/callback?state=state&code=x HTTP/1.1\r\nhost: 127.0.0.1:14554\r\n\r\n",
            "GET /oauth/callback?state=state&code=x HTTP/2\r\nhost: 127.0.0.1:14554\r\n\r\n",
            "GET /oauth/callback?state=state&code=x HTTP/1.1 extra\r\nhost: 127.0.0.1:14554\r\n\r\n",
            "GET /oauth/callback?state=state&code=x HTTP/1.1\r\nhost: evil.example\r\n\r\n",
            "GET /oauth/callback?state=state&code=x HTTP/1.1\r\nhost: 127.0.0.1:14554\r\ncontent-length: 1\r\n\r\n",
            "GET /oauth/callback?state=state&code=x HTTP/1.1\r\nhost: 127.0.0.1:14554\r\ntransfer-encoding: chunked\r\n\r\n",
        ];
        for raw in invalid {
            assert!(
                !matches!(
                    parse_request(raw.as_bytes(), &expected),
                    ParsedCallback::Terminal(_)
                ),
                "accepted {raw:?}"
            );
        }
    }

    #[tokio::test]
    async fn receiver_ignores_idle_and_wrong_state_connections_before_split_success() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("listener");
        let port = listener.local_addr().expect("address").port();
        let receiver = OAuthLoopbackReceiver::new(
            listener,
            Url::parse(&format!("http://127.0.0.1:{port}/oauth/callback")).expect("URI"),
            "right-state".to_string(),
            Instant::now() + Duration::from_secs(5),
        )
        .expect("receiver");
        let send = async move {
            let _idle = TcpStream::connect(("127.0.0.1", port)).await.expect("idle");
            let mut wrong = TcpStream::connect(("127.0.0.1", port))
                .await
                .expect("wrong");
            wrong
                .write_all(
                    format!("GET /oauth/callback?state=wrong&error=access_denied HTTP/1.1\r\nhost: 127.0.0.1:{port}\r\n\r\n").as_bytes(),
                )
                .await
                .expect("write wrong callback");
            let mut valid = TcpStream::connect(("127.0.0.1", port))
                .await
                .expect("valid");
            valid
                .write_all(b"GET /oauth/callback?state=right-state&co")
                .await
                .expect("write first half");
            valid
                .write_all(
                    format!("de=test-code HTTP/1.1\r\nhost: 127.0.0.1:{port}\r\n\r\n").as_bytes(),
                )
                .await
                .expect("write second half");
            let mut response = Vec::new();
            valid.read_to_end(&mut response).await.expect("response");
            let response = String::from_utf8(response).expect("UTF-8 response");
            assert!(response.starts_with("HTTP/1.1 200 OK"));
            assert!(response.contains("cache-control: no-store"));
            assert!(response.contains("content-security-policy: default-src 'none'"));
        };
        let (outcome, ()) = tokio::join!(receiver.receive(), send);
        assert!(matches!(
            outcome.expect("callback"),
            OAuthCallbackOutcome::AuthorizationCode(code) if code == "test-code"
        ));
    }

    #[tokio::test]
    async fn receiver_times_out_with_idle_connection() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("listener");
        let port = listener.local_addr().expect("address").port();
        let receiver = OAuthLoopbackReceiver::new(
            listener,
            Url::parse(&format!("http://127.0.0.1:{port}/oauth/callback")).expect("URI"),
            "state".to_string(),
            Instant::now() + Duration::from_millis(30),
        )
        .expect("receiver");
        let _idle = TcpStream::connect(("127.0.0.1", port)).await.expect("idle");
        assert!(matches!(
            receiver.receive().await,
            Err(OAuthLoopbackError::TimedOut)
        ));
    }

    #[tokio::test]
    async fn constructor_rejects_unsafe_configuration() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("listener");
        let port = listener.local_addr().expect("address").port();
        let mismatched =
            Url::parse(&format!("http://127.0.0.2:{port}/oauth/callback")).expect("mismatched URI");
        assert!(validate_configuration(&listener, &mismatched, "state").is_err());
        let valid =
            Url::parse(&format!("http://127.0.0.1:{port}/oauth/callback")).expect("valid URI");
        assert!(validate_configuration(&listener, &valid, "").is_err());
        let result = OAuthLoopbackReceiver::new(
            listener,
            Url::parse(&format!("https://example.com:{port}/oauth/callback")).expect("URI"),
            "state".to_string(),
            Instant::now() + Duration::from_secs(1),
        );
        assert!(matches!(
            result,
            Err(OAuthLoopbackError::InvalidConfiguration(_))
        ));
    }
}
