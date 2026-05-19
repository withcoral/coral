use std::time::Duration;

use serde::Serialize;
use tracing::warn;

use crate::feedback::manager::{FeedbackReport, FeedbackUpload};

pub(crate) const HOSTED_FEEDBACK_ENDPOINT: &str = "https://feedback.withcoral.com/ingest";
const HOSTED_FEEDBACK_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) trait FeedbackPublisher: Send + Sync + 'static {
    fn publish<'a>(
        &'a self,
        report: &'a FeedbackReport,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<FeedbackUpload>> + Send + 'a>>;
}

#[derive(Debug, Default)]
pub(crate) struct NoopFeedbackPublisher;

impl FeedbackPublisher for NoopFeedbackPublisher {
    fn publish<'a>(
        &'a self,
        _report: &'a FeedbackReport,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<FeedbackUpload>> + Send + 'a>>
    {
        Box::pin(async { None })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct HostedFeedbackPublisher {
    client: reqwest::Client,
    endpoint: String,
    timeout: Duration,
}

impl HostedFeedbackPublisher {
    pub(crate) fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint: HOSTED_FEEDBACK_ENDPOINT.to_string(),
            timeout: HOSTED_FEEDBACK_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn with_endpoint(endpoint: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint,
            timeout: HOSTED_FEEDBACK_TIMEOUT,
        }
    }
}

impl FeedbackPublisher for HostedFeedbackPublisher {
    fn publish<'a>(
        &'a self,
        report: &'a FeedbackReport,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<FeedbackUpload>> + Send + 'a>>
    {
        Box::pin(async move {
            let envelope = FeedbackEnvelope::new(report);
            let request = self.client.post(&self.endpoint).json(&envelope).send();
            match tokio::time::timeout(self.timeout, request).await {
                Ok(Ok(response)) if response.status().is_success() => {
                    Some(FeedbackUpload::accepted())
                }
                Ok(Ok(response)) => {
                    let error =
                        format!("remote feedback upload returned HTTP {}", response.status());
                    warn!(feedback_id = %report.id, %error);
                    Some(FeedbackUpload::failed(error))
                }
                Ok(Err(error)) => {
                    let error = format!("remote feedback upload failed: {error}");
                    warn!(feedback_id = %report.id, %error);
                    Some(FeedbackUpload::failed(error))
                }
                Err(_) => {
                    let error = "remote feedback upload timed out".to_string();
                    warn!(feedback_id = %report.id, %error);
                    Some(FeedbackUpload::failed(error))
                }
            }
        })
    }
}

#[derive(Debug, Serialize)]
struct FeedbackEnvelope<'a> {
    schema_version: u8,
    report: FeedbackEnvelopeReport<'a>,
    client: FeedbackEnvelopeClient<'a>,
}

impl<'a> FeedbackEnvelope<'a> {
    fn new(report: &'a FeedbackReport) -> Self {
        Self {
            schema_version: 1,
            report: FeedbackEnvelopeReport {
                id: &report.id,
                workspace: report.workspace.as_str(),
                created_at: report.created_at.to_rfc3339(),
                trying_to_do: &report.trying_to_do,
                tried: &report.tried,
                stuck: &report.stuck,
            },
            client: FeedbackEnvelopeClient {
                coral_version: env!("CARGO_PKG_VERSION"),
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct FeedbackEnvelopeReport<'a> {
    id: &'a str,
    workspace: &'a str,
    created_at: String,
    trying_to_do: &'a str,
    tried: &'a str,
    stuck: &'a str,
}

#[derive(Debug, Serialize)]
struct FeedbackEnvelopeClient<'a> {
    coral_version: &'a str,
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::indexing_slicing,
        reason = "Test fixture asserts exact JSON shape and reads bounded request buffers."
    )]

    use std::net::Ipv4Addr;
    use std::sync::{Arc, Mutex};

    use chrono::Utc;
    use serde_json::Value;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::{FeedbackPublisher, HostedFeedbackPublisher};
    use crate::feedback::manager::FeedbackReport;
    use crate::workspaces::WorkspaceName;

    #[tokio::test]
    async fn hosted_publisher_posts_expected_envelope() {
        let captured: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
        let app_captured = Arc::clone(&captured);
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind local test server");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept request");
            let body = read_http_body(&mut stream).await;
            *app_captured.lock().expect("capture lock") =
                Some(serde_json::from_slice(&body).expect("request json"));
            stream
                .write_all(b"HTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n")
                .await
                .expect("write response");
        });

        let report = FeedbackReport {
            id: "feedback-123".to_string(),
            workspace: WorkspaceName::default(),
            created_at: Utc::now(),
            trying_to_do: "trying".to_string(),
            tried: "tried".to_string(),
            stuck: "stuck".to_string(),
        };
        let publisher = HostedFeedbackPublisher::with_endpoint(format!("http://{addr}/ingest"));

        let upload = publisher.publish(&report).await.expect("upload attempted");

        assert_eq!(
            upload.status,
            crate::feedback::manager::FeedbackUploadStatus::Accepted
        );
        assert_eq!(upload.error_message, None);
        let body = captured
            .lock()
            .expect("capture lock")
            .clone()
            .expect("captured body");
        assert_eq!(body["schema_version"], 1);
        assert_eq!(body["report"]["id"], "feedback-123");
        assert_eq!(body["report"]["workspace"], "default");
        assert_eq!(body["report"]["trying_to_do"], "trying");
        assert_eq!(body["report"]["tried"], "tried");
        assert_eq!(body["report"]["stuck"], "stuck");
        assert_eq!(body["client"]["coral_version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(body["client"].as_object().expect("client object").len(), 1);

        server.abort();
    }

    async fn read_http_body(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut data = Vec::new();
        let mut buf = [0; 4096];
        loop {
            let read = stream.read(&mut buf).await.expect("read request");
            assert!(read > 0, "connection closed before request body");
            data.extend_from_slice(&buf[..read]);
            if let Some((body_start, content_length)) = request_body_bounds(&data) {
                let body_end = body_start + content_length;
                if data.len() >= body_end {
                    return data[body_start..body_end].to_vec();
                }
            }
        }
    }

    fn request_body_bounds(data: &[u8]) -> Option<(usize, usize)> {
        let body_start = data.windows(4).position(|window| window == b"\r\n\r\n")? + 4;
        let headers = std::str::from_utf8(&data[..body_start]).expect("request headers utf8");
        let content_length = headers
            .lines()
            .find_map(|line| line.strip_prefix("content-length: "))
            .or_else(|| {
                headers
                    .lines()
                    .find_map(|line| line.strip_prefix("Content-Length: "))
            })?
            .parse::<usize>()
            .expect("content length");
        Some((body_start, content_length))
    }
}
