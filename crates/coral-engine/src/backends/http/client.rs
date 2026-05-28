//! HTTP client orchestration for manifest-driven HTTP sources.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::error::{DataFusionError, Result};
use serde_json::Value;
use tokio::sync::Notify;

use crate::RequestAuthenticator;
use crate::backends::http::fetch::fetch_rows;
use crate::backends::http::registration_checks::validate_source_scoped_http_config;
use crate::backends::http::target::HttpFetchTarget;
use crate::backends::http::trace::HttpBodyCapture;
use coral_spec::backends::http::{HttpSourceManifest, RateLimitSpec};
use coral_spec::{AuthSpec, HeaderSpec, ParsedTemplate};

const DEFAULT_HTTP_REQUEST_TIMEOUT_SECS: u64 = 30;
const DEFAULT_HTTP_USER_AGENT: &str = concat!("coral/", env!("CARGO_PKG_VERSION"));

pub(super) type HttpResponseResult = Option<(Value, Option<String>)>;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct HttpRequestKey {
    pub(super) source_schema: String,
    pub(super) table_name: String,
    pub(super) method: String,
    pub(super) base_url: String,
    pub(super) url: String,
    pub(super) query_pairs: Vec<(String, String)>,
    pub(super) body: Option<String>,
    pub(super) response_format: String,
    pub(super) allow_404_empty: bool,
    pub(super) link_header_require_results: bool,
    pub(super) filters: BTreeMap<String, String>,
    pub(super) args: BTreeMap<String, String>,
    pub(super) sql_limit: Option<usize>,
    pub(super) projection: Option<Vec<usize>>,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use serde_json::json;
    use tokio::sync::Notify;

    use super::{HttpRequestKey, HttpSourceClient};
    use crate::backends::http::trace::HttpBodyCapture;
    use coral_spec::backends::http::RateLimitSpec;
    use coral_spec::{AuthSpec, ParsedTemplate};

    fn test_client() -> HttpSourceClient {
        HttpSourceClient {
            http: reqwest::Client::new(),
            request_timeout: Duration::from_secs(30),
            source_schema: "test_source".to_string(),
            base_url: ParsedTemplate::parse("https://example.com").expect("base url"),
            auth: AuthSpec::default(),
            request_headers: Vec::new(),
            request_authenticators: HashMap::new(),
            rate_limit: RateLimitSpec::default(),
            resolved_inputs: Arc::new(BTreeMap::new()),
            body_capture: HttpBodyCapture::default(),
            inflight_http_requests: Arc::default(),
        }
    }

    fn request_key() -> HttpRequestKey {
        HttpRequestKey {
            source_schema: "test_source".to_string(),
            table_name: "items".to_string(),
            method: "GET".to_string(),
            base_url: "https://example.com".to_string(),
            url: "https://example.com/items".to_string(),
            query_pairs: vec![("q".to_string(), "same".to_string())],
            body: None,
            response_format: "Json".to_string(),
            allow_404_empty: false,
            link_header_require_results: false,
            filters: BTreeMap::new(),
            args: BTreeMap::new(),
            sql_limit: Some(10),
            projection: Some(vec![0, 1]),
        }
    }

    #[tokio::test]
    async fn execute_inflight_shares_concurrent_success() {
        let client = test_client();
        let calls = Arc::new(AtomicUsize::new(0));
        let key = request_key();

        let first = client.execute_inflight(key.clone(), {
            let calls = Arc::clone(&calls);
            move || {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok(Some((json!({"ok": true}), Some("next".to_string()))))
                }
            }
        });
        let second = client.execute_inflight(key, {
            let calls = Arc::clone(&calls);
            move || {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(Some((json!({"ok": true}), Some("next".to_string()))))
                }
            }
        });

        let (first, second) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(first, second)
        })
        .await
        .expect("in-flight waiters should not hang");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            first.expect("first request succeeds"),
            second.expect("second request shares result")
        );
    }

    #[tokio::test]
    async fn execute_inflight_removes_completed_entries() {
        let client = test_client();
        let calls = Arc::new(AtomicUsize::new(0));
        let key = request_key();

        for _ in 0..2 {
            client
                .execute_inflight(key.clone(), {
                    let calls = Arc::clone(&calls);
                    move || {
                        let calls = Arc::clone(&calls);
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            Ok(Some((json!({"ok": true}), None)))
                        }
                    }
                })
                .await
                .expect("request succeeds");
        }

        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn execute_inflight_removes_cancelled_entries() {
        let client = test_client();
        let calls = Arc::new(AtomicUsize::new(0));
        let leader_started = Arc::new(Notify::new());
        let key = request_key();

        let leader = tokio::spawn({
            let client = client.clone();
            let calls = Arc::clone(&calls);
            let leader_started = Arc::clone(&leader_started);
            let key = key.clone();
            async move {
                client
                    .execute_inflight(key, move || {
                        let calls = Arc::clone(&calls);
                        let leader_started = Arc::clone(&leader_started);
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            leader_started.notify_waiters();
                            std::future::pending().await
                        }
                    })
                    .await
            }
        });

        leader_started.notified().await;

        let follower = tokio::spawn({
            let client = client.clone();
            let calls = Arc::clone(&calls);
            let key = key.clone();
            async move {
                client
                    .execute_inflight(key, move || {
                        let calls = Arc::clone(&calls);
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            Ok(Some((json!({"ok": true}), None)))
                        }
                    })
                    .await
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        leader.abort();

        let result = tokio::time::timeout(Duration::from_secs(5), follower)
            .await
            .expect("cancelled leader should not leave waiters hanging")
            .expect("follower task should not panic")
            .expect("follower request should succeed");

        assert_eq!(result, Some((json!({"ok": true}), None)));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

#[derive(Default)]
struct InflightHttpRequests {
    entries: Mutex<HashMap<HttpRequestKey, Arc<InflightHttpRequest>>>,
}

struct InflightHttpRequest {
    notify: Notify,
    result: Mutex<Option<InflightHttpRequestResult>>,
}

#[derive(Clone)]
enum InflightHttpRequestResult {
    Success(HttpResponseResult),
    Failed,
}

impl InflightHttpRequest {
    fn new() -> Self {
        Self {
            notify: Notify::new(),
            result: Mutex::new(None),
        }
    }
}

struct InflightLeaderGuard {
    inflight_http_requests: Arc<InflightHttpRequests>,
    key: HttpRequestKey,
    entry: Arc<InflightHttpRequest>,
    completed: bool,
}

impl InflightLeaderGuard {
    fn new(
        inflight_http_requests: Arc<InflightHttpRequests>,
        key: HttpRequestKey,
        entry: Arc<InflightHttpRequest>,
    ) -> Self {
        Self {
            inflight_http_requests,
            key,
            entry,
            completed: false,
        }
    }

    fn complete(mut self, result: InflightHttpRequestResult) {
        *self.entry.result.lock().expect("in-flight result poisoned") = Some(result);
        self.remove_entry();
        self.entry.notify.notify_waiters();
        self.completed = true;
    }

    fn remove_entry(&self) {
        let mut entries = self
            .inflight_http_requests
            .entries
            .lock()
            .expect("in-flight request map poisoned");
        if entries
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.entry))
        {
            entries.remove(&self.key);
        }
    }
}

impl Drop for InflightLeaderGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }

        *self.entry.result.lock().expect("in-flight result poisoned") =
            Some(InflightHttpRequestResult::Failed);
        self.remove_entry();
        self.entry.notify.notify_waiters();
    }
}

#[derive(Clone)]
pub(crate) struct HttpSourceClient {
    pub(super) http: reqwest::Client,
    pub(super) request_timeout: Duration,
    pub(super) source_schema: String,
    pub(super) base_url: ParsedTemplate,
    pub(super) auth: AuthSpec,
    pub(super) request_headers: Vec<HeaderSpec>,
    pub(super) request_authenticators: HashMap<String, Arc<dyn RequestAuthenticator>>,
    pub(super) rate_limit: RateLimitSpec,
    pub(super) resolved_inputs: Arc<BTreeMap<String, String>>,
    pub(super) body_capture: HttpBodyCapture,
    inflight_http_requests: Arc<InflightHttpRequests>,
}

impl std::fmt::Debug for HttpSourceClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSourceClient")
            .field("source_schema", &self.source_schema)
            .field("base_url", &self.base_url)
            .field("auth", &self.auth)
            .field("request_headers", &self.request_headers)
            .field("rate_limit", &self.rate_limit)
            .field("body_capture", &self.body_capture)
            .finish_non_exhaustive()
    }
}

impl HttpSourceClient {
    /// Build a backend client from a validated source spec.
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if required credentials are missing or if an
    /// authentication header template cannot be resolved.
    pub(crate) fn from_manifest(
        manifest: &HttpSourceManifest,
        source_secrets: &BTreeMap<String, String>,
        source_variables: &BTreeMap<String, String>,
        request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
        body_capture_max_bytes: Option<usize>,
    ) -> Result<Self> {
        let resolved_inputs =
            coral_spec::resolve_inputs(&manifest.declared_inputs, source_secrets, source_variables);
        validate_source_scoped_http_config(manifest, request_authenticators, &resolved_inputs)?;

        let request_timeout = Duration::from_secs(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS);
        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .user_agent(DEFAULT_HTTP_USER_AGENT)
            .build()
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "failed to build HTTP client for source '{}': {error}",
                    manifest.common.name
                ))
            })?;

        Ok(Self {
            http,
            request_timeout,
            source_schema: manifest.common.name.clone(),
            base_url: manifest.base_url.clone(),
            auth: manifest.auth.clone(),
            request_headers: manifest.request_headers.clone(),
            request_authenticators: request_authenticators.clone(),
            rate_limit: manifest.rate_limit.clone(),
            resolved_inputs: Arc::new(resolved_inputs),
            body_capture: HttpBodyCapture::new(body_capture_max_bytes),
            inflight_http_requests: Arc::new(InflightHttpRequests::default()),
        })
    }

    /// Fetch rows for a single table from the backend API.
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if request templates cannot be resolved, the
    /// `HTTP` request fails, the response payload cannot be interpreted, or the
    /// fetched rows cannot be extracted for the table strategy.
    pub(crate) async fn fetch(
        &self,
        target: &HttpFetchTarget,
        filter_values: &HashMap<String, String>,
        arg_values: &HashMap<String, String>,
        sql_limit: Option<usize>,
        projection: Option<&[usize]>,
    ) -> Result<Vec<Value>> {
        fetch_rows(
            self,
            target,
            filter_values,
            arg_values,
            sql_limit,
            projection,
        )
        .await
    }

    pub(super) async fn execute_inflight<F, Fut>(
        &self,
        key: HttpRequestKey,
        execute: F,
    ) -> Result<HttpResponseResult>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<HttpResponseResult>>,
    {
        loop {
            let (entry, is_leader) = {
                let mut entries = self
                    .inflight_http_requests
                    .entries
                    .lock()
                    .expect("in-flight request map poisoned");
                if let Some(entry) = entries.get(&key) {
                    (Arc::clone(entry), false)
                } else {
                    let entry = Arc::new(InflightHttpRequest::new());
                    entries.insert(key.clone(), Arc::clone(&entry));
                    (entry, true)
                }
            };

            if is_leader {
                let leader = InflightLeaderGuard::new(
                    Arc::clone(&self.inflight_http_requests),
                    key.clone(),
                    Arc::clone(&entry),
                );
                let result = execute().await;
                let shared_result = match &result {
                    Ok(value) => InflightHttpRequestResult::Success(value.clone()),
                    Err(_) => InflightHttpRequestResult::Failed,
                };
                leader.complete(shared_result);
                return result;
            }

            let notified = entry.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let current_result = entry
                .result
                .lock()
                .expect("in-flight result poisoned")
                .clone();
            match current_result {
                Some(InflightHttpRequestResult::Success(value)) => return Ok(value),
                Some(InflightHttpRequestResult::Failed) | None => {
                    // Preserve the exact error shape by retrying as a new leader instead
                    // of cloning or stringifying `DataFusionError` for waiters.
                }
            }
            notified.await;
            let completed_result = entry
                .result
                .lock()
                .expect("in-flight result poisoned")
                .clone();
            match completed_result {
                Some(InflightHttpRequestResult::Success(value)) => return Ok(value),
                Some(InflightHttpRequestResult::Failed) | None => {}
            }
        }
    }
}
