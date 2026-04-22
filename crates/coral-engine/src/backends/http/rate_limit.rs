//! Rate-limit classification and retry policy for HTTP-backed sources.
//!
//! The module exposes a single entry point, [`check_rate_limit`], which turns
//! a response (status + headers) and the current retry counter into a
//! [`RateLimitDecision`] the client loop can act on. Classification, policy, and
//! error formatting are private helpers.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use coral_spec::backends::http::RateLimitSpec;
use reqwest::header::HeaderMap;

const DEFAULT_FALLBACK_RETRY_AFTER: Duration = Duration::from_secs(5);
const MAX_THROTTLE_RETRIES: usize = 2;
const MAX_SHORT_RETRY_AFTER: Duration = Duration::from_secs(15);

/// Inspect `status`/`headers` under the given `spec` and decide whether to
/// continue, retry after a short sleep, or fail with a rate-limit error.
pub(super) fn check_rate_limit(
    status: reqwest::StatusCode,
    headers: &HeaderMap,
    spec: &RateLimitSpec,
    throttle_retries: usize,
) -> RateLimitDecision {
    let signal = classify_rate_limit(status, headers, spec, SystemTime::now());
    decide_retry(signal, throttle_retries)
}

/// What a response is telling us about rate limiting.
///
/// `Quota` means the caller has exhausted its allotment for the current
/// window and must wait for `reset_in` before making progress. `Throttle`
/// means the caller is sending requests too fast; backing off by
/// `retry_after` is usually enough to recover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RateLimitSignal {
    None,
    Quota { reset_in: Option<Duration> },
    Throttle { retry_after: Option<Duration> },
}

/// What the client loop should do after [`check_rate_limit`] has inspected a response.
#[derive(Debug)]
pub(super) enum RateLimitDecision {
    Continue,
    Retry(Duration),
    Fail(RateLimitError),
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RateLimitError {
    kind: RateLimitErrorKind,
    reset_in: Option<Duration>,
    retry_after: Option<Duration>,
}

impl std::fmt::Display for RateLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("rate limit exceeded")?;
        match self.kind {
            RateLimitErrorKind::QuotaExceeded => {
                if let Some(wait) = self.reset_in {
                    write!(f, "; quota resets in {}s", wait.as_secs())?;
                }
            }
            RateLimitErrorKind::Throttled => {
                if let Some(wait) = self.retry_after {
                    write!(f, "; retry after {}s", wait.as_secs())?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum RateLimitErrorKind {
    QuotaExceeded,
    Throttled,
}

fn classify_rate_limit(
    status: reqwest::StatusCode,
    headers: &HeaderMap,
    spec: &RateLimitSpec,
    now: SystemTime,
) -> RateLimitSignal {
    let is_429 = status == reqwest::StatusCode::TOO_MANY_REQUESTS;
    let is_extra = spec.extra_statuses.contains(&status.as_u16());
    if !is_429 && !is_extra {
        return RateLimitSignal::None;
    }

    if remaining_is_zero(headers, spec) {
        return RateLimitSignal::Quota {
            reset_in: parse_reset_in(headers, spec, now),
        };
    }

    let retry_after = if is_429 {
        parse_retry_after(headers, spec, now)
            .or_else(|| parse_reset_in(headers, spec, now))
            .or(Some(DEFAULT_FALLBACK_RETRY_AFTER))
    } else {
        parse_retry_after(headers, spec, now)
            .or_else(|| parse_reset_in(headers, spec, now).filter(|wait| !wait.is_zero()))
    };
    if is_extra && retry_after.is_none() {
        // Extra status without any rate-limit signal — treat as a regular error.
        return RateLimitSignal::None;
    }

    RateLimitSignal::Throttle { retry_after }
}

fn decide_retry(signal: RateLimitSignal, throttle_retries: usize) -> RateLimitDecision {
    match signal {
        RateLimitSignal::None => RateLimitDecision::Continue,
        RateLimitSignal::Quota { reset_in } => RateLimitDecision::Fail(RateLimitError {
            kind: RateLimitErrorKind::QuotaExceeded,
            reset_in,
            retry_after: None,
        }),
        RateLimitSignal::Throttle { retry_after } => {
            if throttle_retries >= MAX_THROTTLE_RETRIES {
                return RateLimitDecision::Fail(RateLimitError {
                    kind: RateLimitErrorKind::Throttled,
                    reset_in: None,
                    retry_after,
                });
            }
            match retry_after {
                Some(wait) if wait <= MAX_SHORT_RETRY_AFTER => RateLimitDecision::Retry(wait),
                _ => RateLimitDecision::Fail(RateLimitError {
                    kind: RateLimitErrorKind::Throttled,
                    reset_in: None,
                    retry_after,
                }),
            }
        }
    }
}

fn remaining_is_zero(headers: &HeaderMap, spec: &RateLimitSpec) -> bool {
    spec.remaining_header
        .as_deref()
        .and_then(|name| headers.get(name))
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        == Some("0")
}

fn parse_retry_after(
    headers: &HeaderMap,
    spec: &RateLimitSpec,
    now: SystemTime,
) -> Option<Duration> {
    let name = spec.retry_after_header.as_deref().unwrap_or("Retry-After");
    let raw = headers.get(name)?.to_str().ok()?.trim();

    if let Ok(secs) = raw.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }

    let when = httpdate::parse_http_date(raw).ok()?;
    Some(when.duration_since(now).unwrap_or_default())
}

fn parse_reset_in(headers: &HeaderMap, spec: &RateLimitSpec, now: SystemTime) -> Option<Duration> {
    let name = spec.reset_header.as_deref()?;
    let raw = headers.get(name)?.to_str().ok()?.trim();
    let reset_epoch = raw.parse::<u64>().ok()?;
    let now_epoch = now.duration_since(UNIX_EPOCH).ok()?.as_secs();
    Some(Duration::from_secs(reset_epoch.saturating_sub(now_epoch)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    #[test]
    fn classify_rate_limit_covers_every_branch() {
        let spec = RateLimitSpec {
            extra_statuses: vec![403],
            remaining_header: Some("X-RateLimit-Remaining".to_string()),
            reset_header: Some("X-RateLimit-Reset".to_string()),
            ..RateLimitSpec::default()
        };
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_100);

        // Bare 403 with no rate-limit signal is a regular auth error, not a rate limit.
        let mut bare = HeaderMap::new();
        bare.insert("X-RateLimit-Reset", HeaderValue::from_static("1700000099"));
        assert_eq!(
            classify_rate_limit(reqwest::StatusCode::FORBIDDEN, &bare, &spec, now),
            RateLimitSignal::None,
        );

        // Bare 429 with no retry hints falls back to the short default retry.
        assert_eq!(
            classify_rate_limit(
                reqwest::StatusCode::TOO_MANY_REQUESTS,
                &HeaderMap::new(),
                &spec,
                now
            ),
            RateLimitSignal::Throttle {
                retry_after: Some(DEFAULT_FALLBACK_RETRY_AFTER),
            },
        );

        // Remaining=0 with a past-epoch reset → Quota with a saturated-to-zero wait.
        let mut quota = HeaderMap::new();
        quota.insert("X-RateLimit-Remaining", HeaderValue::from_static("0"));
        quota.insert("X-RateLimit-Reset", HeaderValue::from_static("1700000099"));
        assert_eq!(
            classify_rate_limit(reqwest::StatusCode::TOO_MANY_REQUESTS, &quota, &spec, now),
            RateLimitSignal::Quota {
                reset_in: Some(Duration::ZERO),
            },
        );

        // 403 with Retry-After but no Remaining=0 → Throttle.
        let mut throttle = HeaderMap::new();
        throttle.insert("Retry-After", HeaderValue::from_static("7"));
        assert_eq!(
            classify_rate_limit(reqwest::StatusCode::FORBIDDEN, &throttle, &spec, now),
            RateLimitSignal::Throttle {
                retry_after: Some(Duration::from_secs(7)),
            },
        );

        // 403 with a future reset-header should also throttle for providers
        // like GitHub that use 403 for rate limiting.
        let mut reset_throttle_403 = HeaderMap::new();
        reset_throttle_403.insert("X-RateLimit-Reset", HeaderValue::from_static("1700000108"));
        assert_eq!(
            classify_rate_limit(
                reqwest::StatusCode::FORBIDDEN,
                &reset_throttle_403,
                &spec,
                now
            ),
            RateLimitSignal::Throttle {
                retry_after: Some(Duration::from_secs(8)),
            },
        );

        // 429 can also recover from reset-header timing without Remaining=0.
        let mut reset_throttle = HeaderMap::new();
        reset_throttle.insert("X-RateLimit-Reset", HeaderValue::from_static("1700000108"));
        assert_eq!(
            classify_rate_limit(
                reqwest::StatusCode::TOO_MANY_REQUESTS,
                &reset_throttle,
                &spec,
                now
            ),
            RateLimitSignal::Throttle {
                retry_after: Some(Duration::from_secs(8)),
            },
        );
    }

    #[test]
    fn decide_retry_enforces_quota_fail_and_throttle_budget() {
        // Quota always fails immediately — never retries.
        assert!(matches!(
            decide_retry(
                RateLimitSignal::Quota {
                    reset_in: Some(Duration::from_secs(1800)),
                },
                0,
            ),
            RateLimitDecision::Fail(RateLimitError {
                kind: RateLimitErrorKind::QuotaExceeded,
                ..
            }),
        ));

        // Short throttle within budget → retry.
        assert!(matches!(
            decide_retry(
                RateLimitSignal::Throttle {
                    retry_after: Some(Duration::from_secs(5)),
                },
                0,
            ),
            RateLimitDecision::Retry(wait) if wait == Duration::from_secs(5),
        ));

        // Long throttle → fail even with retries left.
        assert!(matches!(
            decide_retry(
                RateLimitSignal::Throttle {
                    retry_after: Some(Duration::from_secs(60)),
                },
                0,
            ),
            RateLimitDecision::Fail(RateLimitError {
                kind: RateLimitErrorKind::Throttled,
                ..
            }),
        ));

        // Short throttle but out of retries → fail.
        assert!(matches!(
            decide_retry(
                RateLimitSignal::Throttle {
                    retry_after: Some(Duration::from_secs(1)),
                },
                MAX_THROTTLE_RETRIES,
            ),
            RateLimitDecision::Fail(_),
        ));
    }
}
