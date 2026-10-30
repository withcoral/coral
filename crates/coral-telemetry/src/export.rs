//! Filtering for spans and log records that leave the host over OTLP.

use std::time::Duration;

use opentelemetry::logs::{LogRecord as _, Logger as _, LoggerProvider as _, Severity};
use opentelemetry::trace::Status;
use opentelemetry::{Context, InstrumentationScope, KeyValue, Value};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{LogProcessor, SdkLogRecord, SdkLoggerProvider};
use opentelemetry_sdk::trace::{Span, SpanData, SpanProcessor};
use tracing_subscriber::filter::Targets;

use crate::field;

/// Targets whose spans and events carry request or response bodies. They
/// never leave the host, whatever the operator's filters admit.
const DENIED_TARGETS: &[&str] = &["coral.http.body", "coral.mcp.body"];

/// The name a library's span event leaves with, in place of its text.
const LIBRARY_EVENT_NAME: &str = "event";

/// Wraps the span processor of an off-host exporter so that it receives only
/// what may leave the host.
///
/// A span leaves only when the operator's trace filter admits its target and
/// level and its target is not a body target. `inner` sees every span start,
/// but only those spans end there, so the rest take no room in its export
/// queue. An event of a body target never leaves with its span, and a
/// library's event leaves named `event`, without its text. Every
/// exported span, event and link keeps only the keys the field catalog
/// lists, and a failed span keeps its error status without its text.
#[derive(Debug)]
pub struct OtlpSpanFilter<E> {
    inner: E,
    targets: Targets,
}

impl<E> OtlpSpanFilter<E> {
    /// Filters the spans `inner` receives with the operator's trace filter `targets`.
    pub const fn new(inner: E, targets: Targets) -> Self {
        Self { inner, targets }
    }
}

impl<P> SpanProcessor for OtlpSpanFilter<P>
where
    P: SpanProcessor,
{
    fn on_start(&self, span: &mut Span, cx: &Context) {
        self.inner.on_start(span, cx);
    }

    fn on_end(&self, mut span: SpanData) {
        if !span_matches_targets(&span, &self.targets) || span_has_denied_target(&span) {
            return;
        }
        retain_exported(&mut span.attributes);
        if matches!(span.status, Status::Error { .. }) {
            span.status = Status::error("");
        }
        span.events.events.retain(|event| {
            !string_attribute(&event.attributes, "target")
                .is_some_and(|target| is_body_target(&target))
        });
        for event in &mut span.events.events {
            if !string_attribute(&event.attributes, "target")
                .is_some_and(|target| is_coral_target(&target))
            {
                event.name = LIBRARY_EVENT_NAME.into();
            }
            retain_exported(&mut event.attributes);
        }
        for link in &mut span.links.links {
            retain_exported(&mut link.attributes);
        }
        self.inner.on_end(span);
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.inner.set_resource(resource);
    }
}

/// Wraps the log processor of an off-host exporter so that it receives only
/// what may leave the host.
///
/// An event of a body target never reaches `inner`. Every other record
/// reaches it with its level, target and trace, and with only the keys
/// the field catalog lists. Tracing's default event name, `event
/// <file>:<line>`, leaves as `code.file.path` and `code.line.number`; an
/// event's own name leaves as its event name. A Coral record keeps its text; a library's
/// record, which an operator's log filter can admit, leaves without it.
#[derive(Debug)]
pub struct OtlpLogFilter<P> {
    inner: P,
    /// An empty record to copy each exported record into, since the SDK
    /// lets a processor add a record's attributes but not remove them.
    template: SdkLogRecord,
}

impl<P> OtlpLogFilter<P> {
    /// Filters the log records `inner` receives.
    #[must_use]
    pub fn new(inner: P) -> Self {
        let template = SdkLoggerProvider::builder()
            .build()
            .logger("coral-telemetry")
            .create_log_record();
        Self { inner, template }
    }

    fn exported(&self, record: &SdkLogRecord) -> SdkLogRecord {
        let mut exported = self.template.clone();
        match record.event_name().map(callsite_location) {
            Some(Some((file, line))) => {
                exported.add_attribute("code.file.path", file);
                exported.add_attribute("code.line.number", line);
            }
            Some(None) => exported.set_event_name(record.event_name().unwrap_or_default()),
            None => {}
        }
        if let Some(target) = record.target() {
            exported.set_target(target.clone());
        }
        if let Some(timestamp) = record.timestamp() {
            exported.set_timestamp(timestamp);
        }
        if let Some(timestamp) = record.observed_timestamp() {
            exported.set_observed_timestamp(timestamp);
        }
        if let Some(context) = record.trace_context() {
            exported.set_trace_context(context.trace_id, context.span_id, context.trace_flags);
        }
        if let Some(text) = record.severity_text() {
            exported.set_severity_text(text);
        }
        if let Some(number) = record.severity_number() {
            exported.set_severity_number(number);
        }
        if let Some(body) = record.body()
            && record
                .target()
                .is_some_and(|target| is_coral_target(target))
        {
            exported.set_body(body.clone());
        }
        exported.add_attributes(
            record
                .attributes_iter()
                .filter(|(key, _)| field::is_exported(key.as_str()))
                .cloned(),
        );
        exported
    }
}

impl<P> LogProcessor for OtlpLogFilter<P>
where
    P: LogProcessor,
{
    fn emit(&self, record: &mut SdkLogRecord, instrumentation: &InstrumentationScope) {
        if record.target().is_some_and(|target| is_body_target(target)) {
            return;
        }
        *record = self.exported(record);
        self.inner.emit(record, instrumentation);
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn event_enabled(&self, level: Severity, target: &str, name: Option<&str>) -> bool {
        !is_body_target(target) && self.inner.event_enabled(level, target, name)
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.inner.set_resource(resource);
    }
}

fn retain_exported(attributes: &mut Vec<KeyValue>) {
    attributes.retain(|attribute| field::is_exported(attribute.key.as_str()));
}

fn span_matches_targets(span: &SpanData, targets: &Targets) -> bool {
    let Some(target) = span_string_attribute(span, "target") else {
        return false;
    };
    let Some(level) = span_level_attribute(span) else {
        return false;
    };
    targets.would_enable(&target, &level)
}

fn span_has_denied_target(span: &SpanData) -> bool {
    span_string_attribute(span, "target").is_some_and(|target| is_body_target(&target))
}

/// The file and line of tracing's default name for an event, `event
/// <file>:<line>`. That name is a code location, not an event class, so a
/// record leaves with it as `code.*` keys. `None` for an event's own name.
fn callsite_location(name: &'static str) -> Option<(&'static str, i64)> {
    let (file, line) = name.strip_prefix("event ")?.rsplit_once(':')?;
    Some((file, line.parse().ok()?))
}

fn is_body_target(target: &str) -> bool {
    DENIED_TARGETS.contains(&target)
}

/// Whether `target` is Coral's own, whose event text the source checks keep
/// fixed. A library's event text can be anything, such as an error's.
fn is_coral_target(target: &str) -> bool {
    target == "coral" || target.starts_with("coral_") || target.starts_with("coral.")
}

fn span_string_attribute(span: &SpanData, key: &str) -> Option<String> {
    string_attribute(&span.attributes, key)
}

fn string_attribute(attributes: &[KeyValue], key: &str) -> Option<String> {
    attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .and_then(|attribute| match &attribute.value {
            Value::String(value) => Some(value.to_string()),
            _ => None,
        })
}

fn span_level_attribute(span: &SpanData) -> Option<tracing::Level> {
    match span_string_attribute(span, "level")?.as_str() {
        "TRACE" => Some(tracing::Level::TRACE),
        "DEBUG" => Some(tracing::Level::DEBUG),
        "INFO" => Some(tracing::Level::INFO),
        "WARN" => Some(tracing::Level::WARN),
        "ERROR" => Some(tracing::Level::ERROR),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::logs::{
        AnyValue, LogRecord as _, Logger as _, LoggerProvider as _, Severity,
    };
    use opentelemetry::trace::TraceContextExt as _;
    use opentelemetry::trace::{
        SpanContext, SpanId, Status, TraceFlags, TraceId, TraceState, TracerProvider as _,
    };
    use opentelemetry::{KeyValue, Value};
    use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
    use opentelemetry_sdk::logs::{
        InMemoryLogExporter, SdkLogRecord, SdkLoggerProvider, SimpleLogProcessor,
    };
    use opentelemetry_sdk::trace::{
        InMemorySpanExporter, SdkTracerProvider, SimpleSpanProcessor, SpanData,
    };
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;
    use tracing_subscriber::prelude::*;

    use super::{OtlpLogFilter, OtlpSpanFilter};

    /// Keys a convention or a careless span gives private data. Each stands
    /// for a field that got past every earlier check, so only the filter
    /// stands between it and the collector.
    const FORBIDDEN: &[&str] = &[
        "misclassified",
        "url.full",
        "url.path",
        "url.query",
        "http.request.header.authorization",
        "http.request.header.cookie",
        "http.response.header.set-cookie",
        "user_agent.original",
        "client.address",
        "exception.message",
        "exception.stacktrace",
        "db.query.text",
        "db.statement",
        "user.email",
        "enduser.id",
        "mcp.session.id",
        "gen_ai.tool.call.arguments",
        "gen_ai.tool.call.result",
        "coral.program.source",
        "coral.task.intent",
        "coral.invocation.arguments",
        "coral.invocation.result",
        "coral.run.emission",
        "coral.run.log",
        "coral.saved_function.source",
        "coral.approval.comment",
        "coral.account.metadata",
        "coral.account.token",
        "coral.credential",
        "coral.local.credential",
        // One character from a cataloged key.
        "coral.run.id.value",
        "coral.run.i",
    ];

    fn forbidden() -> impl Iterator<Item = KeyValue> {
        FORBIDDEN.iter().map(|key| KeyValue::new(*key, "secret"))
    }

    /// Exports the spans below through the filter and returns what left the host.
    fn exported(targets: &str) -> Vec<SpanData> {
        let memory = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_span_processor(OtlpSpanFilter::new(
                SimpleSpanProcessor::new(memory.clone()),
                targets.parse().expect("trace filter"),
            ))
            .build();
        let subscriber = tracing_subscriber::Registry::default().with(
            tracing_opentelemetry::layer()
                .with_tracer(provider.tracer("export-filter-test"))
                .with_level(true),
        );
        tracing::subscriber::with_default(subscriber, || {
            let kept = tracing::info_span!(
                target: "coral_app",
                "kept",
                coral.local.path = "/home/secret",
                coral.run.id = "run-1",
                misclassified = "secret",
            );
            for attribute in forbidden() {
                kept.set_attribute(attribute.key, attribute.value);
            }
            kept.add_event(
                "misclassified",
                forbidden()
                    .chain([KeyValue::new("coral.run.id", "run-1")])
                    .collect(),
            );
            kept.add_link_with_attributes(
                SpanContext::new(
                    TraceId::from_hex("00000000000000000000000000000077").unwrap(),
                    SpanId::from_hex("0000000000000088").unwrap(),
                    TraceFlags::SAMPLED,
                    true,
                    TraceState::default(),
                ),
                forbidden()
                    .chain([KeyValue::new("coral.run.id", "run-1")])
                    .collect(),
            );
            kept.set_status(Status::error("secret"));
            kept.in_scope(|| {
                tracing::info!(
                    target: "coral_app",
                    misclassified = "secret",
                    coral.workspace.id = "workspace-1",
                    coral.local.event = "secret",
                    "coral event"
                );
                tracing::info!(target: "coral.mcp.body", "secret");
                tracing::info!(target: "sqlx::query", "secret");
            });
            for body in [
                tracing::info_span!(target: "coral.http.body", "http body"),
                tracing::info_span!(target: "coral.mcp.body", "mcp body"),
                tracing::info_span!(target: "unlisted", "unlisted"),
                tracing::trace_span!(target: "coral_app", "too verbose"),
            ] {
                body.in_scope(|| {});
            }
        });
        provider.force_flush().expect("flush spans");
        let spans = memory.get_finished_spans().expect("finished spans");
        provider.shutdown().expect("provider shutdown");
        spans
    }

    fn keys(attributes: &[KeyValue]) -> Vec<&str> {
        attributes
            .iter()
            .map(|attribute| attribute.key.as_str())
            .collect()
    }

    /// Only admitted spans leave, and they carry only cataloged keys: no
    /// forbidden key leaves on a span, an event or a link, while the cataloged
    /// key beside it does. A failed span leaves failed, without its status
    /// text, and a library's event leaves without its text.
    #[test]
    fn only_admitted_spans_leave_and_only_with_cataloged_keys() {
        // The filter admits both body targets, so only the deny list keeps them home.
        let spans = exported("coral_app=info,coral.http.body=trace,coral.mcp.body=trace");
        let names = spans
            .iter()
            .map(|span| span.name.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(names, ["kept"]);
        let kept = spans.first().expect("kept span");

        assert!(keys(&kept.attributes).contains(&"coral.run.id"));
        for bridged in ["target", "level", "busy_ns", "idle_ns"] {
            assert!(!keys(&kept.attributes).contains(&bridged), "{bridged}");
        }
        assert_eq!(kept.status, Status::error(""));
        let [misclassified, event, library] = kept.events.events.as_slice() else {
            panic!("only the events of admitted targets: {:?}", kept.events);
        };
        assert_eq!(keys(&misclassified.attributes), ["coral.run.id"]);
        assert_eq!(event.name, "coral event");
        assert_eq!(library.name, "event", "a library's event text stays home");
        assert!(keys(&event.attributes).contains(&"coral.workspace.id"));
        let link = kept.links.links.first().expect("link");
        assert_eq!(keys(&link.attributes), ["coral.run.id"]);
        for attributes in [&kept.attributes, &event.attributes, &link.attributes] {
            assert!(
                keys(attributes)
                    .iter()
                    .all(|key| crate::field::is_exported(key)),
                "{:?}",
                keys(attributes)
            );
            assert!(
                !attributes
                    .iter()
                    .any(|attribute| attribute.value == Value::from("/home/secret")
                        || attribute.value == Value::from("secret")),
                "{attributes:?}"
            );
        }
    }

    /// Exports the events below as log records through the filter and
    /// returns what left the host, with the trace of the span they ran in.
    fn exported_logs() -> (Vec<SdkLogRecord>, TraceId) {
        let memory = InMemoryLogExporter::default();
        let provider = SdkLoggerProvider::builder()
            .with_log_processor(OtlpLogFilter::new(SimpleLogProcessor::new(memory.clone())))
            .build();
        let capture = crate::testing::SpanCapture::default();
        let subscriber = tracing_subscriber::Registry::default()
            .with(capture.layer())
            .with(OpenTelemetryTracingBridge::new(&provider));
        let trace = tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("operation");
            span.in_scope(|| {
                tracing::warn!(
                    target: "coral_app",
                    misclassified = "secret",
                    coral.run.id = "run-1",
                    coral.local.path = "/home/secret",
                    "kept"
                );
                tracing::warn!(target: "coral.http.body", body = "secret", "http body");
                tracing::warn!(target: "coral.mcp.body", body = "secret", "mcp body");
                tracing::warn!(target: "sqlx::query", "secret");
                tracing::warn!(name: "coral.test.named", target: "coral_app", "named");
            });
            span.context().span().span_context().trace_id()
        });
        let logger = provider.logger("export-filter-test");
        let mut misclassified = logger.create_log_record();
        misclassified.set_target("coral_app");
        misclassified.add_attributes(
            forbidden()
                .chain([KeyValue::new("coral.run.id", "run-1")])
                .map(|attribute| (attribute.key, AnyValue::from(attribute.value.to_string()))),
        );
        logger.emit(misclassified);
        provider.force_flush().expect("flush logs");
        let records = memory
            .get_emitted_logs()
            .expect("emitted logs")
            .into_iter()
            .map(|log| log.record)
            .collect();
        provider.shutdown().expect("provider shutdown");
        (records, trace)
    }

    /// A log record leaves with its text, level and trace but only cataloged
    /// keys, so no forbidden key leaves, and an event of a body target never
    /// leaves. A library's record
    /// leaves without its text, which Coral's checks never read. Tracing's
    /// callsite name leaves as the code location; an event's own name stays.
    #[test]
    fn log_records_leave_only_with_cataloged_keys() {
        let (records, trace) = exported_logs();
        let [kept, library, named, misclassified] = records.as_slice() else {
            panic!("four exported log records: {records:?}");
        };
        assert_eq!(
            misclassified
                .attributes_iter()
                .map(|(key, value)| (key.as_str(), value.clone()))
                .collect::<Vec<_>>(),
            [("coral.run.id", AnyValue::from("run-1"))]
        );
        assert_eq!(named.event_name(), Some("coral.test.named"));
        assert_eq!(kept.event_name(), None, "a callsite name is no event class");
        assert_eq!(library.target().map(AsRef::as_ref), Some("sqlx::query"));
        assert_eq!(library.body(), None);
        assert_eq!(kept.body(), Some(&AnyValue::from("kept")));
        assert_eq!(kept.severity_number(), Some(Severity::Warn));
        assert_eq!(kept.target().map(AsRef::as_ref), Some("coral_app"));
        assert_eq!(
            kept.trace_context().map(|context| context.trace_id),
            Some(trace)
        );
        let mut attributes = kept
            .attributes_iter()
            .map(|(key, value)| (key.as_str(), value.clone()))
            .collect::<Vec<_>>();
        attributes.sort_by_key(|(key, _)| *key);
        let line = attributes
            .iter()
            .find(|(key, _)| *key == "code.line.number")
            .map(|(_, value)| value.clone());
        assert!(
            matches!(line, Some(AnyValue::Int(line)) if line > 0),
            "{attributes:?}"
        );
        assert_eq!(
            attributes
                .iter()
                .filter(|(key, _)| *key != "code.line.number")
                .cloned()
                .collect::<Vec<_>>(),
            [
                (
                    "code.file.path",
                    AnyValue::from("crates/coral-telemetry/src/export.rs")
                ),
                ("coral.run.id", AnyValue::from("run-1")),
            ]
        );
    }
}
