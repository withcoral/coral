use std::fs;
use std::time::SystemTime;

use serde_json::{Map, Value, json};
use tempfile::TempDir;

use super::super::tests::{
    set_modified_time, timestamped_jsonl_path, trace_record, write_record_file,
    write_record_file_lines,
};
use super::super::{
    StoredTraceStatus, TraceDetailRecord, TraceReadScope, TraceSpanRecord, TraceStore,
    TraceSummaryRecord, TraceWorkspaceScope,
};

pub(super) fn span(trace_id: &str, span_id: &str) -> TestSpan {
    TestSpan {
        record: trace_record(trace_id, span_id),
        attributes: Map::new(),
    }
}

pub(super) struct TestSpan {
    record: TraceSpanRecord,
    attributes: Map<String, Value>,
}

impl TestSpan {
    pub(super) fn named(mut self, name: &str) -> Self {
        self.record.name = name.to_string();
        self
    }

    pub(super) fn remote_root(mut self) -> Self {
        self.record.parent_span_id = Some("remote-parent".to_string());
        self.record.parent_span_is_remote = true;
        self
    }

    pub(super) fn child_of(mut self, parent: &TraceSpanRecord) -> Self {
        self.record.parent_span_id = Some(parent.span_id.clone());
        self
    }

    pub(super) fn entry(mut self, kind: &str, operation_name: &str, workspace: &str) -> Self {
        self.attributes.extend(
            json!({
                "coral.stream.entry": true,
                "coral.stream.kind": kind,
                "coral.stream.name": operation_name,
                "workspace": workspace,
            })
            .as_object()
            .expect("stream entry attributes")
            .clone(),
        );
        self
    }

    pub(super) fn attrs(mut self, attributes: Value) -> Self {
        let Value::Object(attributes) = attributes else {
            panic!("additional span attributes must be an object");
        };
        self.attributes.extend(attributes);
        self
    }

    pub(super) fn status(mut self, status: StoredTraceStatus) -> Self {
        self.record.status = status;
        self
    }

    pub(super) fn times(mut self, start_time_unix_nanos: i64, end_time_unix_nanos: i64) -> Self {
        self.record.start_time_unix_nanos = start_time_unix_nanos;
        self.record.end_time_unix_nanos = end_time_unix_nanos;
        self
    }

    pub(super) fn build(mut self) -> TraceSpanRecord {
        self.record.attributes_json = Value::Object(self.attributes).to_string();
        self.record
    }
}

pub(super) fn project(
    records: &[TraceSpanRecord],
    workspace_name: Option<&str>,
) -> Vec<TraceSummaryRecord> {
    super::summaries(
        records,
        workspace_name.map_or(
            TraceWorkspaceScope::Unrestricted,
            TraceWorkspaceScope::Named,
        ),
    )
}

pub(super) struct TraceFiles {
    temp: TempDir,
}

impl TraceFiles {
    pub(super) fn new() -> Self {
        Self {
            temp: TempDir::new().expect("temp dir"),
        }
    }

    pub(super) fn write(&self, name: &str, records: &[TraceSpanRecord]) {
        write_record_file_lines(&self.temp.path().join(name), records);
    }

    pub(super) fn write_at(&self, record: &TraceSpanRecord, modified: SystemTime) {
        self.write_named_at(&timestamped_jsonl_path(modified), record, modified);
    }

    pub(super) fn write_named_at(
        &self,
        name: &str,
        record: &TraceSpanRecord,
        modified: SystemTime,
    ) {
        let path = self.temp.path().join(name);
        write_record_file(&path, record);
        set_modified_time(&path, modified);
    }

    pub(super) fn write_invalid_at(&self, modified: SystemTime) {
        let path = self.temp.path().join(timestamped_jsonl_path(modified));
        fs::write(&path, [0xff]).expect("write unreadable JSONL text");
        set_modified_time(&path, modified);
    }

    pub(super) fn list(
        &self,
        limit: usize,
        offset: usize,
        workspace_name: Option<&str>,
    ) -> Vec<TraceSummaryRecord> {
        self.list_scoped(
            limit,
            offset,
            workspace_name.map_or(
                TraceWorkspaceScope::Unrestricted,
                TraceWorkspaceScope::Named,
            ),
        )
    }

    pub(super) fn list_scoped(
        &self,
        limit: usize,
        offset: usize,
        scope: TraceWorkspaceScope<'_>,
    ) -> Vec<TraceSummaryRecord> {
        super::list(
            &TraceStore::new(self.temp.path().to_path_buf()),
            limit,
            offset,
            scope,
        )
        .expect("list query stream")
    }

    pub(super) fn get(
        &self,
        trace_id: &str,
        scope: &TraceReadScope,
    ) -> Result<TraceDetailRecord, super::super::TraceStoreError> {
        TraceStore::new(self.temp.path().to_path_buf()).get_query_stream_trace_sync(trace_id, scope)
    }
}
