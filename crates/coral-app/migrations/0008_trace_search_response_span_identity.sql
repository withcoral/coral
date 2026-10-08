CREATE UNIQUE INDEX idx_trace_search_responses_span_identity_unique
    ON trace_search_responses (trace_id, search_span_id);
