-- A Built-in Tool's catalog can declare that the Tool is `enabled` when no
-- exact rule matches, as `saveFunction` and `deleteFunction` do (ADR 0031).
-- Policy reads it from each retained Generation beside the Tool's hints and
-- HTTP method. Every existing row, and every row an older Coral writes,
-- declares no default and keeps the one its hints and method give.
ALTER TABLE materialized_tools
    ADD COLUMN enabled_by_default BOOLEAN NOT NULL DEFAULT FALSE
        CONSTRAINT materialized_tools_enabled_by_default_builtin
            CHECK (NOT enabled_by_default OR protocol = 'builtin');
