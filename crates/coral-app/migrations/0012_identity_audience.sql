-- Existing rows predate pinned identity audiences and remain NULL/NULL so
-- management APIs can expose them without inventing authorization semantics.
ALTER TABLE identities
    ADD COLUMN identity_spec_audience_port BIGINT
    CHECK (
        identity_spec_audience_port IS NULL
        OR identity_spec_audience_port BETWEEN 1 AND 65535
    );

ALTER TABLE identities
    ADD COLUMN identity_spec_audience_host TEXT
    CHECK (
        (
            identity_spec_audience_host IS NULL
            AND identity_spec_audience_port IS NULL
        )
        OR (
            identity_spec_audience_host IS NOT NULL
            AND length(trim(identity_spec_audience_host)) > 0
        )
    );
