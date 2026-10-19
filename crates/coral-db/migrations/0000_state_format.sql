-- Infrastructure marker owned by the fresh-state fence, not a domain schema.
CREATE TABLE coral_state_format (
    id TEXT PRIMARY KEY NOT NULL,
    format TEXT NOT NULL,
    CONSTRAINT coral_state_format_singleton CHECK (id = 'current'),
    CONSTRAINT coral_state_format_value CHECK (format = 'coral-state-v1')
);

INSERT INTO coral_state_format (id, format)
VALUES ('current', 'coral-state-v1');
