-- A compile relayed through the database (ADR 0027). A Coral Instance that
-- holds no pull stream with room writes a row with its own deadline; any Coral
-- Instance with room, the requester included, takes the oldest untaken row,
-- serves it and writes the answer back. A row with no call asks only for room,
-- and its answer is empty. The requester deletes its row when it reads the
-- answer or its deadline passes; a pickup deletes the rows past their deadline
-- it finds. call and answer hold the encoded Compile and Compiled messages.
-- Times are Unix nanoseconds. Postgres stores the messages as BYTEA.
CREATE TABLE compile_requests (
    id TEXT PRIMARY KEY,
    requester TEXT NOT NULL,
    call BYTEA,
    created_at BIGINT NOT NULL,
    deadline BIGINT NOT NULL,
    taken_by TEXT,
    answer BYTEA
);

-- The pickup reads the oldest untaken row. The table is new, so the index
-- builds at once; lock_timeout is the migration lint's rule for every index.
SET LOCAL lock_timeout = '5s';

CREATE INDEX compile_requests_untaken ON compile_requests (created_at, id)
    WHERE taken_by IS NULL;
