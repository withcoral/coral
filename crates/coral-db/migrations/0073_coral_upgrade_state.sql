-- The Coral releases that ran on this database. Coral reads the row before it
-- migrates and writes it after, at every start.
--
-- `newest_release` is the newest release that started here. A migration that
-- removes what older releases use names the release that stopped using it. On
-- Postgres, where two releases can run at once, Coral applies that migration
-- only when the named release has started here.
--
-- `oldest_release` is the oldest release that runs on this schema: the newest
-- one that such a migration names. An older Coral does not start.
--
-- Each is major * 1e12 + minor * 1e6 + patch, so one statement can keep the
-- greater of two releases.
CREATE TABLE coral_upgrade_state (
    id TEXT PRIMARY KEY NOT NULL,
    newest_release BIGINT NOT NULL,
    oldest_release BIGINT NOT NULL,
    CONSTRAINT coral_upgrade_state_singleton CHECK (id = 'current')
);
