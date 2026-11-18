-- The step that a Coral's plan must have for that Coral to run on this
-- database. Coral reads the row before it migrates, at every start.
--
-- A migration that removes what older Corals use names, in its
-- `rolling-upgrade` comment, the step of the change that stopped using it.
-- `required_step` is the highest step that an applied migration names so. A
-- Coral whose plan lacks it still uses what was removed, and does not start.
--
-- That comment also names the first release that has the step, and
-- `required_release` keeps it beside the step. Coral decides nothing by it:
-- the Coral that does not start tells the operator to start that release.
--
-- The table has no row until such a migration is applied.
CREATE TABLE coral_upgrade_state (
    id TEXT PRIMARY KEY NOT NULL,
    required_step BIGINT NOT NULL,
    required_release TEXT NOT NULL,
    CONSTRAINT coral_upgrade_state_singleton CHECK (id = 'current')
);
