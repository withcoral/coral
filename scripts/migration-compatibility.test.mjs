import assert from 'node:assert/strict'
import test from 'node:test'

import { check } from './migration-compatibility.mjs'

const file = '0010_runs.sql'
const problems = (sql) => check(file, sql, ['v1.0.0']).join('\n')

// What each statement does to a Coral release that runs while it is applied.
const breaking = {
  'ALTER TABLE workspaces DROP COLUMN name;': /drops a column/,
  'ALTER TABLE workspaces DROP name;': /drops a column/,
  'DROP TABLE workspaces;': /drops the table/,
  'DROP INDEX workspaces_name;': /drops the index/,
  'ALTER TABLE workspaces RENAME COLUMN name TO title;': /renames/,
  'ALTER TABLE workspaces RENAME TO spaces;': /renames/,
  'ALTER INDEX workspaces_name RENAME TO spaces_name;': /renames/,
  'ALTER TABLE workspaces ALTER COLUMN name TYPE BIGINT;': /changes a column type/,
  'ALTER TABLE workspaces ALTER COLUMN "my name" SET DATA TYPE BIGINT;': /changes a column type/,
  'ALTER TABLE workspaces ALTER COLUMN name SET NOT NULL;': /requires a value/,
  'ALTER TABLE workspaces ALTER COLUMN name DROP DEFAULT;': /drops a default/,
  'ALTER TABLE workspaces ADD COLUMN owner TEXT NOT NULL;': /adds a required column/,
  // One ALTER TABLE holds an additive action and a breaking one.
  "ALTER TABLE workspaces\n  ADD COLUMN kind TEXT NOT NULL DEFAULT 'a',\n  ADD COLUMN owner TEXT NOT NULL;":
    /adds a required column/,
  "ALTER TABLE workspaces ADD CONSTRAINT workspaces_kind CHECK (kind IN ('a', 'b')) NOT VALID;": /adds a constraint/,
  'ALTER TABLE workspaces DROP CONSTRAINT workspaces_kind;': /drops a constraint/,
  'CREATE UNIQUE INDEX workspaces_name ON workspaces (name);': /adds a unique index/,
  "UPDATE workspaces SET name = lower(name) WHERE name <> '';": /rewrites values/,
  // A backfill may not also rewrite a column that a release already reads.
  "ALTER TABLE workspaces ADD COLUMN slug TEXT;\nUPDATE workspaces SET slug = name, name = '';": /rewrites values/,
  "DELETE FROM workspaces WHERE name = '';": /deletes rows/,
  // A name may carry its schema, and each part may be quoted.
  'ALTER TABLE "public"."workspaces" DROP COLUMN name;': /drops a column/,
  'ALTER TABLE public."workspaces" ADD COLUMN owner TEXT NOT NULL;': /adds a required column/,
  'UPDATE "public"."workspaces" SET name = 1;': /rewrites values/,
  // Another table of the same name in another schema is not the new table.
  'CREATE TABLE audit.workspaces (id TEXT);\nALTER TABLE workspaces DROP COLUMN name;': /drops a column/,
  // A write that another statement carries.
  "WITH old AS (SELECT id FROM workspaces) UPDATE workspaces SET name = '' FROM old;": /rewrites values/,
  'WITH gone AS (DELETE FROM workspaces RETURNING id) SELECT count(*) FROM gone;': /rewrites values/,
  "INSERT INTO workspaces (id, name) VALUES ('a', 'b') ON CONFLICT (id) DO UPDATE SET name = 'b';": /rewrites values/,
  'MERGE INTO workspaces USING runs ON workspaces.id = runs.id WHEN MATCHED THEN DELETE;': /rewrites values/,
  // Neither an escaped quote nor a quoted name ends a string early.
  "INSERT INTO runs (id) VALUES (E'a\\'b');\nDROP TABLE workspaces;\nINSERT INTO runs (id) VALUES ('z');":
    /drops the table/,
  'ALTER TABLE "it\'s" ADD COLUMN owner TEXT;\nDROP TABLE workspaces;\nINSERT INTO runs (id) VALUES (\'z\');':
    /drops the table/,
  // A default of NULL gives an older release's insert no value.
  'ALTER TABLE workspaces ADD COLUMN owner TEXT NOT NULL DEFAULT NULL;': /adds a required column/,
  // The table may be one that a release already uses.
  'CREATE TABLE IF NOT EXISTS workspaces (id TEXT);\nALTER TABLE workspaces DROP COLUMN name;': /drops a column/,
  // Nor is a column new when the migration only may have added it.
  "ALTER TABLE workspaces ADD COLUMN IF NOT EXISTS name TEXT;\nUPDATE workspaces SET name = '';": /rewrites values/,
  // A comment between two words separates them.
  'ALTER/**/TABLE workspaces DROP COLUMN name;': /drops a column/,
  // Every table of one DROP TABLE counts, not only the first.
  'CREATE TABLE scratch (id TEXT);\nDROP TABLE scratch, workspaces;': /drops the table/,
  // Two quoted names are one table only when they are the same text.
  'CREATE TABLE "a-b" (id TEXT);\nDROP TABLE "a_b";': /drops the table: DROP TABLE "a_b"/,
  'CREATE TABLE "Workspaces" (id TEXT);\nDROP TABLE workspaces;': /drops the table/,
  // What the first version of this check did not read.
  "DO $$ BEGIN ALTER TABLE workspaces DROP COLUMN name; END $$;": /runs a block that this check cannot read/,
  'CREATE TRIGGER stamp BEFORE INSERT ON workspaces FOR EACH ROW EXECUTE FUNCTION stamp();': /adds a trigger/,
  'ALTER TABLE workspaces SET SCHEMA archive;': /moves a table to another schema/,
  "INSERT OR REPLACE INTO workspaces (id) VALUES ('a');": /rewrites values/,
  "REPLACE INTO workspaces (id) VALUES ('a');": /rewrites values/,
  // A declaration is a comment, not text that a string holds.
  "INSERT INTO runs (id) VALUES ('\n-- rolling-upgrade: compatible. Not a comment.\n');\nDROP TABLE workspaces;":
    /drops the table/,
}

for (const [sql, reason] of Object.entries(breaking)) {
  test(`a migration that runs \`${sql}\` can break an older release`, () => {
    const found = problems(sql)
    assert.match(found, /0010_runs\.sql can break a Coral release/)
    assert.match(found, reason)
  })
}

const compatible = {
  'a new table that it then changes as it likes':
    'CREATE TABLE runs (id TEXT PRIMARY KEY, name TEXT NOT NULL, old TEXT);\n' +
    'CREATE UNIQUE INDEX runs_name ON runs (name);\n' +
    "ALTER TABLE runs ADD CONSTRAINT runs_name_set CHECK (name <> ''), DROP COLUMN old;\n" +
    "INSERT INTO runs (id, name) VALUES ('a', 'b') ON CONFLICT (id) DO UPDATE SET name = 'b';\n" +
    "UPDATE runs SET name = 'c';\nDELETE FROM runs;\n",
  'a scratch table that it drops': 'CREATE TABLE scratch (id TEXT);\nDROP TABLE scratch;\n',
  'scratch tables that it drops together, one by its quoted name':
    'CREATE TABLE scratch (id TEXT);\nCREATE TABLE "a-b" (id TEXT);\nDROP TABLE "scratch", "a-b" CASCADE;\n',
  'a nullable column': 'ALTER TABLE workspaces ADD COLUMN owner TEXT;\n',
  'a required column with a default': "ALTER TABLE workspaces ADD COLUMN kind TEXT NOT NULL DEFAULT 'a';\n",
  'a column and its backfill':
    'ALTER TABLE workspaces ADD COLUMN slug TEXT;\n' +
    "UPDATE workspaces AS w SET slug = lower(replace(w.name, ',', '')) WHERE slug IS NULL;\n",
  'an index': 'CREATE INDEX workspaces_name ON workspaces (name);\n',
  'rows for a table that a release reads': "INSERT INTO workspaces (id) VALUES ('a') ON CONFLICT DO NOTHING;\n",
  // Neither a comment nor a string is SQL.
  'words that only a comment, a string and a function body hold':
    '-- This does not DROP TABLE workspaces; it keeps it.\n' +
    '/* Nor does it DELETE FROM workspaces; */\n' +
    "INSERT INTO workspaces (id) VALUES ('x; DROP TABLE workspaces');\n" +
    'CREATE FUNCTION touch() RETURNS trigger AS $body$ BEGIN UPDATE workspaces SET name = 1; END $body$ LANGUAGE plpgsql;\n',
  'a breaking statement that it declares compatible':
    '-- The wider rule allows every value an older release writes.\n' +
    '-- rolling-upgrade: compatible. Widens the kind rule.\n' +
    "ALTER TABLE workspaces DROP CONSTRAINT workspaces_kind,\n  ADD CONSTRAINT workspaces_kind CHECK (kind IN ('a', 'b')) NOT VALID;\n",
  'a table rebuild that it declares compatible':
    '-- rolling-upgrade: compatible. The rebuilt table keeps every column and row.\n' +
    'CREATE TABLE workspaces_new (id TEXT PRIMARY KEY, name TEXT);\n' +
    'INSERT INTO workspaces_new (id, name) SELECT id, name FROM workspaces;\n' +
    'DROP TABLE workspaces;\nALTER TABLE workspaces_new RENAME TO workspaces;\n',
  'a trigger on a table that it creates':
    'CREATE TABLE runs (id TEXT);\nCREATE TRIGGER stamp BEFORE INSERT ON runs FOR EACH ROW EXECUTE FUNCTION stamp();\n',
  'a removal that names the release that stopped using it':
    '-- rolling-upgrade: unused since v1.0.0. No query names the column.\nALTER TABLE workspaces DROP COLUMN name;\n',
}

for (const [name, sql] of Object.entries(compatible)) {
  test(`a migration that holds ${name} passes`, () => {
    assert.equal(problems(sql), '')
  })
}

const drop = 'ALTER TABLE workspaces DROP COLUMN name;\n'
const declarations = {
  'names a release the checkout does not build on': [
    `-- rolling-upgrade: unused since v9.9.9. No query names the column.\n${drop}`,
    /names v9\.9\.9, which is not a release/,
  ],
  'gives no reason': [`-- rolling-upgrade: compatible.\n${drop}`, /must read/],
  'has an unknown form': [`-- rolling-upgrade: safe. Nobody reads it.\n${drop}`, /must read/],
  'is repeated': [`-- rolling-upgrade: compatible. One.\n-- rolling-upgrade: compatible. Two.\n${drop}`, /more than once/],
  // One declaration covers the file, so `compatible` must not hide a removal.
  'calls a dropped table compatible beside a wider rule': [
    '-- rolling-upgrade: compatible. Widens the kind rule.\n' +
      "ALTER TABLE workspaces DROP CONSTRAINT workspaces_kind, ADD CONSTRAINT workspaces_kind CHECK (kind <> '');\n" +
      'DROP TABLE runs;\n',
    /cannot be `compatible`[^]*drops the table: DROP TABLE runs$/,
  ],
  'calls a renamed column compatible': [
    '-- rolling-upgrade: compatible. Same data.\nALTER TABLE workspaces RENAME COLUMN name TO title;\n',
    /cannot be `compatible`/,
  ],
  'calls a changed column type compatible': [
    '-- rolling-upgrade: compatible. Same data.\nALTER TABLE workspaces ALTER COLUMN name TYPE BIGINT;\n',
    /cannot be `compatible`/,
  ],
  'has nothing to declare': [
    '-- rolling-upgrade: compatible. Adds a table.\nCREATE TABLE runs (id TEXT PRIMARY KEY);\n',
    /remove it/,
  ],
}

for (const [name, [sql, problem]] of Object.entries(declarations)) {
  test(`a rolling-upgrade declaration that ${name} fails`, () => {
    assert.match(problems(sql), problem)
  })
}
