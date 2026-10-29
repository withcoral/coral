//! Persistence-foundation behavior shared by `SQLite` and Postgres.

#![allow(
    unused_crate_dependencies,
    reason = "integration tests share the library dependency set"
)]

use std::sync::LazyLock;

use coral_db::{
    Database, DatabaseOptions, DatabaseSystem, MigrationPlan, QueryExecutor, ReadOnlyDatabase,
};
use coral_test_support::TestDatabase;
use sea_query::{Expr, ExprTrait as _, Iden, Query};

static MIGRATOR: LazyLock<MigrationPlan> =
    LazyLock::new(|| MigrationPlan::shared(&sqlx::migrate!("./tests/migrations").migrations));
static DRIFTED_MIGRATOR: LazyLock<MigrationPlan> = LazyLock::new(|| {
    MigrationPlan::shared(&sqlx::migrate!("./tests/drifted_migrations").migrations)
});

mod sqlite_only {
    use super::*;

    #[tokio::test]
    async fn backend_selection_opens_a_live_database() {
        coral_test_support::database_only!(Sqlite);
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("state").join("coral.db");

        let database = Database::connect(DatabaseOptions::Sqlite { path: path.clone() })
            .await
            .expect("connect SQLite");

        assert_eq!(database.system(), DatabaseSystem::Sqlite);
        database.ping().await.expect("ping SQLite");
        assert!(path.is_file());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_a_symlink_without_changing_its_target() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        coral_test_support::database_only!(Sqlite);

        let temp = tempfile::tempdir().expect("temp dir");
        let target = temp.path().join("target.db");
        let path = temp.path().join("coral.db");
        std::fs::write(&target, "existing database").expect("write target");
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644))
            .expect("set target permissions");
        symlink(&target, &path).expect("create symlink");

        let error = match Database::connect(DatabaseOptions::Sqlite { path }).await {
            Ok(database) => {
                database.close().await;
                panic!("SQLite symlink must be rejected");
            }
            Err(error) => error,
        };

        assert!(error.to_string().contains("not a regular file"));
        let target_mode = std::fs::metadata(target)
            .expect("target metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(target_mode, 0o644);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_permissions_are_private() {
        use std::os::unix::fs::PermissionsExt as _;

        coral_test_support::database_only!(Sqlite);

        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("coral.db");
        std::fs::write(&path, []).expect("create database file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("set loose permissions");

        Database::connect(DatabaseOptions::Sqlite { path: path.clone() })
            .await
            .expect("connect SQLite");

        let mode = std::fs::metadata(path)
            .expect("database metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[tokio::test]
    async fn read_only_database_can_inspect_without_write_authority() {
        coral_test_support::database_only!(Sqlite);
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("coral.db");
        let database = Database::connect(DatabaseOptions::Sqlite { path: path.clone() })
            .await
            .expect("connect SQLite");
        database.migrate(&MIGRATOR).await.expect("apply migrations");
        write_record(&database, "read-only", "visible")
            .await
            .expect("write fixture record");
        database.close().await;

        let read_only = ReadOnlyDatabase::connect_read_only(DatabaseOptions::Sqlite { path })
            .await
            .expect("connect read-only SQLite");
        let row: Option<Record> = read_only
            .fetch_optional(select_record("read-only"))
            .await
            .expect("inspect record");

        assert_eq!(
            row,
            Some(Record {
                id: "read-only".to_string(),
                value: "visible".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn read_then_write_waits_for_a_concurrent_writer() {
        coral_test_support::database_only!(Sqlite);
        // One handle queues its own writers; a peer handle stands in for another
        // process, which only SQLite's busy handler arbitrates.
        for peer in [false, true] {
            let temp = tempfile::tempdir().expect("temp dir");
            let options = DatabaseOptions::Sqlite {
                path: temp.path().join("coral.db"),
            };
            let database = Database::connect(options.clone())
                .await
                .expect("connect SQLite");
            database.migrate(&MIGRATOR).await.expect("apply migrations");
            write_record(&database, "seed", "visible")
                .await
                .expect("write fixture record");
            let peer_database = if peer {
                Some(Database::connect(options).await.expect("connect peer"))
            } else {
                None
            };
            let contender_database = peer_database.as_ref().unwrap_or(&database);

            let mut holder = database.begin().await.expect("begin lock holder");
            holder
                .execute(insert_record("holder", "first"))
                .await
                .expect("take the write lock");

            let release = async {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                holder.commit().await
            };
            let contender = async {
                let mut contender = contender_database.begin().await?;
                record(&mut contender, "seed").await;
                contender
                    .execute(insert_record("contender", "second"))
                    .await?;
                contender.commit().await
            };
            let (holder_committed, contender_committed) = tokio::join!(release, contender);

            holder_committed.expect("commit lock holder");
            contender_committed.unwrap_or_else(|error| {
                panic!("peer={peer}: a read-then-write transaction must wait: {error:?}")
            });
            assert!(record(&database, "contender").await.is_some());
        }
    }

    #[tokio::test]
    async fn a_writer_queued_past_the_busy_timeout_reports_a_conflict() {
        coral_test_support::database_only!(Sqlite);
        let temp = tempfile::tempdir().expect("temp dir");
        let database = Database::connect(DatabaseOptions::Sqlite {
            path: temp.path().join("coral.db"),
        })
        .await
        .expect("connect SQLite");
        database.migrate(&MIGRATOR).await.expect("apply migrations");
        let mut holder = database.begin().await.expect("begin lock holder");
        holder
            .execute(insert_record("holder", "first"))
            .await
            .expect("take the write lock");

        // Paused time lets the writer pool's acquire timeout elapse at once.
        let metrics = coral_telemetry::testing::MetricCapture::default();
        let measured = tracing::subscriber::set_default(metrics.subscriber());
        tokio::time::pause();
        let Err(error) = database.begin().await else {
            panic!("a second writer must not begin while the first holds the lock");
        };
        tokio::time::resume();
        drop(measured);

        assert!(matches!(error, coral_db::Error::WriterBusy), "{error:?}");
        assert!(error.is_serialization_conflict());
        assert_eq!(
            metrics
                .named("coral.persistence.transaction.failures")
                .into_iter()
                .map(|point| (point.attributes, point.value))
                .collect::<Vec<_>>(),
            [(
                [("db.system", "sqlite"), ("failure.code", "writer_busy")]
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .into(),
                1.0
            )],
            "the busy writer counts as a failed transaction"
        );
        holder.commit().await.expect("commit lock holder");
        assert!(record(&database, "holder").await.is_some());
    }

    #[tokio::test]
    async fn migrations_reject_unexpected_schema_objects() {
        coral_test_support::database_only!(Sqlite);
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("coral.db");
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let pool = sqlx::SqlitePool::connect_with(options)
            .await
            .expect("open drifted SQLite database");
        sqlx::query(
            "CREATE TABLE persistence_contract_records (id TEXT PRIMARY KEY, value TEXT NOT NULL)",
        )
        .execute(&pool)
        .await
        .expect("create unexpected table");
        pool.close().await;
        let database = Database::connect(DatabaseOptions::Sqlite { path })
            .await
            .expect("connect SQLite");

        let error = database
            .migrate(&MIGRATOR)
            .await
            .expect_err("unexpected schema object must be rejected");

        assert!(
            error.to_string().contains("already exists"),
            "unexpected schema drift error: {error}"
        );
    }

    #[tokio::test]
    async fn a_migration_that_leaves_a_foreign_key_without_its_row_is_reported() {
        coral_test_support::database_only!(Sqlite);
        let fixture = TestDatabase::new(&fixture_plan(&[(1, LINKED_TABLES, LINKED_TABLES)]))
            .await
            .unwrap();
        let orphan = "INSERT INTO fixture_children (id, parent_id) VALUES ('orphan', 'missing');";
        let plan = fixture_plan(&[(1, LINKED_TABLES, LINKED_TABLES), (2, orphan, orphan)]);

        // A boot that commits the migration would skip the check on the next.
        for boot in ["first", "next"] {
            let error = fixture
                .database()
                .migrate(&plan)
                .await
                .expect_err("an orphaned row must be reported");
            assert!(
                matches!(
                    &error,
                    coral_db::Error::MigrationForeignKeys { tables } if tables == "fixture_children"
                ),
                "{boot} boot: {error:?}"
            );
        }

        let children: Vec<(String,)> = fixture
            .database()
            .fetch_all(
                Query::select()
                    .column(sea_query::Alias::new("id"))
                    .from(sea_query::Alias::new("fixture_children"))
                    .to_owned(),
            )
            .await
            .unwrap();
        assert!(children.is_empty(), "{children:?}");
    }
}

/// A parent whose kind a CHECK lists, and a child whose rows go with their parent.
const LINKED_TABLES: &str = "CREATE TABLE fixture_parents (
    id TEXT NOT NULL PRIMARY KEY,
    kind TEXT NOT NULL,
    CONSTRAINT fixture_parents_kind_valid CHECK (kind IN ('listed'))
);
CREATE TABLE fixture_children (
    id TEXT NOT NULL PRIMARY KEY,
    parent_id TEXT NOT NULL REFERENCES fixture_parents(id) ON DELETE CASCADE
);";

/// Drops the parent's CHECK in place.
const DROP_PARENT_KIND_RULE_POSTGRES: &str =
    "ALTER TABLE fixture_parents DROP CONSTRAINT fixture_parents_kind_valid;";

/// Drops the parent's CHECK by rebuilding the table.
const DROP_PARENT_KIND_RULE_SQLITE: &str =
    "CREATE TABLE fixture_parents_new (id TEXT NOT NULL PRIMARY KEY, kind TEXT NOT NULL);
INSERT INTO fixture_parents_new (id, kind) SELECT id, kind FROM fixture_parents;
DROP TABLE fixture_parents;
ALTER TABLE fixture_parents_new RENAME TO fixture_parents;
";

/// A plan of fixture migrations, one per `(version, postgres SQL, SQLite SQL)`.
fn fixture_plan(migrations: &[(i64, &'static str, &'static str)]) -> MigrationPlan {
    use sqlx::SqlSafeStr as _;
    let migrator = |sql_for: fn(&(i64, &'static str, &'static str)) -> &'static str| {
        sqlx::migrate::Migrator::with_migrations(
            migrations
                .iter()
                .map(|migration| {
                    sqlx::migrate::Migration::new(
                        migration.0,
                        format!("fixture {}", migration.0).into(),
                        sqlx::migrate::MigrationType::Simple,
                        sql_for(migration).into_sql_str(),
                        false,
                    )
                })
                .collect(),
        )
    };
    MigrationPlan::new(migrator(|m| m.1), migrator(|m| m.2))
}

#[tokio::test]
async fn each_database_runs_its_own_migration() {
    let plan = fixture_plan(&[(
        1,
        "CREATE TABLE fixture_backends (name TEXT NOT NULL);
         INSERT INTO fixture_backends (name) VALUES ('postgres');",
        "CREATE TABLE fixture_backends (name TEXT NOT NULL);
         INSERT INTO fixture_backends (name) VALUES ('sqlite');",
    )]);
    let fixture = TestDatabase::new(&plan).await.unwrap();
    let database = fixture.database();
    database
        .migrate(&plan)
        .await
        .expect("validate the database's own migration again");

    let names: Vec<(String,)> = database
        .fetch_all(
            Query::select()
                .column(sea_query::Alias::new("name"))
                .from(sea_query::Alias::new("fixture_backends"))
                .to_owned(),
        )
        .await
        .unwrap();
    let backend = match database.system() {
        DatabaseSystem::Sqlite => "sqlite",
        DatabaseSystem::Postgres => "postgres",
    };
    assert_eq!(names, vec![(backend.to_string(),)]);
}

#[tokio::test]
async fn a_migration_that_rebuilds_a_referenced_table_keeps_the_referencing_rows() {
    let fixture = TestDatabase::new(&fixture_plan(&[(1, LINKED_TABLES, LINKED_TABLES)]))
        .await
        .unwrap();
    let database = fixture.database();
    let mut transaction = database.begin().await.unwrap();
    transaction
        .execute_script(
            "INSERT INTO fixture_parents (id, kind) VALUES ('parent', 'listed');
             INSERT INTO fixture_children (id, parent_id) VALUES ('child', 'parent');",
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    database
        .migrate(&fixture_plan(&[
            (1, LINKED_TABLES, LINKED_TABLES),
            (
                2,
                DROP_PARENT_KIND_RULE_POSTGRES,
                DROP_PARENT_KIND_RULE_SQLITE,
            ),
        ]))
        .await
        .expect("drop the parent's kind rule");

    let children: Vec<(String, String)> = database
        .fetch_all(
            Query::select()
                .columns([
                    sea_query::Alias::new("id"),
                    sea_query::Alias::new("parent_id"),
                ])
                .from(sea_query::Alias::new("fixture_children"))
                .to_owned(),
        )
        .await
        .unwrap();
    assert_eq!(children, vec![("child".to_string(), "parent".to_string())]);

    let mut transaction = database.begin().await.unwrap();
    transaction
        .execute_script("INSERT INTO fixture_parents (id, kind) VALUES ('other', 'unlisted');")
        .await
        .expect("the kind rule is gone");
    let error = transaction
        .execute_script(
            "INSERT INTO fixture_children (id, parent_id) VALUES ('orphan', 'missing');",
        )
        .await
        .expect_err("writers still enforce foreign keys after migrating");
    assert!(
        matches!(
            &error,
            coral_db::Error::Sqlx(sqlx::Error::Database(cause)) if cause.is_foreign_key_violation()
        ),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_domain_migration_plan_is_applied_and_validated() {
    let fixture = TestDatabase::new(&MIGRATOR)
        .await
        .expect("create migrated test database");
    fixture
        .database()
        .migrate(&MIGRATOR)
        .await
        .expect("validate applied migrations");
}

#[tokio::test]
async fn backend_specific_sql_runs_inside_a_caller_owned_transaction() {
    let fixture = TestDatabase::new(&MIGRATOR).await.unwrap();
    let database = fixture.database();
    let (script, insert) = match database.system() {
        DatabaseSystem::Sqlite => (
            "CREATE TABLE raw_audit (id TEXT NOT NULL);
             CREATE TRIGGER raw_audit_after_insert
             AFTER INSERT ON persistence_contract_records
             BEGIN
                 INSERT INTO raw_audit (id) VALUES (NEW.id);
             END;",
            "INSERT INTO persistence_contract_records (id, value) VALUES (?1, ?2)",
        ),
        DatabaseSystem::Postgres => (
            "CREATE TABLE raw_audit (id TEXT NOT NULL);
             CREATE FUNCTION raw_audit_insert() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 INSERT INTO raw_audit (id) VALUES (NEW.id);
                 RETURN NEW;
             END;
             $$;
             CREATE TRIGGER raw_audit_after_insert
             AFTER INSERT ON persistence_contract_records
             FOR EACH ROW EXECUTE FUNCTION raw_audit_insert();",
            "INSERT INTO persistence_contract_records (id, value) VALUES ($1, $2)",
        ),
    };

    let mut transaction = database.begin().await.unwrap();
    transaction.execute_script(script).await.unwrap();
    let inserted = transaction
        .execute_sql(
            insert,
            sea_query::Values(vec!["raw".into(), "bound value".into()]),
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    assert_eq!(inserted, 1);
    assert_eq!(
        record(database.as_ref(), "raw").await,
        Some(Record {
            id: "raw".to_string(),
            value: "bound value".to_string(),
        })
    );
    let audited: Vec<(String,)> = database
        .fetch_all(
            Query::select()
                .column(sea_query::Alias::new("id"))
                .from(sea_query::Alias::new("raw_audit"))
                .to_owned(),
        )
        .await
        .unwrap();
    assert_eq!(audited, vec![("raw".to_string(),)]);
}

#[tokio::test]
async fn backend_specific_sql_is_rolled_back_with_its_transaction() {
    let fixture = TestDatabase::new(&MIGRATOR).await.unwrap();
    let database = fixture.database();
    let insert = match database.system() {
        DatabaseSystem::Sqlite => {
            "INSERT INTO persistence_contract_records (id, value) VALUES (?1, ?2)"
        }
        DatabaseSystem::Postgres => {
            "INSERT INTO persistence_contract_records (id, value) VALUES ($1, $2)"
        }
    };

    let mut transaction = database.begin().await.unwrap();
    transaction
        .execute_script("CREATE TABLE raw_rolled_back (id TEXT NOT NULL)")
        .await
        .unwrap();
    transaction
        .execute_sql(
            insert,
            sea_query::Values(vec!["rolled-back-raw".into(), "hidden".into()]),
        )
        .await
        .unwrap();
    transaction.rollback().await.unwrap();

    assert_eq!(record(database.as_ref(), "rolled-back-raw").await, None);
    let mut transaction = database.begin().await.unwrap();
    transaction
        .execute_script("CREATE TABLE raw_rolled_back (id TEXT NOT NULL)")
        .await
        .expect("the rolled-back table must not exist");
    transaction.rollback().await.unwrap();
}

#[derive(Iden)]
enum Records {
    #[iden = "persistence_contract_records"]
    Table,
    Id,
    Value,
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct Record {
    id: String,
    value: String,
}

#[tokio::test]
async fn migrations_reject_changed_applied_versions() {
    let fixture = TestDatabase::new(&MIGRATOR)
        .await
        .expect("create migrated test database");

    let error = fixture
        .database()
        .migrate(&DRIFTED_MIGRATOR)
        .await
        .expect_err("changed migration must be rejected");

    assert!(
        error
            .to_string()
            .contains("was previously applied but has been modified"),
        "unexpected drift error: {error}"
    );
}

#[tokio::test]
async fn domain_repository_contract() {
    let fixture = TestDatabase::new(&MIGRATOR)
        .await
        .expect("create repository test database");
    assert_domain_repository_contract(&fixture.database()).await;
}

#[tokio::test]
async fn read_snapshots_cannot_write() {
    let fixture = TestDatabase::new(&MIGRATOR)
        .await
        .expect("create repository test database");
    let database = fixture.database();

    let mut snapshot = database
        .begin_read_snapshot()
        .await
        .expect("begin read snapshot");
    let error = snapshot
        .execute(insert_record("snapshot", "hidden"))
        .await
        .expect_err("a read snapshot must reject a write");
    snapshot.rollback().await.expect("roll back read snapshot");

    assert!(matches!(error, coral_db::Error::Sqlx(_)), "{error:?}");
    assert_eq!(record(&*database, "snapshot").await, None);
}

#[tokio::test]
async fn a_canceled_read_snapshot_does_not_make_the_next_writer_read_only() {
    let fixture = TestDatabase::new(&MIGRATOR)
        .await
        .expect("create repository test database");
    let database = fixture.database();

    // The pool takes a released connection back in a background task.
    let settle = || tokio::time::sleep(std::time::Duration::from_millis(5));
    // Cancel after each early poll, including the one that sends `BEGIN` before its reply.
    for polls in 1..=4 {
        // An idle connection acquires at once, so a later poll reaches `BEGIN`.
        database
            .begin_read_snapshot()
            .await
            .expect("warm the pool")
            .rollback()
            .await
            .expect("roll back the warm-up snapshot");
        settle().await;
        let mut begin = Box::pin(database.begin_read_snapshot());
        for _ in 0..polls {
            let poll =
                std::future::poll_fn(|cx| std::task::Poll::Ready(begin.as_mut().poll(cx))).await;
            if let std::task::Poll::Ready(snapshot) = poll {
                snapshot
                    .expect("begin read snapshot")
                    .rollback()
                    .await
                    .expect("roll back read snapshot");
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        drop(begin);
        settle().await;
        // The pool hands out idle connections in turn, so later writers reach every one.
        for writer in 0..8 {
            write_record(
                &database,
                &format!("after-cancel-{polls}-{writer}"),
                "visible",
            )
            .await
            .expect("a writer after a canceled read snapshot");
            settle().await;
        }
    }
}

mod postgres_only {
    use super::*;

    #[tokio::test]
    async fn independent_workers_share_committed_state() {
        coral_test_support::database_only!(Postgres);
        let fixture = TestDatabase::new(&MIGRATOR)
            .await
            .expect("create Postgres worker test database");
        let first = fixture.database();
        assert_eq!(first.system(), DatabaseSystem::Postgres);
        let second = fixture
            .connect_peer()
            .await
            .expect("connect independent worker");

        let (first_write, second_write) = tokio::join!(
            write_record(&first, "first-worker", "first"),
            write_record(&second, "second-worker", "second"),
        );
        first_write.expect("first worker write");
        second_write.expect("second worker write");
        assert!(record(first.as_ref(), "second-worker").await.is_some());
        assert!(record(&second, "first-worker").await.is_some());
    }
}

#[tokio::test]
async fn notifications_follow_commits_not_reads_locks_rollbacks_or_zero_row_writes() {
    use std::time::Duration;
    let fixture = TestDatabase::new(&MIGRATOR).await.unwrap();
    let database = fixture.database();
    let mut observer = database.subscribe_changes().unwrap();
    let mut tx = database.begin().await.unwrap();
    tx.execute_change(insert_record("rolled-back-change", "hidden"))
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    write_record(&database, "unobserved", "lock machinery")
        .await
        .unwrap();
    let mut tx = database.begin_read_snapshot().await.unwrap();
    record(&mut tx, "unobserved").await;
    tx.commit().await.unwrap();
    let mut tx = database.begin().await.unwrap();
    tx.execute_change(
        Query::delete()
            .from_table(Records::Table)
            .and_where(Expr::col(Records::Id).eq("absent"))
            .to_owned(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    tokio::time::timeout(Duration::from_millis(50), observer.changed())
        .await
        .expect_err("non-observable writes must not wake a subscriber");
    let mut tx = database.begin().await.unwrap();
    tx.execute_change(insert_record("observed", "committed"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(50), observer.changed())
        .await
        .expect_err("uncommitted writes must not wake a subscriber");
    tx.commit().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), observer.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record(database.as_ref(), "observed").await.unwrap().value,
        "committed"
    );
}

#[tokio::test]
async fn sqlite_commit_notifies_even_when_the_caller_stops_awaiting_the_acknowledgement() {
    use std::time::Duration;
    coral_test_support::database_only!(Sqlite);
    let fixture = TestDatabase::new(&MIGRATOR).await.unwrap();
    let database = fixture.database();
    let mut observer = database.subscribe_changes().unwrap();
    let mut tx = database.begin().await.unwrap();
    tx.execute_change(insert_record("detached-commit", "visible"))
        .await
        .unwrap();
    let mut commit = Box::pin(tx.commit());
    tokio::select! {
        biased;
        result = &mut commit => result.unwrap(),
        () = std::future::ready(()) => {},
    }
    drop(commit);
    tokio::time::timeout(Duration::from_secs(2), observer.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record(database.as_ref(), "detached-commit")
            .await
            .unwrap()
            .value,
        "visible"
    );
}

#[tokio::test]
async fn postgres_notifications_cross_independent_workers() {
    use std::time::Duration;
    coral_test_support::database_only!(Postgres);
    let keepalive = Duration::from_millis(100);
    let fixture = TestDatabase::new(&MIGRATOR).await.unwrap();
    let observer_db = fixture
        .connect_peer_with_notification_keepalive(keepalive)
        .await
        .unwrap();
    let writer = fixture.connect_peer().await.unwrap();
    let mut observer = observer_db.subscribe_changes().unwrap();
    let other_fixture = TestDatabase::new(&MIGRATOR).await.unwrap();
    let other = other_fixture.database();
    let mut unrelated = other.begin().await.unwrap();
    unrelated
        .execute_change(insert_record("other-schema", "private"))
        .await
        .unwrap();
    unrelated.commit().await.unwrap();
    tokio::time::timeout(Duration::from_millis(50), observer.changed())
        .await
        .expect_err("another state schema must not wake this observer");
    let mut tx = writer.begin().await.unwrap();
    tx.execute_change(insert_record("remote", "visible"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), observer.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record(&observer_db, "remote").await.unwrap().value,
        "visible"
    );
    tokio::time::timeout(keepalive * 5, observer.changed())
        .await
        .expect_err("notification keepalives must not invalidate domain views");
    observer_db.close().await;
    assert!(matches!(
        observer.check(),
        Err(coral_db::Error::ObservationUnavailable)
    ));
    writer.close().await;
}

#[tokio::test]
async fn postgres_notification_reconnect_requires_a_resnapshot_after_the_gap() {
    use std::time::Duration;
    coral_test_support::database_only!(Postgres);
    let fixture = TestDatabase::new(&MIGRATOR).await.unwrap();
    let database = fixture.database();
    let mut observer = database.subscribe_changes().unwrap();
    database.interrupt_change_notifications().await.unwrap();
    let lost = tokio::time::timeout(Duration::from_secs(2), observer.changed())
        .await
        .unwrap();
    assert!(matches!(lost, Err(coral_db::Error::ObservationUnavailable)));
    let mut tx = database.begin().await.unwrap();
    tx.execute_change(insert_record("notification-gap", "resnapshot"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), observer.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record(database.as_ref(), "notification-gap")
            .await
            .unwrap()
            .value,
        "resnapshot"
    );
}

async fn assert_domain_repository_contract(database: &Database) {
    database
        .migrate(&MIGRATOR)
        .await
        .expect("validate database migrations");

    let mut committed = database.begin().await.expect("begin committed transaction");
    assert_eq!(committed.system(), database.system());
    committed
        .execute(insert_record("committed", "visible"))
        .await
        .expect("insert committed record");
    committed.commit().await.expect("commit transaction");

    let mut rolled_back = database
        .begin()
        .await
        .expect("begin rolled-back transaction");
    rolled_back
        .execute(insert_record("rolled-back", "hidden"))
        .await
        .expect("insert rolled-back record");
    rolled_back.rollback().await.expect("roll back transaction");

    // A transaction dropped without commit, as an early error return does,
    // rolls back and releases the writer for the next transaction.
    let mut dropped = database.begin().await.expect("begin dropped transaction");
    dropped
        .execute(insert_record("dropped", "hidden"))
        .await
        .expect("insert dropped record");
    drop(dropped);
    write_record(database, "after-drop", "visible")
        .await
        .expect("write after a dropped transaction");
    assert_eq!(record(database, "dropped").await, None);

    assert_eq!(
        record(database, "committed").await,
        Some(Record {
            id: "committed".to_string(),
            value: "visible".to_string(),
        })
    );
    assert_eq!(record(database, "rolled-back").await, None);

    let mut transaction = database
        .begin_read_snapshot()
        .await
        .expect("begin query-executor transaction");
    assert_eq!(
        record(&mut transaction, "committed").await,
        Some(Record {
            id: "committed".to_string(),
            value: "visible".to_string(),
        })
    );
    transaction
        .rollback()
        .await
        .expect("roll back query-executor transaction");
}

async fn write_record(database: &Database, id: &str, value: &str) -> Result<(), coral_db::Error> {
    let mut transaction = database.begin().await?;
    transaction.execute(insert_record(id, value)).await?;
    transaction.commit().await
}

fn insert_record(id: &str, value: &str) -> sea_query::InsertStatement {
    Query::insert()
        .into_table(Records::Table)
        .columns([Records::Id, Records::Value])
        .values_panic([Expr::val(id.to_string()), Expr::val(value.to_string())])
        .to_owned()
}

fn select_record(id: &str) -> sea_query::SelectStatement {
    Query::select()
        .columns([Records::Id, Records::Value])
        .from(Records::Table)
        .and_where(Expr::col(Records::Id).eq(id))
        .to_owned()
}

async fn record(executor: impl QueryExecutor, id: &str) -> Option<Record> {
    executor
        .fetch_optional(select_record(id))
        .await
        .expect("read record")
}

/// Each transaction records how long it was open by how it ended: a commit,
/// an explicit rollback, or a drop, which rolls back. As it begins, its own
/// connection counts as in use.
#[tokio::test]
async fn transactions_record_their_duration_by_outcome_and_return_their_connection() {
    let fixture = TestDatabase::new(&MIGRATOR).await.unwrap();
    let database = fixture.database();
    let metrics = coral_telemetry::testing::MetricCapture::default();
    let _measured = tracing::subscriber::set_default(metrics.subscriber());
    let open = database.begin().await.unwrap();
    let pool = metrics.named("coral.persistence.pool.in_use");
    let [in_use] = pool.as_slice() else {
        panic!("one pool gauge: {pool:?}");
    };
    assert!(
        in_use.value >= 1.0,
        "the open transaction's connection is in use"
    );
    open.commit().await.unwrap();
    database.begin().await.unwrap().rollback().await.unwrap();
    drop(database.begin_read_snapshot().await.unwrap());

    let system = match database.system() {
        DatabaseSystem::Sqlite => "sqlite",
        DatabaseSystem::Postgres => "postgresql",
    };
    let mut durations = metrics
        .named("coral.persistence.transaction.duration")
        .into_iter()
        .map(|point| (point.attributes, point.count))
        .collect::<Vec<_>>();
    durations.sort();
    let expected = |outcome: &str, count| {
        (
            [("db.system", system), ("outcome", outcome)]
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .into(),
            count,
        )
    };
    assert_eq!(
        durations,
        [expected("committed", 1), expected("rolled_back", 2)]
    );
}
