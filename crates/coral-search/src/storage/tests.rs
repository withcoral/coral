//! Boot checks against probe results a live test database cannot produce.

use super::{
    FeatureProbe, PostgresFeatures, SqliteFeatures, check_postgres_features, check_sqlite_features,
};

fn missing(cause: &str) -> FeatureProbe {
    FeatureProbe::Missing {
        cause: cause.to_string(),
    }
}

#[test]
fn a_missing_feature_fails_boot_naming_the_feature_and_its_cause() {
    let postgres = |pg_trgm| {
        check_postgres_features(PostgresFeatures {
            server_version: "17.10".to_string(),
            pg_trgm,
        })
    };
    let sqlite = |fts5, trigram_tokenizer| {
        check_sqlite_features(SqliteFeatures {
            version: "3.33.0".to_string(),
            fts5,
            trigram_tokenizer,
        })
    };

    for (case, result, expected) in [
        (
            "Postgres with pg_trgm",
            postgres(FeatureProbe::Present),
            None,
        ),
        (
            "Postgres without pg_trgm",
            postgres(missing("permission denied to create extension \"pg_trgm\"")),
            Some(
                "Postgres 17.10 does not provide required search feature: pg_trgm extension \
                 (permission denied to create extension \"pg_trgm\"); install pg_trgm in a \
                 schema on Coral's search path, or let Coral's database role create it",
            ),
        ),
        (
            "SQLite with FTS5 and the trigram tokenizer",
            sqlite(FeatureProbe::Present, FeatureProbe::Present),
            None,
        ),
        (
            "SQLite without FTS5",
            sqlite(
                missing("no such module: fts5"),
                missing("no such module: fts5"),
            ),
            Some(
                "SQLite 3.33.0 does not provide required search feature: FTS5 (no such module: \
                 fts5); use a Coral build whose SQLite includes FTS5 with the trigram tokenizer",
            ),
        ),
        (
            "SQLite without the trigram tokenizer",
            sqlite(FeatureProbe::Present, missing("no such tokenizer: trigram")),
            Some(
                "SQLite 3.33.0 does not provide required search feature: FTS5 trigram tokenizer \
                 (no such tokenizer: trigram); use a Coral build whose SQLite includes FTS5 with \
                 the trigram tokenizer",
            ),
        ),
    ] {
        assert_eq!(
            result.err().map(|error| error.to_string()),
            expected.map(str::to_string),
            "{case}"
        );
    }
}
