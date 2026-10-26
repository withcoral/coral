//! Built-in Provider catalog acceptance at the public Module seam.
#![allow(
    unused_crate_dependencies,
    reason = "integration targets inherit the library's direct dependency set"
)]

use coral_catalog::{
    BuiltinCatalog, BuiltinOperation, Error, ExecutionBinding, OperationAvailability,
    OperationHints, OperationKey,
};
use coral_test_support::TestDatabase;
use sea_query::{Alias, Expr, ExprTrait as _, Query};
use sha2::{Digest as _, Sha256};
use std::fmt::Write as _;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table checks each Built-in Operation's retained contract"
)]
async fn each_published_builtin_generation_retains_the_coral_defined_catalog() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    let catalog = BuiltinCatalog::running().unwrap();
    let mut transaction = database.begin().await.unwrap();
    let first =
        coral_catalog::publish_builtin_generation_in_transaction(&mut transaction, &catalog, 1)
            .await
            .unwrap();
    let second =
        coral_catalog::publish_builtin_generation_in_transaction(&mut transaction, &catalog, 2)
            .await
            .unwrap();
    transaction.commit().await.unwrap();
    assert_ne!(first, second, "each Workspace gets its own Generation");

    let mut transaction = database.begin_read_snapshot().await.unwrap();
    coral_catalog::verify_generation_in_transaction(&mut transaction, &first)
        .await
        .unwrap();
    let operations = coral_catalog::operations_in_transaction(&mut transaction, &first)
        .await
        .unwrap();
    let targets = coral_catalog::available_search_targets_in_transaction(&mut transaction, &first)
        .await
        .unwrap();
    // Each declared contract: hints, so the default Operation Access State
    // follows, and the declaration a Program reads.
    for (operation_key, hints, declared) in [
        (
            BuiltinOperation::AddProvider,
            // No read-only hint, so the default Operation Access State is approval_required.
            OperationHints::default(),
            &[
                "type ProviderSourceInput =",
                "\"openapi_document\"",
                "\"openapi_url\"",
                "\"mcp_url\"",
                "export default function (input: {",
                r#""credentialRoute": "none" | "fixed" | "caller""#,
                r#""displayName"?: string"#,
                r#""namespace": string"#,
                r#""source": ProviderSourceInput"#,
                r#"{ "accountRequired": boolean; "authentication": "none" | "bearer" | "header_credentials" | "basic_auth" | "query_credentials" | "oauth_device_code" | "oauth_authorization_code"; "credentialRoute": "none" | "fixed" | "caller"; "displayName": string; "namespace": string; "operations": number; "status": "published" }"#,
                r#"{ "namespace": string; "status": "needs_sign_in"; "url": string }"#,
                "metadata: { kind: \"builtin\" }",
            ][..],
        ),
        (
            BuiltinOperation::ListProviders,
            // Read-only, so the default Operation Access State is enabled.
            OperationHints::new(Some(true), None, Some(true), None),
            &[
                "export default function (input: Record<string, never>)",
                r#"{ "providers": Array<Provider>; "setups": Array<ProviderSetup> }"#,
                // What a person does next on a Setup a Program asked for.
                r#""state": "awaiting_configuration" | "awaiting_sign_in" | "awaiting_discovery" | "ready""#,
                // Every Provider, Coral's own too, has a Call Root.
                r#""callRoot": string"#,
                r#""state": "active" | "disabled""#,
                r#""accountRequired": boolean"#,
                "metadata: { kind: \"builtin\" }",
            ][..],
        ),
        (
            BuiltinOperation::GetProvider,
            // Read-only, so the default Operation Access State is enabled.
            OperationHints::new(Some(true), None, Some(true), None),
            &[
                r#"export default function (input: { "namespace": string })"#,
                r#""source": PublishedProviderSource"#,
                r#""authentication": ConfiguredAuthentication"#,
                r#""requestHeaders": ({} & Record<string, string>)"#,
                r#""state": "active" | "disabled""#,
                "metadata: { kind: \"builtin\" }",
            ][..],
        ),
        (
            BuiltinOperation::GetPolicy,
            // Read-only, so the default Operation Access State is enabled.
            OperationHints::new(Some(true), None, Some(true), None),
            &[
                r#"export default function (input: { "namespace": string })"#,
                r#""rules": ({} & Record<string, "enabled" | "approval_required" | "disabled">)"#,
                "metadata: { kind: \"builtin\" }",
            ][..],
        ),
    ] {
        let key = operation_key.key();
        let operation = operations
            .iter()
            .find(|operation| operation.key().as_str() == key)
            .unwrap_or_else(|| panic!("the catalog defines {key}"));
        assert_eq!(operation.relative_path(), key);
        assert_eq!(operation.availability(), OperationAvailability::Available);
        assert_eq!(operation.hints(), hints, "{key}");
        let ExecutionBinding::Builtin(binding) = operation.execution_binding() else {
            panic!("a built-in Operation has a built-in Execution Binding");
        };
        assert_eq!(binding.operation(), Some(operation_key));
        let declaration = coral_catalog::authoring_declaration_in_transaction(
            &mut transaction,
            &first,
            operation,
        )
        .await
        .unwrap();
        for expected in declared {
            assert!(
                declaration.contains(expected),
                "{key} declaration lacks {expected}: {declaration}"
            );
        }
    }
    // Every built-in Operation is a search target, and nothing else is.
    let mut target_keys = targets
        .iter()
        .map(|target| target.key().as_str())
        .collect::<Vec<_>>();
    target_keys.sort_unstable();
    assert_eq!(
        target_keys,
        ["addProvider", "getPolicy", "getProvider", "listProviders"]
    );

    let graph = coral_catalog::schema_graph_in_transaction(&mut transaction, &first)
        .await
        .unwrap();
    // Search reads the same Operations as the search targets, and the same
    // fields as a full Operation gives.
    let searchable = coral_catalog::searchable_operations_in_transaction(&mut transaction, &first)
        .await
        .unwrap();
    assert_eq!(
        searchable
            .iter()
            .map(|operation| operation.key().as_str())
            .collect::<Vec<_>>(),
        target_keys
    );
    let fields = coral_catalog::StoredDeclarationFields::new(graph);
    let key = OperationKey::parse("addProvider").unwrap();
    let add_provider = searchable
        .iter()
        .find(|operation| operation.key() == &key)
        .expect("addProvider is searchable");
    let full = operations
        .iter()
        .find(|operation| operation.key() == &key)
        .unwrap();
    assert_eq!(add_provider.relative_path(), full.relative_path());
    assert_eq!(add_provider.presentation(), full.presentation());
    let prepared = fields.searchable_operation(add_provider).unwrap();
    assert_eq!(prepared, fields.operation(full).unwrap());
    let paths = prepared
        .entries()
        .iter()
        .map(|entry| entry.path().as_str())
        .collect::<Vec<_>>();
    for path in [
        "input.namespace",
        "input.credentialRoute",
        "input.displayName",
        "input.servers",
        "input.authentication",
        "input.requestHeaders",
    ] {
        assert!(paths.contains(&path), "{path} in {paths:?}");
    }
    assert!(paths.contains(&"result.data.url"), "{paths:?}");
    assert!(paths.contains(&"result.data.operations"), "{paths:?}");

    assert_eq!(
        coral_catalog::credential_contract_in_transaction(&mut transaction, &first)
            .await
            .unwrap(),
        coral_catalog::ProviderCredentialContract::NONE
    );
    assert!(matches!(
        coral_catalog::setup_input_kind_in_transaction(&mut transaction, &first).await,
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(
        coral_catalog::setup_input_in_transaction(&mut transaction, &first).await,
        Err(Error::InvalidInput(_))
    ));
    transaction.commit().await.unwrap();
}

#[tokio::test]
async fn a_builtin_generation_pins_its_catalog_identity_and_decodes_any_well_formed_one() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    let catalog = BuiltinCatalog::running().unwrap();
    let mut transaction = database.begin().await.unwrap();
    let first =
        coral_catalog::publish_builtin_generation_in_transaction(&mut transaction, &catalog, 1)
            .await
            .unwrap();
    let second =
        coral_catalog::publish_builtin_generation_in_transaction(&mut transaction, &catalog, 2)
            .await
            .unwrap();
    transaction.commit().await.unwrap();

    let mut transaction = database.begin_read_snapshot().await.unwrap();
    let pinned = pinned_input(&mut transaction, &first).await;
    assert_eq!(
        pinned_input(&mut transaction, &second).await,
        pinned,
        "one catalog pins one identity"
    );
    assert_eq!(pinned, catalog.identity().as_str());
    transaction.rollback().await.unwrap();
    let digest = pinned
        .strip_prefix("coral-admin-catalog:")
        .unwrap_or_else(|| panic!("{pinned} names no catalog identity"));
    assert!(
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')),
        "{digest} is not lowercase SHA-256"
    );

    // Another Coral version pins its own catalog's identity, and this one still reads it.
    let digest = "0123456789abcdef".repeat(4);
    for (pinned, decodes) in [
        (format!("coral-admin-catalog:{digest}"), true),
        ("coral-admin-catalog-v1".to_string(), false),
        ("coral-admin-catalog:".to_string(), false),
        (
            format!("coral-admin-catalog:{}", "0123456789abcdef".repeat(2)),
            false,
        ),
        (
            format!("coral-admin-catalog:{}", digest.to_uppercase()),
            false,
        ),
        (format!("coral-admin-catalog:{digest}0"), false),
        (digest.clone(), false),
    ] {
        fixture
            .execute_fixture_sql(&format!(
                "UPDATE provider_generations SET pinned_input = '{pinned}', input_digest = '{}' \
                 WHERE id = '{first}'",
                sha256_hex(&pinned)
            ))
            .await
            .unwrap();
        let mut transaction = database.begin_read_snapshot().await.unwrap();
        let outcome =
            coral_catalog::verify_generation_in_transaction(&mut transaction, &first).await;
        transaction.rollback().await.unwrap();
        if decodes {
            assert!(outcome.is_ok(), "{pinned}: {outcome:?}");
        } else {
            assert!(
                matches!(outcome, Err(Error::CorruptData(_))),
                "{pinned}: {outcome:?}"
            );
        }
    }
}

// Catches: a Workspace whose Generation already holds the served catalog
// taking a new Generation at every boot, one that holds another catalog
// keeping it, or a corrupt Generation passing as current.
#[tokio::test]
async fn a_builtin_generation_pins_its_own_catalog_and_no_other() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    let running = BuiltinCatalog::running().unwrap();
    let other =
        BuiltinCatalog::with_unknown_operations([("archiveThings", OperationHints::default())])
            .unwrap();
    let mut transaction = database.begin().await.unwrap();
    let generation =
        coral_catalog::publish_builtin_generation_in_transaction(&mut transaction, &running, 1)
            .await
            .unwrap();
    transaction.commit().await.unwrap();

    let mut transaction = database.begin_read_snapshot().await.unwrap();
    for (catalog, pins) in [(&running, true), (&other, false)] {
        assert_eq!(
            coral_catalog::builtin_generation_pins_in_transaction(
                &mut transaction,
                catalog,
                &generation,
            )
            .await
            .unwrap(),
            pins,
            "{}",
            catalog.identity()
        );
    }
    transaction.rollback().await.unwrap();

    // A pinned identity that disagrees with the Generation's digest is corrupt.
    fixture
        .execute_fixture_sql(&format!(
            "UPDATE provider_generations SET input_digest = '{}' WHERE id = '{generation}'",
            sha256_hex("another catalog")
        ))
        .await
        .unwrap();
    let mut transaction = database.begin_read_snapshot().await.unwrap();
    let corrupt = coral_catalog::builtin_generation_pins_in_transaction(
        &mut transaction,
        &running,
        &generation,
    )
    .await;
    transaction.rollback().await.unwrap();
    assert!(matches!(corrupt, Err(Error::CorruptData(_))), "{corrupt:?}");
}

async fn pinned_input(
    transaction: &mut coral_db::Transaction<'_>,
    generation: &coral_catalog::ProviderGenerationId,
) -> String {
    let (pinned,): (String,) = transaction
        .fetch_optional(
            Query::select()
                .column(Alias::new("pinned_input"))
                .from(Alias::new("provider_generations"))
                .and_where(Expr::col(Alias::new("id")).eq(generation.to_string()))
                .to_owned(),
        )
        .await
        .expect("read the pinned input")
        .expect("the Generation exists");
    pinned
}

fn sha256_hex(value: &str) -> String {
    Sha256::digest(value.as_bytes())
        .iter()
        .fold(String::new(), |mut hex, byte| {
            write!(hex, "{byte:02x}").expect("writing to a String succeeds");
            hex
        })
}

#[tokio::test]
async fn a_builtin_binding_disagreeing_with_its_index_fails_full_and_search_reads() {
    for (case, corruption) in [
        (
            "Operation Key",
            "SET operation_key = 'renamedProvider' WHERE operation_key = 'addProvider'",
        ),
        (
            "input schema",
            "SET input_schema_id = (SELECT input_schema_id FROM materialized_operations \
             WHERE operation_key = 'listProviders') WHERE operation_key = 'addProvider'",
        ),
    ] {
        let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let database = fixture.database();
        let catalog = BuiltinCatalog::running().unwrap();
        let mut transaction = database.begin().await.unwrap();
        let generation =
            coral_catalog::publish_builtin_generation_in_transaction(&mut transaction, &catalog, 1)
                .await
                .unwrap();
        transaction.commit().await.unwrap();
        fixture
            .execute_fixture_sql(&format!(
                "UPDATE materialized_operations {corruption} \
                 AND provider_generation_id = '{generation}'"
            ))
            .await
            .unwrap();

        let mut transaction = database.begin_read_snapshot().await.unwrap();
        let full = coral_catalog::operations_in_transaction(&mut transaction, &generation).await;
        let searchable =
            coral_catalog::searchable_operations_in_transaction(&mut transaction, &generation)
                .await;
        transaction.rollback().await.unwrap();
        assert!(
            matches!(full, Err(Error::CorruptData(_))),
            "{case}: {full:?}"
        );
        assert!(
            matches!(searchable, Err(Error::CorruptData(_))),
            "{case}: {searchable:?}"
        );
    }
}
