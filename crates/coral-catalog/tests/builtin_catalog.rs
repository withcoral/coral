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
    reason = "one Generation's read-back checks every facet Coral stores for the catalog"
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
    let [operation] = operations.as_slice() else {
        panic!("the built-in catalog has exactly one Operation");
    };
    assert_eq!(operation.key().as_str(), "createProvider");
    assert_eq!(operation.relative_path(), "createProvider");
    assert_eq!(operation.availability(), OperationAvailability::Available);
    // No read-only hint, so the default Operation Access State is approval_required.
    assert_eq!(operation.hints(), OperationHints::default());
    let ExecutionBinding::Builtin(binding) = operation.execution_binding() else {
        panic!("a built-in Operation has a built-in Execution Binding");
    };
    assert_eq!(binding.operation(), Some(BuiltinOperation::CreateProvider));

    let declaration =
        coral_catalog::authoring_declaration_in_transaction(&mut transaction, &first, operation)
            .await
            .unwrap();
    for expected in [
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
    ] {
        assert!(
            declaration.contains(expected),
            "declaration lacks {expected}: {declaration}"
        );
    }

    let targets = coral_catalog::available_search_targets_in_transaction(&mut transaction, &first)
        .await
        .unwrap();
    assert_eq!(
        targets
            .iter()
            .map(|target| target.key().as_str())
            .collect::<Vec<_>>(),
        ["createProvider"]
    );
    let graph = coral_catalog::schema_graph_in_transaction(&mut transaction, &first)
        .await
        .unwrap();
    let fields = coral_catalog::StoredDeclarationFields::new(graph)
        .all(&operations)
        .unwrap();
    let key = OperationKey::parse("createProvider").unwrap();
    let paths = fields
        .get(&key)
        .expect("createProvider declaration fields")
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
