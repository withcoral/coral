use coral_db::UnixNanos;
use coral_test_support::TestDatabase;
use coral_workspace::{OrganizationModule, OrganizationRole};
use sea_query::{Alias, Expr, ExprTrait as _, Query};

use super::{LinkOutcome, UserIdentifier, UserIdentifierKind};
use crate::{LoginProvisioning, UserDirectory, UserId, VerifiedLogin};

const NOW: UnixNanos = UnixNanos::new(1_800_000_000_000_000_000);
const ISSUER: &str = "https://issuer.test";
/// The version of the first of the store's two steps. It makes
/// `users.issuer` and `users.subject` optional, and the next step adds the
/// table and copies each Subject.
const FIRST_STORE_MIGRATION_VERSION: i64 = 88;

const fn subject(value: &str) -> UserIdentifier<'_> {
    UserIdentifier::new(UserIdentifierKind::Subject, value)
}

const fn name_id(value: &str) -> UserIdentifier<'_> {
    UserIdentifier::new(UserIdentifierKind::NameId, value)
}

/// The ID of a User that holds no identifier.
fn new_user_id() -> UserId {
    UserId::parse(&uuid::Uuid::new_v4().to_string()).unwrap()
}

async fn link(
    fixture: &TestDatabase,
    identifier: UserIdentifier<'_>,
    user_id: &UserId,
) -> LinkOutcome {
    let database = fixture.database();
    let mut transaction = database.begin().await.unwrap();
    let outcome = super::link(&mut transaction, identifier, user_id, NOW)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    outcome
}

async fn holder(fixture: &TestDatabase, identifier: UserIdentifier<'_>) -> Option<UserId> {
    super::find(&*fixture.database(), identifier).await.unwrap()
}

/// Asserts the holder of each identifier, or that it has none.
async fn assert_holders(
    fixture: &TestDatabase,
    expected: &[(UserIdentifier<'_>, Option<&UserId>)],
) {
    for (identifier, expected) in expected {
        assert_eq!(
            holder(fixture, *identifier).await.as_ref(),
            *expected,
            "{identifier:?}"
        );
    }
}

/// One identifier belongs to at most one User, and a User holds at most one
/// of each kind. A refused link changes nothing.
#[tokio::test]
async fn a_link_keeps_both_uniqueness_rules() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let ana = new_user_id();
    let ben = new_user_id();

    let cases = [
        (
            "a new identifier",
            name_id("ana-1"),
            &ana,
            LinkOutcome::Linked,
        ),
        (
            "the same link again",
            name_id("ana-1"),
            &ana,
            LinkOutcome::AlreadyLinked,
        ),
        (
            "an identifier that another User holds",
            name_id("ana-1"),
            &ben,
            LinkOutcome::HeldBy(ana.clone()),
        ),
        (
            "a second identifier of one kind",
            name_id("ana-2"),
            &ana,
            LinkOutcome::UserHoldsAnother,
        ),
        // Had the link that was refused for this User written a row, this
        // one would be refused too.
        (
            "the first identifier of a User whose link was refused",
            name_id("ben-1"),
            &ben,
            LinkOutcome::Linked,
        ),
        (
            "an identifier that another User holds, for a User who holds one of the kind",
            name_id("ana-1"),
            &ben,
            LinkOutcome::HeldBy(ana.clone()),
        ),
        (
            "the same value under another kind",
            subject("ana-1"),
            &ben,
            LinkOutcome::Linked,
        ),
    ];
    for (case, identifier, user_id, expected) in cases {
        assert_eq!(
            link(&fixture, identifier, user_id).await,
            expected,
            "{case}"
        );
    }

    // No refused link left a row: each identifier has the holder that a
    // `Linked` answer gave it, or none.
    assert_holders(
        &fixture,
        &[
            (name_id("ana-1"), Some(&ana)),
            (name_id("ana-2"), None),
            (name_id("ben-1"), Some(&ben)),
            (subject("ana-1"), Some(&ben)),
            (subject("ben-1"), None),
        ],
    )
    .await;
}

/// The lookup and the uniqueness rule compare a value by its bytes, on both
/// databases: a value that only looks like a held one is another identifier.
#[tokio::test]
async fn a_value_is_compared_by_its_bytes() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let held = "José";
    let first = new_user_id();
    assert_eq!(
        link(&fixture, name_id(held), &first).await,
        LinkOutcome::Linked
    );

    let lookalikes = [
        ("another case", "josé"),
        ("a trailing space", "José "),
        ("a leading space", " José"),
        ("a decomposed accent", "Jose\u{301}"),
    ];
    for (case, value) in lookalikes {
        assert_eq!(
            holder(&fixture, name_id(value)).await,
            None,
            "{case} finds nobody"
        );
        let other = new_user_id();
        assert_eq!(
            link(&fixture, name_id(value), &other).await,
            LinkOutcome::Linked,
            "{case} is its own identifier"
        );
        assert_eq!(
            holder(&fixture, name_id(value)).await,
            Some(other),
            "{case}"
        );
    }
    assert_eq!(holder(&fixture, name_id(held)).await, Some(first));
}

/// The two steps of the store keep each User and copy its Subject: a User
/// from before the upgrade is found through the store, and by the sign-in.
#[tokio::test]
async fn the_upgrade_keeps_each_user_and_copies_its_subject_into_the_store() {
    let before = coral_test_support::migration_plan_before(FIRST_STORE_MIGRATION_VERSION);
    let fixture = TestDatabase::new(&before).await.unwrap();
    let ana = insert_user_as_the_release_before(&fixture, "ana").await;
    let ben = insert_user_as_the_release_before(&fixture, "ben").await;

    fixture
        .database()
        .migrate(coral_db::migration_plan())
        .await
        .unwrap();

    assert_holders(
        &fixture,
        &[
            (subject("ana"), Some(&ana)),
            (subject("ben"), Some(&ben)),
            (subject("cleo"), None),
            (name_id("ana"), None),
        ],
    )
    .await;
    let directory = UserDirectory::new(fixture.database());
    assert_eq!(sign_in(&directory, "ana").await, ana);
}

/// A User that the release before this one made during the upgrade holds its
/// Subject in `users.subject` and no row in the store. Its sign-in finds it
/// there, and the store gains the row. A sign-in under another issuer is
/// refused and links nothing.
#[tokio::test]
async fn a_user_with_a_subject_in_its_column_and_no_row_is_found_and_linked() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let directory = UserDirectory::new(fixture.database());
    let dana = insert_user_as_the_release_before(&fixture, "dana").await;
    assert_holders(&fixture, &[(subject("dana"), None)]).await;

    let other_issuer = VerifiedLogin::oidc("https://other.test", "dana", None, NOW);
    assert_eq!(
        directory.provision_login(other_issuer).await.unwrap(),
        LoginProvisioning::IssuerMismatch
    );
    assert_holders(&fixture, &[(subject("dana"), None)]).await;

    assert_eq!(sign_in(&directory, "dana").await, dana);
    assert_holders(&fixture, &[(subject("dana"), Some(&dana))]).await;
    assert_eq!(sign_in(&directory, "dana").await, dana);
}

/// A sign-in by another kind than a Subject makes a User whose issuer and
/// Subject in `users` are empty. The store finds it again, and a Subject of
/// the same value names another person.
#[tokio::test]
async fn a_user_whose_two_columns_are_empty_is_found_through_the_store() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let directory = UserDirectory::new(fixture.database());
    let gil = provision(&directory, name_id_login("gil")).await;
    assert_eq!(columns(&fixture, &gil).await, (None, None));
    assert_eq!(provision(&directory, name_id_login("gil")).await, gil);

    let subject_gil = sign_in(&directory, "gil").await;
    assert_ne!(subject_gil, gil);
    assert_eq!(
        columns(&fixture, &subject_gil).await,
        (Some(ISSUER.to_owned()), Some("gil".to_owned()))
    );
}

/// Two first sign-ins of one person that arrive together make one User.
#[tokio::test]
async fn two_first_sign_ins_of_one_person_together_make_one_user() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let directory = UserDirectory::new(fixture.database());
    let (first, second) = tokio::join!(
        provision(&directory, name_id_login("ivy")),
        provision(&directory, name_id_login("ivy")),
    );
    assert_eq!(first, second);
    assert_holders(&fixture, &[(name_id("ivy"), Some(&first))]).await;
}

/// Two links of one identifier to two Users that arrive together: one is
/// linked, and the other reads the holder and changes nothing.
#[tokio::test]
async fn two_links_together_leave_one_holder() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let ana = new_user_id();
    let ben = new_user_id();
    let outcomes = tokio::join!(
        link(&fixture, name_id("joy"), &ana),
        link(&fixture, name_id("joy"), &ben),
    );
    let winner = match outcomes {
        (LinkOutcome::Linked, LinkOutcome::HeldBy(holder))
        | (LinkOutcome::HeldBy(holder), LinkOutcome::Linked) => holder,
        outcomes => panic!("expected one link and one holder, got {outcomes:?}"),
    };
    assert_holders(&fixture, &[(name_id("joy"), Some(&winner))]).await;
}

/// The first Owner is named by a kind and a value. An OIDC login is always
/// verified under the configured issuer, so for every OIDC login this gives
/// the answer that the match on the issuer and the Subject gave.
#[tokio::test]
async fn the_first_owner_is_the_login_with_the_configured_kind_and_value() {
    use OrganizationRole::{Member, Owner};
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let cases = [
        (
            "the configured Subject",
            subject("o1"),
            oidc_login("o1"),
            Owner,
        ),
        ("another Subject", subject("o2"), oidc_login("m2"), Member),
        (
            "the Subject in another case",
            subject("o3"),
            oidc_login("O3"),
            Member,
        ),
        (
            "the Subject's value as a NameID",
            subject("o4"),
            name_id_login("o4"),
            Member,
        ),
        (
            "the configured NameID",
            name_id("o5"),
            name_id_login("o5"),
            Owner,
        ),
        (
            "the NameID's value as a Subject",
            name_id("o6"),
            oidc_login("o6"),
            Member,
        ),
    ];
    let organization = OrganizationModule::new(fixture.database());
    for (case, owner, login, expected) in cases {
        let directory = UserDirectory::new(fixture.database()).with_bootstrap_owner(owner);
        let user = provision(&directory, login).await;
        let role = organization
            .resolve_user_authority(&user)
            .await
            .unwrap()
            .organization_role();
        assert_eq!(role, Some(expected), "{case}");
    }
}

/// Writes a `users` row as the release before this one does, and no row in
/// the store.
async fn insert_user_as_the_release_before(fixture: &TestDatabase, oidc_subject: &str) -> UserId {
    let user_id = new_user_id();
    fixture
        .execute_fixture_sql(&format!(
            "INSERT INTO users (user_id, issuer, subject, display_name, \
             created_at_unix_nanos, last_login_at_unix_nanos) \
             VALUES ('{}', '{ISSUER}', '{oidc_subject}', NULL, 1, 1)",
            user_id.as_str()
        ))
        .await
        .unwrap();
    user_id
}

/// The issuer and the Subject that the User's `users` row keeps.
async fn columns(fixture: &TestDatabase, user_id: &UserId) -> (Option<String>, Option<String>) {
    fixture
        .database()
        .fetch_optional(
            Query::select()
                .columns([Alias::new("issuer"), Alias::new("subject")])
                .from(Alias::new("users"))
                .and_where(Expr::col(Alias::new("user_id")).eq(user_id.as_str()))
                .to_owned(),
        )
        .await
        .unwrap()
        .expect("the User has a users row")
}

fn oidc_login(oidc_subject: &str) -> VerifiedLogin<'_> {
    VerifiedLogin::oidc(ISSUER, oidc_subject, None, NOW)
}

fn name_id_login(value: &str) -> VerifiedLogin<'_> {
    VerifiedLogin::new(name_id(value), None, NOW)
}

/// The User of an OIDC sign-in with `oidc_subject`.
async fn sign_in(directory: &UserDirectory, oidc_subject: &str) -> UserId {
    provision(directory, oidc_login(oidc_subject)).await
}

async fn provision(directory: &UserDirectory, login: VerifiedLogin<'_>) -> UserId {
    match directory.provision_login(login).await.unwrap() {
        LoginProvisioning::Provisioned(user_id) => user_id,
        LoginProvisioning::IssuerMismatch | LoginProvisioning::Disabled => {
            panic!("expected a provisioned login")
        }
    }
}
