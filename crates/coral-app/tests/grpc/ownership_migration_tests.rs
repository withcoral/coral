//! What the one-time local-ownership upgrade does to an install, and what it
//! must not do, across every deployment-mode change the product specifies.
//!
//! Every scenario here starts a real server over a state directory and then
//! starts another one over the same directory, because a deployment's mode is
//! settled when its server starts. State is read back from the deployment's own
//! database rather than through a listing: under the single-user policy the
//! local principal is answered about every workspace whether or not a
//! membership row exists, so only the rows themselves show what the upgrade
//! wrote.

use std::path::Path;

use coral_api::v1::{ListSourcesRequest, WorkspaceRole};
use coral_client::AppClient;
use tonic::{Code, Request};

use crate::harness::{
    Admission, Install, concealed_refusal, create_workspace, execute_sql, membership_rows,
    named_workspace,
};

/// The user id the upgrade writes its ownership under.
const LOCAL_OWNER: &str = "coral:local";

/// A valid workspace name no test ever creates, so a refusal naming it can only
/// come from the workspace rule rather than from a row.
const MISSING_WORKSPACE: &str = "never_existed";

/// A legacy install whose `analytics` workspace already has a source installed
/// in it, so an upgrade that dropped a workspace's contents would show up here.
const LEGACY_CONFIG_WITH_DATA: &str = r#"
version = 1

[workspaces.analytics]

[workspaces.analytics.sources.legacy_messages]
origin = "imported"
version = "0.1.0"

[workspaces.reports]
"#;

/// Writes the `config.toml` a pre-membership Coral left behind.
///
/// Its workspaces exist only as config tables. The legacy cutover is what turns
/// them into directory rows, and it leaves them with no memberships at all,
/// which is precisely the state the upgrade exists to repair.
fn write_legacy_config(config_dir: &Path, config: &str) {
    std::fs::create_dir_all(config_dir).expect("create config dir");
    std::fs::write(config_dir.join("config.toml"), config).expect("write legacy config");
}

fn legacy_config_naming(workspaces: &[&str]) -> String {
    let mut config = String::from("version = 1\n");
    for workspace in workspaces {
        config.push_str("\n[workspaces.");
        config.push_str(workspace);
        config.push_str("]\n");
    }
    config
}

/// The membership rows an upgrade that adopted `workspaces` leaves behind.
fn adopted_by_local(workspaces: &[&str]) -> Vec<(String, String, String)> {
    workspaces
        .iter()
        .map(|workspace| {
            (
                (*workspace).to_string(),
                LOCAL_OWNER.to_string(),
                "owner".to_string(),
            )
        })
        .collect()
}

/// What a caller is told when they name a workspace, with the name they
/// supplied themselves factored out. Two names that agree here are
/// indistinguishable to that caller.
async fn refusal(client: &AppClient, workspace: &str) -> (Code, String, Vec<String>) {
    let status = execute_sql(client, workspace, "select 1")
        .await
        .expect_err("a caller with no membership must be refused");
    concealed_refusal(&status, workspace)
}

async fn source_names(client: &AppClient, workspace: &str) -> Vec<String> {
    client
        .source_client()
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(named_workspace(workspace)),
        }))
        .await
        .expect("a workspace's owner may list its sources")
        .into_inner()
        .sources
        .into_iter()
        .map(|source| source.name)
        .collect()
}

/// Reads the caller's own listing in a stable order.
async fn listed_memberships(client: &AppClient) -> Vec<(String, WorkspaceRole)> {
    let mut rows = membership_rows(client).await;
    rows.sort();
    rows
}

/// The upgrade an existing single-user install gets on its first start: every
/// workspace keeps its name and its contents, and each ownerless one gains
/// exactly one owner.
#[tokio::test]
async fn a_single_user_upgrade_adopts_every_legacy_workspace_once_and_keeps_its_name_and_data() {
    let install = Install::new();
    write_legacy_config(install.config_dir(), LEGACY_CONFIG_WITH_DATA);

    let deployment = install
        .start(Admission::LocalPrincipal)
        .await
        .expect("a legacy install must come up in single-user mode");

    assert_eq!(
        deployment.workspace_names().await,
        vec!["analytics".to_string(), "reports".to_string()],
        "the upgrade must not rename, drop, or invent a workspace"
    );
    assert_eq!(
        deployment.memberships().await,
        adopted_by_local(&["analytics", "reports"]),
        "every ownerless workspace is adopted, and each of them exactly once"
    );
    assert!(deployment.local_ownership_migration_claimed().await);
    assert_eq!(
        source_names(&deployment.as_host().await, "analytics").await,
        vec!["legacy_messages".to_string()],
        "a workspace must come out of the upgrade with the contents it went in with"
    );
}

/// An operator who enables login without first starting the new version in
/// single-user mode gets no upgrade at all: the legacy workspaces stay in
/// place, unreachable and undisclosed, and the marker stays unclaimed so a
/// later single-user start can still run it.
#[tokio::test]
async fn a_direct_shared_upgrade_conceals_the_legacy_workspaces_and_leaves_the_upgrade_unclaimed() {
    let install = Install::new();
    write_legacy_config(install.config_dir(), &legacy_config_naming(&["legacy"]));

    let deployment = install
        .start(Admission::Tokens)
        .await
        .expect("an ownerless workspace must not refuse a shared server its startup");
    let alice = deployment.seed_user("alice", "Alice").await;
    let alice_client = deployment.as_person(&alice).await;

    assert_eq!(
        deployment.workspace_names().await,
        vec!["legacy".to_string()]
    );
    assert!(
        deployment.memberships().await.is_empty(),
        "a shared deployment appoints nobody"
    );
    assert!(
        !deployment.local_ownership_migration_claimed().await,
        "claiming the marker here would retire the upgrade for a later single-user start"
    );
    assert_eq!(listed_memberships(&alice_client).await, Vec::new());
    assert_eq!(
        refusal(&alice_client, "legacy").await,
        refusal(&alice_client, MISSING_WORKSPACE).await,
        "an ownerless workspace must be indistinguishable from one that never existed"
    );
}

/// Configuring login over an upgraded install keeps the built-in local user's
/// ownership on record without handing any of it to an authenticated user. The
/// workspace the local user created after the upgrade is owned from the moment
/// it exists, so it survives the switch on the same footing as the adopted one.
#[tokio::test]
async fn login_over_an_upgraded_install_keeps_the_local_owner_and_grants_nobody_else() {
    let install = Install::new();
    write_legacy_config(install.config_dir(), &legacy_config_naming(&["notebook"]));

    let single_user = install
        .start(Admission::LocalPrincipal)
        .await
        .expect("start the upgraded single-user deployment");
    create_workspace(&single_user.as_host().await, "journal")
        .await
        .expect("the host creates a workspace of its own");

    assert_eq!(
        single_user.memberships().await,
        adopted_by_local(&["journal", "notebook"]),
        "a workspace created after the upgrade is owned at creation rather than ownerless"
    );

    let install = single_user.shutdown().await;
    let shared = install
        .start(Admission::Tokens)
        .await
        .expect("configure login over the same state");
    let bob = shared.seed_user("bob", "Bob").await;
    let bob_client = shared.as_person(&bob).await;

    assert_eq!(
        shared.memberships().await,
        adopted_by_local(&["journal", "notebook"]),
        "the local user's ownership remains in the database"
    );
    assert_eq!(
        listed_memberships(&bob_client).await,
        Vec::new(),
        "an authenticated user inherits none of it"
    );
    for workspace in ["journal", "notebook"] {
        assert_eq!(
            refusal(&bob_client, workspace).await,
            refusal(&bob_client, MISSING_WORKSPACE).await,
            "{workspace} was disclosed to a user who was never given it"
        );
    }

    let install = shared.shutdown().await;
    let single_user = install
        .start(Admission::LocalPrincipal)
        .await
        .expect("disable login again");

    assert_eq!(
        listed_memberships(&single_user.as_host().await).await,
        vec![
            ("journal".to_string(), WorkspaceRole::Owner),
            ("notebook".to_string(), WorkspaceRole::Owner),
        ],
        "both workspaces are still the local user's once it is admitted again"
    );
}

/// Disabling login on a shared deployment runs the upgrade on the next start.
/// It adopts the workspaces that are still ownerless and leaves every workspace
/// an authenticated user already owns exactly as it was.
#[tokio::test]
async fn a_shared_deployment_switched_to_single_user_adopts_only_the_still_ownerless_workspaces() {
    let install = Install::new();
    write_legacy_config(install.config_dir(), &legacy_config_naming(&["orphan"]));

    let shared = install
        .start(Admission::Tokens)
        .await
        .expect("start shared over a legacy install");
    let carol = shared.seed_user("carol", "Carol").await;
    create_workspace(&shared.as_person(&carol).await, "carol_space")
        .await
        .expect("an authenticated user creates their own workspace");

    let install = shared.shutdown().await;
    let single_user = install
        .start(Admission::LocalPrincipal)
        .await
        .expect("disable login on the next start");

    assert_eq!(
        single_user.memberships().await,
        vec![
            (
                "carol_space".to_string(),
                carol.clone(),
                "owner".to_string()
            ),
            (
                "orphan".to_string(),
                LOCAL_OWNER.to_string(),
                "owner".to_string()
            ),
        ],
        "the upgrade takes only what nobody owned"
    );
    assert!(single_user.local_ownership_migration_claimed().await);
}

/// The two situations startup has to tell an operator about stay distinct in
/// the state it reads them from: `adopted` is owned only by the built-in local
/// user, `orphan` by nobody at all. They need different repairs — transfer
/// ownership off the local user versus appoint one — while being equally
/// unreachable to every authenticated caller.
#[tokio::test]
async fn a_shared_start_keeps_the_two_inaccessible_categories_distinct() {
    let install = Install::new();
    write_legacy_config(install.config_dir(), &legacy_config_naming(&["adopted"]));

    let single_user = install
        .start(Admission::LocalPrincipal)
        .await
        .expect("start the upgraded single-user deployment");
    // This one never went through the upgrade, which is the only way a
    // workspace still has no owner at all once the marker is claimed.
    single_user.seed_ownerless_workspace("orphan").await;

    let install = single_user.shutdown().await;
    let shared = install
        .start(Admission::Tokens)
        .await
        .expect("a workspace nobody can reach must not refuse the server its startup");
    let dana = shared.seed_user("dana", "Dana").await;
    let dana_client = shared.as_person(&dana).await;

    assert_eq!(
        shared.workspace_names().await,
        vec!["adopted".to_string(), "orphan".to_string()],
        "both categories keep serving nobody rather than being removed"
    );
    assert_eq!(
        shared.memberships().await,
        adopted_by_local(&["adopted"]),
        "`adopted` has the local user as its only owner and `orphan` has none"
    );
    for workspace in ["adopted", "orphan"] {
        assert_eq!(
            refusal(&dana_client, workspace).await,
            refusal(&dana_client, MISSING_WORKSPACE).await,
            "{workspace} was disclosed to a user who cannot reach it"
        );
    }
}

/// A row squatting on the local user's unique empty subject swallows the
/// upgrade's own insert of the local user, so it fails after it has already
/// claimed its marker. The claim must not survive: startup fails outright, the
/// state is left exactly as it was, and the start after an operator repairs the
/// directory runs the whole upgrade.
#[tokio::test]
async fn a_rolled_back_upgrade_leaves_no_claim_and_a_later_start_retries_it() {
    let install = Install::new();
    write_legacy_config(
        install.config_dir(),
        &legacy_config_naming(&["first", "second"]),
    );

    let shared = install
        .start(Admission::Tokens)
        .await
        .expect("start shared over a legacy install");
    // A verified identity always carries a non-empty `sub`, so only a corrupted
    // directory can hold the local user's empty subject.
    let squatter = shared
        .seed_user_with_subject("squatter", "", "Squatter")
        .await;
    let install = shared.shutdown().await;

    let Err(error) = install.start(Admission::LocalPrincipal).await else {
        panic!("the upgrade must fail on the local user row it could not write")
    };

    assert!(
        error.contains("the local user row is absent"),
        "unexpected startup failure: {error}"
    );

    let shared = install
        .start(Admission::Tokens)
        .await
        .expect("a shared start never runs the upgrade");

    assert!(
        !shared.local_ownership_migration_claimed().await,
        "a claim that outlived its rolled-back transaction would retire the upgrade forever"
    );
    assert!(
        shared.memberships().await.is_empty(),
        "a rolled-back upgrade must leave no ownership behind either"
    );

    shared.remove_user(&squatter).await;
    let install = shared.shutdown().await;
    let retried = install
        .start(Admission::LocalPrincipal)
        .await
        .expect("the repaired install upgrades");

    assert_eq!(
        retried.memberships().await,
        adopted_by_local(&["first", "second"])
    );
    assert!(retried.local_ownership_migration_claimed().await);
}
