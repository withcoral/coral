//! Public gRPC evidence that a workspace's functions and its search index are
//! read by everyone inside it and rewritten by nobody else.
//!
//! These two families sit on opposite sides of the same line. Searching a
//! workspace and listing or calling its functions are reads of what it holds,
//! so membership is the whole requirement. Installing a function or rebuilding,
//! draining, and clearing an index changes what every member then sees, so
//! those are an owner's act.
//!
//! The agent boundary is what makes the line worth proving over the wire. A
//! data-plane read carries the person's workspace access, so an MCP-audience
//! credential searches and lists functions exactly as the person does — while
//! the same credential behind the *owner* still installs nothing, because the
//! control-plane restriction is settled before any role is read. Both halves
//! are asserted here; a suite that only proved the denial would pass against a
//! server that had shut agents out of reading too.
//!
//! Every refusal is proved to be an absence rather than an error code. Each
//! probe request carries input the work itself rejects — SQL no compiler
//! accepts, a function name no parser accepts, an empty query, an undecodable
//! provider, an unspecified scope — so the caller who is let through is told
//! what is wrong with their request and the caller who is not never gets that
//! far.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

use coral_api::CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND;
use coral_api::v1::{
    AddFunctionRequest, ClearSearchDataRequest, DeleteFunctionRequest, DrainSearchQueueRequest,
    ExecuteSqlRequest, FunctionWriteSurface, ListFunctionsRequest, RebuildSearchIndexRequest,
    SearchClearTarget, SearchDataScope, SearchIndexProvider, SearchRequest, WorkspaceRole,
    search_clear_target,
};
use coral_client::{AppClient, batches_to_json_rows, decode_execute_sql_response};
use serde_json::{Value, json};
use tonic::{Code, Request, Status};

#[path = "grpc/harness.rs"]
#[expect(
    dead_code,
    reason = "The shared harness serves several integration binaries; this one exercises the shared-deployment half of it."
)]
mod harness;

use crate::harness::{
    SharedDeployment, add_member, concealed_refusal, create_workspace, named_workspace,
};

/// SQL no compiler accepts, so a caller who reaches function installation is
/// told what is wrong with it and a caller who does not never gets that far.
const UNPARSEABLE_SQL: &str = "this is not sql";

/// A function name no parser accepts, for the same reason.
const UNPARSEABLE_FUNCTION: &str = "not a function name";

/// A provider number no decoder maps, so answering it is proof the rebuild
/// request was built at all.
const UNDECODABLE_PROVIDER: i32 = 9_999;

/// The function the owner installs, published under the name its frontmatter
/// declares so a member can call it by that name.
const ECHO_FUNCTION_SQL: &str = r"/*
name: echo_value
schema: functions
description: Echo one value
guide: Use this function to echo a typed value.
*/

select cast($value as VARCHAR) as value
";

const ECHO_FUNCTION: &str = "echo_value";

/// Calls the installed function, so a member's read of it is the function
/// running rather than only its name being listed.
const ECHO_CALL: &str = "select * from functions.echo_value(value => 'hello')";

async fn add_function(client: &AppClient, name: &str, sql: &str) -> Result<String, Status> {
    client
        .function_client()
        .add_function(Request::new(AddFunctionRequest {
            workspace: Some(named_workspace(name)),
            sql: sql.to_string(),
            fail_if_exists: false,
            write_surface: FunctionWriteSurface::Cli as i32,
        }))
        .await
        .map(|response| {
            response
                .into_inner()
                .function
                .expect("an accepted install answers with the installed function")
                .name
        })
}

async fn list_functions(client: &AppClient, name: &str) -> Result<Vec<String>, Status> {
    client
        .function_client()
        .list_functions(Request::new(ListFunctionsRequest {
            workspace: Some(named_workspace(name)),
        }))
        .await
        .map(|response| {
            response
                .into_inner()
                .functions
                .into_iter()
                .map(|function| function.name)
                .collect()
        })
}

async fn delete_function(client: &AppClient, name: &str, function: &str) -> Result<(), Status> {
    client
        .function_client()
        .delete_function(Request::new(DeleteFunctionRequest {
            workspace: Some(named_workspace(name)),
            name: function.to_string(),
        }))
        .await
        .map(|_| ())
}

async fn search(client: &AppClient, name: &str, query: &str) -> Result<(), Status> {
    client
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(named_workspace(name)),
            query: query.to_string(),
            limit: 0,
        }))
        .await
        .map(|_| ())
}

async fn rebuild_search_index(client: &AppClient, name: &str, provider: i32) -> Result<(), Status> {
    client
        .search_client()
        .rebuild_search_index(Request::new(RebuildSearchIndexRequest {
            workspace: Some(named_workspace(name)),
            force: true,
            provider,
        }))
        .await
        .map(|_| ())
}

async fn drain_search_queue(client: &AppClient, name: &str) -> Result<(), Status> {
    client
        .search_client()
        .drain_search_queue(Request::new(DrainSearchQueueRequest {
            workspace: Some(named_workspace(name)),
            budget_ms: 1,
        }))
        .await
        .map(|_| ())
}

async fn clear_search_data(
    client: &AppClient,
    name: &str,
    scope: SearchDataScope,
    target: Option<SearchClearTarget>,
) -> Result<(), Status> {
    client
        .search_client()
        .clear_search_data(Request::new(ClearSearchDataRequest {
            workspace: Some(named_workspace(name)),
            scope: scope as i32,
            target,
        }))
        .await
        .map(|_| ())
}

/// Runs one statement as `client` and hands back the rows, so calling a
/// function proves the function ran rather than only that the statement was
/// allowed.
async fn execute_sql_rows(client: &AppClient, name: &str, sql: &str) -> Result<Vec<Value>, Status> {
    let response = client
        .query_client()
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(named_workspace(name)),
            sql: sql.to_string(),
            guide_read_context: None,
            task_attribution: None,
        }))
        .await?
        .into_inner();
    let decoded = decode_execute_sql_response(&response)
        .map_err(|error| Status::internal(error.to_string()))?;
    batches_to_json_rows(decoded.batches()).map_err(|error| Status::internal(error.to_string()))
}

/// The five requests that change what a workspace's members can run or find.
async fn every_mutation(client: &AppClient, name: &str) -> Vec<(&'static str, Result<(), Status>)> {
    vec![
        (
            "AddFunction",
            add_function(client, name, UNPARSEABLE_SQL)
                .await
                .map(|_| ()),
        ),
        (
            "DeleteFunction",
            delete_function(client, name, UNPARSEABLE_FUNCTION).await,
        ),
        (
            "RebuildSearchIndex",
            rebuild_search_index(client, name, UNDECODABLE_PROVIDER).await,
        ),
        ("DrainSearchQueue", drain_search_queue(client, name).await),
        (
            "ClearSearchData",
            clear_search_data(client, name, SearchDataScope::Unspecified, None).await,
        ),
    ]
}

/// Every function and search RPC, reads first.
async fn every_rpc(client: &AppClient, name: &str) -> Vec<(&'static str, Result<(), Status>)> {
    let mut rpcs = vec![
        (
            "ListFunctions",
            list_functions(client, name).await.map(|_| ()),
        ),
        ("Search", search(client, name, "").await),
    ];
    rpcs.extend(every_mutation(client, name).await);
    rpcs
}

/// Reports only what a refused caller is told on each RPC: the surface beside
/// the code, the message with the workspace name they supplied themselves
/// factored out, and the structured reasons.
async fn refusals(
    client: &AppClient,
    name: &str,
) -> Vec<(&'static str, Code, String, Vec<String>)> {
    let mut refused = Vec::new();
    for (rpc, result) in every_rpc(client, name).await {
        let status = result.expect_err("a refused caller must not be answered by these RPCs");
        let (code, message, reasons) = concealed_refusal(&status, name);
        refused.push((rpc, code, message, reasons));
    }
    refused
}

/// Asserts `client` reads both families and changes neither.
async fn reads_both_and_changes_neither(client: &AppClient, name: &str, who: &str) {
    assert_eq!(
        list_functions(client, name)
            .await
            .unwrap_or_else(|status| panic!("{who} lists the workspace's functions: {status}")),
        vec![ECHO_FUNCTION.to_string()],
    );
    assert_eq!(
        execute_sql_rows(client, name, ECHO_CALL)
            .await
            .unwrap_or_else(|status| panic!("{who} calls the installed function: {status}")),
        vec![json!({ "value": "hello" })],
    );
    search(client, name, "echo one value")
        .await
        .unwrap_or_else(|status| panic!("{who} searches the workspace: {status}"));

    for (rpc, result) in every_mutation(client, name).await {
        assert_eq!(
            result
                .expect_err("only an owner changes a function or a search index")
                .code(),
            Code::PermissionDenied,
            "{who} reached {rpc}",
        );
    }
}

/// The owner reaches every function and search RPC in their own workspace: the
/// real calls succeed, and where a probe request is refused it is the request
/// that stopped them rather than the gate.
#[tokio::test]
async fn an_owner_reaches_every_function_and_search_rpc_in_their_own_workspace() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("fs-own-ada", "Ada").await;
    let owner = deployment.as_person(&ada).await;
    create_workspace(&owner, "fs-own")
        .await
        .expect("the creator makes their own workspace");

    assert_eq!(
        add_function(&owner, "fs-own", ECHO_FUNCTION_SQL)
            .await
            .expect("the owner installs a function"),
        ECHO_FUNCTION,
    );
    assert_eq!(
        list_functions(&owner, "fs-own")
            .await
            .expect("the owner lists their functions"),
        vec![ECHO_FUNCTION.to_string()],
    );
    search(&owner, "fs-own", "echo one value")
        .await
        .expect("the owner searches their workspace");
    rebuild_search_index(&owner, "fs-own", SearchIndexProvider::All as i32)
        .await
        .expect("the owner rebuilds the search index");
    drain_search_queue(&owner, "fs-own")
        .await
        .expect("the owner drains the search queue");
    clear_search_data(
        &owner,
        "fs-own",
        SearchDataScope::All,
        Some(SearchClearTarget {
            target: Some(search_clear_target::Target::Workspace(true)),
        }),
    )
    .await
    .expect("the owner clears the workspace's search data");

    for (rpc, result) in every_rpc(&owner, "fs-own").await {
        if let Err(status) = result {
            let (code, message, reasons) = concealed_refusal(&status, "fs-own");
            assert!(
                code != Code::PermissionDenied
                    && reasons.as_slice() != [CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND],
                "the owner must be stopped by the request, never by the gate: {rpc} {code} {message}",
            );
        }
    }

    delete_function(&owner, "fs-own", ECHO_FUNCTION)
        .await
        .expect("the owner deletes the function");
    assert_eq!(
        list_functions(&owner, "fs-own")
            .await
            .expect("the owner lists their functions"),
        Vec::<String>::new(),
    );
}

/// Membership opens both families for reading and neither for writing: a member
/// lists the workspace's functions, runs one, and searches it, while every
/// request that would change what the workspace runs or finds is refused. The
/// owner's own listing afterwards is the control that the refusals removed and
/// installed nothing.
#[tokio::test]
async fn a_member_reads_functions_and_search_but_changes_neither() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("fs-member-ada", "Ada").await;
    let bob = deployment.seed_user("fs-member-bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    let member = deployment.as_person(&bob).await;
    create_workspace(&owner, "fs-member")
        .await
        .expect("the creator makes their own workspace");
    add_function(&owner, "fs-member", ECHO_FUNCTION_SQL)
        .await
        .expect("the owner installs a function");
    add_member(&owner, "fs-member", &bob, WorkspaceRole::Member)
        .await
        .expect("the owner grants membership");

    reads_both_and_changes_neither(&member, "fs-member", "a member").await;

    assert_eq!(
        list_functions(&owner, "fs-member")
            .await
            .expect("the owner lists their functions"),
        vec![ECHO_FUNCTION.to_string()],
        "the refused delete removed nothing and the refused install added nothing",
    );
}

/// The data plane carries the person's workspace access and the control plane
/// does not, so an agent credential is on both sides of the same line.
///
/// The owner's own agent is the half that matters: their role would make every
/// mutation theirs to perform, and the credential is refused anyway — a
/// prompt-injected agent cannot publish SQL every member then runs, nor clear
/// the index every member searches. The member's agent proves the other half,
/// that the reads were not shut off along with the writes.
#[tokio::test]
async fn an_agent_credential_reads_both_families_and_changes_neither() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("fs-agent-ada", "Ada").await;
    let bob = deployment.seed_user("fs-agent-bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    create_workspace(&owner, "fs-agent")
        .await
        .expect("the creator makes their own workspace");
    add_function(&owner, "fs-agent", ECHO_FUNCTION_SQL)
        .await
        .expect("the owner installs a function");
    add_member(&owner, "fs-agent", &bob, WorkspaceRole::Member)
        .await
        .expect("the owner grants membership");

    reads_both_and_changes_neither(
        &deployment.as_agent(&bob).await,
        "fs-agent",
        "a member's agent",
    )
    .await;
    reads_both_and_changes_neither(
        &deployment.as_agent(&ada).await,
        "fs-agent",
        "the owner's own agent",
    )
    .await;

    assert_eq!(
        list_functions(&owner, "fs-agent")
            .await
            .expect("the owner lists their functions"),
        vec![ECHO_FUNCTION.to_string()],
        "no agent credential changed the function set",
    );
}

/// Neither family may answer a question its caller may not ask. A workspace a
/// non-member holds no membership in has to read exactly like a name nobody
/// ever created — and read as the *absent* workspace specifically, since a
/// uniform "denied" would agree with itself while still confirming the name
/// exists.
#[tokio::test]
async fn a_non_members_refusals_read_exactly_like_an_absent_workspace() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("fs-conceal-ada", "Ada").await;
    let bob = deployment.seed_user("fs-conceal-bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    let outsider = deployment.as_person(&bob).await;
    create_workspace(&owner, "fs-conceal")
        .await
        .expect("the creator makes their own workspace");
    add_function(&owner, "fs-conceal", ECHO_FUNCTION_SQL)
        .await
        .expect("the owner installs a function");

    let existing = refusals(&outsider, "fs-conceal").await;
    assert_eq!(
        existing,
        refusals(&outsider, "fs-ghost").await,
        "an existing workspace must be indistinguishable from one that never existed",
    );
    assert!(
        existing
            .iter()
            .all(|(_, code, _, reasons)| *code == Code::NotFound
                && reasons.as_slice() == [CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND]),
        "both must read as the absent workspace, not as a denial that confirms one: {existing:?}",
    );

    assert_eq!(
        list_functions(&owner, "fs-conceal")
            .await
            .expect("the owner lists their functions"),
        vec![ECHO_FUNCTION.to_string()],
        "the concealed refusals changed nothing behind the concealment",
    );
}
