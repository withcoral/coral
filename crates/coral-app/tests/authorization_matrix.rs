//! Freezes the access classification of every authenticated Coral RPC.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

use std::collections::BTreeSet;
use std::fs;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Access {
    Read,
    Manage,
    AnyHuman,
    OwnerOfAny,
    Open,
}

const AUTHORIZATION_MATRIX: &[(&str, Access)] = &[
    ("coral.v1.CatalogService/ListCatalog", Access::Read),
    ("coral.v1.CatalogService/SearchCatalog", Access::Read),
    ("coral.v1.CatalogService/DescribeTable", Access::Read),
    ("coral.v1.CatalogService/ListColumns", Access::Read),
    ("coral.v1.FeedbackService/SubmitFeedback", Access::Read),
    ("coral.v1.FunctionService/AddFunction", Access::Manage),
    ("coral.v1.FunctionService/ListFunctions", Access::Read),
    ("coral.v1.FunctionService/DeleteFunction", Access::Manage),
    ("coral.v1.QueryService/ExecuteSql", Access::Read),
    ("coral.v1.QueryService/ExplainSql", Access::Read),
    ("coral.v1.SearchService/Search", Access::Read),
    ("coral.v1.SearchService/RebuildSearchIndex", Access::Manage),
    ("coral.v1.SearchService/DrainSearchQueue", Access::Manage),
    ("coral.v1.SearchService/ClearSearchData", Access::Manage),
    ("coral.v1.SourceService/DiscoverSources", Access::Manage),
    ("coral.v1.SourceService/ListSources", Access::Manage),
    ("coral.v1.SourceService/GetSource", Access::Manage),
    ("coral.v1.SourceService/GetSourceInfo", Access::Manage),
    ("coral.v1.SourceService/CreateBundledSource", Access::Manage),
    (
        "coral.v1.SourceService/CreateBundledSourceWithOAuth",
        Access::Manage,
    ),
    ("coral.v1.SourceService/ImportSource", Access::Manage),
    ("coral.v1.SourceService/DeleteSource", Access::Manage),
    ("coral.v1.SourceService/ValidateSource", Access::Manage),
    ("coral.v1.TaskService/StartTask", Access::Read),
    ("coral.v1.TaskService/EndTask", Access::Read),
    ("coral.v1.TraceService/ListTraces", Access::Manage),
    ("coral.v1.TraceService/GetTrace", Access::Manage),
    ("coral.v1.UserService/ListUsers", Access::OwnerOfAny),
    ("coral.v1.UserService/GetCurrentUser", Access::AnyHuman),
    ("coral.v1.WorkspaceService/ListWorkspaces", Access::Read),
    (
        "coral.v1.WorkspaceService/CreateWorkspace",
        Access::AnyHuman,
    ),
    ("coral.v1.WorkspaceService/DeleteWorkspace", Access::Manage),
    (
        "coral.v1.WorkspaceService/ListWorkspaceMembers",
        Access::Manage,
    ),
    (
        "coral.v1.WorkspaceService/AddWorkspaceMember",
        Access::Manage,
    ),
    (
        "coral.v1.WorkspaceService/RemoveWorkspaceMember",
        Access::Manage,
    ),
];

const OPEN_RPCS: &[(&str, Access)] = &[
    ("grpc.health.v1.Health/Check", Access::Open),
    ("grpc.health.v1.Health/Watch", Access::Open),
];

#[test]
fn every_coral_rpc_has_exactly_one_frozen_authorization() {
    let discovered = coral_rpc_names();
    let discovered_set = discovered
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        discovered.len(),
        discovered_set.len(),
        "duplicate RPC definitions in Coral protos"
    );

    let classified = AUTHORIZATION_MATRIX
        .iter()
        .map(|(rpc, _access)| *rpc)
        .collect::<Vec<_>>();
    let classified_set = classified.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(
        classified.len(),
        classified_set.len(),
        "duplicate authorization matrix entries"
    );

    let omitted = discovered_set
        .difference(&classified_set)
        .collect::<Vec<_>>();
    let stale = classified_set
        .difference(&discovered_set)
        .collect::<Vec<_>>();
    assert!(
        omitted.is_empty() && stale.is_empty(),
        "authorization matrix mismatch; omitted={omitted:?}, stale={stale:?}"
    );
    assert!(
        AUTHORIZATION_MATRIX
            .iter()
            .all(|(rpc, access)| rpc.starts_with("coral.v1.") && *access != Access::Open)
    );
    assert!(
        OPEN_RPCS
            .iter()
            .all(|(rpc, access)| rpc.starts_with("grpc.health.v1.Health/")
                && *access == Access::Open)
    );
}

fn coral_rpc_names() -> Vec<String> {
    let mut methods = Vec::new();
    let proto_dir = format!("{}/../coral-api/proto/coral/v1", env!("CARGO_MANIFEST_DIR"));
    let mut proto_paths = fs::read_dir(proto_dir)
        .expect("read Coral proto directory")
        .map(|entry| entry.expect("read Coral proto entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "proto")
        })
        .collect::<Vec<_>>();
    proto_paths.sort();
    for path in proto_paths {
        let proto = fs::read_to_string(path).expect("read Coral proto");
        let mut service = None;
        for raw_line in proto.lines() {
            let line = raw_line.split("//").next().unwrap_or_default().trim();
            if let Some(declaration) = line.strip_prefix("service ") {
                service = declaration.split_whitespace().next();
            } else if let (Some(service), Some(declaration)) = (service, line.strip_prefix("rpc "))
            {
                let method = declaration
                    .split_once('(')
                    .map(|(method, _arguments)| method.trim())
                    .expect("RPC declaration must contain '('");
                methods.push(format!("coral.v1.{service}/{method}"));
            } else if service.is_some() && line == "}" {
                service = None;
            }
        }
    }
    methods
}
