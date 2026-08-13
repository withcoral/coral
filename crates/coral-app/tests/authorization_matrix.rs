//! Freezes the authorization classification of every served gRPC method.

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
    SelfScoped,
    AnyAuthenticatedHuman,
    OwnerOfAnyWorkspace,
    Open,
}

const AUTHORIZATION_MATRIX: &[(&str, Access)] = &[
    ("coral.v1.CatalogService/ListCatalog", Access::Read),
    ("coral.v1.CatalogService/SearchCatalog", Access::Read),
    (
        "coral.v1.CatalogService/DescribeCatalogSurface",
        Access::Read,
    ),
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
    (
        "coral.v1.UserService/ListUsers",
        Access::OwnerOfAnyWorkspace,
    ),
    (
        "coral.v1.UserService/GetCurrentUser",
        Access::AnyAuthenticatedHuman,
    ),
    (
        "coral.v1.WorkspaceService/ListWorkspaces",
        Access::SelfScoped,
    ),
    (
        "coral.v1.WorkspaceService/CreateWorkspace",
        Access::AnyAuthenticatedHuman,
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
    ("grpc.health.v1.Health/Check", Access::Open),
    ("grpc.health.v1.Health/Watch", Access::Open),
];

#[test]
fn every_served_rpc_has_exactly_one_frozen_authorization() {
    let mut discovered = coral_rpc_names();
    discovered.extend([
        "grpc.health.v1.Health/Check".to_string(),
        "grpc.health.v1.Health/Watch".to_string(),
    ]);
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
    assert!(AUTHORIZATION_MATRIX.iter().all(|(rpc, access)| {
        let is_health = rpc.starts_with("grpc.health.v1.Health/");
        (is_health && *access == Access::Open)
            || (rpc.starts_with("coral.v1.") && *access != Access::Open)
    }));
}

#[test]
fn proto_parser_handles_comments_multiline_declarations_and_rpc_option_bodies() {
    let proto = r#"
        /* service Ignored { rpc Hidden(HiddenRequest) returns (HiddenResponse); } */
        service
          SplitService
        {
          // rpc CommentedOut(CommentedRequest) returns (CommentedResponse);
          rpc
            First
          (
            FirstRequest
          )
          returns
          (
            FirstResponse
          ) {
            option (google.api.http) = {
              post: "/v1/{name}";
            };
          }
          rpc Later(LaterRequest) returns (LaterResponse);
        }
    "#;

    assert_eq!(
        rpc_names_in_proto(proto),
        ["coral.v1.SplitService/First", "coral.v1.SplitService/Later"]
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
        methods.extend(rpc_names_in_proto(&proto));
    }
    methods
}

fn rpc_names_in_proto(proto: &str) -> Vec<String> {
    let tokens = proto_tokens(proto);
    let mut methods = Vec::new();
    let mut cursor = 0;
    while cursor < tokens.len() {
        if tokens.get(cursor).map(String::as_str) != Some("service") {
            cursor += 1;
            continue;
        }
        let service = tokens.get(cursor + 1).expect("service must have a name");
        cursor += 2;
        while tokens.get(cursor).map(String::as_str) != Some("{") {
            cursor += 1;
            assert!(cursor < tokens.len(), "service must have a body");
        }
        cursor += 1;
        let mut depth = 1;
        while depth > 0 {
            let token = tokens
                .get(cursor)
                .map(String::as_str)
                .expect("service body must close");
            match token {
                "{" => depth += 1,
                "}" => depth -= 1,
                "rpc" if depth == 1 => {
                    let method = tokens.get(cursor + 1).expect("RPC must have a name");
                    assert!(method != "{" && method != "}", "RPC must have a name");
                    methods.push(format!("coral.v1.{service}/{method}"));
                }
                _ => {}
            }
            cursor += 1;
        }
    }
    methods
}

fn proto_tokens(proto: &str) -> Vec<String> {
    let bytes = proto.as_bytes();
    let mut tokens = Vec::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        let byte = *bytes.get(cursor).expect("cursor is in bounds");
        match byte {
            b'/' if bytes.get(cursor + 1) == Some(&b'/') => {
                cursor += 2;
                while bytes.get(cursor).is_some_and(|byte| *byte != b'\n') {
                    cursor += 1;
                }
            }
            b'/' if bytes.get(cursor + 1) == Some(&b'*') => {
                cursor += 2;
                while bytes.get(cursor..cursor + 2) != Some(b"*/") {
                    cursor += 1;
                    assert!(cursor + 1 < bytes.len(), "block comment must close");
                }
                cursor += 2;
            }
            quote @ (b'"' | b'\'') => {
                cursor += 1;
                while let Some(byte) = bytes.get(cursor).filter(|byte| **byte != quote) {
                    cursor += usize::from(*byte == b'\\') + 1;
                }
                assert!(cursor < bytes.len(), "quoted string must close");
                cursor += 1;
            }
            b'{' | b'}' => {
                tokens.push(char::from(byte).to_string());
                cursor += 1;
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                let start = cursor;
                cursor += 1;
                while bytes
                    .get(cursor)
                    .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
                {
                    cursor += 1;
                }
                let identifier = std::str::from_utf8(
                    bytes.get(start..cursor).expect("identifier range is valid"),
                )
                .expect("protobuf identifiers are ASCII");
                tokens.push(identifier.to_string());
            }
            _ => cursor += 1,
        }
    }
    tokens
}
