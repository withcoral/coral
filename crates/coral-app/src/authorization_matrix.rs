//! The one written-down answer to "who may call this RPC".
//!
//! Access rules are enforced inside each service, where the request and its
//! workspace are in hand. That is the right place to enforce them and the
//! wrong place to read them: a reviewer asking whether some RPC is guarded at
//! all would have to visit every handler and notice the one that is missing.
//! This module states the rule for every RPC in one table, and its tests fail
//! when an RPC exists without an entry, when an entry names an RPC that no
//! longer exists, or when the same RPC is classified twice.
//!
//! The table is a declaration, not a dispatcher. Nothing routes through it;
//! services keep enforcing their own rule, and the table is what makes the set
//! of those rules reviewable.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the matrix is a declaration proved by its own tests, not a dispatcher: by design no runtime path reads it, so every item is dead outside them"
    )
)]

use tonic_health::pb::health_server::SERVICE_NAME as HEALTH_SERVICE;

use self::Classification::{AnyHuman, LocalOnly, Manage, Open, OwnerDirectory, Read, SelfScoped};

/// The rule one RPC answers to.
///
/// [`Self::Read`] and [`Self::Manage`] are the two workspace-scoped levels and
/// mirror `WorkspaceAction`; the other four name the shapes that have no
/// workspace to scope to, and [`Self::Open`] names the absence of a rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Classification {
    /// No principal is required. Reserved for the transport's own liveness
    /// surface, which must answer before authentication exists to fail.
    Open,
    /// Any admitted caller, with nothing further to authorize: either the
    /// response is filtered to the caller's own rows, or it holds nothing that
    /// differs between callers.
    SelfScoped,
    /// Any admitted caller that is a person rather than an agent credential.
    /// Agents are excluded because the act creates authority rather than
    /// exercising it.
    AnyHuman,
    /// Any admitted caller who owns at least one workspace. Directory
    /// authority is ownership: the directory exists so an owner can name
    /// somebody as a member.
    OwnerDirectory,
    /// Only the built-in local principal, and only on a deployment that
    /// admits it. Host-global state has no workspace to scope to and shared
    /// mode deliberately has no superuser, so a shared deployment exposes it
    /// to nobody.
    LocalOnly,
    /// Members and owners of the named workspace.
    Read,
    /// Owners of the named workspace only.
    Manage,
}

/// One RPC and the rule it answers to.
pub(crate) struct RpcRule {
    /// Fully qualified gRPC service name, as it appears on the wire.
    pub(crate) service: &'static str,
    /// Method name, as declared in the service.
    pub(crate) method: &'static str,
    /// What the RPC requires of its caller.
    pub(crate) classification: Classification,
}

/// Declares one row of the matrix, short enough to keep a row on a line.
const fn rule(
    service: &'static str,
    method: &'static str,
    classification: Classification,
) -> RpcRule {
    RpcRule {
        service,
        method,
        classification,
    }
}

/// Every RPC this server mounts, classified exactly once.
///
/// The tests below hold this to the shipped protobuf definitions in both
/// directions, so an RPC cannot be added without a decision about who may call
/// it, and a rule cannot outlive the RPC it was written for.
pub(crate) const AUTHORIZATION_MATRIX: &[RpcRule] = &[
    // Liveness answers before there is a principal to answer for, so it is the
    // only surface with no rule at all.
    rule(HEALTH_SERVICE, "Check", Open),
    rule(HEALTH_SERVICE, "Watch", Open),
    // Reading a workspace's tables and columns is reading its contents.
    rule("coral.v1.CatalogService", "ListCatalog", Read),
    rule("coral.v1.CatalogService", "SearchCatalog", Read),
    rule("coral.v1.CatalogService", "DescribeCatalogSurface", Read),
    rule("coral.v1.CatalogService", "ListColumns", Read),
    // Feature flags are host-global rather than workspace-scoped: there is no
    // workspace whose owner could be entitled to them. Reading is `SelfScoped`
    // in the sense that admission is the whole check — the listing carries the
    // same keys, descriptions, and enabled state for every caller, and a page
    // that cannot read it cannot say why its switches do nothing. Changing
    // them is what stays with the host.
    rule("coral.v1.FeatureService", "ListFeatures", SelfScoped),
    rule("coral.v1.FeatureService", "SetFeature", LocalOnly),
    rule("coral.v1.FeedbackService", "SubmitFeedback", Read),
    // Listing and using functions is reading; changing the function set
    // changes what every member of the workspace can run.
    rule("coral.v1.FunctionService", "ListFunctions", Read),
    rule("coral.v1.FunctionService", "AddFunction", Manage),
    rule("coral.v1.FunctionService", "DeleteFunction", Manage),
    // Onboarding records what a person has been shown, so it belongs to a
    // person: an agent holding someone's credential has no onboarding of its
    // own, and the service refuses one rather than answering for the human.
    rule(
        "coral.v1.GuiOnboardingService",
        "GetGuiOnboardingState",
        AnyHuman,
    ),
    rule(
        "coral.v1.GuiOnboardingService",
        "CompleteGuiOnboarding",
        AnyHuman,
    ),
    rule("coral.v1.QueryService", "ExecuteSql", Read),
    rule("coral.v1.QueryService", "ExplainSql", Read),
    // Searching reads the workspace; rebuilding, draining, and clearing the
    // index are maintenance of it.
    rule("coral.v1.SearchService", "Search", Read),
    rule("coral.v1.SearchService", "RebuildSearchIndex", Manage),
    rule("coral.v1.SearchService", "DrainSearchQueue", Manage),
    rule("coral.v1.SearchService", "ClearSearchData", Manage),
    // Every source RPC manages, including the reads: their responses carry
    // source configuration and credential metadata. A member-readable source
    // view needs a separate redacted response before it can be classified
    // Read.
    rule("coral.v1.SourceService", "DiscoverSources", Manage),
    rule("coral.v1.SourceService", "ListSources", Manage),
    rule("coral.v1.SourceService", "GetSource", Manage),
    rule("coral.v1.SourceService", "GetSourceInfo", Manage),
    rule("coral.v1.SourceService", "CreateBundledSource", Manage),
    rule(
        "coral.v1.SourceService",
        "CreateBundledSourceWithOAuth",
        Manage,
    ),
    rule("coral.v1.SourceService", "ImportSource", Manage),
    rule("coral.v1.SourceService", "DeleteSource", Manage),
    rule("coral.v1.SourceService", "ValidateSource", Manage),
    // Task attribution rides along with the reads it labels.
    rule("coral.v1.TaskService", "StartTask", Read),
    rule("coral.v1.TaskService", "EndTask", Read),
    // Traces replay what other callers ran, including their SQL, so reading
    // them is an owner's power rather than a member's.
    rule("coral.v1.TraceService", "ListTraces", Manage),
    rule("coral.v1.TraceService", "GetTrace", Manage),
    // The caller's own identity is self-scoped; the deployment-wide directory
    // is not, and ownership is what entitles a caller to read it.
    rule("coral.v1.UserService", "GetCurrentUser", SelfScoped),
    rule("coral.v1.UserService", "ListUsers", OwnerDirectory),
    // Listing returns the caller's own memberships. Creation grants its caller
    // ownership, which is why an agent credential may not perform it. The rest
    // change a workspace or who may reach it.
    rule("coral.v1.WorkspaceService", "ListWorkspaces", SelfScoped),
    rule("coral.v1.WorkspaceService", "CreateWorkspace", AnyHuman),
    rule("coral.v1.WorkspaceService", "DeleteWorkspace", Manage),
    rule("coral.v1.WorkspaceService", "ListWorkspaceMembers", Manage),
    rule("coral.v1.WorkspaceService", "AddWorkspaceMember", Manage),
    rule("coral.v1.WorkspaceService", "RemoveWorkspaceMember", Manage),
];

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::PathBuf;

    use super::{AUTHORIZATION_MATRIX, Classification, HEALTH_SERVICE};

    /// The protobuf definitions this crate serves.
    fn proto_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../coral-api/proto/coral/v1")
    }

    /// Every RPC declared in the shipped protos, as `package.Service/Method`.
    ///
    /// This reads the `.proto` sources rather than a hand-kept list so that a
    /// new RPC — or a whole new service file — is discovered by the test that
    /// demands a classification for it.
    fn declared_rpcs() -> BTreeSet<String> {
        let entries = fs::read_dir(proto_dir()).expect("proto directory is readable");
        let mut declared = BTreeSet::new();
        let mut files = 0_usize;
        for entry in entries {
            let path = entry.expect("proto directory entry").path();
            if path.extension().is_none_or(|ext| ext != "proto") {
                continue;
            }
            files += 1;
            let text = fs::read_to_string(&path).expect("proto file is readable");
            let mut package: Option<&str> = None;
            let mut service: Option<&str> = None;
            for line in text.lines() {
                let line = line.trim();
                if line.starts_with("//") {
                    continue;
                }
                if let Some(rest) = line.strip_prefix("package ") {
                    package = Some(rest.trim_end_matches(';').trim());
                } else if let Some(rest) = line.strip_prefix("service ") {
                    service = Some(rest.trim_end_matches('{').trim());
                } else if let Some(rest) = line.strip_prefix("rpc ") {
                    let method = rest
                        .split('(')
                        .next()
                        .expect("split always yields a first part")
                        .trim();
                    let package = package.expect("a package precedes the rpc that uses it");
                    let service = service.expect("a service precedes the rpcs it declares");
                    declared.insert(format!("{package}.{service}/{method}"));
                }
            }
        }
        assert!(files > 0, "found no protos to walk under {:?}", proto_dir());
        declared
    }

    /// The matrix's own view of the same identifiers.
    fn classified_rpcs() -> Vec<String> {
        AUTHORIZATION_MATRIX
            .iter()
            .map(|rule| format!("{}/{}", rule.service, rule.method))
            .collect()
    }

    #[test]
    fn no_rpc_is_classified_twice() {
        let classified = classified_rpcs();
        let distinct: BTreeSet<&String> = classified.iter().collect();
        assert_eq!(
            distinct.len(),
            classified.len(),
            "the matrix classifies some RPC more than once"
        );
    }

    #[test]
    fn every_declared_rpc_is_classified_and_no_rule_outlives_its_rpc() {
        let declared = declared_rpcs();
        let classified: BTreeSet<String> = classified_rpcs()
            .into_iter()
            .filter(|rpc| rpc.starts_with("coral.v1."))
            .collect();

        let unclassified: Vec<&String> = declared.difference(&classified).collect();
        assert!(
            unclassified.is_empty(),
            "these RPCs exist but no one decided who may call them: {unclassified:?}"
        );
        let stale: Vec<&String> = classified.difference(&declared).collect();
        assert!(
            stale.is_empty(),
            "these matrix rules name RPCs that no longer exist: {stale:?}"
        );
    }

    /// Liveness is the only surface that answers without a principal. Anything
    /// else classified `Open` would be an unauthenticated hole, and a liveness
    /// probe classified otherwise could not answer before authentication.
    #[test]
    fn health_and_readiness_are_the_only_open_surface() {
        for rule in AUTHORIZATION_MATRIX {
            assert_eq!(
                rule.classification == Classification::Open,
                rule.service == HEALTH_SERVICE,
                "{}/{} disagrees with the rule that only {HEALTH_SERVICE} is open",
                rule.service,
                rule.method
            );
        }
    }

    /// Feature state is host-global, and a shared deployment has no superuser
    /// to entrust it to, so *changing* it stays with the local principal.
    /// Reading it does not: the page that renders the switches has to say
    /// which ones are on. The split is asserted rather than assumed, so
    /// widening the mutation half would have to be written down here first.
    #[test]
    fn only_changing_a_feature_is_local_only() {
        let features: Vec<&super::RpcRule> = AUTHORIZATION_MATRIX
            .iter()
            .filter(|rule| rule.service == "coral.v1.FeatureService")
            .collect();
        assert!(!features.is_empty(), "the feature service lost its rules");
        for rule in features {
            let expected = match rule.method {
                "SetFeature" => Classification::LocalOnly,
                "ListFeatures" => Classification::SelfScoped,
                method => panic!("the feature service gained {method} without a decision"),
            };
            assert_eq!(
                rule.classification, expected,
                "{}/{} no longer answers to the rule it was split under",
                rule.service, rule.method
            );
        }
    }

    /// Source responses carry configuration and credential metadata, so even
    /// their reads are owner-only until a redacted projection exists.
    #[test]
    fn every_source_rpc_requires_manage() {
        let sources: Vec<&super::RpcRule> = AUTHORIZATION_MATRIX
            .iter()
            .filter(|rule| rule.service == "coral.v1.SourceService")
            .collect();
        assert!(!sources.is_empty(), "the source service lost its rules");
        for rule in sources {
            assert_eq!(
                rule.classification,
                Classification::Manage,
                "{}/{} would expose source configuration to a member",
                rule.service,
                rule.method
            );
        }
    }
}
