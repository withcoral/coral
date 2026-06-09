use coral_capabilities::{Capability, ProviderOriginKind, SupportStatus, UpstreamBinding};

use crate::exports::{
    Binding, BindingBuildContext, BindingContribution, BindingContributor, ExportRef,
    TypescriptBinding,
};
use crate::paths::{identifier_segment, is_version_segment, pascal_segment};

/// Built-in TypeScript binding contributor.
#[derive(Debug, Default)]
pub struct TypescriptBindingContributor;

impl TypescriptBindingContributor {
    /// Creates a TypeScript contributor.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl BindingContributor for TypescriptBindingContributor {
    fn name(&self) -> &'static str {
        "typescript"
    }

    fn contribute(
        &self,
        capability: &Capability,
        ctx: &BindingBuildContext,
    ) -> crate::exports::Result<BindingContribution> {
        if capability.display.support_status == SupportStatus::Unsupported {
            return Ok(BindingContribution::empty());
        }
        let path = typescript_path(capability, ctx);
        let binding = TypescriptBinding {
            ref_: ExportRef::typescript(&path),
            args_type_name: typescript_type_name(&path, "Args"),
            result_type_name: typescript_type_name(&path, "Result"),
            path: path.clone(),
        };
        Ok(BindingContribution {
            bindings: vec![Binding::Typescript(binding)],
            search_text: path.clone(),
            diagnostics: Vec::new(),
            binding_diagnostics: Vec::new(),
        })
    }
}

fn typescript_path(capability: &Capability, ctx: &BindingBuildContext) -> Vec<String> {
    let mut path = vec![
        identifier_segment(ctx.source_key.as_str()),
        identifier_segment(&capability.interface_id),
    ];
    match capability.provider_origin.kind {
        ProviderOriginKind::RestOperation => {
            path.extend(rest_operation_path_segments(capability));
        }
        ProviderOriginKind::GraphqlRootField => {
            path.extend(graphql_operation_path_segments(capability));
        }
        ProviderOriginKind::McpTool | ProviderOriginKind::FileRelation => {
            path.push(identifier_segment(
                &capability.provider_origin.provider_name,
            ));
        }
    }
    path
}

fn rest_operation_path_segments(capability: &Capability) -> Vec<String> {
    let group = rest_operation_group(capability);
    let leaf = rest_operation_leaf(capability, &group);
    vec![group, leaf]
}

fn rest_operation_group(capability: &Capability) -> String {
    if let Some(tag) = capability
        .provider_origin
        .tags
        .iter()
        .find(|tag| !tag.trim().is_empty())
    {
        return identifier_segment(tag);
    }
    // Tags are the provider-authored grouping model. Without tags, use the URL
    // shape instead of inferring a namespace from operationId casing.
    rest_path_group(capability).unwrap_or_else(|| "root".to_string())
}

fn rest_path_group(capability: &Capability) -> Option<String> {
    let UpstreamBinding::Rest(binding) = &capability.upstream_binding else {
        return None;
    };
    binding
        .path_template
        .split('/')
        .map(str::trim)
        .find(|segment| useful_path_segment(segment))
        .map(identifier_segment)
}

fn useful_path_segment(segment: &str) -> bool {
    if segment.is_empty()
        || segment.starts_with('{')
        || segment.starts_with(':')
        || matches!(segment.to_ascii_lowercase().as_str(), "api" | "apis")
    {
        return false;
    }
    !is_version_segment(segment)
}

fn rest_operation_leaf(capability: &Capability, group: &str) -> String {
    let provider_operation_id = capability.provider_origin.provider_name.trim();
    let raw = if provider_operation_id.is_empty() {
        capability.operation_id.as_str()
    } else {
        provider_operation_id
    };
    let leaf = strip_matching_operation_group(raw, group).unwrap_or_else(|| raw.to_string());
    identifier_segment(&leaf)
}

fn strip_matching_operation_group(operation_id: &str, group: &str) -> Option<String> {
    // Only strip provider-authored hierarchy separators. CamelCase prefixes are
    // intentionally preserved so operationIds like ValidateMonitor stay intact.
    if !operation_id.contains(['/', '.']) {
        return None;
    }
    let mut segments = operation_id
        .split(['/', '.'])
        .filter(|segment| !segment.is_empty());
    let first = segments.next()?;
    if identifier_segment(first) != group {
        return None;
    }
    let remainder = segments.collect::<Vec<_>>().join("/");
    if remainder.is_empty() {
        None
    } else {
        Some(remainder)
    }
}

fn graphql_operation_path_segments(capability: &Capability) -> Vec<String> {
    let operation_kind = match &capability.upstream_binding {
        UpstreamBinding::Graphql(binding) => Some(binding.graphql_operation_kind.as_keyword()),
        _ => None,
    };
    let Some(kind) = operation_kind else {
        return vec![identifier_segment(&capability.operation_id)];
    };
    let prefix = format!("{kind}_");
    let field = capability
        .operation_id
        .strip_prefix(&prefix)
        .unwrap_or(capability.operation_id.as_str());
    vec![kind.to_string(), identifier_segment(field)]
}

/// Builds a deterministic TypeScript type name from a binding path.
#[must_use]
pub fn typescript_type_name(path: &[String], suffix: &str) -> String {
    let mut out = String::new();
    for segment in path {
        out.push_str(&pascal_segment(segment));
    }
    out.push_str(suffix);
    out
}

#[cfg(test)]
mod tests {
    use coral_capabilities::{
        Capability, FileFormatDescriptor, FileScanBinding, GraphqlOperationBinding,
        GraphqlOperationKind, McpTaskSupport, McpToolUpstreamBinding, ProviderOrigin,
        ProviderOriginKind, SourceId, UpstreamBinding,
    };

    use crate::SourceKey;
    use crate::exports::{Binding, BindingBuildContext, BindingContributor};

    use super::TypescriptBindingContributor;

    #[test]
    fn typescript_contributor_uses_source_key_namespace() {
        let source_id = SourceId("src_demo".to_string());
        let capability = Capability::new(
            source_id.clone(),
            "rest",
            "list_issues",
            ProviderOrigin {
                kind: ProviderOriginKind::FileRelation,
                snapshot_ref: "interfaces/files/provider-snapshot.yaml#/files/issues".to_string(),
                provider_name: "List Issues".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::FileRead(FileScanBinding {
                file_refs: Vec::new(),
                format: FileFormatDescriptor::Jsonl,
                schema_ref: None,
            }),
        );
        let ctx = BindingBuildContext {
            source_id,
            display_name: "Renamed Demo".to_string(),
            source_key: SourceKey("demo_key".to_string()),
        };
        let contribution = TypescriptBindingContributor::new()
            .contribute(&capability, &ctx)
            .expect("contribute");
        assert_eq!(
            contribution.search_text.first().map(String::as_str),
            Some("demoKey")
        );
    }

    #[test]
    fn typescript_contributor_disambiguates_graphql_operation_kinds() {
        let source_id = SourceId("src_linear".to_string());
        let query = Capability::new(
            source_id.clone(),
            "graph",
            "query_project_update",
            ProviderOrigin {
                kind: ProviderOriginKind::GraphqlRootField,
                snapshot_ref:
                    "interfaces/graph/provider-snapshot.yaml#/root_fields/query_project_update"
                        .to_string(),
                provider_name: "projectUpdate".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::Graphql(GraphqlOperationBinding {
                endpoint_ref: "source/src_linear/interface/graph/endpoint/default".to_string(),
                operation_name: "QueryProjectUpdate".to_string(),
                graphql_operation_kind: GraphqlOperationKind::Query,
                document_ref:
                    "source/src_linear/interface/graph/generated/query_project_update.graphql"
                        .to_string(),
                selection_set: Some("id".to_string()),
                variable_bindings: Vec::new(),
                response_path: vec!["projectUpdate".to_string()],
            }),
        );
        let mutation = Capability::new(
            source_id.clone(),
            "graph",
            "mutation_project_update",
            ProviderOrigin {
                kind: ProviderOriginKind::GraphqlRootField,
                snapshot_ref:
                    "interfaces/graph/provider-snapshot.yaml#/root_fields/mutation_project_update"
                        .to_string(),
                provider_name: "projectUpdate".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::Graphql(GraphqlOperationBinding {
                endpoint_ref: "source/src_linear/interface/graph/endpoint/default".to_string(),
                operation_name: "MutationProjectUpdate".to_string(),
                graphql_operation_kind: GraphqlOperationKind::Mutation,
                document_ref:
                    "source/src_linear/interface/graph/generated/mutation_project_update.graphql"
                        .to_string(),
                selection_set: Some("id".to_string()),
                variable_bindings: Vec::new(),
                response_path: vec!["projectUpdate".to_string()],
            }),
        );
        let ctx = BindingBuildContext {
            source_id,
            display_name: "Linear".to_string(),
            source_key: SourceKey("linear_graphql".to_string()),
        };

        let query = TypescriptBindingContributor::new()
            .contribute(&query, &ctx)
            .expect("query contribution");
        let mutation = TypescriptBindingContributor::new()
            .contribute(&mutation, &ctx)
            .expect("mutation contribution");

        let Some(Binding::Typescript(query_binding)) = query.bindings.first() else {
            panic!("expected TypeScript query binding");
        };
        let Some(Binding::Typescript(mutation_binding)) = mutation.bindings.first() else {
            panic!("expected TypeScript mutation binding");
        };
        assert_eq!(
            query_binding.ref_.value,
            "typescript:linearGraphql.graph.query.projectUpdate"
        );
        assert_eq!(
            mutation_binding.ref_.value,
            "typescript:linearGraphql.graph.mutation.projectUpdate"
        );
    }

    #[test]
    fn typescript_contributor_uses_rest_tags_and_provider_operation_id_leaf() {
        let source_id = SourceId("src_datadog".to_string());
        let capability = Capability::new(
            source_id.clone(),
            "rest_v1",
            "validate_monitor",
            ProviderOrigin {
                kind: ProviderOriginKind::RestOperation,
                snapshot_ref:
                    "interfaces/rest_v1/provider-snapshot.yaml#/operations/validate_monitor"
                        .to_string(),
                provider_name: "ValidateMonitor".to_string(),
                tags: vec!["Monitors".to_string()],
            },
            UpstreamBinding::Rest(coral_capabilities::RestUpstreamBinding {
                operation_ref:
                    "interfaces/rest_v1/provider-snapshot.yaml#/operations/validate_monitor"
                        .to_string(),
                method: coral_capabilities::HttpMethod::Get,
                path_template: "/api/v1/monitor/validate".to_string(),
                parameter_bindings: Vec::new(),
                request_bodies: Vec::new(),
                responses: Vec::new(),
                pagination: None,
            }),
        );
        let ctx = BindingBuildContext {
            source_id,
            display_name: "Datadog".to_string(),
            source_key: SourceKey("datadog".to_string()),
        };

        let contribution = TypescriptBindingContributor::new()
            .contribute(&capability, &ctx)
            .expect("rest contribution");

        let Some(Binding::Typescript(binding)) = contribution.bindings.first() else {
            panic!("expected TypeScript binding");
        };
        assert_eq!(
            binding.path,
            ["datadog", "restV1", "monitors", "validateMonitor"]
        );
        assert_eq!(
            binding.ref_.value,
            "typescript:datadog.restV1.monitors.validateMonitor"
        );
    }

    #[test]
    fn typescript_contributor_skips_bare_version_segments_for_untagged_rest_groups() {
        let source_id = SourceId("src_twilio".to_string());
        let capability = Capability::new(
            source_id.clone(),
            "rest",
            "list_accounts",
            ProviderOrigin {
                kind: ProviderOriginKind::RestOperation,
                snapshot_ref: "interfaces/rest/provider-snapshot.yaml#/operations/list_accounts"
                    .to_string(),
                provider_name: "ListAccounts".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::Rest(coral_capabilities::RestUpstreamBinding {
                operation_ref: "interfaces/rest/provider-snapshot.yaml#/operations/list_accounts"
                    .to_string(),
                method: coral_capabilities::HttpMethod::Get,
                path_template: "/2010-04-01/accounts".to_string(),
                parameter_bindings: Vec::new(),
                request_bodies: Vec::new(),
                responses: Vec::new(),
                pagination: None,
            }),
        );
        let ctx = BindingBuildContext {
            source_id,
            display_name: "Twilio".to_string(),
            source_key: SourceKey("twilio".to_string()),
        };

        let contribution = TypescriptBindingContributor::new()
            .contribute(&capability, &ctx)
            .expect("rest contribution");

        let Some(Binding::Typescript(binding)) = contribution.bindings.first() else {
            panic!("expected TypeScript binding");
        };
        assert_eq!(
            binding.ref_.value,
            "typescript:twilio.rest.accounts.listAccounts"
        );
    }

    #[test]
    fn typescript_contributor_skips_prerelease_version_segments_for_untagged_rest_groups() {
        let source_id = SourceId("src_google".to_string());
        let capability = Capability::new(
            source_id.clone(),
            "rest",
            "list_projects",
            ProviderOrigin {
                kind: ProviderOriginKind::RestOperation,
                snapshot_ref: "interfaces/rest/provider-snapshot.yaml#/operations/list_projects"
                    .to_string(),
                provider_name: "ListProjects".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::Rest(coral_capabilities::RestUpstreamBinding {
                operation_ref: "interfaces/rest/provider-snapshot.yaml#/operations/list_projects"
                    .to_string(),
                method: coral_capabilities::HttpMethod::Get,
                path_template: "/api/v1beta1/projects".to_string(),
                parameter_bindings: Vec::new(),
                request_bodies: Vec::new(),
                responses: Vec::new(),
                pagination: None,
            }),
        );
        let ctx = BindingBuildContext {
            source_id,
            display_name: "Google".to_string(),
            source_key: SourceKey("google".to_string()),
        };

        let contribution = TypescriptBindingContributor::new()
            .contribute(&capability, &ctx)
            .expect("rest contribution");

        let Some(Binding::Typescript(binding)) = contribution.bindings.first() else {
            panic!("expected TypeScript binding");
        };
        assert_eq!(
            binding.ref_.value,
            "typescript:google.rest.projects.listProjects"
        );
    }

    #[test]
    fn typescript_contributor_strips_matching_group_from_slash_operation_ids() {
        let source_id = SourceId("src_github".to_string());
        let capability = Capability::new(
            source_id.clone(),
            "rest",
            "agent_tasks_list_tasks",
            ProviderOrigin {
                kind: ProviderOriginKind::RestOperation,
                snapshot_ref:
                    "interfaces/rest/provider-snapshot.yaml#/operations/agent_tasks_list_tasks"
                        .to_string(),
                provider_name: "agent-tasks/list-tasks".to_string(),
                tags: vec!["Agent tasks".to_string()],
            },
            UpstreamBinding::Rest(coral_capabilities::RestUpstreamBinding {
                operation_ref:
                    "interfaces/rest/provider-snapshot.yaml#/operations/agent_tasks_list_tasks"
                        .to_string(),
                method: coral_capabilities::HttpMethod::Get,
                path_template: "/agent-tasks".to_string(),
                parameter_bindings: Vec::new(),
                request_bodies: Vec::new(),
                responses: Vec::new(),
                pagination: None,
            }),
        );
        let ctx = BindingBuildContext {
            source_id,
            display_name: "GitHub".to_string(),
            source_key: SourceKey("github".to_string()),
        };

        let contribution = TypescriptBindingContributor::new()
            .contribute(&capability, &ctx)
            .expect("rest contribution");

        let Some(Binding::Typescript(binding)) = contribution.bindings.first() else {
            panic!("expected TypeScript binding");
        };
        assert_eq!(
            binding.ref_.value,
            "typescript:github.rest.agentTasks.listTasks"
        );
    }

    #[test]
    fn typescript_contributor_keeps_mcp_interface_segment() {
        let source_id = SourceId("src_slack".to_string());
        let capability = Capability::new(
            source_id.clone(),
            "mcp",
            "slack_search_public",
            ProviderOrigin {
                kind: ProviderOriginKind::McpTool,
                snapshot_ref: "interfaces/mcp/provider-snapshot.yaml#/tools/slack_search_public"
                    .to_string(),
                provider_name: "slackSearchPublic".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::McpTool(McpToolUpstreamBinding {
                server_ref: "source/src_slack/interface/mcp/server/default".to_string(),
                tool_name: "slackSearchPublic".to_string(),
                task_support: McpTaskSupport::Forbidden,
            }),
        );
        let ctx = BindingBuildContext {
            source_id,
            display_name: "Slack".to_string(),
            source_key: SourceKey("slack".to_string()),
        };

        let contribution = TypescriptBindingContributor::new()
            .contribute(&capability, &ctx)
            .expect("mcp contribution");

        let Some(Binding::Typescript(binding)) = contribution.bindings.first() else {
            panic!("expected TypeScript binding");
        };
        assert_eq!(binding.path, ["slack", "mcp", "slackSearchPublic"]);
        assert_eq!(binding.ref_.value, "typescript:slack.mcp.slackSearchPublic");
    }
}
