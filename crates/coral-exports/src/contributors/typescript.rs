use coral_capabilities::{
    Capability, GraphqlOperationKind, ProviderOriginKind, SupportStatus, UpstreamBinding,
};

use crate::exports::{
    Binding, BindingBuildContext, BindingContribution, BindingContributor, ExportRef,
    TypescriptBinding,
};
use crate::paths::{identifier_segment, pascal_segment};

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
    let operation_segments = rest_operation_id_path_segments(&capability.operation_id);
    if operation_segments.len() > 1 {
        return operation_segments;
    }
    rest_provider_path_segments(&capability.provider_origin.provider_name)
}

fn rest_operation_id_path_segments(operation_id: &str) -> Vec<String> {
    let mut segments = operation_id.splitn(2, '_');
    let Some(group) = segments.next().filter(|segment| !segment.is_empty()) else {
        return Vec::new();
    };
    let Some(action) = segments.next().filter(|segment| !segment.is_empty()) else {
        return vec![identifier_segment(group)];
    };
    vec![identifier_segment(group), identifier_segment(action)]
}

fn rest_provider_path_segments(provider_name: &str) -> Vec<String> {
    if let Some((group, action)) = provider_name.split_once('/')
        && !group.is_empty()
        && !action.is_empty()
    {
        return vec![identifier_segment(group), identifier_segment(action)];
    }
    let Some((group, action)) = split_camel_group_action(provider_name) else {
        return vec![identifier_segment(provider_name)];
    };
    vec![identifier_segment(group), identifier_segment(action)]
}

fn graphql_operation_path_segments(capability: &Capability) -> Vec<String> {
    let operation_kind = match &capability.upstream_binding {
        UpstreamBinding::Graphql(binding) => match binding.graphql_operation_kind {
            GraphqlOperationKind::Query => Some("query"),
            GraphqlOperationKind::Mutation => Some("mutation"),
            GraphqlOperationKind::Subscription => Some("subscription"),
        },
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

fn split_camel_group_action(value: &str) -> Option<(&str, &str)> {
    let bytes = value.as_bytes();
    for index in 1..bytes.len() {
        let previous = bytes.get(index.wrapping_sub(1)).copied()?;
        let current = bytes.get(index).copied()?;
        let next = bytes.get(index + 1).copied();
        let split_before_upper_after_lower =
            previous.is_ascii_lowercase() && current.is_ascii_uppercase();
        let split_acronym_before_word = previous.is_ascii_uppercase()
            && current.is_ascii_uppercase()
            && next.is_some_and(|next| next.is_ascii_lowercase());
        if split_before_upper_after_lower || split_acronym_before_word {
            let (group, action) = value.split_at(index);
            if !group.is_empty() && !action.is_empty() {
                return Some((group, action));
            }
        }
    }
    None
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
    fn typescript_contributor_splits_rest_group_and_action_segments() {
        let source_id = SourceId("src_github".to_string());
        let capability = Capability::new(
            source_id.clone(),
            "rest",
            "users_get_authenticated",
            ProviderOrigin {
                kind: ProviderOriginKind::RestOperation,
                snapshot_ref:
                    "interfaces/rest/provider-snapshot.yaml#/operations/users_get_authenticated"
                        .to_string(),
                provider_name: "users/get-authenticated".to_string(),
            },
            UpstreamBinding::Rest(coral_capabilities::RestUpstreamBinding {
                operation_ref:
                    "interfaces/rest/provider-snapshot.yaml#/operations/users_get_authenticated"
                        .to_string(),
                method: coral_capabilities::HttpMethod::Get,
                path_template: "/user".to_string(),
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
            binding.path,
            ["github", "rest", "users", "getAuthenticated"]
        );
        assert_eq!(
            binding.ref_.value,
            "typescript:github.rest.users.getAuthenticated"
        );
    }

    #[test]
    fn typescript_contributor_uses_unique_rest_operation_id_suffixes() {
        let source_id = SourceId("src_github".to_string());
        let first = Capability::new(
            source_id.clone(),
            "rest",
            "pulls_list_reviews",
            ProviderOrigin {
                kind: ProviderOriginKind::RestOperation,
                snapshot_ref:
                    "interfaces/rest/provider-snapshot.yaml#/operations/pulls_list_reviews"
                        .to_string(),
                provider_name: "pulls/list-reviews".to_string(),
            },
            UpstreamBinding::Rest(coral_capabilities::RestUpstreamBinding {
                operation_ref:
                    "interfaces/rest/provider-snapshot.yaml#/operations/pulls_list_reviews"
                        .to_string(),
                method: coral_capabilities::HttpMethod::Get,
                path_template: "/repos/{owner}/{repo}/pulls/{pull_number}/reviews".to_string(),
                parameter_bindings: Vec::new(),
                request_bodies: Vec::new(),
                responses: Vec::new(),
                pagination: None,
            }),
        );
        let second = Capability::new(
            source_id.clone(),
            "rest",
            "pulls_list_reviews_2",
            ProviderOrigin {
                kind: ProviderOriginKind::RestOperation,
                snapshot_ref:
                    "interfaces/rest/provider-snapshot.yaml#/operations/pulls_list_reviews_2"
                        .to_string(),
                provider_name: "pullsListReviews".to_string(),
            },
            UpstreamBinding::Rest(coral_capabilities::RestUpstreamBinding {
                operation_ref:
                    "interfaces/rest/provider-snapshot.yaml#/operations/pulls_list_reviews_2"
                        .to_string(),
                method: coral_capabilities::HttpMethod::Get,
                path_template: "/repos/{owner}/{repo}/pulls/{pull_number}/reviews".to_string(),
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

        let first = TypescriptBindingContributor::new()
            .contribute(&first, &ctx)
            .expect("first contribution");
        let second = TypescriptBindingContributor::new()
            .contribute(&second, &ctx)
            .expect("second contribution");

        let Some(Binding::Typescript(first_binding)) = first.bindings.first() else {
            panic!("expected first TypeScript binding");
        };
        let Some(Binding::Typescript(second_binding)) = second.bindings.first() else {
            panic!("expected second TypeScript binding");
        };
        assert_eq!(
            first_binding.ref_.value,
            "typescript:github.rest.pulls.listReviews"
        );
        assert_eq!(
            second_binding.ref_.value,
            "typescript:github.rest.pulls.listReviews2"
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
