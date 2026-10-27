//! The Tool retriever: Tools of the Workspace's visible, ready
//! Providers, ranked inside the database.
//!
//! Visibility stays with the owners. coral-provider decides whether a
//! Provider is ready for the caller; coral-policy decides whether an
//! Tool is disabled or needs approval. The retriever asks them once per
//! request and loads no Tool catalog before ranking: catalog reads happen
//! at hydration and in `describe`, for single Tools.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use coral_db::Transaction;
use coral_policy::ExactToolRules;
use coral_provider::ActiveProvider;

use super::plan::QueryPlan;
use super::repository;
use super::{
    Answer, EntityRef, EntityRetriever, Hydrated, Listing, QueryError, QueryRequest,
    RequestContext, RetrievalError, ToolRef, count,
};
use crate::eligibility::tool_approval;
use crate::evidence::{EvidenceBuilder, EvidenceTerms};
use crate::hit::{HitMaterial, hit_text};
use crate::signature::Callable;
use crate::{
    CallPath, EntityKind, Error, ExactAuthoringDescription, ProviderCallRoot, RetrieverCoverage,
    SearchEvidence, SearchHit, ToolAuthoringDescription,
};

/// Ranks, hydrates, and describes Tools.
#[derive(Debug, Default)]
pub(crate) struct ToolRetriever {
    /// Makes the ranked statement fail, for tests of a retriever error.
    #[cfg(test)]
    pub(crate) fail_ranking: bool,
}

#[async_trait]
impl EntityRetriever for ToolRetriever {
    fn kind(&self) -> EntityKind {
        EntityKind::ProviderTool
    }

    async fn list(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        provider_filter: Option<&ProviderCallRoot>,
    ) -> Result<Listing, Error> {
        let providers = filtered_providers(transaction, context, provider_filter).await?;
        let units = providers.len();
        let rules = Arc::new(
            coral_policy::exact_tool_rules_in_transaction(transaction, context.workspace_id)
                .await?,
        );
        let mut listed = Vec::new();
        for provider in providers {
            let targets = coral_catalog::available_search_targets_in_transaction(
                transaction,
                provider.generation_id(),
            )
            .await
            .map_err(coral_provider::Error::from)?;
            let provider = Arc::new(provider);
            for target in targets {
                if !coral_provider::reaches(
                    provider.provider().call_root(),
                    target.key(),
                    context.authority.effective_user_role(),
                ) {
                    continue;
                }
                if rules.disables(provider.provider().id(), &target) {
                    continue;
                }
                listed.push(EntityRef::Tool(ToolRef {
                    call_path: CallPath::tool(
                        provider.provider().call_root(),
                        target.relative_path(),
                    )?,
                    provider: Arc::clone(&provider),
                    tool_key: target.key().clone(),
                    rules: Arc::clone(&rules),
                    near_spellings: Arc::new([]),
                }));
            }
        }
        Ok(Listing {
            entities: listed,
            units,
        })
    }

    async fn query(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        request: &QueryRequest<'_>,
    ) -> Result<Answer, QueryError> {
        let providers = filtered_providers(transaction, context, request.provider_filter).await?;
        let rules = Arc::new(
            coral_policy::exact_tool_rules_in_transaction(transaction, context.workspace_id)
                .await
                .map_err(Error::from)?,
        );
        let eligible = providers.len();
        let Some(plan) = QueryPlan::build(request.terms) else {
            // No term can select anything, so no unit waits on the index.
            return Ok(Answer {
                window: Vec::new(),
                coverage: RetrieverCoverage::new(count(eligible), count(eligible), 0, false),
            });
        };
        let complete = repository::complete_generations(transaction, context.workspace_id)
            .await
            .map_err(QueryError::retrieval)?;
        // A Provider whose active Generation has no complete index state is
        // not searched: not indexed yet, or its attempt failed.
        let searched = providers
            .into_iter()
            .filter(|provider| complete.contains(&provider.generation_id().to_string()))
            .map(|provider| (provider.provider().id().clone(), Arc::new(provider)))
            .collect::<HashMap<_, _>>();

        #[cfg(test)]
        if self.fail_ranking {
            repository::failing_statement(transaction)
                .await
                .map_err(QueryError::retrieval)?;
        }

        // Ranking leaves out what policy disables and what the caller does not reach.
        let mut excluded = rules
            .disabled()
            .map(|(provider, key)| (provider.clone(), key.clone()))
            .collect::<Vec<_>>();
        for provider in searched.values() {
            excluded.extend(
                coral_provider::unreached_tools_in_transaction(
                    transaction,
                    provider,
                    context.authority.effective_user_role(),
                )
                .await
                .map_err(Error::from)?
                .into_iter()
                .map(|key| (provider.provider().id().clone(), key)),
            );
        }
        let ranked = if searched.is_empty() {
            repository::Ranking {
                entities: Vec::new(),
                near_spellings: Vec::new(),
            }
        } else {
            let provider_ids = searched.keys().cloned().collect::<Vec<_>>();
            let generation_ids = searched
                .values()
                .map(|provider| provider.generation_id().clone())
                .collect::<Vec<_>>();
            repository::rank_tools(
                transaction,
                context.workspace_id,
                &provider_ids,
                &generation_ids,
                &excluded,
                &plan,
                request.window + 1,
            )
            .await
            .map_err(QueryError::Retriever)?
        };
        let has_more = ranked.entities.len() > request.window;
        let near_spellings: Arc<[String]> = ranked.near_spellings.into();
        let mut window = Vec::new();
        for tool in ranked.entities.into_iter().take(request.window) {
            let provider = searched
                .get(&tool.provider_id)
                .ok_or(QueryError::Retriever(RetrievalError::Corrupt(
                    "a ranked Tool's Provider was not searched",
                )))?;
            window.push(EntityRef::Tool(ToolRef {
                provider: Arc::clone(provider),
                tool_key: tool.tool_key,
                rules: Arc::clone(&rules),
                call_path: tool.call_path,
                near_spellings: Arc::clone(&near_spellings),
            }));
        }
        Ok(Answer {
            coverage: RetrieverCoverage::new(
                count(eligible),
                count(searched.len()),
                count(window.len()),
                has_more,
            ),
            window,
        })
    }

    async fn hydrate(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        entity: &EntityRef,
    ) -> Result<Hydrated, Error> {
        let EntityRef::Tool(tool) = entity else {
            return Ok(Hydrated::Dropped);
        };
        let hydrated = match coral_provider::hydrate_tool_in_transaction(
            transaction,
            context.loaded,
            &tool.provider,
            &tool.tool_key,
        )
        .await
        {
            Ok(Some(hydrated)) => hydrated,
            Ok(None) => return Ok(Hydrated::Dropped),
            // A corrupt Tool leaves the page; the others stay.
            Err(
                coral_provider::Error::CorruptData(_)
                | coral_provider::Error::Catalog(coral_catalog::Error::CorruptData(_)),
            ) => return Ok(Hydrated::Unreadable),
            Err(error) => return Err(error.into()),
        };
        let Some(description) =
            describe_material(context, hydrated.material(), &tool.rules, &tool.call_path)?
        else {
            return Ok(Hydrated::Dropped);
        };
        let material = HitMaterial {
            declaration: description.typescript_declaration(),
            callable: Callable::Tool {
                leaf: description.call_path().leaf(),
            },
            description: description.description(),
            guidance: description.guidance(),
            tags: description.tags(),
        };
        let Some(terms) = context.terms else {
            return Ok(Hydrated::Hit(Box::new(SearchHit::from_tool(
                &description,
                hit_text(&material, None),
                SearchEvidence::default(),
            ))));
        };
        let terms = EvidenceTerms::new(terms, &tool.near_spellings);
        let mut evidence = EvidenceBuilder::new(&terms);
        evidence
            .field("call_path", [description.call_path().as_str()])
            .field(
                "provider_namespace",
                [description.provider_call_root().name()],
            )
            .field(
                "provider_display_name",
                [description.provider_display_name()],
            )
            .field("title", description.title())
            .field("description", [description.description()])
            .tags(description.tags().iter().map(String::as_str))
            .field("guidance", description.guidance());
        for field in hydrated.declaration_fields() {
            evidence.path(field.path().as_str(), field.text());
        }
        let Some(matched) = evidence.build() else {
            tracing::debug!(
                call_path = description.call_path().as_str(),
                "a ranked Tool has no evidence and leaves the page"
            );
            return Ok(Hydrated::Dropped);
        };
        let text = hit_text(&material, Some((&terms, &matched)));
        Ok(Hydrated::Hit(Box::new(SearchHit::from_tool(
            &description,
            text,
            matched.evidence,
        ))))
    }

    async fn describe(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        call_path: &CallPath,
    ) -> Result<Option<ExactAuthoringDescription>, Error> {
        Ok(
            describe_tools(transaction, context, std::slice::from_ref(call_path))
                .await?
                .pop()
                .map(ExactAuthoringDescription::tool),
        )
    }
}

/// The complete description of each Tool in `call_paths` that the
/// caller can describe, in order. Every other path is omitted. One policy read
/// and one visibility check per Call Root serve the whole batch.
pub(crate) async fn describe_tools(
    transaction: &mut Transaction<'_>,
    context: RequestContext<'_>,
    call_paths: &[CallPath],
) -> Result<Vec<ToolAuthoringDescription>, Error> {
    let rules =
        coral_policy::exact_tool_rules_in_transaction(transaction, context.workspace_id).await?;
    let mut providers = HashMap::new();
    let mut descriptions = Vec::new();
    for call_path in call_paths {
        let Some((call_root, relative_path)) =
            ProviderCallRoot::split_call_path(call_path.as_str())
        else {
            continue;
        };
        if !providers.contains_key(&call_root) {
            let provider = visible_providers(transaction, context, Some(&call_root))
                .await?
                .into_iter()
                .next();
            providers.insert(call_root.clone(), provider);
        }
        let Some(Some(provider)) = providers.get(&call_root) else {
            continue;
        };
        let Some(material) = coral_provider::tool_material_by_relative_path_in_transaction(
            transaction,
            provider,
            relative_path,
        )
        .await?
        else {
            continue;
        };
        descriptions.extend(describe_material(context, &material, &rules, call_path)?);
    }
    Ok(descriptions)
}

/// The Call Roots of the Workspace's Active Providers that are ready for the
/// caller, in order: what a Provider filter may name.
pub(crate) async fn searchable_call_roots(
    transaction: &mut Transaction<'_>,
    context: RequestContext<'_>,
) -> Result<Vec<ProviderCallRoot>, Error> {
    Ok(call_roots(
        &visible_providers(transaction, context, None).await?,
    ))
}

/// The Providers a search's filter leaves, all visible ones without a filter.
/// A filter reads only the Provider it names, so another Provider's state
/// cannot fail the search. A filter that names no visible Provider is refused
/// with the Call Roots the caller can search, so a wrong guess never reads as
/// an empty catalog.
async fn filtered_providers(
    transaction: &mut Transaction<'_>,
    context: RequestContext<'_>,
    provider_filter: Option<&ProviderCallRoot>,
) -> Result<Vec<ActiveProvider>, Error> {
    let filtered = visible_providers(transaction, context, provider_filter).await?;
    if provider_filter.is_some() && filtered.is_empty() {
        return Err(Error::UnknownProviderCallRoot {
            searchable: searchable_call_roots(transaction, context).await?,
        });
    }
    Ok(filtered)
}

fn call_roots(providers: &[ActiveProvider]) -> Vec<ProviderCallRoot> {
    let mut call_roots = providers
        .iter()
        .map(|provider| provider.provider().call_root().clone())
        .collect::<Vec<_>>();
    call_roots.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    call_roots.dedup();
    call_roots
}

/// The Workspace's Active Providers that are ready for the caller, cut to the
/// Call Root filter. Reads no Tool catalog, apart from the Built-in
/// Provider's Tool identities that reach is decided on.
async fn visible_providers(
    transaction: &mut Transaction<'_>,
    context: RequestContext<'_>,
    provider_filter: Option<&ProviderCallRoot>,
) -> Result<Vec<ActiveProvider>, Error> {
    let mut visible = Vec::new();
    for provider in
        coral_provider::active_providers_in_transaction(transaction, context.workspace_id).await?
    {
        if provider_filter.is_some_and(|root| provider.provider().call_root() != root) {
            continue;
        }
        if coral_provider::account::discoverable_in_transaction(
            transaction,
            &provider,
            context.actor.principal(),
            context.authority,
        )
        .await?
        {
            visible.push(provider);
        }
    }
    Ok(visible)
}

/// The complete description of one current Tool, or `None` when the
/// caller does not reach it, policy disables it, or its path is not the
/// expected one.
fn describe_material(
    context: RequestContext<'_>,
    material: &coral_provider::ToolAuthoringMaterial,
    rules: &ExactToolRules,
    call_path: &CallPath,
) -> Result<Option<ToolAuthoringDescription>, Error> {
    if CallPath::tool(material.provider_call_root(), material.relative_path())? != *call_path {
        return Ok(None);
    }
    if !coral_provider::reaches(
        material.provider_call_root(),
        material.tool_key(),
        context.authority.effective_user_role(),
    ) {
        return Ok(None);
    }
    let Some(approval) = tool_approval(
        rules,
        material.provider_id(),
        material.tool_key(),
        material.hints(),
        material.http_method(),
    ) else {
        return Ok(None);
    };
    Ok(Some(ToolAuthoringDescription::new(
        call_path.clone(),
        material.typescript_declaration().to_string(),
        material.provider_call_root().clone(),
        material.provider_id().clone(),
        material.provider_display_name().to_string(),
        material.presentation().clone(),
        material.hints(),
        approval,
    )))
}
