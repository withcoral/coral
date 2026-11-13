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
use coral_embed::MANIFEST;
use coral_policy::ExactToolRules;
use coral_provider::{ActiveProvider, ProviderId};
use coral_telemetry::safe;
use coral_workspace::WorkspaceId;

use super::plan::QueryPlan;
use super::repository;
use super::vector_list::{self, VectorList};
use super::{
    Answer, EntityRef, EntityRetriever, Hydrated, LexicalReason, Listing, QueryError, QueryRequest,
    RequestContext, RetrievalError, Semantic, ToolRef, WindowEntry, count,
};
use crate::eligibility::tool_approval;
use crate::hit::{HitMaterial, hit_text};
use crate::signature::Callable;
use crate::{
    CallPath, EntityKind, Error, ExactAuthoringDescription, ProviderCallRoot, ProviderFilter,
    RetrieverCoverage, SearchHit, SearchableProvider, ToolAuthoringDescription,
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
        provider_filter: &ProviderFilter,
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
                semantic: Some(Semantic::of(&request.vector)),
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
        let named = named_providers(&plan, &searched);
        let provider_ids = searched.keys().cloned().collect::<Vec<_>>();
        let generation_ids = searched
            .values()
            .map(|provider| provider.generation_id().clone())
            .collect::<Vec<_>>();
        let scope = repository::ToolScope {
            providers: &provider_ids,
            generations: &generation_ids,
            excluded: &excluded,
            named: &named,
        };
        let (lexical, vector) =
            lexical_and_vector_lists(transaction, context.workspace_id, &scope, &plan, request)
                .await?;
        let semantic = Semantic::of(&vector);
        let (merged, has_more) = vector_list::merged_window(
            lexical,
            vector.ok(),
            |tool: &repository::RankedTool| tool.doc_id.clone(),
            request.window,
        );
        let mut window = Vec::new();
        for entry in merged {
            let strong_by_vector = entry.strong_by_vector();
            let tool = entry.entry;
            let provider = searched
                .get(&tool.provider_id)
                .ok_or(QueryError::Retriever(RetrievalError::Corrupt(
                    "a ranked Tool's Provider was not searched",
                )))?;
            window.push(WindowEntry {
                strong: strong_by_vector
                    || strong_tool(&plan, &tool.matched, named.contains(&tool.provider_id)),
                entity: EntityRef::Tool(ToolRef {
                    provider: Arc::clone(provider),
                    tool_key: tool.tool_key,
                    rules: Arc::clone(&rules),
                    call_path: tool.call_path,
                }),
            });
        }
        Ok(Answer {
            coverage: RetrieverCoverage::new(
                count(eligible),
                count(searched.len()),
                count(window.len()),
                has_more,
            ),
            window,
            semantic: Some(semantic),
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
        let material = match coral_provider::tool_material_in_transaction(
            transaction,
            &tool.provider,
            &tool.tool_key,
        )
        .await
        {
            Ok(Some(material)) => material,
            Ok(None) => return Ok(Hydrated::Dropped),
            // A corrupt Tool leaves the page; the others stay.
            Err(
                coral_provider::Error::CorruptData(_)
                | coral_provider::Error::Catalog(coral_catalog::Error::CorruptData(_)),
            ) => return Ok(Hydrated::Unreadable),
            Err(error) => return Err(error.into()),
        };
        let Some(description) =
            describe_material(context, &material, &tool.rules, &tool.call_path)?
        else {
            return Ok(Hydrated::Dropped);
        };
        let text = hit_text(&HitMaterial {
            declaration: description.typescript_declaration(),
            callable: Callable::Tool {
                leaf: description.call_path().leaf(),
            },
            description: description.description(),
        });
        Ok(Hydrated::Hit(Box::new(SearchHit::from_tool(
            &description,
            text,
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

/// The lexical list, with one row past the window, and the vector list, or
/// why the vector side gave none.
async fn lexical_and_vector_lists(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    scope: &repository::ToolScope<'_>,
    plan: &QueryPlan,
    request: &QueryRequest<'_>,
) -> Result<
    (
        Vec<repository::RankedTool>,
        Result<VectorList<repository::RankedTool>, LexicalReason>,
    ),
    QueryError,
> {
    if scope.providers.is_empty() {
        // No Provider is searched, so neither list can hold a Tool.
        let nothing = request.vector.map(|_vector| VectorList {
            entries: Vec::new(),
            has_more: false,
        });
        return Ok((Vec::new(), nothing));
    }
    let lexical =
        repository::rank_tools(transaction, workspace_id, scope, plan, request.window + 1)
            .await
            .map_err(QueryError::Retriever)?;
    let vector = match request.vector {
        Ok(vector) => {
            vector_side(
                transaction,
                workspace_id,
                scope,
                plan,
                vector,
                request.window,
            )
            .await?
        }
        Err(reason) => Err(reason),
    };
    Ok((lexical, vector))
}

/// The vector list, read in its own savepoint. When the vector side fails,
/// the savepoint rolls back, the retriever ranks by words alone, and the
/// page does not say so.
async fn vector_side(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    scope: &repository::ToolScope<'_>,
    plan: &QueryPlan,
    vector: &[f32],
    window: usize,
) -> Result<Result<VectorList<repository::RankedTool>, LexicalReason>, QueryError> {
    repository::begin_vector_query(transaction)
        .await
        .map_err(QueryError::retrieval)?;
    match nearest_current_tools(transaction, workspace_id, scope, plan, vector, window).await {
        Ok(tools) => {
            repository::end_vector_query(transaction)
                .await
                .map_err(QueryError::retrieval)?;
            Ok(Ok(tools))
        }
        Err(error) => {
            repository::abandon_vector_query(transaction)
                .await
                .map_err(QueryError::retrieval)?;
            tracing::warn!(
                coral.workspace.id = safe::id(workspace_id),
                error.type = safe::kind(error.code()),
                "the Tool retriever's vector query failed; it ranks by words alone"
            );
            Ok(Err(LexicalReason::VectorQueryFailed))
        }
    }
}

/// The vector list: the `window` Tools of the scope whose vectors of the
/// current Embedder revision are nearest to `vector`, less each whose vector
/// was made from other text than its row holds now.
async fn nearest_current_tools(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    scope: &repository::ToolScope<'_>,
    plan: &QueryPlan,
    vector: &[f32],
    window: usize,
) -> Result<VectorList<repository::RankedTool>, RetrievalError> {
    let revision = MANIFEST.embedder_revision();
    let mut nearest = repository::nearest_tools(
        transaction,
        workspace_id,
        scope,
        revision.as_str(),
        vector,
        window + 1,
    )
    .await?;
    let has_more = nearest.len() > window;
    nearest.truncate(window);
    let doc_ids = nearest
        .iter()
        .map(|vector| vector.doc_id.as_str())
        .collect::<Vec<_>>();
    let rows = repository::current_tools(transaction, workspace_id, plan, &doc_ids).await?;
    Ok(VectorList {
        entries: vector_list::keep_current(nearest, rows),
        has_more,
    })
}

/// The Postgres plan of the vector query over the Tools of `providers`'
/// `generations`, less `excluded`, one line per row.
#[cfg(test)]
pub(crate) async fn explain_vector_query(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    providers: &[ProviderId],
    generations: &[coral_catalog::ProviderGenerationId],
    excluded: &[(ProviderId, coral_catalog::ToolKey)],
) -> Result<Vec<String>, coral_db::Error> {
    let vector = vec![0.5; MANIFEST.dimension];
    repository::explain_nearest_tools(
        transaction,
        workspace_id,
        &repository::ToolScope {
            providers,
            generations,
            excluded,
            named: &[],
        },
        MANIFEST.embedder_revision().as_str(),
        &vector,
    )
    .await
}

/// Whether a ranked Tool is a strong match, by the Tool retriever's own
/// rule: a field holds the query's words in order, the query names its
/// Provider, or its name and title start words with at least half of the
/// query's content words, as the statement's name score counts prefixes. So
/// a Tool of an unnamed Provider whose name and title start fewer than half
/// of those words, and that holds them in order in no field, is weak however
/// often its description, schema text or a near-spelling meets the query.
fn strong_tool(plan: &QueryPlan, matched: &repository::RowMatch, named: bool) -> bool {
    let words = plan.content_word_count();
    matched.phrase_points > 0
        || named
        || (words > 0
            && 2 * plan.content_words_starting([matched.name.as_str(), matched.title.as_str()])
                >= words)
}

/// The searched Providers the query names. Their Tools rank first within
/// their phrase points and are strong matches, and no Tool is filtered out.
/// With a Provider filter only that Provider is searched, so naming it
/// changes no Tool's order, only which Tools are strong.
fn named_providers(
    plan: &QueryPlan,
    searched: &HashMap<ProviderId, Arc<ActiveProvider>>,
) -> Vec<ProviderId> {
    searched
        .iter()
        .filter(|(_id, provider)| {
            let provider = provider.provider();
            plan.names_provider(
                provider
                    .call_root()
                    .namespace()
                    .map(coral_provider::ProviderNamespace::as_str),
                provider.display_name(),
            )
        })
        .map(|(id, _provider)| id.clone())
        .collect()
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
        match call_root {
            ProviderCallRoot::Owner(_) | ProviderCallRoot::Builtin => {}
            // No Generation holds a Private or Shared Tool: coral-tools
            // keeps them, and their own retriever describes them.
            ProviderCallRoot::Private | ProviderCallRoot::Shared => continue,
        }
        if !providers.contains_key(&call_root) {
            let provider =
                visible_providers(transaction, context, &ProviderFilter::one(&call_root))
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
        &visible_providers(transaction, context, &ProviderFilter::default()).await?,
    ))
}

/// The Workspace's Active Providers that are ready for the caller, by
/// namespace: what a Provider filter may name, and what each is called.
pub(crate) async fn searchable_providers(
    transaction: &mut Transaction<'_>,
    context: RequestContext<'_>,
) -> Result<Vec<SearchableProvider>, Error> {
    let mut searchable = visible_providers(transaction, context, &ProviderFilter::default())
        .await?
        .into_iter()
        .map(|active| {
            SearchableProvider::new(
                active.provider().call_root().clone(),
                active.provider().display_name().to_owned(),
            )
        })
        .collect::<Vec<_>>();
    searchable.sort_by(|left, right| left.namespace().cmp(right.namespace()));
    searchable.dedup_by(|left, right| left.call_root() == right.call_root());
    Ok(searchable)
}

/// The Providers a search's filter leaves, all visible ones without a filter.
/// A filter reads only the Providers it names, so another Provider's state
/// cannot fail the search. A filter with a value that names no visible
/// Provider is refused with the Call Roots the caller can search, so a wrong
/// guess never reads as an empty catalog or a narrower one.
async fn filtered_providers(
    transaction: &mut Transaction<'_>,
    context: RequestContext<'_>,
    provider_filter: &ProviderFilter,
) -> Result<Vec<ActiveProvider>, Error> {
    let filtered = visible_providers(transaction, context, provider_filter).await?;
    let unmatched = provider_filter.unmatched(
        &filtered
            .iter()
            .map(|provider| provider.provider().call_root())
            .collect::<Vec<_>>(),
    );
    if !unmatched.is_empty() {
        return Err(Error::UnknownProviderCallRoot {
            searchable: searchable_call_roots(transaction, context).await?,
            unmatched,
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
    provider_filter: &ProviderFilter,
) -> Result<Vec<ActiveProvider>, Error> {
    let mut visible = Vec::new();
    for provider in
        coral_provider::active_providers_in_transaction(transaction, context.workspace_id).await?
    {
        if !provider_filter.admits(provider.provider().call_root()) {
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
        material.policy_facts(),
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
