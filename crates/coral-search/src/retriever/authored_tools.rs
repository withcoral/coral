//! The Authored Tool retriever: one kind of Authored Tool the requesting
//! Principal can discover, ranked inside the database. Universal Search
//! runs one for the Principal's own Private Tools and one for the
//! Workspace's Shared Tools.
//!
//! coral-tools decides who discovers an Authored Tool: a Private Tool is
//! visible to its owner only, and a live Shared Tool to every Principal of
//! its Workspace. The statement holds the first as a condition on the row's
//! own Principal columns; a Shared Tool's row names no Principal.

use std::collections::HashMap;

use async_trait::async_trait;
use coral_catalog::AuthoredToolKind;
use coral_db::Transaction;
use coral_tools::AuthoredToolSearchMaterial;

use super::plan::QueryPlan;
use super::repository;
use super::{
    Answer, AuthoredToolRef, EntityRef, EntityRetriever, Hydrated, Listing, QueryError,
    QueryRequest, RequestContext, WindowEntry, count,
};
use crate::hit::{HitMaterial, hit_text};
use crate::signature::Callable;
use crate::{
    ApprovalRequirement, AuthoredToolAuthoringDescription, CallPath, EntityKind, Error,
    ExactAuthoringDescription, ProviderFilter, RetrieverCoverage, SearchHit,
};

/// Ranks, hydrates, and describes the Authored Tools of one kind.
#[derive(Debug)]
pub(crate) struct AuthoredToolRetriever {
    pub(crate) kind: AuthoredToolKind,
    /// Makes the ranked statement fail, for tests of a retriever error.
    #[cfg(test)]
    pub(crate) fail_ranking: bool,
}

impl AuthoredToolRetriever {
    /// The retriever of the Authored Tools of `kind`.
    pub(crate) const fn new(kind: AuthoredToolKind) -> Self {
        Self {
            kind,
            #[cfg(test)]
            fail_ranking: false,
        }
    }
}

#[async_trait]
impl EntityRetriever for AuthoredToolRetriever {
    fn kind(&self) -> EntityKind {
        EntityKind::authored(self.kind)
    }

    // TODO: a Provider filter could keep only the Authored Tools that call
    // the named Providers. Until then the filter narrows Tools only.
    async fn list(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        _provider_filter: &ProviderFilter,
    ) -> Result<Listing, Error> {
        let entities = self
            .current(transaction, context)
            .await?
            .into_iter()
            .map(authored_tool_ref)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Listing {
            units: entities.len(),
            entities,
        })
    }

    async fn query(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        request: &QueryRequest<'_>,
    ) -> Result<Answer, QueryError> {
        let current = self
            .current(transaction, context)
            .await?
            .into_iter()
            .map(|tool| (tool.id().clone(), tool))
            .collect::<HashMap<_, _>>();
        let Some(plan) = QueryPlan::build(request.terms) else {
            // No term can select anything, so no unit waits on the index.
            let eligible = count(current.len());
            return Ok(Answer {
                window: Vec::new(),
                coverage: RetrieverCoverage::new(eligible, eligible, 0, false),
                semantic: None,
            });
        };
        let scope = repository::AuthoredToolScope {
            kind: self.kind,
            principal: context.actor.principal(),
        };
        let indexed = repository::indexed_authored_tools(transaction, context.workspace_id, scope)
            .await
            .map_err(QueryError::retrieval)?;
        let searched = current
            .keys()
            .filter(|id| indexed.contains(&id.to_string()))
            .count();

        #[cfg(test)]
        if self.fail_ranking {
            repository::failing_statement(transaction)
                .await
                .map_err(QueryError::retrieval)?;
        }

        let ranked = if searched > 0 {
            repository::rank_authored_tools(
                transaction,
                context.workspace_id,
                scope,
                &plan,
                request.window + 1,
            )
            .await
            .map_err(QueryError::Retriever)?
        } else {
            Vec::new()
        };
        let has_more = ranked.len() > request.window;
        let mut window = Vec::new();
        for ranked in ranked.into_iter().take(request.window) {
            // A delete or a removal leaves the Tool's row for a later
            // write, so only what the caller discovers now may answer.
            let Some(material) = current.get(&ranked.id) else {
                continue;
            };
            window.push(WindowEntry {
                entity: EntityRef::AuthoredTool(AuthoredToolRef {
                    material: material.clone(),
                    call_path: ranked.call_path,
                }),
                strong: strong_authored_tool(&plan, &ranked.matched),
            });
        }
        Ok(Answer {
            coverage: RetrieverCoverage::new(
                count(current.len()),
                count(searched),
                count(window.len()),
                has_more,
            ),
            window,
            semantic: None,
        })
    }

    async fn hydrate(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        entity: &EntityRef,
    ) -> Result<Hydrated, Error> {
        let EntityRef::AuthoredTool(tool) = entity else {
            return Ok(Hydrated::Dropped);
        };
        if tool.material.id().kind() != self.kind {
            return Ok(Hydrated::Dropped);
        }
        let material = match coral_tools::hydrate_authored_tool_search_material_in_transaction(
            transaction,
            context.workspace_id,
            context.actor.principal(),
            &tool.material,
        )
        .await
        {
            Ok(Some(material)) => material,
            Ok(None) => return Ok(Hydrated::Dropped),
            // A corrupt Tool leaves the page; the others stay.
            Err(coral_tools::Error::CorruptData(_)) => return Ok(Hydrated::Unreadable),
            Err(error) => return Err(error.into()),
        };
        let text = hit_text(&HitMaterial {
            declaration: material.declaration().as_str(),
            callable: Callable::AuthoredTool {
                leaf: tool.call_path.leaf(),
            },
            description: material.description().as_str(),
        });
        Ok(Hydrated::Hit(Box::new(SearchHit::from_authored_tool(
            tool.call_path.clone(),
            text,
        ))))
    }

    async fn describe(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        call_path: &CallPath,
    ) -> Result<Option<ExactAuthoringDescription>, Error> {
        let Some(path) = call_path.authored_tool_path(self.kind) else {
            return Ok(None);
        };
        let Some(exact) = coral_tools::exact_authored_tool_description_in_transaction(
            transaction,
            context.workspace_id,
            context.actor.principal(),
            &path,
        )
        .await?
        else {
            return Ok(None);
        };
        let Some(policy) = AuthoredToolPolicy::load(transaction, context, self.kind).await? else {
            return Ok(None);
        };
        // A disabled Tool leaves describe, as a disabled Provider Tool does.
        let approval_requirement =
            match policy.access_state(exact.id(), exact.default_access_state()) {
                coral_policy::ToolAccessState::Enabled => ApprovalRequirement::NotRequired,
                coral_policy::ToolAccessState::ApprovalRequired => ApprovalRequirement::Required,
                coral_policy::ToolAccessState::Disabled => return Ok(None),
            };
        Ok(Some(ExactAuthoringDescription::authored_tool(
            AuthoredToolAuthoringDescription::new(
                self.kind,
                call_path.clone(),
                exact.authoring().description().as_str().to_string(),
                exact.authoring().declaration().as_str().to_string(),
                policy.provider.id().clone(),
                policy.provider.display_name().to_owned(),
                exact.revision(),
                approval_requirement,
            ),
            exact.detail().clone(),
        )))
    }
}

impl AuthoredToolRetriever {
    /// The Authored Tools of this kind that the Principal can discover and
    /// that policy lets it call: none while the kind's Provider is
    /// disabled, and none that policy disables. Reads no source or
    /// declaration.
    async fn current(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
    ) -> Result<Vec<AuthoredToolSearchMaterial>, Error> {
        let Some(policy) = AuthoredToolPolicy::load(transaction, context, self.kind).await? else {
            return Ok(Vec::new());
        };
        Ok(coral_tools::authored_tool_search_material_in_transaction(
            transaction,
            context.workspace_id,
            context.actor.principal(),
            self.kind,
        )
        .await?
        .into_iter()
        .filter(|tool| {
            policy.access_state(tool.id(), tool.default_access_state())
                != coral_policy::ToolAccessState::Disabled
        })
        .collect())
    }
}

/// Whether a ranked Authored Tool is a strong match, by its retriever's own
/// rule: a field holds the query's words in order, or a word of its path
/// starts with a content word of the query. One that shares only description
/// words with the query is weak, so it follows every strong Tool.
fn strong_authored_tool(plan: &QueryPlan, matched: &repository::RowMatch) -> bool {
    matched.phrase_points > 0 || plan.content_words_starting([matched.name.as_str()]) > 0
}

/// How the Authored Tools of one kind are decided for the caller, read once
/// per request: the Workspace's active Provider of the kind, the Owners'
/// exact rules, and the caller's standing, which decides whether a rule
/// applies to its own Private Tools.
struct AuthoredToolPolicy {
    kind: AuthoredToolKind,
    provider: coral_provider::Provider,
    rules: coral_policy::ExactToolRules,
    caller: coral_policy::PrivateToolOwner,
}

impl AuthoredToolPolicy {
    /// `None` while an Owner disables the kind's Provider, which stops
    /// every Tool of the kind. Coral installs it in every Workspace before
    /// it serves requests.
    async fn load(
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        kind: AuthoredToolKind,
    ) -> Result<Option<Self>, Error> {
        let Some(provider) = coral_provider::authored_tool_provider_in_transaction(
            transaction,
            context.workspace_id,
            kind,
        )
        .await?
        .filter(|provider| provider.state() == coral_provider::ProviderState::Active) else {
            return Ok(None);
        };
        Ok(Some(Self {
            kind,
            provider,
            rules: coral_policy::exact_tool_rules_in_transaction(transaction, context.workspace_id)
                .await?,
            caller: coral_policy::PrivateToolOwner::of(
                context.actor.principal().kind(),
                context.authority.effective_user_role(),
            ),
        }))
    }

    /// The decision for the Authored Tool `id`, whose current revision
    /// declares `declared`.
    fn access_state(
        &self,
        id: &coral_tools::AuthoredToolId,
        declared: coral_policy::ToolAccessState,
    ) -> coral_policy::ToolAccessState {
        self.rules.authored_tool_access_state(
            self.provider.id(),
            &id.tool_key(),
            self.kind,
            declared,
            self.caller,
        )
    }
}

fn authored_tool_ref(material: AuthoredToolSearchMaterial) -> Result<EntityRef, Error> {
    Ok(EntityRef::AuthoredTool(AuthoredToolRef {
        call_path: CallPath::authored_tool(material.path())?,
        material,
    }))
}
