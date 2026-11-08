//! The Saved Function retriever: the requesting Principal's own Saved
//! Functions, ranked inside the database.
//!
//! coral-functions decides that a Saved Function is visible to its owner
//! only; the statement holds that as a condition on the row's own Principal
//! columns.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use coral_db::Transaction;
use coral_functions::SavedFunctionSearchMaterial;
use coral_telemetry::safe;

use super::plan::QueryPlan;
use super::repository;
use super::{
    Answer, EntityRef, EntityRetriever, Hydrated, Listing, QueryError, QueryRequest,
    RequestContext, SavedFunctionRef, WindowEntry, count,
};
use crate::evidence::{EvidenceBuilder, EvidenceTerms};
use crate::hit::{HitMaterial, hit_text};
use crate::signature::Callable;
use crate::{
    ApprovalRequirement, CallPath, EntityKind, Error, ExactAuthoringDescription,
    PrivateToolAuthoringDescription, ProviderCallRoot, RetrieverCoverage, SearchEvidence,
    SearchHit,
};

/// Ranks, hydrates, and describes Saved Functions.
#[derive(Debug, Default)]
pub(crate) struct SavedFunctionRetriever {
    /// Makes the ranked statement fail, for tests of a retriever error.
    #[cfg(test)]
    pub(crate) fail_ranking: bool,
}

#[async_trait]
impl EntityRetriever for SavedFunctionRetriever {
    fn kind(&self) -> EntityKind {
        EntityKind::SavedFunction
    }

    // TODO: a Provider filter could keep only the Saved Functions that call
    // the named Providers. Until then the filter narrows Tools only.
    async fn list(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        _provider_filter: &ProviderFilter,
    ) -> Result<Listing, Error> {
        let entities = current_functions(transaction, context)
            .await?
            .into_iter()
            .map(function_ref)
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
        let functions = current_functions(transaction, context)
            .await?
            .into_iter()
            .map(|function| (function.id().clone(), function))
            .collect::<HashMap<_, _>>();
        let Some(plan) = QueryPlan::build(request.terms) else {
            // No term can select anything, so no unit waits on the index.
            let eligible = count(functions.len());
            return Ok(Answer {
                window: Vec::new(),
                coverage: RetrieverCoverage::new(eligible, eligible, 0, false),
            });
        };
        let indexed = repository::indexed_functions(
            transaction,
            context.workspace_id,
            context.actor.principal(),
        )
        .await
        .map_err(QueryError::retrieval)?;
        let searched = functions
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
            repository::rank_functions(
                transaction,
                context.workspace_id,
                context.actor.principal(),
                &plan,
                request.window + 1,
            )
            .await
            .map_err(QueryError::Retriever)?
        } else {
            repository::Ranking {
                entities: Vec::new(),
                near_spellings: Vec::new(),
            }
        };
        let has_more = ranked.entities.len() > request.window;
        let near_spellings: Arc<[String]> = ranked.near_spellings.into();
        let mut window = Vec::new();
        for function in ranked.entities.into_iter().take(request.window) {
            // A function's delete leaves its row for a later write, so only
            // what the owner holds now may answer.
            let Some(material) = functions.get(&function.saved_function_id) else {
                continue;
            };
            window.push(WindowEntry {
                entity: EntityRef::SavedFunction(SavedFunctionRef {
                    material: material.clone(),
                    call_path: function.call_path,
                    near_spellings: Arc::clone(&near_spellings),
                }),
                strong: strong_function(&plan, &function.matched),
            });
        }
        Ok(Answer {
            coverage: RetrieverCoverage::new(
                count(functions.len()),
                count(searched),
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
        let EntityRef::SavedFunction(function) = entity else {
            return Ok(Hydrated::Dropped);
        };
        let material = match coral_functions::hydrate_saved_function_search_material_in_transaction(
            transaction,
            context.workspace_id,
            context.actor.principal(),
            &function.material,
        )
        .await
        {
            Ok(Some(material)) => material,
            Ok(None) => return Ok(Hydrated::Dropped),
            // A corrupt function leaves the page; the others stay.
            Err(coral_functions::Error::CorruptData(_)) => return Ok(Hydrated::Unreadable),
            Err(error) => return Err(error.into()),
        };
        let hit_material = HitMaterial {
            declaration: material.declaration().as_str(),
            callable: Callable::SavedFunction {
                leaf: function.call_path.leaf(),
            },
            description: material.description().as_str(),
            guidance: None,
            tags: &[],
        };
        let Some(terms) = context.terms else {
            return Ok(Hydrated::Hit(Box::new(SearchHit::from_saved_function(
                function.call_path.clone(),
                hit_text(&hit_material, None),
                SearchEvidence::default(),
            ))));
        };
        let terms = EvidenceTerms::new(terms, &function.near_spellings);
        let mut evidence = EvidenceBuilder::new(&terms);
        evidence
            .field("call_path", [function.call_path.as_str()])
            .field("description", [hit_material.description]);
        let declaration =
            coral_functions::saved_function_declaration_search_material_in_transaction(
                transaction,
                context.workspace_id,
                context.actor.principal(),
                function.material.path(),
            )
            .await?;
        for field in declaration
            .as_ref()
            .and_then(coral_functions::SavedFunctionDeclarationSearchMaterial::declaration_fields)
            .unwrap_or_default()
        {
            evidence.path(field.path(), field.text());
        }
        let Some(matched) = evidence.build() else {
            tracing::debug!(
                coral.saved_function.call_path =
                    safe::id(&super::RankedCallPath(&function.call_path)),
                "a ranked Saved Function has no evidence and leaves the page"
            );
            return Ok(Hydrated::Dropped);
        };
        let text = hit_text(&hit_material, Some((&terms, &matched)));
        Ok(Hydrated::Hit(Box::new(SearchHit::from_saved_function(
            function.call_path.clone(),
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
        let Some(path) = call_path
            .private_tool_path()
            .and_then(|relative| coral_functions::SavedFunctionPath::parse(relative).ok())
        else {
            return Ok(None);
        };
        let Some(exact) = coral_functions::exact_saved_function_description_in_transaction(
            transaction,
            context.workspace_id,
            context.actor.principal(),
            &path,
        )
        .await?
        else {
            return Ok(None);
        };
        // Coral installs it in every Workspace before it serves requests.
        let Some(provider) =
            coral_provider::private_provider_in_transaction(transaction, context.workspace_id)
                .await?
        else {
            return Ok(None);
        };
        // No exact rule can name a Private Tool yet, so its default decides
        // (#2977 brings Owner rules). A disabled Tool leaves describe, as a
        // disabled Provider Tool does.
        let approval_requirement = match exact.default_access_state() {
            coral_policy::ToolAccessState::Enabled => ApprovalRequirement::NotRequired,
            coral_policy::ToolAccessState::ApprovalRequired => ApprovalRequirement::Required,
            coral_policy::ToolAccessState::Disabled => return Ok(None),
        };
        Ok(Some(ExactAuthoringDescription::private_tool(
            PrivateToolAuthoringDescription::new(
                call_path.clone(),
                exact.authoring().description().as_str().to_string(),
                exact.authoring().declaration().as_str().to_string(),
                provider.id().clone(),
                provider.display_name().to_owned(),
                exact.revision(),
                approval_requirement,
            ),
            exact.detail().clone(),
        )))
    }
}

/// Whether a ranked Saved Function is a strong match, by its retriever's own
/// rule: a field holds the query's words in order, or a word of its path
/// starts with a content word of the query. One that shares only description
/// words with the query is weak, so it follows every strong Tool.
fn strong_function(plan: &QueryPlan, matched: &repository::RowMatch) -> bool {
    matched.phrase_points > 0 || plan.content_words_starting([matched.name.as_str()]) > 0
}

/// The Principal's current Saved Functions. Reads no source or declaration.
async fn current_functions(
    transaction: &mut Transaction<'_>,
    context: RequestContext<'_>,
) -> Result<Vec<SavedFunctionSearchMaterial>, Error> {
    Ok(
        coral_functions::saved_function_search_material_in_transaction(
            transaction,
            context.workspace_id,
            context.actor.principal(),
        )
        .await?,
    )
}

fn function_ref(material: SavedFunctionSearchMaterial) -> Result<EntityRef, Error> {
    Ok(EntityRef::SavedFunction(SavedFunctionRef {
        call_path: CallPath::saved_function(material.path().as_str())?,
        material,
        near_spellings: Arc::new([]),
    }))
}
