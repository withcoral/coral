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

use super::plan::QueryPlan;
use super::repository;
use super::{
    Answer, EntityRef, EntityRetriever, Hydrated, Listing, QueryError, QueryRequest,
    RequestContext, SavedFunctionRef, count,
};
use crate::evidence::{EvidenceBuilder, EvidenceTerms};
use crate::hit::{HitMaterial, hit_text};
use crate::signature::Callable;
use crate::{
    CallPath, EntityKind, Error, ExactAuthoringDescription, ProviderCallRoot, RetrieverCoverage,
    SavedFunctionAuthoringDescription, SearchEvidence, SearchHit,
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
        _provider_filter: Option<&ProviderCallRoot>,
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
            // A row of a function deleted after the owner read cannot rank:
            // the cascade removes it in the same snapshot.
            let Some(material) = functions.get(&function.saved_function_id) else {
                continue;
            };
            window.push(EntityRef::SavedFunction(SavedFunctionRef {
                material: material.clone(),
                call_path: function.call_path,
                near_spellings: Arc::clone(&near_spellings),
            }));
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
                call_path = function.call_path.as_str(),
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
            .as_str()
            .strip_prefix("coral.functions.")
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
        Ok(Some(ExactAuthoringDescription::saved_function(
            SavedFunctionAuthoringDescription::new(
                call_path.clone(),
                exact.authoring().description().as_str().to_string(),
                exact.authoring().declaration().as_str().to_string(),
            ),
            exact.detail().clone(),
        )))
    }
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
