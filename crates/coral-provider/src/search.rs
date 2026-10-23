//! What Provider hands to Universal Search: the entries it may index about a
//! Generation, and Provider reads that load no Operation catalog.
//!
//! Provider decides what may be searched; Universal Search decides how it is
//! stored and ranked. The entries are secret-free and carry no visibility:
//! Universal Search asks the owners about visibility at query time.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use coral_catalog::{
    CatalogDiagnostic, DeclarationIndexEntry, MaterializedOperation, OperationAvailability,
    OperationKey, PreparedDeclarationFields, ProviderGenerationId, SearchableOperation,
    StoredDeclarationFields,
};
use coral_db::Transaction;
use coral_workspace::WorkspaceId;

use crate::access::ProviderCallRoot;
use crate::{Error, OperationAuthoringMaterial, Provider, ProviderId, ProviderState, repository};

/// Everything Universal Search may index about one Generation: the Provider's
/// identity and one entry per structurally available Operation.
///
/// It holds the Generation's schema graph and prepares each entry's
/// declaration fields as [`Self::operations`] reads it. A caller that turns
/// each entry into what it keeps, and drops the entry, holds one Operation's
/// fields at a time: a large Provider's fields together run to gigabytes.
pub struct GenerationEntries {
    workspace_id: WorkspaceId,
    provider_id: ProviderId,
    call_root: ProviderCallRoot,
    display_name: String,
    generation_id: ProviderGenerationId,
    operations: Vec<SearchableOperation>,
    /// Behind a mutex because its type names are built on first use, and
    /// boxed so a future that holds the entries stays small.
    fields: Box<Mutex<StoredDeclarationFields>>,
}

impl GenerationEntries {
    /// Returns the Workspace that owns the Provider.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    /// Returns the Provider's stable identity.
    #[must_use]
    pub const fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }

    /// Returns the Provider's immutable Call Root, the start of every Call Path.
    #[must_use]
    pub const fn call_root(&self) -> &ProviderCallRoot {
        &self.call_root
    }

    /// Returns the Provider's display name.
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    /// Returns the Generation the entries describe.
    #[must_use]
    pub const fn generation_id(&self) -> &ProviderGenerationId {
        &self.generation_id
    }

    /// Prepares one entry per structurally available Operation, in
    /// Operation-Key order, each when it is read.
    ///
    /// # Errors
    ///
    /// An item is corrupt data when the stored package can no longer be
    /// prepared.
    pub fn operations(&self) -> impl Iterator<Item = Result<OperationEntry<'_>, Error>> {
        self.operations.iter().map(|operation| {
            Ok(OperationEntry {
                operation,
                // A walk that panicked leaves at most a partly built type-name
                // cache, which the next walk extends; the graph is unchanged.
                declaration_fields: self
                    .fields
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .searchable_operation(operation)?,
            })
        })
    }

    /// Drops every Operation, as if the Generation held none.
    #[cfg(any(test, feature = "test-fixtures"))]
    #[must_use]
    pub fn without_operations(mut self) -> Self {
        self.operations.clear();
        self
    }
}

/// What Universal Search may index about one structurally available Operation:
/// typed fields, never JSON, and nothing secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationEntry<'a> {
    operation: &'a SearchableOperation,
    declaration_fields: PreparedDeclarationFields,
}

impl<'a> OperationEntry<'a> {
    /// Returns the Operation Key inside its Generation.
    #[must_use]
    pub const fn key(&self) -> &'a OperationKey {
        self.operation.key()
    }

    /// Returns the Call Path below the Provider Call Root.
    #[must_use]
    pub fn relative_path(&self) -> &'a str {
        self.operation.relative_path()
    }

    /// Returns whether the Provider marks the Operation deprecated. This is an
    /// attribute, not searchable text.
    #[must_use]
    pub const fn deprecated(&self) -> bool {
        self.operation.presentation().deprecated()
    }

    /// Returns the short title, when the Provider gives one.
    #[must_use]
    pub fn title(&self) -> Option<&'a str> {
        self.operation.presentation().title()
    }

    /// Returns the description.
    #[must_use]
    pub fn description(&self) -> &'a str {
        self.operation.presentation().description()
    }

    /// Returns the tags.
    #[must_use]
    pub fn tags(&self) -> &'a [String] {
        self.operation.presentation().tags()
    }

    /// Returns the guidance, when the Provider gives some.
    #[must_use]
    pub fn guidance(&self) -> Option<&'a str> {
        self.operation.presentation().guidance()
    }

    /// Returns the readable declaration paths with the values the schema lists.
    #[must_use]
    pub fn declaration_fields(&self) -> &[DeclarationIndexEntry] {
        self.declaration_fields.entries()
    }

    /// Returns the warnings left while preparing the declaration fields: a
    /// bound that stopped the walk, or a schema it could not read. They
    /// concern search only and never change the Operation's availability.
    #[must_use]
    pub fn search_warnings(&self) -> &[CatalogDiagnostic] {
        self.declaration_fields.warnings()
    }
}

/// Reads the entries of the Provider's active Generation from what the
/// catalog stores: its schema graph and what search reads of its Operation
/// rows. The entries rebuild each Operation's declaration fields by the
/// current materialization rules as they are read. The search index runs this
/// after the commit that activated the Generation, and its reconcile task runs
/// it for a Generation that no notification indexed. `Ok(None)` means the
/// Workspace has no such Provider.
///
/// The Generation is the one active when this reads, not one the caller names.
/// When an update replaces the Generation after the caller listed the Provider,
/// the entries describe the new Generation. A publication can still replace it
/// after this read and before the caller writes the index. Search tolerates
/// that: retrieval reads only each Provider's active Generation, and the next
/// reconcile pass removes a replaced Generation's rows. The entries name the
/// Generation they describe; record [`GenerationEntries::generation_id`], not
/// the id the caller listed.
///
/// # Errors
///
/// Returns a shared persistence failure, or corrupt data when the stored
/// package can no longer be read.
pub async fn prepare_active_generation_entries_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: &ProviderId,
) -> Result<Option<GenerationEntries>, Error> {
    let Some(stored) =
        repository::provider_in_transaction(transaction, workspace_id, provider_id).await?
    else {
        return Ok(None);
    };
    let graph =
        coral_catalog::schema_graph_in_transaction(transaction, &stored.generation_id).await?;
    let operations =
        coral_catalog::searchable_operations_in_transaction(transaction, &stored.generation_id)
            .await?;
    let provider = &stored.provider;
    Ok(Some(GenerationEntries {
        workspace_id: provider.owner().clone(),
        provider_id: provider.id().clone(),
        call_root: provider.access().call_root().clone(),
        display_name: provider.display_name().to_string(),
        generation_id: stored.generation_id,
        operations,
        fields: Box::new(Mutex::new(StoredDeclarationFields::new(graph))),
    }))
}

/// One Active Provider of a Workspace with its active Generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveProvider {
    provider: Provider,
    generation_id: ProviderGenerationId,
}

impl ActiveProvider {
    /// Returns the Provider: its identity, Call Root, display name, and
    /// account-setting evidence.
    #[must_use]
    pub const fn provider(&self) -> &Provider {
        &self.provider
    }

    /// Returns the Provider's active Generation.
    #[must_use]
    pub const fn generation_id(&self) -> &ProviderGenerationId {
        &self.generation_id
    }
}

/// One Provider's active Generation and display name, as the search index's
/// reconcile task compares them with what the index holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveGeneration {
    workspace: WorkspaceId,
    provider: ProviderId,
    generation: ProviderGenerationId,
    display_name: String,
}

impl ActiveGeneration {
    pub(crate) const fn new(
        workspace_id: WorkspaceId,
        provider_id: ProviderId,
        generation_id: ProviderGenerationId,
        display_name: String,
    ) -> Self {
        Self {
            workspace: workspace_id,
            provider: provider_id,
            generation: generation_id,
            display_name,
        }
    }

    /// Returns the Workspace that owns the Provider.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace
    }

    /// Returns the Provider's stable identity.
    #[must_use]
    pub const fn provider_id(&self) -> &ProviderId {
        &self.provider
    }

    /// Returns the Provider's active Generation.
    #[must_use]
    pub const fn generation_id(&self) -> &ProviderGenerationId {
        &self.generation
    }

    /// Returns the Provider's current display name. A rename keeps the
    /// Generation, so the index compares the name on its own.
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }
}

/// Reads every Provider's active Generation and display name in every
/// Workspace, for the search index's reconcile task.
///
/// Disabled Providers are included: disabling changes what a caller sees at
/// query time, not what the index holds. This facet reads no Operation catalog.
///
/// # Errors
///
/// Returns a shared persistence or corrupt Provider failure.
pub async fn active_generations_in_transaction(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<ActiveGeneration>, Error> {
    repository::active_generations(transaction).await
}

/// Reads the active Generation of each of a Workspace's Providers, or of the
/// one Provider the filter names, for the search index's state read.
///
/// Disabled Providers are included, as in [`active_generations_in_transaction`].
/// The caller checks Workspace authority. This facet reads no Operation catalog.
///
/// # Errors
///
/// Returns a shared persistence or corrupt Provider failure.
pub async fn workspace_active_generations_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_filter: Option<&ProviderId>,
) -> Result<Vec<ActiveGeneration>, Error> {
    Ok(
        repository::management_providers(transaction, workspace_id, provider_filter)
            .await?
            .into_iter()
            .map(|stored| {
                ActiveGeneration::new(
                    workspace_id.clone(),
                    stored.provider.id().clone(),
                    stored.generation_id,
                    stored.provider.display_name().to_owned(),
                )
            })
            .collect(),
    )
}

/// Reads a Workspace's Active Providers with their active Generations.
///
/// This facet reads no Operation catalog, so it stays cheap for a Workspace
/// with large catalogs. Disabled Providers are left out.
///
/// # Errors
///
/// Returns a shared persistence or corrupt Provider failure.
pub async fn active_providers_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Vec<ActiveProvider>, Error> {
    active_providers(transaction, workspace_id, None).await
}

pub(crate) async fn active_providers(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_filter: Option<&ProviderId>,
) -> Result<Vec<ActiveProvider>, Error> {
    Ok(
        repository::management_providers(transaction, workspace_id, provider_filter)
            .await?
            .into_iter()
            .filter(|stored| stored.provider.state() == ProviderState::Active)
            .map(|stored| ActiveProvider {
                provider: stored.provider,
                generation_id: stored.generation_id,
            })
            .collect(),
    )
}

/// One Operation of a Provider's active Generation as a search hit shows it:
/// its complete authoring material and its readable declaration fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HydratedOperation {
    material: OperationAuthoringMaterial,
    declaration_fields: Vec<DeclarationIndexEntry>,
}

impl HydratedOperation {
    /// Returns the complete authoring material.
    #[must_use]
    pub const fn material(&self) -> &OperationAuthoringMaterial {
        &self.material
    }

    /// Returns the readable declaration paths with the values the schema
    /// lists, as the index prepares them from the stored Generation.
    #[must_use]
    pub fn declaration_fields(&self) -> &[DeclarationIndexEntry] {
        &self.declaration_fields
    }
}

/// The stored Generations one search request has loaded for its hits.
///
/// Loading a Generation's schema graph and naming its types cost time in
/// proportion to the whole Provider, while one hit needs a single Operation's
/// fields. A request keeps one of these so hits of the same Generation load
/// it once. Generations are immutable, so what it holds never goes stale.
#[derive(Default)]
pub struct LoadedGenerations(Mutex<HashMap<ProviderGenerationId, StoredDeclarationFields>>);

impl LoadedGenerations {
    fn has(&self, generation_id: &ProviderGenerationId) -> bool {
        self.0
            .lock()
            .expect("loaded generations mutex poisoned")
            .contains_key(generation_id)
    }

    fn operation_fields(
        &self,
        generation_id: &ProviderGenerationId,
        operation: &MaterializedOperation,
    ) -> Option<Result<Vec<DeclarationIndexEntry>, coral_catalog::Error>> {
        self.0
            .lock()
            .expect("loaded generations mutex poisoned")
            .get(generation_id)
            .map(|fields| {
                fields
                    .operation(operation)
                    .map(PreparedDeclarationFields::into_entries)
            })
    }

    fn keep(&self, generation_id: ProviderGenerationId, fields: StoredDeclarationFields) {
        self.0
            .lock()
            .expect("loaded generations mutex poisoned")
            .insert(generation_id, fields);
    }
}

/// Hydrates one Operation of the Provider's active Generation for a search
/// hit, after ranking: its authoring material and its declaration fields,
/// rebuilt from the stored schema graph. The graph is read once per
/// Generation for each `loaded`.
///
/// `Ok(None)` when the Generation has no structurally available Operation
/// under the key. The caller decides visibility.
///
/// # Errors
///
/// Returns a shared persistence failure, or corrupt data when the stored
/// package can no longer be prepared.
pub async fn hydrate_operation_in_transaction(
    transaction: &mut Transaction<'_>,
    loaded: &LoadedGenerations,
    provider: &ActiveProvider,
    operation_key: &OperationKey,
) -> Result<Option<HydratedOperation>, Error> {
    let Some(operation) = coral_catalog::operation_in_transaction(
        transaction,
        provider.generation_id(),
        operation_key,
    )
    .await?
    .filter(|operation| operation.availability() == OperationAvailability::Available) else {
        return Ok(None);
    };
    let generation_id = provider.generation_id();
    if !loaded.has(generation_id) {
        let graph = coral_catalog::schema_graph_in_transaction(transaction, generation_id).await?;
        // Keep it before reading this Operation, so one corrupt Operation does
        // not make every later hit of the Generation load the graph again.
        loaded.keep(generation_id.clone(), StoredDeclarationFields::new(graph));
    }
    let declaration_fields =
        loaded
            .operation_fields(generation_id, &operation)
            .ok_or(Error::CorruptData(
                "a loaded Generation is missing from the request".into(),
            ))??;
    let declaration =
        coral_catalog::authoring_declaration_in_transaction(transaction, generation_id, &operation)
            .await?;
    Ok(Some(HydratedOperation {
        material: OperationAuthoringMaterial::new(
            provider.provider().access(),
            provider.provider(),
            &operation,
            declaration,
        ),
        declaration_fields,
    }))
}

/// Reads one Operation of the Provider's active Generation by its path below
/// the Call Root, for an exact description. Reads no other Operation.
///
/// `Ok(None)` when no structurally available Operation has the path. The
/// caller decides visibility.
///
/// # Errors
///
/// Returns a shared persistence or corrupt-package failure.
pub async fn operation_material_by_relative_path_in_transaction(
    transaction: &mut Transaction<'_>,
    provider: &ActiveProvider,
    relative_path: &str,
) -> Result<Option<OperationAuthoringMaterial>, Error> {
    let Some(operation) = coral_catalog::operation_by_relative_path_in_transaction(
        transaction,
        provider.generation_id(),
        relative_path,
    )
    .await?
    .filter(|operation| operation.availability() == OperationAvailability::Available) else {
        return Ok(None);
    };
    let declaration = coral_catalog::authoring_declaration_in_transaction(
        transaction,
        provider.generation_id(),
        &operation,
    )
    .await?;
    Ok(Some(OperationAuthoringMaterial::new(
        provider.provider().access(),
        provider.provider(),
        &operation,
        declaration,
    )))
}
