use std::sync::Arc;
use std::time::Duration;

use coral_catalog::AuthoredToolKind;
use coral_db::{Database, Transaction};
use coral_run_executor::CompilerClient;
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, Principal, PrincipalId, PrincipalKind,
    PrincipalWorkspaceEvidence, WorkspaceAction, WorkspaceId, WorkspaceModule, WorkspaceRole,
};

use crate::authoring::{
    Authoring, PreparedAuthoredTool, Typing, missing_linked_tool, now_unix_nanos,
    tool_paths_overlap, v8_compiler,
};
use crate::linking::LinkedToolReference;
use crate::repository;
use crate::tool_typing::ToolDeclarationReader;
use crate::types::AuthoredToolPath;
use crate::types::{
    AdministrativeLifecycleEvent, LifecycleAction, PrivateTool, PrivateToolChangeContext,
    PrivateToolId, PrivateToolPath, PrivateToolSave,
};
use crate::{Error, PrivateToolDeletion, PrivateToolInspection};

/// Opaque compiled save with its exact authenticated scope and authoring evidence.
pub struct PreparedSave {
    context: PrivateToolChangeContext,
    workspace_id: WorkspaceId,
    save: PrivateToolSave,
    prepared: PreparedAuthoredTool,
}

/// Atomic current Private Tool lifecycle Module.
///
/// This transport-neutral Interface owns create-or-replace, Principal inspection,
/// Owner inspection and deletion of Agent definitions, authoring
/// compilation, and safe lifecycle evidence. Compilation happens outside the
/// successful authoring transaction; current authority, state, definition, and
/// Event commit together at the final linearization point.
#[derive(Clone)]
pub struct PrivateToolModule {
    database: Arc<Database>,
    workspaces: WorkspaceModule,
    inspection: PrivateToolInspection,
    deletion: PrivateToolDeletion,
    authoring: Authoring,
}

impl PrivateToolModule {
    /// Builds the Module with a warm compiler of its own, within the
    /// deployment's compiler heap budget. `compilation_timeout` bounds each
    /// save's preparation; production passes [`crate::COMPILATION_TIMEOUT`].
    /// A composed process shares its one compiler through
    /// [`Self::with_compiler_client`] instead.
    ///
    /// # Errors
    ///
    /// Returns an initialization error when the bounded compiler cannot start.
    pub fn new(
        database: Arc<Database>,
        workspaces: WorkspaceModule,
        isolate_limits: coral_v8::IsolateLimits,
        compilation_timeout: Duration,
    ) -> Result<Self, Error> {
        let compiler = coral_run_executor::InProcessCompiler::new(isolate_limits)
            .map_err(|error| Error::CompilerUnavailable(error.to_string()))?;
        Ok(Self::with_compiler_client(
            database,
            workspaces,
            Arc::new(compiler),
            compilation_timeout,
        ))
    }

    /// Builds the Module as [`Self::new`] does, except that it compiles
    /// through `compiler`, the process's one warm compiler.
    #[must_use]
    pub fn with_compiler_client(
        database: Arc<Database>,
        workspaces: WorkspaceModule,
        compiler: Arc<dyn CompilerClient>,
        compilation_timeout: Duration,
    ) -> Self {
        Self {
            inspection: PrivateToolInspection::new(database.clone(), workspaces.clone()),
            deletion: PrivateToolDeletion::new(database.clone(), workspaces.clone()),
            authoring: Authoring::new(database.clone(), v8_compiler(compiler), compilation_timeout),
            database,
            workspaces,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_compiler(
        database: Arc<Database>,
        workspaces: WorkspaceModule,
        compiler: Arc<dyn crate::authoring::AuthoredToolCompiler>,
    ) -> Self {
        Self {
            inspection: PrivateToolInspection::new(database.clone(), workspaces.clone()),
            deletion: PrivateToolDeletion::new(database.clone(), workspaces.clone()),
            authoring: Authoring::new(
                database.clone(),
                compiler,
                coral_test_support::TEST_COMPILATION_TIMEOUT,
            ),
            database,
            workspaces,
        }
    }

    /// Atomically creates or replaces the current definition at one owned path.
    ///
    /// Source is compiled before a write transaction. A successful final
    /// transaction rechecks active Workspace authority and every directly named
    /// Private Tool dependency, then commits source, description, declaration,
    /// bounded diagnostics, and one lifecycle Event. Occurrence time is metadata
    /// and never a stale-write guard. Tools stay untyped; use
    /// [`Self::prepare_save`] with a reader to type them.
    ///
    /// This save does not notify Universal Search: coral-search depends on
    /// this crate, so the call would be a cycle. A production caller notifies
    /// `SearchIndex::sync_authored_tool` after its commit, as
    /// `coral.addTool` and `coral.updateTool` do. Until then, the reconcile task
    /// indexes the Private Tool.
    ///
    /// # Errors
    ///
    /// Returns input, authority, authoring, timeout, or persistence failure.
    /// Failure leaves both the current definition and lifecycle evidence unchanged.
    pub async fn save(
        &self,
        context: &PrivateToolChangeContext,
        workspace_id: &WorkspaceId,
        save: PrivateToolSave,
    ) -> Result<PrivateTool, Error> {
        let prepared = self.prepare_save(context, workspace_id, save, None).await?;
        let mut transaction = self.database.begin().await?;
        let tool = self.save_in_transaction(&mut transaction, prepared).await?;
        transaction.commit().await?;
        Ok(tool)
    }

    /// Prepares non-executing authoring work outside the final write transaction.
    ///
    /// The source is typed against the current declarations of the Private
    /// Tools it names and, through `tools`, of the Tools it names
    /// that the caller can discover. Without `tools`, `coral.providers`
    /// and the Built-in Tools stay untyped. A source that cannot be analyzed is
    /// prepared untyped, so preparation reports its own failure. Type
    /// diagnostics are advisory.
    ///
    /// # Errors
    /// Returns authority, compilation, or authoring-limit failures without changes.
    pub async fn prepare_save(
        &self,
        context: &PrivateToolChangeContext,
        workspace_id: &WorkspaceId,
        save: PrivateToolSave,
        tools: Option<&dyn ToolDeclarationReader>,
    ) -> Result<PreparedSave, Error> {
        self.authorize_active(context.actor(), workspace_id).await?;
        if save.has_precondition() {
            save.admits(
                self.current_revision(context.actor(), workspace_id, save.path())
                    .await?,
            )?;
        }
        let prepared = self
            .authoring
            .prepare(
                workspace_id,
                &AuthoredToolPath::Private(save.path().clone()),
                save.source().as_str(),
                Typing {
                    private_tools_of: Some(context.actor().principal()),
                    tools,
                },
            )
            .await?;
        Ok(PreparedSave {
            context: context.clone(),
            workspace_id: workspace_id.clone(),
            save,
            prepared,
        })
    }

    /// Stages a complete save and lifecycle evidence; the caller owns commit.
    ///
    /// # Errors
    /// Rechecks current authority and dependencies. Failure requires rollback.
    pub async fn save_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        prepared_save: PreparedSave,
    ) -> Result<PrivateTool, Error> {
        let PreparedSave {
            context,
            workspace_id,
            save,
            prepared,
        } = prepared_save;
        let context = &context;
        let workspace_id = &workspace_id;
        let (authority, previous) = self
            .hold_save_inputs_in_transaction(
                transaction,
                context.actor(),
                workspace_id,
                save.path(),
                &prepared.linked,
            )
            .await?;
        save.admits(previous.as_ref().map(PrivateTool::revision))?;
        let (id, revision, action) = match previous.as_ref() {
            None => (PrivateToolId::generate(), 1, LifecycleAction::Created),
            Some(previous) => (
                previous.id().clone(),
                previous.revision.checked_add(1).ok_or_else(|| {
                    Error::CorruptData("Private Tool revision overflowed".to_string())
                })?,
                LifecycleAction::Replaced,
            ),
        };
        // A new Private Tool's default is `enabled` unless the save
        // declares another, and a replacement keeps its Private Tool's unless the save
        // declares another. The Event names the default only when it moves.
        let previous_default = previous.as_ref().map(PrivateTool::default_access_state);
        let default_access_state = save.default_access_state(previous_default);
        let mut changed_field_paths = vec![
            "source",
            "description",
            "declaration",
            "declaration_fields",
            "diagnostics",
        ];
        if default_access_state != previous_default.unwrap_or(PrivateToolSave::NEW_TOOL_DEFAULT) {
            changed_field_paths.push("default_access_state");
        }
        let tool = PrivateTool::new(
            id,
            workspace_id.clone(),
            context.actor().principal().clone(),
            save.path().clone(),
            save.description().clone(),
            save.source().clone(),
            prepared.declaration,
            prepared.declaration_field_projection,
            prepared.diagnostics,
            prepared.diagnostics_truncated,
            context.occurred_at_unix_nanos(),
            revision,
            default_access_state,
        );
        authority.require_current_at(now_unix_nanos()?)?;
        match previous.as_ref() {
            None => repository::insert_current(transaction, &tool).await?,
            Some(previous) => {
                repository::replace_current(transaction, previous, &tool).await?;
            }
        }
        repository::insert_lifecycle_event(
            transaction,
            repository::NewLifecycleEvent {
                workspace_id,
                actor: context.actor().principal(),
                private_user_id: (tool.principal().kind() == coral_workspace::PrincipalKind::User)
                    .then_some(tool.principal().id()),
                action,
                target_id: tool.id(),
                target_path: tool.path(),
                changed_field_paths: &changed_field_paths,
                occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
                trace_id: context.trace_id(),
            },
        )
        .await?;
        Ok(tool)
    }

    /// Returns the current definition at one path owned by the authenticated Principal.
    ///
    /// # Errors
    ///
    /// An absent, inaccessible, or differently owned path returns
    /// [`Error::NotFound`].
    pub async fn get(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        path: &PrivateToolPath,
    ) -> Result<PrivateTool, Error> {
        self.inspection.get(actor, workspace_id, path).await
    }

    /// Lists one Workspace-owned Agent's current Private Tools for a Workspace Owner.
    ///
    /// User-owned Private Tools remain private and cannot be selected through this Interface.
    /// The Agent's stable identity is an inspection target, never an authoring identity
    /// for the calling User.
    ///
    /// # Errors
    ///
    /// Requires Workspace Owner authority and returns authority or
    /// persistence failures.
    pub async fn list_agent(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        agent_id: &PrincipalId,
    ) -> Result<Vec<PrivateTool>, Error> {
        let user = actor.authenticated_user().ok_or(Error::PermissionDenied(
            "only a Workspace Owner can inspect Agent Private Tools",
        ))?;
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.authorize_agent_inspection_in_transaction(&mut transaction, user, workspace_id)
            .await?;
        let agent = Principal::from_parts(agent_id.clone(), PrincipalKind::Agent);
        let tools =
            repository::list_current_in_transaction(&mut transaction, workspace_id, &agent).await?;
        transaction.commit().await?;
        Ok(tools)
    }

    /// Returns one Workspace-owned Agent Private Tool to a Workspace Owner.
    ///
    /// # Errors
    ///
    /// Requires Workspace Owner authority. An absent path remains
    /// [`Error::NotFound`].
    pub async fn get_agent(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        agent_id: &PrincipalId,
        path: &PrivateToolPath,
    ) -> Result<PrivateTool, Error> {
        let user = actor
            .authenticated_user()
            .ok_or(Error::NotFound(AuthoredToolKind::Private))?;
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.authorize_agent_inspection_in_transaction(&mut transaction, user, workspace_id)
            .await
            .map_err(conceal_inspection_error)?;
        let agent = Principal::from_parts(agent_id.clone(), PrincipalKind::Agent);
        let tool =
            repository::find_current_in_transaction(&mut transaction, workspace_id, &agent, path)
                .await?
                .ok_or(Error::NotFound(AuthoredToolKind::Private))?;
        transaction.commit().await?;
        Ok(tool)
    }

    /// Atomically deletes one Workspace-owned Agent Private Tool as a Workspace Owner.
    ///
    /// The accountable User remains the lifecycle actor and never authors or
    /// executes as the Agent.
    ///
    /// # Errors
    ///
    /// Requires Workspace Owner authority in an active Workspace. Absence is
    /// [`Error::NotFound`].
    pub async fn delete_agent(
        &self,
        context: &PrivateToolChangeContext,
        workspace_id: &WorkspaceId,
        agent_id: &PrincipalId,
        path: &PrivateToolPath,
    ) -> Result<PrivateToolId, Error> {
        let user = context
            .actor()
            .authenticated_user()
            .ok_or(Error::NotFound(AuthoredToolKind::Private))?;
        let mut transaction = self.database.begin().await?;
        self.workspaces
            .check_configuration_in_transaction(&mut transaction, user, workspace_id)
            .await
            .map_err(conceal_agent_private_tool_error)?;
        let agent = Principal::from_parts(agent_id.clone(), PrincipalKind::Agent);
        let tool =
            repository::find_current_in_transaction(&mut transaction, workspace_id, &agent, path)
                .await?
                .ok_or(Error::NotFound(AuthoredToolKind::Private))?;
        let deleted_id =
            delete_current_with_event(&mut transaction, context, workspace_id, &tool).await?;
        transaction.commit().await?;
        Ok(deleted_id)
    }

    /// Atomically deletes one current path and records its lifecycle Event.
    ///
    /// # Errors
    ///
    /// Requires the owning Principal and an active Workspace. Absence or other
    /// ownership is concealed as [`Error::NotFound`].
    pub async fn delete(
        &self,
        context: &PrivateToolChangeContext,
        workspace_id: &WorkspaceId,
        path: &PrivateToolPath,
    ) -> Result<PrivateToolId, Error> {
        self.deletion.delete(context, workspace_id, path).await
    }

    /// Stages deletion and lifecycle evidence in the caller's transaction.
    ///
    /// # Errors
    /// Rechecks authority and exact current ownership. Failure requires rollback.
    pub async fn delete_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        context: &PrivateToolChangeContext,
        workspace_id: &WorkspaceId,
        path: &PrivateToolPath,
    ) -> Result<PrivateToolId, Error> {
        self.deletion
            .delete_in_transaction(transaction, context, workspace_id, path)
            .await
    }

    /// Lists the current User's safe Private Tool lifecycle evidence.
    ///
    /// Agents cannot browse administrative history in this staged Interface.
    /// Source and description values never enter the returned evidence.
    ///
    /// # Errors
    ///
    /// Returns current Workspace authority, Agent-history, or persistence failure.
    pub async fn list_lifecycle_events(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<AdministrativeLifecycleEvent>, Error> {
        let Some(user) = actor.authenticated_user() else {
            return Err(Error::PermissionDenied(
                "Agents cannot inspect Private Tool lifecycle history",
            ));
        };
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.workspaces
            .check_in_transaction(
                &mut transaction,
                user,
                workspace_id,
                WorkspaceAction::Inspect,
            )
            .await?;
        let events = repository::list_lifecycle_events_in_transaction(
            &mut transaction,
            workspace_id,
            actor.principal(),
        )
        .await?;
        transaction.commit().await?;
        Ok(events)
    }

    async fn hold_save_inputs_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        path: &PrivateToolPath,
        references: &[LinkedToolReference],
    ) -> Result<(PrincipalWorkspaceEvidence, Option<PrivateTool>), Error> {
        let authority = self
            .authorize_active_in_transaction(transaction, actor, workspace_id)
            .await?;
        // The Workspace hold acquired above serializes namespace decisions for
        // both absent and existing paths across SQLite and Postgres.
        let current_private_tools =
            repository::list_current_in_transaction(transaction, workspace_id, actor.principal())
                .await?;
        if current_private_tools
            .iter()
            .any(|tool| tool_paths_overlap(tool.path().as_str(), path.as_str()))
        {
            return Err(Error::PathConflict(AuthoredToolKind::Private));
        }

        // A save holds the caller's own Private Tools that its source names.
        // A Shared Tool it names is linked, and held, by each Run.
        let private_references = references
            .iter()
            .filter_map(|reference| match reference.path() {
                AuthoredToolPath::Private(named) => Some((named, reference)),
                AuthoredToolPath::Shared(_) => None,
            })
            .collect::<Vec<_>>();
        let mut held_paths = private_references
            .iter()
            .map(|(named, _reference)| (*named).clone())
            .chain(std::iter::once(path.clone()))
            .collect::<Vec<_>>();
        held_paths.sort();
        held_paths.dedup();
        for held_path in &held_paths {
            let exists = repository::hold_current_path_in_transaction(
                transaction,
                workspace_id,
                actor.principal(),
                held_path,
            )
            .await?;
            if !exists
                && let Some((_named, reference)) = private_references
                    .iter()
                    .find(|(named, _reference)| *named == held_path)
            {
                return Err(missing_linked_tool(
                    &AuthoredToolPath::Private(path.clone()),
                    reference,
                ));
            }
        }
        let previous = current_private_tools
            .into_iter()
            .find(|tool| tool.path() == path);
        Ok((authority, previous))
    }

    async fn authorize_agent_inspection_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        user: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
    ) -> Result<(), Error> {
        let evidence = self
            .workspaces
            .check_in_transaction(transaction, user, workspace_id, WorkspaceAction::Inspect)
            .await?;
        if evidence.effective_role() != WorkspaceRole::Owner {
            return Err(Error::PermissionDenied(
                "only a Workspace Owner can inspect Agent Private Tools",
            ));
        }
        Ok(())
    }

    /// The revision of the actor's Private Tool at `path`, from a snapshot.
    /// Only a precheck reads it: the save decides under its hold.
    async fn current_revision(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        path: &PrivateToolPath,
    ) -> Result<Option<u64>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let current = repository::find_current_in_transaction(
            &mut transaction,
            workspace_id,
            actor.principal(),
            path,
        )
        .await?;
        transaction.commit().await?;
        Ok(current.as_ref().map(PrivateTool::revision))
    }

    async fn authorize_active(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
    ) -> Result<(), Error> {
        self.workspaces
            .check_authoring(actor, workspace_id, now_unix_nanos()?)
            .await?;
        Ok(())
    }

    async fn authorize_active_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        self.workspaces
            .check_authoring_in_transaction(transaction, actor, workspace_id, now_unix_nanos()?)
            .await
            .map_err(Into::into)
    }
}

/// Removes current definitions in scopes where access ends and records their deletion evidence.
///
/// Administration supplies Workspace-prepared access-removal evidence bound
/// to this transaction. This facet neither commits
/// nor initializes a compiler; retained execution snapshots are untouched.
///
/// # Errors
///
/// Returns a persistence or corrupt-definition failure, requiring the caller to
/// roll back its complete deletion transaction.
pub async fn delete_current_for_access_removal_in_transaction(
    transaction: &mut Transaction<'_>,
    context: &coral_workspace::ChangeContext,
    prepared: &coral_workspace::PreparedAccessRemoval,
) -> Result<(), Error> {
    prepared.verify_transaction(transaction, context)?;
    let change = PrivateToolChangeContext::new(
        AuthenticatedPrincipal::user(context.actor().clone()),
        context.occurred_at_unix_nanos(),
        context.trace_id().clone(),
    );
    for scope in prepared.lost_access() {
        let tools = repository::list_current_in_transaction(
            transaction,
            scope.workspace_id(),
            scope.principal(),
        )
        .await?;
        for tool in tools {
            delete_current_with_event(transaction, &change, scope.workspace_id(), &tool).await?;
        }
    }
    Ok(())
}

pub(crate) async fn delete_current_with_event(
    transaction: &mut Transaction<'_>,
    context: &PrivateToolChangeContext,
    workspace_id: &WorkspaceId,
    tool: &PrivateTool,
) -> Result<PrivateToolId, Error> {
    repository::delete_current(transaction, tool).await?;
    repository::insert_lifecycle_event(
        transaction,
        repository::NewLifecycleEvent {
            workspace_id,
            actor: context.actor().principal(),
            private_user_id: (tool.principal().kind() == coral_workspace::PrincipalKind::User)
                .then_some(tool.principal().id()),
            action: LifecycleAction::Deleted,
            target_id: tool.id(),
            target_path: tool.path(),
            changed_field_paths: &[
                "source",
                "description",
                "declaration",
                "declaration_fields",
                "diagnostics",
            ],
            occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
            trace_id: context.trace_id(),
        },
    )
    .await?;
    Ok(tool.id().clone())
}

pub(crate) fn conceal_inspection_error(error: Error) -> Error {
    match error {
        Error::PermissionDenied(_)
        | Error::Workspace(
            coral_workspace::Error::WorkspaceNotFound | coral_workspace::Error::PermissionDenied(_),
        ) => Error::NotFound(AuthoredToolKind::Private),
        other => other,
    }
}

fn conceal_agent_private_tool_error(error: coral_workspace::Error) -> Error {
    match error {
        coral_workspace::Error::WorkspaceNotFound | coral_workspace::Error::PermissionDenied(_) => {
            Error::NotFound(AuthoredToolKind::Private)
        }
        other => Error::Workspace(other),
    }
}
