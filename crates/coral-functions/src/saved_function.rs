use coral_telemetry::metric::CompilerRole;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use coral_db::{Database, Transaction};
use coral_telemetry::safe;
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, Principal, PrincipalId, PrincipalKind,
    PrincipalWorkspaceEvidence, WorkspaceAction, WorkspaceId, WorkspaceModule, WorkspaceRole,
};
use tokio::time::{timeout, timeout_at};

use coral_v8::{
    COMPILATION_TIMEOUT, CallableDeclaration, CompileFailure, CoralDeclarations,
    SavedFunctionCallPaths,
};

use crate::repository;
use crate::tool_typing::{self, ToolDeclarationReader};
use crate::types::{
    AdministrativeLifecycleEvent, LifecycleAction, SAVED_FUNCTION_DECLARATION_MAX_BYTES,
    SAVED_FUNCTION_DIAGNOSTIC_MAX_COUNT, SavedFunction, SavedFunctionChangeContext,
    SavedFunctionDeclaration, SavedFunctionDeclarationFieldProjection, SavedFunctionDiagnostic,
    SavedFunctionId, SavedFunctionPath, SavedFunctionSave,
};
use crate::{Error, SavedFunctionDeletion, SavedFunctionInspection};

pub(crate) struct SavedFunctionReference {
    path: SavedFunctionPath,
    line: u64,
    column: u64,
}

pub(crate) fn now_unix_nanos() -> Result<i64, Error> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::Clock(error.to_string()))?;
    i64::try_from(elapsed.as_nanos())
        .map_err(|_error| Error::Clock("current time exceeds durable nanosecond range".to_string()))
}

pub(crate) struct PreparedSavedFunction {
    pub(crate) declaration: SavedFunctionDeclaration,
    pub(crate) declaration_field_projection: SavedFunctionDeclarationFieldProjection,
    pub(crate) diagnostics: Vec<SavedFunctionDiagnostic>,
    pub(crate) diagnostics_truncated: bool,
    pub(crate) references: Vec<SavedFunctionReference>,
}

pub(crate) trait SavedFunctionCompiler: Send + Sync {
    /// Finds the Call Paths to type by `deadline`. `None` leaves the source
    /// untyped, so `prepare` reports any failure of a source this cannot
    /// analyze.
    fn call_paths(&self, deadline: Instant, source: &str) -> Option<SavedFunctionCallPaths>;

    /// Prepares `source`, typed by `declarations` when there are any.
    fn prepare(
        &self,
        deadline: Instant,
        source: &str,
        declarations: Option<&CoralDeclarations>,
    ) -> Result<PreparedSavedFunction, CompileFailure>;
}

struct V8SavedFunctionCompiler {
    compiler: coral_v8::ProgramCompiler,
}

impl V8SavedFunctionCompiler {
    fn new(isolate_limits: coral_v8::IsolateLimits) -> Result<Self, Error> {
        Ok(Self {
            compiler: coral_v8::ProgramCompiler::new(isolate_limits, CompilerRole::Authoring)
                .map_err(|error| Error::CompilerUnavailable(error.to_string()))?,
        })
    }
}

impl SavedFunctionCompiler for V8SavedFunctionCompiler {
    fn call_paths(&self, deadline: Instant, source: &str) -> Option<SavedFunctionCallPaths> {
        self.compiler
            .saved_function_call_paths(deadline, source)
            .inspect_err(|error| {
                tracing::debug!(
                    error.type = safe::kind(error.code()),
                    "Call Path analysis failed; the source is checked untyped"
                );
            })
            .ok()
    }

    fn prepare(
        &self,
        deadline: Instant,
        source: &str,
        declarations: Option<&CoralDeclarations>,
    ) -> Result<PreparedSavedFunction, CompileFailure> {
        let compiled = match declarations {
            Some(declarations) => {
                self.compiler
                    .prepare_typed_saved_function(deadline, source, declarations)
            }
            None => self.compiler.prepare_saved_function(deadline, source),
        }?;
        let declaration = SavedFunctionDeclaration::from_compiler(compiled.declaration())
            .map_err(|error| error.to_string())?;
        let declaration_field_projection =
            SavedFunctionDeclarationFieldProjection::from_compiler(compiled.declaration_fields());
        let diagnostics_truncated =
            compiled.diagnostics().len() > SAVED_FUNCTION_DIAGNOSTIC_MAX_COUNT;
        let diagnostics = compiled
            .diagnostics()
            .iter()
            .take(SAVED_FUNCTION_DIAGNOSTIC_MAX_COUNT)
            .map(SavedFunctionDiagnostic::from_compiler)
            .collect();
        let references = compiled
            .references()
            .iter()
            .map(|reference| {
                let path = SavedFunctionPath::parse(reference.path()).map_err(|error| {
                    format!("at {}:{}: {error}", reference.line(), reference.column())
                })?;
                Ok(SavedFunctionReference {
                    path,
                    line: reference.line(),
                    column: reference.column(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(PreparedSavedFunction {
            declaration,
            declaration_field_projection,
            diagnostics,
            diagnostics_truncated,
            references,
        })
    }
}

/// Opaque compiled save with its exact authenticated scope and authoring evidence.
pub struct PreparedSave {
    context: SavedFunctionChangeContext,
    workspace_id: WorkspaceId,
    save: SavedFunctionSave,
    prepared: PreparedSavedFunction,
}

/// Atomic current Saved Function lifecycle Module.
///
/// This transport-neutral Interface owns create-or-replace, Principal inspection,
/// Owner inspection and deletion of Agent definitions, authoring
/// compilation, and safe lifecycle evidence. Compilation happens outside the
/// successful authoring transaction; current authority, state, definition, and
/// Event commit together at the final linearization point.
#[derive(Clone)]
pub struct SavedFunctionModule {
    database: Arc<Database>,
    workspaces: WorkspaceModule,
    inspection: SavedFunctionInspection,
    deletion: SavedFunctionDeletion,
    compiler: Arc<dyn SavedFunctionCompiler>,
}

impl SavedFunctionModule {
    /// Builds the Module with a deployment-selected compiler heap budget.
    ///
    /// # Errors
    ///
    /// Returns an initialization error when the bounded compiler cannot start.
    pub fn new(
        database: Arc<Database>,
        workspaces: WorkspaceModule,
        isolate_limits: coral_v8::IsolateLimits,
    ) -> Result<Self, Error> {
        Ok(Self {
            inspection: SavedFunctionInspection::new(database.clone(), workspaces.clone()),
            deletion: SavedFunctionDeletion::new(database.clone(), workspaces.clone()),
            database,
            workspaces,
            compiler: Arc::new(V8SavedFunctionCompiler::new(isolate_limits)?),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_compiler(
        database: Arc<Database>,
        workspaces: WorkspaceModule,
        compiler: Arc<dyn SavedFunctionCompiler>,
    ) -> Self {
        Self {
            inspection: SavedFunctionInspection::new(database.clone(), workspaces.clone()),
            deletion: SavedFunctionDeletion::new(database.clone(), workspaces.clone()),
            database,
            workspaces,
            compiler,
        }
    }

    /// Atomically creates or replaces the current definition at one owned path.
    ///
    /// Source is compiled before a write transaction. A successful final
    /// transaction rechecks active Workspace authority and every directly named
    /// Saved Function dependency, then commits source, description, declaration,
    /// bounded diagnostics, and one lifecycle Event. Occurrence time is metadata
    /// and never a stale-write guard. Tools stay untyped; use
    /// [`Self::prepare_save`] with a reader to type them.
    ///
    /// This save does not notify Universal Search: coral-search depends on
    /// this crate, so the call would be a cycle. A production caller notifies
    /// `SearchIndex::index_function` after this returns, as the Task command's
    /// save does. Until then, the reconcile task indexes the function.
    ///
    /// # Errors
    ///
    /// Returns input, authority, authoring, timeout, or persistence failure.
    /// Failure leaves both the current definition and lifecycle evidence unchanged.
    pub async fn save(
        &self,
        context: &SavedFunctionChangeContext,
        workspace_id: &WorkspaceId,
        save: SavedFunctionSave,
    ) -> Result<SavedFunction, Error> {
        let prepared = self.prepare_save(context, workspace_id, save, None).await?;
        let mut transaction = self.database.begin().await?;
        let function = self.save_in_transaction(&mut transaction, prepared).await?;
        transaction.commit().await?;
        Ok(function)
    }

    /// Prepares non-executing authoring work outside the final write transaction.
    ///
    /// The source is typed against the current declarations of the Saved
    /// Functions it names and, through `tools`, of the Tools it names
    /// that the caller can discover. Without `tools`, `coral.providers`
    /// and `coral.workspace` stay untyped. A source that cannot be analyzed is
    /// prepared untyped, so preparation reports its own failure. Type
    /// diagnostics are advisory.
    ///
    /// # Errors
    /// Returns authority, compilation, or authoring-limit failures without changes.
    pub async fn prepare_save(
        &self,
        context: &SavedFunctionChangeContext,
        workspace_id: &WorkspaceId,
        save: SavedFunctionSave,
        tools: Option<&dyn ToolDeclarationReader>,
    ) -> Result<PreparedSave, Error> {
        self.authorize_active(context.actor(), workspace_id).await?;
        let prepared = self
            .prepare_source(
                context.actor(),
                workspace_id,
                save.path(),
                save.source().as_str(),
                tools,
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
    ) -> Result<SavedFunction, Error> {
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
                &prepared.references,
            )
            .await?;
        let (id, revision, action) = match previous.as_ref() {
            None => (SavedFunctionId::generate(), 1, LifecycleAction::Created),
            Some(previous) => (
                previous.id().clone(),
                previous.revision.checked_add(1).ok_or_else(|| {
                    Error::CorruptData("Saved Function revision overflowed".to_string())
                })?,
                LifecycleAction::Replaced,
            ),
        };
        let function = SavedFunction::new(
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
        );
        authority.require_current_at(now_unix_nanos()?)?;
        match previous.as_ref() {
            None => repository::insert_current(transaction, &function).await?,
            Some(previous) => {
                repository::replace_current(transaction, previous, &function).await?;
            }
        }
        repository::insert_lifecycle_event(
            transaction,
            repository::NewLifecycleEvent {
                workspace_id,
                actor: context.actor().principal(),
                private_user_id: (function.principal().kind()
                    == coral_workspace::PrincipalKind::User)
                    .then_some(function.principal().id()),
                action,
                target_id: function.id(),
                target_path: function.path(),
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
        Ok(function)
    }

    /// Returns the current definition at one path owned by the authenticated Principal.
    ///
    /// # Errors
    ///
    /// An absent, inaccessible, or differently owned path returns
    /// [`Error::SavedFunctionNotFound`].
    pub async fn get(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        path: &SavedFunctionPath,
    ) -> Result<SavedFunction, Error> {
        self.inspection.get(actor, workspace_id, path).await
    }

    /// Lists one Workspace-owned Agent's current Saved Functions for a Workspace Owner.
    ///
    /// User-owned functions remain private and cannot be selected through this Interface.
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
    ) -> Result<Vec<SavedFunction>, Error> {
        let user = actor.authenticated_user().ok_or(Error::PermissionDenied(
            "only a Workspace Owner can inspect Agent Saved Functions",
        ))?;
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.authorize_agent_inspection_in_transaction(&mut transaction, user, workspace_id)
            .await?;
        let agent = Principal::from_parts(agent_id.clone(), PrincipalKind::Agent);
        let functions =
            repository::list_current_in_transaction(&mut transaction, workspace_id, &agent).await?;
        transaction.commit().await?;
        Ok(functions)
    }

    /// Returns one Workspace-owned Agent Saved Function to a Workspace Owner.
    ///
    /// # Errors
    ///
    /// Requires Workspace Owner authority. An absent path remains
    /// [`Error::SavedFunctionNotFound`].
    pub async fn get_agent(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        agent_id: &PrincipalId,
        path: &SavedFunctionPath,
    ) -> Result<SavedFunction, Error> {
        let user = actor
            .authenticated_user()
            .ok_or(Error::SavedFunctionNotFound)?;
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.authorize_agent_inspection_in_transaction(&mut transaction, user, workspace_id)
            .await
            .map_err(conceal_inspection_error)?;
        let agent = Principal::from_parts(agent_id.clone(), PrincipalKind::Agent);
        let function =
            repository::find_current_in_transaction(&mut transaction, workspace_id, &agent, path)
                .await?
                .ok_or(Error::SavedFunctionNotFound)?;
        transaction.commit().await?;
        Ok(function)
    }

    /// Atomically deletes one Workspace-owned Agent Saved Function as a Workspace Owner.
    ///
    /// The accountable User remains the lifecycle actor and never authors or
    /// executes as the Agent.
    ///
    /// # Errors
    ///
    /// Requires Workspace Owner authority in an active Workspace. Absence is
    /// [`Error::SavedFunctionNotFound`].
    pub async fn delete_agent(
        &self,
        context: &SavedFunctionChangeContext,
        workspace_id: &WorkspaceId,
        agent_id: &PrincipalId,
        path: &SavedFunctionPath,
    ) -> Result<SavedFunctionId, Error> {
        let user = context
            .actor()
            .authenticated_user()
            .ok_or(Error::SavedFunctionNotFound)?;
        let mut transaction = self.database.begin().await?;
        self.workspaces
            .check_configuration_in_transaction(&mut transaction, user, workspace_id)
            .await
            .map_err(conceal_agent_function_error)?;
        let agent = Principal::from_parts(agent_id.clone(), PrincipalKind::Agent);
        let function =
            repository::find_current_in_transaction(&mut transaction, workspace_id, &agent, path)
                .await?
                .ok_or(Error::SavedFunctionNotFound)?;
        let deleted_id =
            delete_current_with_event(&mut transaction, context, workspace_id, &function).await?;
        transaction.commit().await?;
        Ok(deleted_id)
    }

    /// Atomically deletes one current path and records its lifecycle Event.
    ///
    /// # Errors
    ///
    /// Requires the owning Principal and an active Workspace. Absence or other
    /// ownership is concealed as [`Error::SavedFunctionNotFound`].
    pub async fn delete(
        &self,
        context: &SavedFunctionChangeContext,
        workspace_id: &WorkspaceId,
        path: &SavedFunctionPath,
    ) -> Result<SavedFunctionId, Error> {
        self.deletion.delete(context, workspace_id, path).await
    }

    /// Stages deletion and lifecycle evidence in the caller's transaction.
    ///
    /// # Errors
    /// Rechecks authority and exact current ownership. Failure requires rollback.
    pub async fn delete_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        context: &SavedFunctionChangeContext,
        workspace_id: &WorkspaceId,
        path: &SavedFunctionPath,
    ) -> Result<SavedFunctionId, Error> {
        self.deletion
            .delete_in_transaction(transaction, context, workspace_id, path)
            .await
    }

    /// Lists the current User's safe Saved Function lifecycle evidence.
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
                "Agents cannot inspect Saved Function lifecycle history",
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
        path: &SavedFunctionPath,
        references: &[SavedFunctionReference],
    ) -> Result<(PrincipalWorkspaceEvidence, Option<SavedFunction>), Error> {
        let authority = self
            .authorize_active_in_transaction(transaction, actor, workspace_id)
            .await?;
        // The Workspace hold acquired above serializes namespace decisions for
        // both absent and existing paths across SQLite and Postgres.
        let current_functions =
            repository::list_current_in_transaction(transaction, workspace_id, actor.principal())
                .await?;
        if current_functions
            .iter()
            .any(|function| saved_function_paths_overlap(function.path(), path))
        {
            return Err(Error::SavedFunctionAdmission(
                "Saved Function Path conflicts with an existing virtual namespace".to_string(),
            ));
        }

        let mut held_paths = references
            .iter()
            .map(|reference| reference.path.clone())
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
                && let Some(reference) = references
                    .iter()
                    .find(|reference| &reference.path == held_path)
            {
                return Err(missing_saved_function_dependency(path, reference));
            }
        }
        let previous = current_functions
            .into_iter()
            .find(|function| function.path() == path);
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
                "only a Workspace Owner can inspect Agent Saved Functions",
            ));
        }
        Ok(())
    }

    async fn prepare_source(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        path: &SavedFunctionPath,
        source: &str,
        tools: Option<&dyn ToolDeclarationReader>,
    ) -> Result<PreparedSavedFunction, Error> {
        let source_name = format!("Saved Function coral.functions.{}", path.as_str());
        let deadline = Instant::now() + COMPILATION_TIMEOUT;
        let preparation = async {
            let declarations = self
                .declarations(actor, workspace_id, source, tools, deadline)
                .await?;
            let compiler = self.compiler.clone();
            let source = source.to_string();
            tokio::task::spawn_blocking(move || {
                compiler.prepare(deadline, &source, declarations.as_ref())
            })
            .await
            .map_err(|_join_error| Error::CompilerWorkerStopped)?
            .map_err(|failure| match failure {
                CompileFailure::Rejected(message) => {
                    Error::SavedFunctionAdmission(format!("{source_name}: {message}"))
                }
                CompileFailure::Busy => Error::CompilerBusy,
                CompileFailure::TimedOut => Error::SavedFunctionCompilationTimedOut,
                CompileFailure::Stopped => Error::CompilerWorkerStopped,
                CompileFailure::Failed(kind) => Error::CompilerUnavailable(kind.to_string()),
            })
        };
        timeout(COMPILATION_TIMEOUT, preparation)
            .await
            .map_err(|_elapsed| Error::SavedFunctionCompilationTimedOut)?
    }

    /// The declarations that type `source`: the current ones of the Saved
    /// Functions it names and, through `tools`, of the Tools it
    /// names that the caller can discover. `None` leaves it untyped.
    ///
    /// Typing stops by its own deadline, so a compile due by `deadline`
    /// always keeps its reserve. What typing cannot finish stays untyped.
    async fn declarations(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        source: &str,
        tools: Option<&dyn ToolDeclarationReader>,
        deadline: Instant,
    ) -> Result<Option<CoralDeclarations>, Error> {
        let Some(typing_deadline) = tool_typing::typing_deadline(deadline) else {
            tracing::warn!("No time is left to type the source; it is checked untyped");
            return Ok(None);
        };
        let typing = async {
            let compiler = self.compiler.clone();
            let analyzed = source.to_string();
            let Some(named) = tokio::task::spawn_blocking(move || {
                compiler.call_paths(typing_deadline, &analyzed)
            })
            .await
            .map_err(|_join_error| Error::CompilerWorkerStopped)?
            else {
                return Ok(None);
            };
            let Some(function_paths) = named
                .functions()
                .iter()
                .map(|reference| SavedFunctionPath::parse(reference.path()).ok())
                .collect::<Option<Vec<_>>>()
            else {
                return Ok(None);
            };
            let functions = if function_paths.is_empty() {
                None
            } else {
                self.function_declarations(actor, workspace_id, &function_paths)
                    .await
                    .inspect_err(|error| {
                        tracing::warn!(
                            error.type = safe::kind(error.code()),
                            "Saved Function declarations are unavailable; they are checked untyped"
                        );
                    })
                    .ok()
            };
            let tools = match tools {
                Some(reader) => {
                    tool_typing::tool_declarations(reader, named.tools().call_paths()).await
                }
                None => None,
            };
            if functions.is_none() && tools.is_none() {
                return Ok(None);
            }
            let mut declarations = CoralDeclarations::new(SAVED_FUNCTION_DECLARATION_MAX_BYTES);
            if let Some(functions) = functions {
                declarations = declarations.with_functions(functions);
            }
            if let Some(tools) = tools {
                declarations = declarations.with_tools(named.tools(), tools);
            }
            Ok(Some(declarations))
        };
        timeout_at(typing_deadline.into(), typing)
            .await
            .unwrap_or_else(|_elapsed| {
                tracing::warn!("Typing was too slow; the source is checked untyped");
                Ok(None)
            })
    }

    /// The current declaration of each named Saved Function the caller owns.
    async fn function_declarations(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        paths: &[SavedFunctionPath],
    ) -> Result<Vec<CallableDeclaration>, Error> {
        let mut functions = Vec::new();
        if paths.is_empty() {
            return Ok(functions);
        }
        let mut transaction = self.database.begin_read_snapshot().await?;
        for path in paths {
            if let Some(function) = repository::find_current_in_transaction(
                &mut transaction,
                workspace_id,
                actor.principal(),
                path,
            )
            .await?
            {
                functions.push(CallableDeclaration::new(
                    path.segments(),
                    function.declaration().as_str().to_string(),
                ));
            }
        }
        transaction.commit().await?;
        Ok(functions)
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

fn missing_saved_function_dependency(
    source_path: &SavedFunctionPath,
    reference: &SavedFunctionReference,
) -> Error {
    Error::SavedFunctionAdmission(format!(
        "Saved Function coral.functions.{}:{}:{} references missing Saved Function coral.functions.{}",
        source_path.as_str(),
        reference.line,
        reference.column,
        reference.path.as_str()
    ))
}

fn saved_function_paths_overlap(
    current: &SavedFunctionPath,
    requested: &SavedFunctionPath,
) -> bool {
    current != requested
        && (requested
            .as_str()
            .strip_prefix(current.as_str())
            .is_some_and(|suffix| suffix.starts_with('.'))
            || current
                .as_str()
                .strip_prefix(requested.as_str())
                .is_some_and(|suffix| suffix.starts_with('.')))
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
    let change = SavedFunctionChangeContext::new(
        AuthenticatedPrincipal::user(context.actor().clone()),
        context.occurred_at_unix_nanos(),
        context.trace_id().clone(),
    );
    for scope in prepared.lost_access() {
        let functions = repository::list_current_in_transaction(
            transaction,
            scope.workspace_id(),
            scope.principal(),
        )
        .await?;
        for function in functions {
            delete_current_with_event(transaction, &change, scope.workspace_id(), &function)
                .await?;
        }
    }
    Ok(())
}

pub(crate) async fn delete_current_with_event(
    transaction: &mut Transaction<'_>,
    context: &SavedFunctionChangeContext,
    workspace_id: &WorkspaceId,
    function: &SavedFunction,
) -> Result<SavedFunctionId, Error> {
    repository::delete_current(transaction, function).await?;
    repository::insert_lifecycle_event(
        transaction,
        repository::NewLifecycleEvent {
            workspace_id,
            actor: context.actor().principal(),
            private_user_id: (function.principal().kind() == coral_workspace::PrincipalKind::User)
                .then_some(function.principal().id()),
            action: LifecycleAction::Deleted,
            target_id: function.id(),
            target_path: function.path(),
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
    Ok(function.id().clone())
}

pub(crate) fn conceal_inspection_error(error: Error) -> Error {
    match error {
        Error::PermissionDenied(_)
        | Error::Workspace(
            coral_workspace::Error::WorkspaceNotFound | coral_workspace::Error::PermissionDenied(_),
        ) => Error::SavedFunctionNotFound,
        other => other,
    }
}

fn conceal_agent_function_error(error: coral_workspace::Error) -> Error {
    match error {
        coral_workspace::Error::WorkspaceNotFound | coral_workspace::Error::PermissionDenied(_) => {
            Error::SavedFunctionNotFound
        }
        other => Error::Workspace(other),
    }
}
