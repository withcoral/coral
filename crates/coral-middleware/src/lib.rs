//! Workspace-owned Middleware for Coral.
//!
//! [`MiddlewareModule`] owns current Middleware definitions: their source,
//! description, scopes, chain order and activation, with atomic
//! Administrative Lifecycle Events. Only a Workspace Owner User can read or
//! change Middleware. Every accepted save, scope change or move disables the
//! changed Middleware, so an Owner always enables reviewed code explicitly.
//!
//! Saving validates source with Coral's compiler-only V8 adapter outside the
//! write transaction; a failed save changes neither the definition nor its
//! evidence. Run admission selects the enabled chain with
//! [`select_enabled_chain_in_transaction`] and holds it unchanged at final
//! acceptance with [`hold_chain_in_transaction`]; Run owns compilation and the
//! pinned snapshot.

mod error;
mod repository;
mod types;

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use coral_db::{Database, Transaction};
use coral_workspace::{
    AuthenticatedUser, AuthoritySource, WorkspaceAction, WorkspaceId, WorkspaceModule,
    WorkspaceRole,
};

pub use coral_v8::DiagnosticCategory as MiddlewareDiagnosticCategory;
pub use error::Error;
pub use types::{
    AdministrativeLifecycleEvent, LifecycleAction, MIDDLEWARE_DESCRIPTION_MAX_BYTES,
    MIDDLEWARE_DIAGNOSTIC_MAX_COUNT, MIDDLEWARE_MAX_COUNT, MIDDLEWARE_NAME_MAX_BYTES,
    MIDDLEWARE_SOURCE_MAX_BYTES, Middleware, MiddlewareChangeContext, MiddlewareDescription,
    MiddlewareDiagnostic, MiddlewareId, MiddlewareName, MiddlewareSave, MiddlewareScopes,
    MiddlewareSelection, MiddlewareSource, MoveDirection,
};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Returns the Middleware Module's domain-owned migration plan.
#[must_use]
pub const fn migrator() -> &'static sqlx::migrate::Migrator {
    &MIGRATOR
}

const COMPILATION_TIMEOUT: Duration = coral_v8::COMPILATION_TIMEOUT;

pub(crate) trait MiddlewareCompiler: Send + Sync {
    fn prepare(&self, deadline: Instant, source: &str)
    -> Result<Vec<MiddlewareDiagnostic>, String>;
}

struct V8MiddlewareCompiler {
    compiler: coral_v8::ProgramCompiler,
}

impl MiddlewareCompiler for V8MiddlewareCompiler {
    fn prepare(
        &self,
        deadline: Instant,
        source: &str,
    ) -> Result<Vec<MiddlewareDiagnostic>, String> {
        let compiled = self
            .compiler
            .prepare_middleware(deadline, source)
            .map_err(coral_v8::format_compiler_error)?;
        Ok(compiled
            .diagnostics()
            .iter()
            .take(MIDDLEWARE_DIAGNOSTIC_MAX_COUNT)
            .map(|diagnostic| MiddlewareDiagnostic {
                category: diagnostic.category(),
                code: diagnostic.code(),
                message: diagnostic.message().to_string(),
                line: diagnostic.line(),
                column: diagnostic.column(),
            })
            .collect())
    }
}

/// Opaque validated save, bound to its exact authenticated scope.
pub struct PreparedMiddlewareSave {
    context: MiddlewareChangeContext,
    workspace_id: WorkspaceId,
    save: MiddlewareSave,
    diagnostics: Vec<MiddlewareDiagnostic>,
}

/// Current Workspace Middleware lifecycle Module.
#[derive(Clone)]
pub struct MiddlewareModule {
    database: Arc<Database>,
    workspaces: WorkspaceModule,
    compiler: Option<Arc<dyn MiddlewareCompiler>>,
}

impl MiddlewareModule {
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
        let compiler = coral_v8::ProgramCompiler::new(isolate_limits)
            .map_err(|error| Error::CompilerUnavailable(error.to_string()))?;
        Ok(Self {
            database,
            workspaces,
            compiler: Some(Arc::new(V8MiddlewareCompiler { compiler })),
        })
    }

    /// Builds a management-only Module that reads and administers Middleware
    /// without a compiler. Saving through it fails as unavailable.
    #[must_use]
    pub const fn without_authoring(database: Arc<Database>, workspaces: WorkspaceModule) -> Self {
        Self {
            database,
            workspaces,
            compiler: None,
        }
    }

    /// Creates or replaces one Middleware by name and leaves it disabled.
    ///
    /// A new Middleware joins the chain innermost with no scopes. A replaced
    /// Middleware keeps its scopes and position.
    ///
    /// # Errors
    ///
    /// Returns input, authority, authoring, timeout, limit or persistence
    /// failure. Failure leaves the accepted definition and evidence unchanged.
    pub async fn save(
        &self,
        context: &MiddlewareChangeContext,
        workspace_id: &WorkspaceId,
        save: MiddlewareSave,
    ) -> Result<Middleware, Error> {
        let prepared = self.prepare_save(context, workspace_id, save).await?;
        let mut transaction = self.database.begin().await?;
        let middleware = self.save_in_transaction(&mut transaction, prepared).await?;
        transaction.commit().await?;
        Ok(middleware)
    }

    /// Validates source outside the final write transaction.
    ///
    /// # Errors
    ///
    /// Returns authority or authoring failures without changes.
    pub async fn prepare_save(
        &self,
        context: &MiddlewareChangeContext,
        workspace_id: &WorkspaceId,
        save: MiddlewareSave,
    ) -> Result<PreparedMiddlewareSave, Error> {
        let user = owner_user(context)?;
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.require_owner_in_snapshot(&mut transaction, user, workspace_id)
            .await?;
        transaction.commit().await?;
        let diagnostics = self.prepare_source(save.name(), save.source()).await?;
        Ok(PreparedMiddlewareSave {
            context: context.clone(),
            workspace_id: workspace_id.clone(),
            save,
            diagnostics,
        })
    }

    /// Stages a validated save and its lifecycle evidence; the caller commits.
    ///
    /// # Errors
    ///
    /// Rechecks Owner authority and the Workspace limit. Failure requires rollback.
    pub async fn save_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        prepared: PreparedMiddlewareSave,
    ) -> Result<Middleware, Error> {
        let PreparedMiddlewareSave {
            context,
            workspace_id,
            save,
            diagnostics,
        } = prepared;
        let (user, source) = self
            .hold_owner(transaction, &context, &workspace_id)
            .await?;
        let chain = repository::list_in_transaction(transaction, &workspace_id).await?;
        let now = context.occurred_at_unix_nanos();
        let (middleware, action) =
            if let Some(previous) = chain.iter().find(|item| item.name() == save.name()) {
                (
                    Middleware {
                        description: save.description().clone(),
                        source: save.source().clone(),
                        diagnostics,
                        enabled: false,
                        revision: next_revision(previous)?,
                        updated_at_unix_nanos: now,
                        ..previous.clone()
                    },
                    LifecycleAction::Updated,
                )
            } else {
                if chain.len() >= MIDDLEWARE_MAX_COUNT {
                    return Err(Error::LimitExceeded(format!(
                        "a Workspace can hold at most {MIDDLEWARE_MAX_COUNT} Middleware"
                    )));
                }
                (
                    Middleware {
                        id: MiddlewareId::generate(),
                        workspace_id: workspace_id.clone(),
                        name: save.name().clone(),
                        description: save.description().clone(),
                        source: save.source().clone(),
                        diagnostics,
                        scopes: MiddlewareScopes::default(),
                        enabled: false,
                        position: u32::try_from(chain.len()).map_err(|_overflow| {
                            Error::CorruptData("Middleware chain is too long".to_string())
                        })?,
                        revision: 1,
                        created_at_unix_nanos: now,
                        updated_at_unix_nanos: now,
                    },
                    LifecycleAction::Created,
                )
            };
        if action == LifecycleAction::Created {
            repository::insert(transaction, &middleware).await?;
        } else {
            repository::replace(transaction, &middleware).await?;
        }
        record(
            transaction,
            &context,
            user,
            source,
            &middleware,
            action,
            &["description", "source", "diagnostics", "enabled"],
        )
        .await?;
        Ok(middleware)
    }

    /// Lists the Workspace chain, outermost first.
    ///
    /// # Errors
    ///
    /// Requires Workspace Owner authority.
    pub async fn list(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<Middleware>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.require_owner_in_snapshot(&mut transaction, actor, workspace_id)
            .await?;
        let chain = repository::list_in_transaction(&mut transaction, workspace_id).await?;
        transaction.commit().await?;
        Ok(chain)
    }

    /// Returns one Middleware with its source.
    ///
    /// # Errors
    ///
    /// Requires Workspace Owner authority; absence is [`Error::MiddlewareNotFound`].
    pub async fn get(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
        id: &MiddlewareId,
    ) -> Result<Middleware, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.require_owner_in_snapshot(&mut transaction, actor, workspace_id)
            .await
            .map_err(conceal)?;
        let middleware = repository::find_in_transaction(&mut transaction, workspace_id, id)
            .await?
            .ok_or(Error::MiddlewareNotFound)?;
        transaction.commit().await?;
        Ok(middleware)
    }

    /// Enables or disables one Middleware for future Runs.
    ///
    /// Repeating the current state changes nothing and records no Event.
    ///
    /// # Errors
    ///
    /// Requires Workspace Owner authority in an active Workspace.
    pub async fn set_enabled(
        &self,
        context: &MiddlewareChangeContext,
        workspace_id: &WorkspaceId,
        id: &MiddlewareId,
        enabled: bool,
    ) -> Result<Middleware, Error> {
        self.change(context, workspace_id, id, |current, _chain| {
            if current.enabled == enabled {
                return Ok(None);
            }
            let action = if enabled {
                LifecycleAction::Enabled
            } else {
                LifecycleAction::Disabled
            };
            Ok(Some((
                vec![Middleware {
                    enabled,
                    ..current.clone()
                }],
                action,
                &["enabled"][..],
            )))
        })
        .await
    }

    /// Replaces one Middleware's scopes and disables it.
    ///
    /// # Errors
    ///
    /// Requires Workspace Owner authority in an active Workspace.
    pub async fn set_scopes(
        &self,
        context: &MiddlewareChangeContext,
        workspace_id: &WorkspaceId,
        id: &MiddlewareId,
        scopes: MiddlewareScopes,
    ) -> Result<Middleware, Error> {
        self.change(context, workspace_id, id, |current, _chain| {
            if current.scopes == scopes {
                return Ok(None);
            }
            Ok(Some((
                vec![Middleware {
                    scopes,
                    enabled: false,
                    ..current.clone()
                }],
                LifecycleAction::ScopesUpdated,
                &["scopes", "enabled"][..],
            )))
        })
        .await
    }

    /// Moves one Middleware one step and disables it. The neighbor it swaps
    /// with keeps its activation.
    ///
    /// # Errors
    ///
    /// Rejects moving past either end of the chain. Requires Workspace Owner
    /// authority in an active Workspace.
    pub async fn move_middleware(
        &self,
        context: &MiddlewareChangeContext,
        workspace_id: &WorkspaceId,
        id: &MiddlewareId,
        direction: MoveDirection,
    ) -> Result<Middleware, Error> {
        self.change(context, workspace_id, id, |current, chain| {
            let target = match direction {
                MoveDirection::Up => current.position.checked_sub(1),
                MoveDirection::Down => current.position.checked_add(1),
            };
            let neighbor = target
                .and_then(|position| chain.iter().find(|item| item.position == position))
                .ok_or_else(|| {
                    Error::InvalidInput("Middleware is already at that end of the chain".into())
                })?;
            Ok(Some((
                vec![
                    Middleware {
                        position: neighbor.position,
                        enabled: false,
                        ..current.clone()
                    },
                    Middleware {
                        position: current.position,
                        ..neighbor.clone()
                    },
                ],
                LifecycleAction::Moved,
                &["position", "enabled"][..],
            )))
        })
        .await
    }

    /// Deletes one Middleware. Later Middleware move one step outward.
    /// In-progress Runs keep their pinned chain.
    ///
    /// # Errors
    ///
    /// Requires Workspace Owner authority in an active Workspace.
    pub async fn delete(
        &self,
        context: &MiddlewareChangeContext,
        workspace_id: &WorkspaceId,
        id: &MiddlewareId,
    ) -> Result<MiddlewareId, Error> {
        let mut transaction = self.database.begin().await?;
        let (user, source) = self
            .hold_owner(&mut transaction, context, workspace_id)
            .await
            .map_err(conceal)?;
        let chain = repository::list_in_transaction(&mut transaction, workspace_id).await?;
        let current = chain
            .iter()
            .find(|item| item.id() == id)
            .ok_or(Error::MiddlewareNotFound)?;
        repository::delete(&mut transaction, current).await?;
        for later in chain.iter().filter(|item| item.position > current.position) {
            // A shifted neighbor keeps its activation but gets a new revision,
            // as in every other change, so Run admission sees the new order.
            repository::replace(
                &mut transaction,
                &Middleware {
                    position: later.position - 1,
                    revision: next_revision(later)?,
                    updated_at_unix_nanos: context.occurred_at_unix_nanos(),
                    ..later.clone()
                },
            )
            .await?;
        }
        record(
            &mut transaction,
            context,
            user,
            source,
            current,
            LifecycleAction::Deleted,
            &["source", "description", "scopes", "position", "enabled"],
        )
        .await?;
        transaction.commit().await?;
        Ok(id.clone())
    }

    /// Lists safe Middleware lifecycle evidence, oldest first.
    ///
    /// # Errors
    ///
    /// Requires Workspace Owner authority.
    pub async fn list_lifecycle_events(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<AdministrativeLifecycleEvent>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.require_owner_in_snapshot(&mut transaction, actor, workspace_id)
            .await?;
        let events =
            repository::list_lifecycle_events_in_transaction(&mut transaction, workspace_id)
                .await?;
        transaction.commit().await?;
        Ok(events)
    }

    async fn change(
        &self,
        context: &MiddlewareChangeContext,
        workspace_id: &WorkspaceId,
        id: &MiddlewareId,
        plan: impl FnOnce(
            &Middleware,
            &[Middleware],
        ) -> Result<
            Option<(Vec<Middleware>, LifecycleAction, &'static [&'static str])>,
            Error,
        >,
    ) -> Result<Middleware, Error> {
        let mut transaction = self.database.begin().await?;
        let (user, source) = self
            .hold_owner(&mut transaction, context, workspace_id)
            .await
            .map_err(conceal)?;
        let chain = repository::list_in_transaction(&mut transaction, workspace_id).await?;
        let current = chain
            .iter()
            .find(|item| item.id() == id)
            .ok_or(Error::MiddlewareNotFound)?;
        let Some((mut changed, action, fields)) = plan(current, &chain)? else {
            transaction.rollback().await?;
            return Ok(current.clone());
        };
        for item in &mut changed {
            // Every change, including a shifted neighbor's position, gets a new
            // revision so Run admission detects a changed chain.
            item.revision = next_revision(item)?;
            item.updated_at_unix_nanos = context.occurred_at_unix_nanos();
            repository::replace(&mut transaction, item).await?;
        }
        let updated = changed
            .into_iter()
            .find(|item| item.id == current.id)
            .ok_or_else(|| Error::CorruptData("Middleware change lost its target".to_string()))?;
        record(
            &mut transaction,
            context,
            user,
            source,
            &updated,
            action,
            fields,
        )
        .await?;
        transaction.commit().await?;
        Ok(updated)
    }

    async fn hold_owner<'c>(
        &self,
        transaction: &mut Transaction<'_>,
        context: &'c MiddlewareChangeContext,
        workspace_id: &WorkspaceId,
    ) -> Result<(&'c AuthenticatedUser, AuthoritySource), Error> {
        let user = owner_user(context)?;
        let evidence = self
            .workspaces
            .check_configuration_in_transaction(transaction, user, workspace_id)
            .await?;
        Ok((user, evidence.authority_source()))
    }

    async fn require_owner_in_snapshot(
        &self,
        transaction: &mut Transaction<'_>,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
    ) -> Result<(), Error> {
        let evidence = self
            .workspaces
            .check_in_transaction(transaction, actor, workspace_id, WorkspaceAction::Inspect)
            .await?;
        if evidence.effective_role() == WorkspaceRole::Owner {
            Ok(())
        } else {
            Err(Error::PermissionDenied(
                "only a Workspace Owner can manage Middleware",
            ))
        }
    }

    async fn prepare_source(
        &self,
        name: &MiddlewareName,
        source: &MiddlewareSource,
    ) -> Result<Vec<MiddlewareDiagnostic>, Error> {
        let compiler = self.compiler.clone().ok_or_else(|| {
            Error::CompilerUnavailable("Middleware authoring is unavailable".into())
        })?;
        let source = source.as_str().to_string();
        let deadline = Instant::now() + COMPILATION_TIMEOUT;
        let preparation = tokio::task::spawn_blocking(move || compiler.prepare(deadline, &source));
        match tokio::time::timeout(COMPILATION_TIMEOUT, preparation).await {
            Err(_elapsed) => Err(Error::CompilationTimedOut),
            Ok(Err(_join_error)) => Err(Error::CompilerWorkerStopped),
            Ok(Ok(Err(message))) => Err(Error::Authoring(format!(
                "Middleware {}: {message}",
                name.as_str()
            ))),
            Ok(Ok(Ok(diagnostics))) => Ok(diagnostics),
        }
    }
}

fn owner_user(context: &MiddlewareChangeContext) -> Result<&AuthenticatedUser, Error> {
    context
        .actor()
        .authenticated_user()
        .ok_or(Error::PermissionDenied(
            "only a Workspace Owner can manage Middleware",
        ))
}

fn next_revision(middleware: &Middleware) -> Result<i64, Error> {
    middleware
        .revision
        .checked_add(1)
        .ok_or_else(|| Error::CorruptData("Middleware revision overflowed".to_string()))
}

async fn record(
    transaction: &mut Transaction<'_>,
    context: &MiddlewareChangeContext,
    user: &AuthenticatedUser,
    source: AuthoritySource,
    middleware: &Middleware,
    action: LifecycleAction,
    fields: &[&str],
) -> Result<(), Error> {
    repository::insert_lifecycle_event(
        transaction,
        repository::NewLifecycleEvent {
            workspace_id: middleware.workspace_id(),
            actor_user_id: user.user_id().as_str(),
            authority_source: source,
            action,
            target: middleware,
            changed_field_paths: fields,
            occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
            trace_id: context.trace_id().as_str(),
        },
    )
    .await
}

fn conceal(error: Error) -> Error {
    match error {
        Error::Workspace(
            coral_workspace::Error::WorkspaceNotFound | coral_workspace::Error::PermissionDenied(_),
        ) => Error::MiddlewareNotFound,
        other => other,
    }
}

/// Selects the enabled chain, outermost first, for Run admission.
///
/// The caller owns a coherent read transaction and authority checks.
///
/// # Errors
///
/// Returns a persistence or corrupt-data failure.
pub async fn select_enabled_chain_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Vec<MiddlewareSelection>, Error> {
    Ok(repository::list_in_transaction(transaction, workspace_id)
        .await?
        .into_iter()
        .filter(Middleware::enabled)
        .map(|middleware| MiddlewareSelection {
            id: middleware.id,
            name: middleware.name,
            source: middleware.source,
            scopes: middleware.scopes,
            revision: middleware.revision,
        })
        .collect())
}

/// Rechecks the enabled chain inside Run acceptance and reports whether it
/// still matches the compiled selection exactly, in order and revision.
///
/// The caller must already hold the Workspace, as Run admission authority
/// does. Every Middleware change takes the same hold, so the answer stays
/// true until the acceptance transaction ends.
///
/// # Errors
///
/// Returns a persistence or corrupt-data failure.
pub async fn hold_chain_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    selection: &[MiddlewareSelection],
) -> Result<bool, Error> {
    let current = select_enabled_chain_in_transaction(transaction, workspace_id).await?;
    Ok(current.len() == selection.len()
        && current.iter().zip(selection).all(|(current, selected)| {
            current.id == selected.id && current.revision == selected.revision
        }))
}

/// Returns the current wall-clock time for Adapters that build change context.
///
/// # Errors
///
/// Returns a clock failure outside the durable nanosecond range.
pub fn now_unix_nanos() -> Result<i64, Error> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::Clock(error.to_string()))?;
    i64::try_from(elapsed.as_nanos())
        .map_err(|_error| Error::Clock("current time exceeds durable nanosecond range".to_string()))
}

#[cfg(test)]
mod tests;
