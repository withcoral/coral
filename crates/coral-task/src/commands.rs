//! Attributed command choreography. Tool decisions and writes stay with their owners.
use crate::{
    AttemptEvidence, AttemptKind, AttributedCommand, DiscoveryContext, Error, InteractionId,
    NewTaskCommand, TaskModule,
};
use crate::{interaction_repository, repository};
use coral_db::Transaction;
use coral_run::RunModule;
use coral_search::{SearchPage, SearchRequest, UniversalSearchModule as SearchModule};
use coral_telemetry::safe;
use std::time::{SystemTime, UNIX_EPOCH};

/// The longest Call Path `describe` accepts. It lives here, not in `CallPath::parse`,
/// until the catalog marks a Tool with a longer Call Path unavailable: search
/// builds Tool Call Paths through the same type.
const DESCRIBE_CALL_PATH_MAX_BYTES: usize = 1024;

/// Task-aware commands above independently usable operation domains.
#[derive(Clone)]
pub struct TaskCommands {
    tasks: TaskModule,
    search: SearchModule,
    runs: RunModule,
    #[cfg(test)]
    pub(crate) control: std::sync::Arc<crate::command_tests::Control>,
}
impl TaskCommands {
    /// Composes real domain modules; no callbacks or generic operation registry.
    #[must_use]
    pub fn new(tasks: TaskModule, search: SearchModule, runs: RunModule) -> Self {
        Self {
            tasks,
            search,
            runs,
            #[cfg(test)]
            control: std::sync::Arc::default(),
        }
    }

    /// Retains exactly one requested search page before returning it.
    ///
    /// No MCP tool calls this: agents discover with `coral.workspace.search`
    /// inside a Run. It still writes the `search` record that Task history
    /// keeps from before, so tests of retained history make one as Coral did.
    /// # Errors
    /// Begin failure prevents discovery; incomplete persistence never implies success.
    pub async fn search(
        &self,
        call: &AttributedCommand,
        input: coral_search::SearchInput,
    ) -> Result<SearchPage, Error> {
        let id = self.begin(call, AttemptKind::Search).await?;
        let request = match SearchRequest::try_from(input) {
            Ok(request) => request,
            Err(error) => return Err(self.reject(call, &id, error.into()).await),
        };
        let page = match self
            .search
            .search(&call.actor, &call.workspace_id, &request)
            .await
        {
            Ok(page) => page,
            Err(error) => return Err(self.reject(call, &id, error.into()).await),
        };
        let evidence = AttemptEvidence::Discovery {
            context: DiscoveryContext::Search {
                query: request
                    .search_query()
                    .map(|query| query.as_str().to_owned()),
                provider: request
                    .provider()
                    .map(|provider| coral_search::provider_filter_value(provider).to_owned()),
                limit: request.limit().value(),
                offset: request.offset(),
            },
            response: coral_search::projection::search(&page),
        };
        let mut transaction = self.tasks.database.begin().await?;
        self.complete(&mut transaction, call, &id, &evidence)
            .await?;
        transaction.commit().await?;
        Ok(page)
    }

    /// Retains the exact description, including editable source and
    /// diagnostics, or the `types` of it that the caller names.
    ///
    /// No MCP tool calls this, as for [`Self::search`]: it writes the
    /// `describe` record Task history keeps from before.
    /// # Errors
    /// Concealed probes never begin; incomplete recording prevents success.
    pub async fn describe(
        &self,
        call: &AttributedCommand,
        path: &str,
        types: &[String],
    ) -> Result<coral_search::ExactAuthoringDescription, Error> {
        let parsed = if path.len() > DESCRIBE_CALL_PATH_MAX_BYTES {
            Err(coral_search::Error::InvalidInput(format!(
                "Call Path must be at most {DESCRIBE_CALL_PATH_MAX_BYTES} UTF-8 bytes"
            )))
        } else {
            coral_search::CallPath::parse(path)
        };
        let path = match parsed {
            Ok(path) => path,
            Err(error) => {
                let id = self.begin(call, AttemptKind::Describe).await?;
                return Err(self.reject(call, &id, error.into()).await);
            }
        };
        self.search
            .check_description_visibility(&call.actor, &call.workspace_id, &path)
            .await?;
        let id = self.begin(call, AttemptKind::Describe).await?;
        let result = async {
            let description = self
                .search
                .describe(&call.actor, &call.workspace_id, &path, types)
                .await?;
            let evidence = AttemptEvidence::Discovery {
                context: DiscoveryContext::Describe {
                    call_path: path.as_str().to_owned(),
                    types: types.to_vec(),
                },
                response: coral_search::projection::exact_description(&description),
            };
            let mut transaction = self.tasks.database.begin().await?;
            self.complete(&mut transaction, call, &id, &evidence)
                .await?;
            transaction.commit().await?;
            Ok(description)
        }
        .await;
        match result {
            Ok(value) => Ok(value),
            Err(error) => Err(self.reject(call, &id, error).await),
        }
    }

    /// Atomically commits Run acceptance and attribution before post-commit launch.
    /// # Errors
    /// Safe admission failures create no Run. A launch error never undoes acceptance.
    pub async fn submit(
        &self,
        call: &AttributedCommand,
        source: &str,
        timeout_ms: u64,
    ) -> Result<coral_run::RunUpdate, Error> {
        let id = self.begin(call, AttemptKind::Submit).await?;
        let accepted = async {
            let submission = coral_run::RunSubmission::new(
                coral_run::ProgramSource::parse(source)?,
                coral_run::RunTimeout::from_millis(timeout_ms)?,
            );
            let prepared = self
                .runs
                .prepare(&call.actor, &call.workspace_id, submission)
                .await?;
            let mut transaction = self.tasks.database.begin().await?;
            self.hold_attempt(&mut transaction, call).await?;
            let staged = self
                .runs
                .accept_in_transaction(&mut transaction, prepared)
                .await?;
            self.complete(
                &mut transaction,
                call,
                &id,
                &AttemptEvidence::Run(staged.run_id().clone()),
            )
            .await?;
            transaction.commit().await?;
            Ok(staged)
        }
        .await;
        let staged = match accepted {
            Ok(staged) => staged,
            Err(error) => return Err(self.reject(call, &id, error).await),
        };
        // This is deliberately outside the rejection path: accepted work remains accepted.
        #[cfg(test)]
        self.control
            .pause(crate::command_tests::Checkpoint::AfterCommit)
            .await;
        Ok(self.runs.launch(staged).await?)
    }

    /// Starts a Task for the call's goal and accepts its first Run in one
    /// commit, then launches the Run. Admission comes first, so a refused
    /// Program starts no Task and leaves nothing to record.
    ///
    /// Once that commit succeeds, the Task is returned beside the launch's
    /// result, even when the launch fails. The caller only has the goal, so
    /// without the Task a retry would start a second one.
    /// # Errors
    /// Safe admission failures create no Task and no Run. A launch error
    /// comes back beside the Task and never undoes acceptance.
    pub async fn submit_new_task(
        &self,
        call: &NewTaskCommand,
        source: &str,
        timeout_ms: u64,
    ) -> Result<(crate::Task, Result<coral_run::RunUpdate, Error>), Error> {
        let submission = coral_run::RunSubmission::new(
            coral_run::ProgramSource::parse(source)?,
            coral_run::RunTimeout::from_millis(timeout_ms)?,
        );
        let prepared = self
            .runs
            .prepare(&call.actor, &call.workspace_id, submission)
            .await?;
        let started_at = now()?;
        let mut transaction = self.tasks.database.begin().await?;
        let task = self
            .tasks
            .start_in_transaction(
                &mut transaction,
                &call.actor,
                &call.workspace_id,
                call.goal.clone(),
                started_at,
            )
            .await?;
        tracing::Span::current().record("coral.task.id", safe::id(&task.telemetry_id()));
        let attributed = AttributedCommand::new(
            call.actor.clone(),
            call.workspace_id.clone(),
            task.id().clone(),
            call.intent.clone(),
        );
        let id = InteractionId::generate();
        interaction_repository::insert(
            &mut transaction,
            &attributed,
            &id,
            AttemptKind::Submit,
            started_at,
        )
        .await?;
        let staged = self
            .runs
            .accept_in_transaction(&mut transaction, prepared)
            .await?;
        self.complete(
            &mut transaction,
            &attributed,
            &id,
            &AttemptEvidence::Run(staged.run_id().clone()),
        )
        .await?;
        transaction.commit().await?;
        #[cfg(test)]
        self.control
            .pause(crate::command_tests::Checkpoint::AfterCommit)
            .await;
        let launched = self.runs.launch(staged).await.map_err(Error::from);
        Ok((task, launched))
    }

    async fn hold_attempt(
        &self,
        transaction: &mut Transaction<'_>,
        call: &AttributedCommand,
    ) -> Result<(), Error> {
        if !repository::hold(transaction, &call.workspace_id, &call.task_id).await? {
            return Err(Error::TaskNotFound);
        }
        Ok(())
    }

    async fn reject(&self, call: &AttributedCommand, id: &InteractionId, error: Error) -> Error {
        let failure = match &error {
            Error::Functions(error) => error.safe_rejection(),
            Error::Run(error) => error.safe_rejection(),
            Error::Search(error) => error.safe_rejection(),
            _ => None,
        };
        if let Some((code, message)) = failure {
            let recorded = async {
                let mut transaction = self.tasks.database.begin().await?;
                self.complete(
                    &mut transaction,
                    call,
                    id,
                    &AttemptEvidence::Rejected(crate::AttemptFailure {
                        code: code.into(),
                        message,
                    }),
                )
                .await?;
                transaction.commit().await?;
                Ok::<_, Error>(())
            }
            .await;
            if let Err(recording_error) = recorded {
                return recording_error;
            }
        }
        error
    }

    async fn begin(
        &self,
        call: &AttributedCommand,
        kind: AttemptKind,
    ) -> Result<InteractionId, Error> {
        let observed_at = now()?;
        self.tasks
            .workspaces
            .check_principal_inspection(&call.actor, &call.workspace_id, observed_at)
            .await?;
        #[cfg(test)]
        self.control
            .pause(crate::command_tests::Checkpoint::BeforeBeginLock)
            .await;
        let mut transaction = self.tasks.database.begin().await?;
        if !repository::hold(&mut transaction, &call.workspace_id, &call.task_id).await? {
            return Err(Error::TaskNotFound);
        }
        let authority = self
            .tasks
            .workspaces
            .check_principal_inspection_in_transaction(
                &mut transaction,
                &call.actor,
                &call.workspace_id,
                now()?,
            )
            .await?;
        let task = repository::find_attributed(
            &mut transaction,
            &call.workspace_id,
            &call.task_id,
            call.actor.principal(),
        )
        .await?
        .ok_or(Error::TaskNotFound)?;
        // The command runs in its caller's span, such as an MCP tool call's.
        tracing::Span::current().record("coral.task.id", safe::id(&task.telemetry_id()));
        if task.outcome().is_some() {
            return Err(Error::TaskClosed);
        }
        let id = InteractionId::generate();
        #[cfg(test)]
        self.control
            .fault(crate::command_tests::Checkpoint::Begin, &mut transaction)
            .await?;
        let now = now()?;
        authority.require_current_at(now)?;
        interaction_repository::insert(
            &mut transaction,
            call,
            &id,
            kind,
            now.max(task.started_at_unix_nanos()),
        )
        .await?;
        repository::touch(&mut transaction, &call.workspace_id, &call.task_id, now).await?;
        #[cfg(test)]
        self.control
            .pause(crate::command_tests::Checkpoint::BeforeBeginCommit)
            .await;
        transaction.commit().await?;
        #[cfg(test)]
        self.control
            .pause(crate::command_tests::Checkpoint::AfterBegin)
            .await;
        Ok(id)
    }

    async fn complete(
        &self,
        transaction: &mut Transaction<'_>,
        call: &AttributedCommand,
        id: &InteractionId,
        evidence: &AttemptEvidence,
    ) -> Result<(), Error> {
        if !repository::hold(transaction, &call.workspace_id, &call.task_id).await? {
            return Err(Error::TaskNotFound);
        }
        let now = now()?;
        #[cfg(test)]
        self.control
            .fault(crate::command_tests::Checkpoint::Completion, transaction)
            .await?;
        #[cfg(test)]
        self.control
            .pause(crate::command_tests::Checkpoint::CompletionLocked)
            .await;
        if !interaction_repository::complete(transaction, call, id, evidence, now).await? {
            return Err(Error::InteractionNotFound);
        }
        repository::touch(transaction, &call.workspace_id, &call.task_id, now).await?;
        #[cfg(test)]
        self.control
            .fault(crate::command_tests::Checkpoint::Commit, transaction)
            .await?;
        Ok(())
    }
}

pub(super) fn now() -> Result<i64, Error> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_nanos()).ok())
        .ok_or(Error::Clock)
}

/// One retained authoring Attempt: a `save` or `delete` that Task history
/// keeps from before agents saved Saved Functions from Programs (ADR 0031).
#[cfg(any(test, feature = "test-fixtures"))]
#[derive(Debug, Clone, Copy)]
pub enum RetainedChange<'a> {
    /// Saves an untyped function.
    Save {
        /// The path relative to `coral.functions`.
        path: &'a str,
        /// The description search shows.
        description: &'a str,
        /// The TypeScript module source.
        source: &'a str,
    },
    /// Deletes the function at `path`.
    Delete {
        /// The path relative to `coral.functions`.
        path: &'a str,
    },
}

/// Writes the Attempts of [`RetainedChange`] as Coral wrote them, so tests
/// can read retained history. No surface records these now.
#[cfg(any(test, feature = "test-fixtures"))]
impl TaskCommands {
    /// Makes `change` through `functions` as `call`'s Principal and
    /// completes its Attempt, naming the function, in one transaction. A
    /// failure records the Attempt's rejection. Coral refused a delete of an
    /// absent function before the Attempt began, so this records one that
    /// Coral did not. Universal Search is not told; a reconcile pass finds
    /// the function.
    ///
    /// # Errors
    ///
    /// Returns an input, authoring, absent-function, authority or
    /// persistence failure.
    pub async fn record_retained(
        &self,
        functions: &coral_functions::SavedFunctionModule,
        call: &AttributedCommand,
        change: RetainedChange<'_>,
    ) -> Result<coral_functions::SavedFunctionId, Error> {
        let kind = match change {
            RetainedChange::Save { .. } => AttemptKind::Save,
            RetainedChange::Delete { .. } => AttemptKind::Delete,
        };
        let id = self.begin(call, kind).await?;
        let result = async {
            let context = retained_context(call)?;
            // A save compiles before the transaction, as Coral did.
            let ready = match change {
                RetainedChange::Save {
                    path,
                    description,
                    source,
                } => {
                    let save = coral_functions::SavedFunctionSave::new(
                        coral_functions::SavedFunctionPath::parse(path)?,
                        coral_functions::SavedFunctionDescription::parse(description)?,
                        coral_functions::SavedFunctionSource::parse(source)?,
                    );
                    RetainedReady::Save(Box::new(
                        functions
                            .prepare_save(&context, &call.workspace_id, save, None)
                            .await?,
                    ))
                }
                RetainedChange::Delete { path } => {
                    RetainedReady::Delete(coral_functions::SavedFunctionPath::parse(path)?)
                }
            };
            let mut transaction = self.tasks.database.begin().await?;
            self.hold_attempt(&mut transaction, call).await?;
            let function = match ready {
                RetainedReady::Save(prepared) => functions
                    .save_in_transaction(&mut transaction, *prepared)
                    .await?
                    .id()
                    .clone(),
                RetainedReady::Delete(path) => {
                    functions
                        .delete_in_transaction(
                            &mut transaction,
                            &context,
                            &call.workspace_id,
                            &path,
                        )
                        .await?
                }
            };
            let evidence = AttemptEvidence::SavedFunction(function.clone());
            self.complete(&mut transaction, call, &id, &evidence)
                .await?;
            transaction.commit().await?;
            Ok(function)
        }
        .await;
        match result {
            Ok(function) => Ok(function),
            Err(error) => Err(self.reject(call, &id, error).await),
        }
    }
}

/// A [`RetainedChange`] ready to write: a save compiled, or a delete's path.
#[cfg(any(test, feature = "test-fixtures"))]
enum RetainedReady {
    Save(Box<coral_functions::PreparedSave>),
    Delete(coral_functions::SavedFunctionPath),
}

#[cfg(any(test, feature = "test-fixtures"))]
fn retained_context(
    call: &AttributedCommand,
) -> Result<coral_functions::SavedFunctionChangeContext, Error> {
    Ok(coral_functions::SavedFunctionChangeContext::new(
        call.actor.clone(),
        now()?,
        coral_workspace::TraceId::current_or_new(),
    ))
}
