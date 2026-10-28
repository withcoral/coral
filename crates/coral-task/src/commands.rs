//! Attributed command choreography. Tool decisions and writes stay with their owners.
use crate::{
    AttemptEvidence, AttemptKind, AttributedCommand, DiscoveryContext, Error, InteractionId,
    TaskModule,
};
use crate::{interaction_repository, repository};
use coral_db::Transaction;
use coral_functions::SavedFunctionModule;
use coral_run::RunModule;
use coral_search::{SearchIndex, SearchPage, SearchRequest, UniversalSearchModule as SearchModule};
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};
use std::time::{SystemTime, UNIX_EPOCH};

/// The longest Call Path `describe` accepts. It lives here, not in `CallPath::parse`,
/// until the catalog marks a Tool with a longer Call Path unavailable: search
/// builds Tool Call Paths through the same type.
const DESCRIBE_CALL_PATH_MAX_BYTES: usize = 1024;

/// Reads Tool declarations as `describe` would for one caller, so typing
/// adds nothing the caller cannot already discover.
struct CallerToolDeclarations<'a> {
    search: &'a SearchModule,
    actor: &'a AuthenticatedPrincipal,
    workspace_id: &'a WorkspaceId,
}

#[async_trait::async_trait]
impl coral_functions::ToolDeclarationReader for CallerToolDeclarations<'_> {
    async fn read(
        &self,
        call_paths: &[String],
    ) -> Result<Vec<coral_functions::ToolDeclaration>, Box<dyn std::error::Error + Send + Sync>>
    {
        let call_paths = call_paths
            .iter()
            .filter_map(|call_path| coral_search::CallPath::parse(call_path).ok())
            .collect::<Vec<_>>();
        Ok(self
            .search
            .describe_tools(self.actor, self.workspace_id, &call_paths)
            .await?
            .into_iter()
            .map(|description| {
                coral_functions::ToolDeclaration::new(
                    description.call_path().as_str().to_owned(),
                    description.typescript_declaration().to_owned(),
                )
            })
            .collect())
    }
}

/// Task-aware commands above independently usable operation domains.
#[derive(Clone)]
pub struct TaskCommands {
    tasks: TaskModule,
    functions: SavedFunctionModule,
    search: SearchModule,
    index: SearchIndex,
    runs: RunModule,
    #[cfg(test)]
    pub(crate) control: std::sync::Arc<crate::command_tests::Control>,
}
impl TaskCommands {
    /// Composes real domain modules; no callbacks or generic operation registry.
    #[must_use]
    pub fn new(
        tasks: TaskModule,
        functions: SavedFunctionModule,
        search: SearchModule,
        index: SearchIndex,
        runs: RunModule,
    ) -> Self {
        Self {
            tasks,
            functions,
            search,
            index,
            runs,
            #[cfg(test)]
            control: std::sync::Arc::default(),
        }
    }

    /// The Call Roots of the Providers the caller can search, as Universal
    /// Search decides them. Retains nothing.
    /// # Errors
    /// Discovery authority and persistence failures.
    pub async fn searchable_providers(
        &self,
        actor: &coral_workspace::AuthenticatedPrincipal,
        workspace_id: &coral_workspace::WorkspaceId,
    ) -> Result<Vec<coral_search::ProviderCallRoot>, Error> {
        Ok(self
            .search
            .searchable_providers(actor, workspace_id)
            .await?)
    }

    /// Retains exactly one requested search page before returning it.
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
                cursor: request.cursor().map(ToString::to_string),
            },
            response: coral_search::projection::search(&page),
        };
        let mut transaction = self.tasks.database.begin().await?;
        self.complete(&mut transaction, call, &id, &evidence)
            .await?;
        transaction.commit().await?;
        Ok(page)
    }

    /// Saves one callable and its lifecycle evidence atomically with attempt completion.
    /// # Errors
    /// Authorized input and compilation failures are retained; no partial save commits.
    pub async fn save_function(
        &self,
        call: &AttributedCommand,
        path: &str,
        description: &str,
        source: &str,
    ) -> Result<coral_functions::SavedFunction, Error> {
        let id = self.begin(call, AttemptKind::Save).await?;
        let result = async {
            let save = coral_functions::SavedFunctionSave::new(
                coral_functions::SavedFunctionPath::parse(path)?,
                coral_functions::SavedFunctionDescription::parse(description)?,
                coral_functions::SavedFunctionSource::parse(source)?,
            );
            let context = Self::function_context(call)?;
            let prepared = self
                .functions
                .prepare_save(
                    &context,
                    &call.workspace_id,
                    save,
                    Some(&CallerToolDeclarations {
                        search: &self.search,
                        actor: &call.actor,
                        workspace_id: &call.workspace_id,
                    }),
                )
                .await?;
            let mut transaction = self.tasks.database.begin().await?;
            self.hold_attempt(&mut transaction, call).await?;
            let function = self
                .functions
                .save_in_transaction(&mut transaction, prepared)
                .await?;
            self.complete(
                &mut transaction,
                call,
                &id,
                &AttemptEvidence::SavedFunction(function.id().clone()),
            )
            .await?;
            transaction.commit().await?;
            Ok(function)
        }
        .await;
        let function = match result {
            Ok(function) => function,
            Err(error) => return Err(self.reject(call, &id, error).await),
        };
        // Outside the rejection path: indexing never undoes or rejects a
        // committed save.
        self.index
            .index_function(coral_functions::SavedFunctionEntry::from_saved(&function));
        Ok(function)
    }

    /// Deletes an exactly visible owned function with atomic attempt completion.
    /// # Errors
    /// Unauthorized probes create no attempt; admitted failures never invent a save.
    pub async fn delete_function(
        &self,
        call: &AttributedCommand,
        path: &str,
    ) -> Result<coral_functions::SavedFunctionId, Error> {
        let path = match coral_functions::SavedFunctionPath::parse(path) {
            Ok(path) => path,
            Err(error) => {
                let id = self.begin(call, AttemptKind::Delete).await?;
                return Err(self.reject(call, &id, error.into()).await);
            }
        };
        self.functions
            .get(&call.actor, &call.workspace_id, &path)
            .await?;
        let id = self.begin(call, AttemptKind::Delete).await?;
        let result = async {
            let mut transaction = self.tasks.database.begin().await?;
            self.hold_attempt(&mut transaction, call).await?;
            let deleted = self
                .functions
                .delete_in_transaction(
                    &mut transaction,
                    &Self::function_context(call)?,
                    &call.workspace_id,
                    &path,
                )
                .await?;
            self.complete(
                &mut transaction,
                call,
                &id,
                &AttemptEvidence::SavedFunction(deleted.clone()),
            )
            .await?;
            transaction.commit().await?;
            Ok(deleted)
        }
        .await;
        match result {
            Ok(value) => Ok(value),
            Err(error) => Err(self.reject(call, &id, error).await),
        }
    }

    /// Retains the exact description, including editable source and
    /// diagnostics, or the `types` of it that the caller names.
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

    fn function_context(
        call: &AttributedCommand,
    ) -> Result<coral_functions::SavedFunctionChangeContext, Error> {
        Ok(coral_functions::SavedFunctionChangeContext::new(
            call.actor.clone(),
            now()?,
            call.trace_id.clone(),
        ))
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
