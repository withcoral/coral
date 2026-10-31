//! Program admission: bounded source-closure resolution, compilation outside
//! write transactions, and atomic acceptance of one complete package.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Instant;

use coral_db::Transaction;
use coral_functions::{SavedFunctionPath, SavedFunctionSelection};
use coral_telemetry::safe;
use coral_v8::CompileFailure;
use coral_workspace::{AuthenticatedPrincipal, Principal, WorkspaceId};
use tokio::time::timeout;
use uuid::Uuid;

use crate::engine::{ProgramPackage, SavedFunctionReference};
use crate::shutdown::Admitted;
use crate::types::AcceptedRun;
use crate::{
    COMBINED_TYPESCRIPT_MAX_BYTES, COMPILATION_TIMEOUT, Error, LINKED_SAVED_FUNCTION_MAX_COUNT,
    ProgramSource, RunCursor, RunId, RunModule, RunState, RunSubmission, RunUpdate, run_repository,
};

#[derive(Clone)]
enum SavedFunctionReferenceSource {
    SubmittedProgram,
    SavedFunction(SavedFunctionPath),
}

struct PendingSavedFunctionReference {
    source: SavedFunctionReferenceSource,
    reference: SavedFunctionReference,
}

/// Opaque compiled submission, bound to the exact submitting scope.
pub struct PreparedRun {
    actor: AuthenticatedPrincipal,
    workspace_id: WorkspaceId,
    submission: RunSubmission,
    package: ProgramPackage,
    linked: Vec<SavedFunctionSelection>,
}

/// Scope-bound evidence of staged acceptance, not proof that its transaction committed.
pub struct StagedRun {
    run: AcceptedRun,
    actor: AuthenticatedPrincipal,
    /// The `coral.run.accept` span, which ends once the Run is launched.
    accept: tracing::Span,
    /// Holds the admission until `launch` registers the Run's owner, or the
    /// staged Run is dropped: a shutdown waits for it.
    admitted: Admitted,
}

impl StagedRun {
    /// Returns the identity staged for this submission.
    #[must_use]
    pub const fn run_id(&self) -> &RunId {
        &self.run.id
    }

    /// Returns the exact accepting Workspace.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.run.workspace_id
    }

    /// Returns the accountable Principal.
    #[must_use]
    pub const fn principal(&self) -> &Principal {
        &self.run.principal
    }
}

impl RunModule {
    /// Validates and durably accepts one complete Program package.
    ///
    /// Compilation occurs before the final write transaction. The final short
    /// transaction rechecks Workspace and Principal Run authority before
    /// atomically writing the package, Run, and `run.started` Event. Execution
    /// is scheduled only after that transaction commits.
    ///
    /// # Errors
    ///
    /// Returns input, authority, compilation, package-limit, clock, worker, or
    /// persistence errors, and [`Error::ShuttingDown`] once the process is
    /// shutting down. Preparation and transactional rejection create no Run.
    /// A commit or post-commit launch error does not prove acceptance was undone.
    pub async fn start(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        submission: RunSubmission,
    ) -> Result<RunUpdate, Error> {
        let prepared = self.prepare(actor, workspace_id, submission).await?;
        let mut transaction = self.database.begin().await?;
        let staged = self
            .accept_in_transaction(&mut transaction, prepared)
            .await?;
        transaction.commit().await?;
        self.launch(staged).await
    }

    /// Compiles outside the final acceptance transaction, without executing code.
    ///
    /// # Errors
    /// Returns current authority or bounded admission failure, or
    /// [`Error::ShuttingDown`] once the process is shutting down; creates no Run.
    pub async fn prepare(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        submission: RunSubmission,
    ) -> Result<PreparedRun, Error> {
        self.admission.check()?;
        self.workspaces
            .check_run_admission(actor, workspace_id, self.now()?)
            .await?;
        let (package, linked) = self
            .compile_submission(actor, workspace_id, submission.source())
            .await?;

        Ok(PreparedRun {
            actor: actor.clone(),
            workspace_id: workspace_id.clone(),
            submission,
            package,
            linked,
        })
    }

    /// Stages acceptance in the caller's transaction. This is not proof of commit.
    ///
    /// # Errors
    /// Rechecks current authority, source selections, and persistence
    /// invariants; [`Error::ShuttingDown`] once the process is shutting down.
    pub async fn accept_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        prepared: PreparedRun,
    ) -> Result<StagedRun, Error> {
        let admitted = self.admission.admit()?;
        let accept = crate::trace::accept_span();
        let trace = coral_telemetry::RunTrace::begin(&accept);
        let PreparedRun {
            actor,
            workspace_id,
            submission,
            package,
            linked,
        } = prepared;
        let actor = &actor;
        let workspace_id = &workspace_id;
        let authority = self
            .workspaces
            .check_run_admission_in_transaction(transaction, actor, workspace_id, self.now()?)
            .await?;
        let mut snapshots_by_path = linked.iter().collect::<Vec<_>>();
        snapshots_by_path.sort_by(|left, right| left.path().cmp(right.path()));
        for snapshot in snapshots_by_path {
            if !coral_functions::hold_source_selection_in_transaction(
                transaction,
                workspace_id,
                actor.principal(),
                snapshot,
            )
            .await?
            {
                return Err(Error::ProgramAdmission(
                    "Saved Function changed during Program admission".to_string(),
                ));
            }
        }
        let accepted_at_unix_nanos = self.now()?;
        authority.require_current_at(accepted_at_unix_nanos)?;
        let deadline_at_unix_nanos = accepted_at_unix_nanos
            .checked_add(submission.timeout().nanoseconds())
            .ok_or_else(|| {
                Error::InvalidInput("Run deadline exceeds durable time range".to_string())
            })?;
        // The Run Owner's claim is written with acceptance, so no running Run
        // is ever unclaimed; the owner renews the lease with every chunk.
        let worker_claim_expires_at_unix_nanos =
            self.execution_policy.lease_until(accepted_at_unix_nanos)?;
        // The primary key proves the identifier unused: a conflict writes
        // nothing, and the next attempt draws fresh words.
        let mut attempts = 0;
        let run = loop {
            let run = AcceptedRun {
                id: RunId::generate()?,
                workspace_id: workspace_id.clone(),
                principal: actor.principal().clone(),
                accepted_at_unix_nanos,
                deadline_at_unix_nanos,
                shutdown_grace_nanos: self.execution_policy.shutdown_grace_nanos(),
                value_limits: self.value_limits,
                claim_id: Uuid::new_v4().hyphenated().to_string(),
                worker_claim_expires_at_unix_nanos,
                linked_snapshots: linked
                    .iter()
                    .map(|snapshot| {
                        (
                            snapshot.path().as_str().to_string(),
                            snapshot.id().to_string(),
                        )
                    })
                    .collect(),
                trace: trace.clone(),
            };
            if run_repository::insert_acceptance(
                transaction,
                &run,
                &self.instance,
                submission.source(),
                &package,
                &linked,
            )
            .await?
            {
                break run;
            }
            attempts += 1;
            if attempts == coral_identifier::ISSUE_ATTEMPTS {
                return Err(Error::IdentifierIssuance(format!(
                    "{attempts} identifiers in a row were already in use"
                )));
            }
        };
        accept.record("coral.run.id", safe::id(&run.held_id()));
        Ok(StagedRun {
            run,
            actor: actor.clone(),
            accept,
            admitted,
        })
    }

    /// Launches durably accepted work. Call only after committing acceptance.
    /// Re-reading acceptance prevents a rolled-back staging token from executing.
    /// Existing worker claims fence duplicate launch hints.
    ///
    /// # Errors
    /// Returns an error if durable acceptance cannot be read; never undoes acceptance.
    pub async fn launch(&self, staged: StagedRun) -> Result<RunUpdate, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        run_repository::find_exact_in_transaction(
            &mut transaction,
            &staged.run.workspace_id,
            &staged.run.id,
        )
        .await?
        .ok_or(Error::RunNotFound)?;
        transaction.commit().await?;
        let update = accepted_update(&staged.run)?;
        // The request that accepted the Run links to its trace, as the
        // accept span links back to it.
        crate::trace::link_request(staged.run.trace.as_ref());
        let StagedRun {
            run,
            actor,
            accept,
            admitted,
        } = staged;
        drop(accept);
        crate::trace::record_run_accepted(actor.principal());
        self.schedule_execution(run, actor);
        // The owner is registered: shutdown counts it from here.
        drop(admitted);
        Ok(update)
    }

    /// [`Self::compile_package`], timed as `coral.program.compile.duration`
    /// by how it ended.
    async fn compile_submission(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        submitted_source: &ProgramSource,
    ) -> Result<(ProgramPackage, Vec<SavedFunctionSelection>), Error> {
        use coral_telemetry::metric::{self, CompileOutcome};
        let compilation_started = Instant::now();
        let compiled = self
            .compile_package(actor, workspace_id, submitted_source, compilation_started)
            .await;
        let outcome = match &compiled {
            Ok(_) => CompileOutcome::Ok,
            Err(Error::CompilationTimedOut) => CompileOutcome::TimedOut,
            Err(
                Error::CompilerBusy
                | Error::ProgramWorkerStopped
                | Error::ProgramRuntimeUnavailable(_)
                | Error::CorruptData(_),
            ) => CompileOutcome::Unavailable,
            Err(error) if error.database_error().is_some() => CompileOutcome::Unavailable,
            Err(_) => CompileOutcome::Rejected,
        };
        metric::program_compiled(outcome, compilation_started.elapsed());
        compiled
    }

    async fn compile_package(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        submitted_source: &ProgramSource,
        compilation_started: Instant,
    ) -> Result<(ProgramPackage, Vec<SavedFunctionSelection>), Error> {
        let source = submitted_source.as_str().to_string();
        let roots = self
            .analyze_saved_function_paths(
                source.clone(),
                SavedFunctionReferenceSource::SubmittedProgram,
                compilation_started,
            )
            .await?;
        let linked = self
            .read_source_closure(actor, workspace_id, roots, compilation_started)
            .await?;

        validate_source_closure(
            &source,
            linked.iter().map(|snapshot| snapshot.source().as_str()),
        )?;
        let engine = self.engine.clone();
        let compile_linked = linked.clone();
        let compile = tokio::task::spawn_blocking(move || {
            engine.compile_with_saved_functions(
                compilation_started + COMPILATION_TIMEOUT,
                &source,
                &compile_linked,
            )
        });
        let remaining_compilation_time = COMPILATION_TIMEOUT
            .checked_sub(compilation_started.elapsed())
            .ok_or(Error::CompilationTimedOut)?;
        let package = match timeout(remaining_compilation_time, compile).await {
            Err(_elapsed) => return Err(Error::CompilationTimedOut),
            Ok(Err(_join_error)) => return Err(Error::ProgramWorkerStopped),
            Ok(Ok(Err(failure))) => return Err(domain_error(failure)),
            Ok(Ok(Ok(package))) => package,
        };
        package.validate()?;
        Ok((package, linked))
    }

    async fn read_source_closure(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        roots: Vec<SavedFunctionReference>,
        compilation_started: Instant,
    ) -> Result<Vec<SavedFunctionSelection>, Error> {
        if roots.is_empty() {
            return Ok(Vec::new());
        }

        let root_paths = roots
            .iter()
            .map(|reference| reference.path.clone())
            .collect::<Vec<_>>();
        let mut transaction = self.database.begin_read_snapshot().await?;
        let mut pending = roots
            .into_iter()
            .map(|reference| PendingSavedFunctionReference {
                source: SavedFunctionReferenceSource::SubmittedProgram,
                reference,
            })
            .collect::<VecDeque<_>>();
        let mut snapshots = BTreeMap::new();
        let mut dependencies = BTreeMap::new();
        while let Some(pending_reference) = pending.pop_front() {
            let path = &pending_reference.reference.path;
            if snapshots.contains_key(path) {
                continue;
            }
            if snapshots.len() == LINKED_SAVED_FUNCTION_MAX_COUNT {
                return Err(Error::ProgramAdmission(format!(
                    "Program may link at most {LINKED_SAVED_FUNCTION_MAX_COUNT} Saved Functions"
                )));
            }
            let snapshot = coral_functions::select_source_in_transaction(
                &mut transaction,
                workspace_id,
                actor.principal(),
                path,
            )
            .await?
            .ok_or_else(|| missing_saved_function(&pending_reference))?;
            let referenced = self
                .analyze_saved_function_paths(
                    snapshot.source().as_str().to_string(),
                    SavedFunctionReferenceSource::SavedFunction(path.clone()),
                    compilation_started,
                )
                .await?;
            pending.extend(referenced.iter().cloned().map(|reference| {
                PendingSavedFunctionReference {
                    source: SavedFunctionReferenceSource::SavedFunction(path.clone()),
                    reference,
                }
            }));
            dependencies.insert(
                path.clone(),
                referenced
                    .into_iter()
                    .map(|reference| reference.path)
                    .collect(),
            );
            snapshots.insert(path.clone(), snapshot);
        }
        transaction.commit().await?;

        order_saved_function_closure(&root_paths, &dependencies, &snapshots)
    }

    async fn analyze_saved_function_paths(
        &self,
        source: String,
        source_kind: SavedFunctionReferenceSource,
        compilation_started: Instant,
    ) -> Result<Vec<SavedFunctionReference>, Error> {
        let engine = self.engine.clone();
        let analyze = tokio::task::spawn_blocking(move || match source_kind {
            SavedFunctionReferenceSource::SubmittedProgram => engine
                .program_saved_function_references(
                    compilation_started + COMPILATION_TIMEOUT,
                    &source,
                ),
            SavedFunctionReferenceSource::SavedFunction(path) => engine
                .saved_function_dependency_references(
                    compilation_started + COMPILATION_TIMEOUT,
                    &path,
                    &source,
                ),
        });
        let remaining_compilation_time = COMPILATION_TIMEOUT
            .checked_sub(compilation_started.elapsed())
            .ok_or(Error::CompilationTimedOut)?;
        match timeout(remaining_compilation_time, analyze).await {
            Err(_elapsed) => Err(Error::CompilationTimedOut),
            Ok(Err(_join_error)) => Err(Error::ProgramWorkerStopped),
            Ok(Ok(Err(failure))) => Err(domain_error(failure)),
            Ok(Ok(Ok(paths))) => Ok(paths),
        }
    }
}

/// Only a rejection judges the Program; the others tell its author nothing to fix.
fn domain_error(failure: CompileFailure) -> Error {
    match failure {
        CompileFailure::Rejected(message) => Error::ProgramAdmission(message),
        CompileFailure::Busy => Error::CompilerBusy,
        CompileFailure::TimedOut => Error::CompilationTimedOut,
        CompileFailure::Stopped => Error::ProgramWorkerStopped,
        CompileFailure::Failed(kind) => {
            tracing::warn!(
                error.type = safe::kind(kind),
                "Program compiler failed; the caller sees an internal failure"
            );
            Error::ProgramRuntimeUnavailable(kind.to_string())
        }
    }
}

fn accepted_update(run: &AcceptedRun) -> Result<RunUpdate, Error> {
    RunUpdate::new(
        run.id.clone(),
        RunState::Running,
        Vec::new(),
        RunCursor::new(&run.id, 1, [0; 32]),
        false,
        None,
        run.value_limits,
    )
}

fn missing_saved_function(pending: &PendingSavedFunctionReference) -> Error {
    let source_name = match &pending.source {
        SavedFunctionReferenceSource::SubmittedProgram => "Program".to_string(),
        SavedFunctionReferenceSource::SavedFunction(path) => {
            format!("Saved Function coral.functions.{}", path.as_str())
        }
    };
    Error::ProgramAdmission(format!(
        "{source_name}:{}:{} references missing Saved Function coral.functions.{}",
        pending.reference.line,
        pending.reference.column,
        pending.reference.path.as_str()
    ))
}

fn order_saved_function_closure(
    roots: &[SavedFunctionPath],
    dependencies: &BTreeMap<SavedFunctionPath, Vec<SavedFunctionPath>>,
    snapshots: &BTreeMap<SavedFunctionPath, SavedFunctionSelection>,
) -> Result<Vec<SavedFunctionSelection>, Error> {
    fn visit(
        path: &SavedFunctionPath,
        dependencies: &BTreeMap<SavedFunctionPath, Vec<SavedFunctionPath>>,
        snapshots: &BTreeMap<SavedFunctionPath, SavedFunctionSelection>,
        visiting: &mut BTreeSet<SavedFunctionPath>,
        visited: &mut BTreeSet<SavedFunctionPath>,
        ordered: &mut Vec<SavedFunctionSelection>,
    ) -> Result<(), Error> {
        if visited.contains(path) || !visiting.insert(path.clone()) {
            return Ok(());
        }
        let referenced = dependencies.get(path).ok_or_else(|| {
            Error::CorruptData("Saved Function closure lost dependency evidence".to_string())
        })?;
        for dependency in referenced {
            visit(
                dependency,
                dependencies,
                snapshots,
                visiting,
                visited,
                ordered,
            )?;
        }
        visiting.remove(path);
        visited.insert(path.clone());
        ordered.push(
            snapshots
                .get(path)
                .ok_or_else(|| {
                    Error::CorruptData("Saved Function closure lost source evidence".to_string())
                })?
                .clone(),
        );
        Ok(())
    }

    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut ordered = Vec::with_capacity(snapshots.len());
    for root in roots {
        visit(
            root,
            dependencies,
            snapshots,
            &mut visiting,
            &mut visited,
            &mut ordered,
        )?;
    }
    Ok(ordered)
}

fn validate_source_closure<'a>(
    submitted_source: &str,
    linked_sources: impl IntoIterator<Item = &'a str>,
) -> Result<(), Error> {
    let mut linked_count = 0usize;
    let mut combined_bytes = submitted_source.len();
    for source in linked_sources {
        linked_count = linked_count.saturating_add(1);
        combined_bytes = combined_bytes.saturating_add(source.len());
    }
    if linked_count > LINKED_SAVED_FUNCTION_MAX_COUNT {
        return Err(Error::ProgramAdmission(format!(
            "Program may link at most {LINKED_SAVED_FUNCTION_MAX_COUNT} Saved Functions"
        )));
    }
    if combined_bytes > COMBINED_TYPESCRIPT_MAX_BYTES {
        return Err(Error::PackageTooLarge {
            component: "combined TypeScript source",
            maximum_bytes: COMBINED_TYPESCRIPT_MAX_BYTES,
        });
    }
    Ok(())
}
