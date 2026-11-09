//! Program admission: bounded source-closure resolution, compilation outside
//! write transactions, and atomic acceptance of one complete package.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::time::{Duration, Instant};

use coral_catalog::SEARCH_PATHS_MAX;
use coral_db::Transaction;
use coral_telemetry::safe;
use coral_tools::{PrivateToolPath, PrivateToolSelection};
use coral_v8::{BuiltinTools, CompileFailure};
use coral_workspace::{AuthenticatedPrincipal, Principal, WorkspaceId};
use tokio::time::timeout_at;
use uuid::Uuid;

use crate::engine::{PrivateToolReference, ProgramEngine, ProgramPackage, ProgramReferences};
use crate::shutdown::Admitted;
use crate::types::AcceptedRun;
use crate::{
    COMBINED_TYPESCRIPT_MAX_BYTES, Error, LINKED_PRIVATE_TOOL_MAX_COUNT, ProgramSource, RunCursor,
    RunId, RunModule, RunState, RunSubmission, RunUpdate, run_repository,
};

/// The most candidate Call Paths admission checks, as Private Tool typing
/// bounds them. A Program that names more has its paths checked when called.
const TOOL_CHECK_MAX_PATHS: usize = 128;

/// The longest admission's Call Path check may take.
const TOOL_CHECK_BUDGET: Duration = Duration::from_millis(500);

/// The compilation time the Call Path check always leaves, so the check never
/// makes a Program time out that would have compiled without it.
const TOOL_CHECK_COMPILATION_RESERVE: Duration = Duration::from_secs(1);

#[derive(Clone)]
enum PrivateToolReferenceSource {
    SubmittedProgram,
    PrivateTool(PrivateToolPath),
}

/// The Private Tools a Program links, in initialization order, and the
/// literal Tool chains each one's source names.
type LinkedClosure = (
    Vec<PrivateToolSelection>,
    Vec<(PrivateToolPath, coral_v8::ToolReferences)>,
);

struct PendingPrivateToolReference {
    source: PrivateToolReferenceSource,
    reference: PrivateToolReference,
}

/// Opaque compiled submission, bound to the exact submitting scope.
pub struct PreparedRun {
    actor: AuthenticatedPrincipal,
    workspace_id: WorkspaceId,
    submission: RunSubmission,
    package: ProgramPackage,
    linked: Vec<PrivateToolSelection>,
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
            if !coral_tools::hold_source_selection_in_transaction(
                transaction,
                workspace_id,
                actor.principal(),
                snapshot,
            )
            .await?
            {
                return Err(Error::ProgramAdmission(
                    "Private Tool changed during Program admission".to_string(),
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
                            crate::types::LinkedPrivateTool {
                                binding: snapshot.execution_binding(),
                                declared_default: snapshot.default_access_state(),
                            },
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
                self.compilation_timeout,
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
    ) -> Result<(ProgramPackage, Vec<PrivateToolSelection>), Error> {
        use coral_telemetry::metric::{self, CompileOutcome};
        let compilation_started = Instant::now();
        let compiled = self
            .compile_package(
                actor,
                workspace_id,
                submitted_source,
                compilation_started + self.compilation_timeout,
            )
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
        compilation_deadline: Instant,
    ) -> Result<(ProgramPackage, Vec<PrivateToolSelection>), Error> {
        let source = submitted_source.as_str().to_string();
        let named = self
            .analyze_program(source.clone(), compilation_deadline)
            .await?;
        if let Some(tools) = &named.tools {
            self.check_tool_calls(actor, workspace_id, "Program", tools, compilation_deadline)
                .await?;
        }
        let (linked, linked_tools) = self
            .read_source_closure(
                actor,
                workspace_id,
                named.private_tools,
                compilation_deadline,
            )
            .await?;
        // A stored source is checked as the Program is, at every admission
        // that links it: an old root fails here, not when called.
        for (path, tools) in &linked_tools {
            let source_name = format!("Private Tool coral.private.{}", path.as_str());
            self.check_tool_calls(
                actor,
                workspace_id,
                &source_name,
                tools,
                compilation_deadline,
            )
            .await?;
        }

        validate_source_closure(
            &source,
            linked.iter().map(|snapshot| snapshot.source().as_str()),
        )?;
        let builtin_tools = self.builtin_tools(workspace_id).await?;
        let engine = self.engine.clone();
        let compile_linked = linked.clone();
        let compile = tokio::task::spawn_blocking(move || {
            engine.compile_with_private_tools(
                compilation_deadline,
                &source,
                &compile_linked,
                &builtin_tools,
            )
        });
        let package = match timeout_at(compilation_deadline.into(), compile).await {
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
        roots: Vec<PrivateToolReference>,
        compilation_deadline: Instant,
    ) -> Result<LinkedClosure, Error> {
        if roots.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let root_paths = roots
            .iter()
            .map(|reference| reference.path.clone())
            .collect::<Vec<_>>();
        let mut transaction = self.database.begin_read_snapshot().await?;
        let mut pending = roots
            .into_iter()
            .map(|reference| PendingPrivateToolReference {
                source: PrivateToolReferenceSource::SubmittedProgram,
                reference,
            })
            .collect::<VecDeque<_>>();
        let mut snapshots = BTreeMap::new();
        let mut dependencies = BTreeMap::new();
        let mut tools = Vec::new();
        while let Some(pending_reference) = pending.pop_front() {
            let path = &pending_reference.reference.path;
            if snapshots.contains_key(path) {
                continue;
            }
            if snapshots.len() == LINKED_PRIVATE_TOOL_MAX_COUNT {
                return Err(Error::ProgramAdmission(format!(
                    "Program may link at most {LINKED_PRIVATE_TOOL_MAX_COUNT} Private Tools"
                )));
            }
            let snapshot = coral_tools::select_source_in_transaction(
                &mut transaction,
                workspace_id,
                actor.principal(),
                path,
            )
            .await?;
            let Some(snapshot) = snapshot else {
                // The lookup for suggestions reads in its own snapshot.
                transaction.rollback().await?;
                return Err(self
                    .missing_private_tool(
                        actor,
                        workspace_id,
                        &pending_reference,
                        compilation_deadline,
                    )
                    .await);
            };
            let ProgramReferences {
                private_tools: referenced,
                tools: named_tools,
            } = self
                .analyze_private_tool(
                    snapshot.source().as_str().to_string(),
                    path.clone(),
                    compilation_deadline,
                )
                .await?;
            if let Some(named_tools) = named_tools {
                tools.push((path.clone(), named_tools));
            }
            pending.extend(referenced.iter().cloned().map(|reference| {
                PendingPrivateToolReference {
                    source: PrivateToolReferenceSource::PrivateTool(path.clone()),
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

        Ok((
            order_private_tool_closure(&root_paths, &dependencies, &snapshots)?,
            tools,
        ))
    }

    async fn analyze_program(
        &self,
        source: String,
        compilation_deadline: Instant,
    ) -> Result<ProgramReferences, Error> {
        self.analyze(compilation_deadline, move |engine, deadline| {
            engine.program_references(deadline, &source)
        })
        .await
    }

    /// Refuses a Program whose literal `coral.providers` or
    /// Built-in Tool call, in its body or in `source_name`, a Private Tool
    /// it links, names no Tool the caller can call, naming
    /// each such path and the nearest ones it can discover. A computed path
    /// is checked only when called.
    ///
    /// A path names a Tool when Invocation admission would resolve it now,
    /// so the check refuses exactly the calls that would fail with
    /// `tool_not_found`, and a call to a disabled Tool still runs to be
    /// recorded and refused. The unknown-Tool diagnostic's rule decides
    /// which chains those paths leave unknown. Neither reads the search
    /// index. The check is bounded like Private Tool typing: a Program
    /// that names more candidate paths, or a check that fails or runs out of
    /// its time, leaves the paths to be checked when called.
    async fn check_tool_calls(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        source_name: &str,
        tools: &coral_v8::ToolReferences,
        compilation_deadline: Instant,
    ) -> Result<(), Error> {
        let candidates = tools.call_paths();
        if candidates.is_empty() || candidates.len() > TOOL_CHECK_MAX_PATHS {
            return Ok(());
        }
        let now = Instant::now();
        let Some(deadline) = compilation_deadline
            .checked_sub(TOOL_CHECK_COMPILATION_RESERVE)
            .map(|latest| latest.min(now + TOOL_CHECK_BUDGET))
            .filter(|deadline| *deadline > now)
        else {
            tracing::warn!("No time is left to check the Program's Call Paths before it runs");
            return Ok(());
        };
        let search = coral_search::UniversalSearchModule::new(
            self.database.clone(),
            self.workspaces.clone(),
        );
        let check = async {
            let callable = self
                .callable_tools(actor, workspace_id, candidates)
                .await
                .map_err(|_error| "catalog_unavailable")?;
            let engine = self.engine.clone();
            let tools = tools.clone();
            tokio::task::spawn_blocking(move || {
                engine.unknown_tool_calls(deadline, &tools, &callable)
            })
            .await
            .map_err(|_join_error| "compiler_stopped")?
            .map_err(|_failure| "compiler_failed")
        };
        let unknown = match timeout_at(deadline.into(), check).await {
            Ok(Ok(unknown)) => unknown,
            Ok(Err(kind)) => {
                tracing::warn!(
                    error.type = safe::kind(kind),
                    "The Program's Call Paths are checked only when called"
                );
                return Ok(());
            }
            Err(_elapsed) => {
                tracing::warn!(
                    "Checking the Program's Call Paths took too long; they are checked only when called"
                );
                return Ok(());
            }
        };
        if unknown.is_empty() {
            return Ok(());
        }
        let mut named = Vec::<&coral_v8::UnknownToolCall>::new();
        for call in &unknown {
            if !named
                .iter()
                .any(|known| known.call_path() == call.call_path())
            {
                named.push(call);
            }
        }
        let suggestions = timeout_at(
            compilation_deadline.into(),
            unknown_path_suggestions(
                &search,
                actor,
                workspace_id,
                named.iter().map(|call| call.call_path()),
            ),
        )
        .await
        .unwrap_or_default();
        let message = named
            .iter()
            .map(|call| {
                format!(
                    "{source_name}:{}:{}: {}",
                    call.line(),
                    call.column(),
                    coral_catalog::unknown_call_path_message(
                        call.call_path(),
                        suggestions
                            .get(call.call_path())
                            .map_or(&[][..], Vec::as_slice),
                    )
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        Err(Error::ProgramAdmission(
            coral_v8::bounded_diagnostic_message(&message),
        ))
    }

    /// Refuses a Program that calls, in its body or in a Private Tool it
    /// links, a Private Tool the caller does not have: like any path that
    /// names no Tool, with the caller's nearest Private Tools, which a
    /// lookup bounded by the compilation deadline finds.
    async fn missing_private_tool(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        pending: &PendingPrivateToolReference,
        compilation_deadline: Instant,
    ) -> Error {
        let source_name = match &pending.source {
            PrivateToolReferenceSource::SubmittedProgram => "Program".to_string(),
            PrivateToolReferenceSource::PrivateTool(path) => {
                format!("Private Tool coral.private.{}", path.as_str())
            }
        };
        let call_path = format!("coral.private.{}", pending.reference.path.as_str());
        let search = coral_search::UniversalSearchModule::new(
            self.database.clone(),
            self.workspaces.clone(),
        );
        let suggestions = timeout_at(
            compilation_deadline.into(),
            unknown_path_suggestions(
                &search,
                actor,
                workspace_id,
                std::iter::once(call_path.as_str()),
            ),
        )
        .await
        .unwrap_or_default();
        Error::ProgramAdmission(coral_v8::bounded_diagnostic_message(&format!(
            "{source_name}:{}:{}: {}",
            pending.reference.line,
            pending.reference.column,
            coral_catalog::unknown_call_path_message(
                &call_path,
                suggestions.get(&call_path).map_or(&[][..], Vec::as_slice),
            )
        )))
    }

    /// The Workspace's Built-in Tools, which the Run calls on `coral`: the
    /// ones its Built-in Provider holds now, as discovery and Invocation
    /// admission read them. A newer or older Coral may have written them, so
    /// a Tool this version cannot perform fails when called, as
    /// `tool_unavailable`, and not as a missing member.
    async fn builtin_tools(&self, workspace_id: &WorkspaceId) -> Result<BuiltinTools, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let paths =
            coral_provider::builtin_tool_paths_in_transaction(&mut transaction, workspace_id)
                .await?;
        transaction.commit().await?;
        Ok(BuiltinTools::new(paths))
    }

    /// The paths among `call_paths` that Invocation admission would
    /// resolve for `actor` now, from one read snapshot.
    async fn callable_tools(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        call_paths: &[String],
    ) -> Result<Vec<String>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let now = self.now()?;
        let mut callable = Vec::new();
        for call_path in call_paths {
            if coral_provider::resolves_invocation_tool_in_snapshot(
                &mut transaction,
                workspace_id,
                coral_provider::InvocationCaller::new(&self.workspaces, actor, now),
                call_path,
            )
            .await?
            {
                callable.push(call_path.clone());
            }
        }
        transaction.commit().await?;
        Ok(callable)
    }

    async fn analyze_private_tool(
        &self,
        source: String,
        path: PrivateToolPath,
        compilation_deadline: Instant,
    ) -> Result<ProgramReferences, Error> {
        self.analyze(compilation_deadline, move |engine, deadline| {
            engine.private_tool_references(deadline, &path, &source)
        })
        .await
    }

    /// Runs one analysis on a blocking worker within the admission deadline.
    async fn analyze<T: Send + 'static>(
        &self,
        compilation_deadline: Instant,
        analysis: impl FnOnce(&dyn ProgramEngine, Instant) -> Result<T, CompileFailure> + Send + 'static,
    ) -> Result<T, Error> {
        let engine = self.engine.clone();
        let analyze =
            tokio::task::spawn_blocking(move || analysis(engine.as_ref(), compilation_deadline));
        match timeout_at(compilation_deadline.into(), analyze).await {
            Err(_elapsed) => Err(Error::CompilationTimedOut),
            Ok(Err(_join_error)) => Err(Error::ProgramWorkerStopped),
            Ok(Ok(Err(failure))) => Err(domain_error(failure)),
            Ok(Ok(Ok(analyzed))) => Ok(analyzed),
        }
    }
}

/// The nearest Discoverable Call Paths to the first [`SEARCH_PATHS_MAX`]
/// unknown paths, by path, from one lookup. Discovery is advice: a failed
/// lookup suggests nothing.
async fn unknown_path_suggestions<'a>(
    search: &coral_search::UniversalSearchModule,
    actor: &AuthenticatedPrincipal,
    workspace_id: &WorkspaceId,
    unknown: impl Iterator<Item = &'a str>,
) -> HashMap<String, Vec<String>> {
    let paths = unknown
        .filter(|path| path.len() <= coral_catalog::SEARCH_PATH_MAX_BYTES)
        .take(SEARCH_PATHS_MAX)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let Ok(paths) = coral_search::ExactPaths::new(paths, Vec::new()) else {
        return HashMap::new();
    };
    match search
        .discover(
            actor,
            workspace_id,
            &coral_search::DiscoveryRequest::Paths(paths),
        )
        .await
    {
        Ok(coral_search::Discovery::Exact(lookup)) => lookup
            .missing()
            .iter()
            .map(|missing| {
                (
                    missing.call_path().to_owned(),
                    missing.suggestions().to_vec(),
                )
            })
            .collect(),
        Ok(coral_search::Discovery::Page(_)) | Err(_) => HashMap::new(),
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

fn order_private_tool_closure(
    roots: &[PrivateToolPath],
    dependencies: &BTreeMap<PrivateToolPath, Vec<PrivateToolPath>>,
    snapshots: &BTreeMap<PrivateToolPath, PrivateToolSelection>,
) -> Result<Vec<PrivateToolSelection>, Error> {
    fn visit(
        path: &PrivateToolPath,
        dependencies: &BTreeMap<PrivateToolPath, Vec<PrivateToolPath>>,
        snapshots: &BTreeMap<PrivateToolPath, PrivateToolSelection>,
        visiting: &mut BTreeSet<PrivateToolPath>,
        visited: &mut BTreeSet<PrivateToolPath>,
        ordered: &mut Vec<PrivateToolSelection>,
    ) -> Result<(), Error> {
        if visited.contains(path) || !visiting.insert(path.clone()) {
            return Ok(());
        }
        let referenced = dependencies.get(path).ok_or_else(|| {
            Error::CorruptData("Private Tool closure lost dependency evidence".to_string())
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
                    Error::CorruptData("Private Tool closure lost source evidence".to_string())
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
    if linked_count > LINKED_PRIVATE_TOOL_MAX_COUNT {
        return Err(Error::ProgramAdmission(format!(
            "Program may link at most {LINKED_PRIVATE_TOOL_MAX_COUNT} Private Tools"
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
