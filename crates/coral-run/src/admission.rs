//! Program admission: bounded source-closure resolution, compilation outside
//! write transactions, and atomic acceptance of one complete package.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;

use coral_catalog::SEARCH_PATHS_MAX;
use coral_db::Transaction;
use coral_telemetry::safe;
use coral_tools::{AuthoredToolPath, LinkedToolSelection};
use coral_v8::{BuiltinTools, CompileFailure};
use coral_workspace::{AuthenticatedPrincipal, Principal, WorkspaceId};
use tokio::time::timeout_at;
use uuid::Uuid;

use coral_run_executor::tool_check_deadline;

use crate::engine::{AdmissionReferences, AdmissionSource, PackageFailure, ProgramPackage};
use crate::shutdown::Admitted;
use crate::types::AcceptedRun;
use crate::{
    COMBINED_TYPESCRIPT_MAX_BYTES, Error, LINKED_TOOL_MAX_COUNT, ProgramSource, RunCursor, RunId,
    RunModule, RunState, RunSubmission, RunUpdate, run_repository,
};

/// The most candidate Call Paths admission checks, as Private Tool typing
/// bounds them. A Program that names more has its paths checked when called.
const TOOL_CHECK_MAX_PATHS: usize = 128;

/// The source that names a linked Tool: the submitted Program, or a
/// linked Private or Shared Tool.
#[derive(Clone)]
enum ReferenceSource {
    SubmittedProgram,
    Linked(AuthoredToolPath),
}

impl ReferenceSource {
    fn name(&self) -> String {
        match self {
            Self::SubmittedProgram => "Program".to_string(),
            Self::Linked(path) => path.source_name(),
        }
    }
}

/// The Private and Shared Tools a Program links, in initialization order,
/// and the candidate Tool Call Paths their sources name.
type LinkedClosure = (Vec<LinkedToolSelection>, Vec<String>);

/// What stopped one level of the closure read before it read every source.
enum ClosureStop {
    /// A source names a Private Tool the caller does not have or a Shared
    /// Tool the Workspace does not have.
    Missing(PendingReference),
    /// Any other refusal or failure.
    Failed(Error),
}

/// One literal reference to a linked Tool, and where its source names it.
struct PendingReference {
    source: ReferenceSource,
    path: AuthoredToolPath,
    line: u64,
    column: u64,
}

/// The linked Tools `named` names, as references from `source`.
fn pending_references(
    source: &ReferenceSource,
    named: &AdmissionReferences,
) -> impl Iterator<Item = PendingReference> {
    named
        .linked
        .iter()
        .map(|reference| PendingReference {
            source: source.clone(),
            path: reference.path().clone(),
            line: reference.line(),
            column: reference.column(),
        })
        .collect::<Vec<_>>()
        .into_iter()
}

/// Opaque compiled submission, bound to the exact submitting scope.
pub struct PreparedRun {
    actor: AuthenticatedPrincipal,
    workspace_id: WorkspaceId,
    submission: RunSubmission,
    package: ProgramPackage,
    linked: Vec<LinkedToolSelection>,
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
        hold_linked_sources(transaction, workspace_id, actor, &linked).await?;
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
                            snapshot.path().call_path(),
                            crate::types::LinkedTool::of(snapshot),
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
    ) -> Result<(ProgramPackage, Vec<LinkedToolSelection>), Error> {
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
    ) -> Result<(ProgramPackage, Vec<LinkedToolSelection>), Error> {
        let source = submitted_source.as_str().to_string();
        let named = self
            .analyze_program(source.clone(), compilation_deadline)
            .await?;
        let roots = pending_references(&ReferenceSource::SubmittedProgram, &named).collect();
        let (linked, mut candidates) = self
            .read_source_closure(actor, workspace_id, roots, compilation_deadline)
            .await?;
        validate_source_closure(
            &source,
            linked.iter().map(|linked| linked.source().as_str()),
        )?;
        // A stored source is checked as the Program is, at every admission
        // that links it: an old root fails at compile, not when called.
        candidates.extend(named.tool_call_paths);
        candidates.sort();
        candidates.dedup();
        let callable = self
            .callable_tool_paths(actor, workspace_id, &candidates, compilation_deadline)
            .await;
        let builtin_tools = self.builtin_tools(workspace_id).await?;
        let engine = self.engine.clone();
        let compile_linked = linked.clone();
        let compile = tokio::task::spawn_blocking(move || {
            engine.compile(
                compilation_deadline,
                &source,
                &compile_linked,
                &builtin_tools,
                callable.as_deref(),
            )
        });
        let package = match timeout_at(compilation_deadline.into(), compile).await {
            Err(_elapsed) => return Err(Error::CompilationTimedOut),
            Ok(Err(_join_error)) => return Err(Error::ProgramWorkerStopped),
            Ok(Ok(Err(PackageFailure::UnknownTools {
                linked: unknown_in,
                calls,
            }))) => {
                let source_name = match unknown_in {
                    None => "Program".to_string(),
                    Some(index) => linked
                        .get(index)
                        .ok_or_else(|| {
                            Error::CorruptData(
                                "compiler named a linked Tool the Program does not link"
                                    .to_string(),
                            )
                        })?
                        .path()
                        .source_name(),
                };
                return Err(self
                    .unknown_tools(
                        actor,
                        workspace_id,
                        &source_name,
                        &calls,
                        compilation_deadline,
                    )
                    .await);
            }
            Ok(Ok(Err(PackageFailure::Compile(failure)))) => return Err(domain_error(failure)),
            Ok(Ok(Ok(package))) => package,
        };
        package.validate()?;
        Ok((package, linked))
    }

    /// Reads the closure of Tools the Program links: each Private Tool is
    /// the caller's own current definition, wherever it is named, including
    /// inside a Shared Tool, and each Shared Tool is the Workspace's current
    /// revision (ADR 0034). It reads in one read snapshot, one level of the
    /// graph at a time, with one analysis per level, and reports what
    /// reading one source at a time in the same order would find first.
    async fn read_source_closure(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        roots: Vec<PendingReference>,
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
        let mut level = roots;
        let mut snapshots = BTreeMap::new();
        let mut dependencies = BTreeMap::new();
        let mut tool_call_paths = Vec::new();
        while !level.is_empty() {
            let mut paths = Vec::new();
            let mut sources = Vec::new();
            let mut stopped = None;
            for pending_reference in level {
                let path = &pending_reference.path;
                if snapshots.contains_key(path) {
                    continue;
                }
                if snapshots.len() == LINKED_TOOL_MAX_COUNT {
                    stopped = Some(ClosureStop::Failed(Error::ProgramAdmission(format!(
                        "Program may link at most {LINKED_TOOL_MAX_COUNT} Private and Shared Tools"
                    ))));
                    break;
                }
                let snapshot = match coral_tools::select_linked_tool_in_transaction(
                    &mut transaction,
                    workspace_id,
                    actor.principal(),
                    path,
                )
                .await
                {
                    Ok(Some(snapshot)) => snapshot,
                    Ok(None) => {
                        stopped = Some(ClosureStop::Missing(pending_reference));
                        break;
                    }
                    Err(error) => {
                        stopped = Some(ClosureStop::Failed(error.into()));
                        break;
                    }
                };
                sources.push(AdmissionSource::Linked {
                    path: path.clone(),
                    source: snapshot.source().as_str().to_string(),
                });
                paths.push(path.clone());
                snapshots.insert(path.clone(), snapshot);
            }
            // The sources read before the one that stopped the level come
            // first, so their rejection wins.
            let analyzed = if sources.is_empty() {
                Vec::new()
            } else {
                self.analyze(sources, compilation_deadline).await?
            };
            match stopped {
                None => {}
                Some(ClosureStop::Failed(error)) => return Err(error),
                Some(ClosureStop::Missing(pending_reference)) => {
                    // The lookup for suggestions reads in its own snapshot.
                    transaction.rollback().await?;
                    return Err(self
                        .missing_linked_tool(
                            actor,
                            workspace_id,
                            &pending_reference,
                            compilation_deadline,
                        )
                        .await);
                }
            }
            let mut next = Vec::new();
            for (path, named) in paths.into_iter().zip(analyzed) {
                let referenced = pending_references(&ReferenceSource::Linked(path.clone()), &named)
                    .collect::<Vec<_>>();
                dependencies.insert(
                    path,
                    referenced
                        .iter()
                        .map(|reference| reference.path.clone())
                        .collect(),
                );
                next.extend(referenced);
                tool_call_paths.extend(named.tool_call_paths);
            }
            level = next;
        }
        transaction.commit().await?;

        Ok((
            order_linked_closure(&root_paths, &dependencies, &snapshots)?,
            tool_call_paths,
        ))
    }

    async fn analyze_program(
        &self,
        source: String,
        compilation_deadline: Instant,
    ) -> Result<AdmissionReferences, Error> {
        let mut analyzed = self
            .analyze(vec![AdmissionSource::Program(source)], compilation_deadline)
            .await?;
        Ok(analyzed.pop().unwrap_or_default())
    }

    /// The candidate Call Paths that the caller can call, which compile
    /// checks the literal Tool calls of the Program and of each Tool it
    /// links against. A computed path is checked only when called.
    ///
    /// A path names a Tool when Invocation admission would resolve it now,
    /// so the check refuses exactly the calls that would fail with
    /// `tool_not_found`, and a call to a disabled Tool still runs to be
    /// recorded and refused. The unknown-Tool diagnostic's rule decides
    /// which chains those paths leave unknown. Neither reads the search
    /// index. The check is bounded like Private Tool typing: `None`, for
    /// a Program whose sources name more candidate paths or a read that
    /// fails or runs out of its time, leaves the paths to be checked when
    /// called.
    async fn callable_tool_paths(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        candidates: &[String],
        compilation_deadline: Instant,
    ) -> Option<Vec<String>> {
        if candidates.is_empty() || candidates.len() > TOOL_CHECK_MAX_PATHS {
            return None;
        }
        let Some(deadline) = tool_check_deadline(compilation_deadline) else {
            tracing::warn!("No time is left to check the Program's Call Paths before it runs");
            return None;
        };
        match timeout_at(
            deadline.into(),
            self.callable_tools(actor, workspace_id, candidates),
        )
        .await
        {
            Ok(Ok(callable)) => Some(callable),
            Ok(Err(_error)) => {
                tracing::warn!(
                    error.type = safe::kind("catalog_unavailable"),
                    "The Program's Call Paths are checked only when called"
                );
                None
            }
            Err(_elapsed) => {
                tracing::warn!(
                    "Checking the Program's Call Paths took too long; they are checked only when called"
                );
                None
            }
        }
    }

    /// Refuses a Program whose literal calls, in its body or in
    /// `source_name`, a Tool it links, name no Tool the caller can call,
    /// naming each such path once, where it is, and the nearest ones the
    /// caller can discover.
    async fn unknown_tools(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        source_name: &str,
        unknown: &[coral_v8::UnknownToolCall],
        compilation_deadline: Instant,
    ) -> Error {
        let search = coral_search::UniversalSearchModule::new(
            self.database.clone(),
            self.workspaces.clone(),
        );
        let mut named = Vec::<&coral_v8::UnknownToolCall>::new();
        for call in unknown {
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
        Error::ProgramAdmission(coral_v8::bounded_diagnostic_message(&message))
    }

    /// Refuses a Program that calls, in its body or in a Tool it links, a
    /// Private Tool the caller does not have or a Shared Tool the Workspace
    /// does not have: like any path that names no Tool, with the nearest
    /// paths the caller can call, which a lookup bounded by the compilation
    /// deadline finds.
    async fn missing_linked_tool(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        pending: &PendingReference,
        compilation_deadline: Instant,
    ) -> Error {
        let source_name = pending.source.name();
        let call_path = pending.path.call_path();
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
            pending.line,
            pending.column,
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

    /// Analyzes `sources` on a blocking worker within the admission
    /// deadline: one result per source, in order.
    async fn analyze(
        &self,
        sources: Vec<AdmissionSource>,
        compilation_deadline: Instant,
    ) -> Result<Vec<AdmissionReferences>, Error> {
        let engine = self.engine.clone();
        let analyze =
            tokio::task::spawn_blocking(move || engine.analyze(compilation_deadline, &sources));
        match timeout_at(compilation_deadline.into(), analyze).await {
            Err(_elapsed) => Err(Error::CompilationTimedOut),
            Ok(Err(_join_error)) => Err(Error::ProgramWorkerStopped),
            Ok(Ok(Err(failure))) => Err(domain_error(failure)),
            Ok(Ok(Ok(analyzed))) => Ok(analyzed),
        }
    }
}

/// Holds every source the Run links as it was selected, through the
/// acceptance commit, or refuses the admission when one changed.
async fn hold_linked_sources(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    actor: &AuthenticatedPrincipal,
    linked: &[LinkedToolSelection],
) -> Result<(), Error> {
    match coral_tools::hold_linked_tools_in_transaction(
        transaction,
        workspace_id,
        actor.principal(),
        linked,
    )
    .await?
    {
        None => Ok(()),
        Some(changed) => Err(Error::ProgramAdmission(format!(
            "{} changed during Program admission",
            changed.kind().name()
        ))),
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

fn order_linked_closure(
    roots: &[AuthoredToolPath],
    dependencies: &BTreeMap<AuthoredToolPath, Vec<AuthoredToolPath>>,
    snapshots: &BTreeMap<AuthoredToolPath, LinkedToolSelection>,
) -> Result<Vec<LinkedToolSelection>, Error> {
    fn visit(
        path: &AuthoredToolPath,
        dependencies: &BTreeMap<AuthoredToolPath, Vec<AuthoredToolPath>>,
        snapshots: &BTreeMap<AuthoredToolPath, LinkedToolSelection>,
        visiting: &mut BTreeSet<AuthoredToolPath>,
        visited: &mut BTreeSet<AuthoredToolPath>,
        ordered: &mut Vec<LinkedToolSelection>,
    ) -> Result<(), Error> {
        if visited.contains(path) || !visiting.insert(path.clone()) {
            return Ok(());
        }
        let referenced = dependencies.get(path).ok_or_else(|| {
            Error::CorruptData("linked Tool closure lost dependency evidence".to_string())
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
                    Error::CorruptData("linked Tool closure lost source evidence".to_string())
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
    if linked_count > LINKED_TOOL_MAX_COUNT {
        return Err(Error::ProgramAdmission(format!(
            "Program may link at most {LINKED_TOOL_MAX_COUNT} Private and Shared Tools"
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
