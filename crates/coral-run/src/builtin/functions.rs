//! Performs `coral.addTool`, `updateTool` and `removeTool`: Private Tool
//! authoring as the invoking Principal, in its own namespace. Each
//! call is this Invocation of this Run, recorded like any other, not a
//! separate Task Interaction. The source a save receives is the module
//! admission extracted for the function its literal call names. The Program
//! that saves a function cannot call it by its path, because admission linked
//! its functions before it ran, and a save or delete outlives the Run that
//! made it (ADR 0031).

use coral_catalog::{RemoveToolArguments, RemovedToolResult, SavedToolResult, ToolSaveArguments};
use coral_functions::{
    SavedFunctionChangeContext, SavedFunctionDescription, SavedFunctionModule, SavedFunctionPath,
    SavedFunctionSave, SavedFunctionSource,
};
use coral_search::SearchIndex;

use super::BuiltinHost;
use crate::error::InputProblem;
use crate::{CoralFailure, Error, Rejection};

/// What Saved Function authoring from a Program acts through: the module
/// that compiles and stores functions, and the index it notifies after each
/// commit.
#[derive(Clone)]
pub struct SavedFunctions {
    // Shared, so a RunModule and every Run's Tool host hold one pointer.
    inner: std::sync::Arc<SavedFunctionsInner>,
}

struct SavedFunctionsInner {
    module: SavedFunctionModule,
    index: SearchIndex,
}

impl SavedFunctions {
    /// Authoring through `module`, notifying `index` after each commit.
    /// Pass the module the rest of the process saves through, so every save
    /// shares one compiler and its deadline.
    #[must_use]
    pub fn new(module: SavedFunctionModule, index: SearchIndex) -> Self {
        Self {
            inner: std::sync::Arc::new(SavedFunctionsInner { module, index }),
        }
    }
}

/// `addTool`'s or `updateTool`'s arguments, each checked by
/// coral-functions.
#[derive(Clone)]
pub(crate) struct SaveTool {
    save: SavedFunctionSave,
}

impl SaveTool {
    /// `addTool`, which only creates.
    pub(super) fn add(arguments: &ToolSaveArguments) -> Result<Self, Rejection> {
        Self::checked(arguments).map(|save| Self {
            save: save.create_only(),
        })
    }

    /// `updateTool`, which only replaces, at the expected revision if any.
    pub(super) fn update(arguments: &ToolSaveArguments) -> Result<Self, Rejection> {
        Self::checked(arguments).map(|save| Self {
            save: save.replace_only(arguments.expected_revision()),
        })
    }

    /// Checks the path, description and source before any other work, and
    /// names every one to correct.
    fn checked(arguments: &ToolSaveArguments) -> Result<SavedFunctionSave, Rejection> {
        let path = SavedFunctionPath::parse(arguments.path());
        let description = SavedFunctionDescription::parse(arguments.description());
        let source = SavedFunctionSource::parse(arguments.source());
        match (path, description, source) {
            (Ok(path), Ok(description), Ok(source)) => {
                let save = SavedFunctionSave::new(path, description, source);
                Ok(match arguments.policy() {
                    Some(policy) => save.declaring_default(policy),
                    None => save,
                })
            }
            (path, description, source) => Err(Rejection::invalid_inputs(
                [
                    ("path", path.err()),
                    ("description", description.err()),
                    ("source", source.err()),
                ]
                .into_iter()
                .filter_map(|(field, error)| error.map(|error| problem(field, &error)))
                .collect(),
            )),
        }
    }

    /// Saves the function as the invoking Principal, typed from the Tools it
    /// can discover, then tells Universal Search. The compile runs before
    /// the write, under the module's own deadline; a Run that ended
    /// meanwhile commits nothing.
    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        let functions = authoring(host)?;
        let context = change_context(host)?;
        let declarations =
            coral_search::CallerToolDeclarations::new(host.search, host.actor, host.workspace_id);
        let prepared = functions
            .inner
            .module
            .prepare_save(
                &context,
                host.workspace_id,
                self.save.clone(),
                Some(&declarations),
            )
            .await
            .map_err(|error| failure("source", error))?;
        #[cfg(test)]
        host.run.prepared();
        if !host.run.still_current().await? {
            return Err(Error::ProgramHostTerminated);
        }
        let mut transaction = host.database.begin().await?;
        let function = functions
            .inner
            .module
            .save_in_transaction(&mut transaction, prepared)
            .await
            .map_err(|error| failure("source", error))?;
        let result = SavedToolResult {
            call_path: call_path(function.path()),
            description: function.description().as_str().to_owned(),
            declaration: function.declaration().as_str().to_owned(),
            diagnostics: function
                .diagnostics()
                .iter()
                .map(coral_search::projection::diagnostic)
                .collect(),
            diagnostics_truncated: function.diagnostics_truncated(),
            revision: function.revision(),
        };
        // As `search` does: fail rather than hand the Program a number V8
        // rounds, and save nothing. Only corrupt compiler output or a
        // corrupt revision holds one.
        if !result.is_exact_in_json() {
            return Err(Error::CorruptData(
                "a saved tool holds an integer past the exact JSON range".to_owned(),
            ));
        }
        transaction.commit().await?;
        // Indexing never undoes or rejects a committed save.
        functions
            .inner
            .index
            .sync_function(host.workspace_id.clone(), function.id().clone());
        Ok(result.into_tool_result())
    }
}

/// `removeTool`'s path, checked by coral-functions.
#[derive(Clone)]
pub(crate) struct RemoveTool {
    path: SavedFunctionPath,
}

impl RemoveTool {
    /// `removeTool`, with its path checked before any other work.
    pub(super) fn new(arguments: &RemoveToolArguments) -> Result<Self, Rejection> {
        SavedFunctionPath::parse(arguments.path())
            .map(|path| Self { path })
            .map_err(|error| Rejection::invalid_inputs(vec![problem("path", &error)]))
    }

    /// Deletes the invoking Principal's function at the path, then tells
    /// Universal Search.
    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        let functions = authoring(host)?;
        let context = change_context(host)?;
        if !host.run.still_current().await? {
            return Err(Error::ProgramHostTerminated);
        }
        let mut transaction = host.database.begin().await?;
        let deleted = functions
            .inner
            .module
            .delete_in_transaction(&mut transaction, &context, host.workspace_id, &self.path)
            .await
            .map_err(|error| failure("path", error))?;
        transaction.commit().await?;
        // The delete never touches the search tables; this removes the
        // function's rows sooner than a reconcile pass.
        functions
            .inner
            .index
            .sync_function(host.workspace_id.clone(), deleted);
        Ok(RemovedToolResult {
            call_path: call_path(&self.path),
        }
        .into_tool_result())
    }
}

/// The authoring wiring; a Run without it cannot author.
fn authoring<'a>(host: &BuiltinHost<'a>) -> Result<&'a SavedFunctions, Error> {
    host.functions
        .ok_or_else(|| Error::InvocationRejected(CoralFailure::ToolUnavailable.into()))
}

fn call_path(path: &SavedFunctionPath) -> String {
    format!("coral.private.{}", path.as_str())
}

/// The invoking Principal, at the Run's clock. The trace id is the
/// Invocation's, so the lifecycle Event joins the Run's trace.
fn change_context(host: &BuiltinHost<'_>) -> Result<SavedFunctionChangeContext, Error> {
    Ok(SavedFunctionChangeContext::new(
        host.actor.clone(),
        host.run.now()?,
        coral_workspace::TraceId::current_or_new(),
    ))
}

/// The reason coral-functions gives for a value it refused. It is bounded,
/// and names only what the Program wrote: its own source, path or
/// description.
fn problem(field: &str, error: &coral_functions::Error) -> InputProblem {
    InputProblem::new(
        field,
        error.safe_rejection().map_or_else(
            || "has a value Coral does not accept".to_owned(),
            |(_code, reason)| reason,
        ),
    )
}

/// A failure the Program can read and correct, or a host error. `field` is
/// the argument an authoring failure names.
fn failure(field: &str, error: coral_functions::Error) -> Error {
    use coral_functions::Error as E;
    match error {
        E::SavedFunctionNotFound => {
            Error::InvocationRejected(CoralFailure::PrivateToolNotFound.into())
        }
        E::PrivateToolExists => Error::InvocationRejected(CoralFailure::PrivateToolExists.into()),
        E::PrivateToolRevisionConflict { current } => Error::InvocationRejected(Rejection::naming(
            CoralFailure::PrivateToolRevisionConflict,
            vec![InputProblem::new(
                "expectedRevision",
                format!("the current revision is {current}"),
            )],
        )),
        // The path holds or lies under the caller's other functions.
        E::SavedFunctionPathConflict => {
            Error::InvocationRejected(Rejection::invalid_inputs(vec![problem("path", &error)]))
        }
        E::InvalidInput(_) | E::SavedFunctionAdmission(_) | E::PrivateToolNotData(_) => {
            Error::InvocationRejected(Rejection::invalid_inputs(vec![problem(field, &error)]))
        }
        E::PackageTooLarge { .. } => Error::InvocationRejected(Rejection::naming(
            CoralFailure::InputTooLarge,
            vec![problem(field, &error)],
        )),
        E::SavedFunctionCompilationTimedOut | E::CompilerBusy => Error::InvocationRejected(
            Rejection::naming(CoralFailure::ToolUnavailable, vec![problem(field, &error)]),
        ),
        E::PermissionDenied(_) => Error::InvocationRejected(CoralFailure::AuthorityLost.into()),
        E::Workspace(error) if crate::enforcement::loses_authority(&error) => {
            Error::InvocationRejected(CoralFailure::AuthorityLost.into())
        }
        other => other.into(),
    }
}
