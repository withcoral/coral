//! Performs `coral.workspace.saveFunction` and `deleteFunction`: Saved
//! Function authoring as the invoking Principal, in its own namespace. Each
//! call is this Invocation of this Run, recorded like any other; the Run's
//! Task records no separate Attempt. The Program that saves a function
//! cannot call it, because admission linked its functions before it ran.

use coral_catalog::{
    DeleteFunctionArguments, DeleteFunctionResult, SaveFunctionArguments, SaveFunctionResult,
};
use coral_functions::{
    SavedFunctionChangeContext, SavedFunctionDescription, SavedFunctionPath, SavedFunctionSave,
    SavedFunctionSource,
};

use super::BuiltinHost;
use crate::error::InputProblem;
use crate::{CoralFailure, Error, Rejection};

/// The largest integer a JSON reader holds exactly.
const JSON_SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;

/// `saveFunction`'s arguments, each checked by coral-functions.
#[derive(Clone)]
pub(crate) struct SaveFunction {
    save: SavedFunctionSave,
}

impl SaveFunction {
    /// Checks the path, description and source before any other work, and
    /// names every one to correct.
    pub(super) fn new(arguments: &SaveFunctionArguments) -> Result<Self, Rejection> {
        let path = SavedFunctionPath::parse(arguments.path());
        let description = SavedFunctionDescription::parse(arguments.description());
        let source = SavedFunctionSource::parse(arguments.source());
        match (path, description, source) {
            (Ok(path), Ok(description), Ok(source)) => Ok(Self {
                save: SavedFunctionSave::new(path, description, source),
            }),
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
    /// can discover, then tells Universal Search.
    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        let Some(functions) = host.functions else {
            return Err(Error::InvocationRejected(
                CoralFailure::ToolUnavailable.into(),
            ));
        };
        let context = change_context(host)?;
        let declarations =
            coral_search::CallerToolDeclarations::new(host.search, host.actor, host.workspace_id);
        let prepared = functions
            .module
            .prepare_save(
                &context,
                host.workspace_id,
                self.save.clone(),
                Some(&declarations),
            )
            .await
            .map_err(|error| failure("source", error))?;
        let mut transaction = host.database.begin().await?;
        let function = functions
            .module
            .save_in_transaction(&mut transaction, prepared)
            .await
            .map_err(|error| failure("source", error))?;
        transaction.commit().await?;
        // Indexing never undoes or rejects a committed save.
        functions
            .index
            .sync_function(host.workspace_id.clone(), function.id().clone());
        // As `search` does: fail rather than hand the Program a number V8
        // rounds. Only corrupt stored data holds one.
        if function.diagnostics().iter().any(|diagnostic| {
            [
                Some(diagnostic.code()),
                diagnostic.line(),
                diagnostic.column(),
            ]
            .into_iter()
            .flatten()
            .any(|number| number > JSON_SAFE_INTEGER_MAX)
        }) {
            return Err(Error::CorruptData(
                "a saved diagnostic holds an integer past the exact JSON range".to_owned(),
            ));
        }
        Ok(SaveFunctionResult {
            call_path: format!("coral.functions.{}", function.path().as_str()),
            description: function.description().as_str().to_owned(),
            declaration: function.declaration().as_str().to_owned(),
            diagnostics: function
                .diagnostics()
                .iter()
                .map(coral_search::projection::diagnostic)
                .collect(),
            diagnostics_truncated: function.diagnostics_truncated(),
        }
        .into_tool_result())
    }
}

/// `deleteFunction`'s path, checked by coral-functions.
#[derive(Clone)]
pub(crate) struct DeleteFunction {
    path: SavedFunctionPath,
}

impl DeleteFunction {
    pub(super) fn new(arguments: &DeleteFunctionArguments) -> Result<Self, Rejection> {
        SavedFunctionPath::parse(arguments.path())
            .map(|path| Self { path })
            .map_err(|error| Rejection::invalid_inputs(vec![problem("path", &error)]))
    }

    /// Deletes the invoking Principal's function at the path, then tells
    /// Universal Search.
    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        let Some(functions) = host.functions else {
            return Err(Error::InvocationRejected(
                CoralFailure::ToolUnavailable.into(),
            ));
        };
        let context = change_context(host)?;
        let mut transaction = host.database.begin().await?;
        let deleted = functions
            .module
            .delete_in_transaction(&mut transaction, &context, host.workspace_id, &self.path)
            .await
            .map_err(|error| failure("path", error))?;
        transaction.commit().await?;
        // The delete never touches the search tables; this removes the
        // function's rows sooner than a reconcile pass.
        functions
            .index
            .sync_function(host.workspace_id.clone(), deleted);
        Ok(DeleteFunctionResult {
            call_path: format!("coral.functions.{}", self.path.as_str()),
        }
        .into_tool_result())
    }
}

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
            Error::InvocationRejected(CoralFailure::SavedFunctionNotFound.into())
        }
        E::InvalidInput(_) | E::SavedFunctionAdmission(_) => {
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
