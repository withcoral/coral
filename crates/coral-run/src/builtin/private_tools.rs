//! Performs `coral.addCustomTool`, `updateCustomTool` and `removeCustomTool`: Private Tool
//! authoring as the invoking Principal, in its own namespace. Each
//! call is this Invocation of this Run, recorded like any other, not a
//! separate Task Interaction. The source a save receives is the module
//! admission extracted for the function its literal call names. The Program
//! that saves a function cannot call it by its path, because admission linked
//! its functions before it ran, and a save or delete outlives the Run that
//! made it (ADR 0031).

use coral_catalog::{
    AuthoredToolKind, RemoveToolArguments, RemovedToolResult, SavedToolResult, ToolSaveArguments,
};
use coral_search::SearchIndex;
use coral_tools::{
    AuthoredToolDescription, AuthoredToolSource, PrivateToolChangeContext, PrivateToolModule,
    PrivateToolPath, PrivateToolSave,
};

use super::BuiltinHost;
use crate::error::InputProblem;
use crate::{CoralFailure, Error, Rejection};

/// What Private Tool authoring from a Program acts through: the module
/// that compiles and stores functions, and the index it notifies after each
/// commit.
#[derive(Clone)]
pub struct PrivateTools {
    // Shared, so a RunModule and every Run's Tool host hold one pointer.
    inner: std::sync::Arc<PrivateToolsInner>,
}

struct PrivateToolsInner {
    module: PrivateToolModule,
    index: SearchIndex,
}

impl PrivateTools {
    /// Authoring through `module`, notifying `index` after each commit.
    /// Pass the module the rest of the process saves through, so every save
    /// shares one compiler and its deadline.
    #[must_use]
    pub fn new(module: PrivateToolModule, index: SearchIndex) -> Self {
        Self {
            inner: std::sync::Arc::new(PrivateToolsInner { module, index }),
        }
    }
}

/// `addCustomTool`'s or `updateCustomTool`'s arguments, each checked by
/// coral-tools.
#[derive(Clone)]
pub(crate) struct SaveTool {
    save: PrivateToolSave,
}

impl SaveTool {
    /// `addCustomTool`, which only creates.
    pub(super) fn add(arguments: &ToolSaveArguments) -> Result<Self, Rejection> {
        Self::checked(arguments).map(|save| Self {
            save: save.create_only(),
        })
    }

    /// `updateCustomTool`, which only replaces, at the expected revision if any.
    pub(super) fn update(arguments: &ToolSaveArguments) -> Result<Self, Rejection> {
        Self::checked(arguments).map(|save| Self {
            save: save.replace_only(arguments.expected_revision()),
        })
    }

    /// Checks the path, description and source before any other work, and
    /// names every one to correct.
    fn checked(arguments: &ToolSaveArguments) -> Result<PrivateToolSave, Rejection> {
        let path = PrivateToolPath::parse(arguments.path());
        let description = AuthoredToolDescription::parse(arguments.description());
        let source = AuthoredToolSource::parse(arguments.source());
        match (path, description, source) {
            (Ok(path), Ok(description), Ok(source)) => {
                Ok(PrivateToolSave::new(path, description, source)
                    .declaring_default(arguments.policy()))
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
        let private_tools = authoring(host)?;
        let context = change_context(host)?;
        let declarations =
            coral_search::CallerToolDeclarations::new(host.search, host.actor, host.workspace_id);
        let prepared = private_tools
            .inner
            .module
            .prepare_save(
                &context,
                host.workspace_id,
                self.save.clone(),
                Some(&declarations),
            )
            .await
            .map_err(|error| self.failure(error))?;
        #[cfg(test)]
        host.run.prepared();
        if !host.run.still_current().await? {
            return Err(Error::ProgramHostTerminated);
        }
        let mut transaction = host.database.begin().await?;
        let private_tool = private_tools
            .inner
            .module
            .save_in_transaction(&mut transaction, prepared)
            .await
            .map_err(|error| self.failure(error))?;
        let result = SavedToolResult {
            call_path: call_path(private_tool.path()),
            description: private_tool.description().as_str().to_owned(),
            declaration: private_tool.declaration().as_str().to_owned(),
            diagnostics: private_tool
                .diagnostics()
                .iter()
                .map(coral_search::projection::diagnostic)
                .collect(),
            diagnostics_truncated: private_tool.diagnostics_truncated(),
            revision: private_tool.revision(),
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
        private_tools
            .inner
            .index
            .sync_authored_tool(host.workspace_id.clone(), private_tool.id().clone());
        Ok(result.into_tool_result())
    }
}

impl SaveTool {
    /// Why this save failed, as the Program reads it. A taken path names
    /// itself and the Tool that changes it; any other failure is about the
    /// source.
    fn failure(&self, error: coral_tools::Error) -> Error {
        if matches!(error, coral_tools::Error::PrivateToolExists) {
            return Error::InvocationRejected(Rejection::naming(
                CoralFailure::PrivateToolExists,
                vec![InputProblem::new(
                    "path",
                    format!(
                        "{} is one of your tools; change it with coral.updateCustomTool, or choose \
                         another path",
                        call_path(self.save.path())
                    ),
                )],
            ));
        }
        failure("source", error)
    }
}

/// `removeCustomTool`'s path, checked by coral-tools.
#[derive(Clone)]
pub(crate) struct RemoveCustomTool {
    path: PrivateToolPath,
}

impl RemoveCustomTool {
    /// `removeCustomTool`, with its path checked before any other work.
    pub(super) fn new(arguments: &RemoveToolArguments) -> Result<Self, Rejection> {
        PrivateToolPath::parse(arguments.path())
            .map(|path| Self { path })
            .map_err(|error| Rejection::invalid_inputs(vec![problem("path", &error)]))
    }

    /// Deletes the invoking Principal's Private Tool at the path together
    /// with its exact rule, then tells Universal Search.
    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        let private_tools = authoring(host)?;
        let context = change_context(host)?;
        if !host.run.still_current().await? {
            return Err(Error::ProgramHostTerminated);
        }
        let mut transaction = host.database.begin().await?;
        let deleted = private_tools
            .inner
            .module
            .delete_in_transaction(&mut transaction, &context, host.workspace_id, &self.path)
            .await
            .map_err(|error| failure("path", error))?;
        // The rule rows are coral-policy's, which coral-tools cannot call,
        // so the delete's transaction joins both.
        coral_policy::remove_authored_tool_rules_in_transaction(
            &mut transaction,
            host.workspace_id,
            AuthoredToolKind::Private,
            &[deleted.tool_key()],
        )
        .await?;
        transaction.commit().await?;
        // The delete never touches the search tables; this removes the
        // Private Tool's rows sooner than a reconcile pass.
        private_tools
            .inner
            .index
            .sync_authored_tool(host.workspace_id.clone(), deleted);
        Ok(RemovedToolResult {
            call_path: call_path(&self.path),
        }
        .into_tool_result())
    }
}

/// The authoring wiring; a Run without it cannot author.
fn authoring<'a>(host: &BuiltinHost<'a>) -> Result<&'a PrivateTools, Error> {
    host.private_tools
        .ok_or_else(|| Error::InvocationRejected(CoralFailure::ToolUnavailable.into()))
}

fn call_path(path: &PrivateToolPath) -> String {
    AuthoredToolKind::Private.call_path(path.as_str())
}

/// The invoking Principal, at the Run's clock. The trace id is the
/// Invocation's, so the lifecycle Event joins the Run's trace.
fn change_context(host: &BuiltinHost<'_>) -> Result<PrivateToolChangeContext, Error> {
    Ok(PrivateToolChangeContext::new(
        host.actor.clone(),
        host.run.now()?,
        coral_workspace::TraceId::current_or_new(),
    ))
}

/// The reason coral-tools gives for a value it refused. It is bounded,
/// and names only what the Program wrote: its own source, path or
/// description.
fn problem(field: &str, error: &coral_tools::Error) -> InputProblem {
    InputProblem::new(
        field,
        error.safe_rejection().map_or_else(
            || "has a value Coral does not accept".to_owned(),
            |(_code, reason)| reason,
        ),
    )
}

/// A failure the Program can read and correct, or a host error. `field` is
/// the argument an authoring failure names. A save names its taken path
/// itself: [`SaveTool::failure`].
fn failure(field: &str, error: coral_tools::Error) -> Error {
    use coral_tools::Error as E;
    match error {
        E::NotFound(AuthoredToolKind::Private) => {
            Error::InvocationRejected(CoralFailure::PrivateToolNotFound.into())
        }
        E::PrivateToolExists => Error::InvocationRejected(CoralFailure::PrivateToolExists.into()),
        E::RevisionConflict {
            kind: AuthoredToolKind::Private,
            current,
        } => Error::InvocationRejected(Rejection::naming(
            CoralFailure::PrivateToolRevisionConflict,
            vec![InputProblem::new(
                "expectedRevision",
                format!("the current revision is {current}"),
            )],
        )),
        // The path holds or lies under the caller's other functions.
        E::PathConflict(AuthoredToolKind::Private) => {
            Error::InvocationRejected(Rejection::invalid_inputs(vec![problem("path", &error)]))
        }
        E::InvalidInput(_)
        | E::Admission(AuthoredToolKind::Private, _)
        | E::NotData(AuthoredToolKind::Private, _) => {
            Error::InvocationRejected(Rejection::invalid_inputs(vec![problem(field, &error)]))
        }
        // Nothing judged the source: the Program may call again.
        E::CompilationTimedOut(AuthoredToolKind::Private) | E::CompilerBusy | E::NoCompiler => {
            Error::InvocationRejected(Rejection::naming(
                CoralFailure::ToolUnavailable,
                vec![problem(field, &error)],
            ))
        }
        E::PermissionDenied(_) => Error::InvocationRejected(CoralFailure::AuthorityLost.into()),
        E::Workspace(error) if crate::enforcement::loses_authority(&error) => {
            Error::InvocationRejected(CoralFailure::AuthorityLost.into())
        }
        other => other.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthoredToolKind, CoralFailure, Error, failure};

    // Catches: a save no compiler judged that ends the Run instead of
    // failing the call as retryable.
    #[test]
    fn a_save_no_compiler_judged_is_a_retryable_tool_failure() {
        use coral_tools::Error as E;
        for error in [
            E::CompilationTimedOut(AuthoredToolKind::Private),
            E::CompilerBusy,
            E::NoCompiler,
        ] {
            let code = error.code();
            match failure("source", error) {
                Error::InvocationRejected(rejection) => {
                    assert_eq!(rejection.failure(), CoralFailure::ToolUnavailable, "{code}");
                }
                other => panic!("{code}: {other:?}"),
            }
        }
    }
}
