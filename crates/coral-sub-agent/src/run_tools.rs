//! Stand-in for the module that runs the Sub-agent's tools.

use coral_run::RunModule;
use coral_task::{TaskCommands, TaskId};
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};

/// Who a Sub-agent's tools act as: the lead's caller, in the lead's Task.
pub(crate) struct Caller {
    pub(crate) principal: AuthenticatedPrincipal,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) task_id: TaskId,
}

pub(crate) struct RunTools {
    _commands: TaskCommands,
    _runs: RunModule,
}

impl RunTools {
    pub(crate) const fn new(commands: TaskCommands, runs: RunModule) -> Self {
        Self {
            _commands: commands,
            _runs: runs,
        }
    }

    /// Runs the tool `name` with the arguments the model wrote, and returns the JSON text the model reads.
    #[expect(
        clippy::unused_async,
        clippy::unused_async_trait_impl,
        reason = "a stand-in"
    )]
    pub(crate) async fn call(&self, caller: &Caller, name: &str, _arguments: &str) -> String {
        let task_id = caller.task_id.as_str();
        format!("{{\"error\":\"{name} is not built yet in {task_id}\"}}")
    }
}
