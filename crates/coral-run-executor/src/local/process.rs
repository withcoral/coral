//! The real process starter: `coral run-executor` as a child of this
//! process.

use std::ffi::OsString;
use std::io::{self, Write as _};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};

use super::{LocalChild, ProcessStarter};
use crate::link::Token;

/// How Coral starts a local child: its own binary, with the Run Executor's
/// flags and nothing of Coral's environment but what it names here.
#[derive(Clone, Debug)]
pub struct ChildCommand {
    /// The `coral` binary: this process's own.
    pub program: PathBuf,
    /// Where the Run Executor listener answers on this host, such as
    /// `http://127.0.0.1:50052`.
    pub coral_address: String,
    /// The most Runs the child holds at once.
    pub max_runs_held: u32,
    /// The child's Program Compiler heap limit.
    pub compiler_heap_bytes: usize,
    /// The child's whole environment.
    pub environment: Vec<(OsString, OsString)>,
}

impl ProcessStarter for ChildCommand {
    fn start(&mut self, token: &Token) -> io::Result<Box<dyn LocalChild>> {
        let mut command = Command::new(&self.program);
        command
            .args(["run-executor", "--token-stdin", "--coral-address"])
            .arg(&self.coral_address)
            .args(["--max-runs-held", &self.max_runs_held.to_string()])
            .args([
                "--compiler-heap-bytes",
                &self.compiler_heap_bytes.to_string(),
            ])
            .env_clear()
            .envs(self.environment.iter().map(|(name, value)| (name, value)))
            // stdout and stderr stay Coral's own.
            .stdin(Stdio::piped());
        own_process_group(&mut command);
        let mut child = command.spawn()?;
        lower_priority(&child);
        let written = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("the child has no stdin"))
            .and_then(|mut stdin| {
                // The token is never copied into another buffer.
                stdin.write_all(token.expose().as_bytes())?;
                stdin.write_all(b"\n")?;
                Ok(stdin)
            });
        match written {
            Ok(stdin) => Ok(Box::new(Process {
                child,
                stdin: Some(stdin),
            })),
            Err(error) => {
                // The supervisor never sees this child, so it ends here.
                drop(child.kill());
                drop(child.wait());
                Err(error)
            }
        }
    }
}

/// The child's own process group, in Coral's session.
#[cfg(unix)]
fn own_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt as _;

    command.process_group(0);
}

/// `BELOW_NORMAL_PRIORITY_CLASS`, `CREATE_NEW_PROCESS_GROUP` and
/// `CREATE_NO_WINDOW`: the priority is set as the process starts.
#[cfg(windows)]
fn own_process_group(command: &mut Command) {
    use std::os::windows::process::CommandExt as _;

    command.creation_flags(0x0000_4000 | 0x0000_0200 | 0x0800_0000);
}

/// Priority +10, below Coral's. A child that keeps Coral's priority still
/// runs.
#[cfg(unix)]
fn lower_priority(child: &Child) {
    let pid = rustix::process::Pid::from_child(child);
    if rustix::process::setpriority_process(Some(pid), 10).is_err() {
        tracing::warn!("a local Run Executor keeps Coral's priority");
    }
}

#[cfg(windows)]
fn lower_priority(_child: &Child) {}

struct Process {
    child: Child,
    /// The pipe the token went down, open until Coral retires the child.
    stdin: Option<ChildStdin>,
}

impl LocalChild for Process {
    fn close_stdin(&mut self) {
        self.stdin = None;
    }

    fn kill(&mut self) {
        // A child that already exited needs no kill.
        drop(self.child.kill());
    }

    fn exited(&mut self) -> Option<Option<i32>> {
        match self.child.try_wait() {
            Ok(status) => Some(status?.code()),
            // A child Coral cannot wait for is no child it can keep.
            Err(_) => Some(None),
        }
    }
}
