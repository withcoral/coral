//! Browser opener helpers for CLI-owned interactive flows.

use std::io;
use std::process::Command;

/// Opens a URL in the user's default browser.
///
/// # Errors
///
/// Returns an error if the platform opener cannot be launched or exits
/// unsuccessfully.
pub(crate) fn open_url(url: &str) -> Result<(), io::Error> {
    let status = if cfg!(target_os = "macos") {
        Command::new("open").arg(url).status()
    } else if cfg!(target_os = "windows") {
        Command::new("cmd").args(["/C", "start", "", url]).status()
    } else {
        Command::new("xdg-open").arg(url).status()
    }?;

    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "browser opener exited with {status}"
        )))
    }
}
