//! Browser opener helpers for CLI-owned interactive flows.

use std::io;
use std::process::{Command, Stdio};

#[derive(Debug, Clone, Copy)]
enum BrowserOpener {
    Macos,
    Windows,
    XdgOpen,
}

impl BrowserOpener {
    fn current() -> Self {
        if cfg!(target_os = "macos") {
            Self::Macos
        } else if cfg!(target_os = "windows") {
            Self::Windows
        } else {
            Self::XdgOpen
        }
    }
}

/// Opens a URL in the user's default browser.
///
/// # Errors
///
/// Returns an error if the platform opener cannot be launched or exits
/// unsuccessfully.
pub(crate) fn open_url(url: &str) -> Result<(), io::Error> {
    let status = browser_command(BrowserOpener::current(), url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;

    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "browser opener exited with {status}"
        )))
    }
}

fn browser_command(opener: BrowserOpener, url: &str) -> Command {
    let mut command = match opener {
        BrowserOpener::Macos => Command::new("open"),
        BrowserOpener::Windows => Command::new("rundll32"),
        BrowserOpener::XdgOpen => Command::new("xdg-open"),
    };
    match opener {
        BrowserOpener::Macos | BrowserOpener::XdgOpen => {
            command.arg(url);
        }
        BrowserOpener::Windows => {
            command.args(["url.dll,FileProtocolHandler", url]);
        }
    }
    command
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use super::{BrowserOpener, browser_command};

    #[test]
    fn windows_opener_passes_query_urls_without_cmd_shell_parsing() {
        let url = "https://provider.example/oauth?response_type=code&client_id=abc&state=xyz";
        let command = browser_command(BrowserOpener::Windows, url);

        assert_eq!(command.get_program(), OsStr::new("rundll32"));
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            vec![OsStr::new("url.dll,FileProtocolHandler"), OsStr::new(url)]
        );
    }
}
