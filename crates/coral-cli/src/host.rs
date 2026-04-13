use std::io::{IsTerminal, Write, stdin, stdout};

use dialoguer::{Input, Password, Select, theme::ColorfulTheme};

/// Terminal-facing output and host operations used by the CLI runner.
pub trait CliHost {
    /// Whether stdin is interactive.
    fn stdin_is_terminal(&self) -> bool;

    /// Whether stdout is interactive.
    fn stdout_is_terminal(&self) -> bool;

    /// Writes raw output to stdout.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    fn print(&mut self, text: &str) -> Result<(), anyhow::Error>;

    /// Writes one line to stdout.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    fn println(&mut self, text: &str) -> Result<(), anyhow::Error> {
        self.print(text)?;
        self.print("\n")
    }

    /// Opens a URL in the user's browser. Returns `true` if opened successfully.
    fn open_url(&mut self, url: &str) -> bool;
}

/// Prompt interactions used by the CLI runner.
pub trait CliPrompter {
    /// Displays a selection prompt.
    ///
    /// # Errors
    /// Returns an error if the prompt interaction fails.
    fn select(
        &mut self,
        prompt: &str,
        items: &[String],
        default: usize,
    ) -> Result<Option<usize>, anyhow::Error>;

    /// Prompts for plain text input.
    ///
    /// # Errors
    /// Returns an error if the prompt interaction fails.
    fn input_text(&mut self, prompt: &str, allow_empty: bool) -> Result<String, anyhow::Error>;

    /// Prompts for secret input.
    ///
    /// # Errors
    /// Returns an error if the prompt interaction fails.
    fn input_secret(&mut self, prompt: &str, allow_empty: bool) -> Result<String, anyhow::Error>;
}

/// Real terminal host for the shipping CLI.
#[derive(Default)]
pub struct RealCliHost;

impl RealCliHost {
    /// Creates the default terminal host.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl CliHost for RealCliHost {
    fn stdin_is_terminal(&self) -> bool {
        stdin().is_terminal()
    }

    fn stdout_is_terminal(&self) -> bool {
        stdout().is_terminal()
    }

    fn print(&mut self, text: &str) -> Result<(), anyhow::Error> {
        let mut handle = stdout().lock();
        handle.write_all(text.as_bytes())?;
        handle.flush()?;
        Ok(())
    }

    fn open_url(&mut self, url: &str) -> bool {
        let result = if cfg!(target_os = "macos") {
            std::process::Command::new("open").arg(url).status()
        } else if cfg!(target_os = "linux") {
            std::process::Command::new("xdg-open").arg(url).status()
        } else if cfg!(target_os = "windows") {
            std::process::Command::new("cmd")
                .args(["/c", "start", url])
                .status()
        } else {
            return false;
        };

        matches!(result, Ok(status) if status.success())
    }
}

/// Real prompt adapter backed by `dialoguer`.
#[derive(Default)]
pub struct DialoguerCliPrompter;

impl DialoguerCliPrompter {
    /// Creates the default prompt adapter.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl CliPrompter for DialoguerCliPrompter {
    fn select(
        &mut self,
        prompt: &str,
        items: &[String],
        default: usize,
    ) -> Result<Option<usize>, anyhow::Error> {
        let theme = ColorfulTheme::default();
        let refs = items.iter().map(String::as_str).collect::<Vec<_>>();
        Ok(Select::with_theme(&theme)
            .with_prompt(prompt)
            .items(&refs)
            .default(default)
            .interact_opt()?)
    }

    fn input_text(&mut self, prompt: &str, allow_empty: bool) -> Result<String, anyhow::Error> {
        let theme = ColorfulTheme::default();
        Ok(Input::<String>::with_theme(&theme)
            .with_prompt(prompt.to_string())
            .allow_empty(allow_empty)
            .interact_text()?)
    }

    fn input_secret(&mut self, prompt: &str, allow_empty: bool) -> Result<String, anyhow::Error> {
        let theme = ColorfulTheme::default();
        Ok(Password::with_theme(&theme)
            .with_prompt(prompt.to_string())
            .allow_empty_password(allow_empty)
            .interact()?)
    }
}
