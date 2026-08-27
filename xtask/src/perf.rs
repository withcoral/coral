//! Performance regression checks for user-visible Coral commands.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde_json::Value;

const DEFAULT_SQL: &str = "select * from coral.tables";

/// The workspace this check creates and measures against.
///
/// A fresh state directory owns no workspace, so the harness has to create the
/// one it benchmarks and name it on every command that follows.
const WORKSPACE: &str = "perf";

#[derive(Debug, clap::Args)]
pub(crate) struct Args {
    /// Path to the release Coral binary to benchmark.
    #[arg(long, default_value = "target/release/coral")]
    coral_bin: PathBuf,

    /// Fail when hyperfine reports a mean above this many seconds.
    #[arg(long, default_value_t = 0.75)]
    max_mean_seconds: f64,

    /// Number of measured hyperfine runs.
    #[arg(long, default_value_t = 5)]
    runs: u32,

    /// Number of hyperfine warmup runs.
    #[arg(long, default_value_t = 1)]
    warmup: u32,

    /// Fake token used to install the GitHub source without real credentials.
    #[arg(long, default_value = "coral-ci-fake-token")]
    github_token: String,
}

pub(crate) fn run(args: &Args) -> Result<bool> {
    validate_args(args)?;
    require_command("hyperfine")?;

    let coral_bin = resolve_coral_bin(&args.coral_bin)?;
    let temp_dir = TempDir::create("coral-tables-perf")?;
    let config_dir = prepare_config_dir(temp_dir.path())?;
    provision_measured_workspace(&coral_bin, &config_dir, &args.github_token)?;

    let result_json = temp_dir.path().join("hyperfine.json");
    run_hyperfine(args, &coral_bin, &config_dir, &result_json)?;
    let result = load_hyperfine_result(&result_json)?;

    Ok(report_measurement(&result, args.max_mean_seconds))
}

/// Prints the measurement and answers whether it stayed within the threshold.
fn report_measurement(result: &HyperfineResult, max_mean_seconds: f64) -> bool {
    println!(
        "coral.tables mean: {:.3}s (stddev {:.3}s, threshold {:.3}s)",
        result.mean, result.stddev, max_mean_seconds
    );
    if is_regression(result.mean, max_mean_seconds) {
        eprintln!(
            "Performance regression: mean {:.3}s exceeds {:.3}s",
            result.mean, max_mean_seconds
        );
        return false;
    }
    true
}

/// The pass/fail rule this check exists to apply: a mean at the threshold still
/// passes, and only one above it counts as a regression.
fn is_regression(mean: f64, max_mean_seconds: f64) -> bool {
    mean > max_mean_seconds
}

fn validate_args(args: &Args) -> Result<()> {
    if args.max_mean_seconds <= 0.0 {
        bail!("--max-mean-seconds must be positive");
    }
    if args.runs == 0 {
        bail!("--runs must be positive");
    }
    Ok(())
}

fn require_command(command: &str) -> Result<()> {
    let status = Command::new(command)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .with_context(|| format!("{command} is required for the coral.tables performance check"))?;
    if !status.success() {
        bail!("{command} is required for the coral.tables performance check");
    }
    Ok(())
}

/// Resolves the binary under test to an absolute path and refuses anything that
/// is not a file we can hand to hyperfine.
fn resolve_coral_bin(coral_bin: &Path) -> Result<PathBuf> {
    let coral_bin = absolute_path(coral_bin)?;
    ensure_executable(&coral_bin)?;
    Ok(coral_bin)
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()
        .context("resolving current directory")?
        .join(path))
}

fn ensure_executable(path: &Path) -> Result<()> {
    let metadata = fs::metadata(path).with_context(|| format!("reading {}", path.display()))?;
    if !metadata.is_file() {
        bail!("Coral binary is not a file: {}", path.display());
    }
    Ok(())
}

/// Lays out a throwaway config directory so the check never reads or writes the
/// developer's own Coral state.
fn prepare_config_dir(temp_dir: &Path) -> Result<PathBuf> {
    let config_dir = temp_dir.join("coral-config");
    fs::create_dir_all(&config_dir)
        .with_context(|| format!("creating {}", config_dir.display()))?;
    fs::write(
        config_dir.join("config.toml"),
        "[credentials]\nstorage = \"database\"\nencryption_key_source = \"file\"\n",
    )
    .with_context(|| format!("writing {}", config_dir.join("config.toml").display()))?;
    Ok(config_dir)
}

/// Brings the fresh state directory to the state the benchmark measures: the
/// workspace exists, carries the github source, and has answered the query once
/// so the timed runs are not paying for first-run work.
fn provision_measured_workspace(
    coral_bin: &Path,
    config_dir: &Path,
    github_token: &str,
) -> Result<()> {
    create_workspace(coral_bin, config_dir)?;
    install_github_source(coral_bin, config_dir, github_token)?;
    run_coral_sql(coral_bin, config_dir)
}

/// Builds a `coral` invocation pinned to this check's config directory and
/// workspace, so every command targets the workspace the check provisions.
fn coral_command(coral_bin: &Path, config_dir: &Path) -> Command {
    let mut command = Command::new(coral_bin);
    command
        .env("CORAL_CONFIG_DIR", config_dir)
        .env("CORAL_WORKSPACE", WORKSPACE);
    command
}

/// Builds the invocation that provisions the workspace the check measures.
///
/// Separate from running it so a test can read back the workspace this check
/// creates and the environment it carries, which is the pairing a fresh state
/// directory depends on and the one that broke when provisioning became
/// explicit.
fn workspace_create_command(coral_bin: &Path, config_dir: &Path) -> Command {
    let mut command = coral_command(coral_bin, config_dir);
    command.args(["workspace", "create", WORKSPACE]);
    command
}

fn create_workspace(coral_bin: &Path, config_dir: &Path) -> Result<()> {
    let output = workspace_create_command(coral_bin, config_dir)
        .output()
        .with_context(|| format!("running {} workspace create", coral_bin.display()))?;

    if !output.status.success() {
        print!("{}", command_log(&output));
        bail!("failed to create the {WORKSPACE} workspace");
    }

    println!("Created the {WORKSPACE} workspace.");
    Ok(())
}

fn install_github_source(coral_bin: &Path, config_dir: &Path, github_token: &str) -> Result<()> {
    let output = coral_command(coral_bin, config_dir)
        .args(["source", "add", "github"])
        .env("GITHUB_TOKEN", github_token)
        .output()
        .with_context(|| format!("running {} source add github", coral_bin.display()))?;

    let log = command_log(&output);

    if !output.status.success() {
        print!("{log}");
        bail!("failed to install github source with fake credentials");
    }

    println!("Installed github source with fake credentials.");
    print_tail(&log, 20);
    Ok(())
}

fn command_log(output: &std::process::Output) -> String {
    let mut log = String::from_utf8_lossy(&output.stdout).into_owned();
    log.push_str(&String::from_utf8_lossy(&output.stderr));
    log
}

fn run_coral_sql(coral_bin: &Path, config_dir: &Path) -> Result<()> {
    let status = coral_command(coral_bin, config_dir)
        .args(["sql", DEFAULT_SQL])
        .stdout(Stdio::null())
        .status()
        .with_context(|| format!("running {} sql", coral_bin.display()))?;
    if !status.success() {
        bail!("coral.tables warmup query failed");
    }
    Ok(())
}

fn run_hyperfine(
    args: &Args,
    coral_bin: &Path,
    config_dir: &Path,
    result_json: &Path,
) -> Result<()> {
    let coral_bin = path_to_str(coral_bin)?;
    let warmup = args.warmup.to_string();
    let runs = args.runs.to_string();
    let result_json = path_to_str(result_json)?;
    let command = format!(
        "{} sql '{}' > /dev/null",
        shell_quote(coral_bin),
        DEFAULT_SQL
    );
    let status = Command::new("hyperfine")
        .args([
            "--warmup",
            &warmup,
            "--runs",
            &runs,
            "--export-json",
            result_json,
            "--command-name",
            "coral tables",
            &command,
        ])
        .env("CORAL_CONFIG_DIR", config_dir)
        .env("CORAL_WORKSPACE", WORKSPACE)
        .status()
        .context("running hyperfine")?;
    if !status.success() {
        bail!("hyperfine failed");
    }
    Ok(())
}

fn load_hyperfine_result(path: &Path) -> Result<HyperfineResult> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let json: Value = serde_json::from_str(&raw).context("parsing hyperfine JSON")?;
    let first = json
        .get("results")
        .and_then(Value::as_array)
        .and_then(|results| results.first())
        .context("hyperfine JSON did not contain results[0]")?;
    let mean = first
        .get("mean")
        .and_then(Value::as_f64)
        .context("hyperfine JSON did not contain results[0].mean")?;
    let stddev = first
        .get("stddev")
        .and_then(Value::as_f64)
        .context("hyperfine JSON did not contain results[0].stddev")?;
    Ok(HyperfineResult { mean, stddev })
}

fn print_tail(log: &str, max_lines: usize) {
    let lines: Vec<&str> = log.lines().collect();
    let start = lines.len().saturating_sub(max_lines);
    for line in lines.iter().skip(start) {
        println!("{line}");
    }
}

fn path_to_str(path: &Path) -> Result<&str> {
    path.to_str()
        .with_context(|| format!("path is not valid UTF-8: {}", path.display()))
}

fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    if value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-'))
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[derive(Debug)]
struct HyperfineResult {
    mean: f64,
    stddev: f64,
}

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn create(prefix: &str) -> Result<Self> {
        let base = std::env::temp_dir();
        let pid = std::process::id();
        for attempt in 0..100 {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let path = base.join(format!("{prefix}-{pid}-{nonce}-{attempt}"));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(error).with_context(|| format!("creating {}", path.display()));
                }
            }
        }
        bail!(
            "failed to allocate temporary directory under {}",
            base.display()
        )
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if let Err(_error) = fs::remove_dir_all(&self.path) {}
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::path::Path;

    use super::{
        HyperfineResult, WORKSPACE, coral_command, is_regression, report_measurement, shell_quote,
        workspace_create_command,
    };

    fn env_of(command: &std::process::Command, key: &str) -> Option<String> {
        command.get_envs().find_map(|(name, value)| {
            (name == OsStr::new(key)).then(|| {
                value
                    .expect("the check sets this variable rather than clearing it")
                    .to_string_lossy()
                    .into_owned()
            })
        })
    }

    /// Every command the check runs has to name the workspace it provisions.
    /// A fresh state directory has none, so an invocation that carries no
    /// selection reaches the server as the legacy `default` and is refused.
    #[test]
    fn every_invocation_carries_the_provisioned_workspace() {
        let command = coral_command(Path::new("/tmp/coral"), Path::new("/tmp/coral-config"));

        assert_eq!(
            env_of(&command, "CORAL_WORKSPACE").as_deref(),
            Some(WORKSPACE)
        );
        assert_eq!(
            env_of(&command, "CORAL_CONFIG_DIR").as_deref(),
            Some("/tmp/coral-config")
        );
    }

    /// The workspace the check measures is the one it creates, so the create
    /// argv and the selection every later command carries must name the same
    /// workspace.
    #[test]
    fn the_check_creates_the_workspace_it_selects() {
        let command = workspace_create_command(Path::new("/tmp/coral"), Path::new("/tmp/cfg"));

        let args: Vec<String> = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert_eq!(args, vec!["workspace", "create", WORKSPACE]);
        assert_eq!(
            env_of(&command, "CORAL_WORKSPACE").as_deref(),
            Some(WORKSPACE),
            "the workspace created and the workspace selected must be the same one"
        );
    }

    #[test]
    fn a_mean_under_the_threshold_is_not_a_regression() {
        assert!(!is_regression(0.5, 0.75));
    }

    #[test]
    fn a_mean_at_the_threshold_is_not_a_regression() {
        assert!(!is_regression(0.75, 0.75));
    }

    #[test]
    fn a_mean_above_the_threshold_is_a_regression() {
        assert!(is_regression(0.751, 0.75));
    }

    #[test]
    fn the_check_passes_only_while_the_mean_stays_within_the_threshold() {
        let within = HyperfineResult {
            mean: 0.5,
            stddev: 0.01,
        };
        let over = HyperfineResult {
            mean: 1.5,
            stddev: 0.01,
        };
        assert!(report_measurement(&within, 0.75));
        assert!(!report_measurement(&over, 0.75));
    }

    #[test]
    fn shell_quote_leaves_safe_paths_unquoted() {
        assert_eq!(shell_quote("/tmp/coral-bin/coral"), "/tmp/coral-bin/coral");
    }

    #[test]
    fn shell_quote_wraps_spaces_and_single_quotes() {
        assert_eq!(
            shell_quote("/tmp/coral bin/it'works"),
            "'/tmp/coral bin/it'\\''works'"
        );
    }
}
