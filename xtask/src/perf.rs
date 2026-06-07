//! Performance regression checks for user-visible Coral commands.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde_json::Value;

const DEFAULT_SQL: &str = "select * from perf_messages.read_files";

#[derive(Debug, clap::Args)]
pub(crate) struct Args {
    /// Path to the release Coral binary to benchmark.
    ///
    /// Defaults to Cargo's active target directory, so environments with a
    /// configured `target-dir` benchmark the binary that `cargo build` wrote.
    #[arg(long)]
    coral_bin: Option<PathBuf>,

    /// Fail when hyperfine reports a mean above this many seconds.
    #[arg(long, default_value_t = 0.75)]
    max_mean_seconds: f64,

    /// Number of measured hyperfine runs.
    #[arg(long, default_value_t = 5)]
    runs: u32,

    /// Number of hyperfine warmup runs.
    #[arg(long, default_value_t = 1)]
    warmup: u32,
}

pub(crate) fn run(args: &Args) -> Result<bool> {
    validate_args(args)?;
    require_command("hyperfine")?;

    let coral_bin = match &args.coral_bin {
        Some(path) => absolute_path(path)?,
        None => default_release_coral_bin()?,
    };
    ensure_executable(&coral_bin)?;

    let temp_dir = TempDir::create("coral-tables-perf")?;
    let config_dir = temp_dir.path().join("coral-config");
    fs::create_dir_all(&config_dir)
        .with_context(|| format!("creating {}", config_dir.display()))?;
    fs::write(
        config_dir.join("config.toml"),
        "[credentials]\nstorage = \"file\"\n",
    )
    .with_context(|| format!("writing {}", config_dir.join("config.toml").display()))?;

    install_perf_source(&coral_bin, &config_dir, temp_dir.path())?;
    run_coral_sql(&coral_bin, &config_dir)?;

    let result_json = temp_dir.path().join("hyperfine.json");
    run_hyperfine(args, &coral_bin, &config_dir, &result_json)?;

    let result = load_hyperfine_result(&result_json)?;
    println!(
        "perf SourceSpec table mean: {:.3}s (stddev {:.3}s, threshold {:.3}s)",
        result.mean, result.stddev, args.max_mean_seconds
    );
    if result.mean > args.max_mean_seconds {
        eprintln!(
            "Performance regression: mean {:.3}s exceeds {:.3}s",
            result.mean, args.max_mean_seconds
        );
        return Ok(false);
    }

    Ok(true)
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
        .with_context(|| format!("{command} is required for the SQL performance check"))?;
    if !status.success() {
        bail!("{command} is required for the SQL performance check");
    }
    Ok(())
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()
        .context("resolving current directory")?
        .join(path))
}

fn default_release_coral_bin() -> Result<PathBuf> {
    let output = Command::new("cargo")
        .args(["metadata", "--format-version=1", "--no-deps", "--locked"])
        .output()
        .context("running cargo metadata to locate target directory")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("cargo metadata failed while locating target directory: {stderr}");
    }

    let metadata: Value =
        serde_json::from_slice(&output.stdout).context("parsing cargo metadata JSON")?;
    let target_dir = metadata
        .get("target_directory")
        .and_then(Value::as_str)
        .context("cargo metadata JSON did not contain target_directory")?;

    Ok(PathBuf::from(target_dir)
        .join("release")
        .join(format!("coral{}", std::env::consts::EXE_SUFFIX)))
}

fn ensure_executable(path: &Path) -> Result<()> {
    let metadata = fs::metadata(path).with_context(|| format!("reading {}", path.display()))?;
    if !metadata.is_file() {
        bail!("Coral binary is not a file: {}", path.display());
    }
    Ok(())
}

fn install_perf_source(coral_bin: &Path, config_dir: &Path, temp_dir: &Path) -> Result<()> {
    let source_dir = temp_dir.join("perf-source");
    fs::create_dir_all(&source_dir)
        .with_context(|| format!("creating {}", source_dir.display()))?;
    fs::write(
        source_dir.join("messages.jsonl"),
        "{\"id\":1,\"text\":\"hello\"}\n{\"id\":2,\"text\":\"world\"}\n",
    )
    .with_context(|| format!("writing {}", source_dir.join("messages.jsonl").display()))?;
    let manifest = source_dir.join("manifest.yaml");
    fs::write(
        &manifest,
        r"
spec_version: 1
kind: source
name: perf_messages
interfaces:
  - id: read_files
    type: file
    files:
      - ./messages.jsonl
    format:
      kind: jsonl
",
    )
    .with_context(|| format!("writing {}", manifest.display()))?;

    let output = Command::new(coral_bin)
        .args(["source", "add", "--file", path_to_str(&manifest)?])
        .env("CORAL_CONFIG_DIR", config_dir)
        .output()
        .with_context(|| format!("running {} source add --file", coral_bin.display()))?;

    let mut log = String::from_utf8_lossy(&output.stdout).into_owned();
    log.push_str(&String::from_utf8_lossy(&output.stderr));

    if !output.status.success() {
        print!("{log}");
        bail!("failed to install perf SourceSpec fixture");
    }

    println!("Installed perf SourceSpec fixture.");
    print_tail(&log, 20);
    Ok(())
}

fn run_coral_sql(coral_bin: &Path, config_dir: &Path) -> Result<()> {
    let status = Command::new(coral_bin)
        .args(["sql", DEFAULT_SQL])
        .env("CORAL_CONFIG_DIR", config_dir)
        .stdout(Stdio::null())
        .status()
        .with_context(|| format!("running {} sql", coral_bin.display()))?;
    if !status.success() {
        bail!("perf SQL warmup query failed");
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
            "coral perf source table",
            &command,
        ])
        .env("CORAL_CONFIG_DIR", config_dir)
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
    use super::shell_quote;

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
