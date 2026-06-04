//! Performance regression checks for user-visible Coral commands.

use std::fs;
use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::{Value, json};
use tiktoken_rs::CoreBPE;

const DEFAULT_SQL: &str = "select * from coral.tables";

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

#[derive(Debug, clap::Args)]
pub(crate) struct McpTokenBenchArgs {
    /// Path to the Coral binary to run as an MCP stdio server.
    #[arg(long, default_value = "target/debug/coral")]
    coral_bin: PathBuf,

    /// Optional config directory. Omit this to use the real live Coral config.
    #[arg(long)]
    config_dir: Option<PathBuf>,

    /// Model name used to select the tokenizer.
    #[arg(long, default_value = "gpt-5")]
    model: String,

    /// Emit JSON instead of a Markdown table.
    #[arg(long)]
    json: bool,
}

pub(crate) fn run(args: &Args) -> Result<bool> {
    validate_args(args)?;
    require_command("hyperfine")?;

    let coral_bin = absolute_path(&args.coral_bin)?;
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

    install_github_source(&coral_bin, &config_dir, &args.github_token)?;
    run_coral_sql(&coral_bin, &config_dir)?;

    let result_json = temp_dir.path().join("hyperfine.json");
    run_hyperfine(args, &coral_bin, &config_dir, &result_json)?;

    let result = load_hyperfine_result(&result_json)?;
    println!(
        "coral.tables mean: {:.3}s (stddev {:.3}s, threshold {:.3}s)",
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

pub(crate) fn run_mcp_token_bench(args: &McpTokenBenchArgs) -> Result<bool> {
    let coral_bin = absolute_path(&args.coral_bin)?;
    ensure_executable(&coral_bin)?;
    let tokenizer = tiktoken_rs::bpe_for_model(&args.model)
        .with_context(|| format!("loading tokenizer for model {}", args.model))?;
    let mut client = McpStdioClient::start(&coral_bin, args.config_dir.as_deref())?;
    client.initialize()?;

    let rows: Vec<McpTokenBenchRow> = default_mcp_token_cases()
        .iter()
        .map(|case| measure_mcp_token_case(&mut client, tokenizer, case))
        .collect::<Result<_>>()?;

    let report = McpTokenBenchReport {
        model: args.model.clone(),
        coral_bin: coral_bin.display().to_string(),
        config_dir: args.config_dir.as_ref().map_or_else(
            || "default live Coral config".to_string(),
            |path| path.display().to_string(),
        ),
        rows,
    };
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).context("serializing token bench report")?
        );
    } else {
        print_mcp_token_report(&report);
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

fn measure_mcp_token_case(
    client: &mut McpStdioClient,
    tokenizer: &CoreBPE,
    case: &McpTokenCase,
) -> Result<McpTokenBenchRow> {
    let response = match &case.request {
        McpTokenRequest::Tool { name, arguments } => client.call_tool(name, arguments)?,
        McpTokenRequest::Request { method, params, .. } => client.request(method, params)?,
    };
    if let Some(error) = response.get("error") {
        return Ok(McpTokenBenchRow {
            case: case.name.to_string(),
            surface: case.request.surface().to_string(),
            model_surface_tokens: None,
            model_surface_chars: None,
            protocol_result_tokens: None,
            protocol_result_chars: None,
            is_error: None,
            error: Some(error.to_string()),
        });
    }
    let result = response
        .get("result")
        .with_context(|| format!("MCP {} response missing result", case.request.surface()))?;
    let model_surface = case.request.model_surface(result)?;
    let protocol_result = serde_json::to_string(result).context("serializing MCP result")?;
    Ok(McpTokenBenchRow {
        case: case.name.to_string(),
        surface: case.request.surface().to_string(),
        model_surface_tokens: Some(tokenizer.encode_with_special_tokens(&model_surface).len()),
        model_surface_chars: Some(model_surface.chars().count()),
        protocol_result_tokens: Some(tokenizer.encode_with_special_tokens(&protocol_result).len()),
        protocol_result_chars: Some(protocol_result.chars().count()),
        is_error: Some(
            result
                .get("isError")
                .or_else(|| result.get("is_error"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
        ),
        error: None,
    })
}

fn default_mcp_token_cases() -> Vec<McpTokenCase> {
    let mut cases = mcp_context_token_cases();
    cases.extend(discovery_mcp_token_cases());
    cases.extend(sql_mcp_token_cases());
    cases
}

fn mcp_context_token_cases() -> Vec<McpTokenCase> {
    vec![
        McpTokenCase {
            name: "context.initialize",
            request: McpTokenRequest::Request {
                method: "initialize",
                params: json!({
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "coral-mcp-token-bench-context",
                        "version": "0.0.0"
                    }
                }),
                surface: McpModelSurface::Result,
            },
        },
        McpTokenCase {
            name: "context.tools_list",
            request: McpTokenRequest::Request {
                method: "tools/list",
                params: json!({}),
                surface: McpModelSurface::Result,
            },
        },
        McpTokenCase {
            name: "context.resources_list",
            request: McpTokenRequest::Request {
                method: "resources/list",
                params: json!({}),
                surface: McpModelSurface::Result,
            },
        },
        McpTokenCase {
            name: "context.resource_guide",
            request: McpTokenRequest::Request {
                method: "resources/read",
                params: json!({
                    "uri": "coral://guide"
                }),
                surface: McpModelSurface::ResourceText,
            },
        },
        McpTokenCase {
            name: "context.resource_tables",
            request: McpTokenRequest::Request {
                method: "resources/read",
                params: json!({
                    "uri": "coral://tables"
                }),
                surface: McpModelSurface::ResourceText,
            },
        },
    ]
}

fn discovery_mcp_token_cases() -> Vec<McpTokenCase> {
    vec![
        McpTokenCase {
            name: "discovery.search.github_pull",
            request: tool_case(
                "search_catalog",
                json!({
                    "pattern": "github|pull|request|pr",
                    "limit": 20
                }),
            ),
        },
        McpTokenCase {
            name: "discovery.search.slack_messages",
            request: tool_case(
                "search_catalog",
                json!({
                    "pattern": "slack|message|channel",
                    "limit": 20
                }),
            ),
        },
        McpTokenCase {
            name: "discovery.search.table_functions",
            request: tool_case(
                "search_catalog",
                json!({
                    "pattern": "logs|events|search",
                    "kind": "table_function",
                    "limit": 20
                }),
            ),
        },
        McpTokenCase {
            name: "discovery.list.github_tables",
            request: tool_case(
                "list_catalog",
                json!({
                    "schema": "github",
                    "kind": "table",
                    "limit": 20
                }),
            ),
        },
        McpTokenCase {
            name: "discovery.list.table_functions",
            request: tool_case(
                "list_catalog",
                json!({
                    "kind": "table_function",
                    "limit": 20
                }),
            ),
        },
        McpTokenCase {
            name: "discovery.columns.github_pulls",
            request: tool_case(
                "list_columns",
                json!({
                    "schema": "github",
                    "table": "pulls",
                    "limit": 50
                }),
            ),
        },
        McpTokenCase {
            name: "discovery.columns.github_pulls_required",
            request: tool_case(
                "list_columns",
                json!({
                    "schema": "github",
                    "table": "pulls",
                    "required_only": true,
                    "limit": 50
                }),
            ),
        },
        McpTokenCase {
            name: "discovery.columns.github_pulls_search",
            request: tool_case(
                "list_columns",
                json!({
                    "schema": "github",
                    "table": "pulls",
                    "pattern": "body|title|state|user",
                    "limit": 20
                }),
            ),
        },
        McpTokenCase {
            name: "discovery.describe.github_pulls",
            request: tool_case(
                "describe_table",
                json!({
                    "schema": "github",
                    "table": "pulls"
                }),
            ),
        },
    ]
}

fn sql_mcp_token_cases() -> Vec<McpTokenCase> {
    vec![
        McpTokenCase {
            name: "sql.wide_coral_tables",
            request: tool_case(
                "sql",
                json!({
                    "sql": "SELECT * FROM coral.tables LIMIT 20"
                }),
            ),
        },
        McpTokenCase {
            name: "sql.narrow_many_tables",
            request: tool_case(
                "sql",
                json!({
                    "sql": "SELECT schema_name, table_name FROM coral.tables LIMIT 50"
                }),
            ),
        },
        McpTokenCase {
            name: "sql.long_catalog_text",
            request: tool_case(
                "sql",
                json!({
                    "sql": "SELECT schema_name, table_name, description, guide FROM coral.tables WHERE schema_name IN ('github','slack') LIMIT 20"
                }),
            ),
        },
        McpTokenCase {
            name: "sql.github_pulls_columns",
            request: tool_case(
                "sql",
                json!({
                    "sql": "SELECT schema_name, table_name, column_name, data_type, description FROM coral.columns WHERE schema_name = 'github' AND table_name = 'pulls' LIMIT 100"
                }),
            ),
        },
        McpTokenCase {
            name: "sql.empty_result",
            request: tool_case(
                "sql",
                json!({
                    "sql": "SELECT schema_name, table_name FROM coral.tables WHERE schema_name = 'definitely_nope' LIMIT 20"
                }),
            ),
        },
        McpTokenCase {
            name: "sql.headroom_contents",
            request: tool_case(
                "sql",
                json!({
                    "sql": "SELECT name, type, path, size FROM github.contents WHERE owner = 'chopratejas' AND repo = 'headroom' AND path = '' LIMIT 20"
                }),
            ),
        },
        McpTokenCase {
            name: "sql.invalid_statement",
            request: tool_case(
                "sql",
                json!({
                    "sql": "DELETE FROM coral.tables"
                }),
            ),
        },
    ]
}

fn print_mcp_token_report(report: &McpTokenBenchReport) {
    println!("model: {}", report.model);
    println!("coral_bin: {}", report.coral_bin);
    println!("config_dir: {}", report.config_dir);
    println!(
        "| case | surface | is error | model surface tokens | model surface chars | protocol result tokens | protocol result chars |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|");
    for row in &report.rows {
        match row.model_surface_tokens {
            Some(model_surface_tokens) => println!(
                "| {} | {} | {} | {} | {} | {} | {} |",
                row.case,
                row.surface,
                row.is_error.unwrap_or(false),
                model_surface_tokens,
                row.model_surface_chars.unwrap_or_default(),
                row.protocol_result_tokens.unwrap_or_default(),
                row.protocol_result_chars.unwrap_or_default()
            ),
            None => println!("| {} | {} | | ERROR | | | |", row.case, row.surface),
        }
    }
    let total_model_surface_tokens: usize = report
        .rows
        .iter()
        .filter_map(|row| row.model_surface_tokens)
        .sum();
    let total_protocol_result_tokens: usize = report
        .rows
        .iter()
        .filter_map(|row| row.protocol_result_tokens)
        .sum();
    println!();
    println!("total_model_surface_tokens: {total_model_surface_tokens}");
    println!("total_protocol_result_tokens: {total_protocol_result_tokens}");
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

fn install_github_source(coral_bin: &Path, config_dir: &Path, github_token: &str) -> Result<()> {
    let output = Command::new(coral_bin)
        .args(["source", "add", "github"])
        .env("CORAL_CONFIG_DIR", config_dir)
        .env("GITHUB_TOKEN", github_token)
        .output()
        .with_context(|| format!("running {} source add github", coral_bin.display()))?;

    let mut log = String::from_utf8_lossy(&output.stdout).into_owned();
    log.push_str(&String::from_utf8_lossy(&output.stderr));

    if !output.status.success() {
        print!("{log}");
        bail!("failed to install github source with fake credentials");
    }

    println!("Installed github source with fake credentials.");
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

struct McpTokenCase {
    name: &'static str,
    request: McpTokenRequest,
}

enum McpTokenRequest {
    Tool {
        name: &'static str,
        arguments: Value,
    },
    Request {
        method: &'static str,
        params: Value,
        surface: McpModelSurface,
    },
}

#[derive(Clone, Copy)]
enum McpModelSurface {
    Result,
    ResourceText,
}

fn tool_case(name: &'static str, arguments: Value) -> McpTokenRequest {
    McpTokenRequest::Tool { name, arguments }
}

impl McpTokenRequest {
    fn surface(&self) -> &str {
        match self {
            Self::Tool { name, .. } => name,
            Self::Request { method, .. } => method,
        }
    }

    fn model_surface(&self, result: &Value) -> Result<String> {
        match self {
            Self::Tool { .. } => Ok(model_text_from_tool_result(result)),
            Self::Request { surface, .. } => match surface {
                McpModelSurface::Result => {
                    serde_json::to_string(result).context("serializing MCP result")
                }
                McpModelSurface::ResourceText => Ok(model_text_from_resource_result(result)),
            },
        }
    }
}

fn model_text_from_tool_result(result: &Value) -> String {
    result
        .get("content")
        .and_then(Value::as_array)
        .and_then(|content| content.first())
        .and_then(|content| content.get("text"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn model_text_from_resource_result(result: &Value) -> String {
    result
        .get("contents")
        .and_then(Value::as_array)
        .and_then(|contents| contents.first())
        .and_then(|content| content.get("text"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

#[derive(Debug, Serialize)]
struct McpTokenBenchReport {
    model: String,
    coral_bin: String,
    config_dir: String,
    rows: Vec<McpTokenBenchRow>,
}

#[derive(Debug, Serialize)]
struct McpTokenBenchRow {
    case: String,
    surface: String,
    model_surface_tokens: Option<usize>,
    model_surface_chars: Option<usize>,
    protocol_result_tokens: Option<usize>,
    protocol_result_chars: Option<usize>,
    is_error: Option<bool>,
    error: Option<String>,
}

struct McpStdioClient {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl McpStdioClient {
    fn start(coral_bin: &Path, config_dir: Option<&Path>) -> Result<Self> {
        let mut command = Command::new(coral_bin);
        command
            .arg("mcp-stdio")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(config_dir) = config_dir {
            command.env("CORAL_CONFIG_DIR", config_dir);
        }
        let mut child = command
            .spawn()
            .with_context(|| format!("starting {} mcp-stdio", coral_bin.display()))?;
        let stdin = child.stdin.take().context("MCP child stdin missing")?;
        let stdout = child.stdout.take().context("MCP child stdout missing")?;
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            next_id: 1,
        })
    }

    fn initialize(&mut self) -> Result<()> {
        self.request(
            "initialize",
            &json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {
                    "name": "coral-mcp-token-bench",
                    "version": "0.0.0"
                }
            }),
        )?;
        self.notification("notifications/initialized", &json!({}))?;
        Ok(())
    }

    fn call_tool(&mut self, name: &str, arguments: &Value) -> Result<Value> {
        self.request(
            "tools/call",
            &json!({
                "name": name,
                "arguments": arguments
            }),
        )
    }

    fn request(&mut self, method: &str, params: &Value) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        self.write_message(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        }))?;
        self.read_response(id)
    }

    fn notification(&mut self, method: &str, params: &Value) -> Result<()> {
        self.write_message(&json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        }))
    }

    fn write_message(&mut self, message: &Value) -> Result<()> {
        serde_json::to_writer(&mut self.stdin, message).context("writing MCP JSON-RPC message")?;
        writeln!(self.stdin).context("terminating MCP JSON-RPC message")?;
        self.stdin.flush().context("flushing MCP stdin")
    }

    fn read_response(&mut self, expected_id: u64) -> Result<Value> {
        let mut line = String::new();
        loop {
            line.clear();
            let bytes = self
                .stdout
                .read_line(&mut line)
                .context("reading MCP stdout")?;
            if bytes == 0 {
                bail!("MCP server exited before response id {expected_id}");
            }
            let message: Value =
                serde_json::from_str(line.trim()).context("parsing MCP JSON-RPC response")?;
            if message.get("id").and_then(Value::as_u64) == Some(expected_id) {
                return Ok(message);
            }
        }
    }
}

impl Drop for McpStdioClient {
    fn drop(&mut self) {
        if let Err(_error) = self.child.kill() {}
        if let Err(_error) = self.child.wait() {}
    }
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
