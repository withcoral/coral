//! Universal Search relevance corpus construction and deterministic replay.
//!
//! Real catalog data and run artifacts remain outside this crate's tracked
//! fixtures. Only explicitly synthetic benchmark inputs belong in Git.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

const FORMAT_VERSION: u32 = 1;
const REPLAY_FORMAT_VERSION: u32 = 3;
const RESPONSE_TOKEN_ENCODING: &str = "o200k_base";
const DEFAULT_TIMEOUT_SECONDS: u64 = 120;
const DEFAULT_GENERATOR_BATCH_SIZE: usize = 10;
const QUESTIONS_PER_SAMPLE: usize = 3;
const COLLECTION_PROTOCOL_VERSION: &str = "coral-search-once-v1";
const COLLECTION_PROMPT_PREFIX: &str = "Find the Coral catalog surface that would help answer the question below.\nCall coral.search exactly once with limit 10. Choose the search query yourself.\nDo not call any other tool and do not execute SQL. Reply DONE after the search.\n\nQuestion: ";
const COLLECTION_PROMPT_SUFFIX: &str = "\n";

const TABLES_QUERY: &str = "SELECT schema_name, table_name, description, guide, required_filters FROM coral.tables WHERE schema_name <> 'coral' ORDER BY schema_name, table_name";
const COLUMNS_QUERY: &str = "SELECT schema_name, table_name, ordinal_position, column_name, data_type, is_nullable, is_virtual, is_required_filter, description FROM coral.columns WHERE schema_name <> 'coral' ORDER BY schema_name, table_name, ordinal_position, column_name";
const FUNCTIONS_QUERY: &str = "SELECT schema_name, function_name, description, arguments_json, result_columns_json, kind, search_limits_json FROM coral.table_functions WHERE schema_name <> 'coral' ORDER BY schema_name, function_name";

#[derive(Debug, clap::Args)]
pub(crate) struct Args {
    #[command(subcommand)]
    command: SearchBenchCommand,
}

#[derive(Debug, clap::Subcommand)]
enum SearchBenchCommand {
    /// Snapshot the live catalog and create seeded hierarchical samples.
    Prepare(PrepareArgs),
    /// Generate three questions per sample with fresh Codex processes.
    Generate(GenerateArgs),
    /// Capture one fresh Codex-to-Coral search call per question.
    Collect(CollectArgs),
    /// Replay frozen queries against a selected Coral binary at limits 10 and 50.
    Replay(ReplayArgs),
    /// Summarize replay ranks as JSON and Markdown.
    Report(ReportArgs),
}

#[derive(Debug, clap::Args)]
struct PrepareArgs {
    /// Run directory for inventory, samples, corpus drafts, and results.
    #[arg(long)]
    dir: PathBuf,
    /// Coral binary whose live catalog should be sampled.
    #[arg(long, default_value = "target/release/coral")]
    coral_bin: PathBuf,
    /// Coral workspace to sample.
    #[arg(long, default_value = "default")]
    workspace: String,
    /// Optional Coral configuration directory.
    #[arg(long)]
    coral_config_dir: Option<PathBuf>,
    /// Number of hierarchical catalog targets to sample.
    #[arg(long, default_value_t = 100)]
    count: usize,
    /// Stable sampler seed.
    #[arg(long)]
    seed: u64,
}

#[derive(Debug, clap::Args)]
struct GenerateArgs {
    /// Run directory created by search-bench prepare.
    #[arg(long)]
    dir: PathBuf,
    /// Codex CLI binary.
    #[arg(long, default_value = "codex")]
    codex_bin: PathBuf,
    /// Fixed model used by every generator process.
    #[arg(long)]
    model: String,
    /// Maximum concurrent generator processes.
    #[arg(long, default_value_t = 10)]
    jobs: usize,
    /// Samples given to each fresh generator process.
    #[arg(long, default_value_t = DEFAULT_GENERATOR_BATCH_SIZE)]
    batch_size: usize,
    /// Per-process timeout in seconds.
    #[arg(long, default_value_t = DEFAULT_TIMEOUT_SECONDS)]
    timeout_seconds: u64,
}

#[derive(Debug, clap::Args)]
struct CollectArgs {
    /// Run directory containing corpus.jsonl.
    #[arg(long)]
    dir: PathBuf,
    /// Coral binary exposed to each isolated Codex process.
    #[arg(long, default_value = "target/release/coral")]
    coral_bin: PathBuf,
    /// Codex CLI binary.
    #[arg(long, default_value = "codex")]
    codex_bin: PathBuf,
    /// Fixed model used by every collection process.
    #[arg(long)]
    model: String,
    /// Coral workspace used by the MCP server.
    #[arg(long, default_value = "default")]
    workspace: String,
    /// Optional Coral configuration directory inherited by the MCP server.
    #[arg(long)]
    coral_config_dir: Option<PathBuf>,
    /// Maximum concurrent isolated Codex processes.
    #[arg(long, default_value_t = 10)]
    jobs: usize,
    /// Per-process timeout in seconds.
    #[arg(long, default_value_t = DEFAULT_TIMEOUT_SECONDS)]
    timeout_seconds: u64,
    /// Preserve successful existing trials and rerun only non-ok or missing cases.
    #[arg(long)]
    retry_failed: bool,
}

#[derive(Debug, clap::Args)]
struct ReplayArgs {
    /// Run directory containing collected-corpus.jsonl.
    #[arg(long)]
    dir: PathBuf,
    /// Optional focused corpus to replay instead of dir/collected-corpus.jsonl.
    #[arg(long)]
    corpus: Option<PathBuf>,
    /// Immutable identifier for this replay candidate.
    #[arg(long)]
    label: String,
    /// Coral binary to evaluate.
    #[arg(long, default_value = "target/release/coral")]
    coral_bin: PathBuf,
    /// Coral workspace used for deterministic replay.
    #[arg(long, default_value = "default")]
    workspace: String,
    /// Optional Coral configuration directory.
    #[arg(long)]
    coral_config_dir: Option<PathBuf>,
    /// Include values observed during earlier queries in search results.
    #[arg(long)]
    enable_observed_values_search: bool,
    /// Maximum concurrent replay cases. Each case performs limits 10 and 50.
    #[arg(long, default_value_t = 10)]
    jobs: usize,
    /// Per-search timeout in seconds.
    #[arg(long, default_value_t = DEFAULT_TIMEOUT_SECONDS)]
    timeout_seconds: u64,
}

#[derive(Debug, clap::Args)]
struct ReportArgs {
    /// Run directory containing replay.jsonl.
    #[arg(long)]
    dir: PathBuf,
    /// Immutable replay identifier to report.
    #[arg(long)]
    label: String,
    /// Optional earlier replay to compare case by case.
    #[arg(long)]
    baseline_label: Option<String>,
}

pub(crate) fn run(args: &Args) -> Result<bool> {
    match &args.command {
        SearchBenchCommand::Prepare(args) => prepare(args),
        SearchBenchCommand::Generate(args) => generate(args),
        SearchBenchCommand::Collect(args) => collect(args),
        SearchBenchCommand::Replay(args) => replay(args),
        SearchBenchCommand::Report(args) => report(args),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Inventory {
    tables: Vec<TableRow>,
    columns: Vec<ColumnRow>,
    functions: Vec<FunctionRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TableRow {
    schema_name: String,
    table_name: String,
    description: String,
    guide: String,
    required_filters: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ColumnRow {
    schema_name: String,
    table_name: String,
    ordinal_position: i64,
    column_name: String,
    data_type: String,
    is_nullable: bool,
    is_virtual: bool,
    is_required_filter: bool,
    description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FunctionRow {
    schema_name: String,
    function_name: String,
    description: String,
    arguments_json: String,
    result_columns_json: String,
    kind: String,
    search_limits_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FunctionArgument {
    name: String,
    required: bool,
    values: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FunctionResultColumn {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
    nullable: bool,
    description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Target {
    schema_name: String,
    surface_kind: String,
    surface_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    field_role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    field_name: Option<String>,
}

impl Target {
    fn is_child(&self) -> bool {
        self.field_name.is_some()
    }

    fn validate_shape(&self) -> Result<()> {
        if self.field_name.is_some() != self.field_role.is_some() {
            bail!("field_name and field_role must either both be present or both be absent");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SampleMetadata {
    description: String,
    guide: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    required: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Sample {
    format_version: u32,
    #[serde(rename = "sample_id")]
    id: String,
    target: Target,
    metadata: SampleMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CorpusCase {
    format_version: u32,
    case_id: String,
    sample_id: String,
    style: String,
    question: String,
    rationale: String,
    target: Target,
    #[serde(skip_serializing_if = "Option::is_none")]
    frozen_query: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GeneratedBatch {
    questions: Vec<GeneratedQuestion>,
}

#[derive(Debug, Deserialize)]
struct GeneratedQuestion {
    sample_id: String,
    style: String,
    question: String,
    rationale: String,
}

fn prepare(args: &PrepareArgs) -> Result<bool> {
    if args.count == 0 {
        bail!("--count must be positive");
    }
    let coral_bin = absolute_path(&args.coral_bin)?;
    ensure_file(&coral_bin, "Coral binary")?;

    let inventory = catalog_inventory(
        &coral_bin,
        &args.workspace,
        args.coral_config_dir.as_deref(),
    )?;
    validate_inventory(&inventory)?;
    let inventory_bytes = inventory_bytes(&inventory)?;
    let inventory_hash = sha256_hex(&inventory_bytes);
    let samples = sample_inventory(&inventory, args.count, args.seed)?;
    let coral_version = command_version(&coral_bin)?;
    let manifest = json!({
        "format_version": FORMAT_VERSION,
        "created_unix_seconds": unix_seconds()?,
        "workspace": args.workspace,
        "seed": args.seed,
        "sample_count": samples.len(),
        "inventory_sha256": inventory_hash,
        "inventory_counts": {
            "tables": inventory.tables.len(),
            "columns": inventory.columns.len(),
            "table_functions": inventory.functions.len()
        },
        "coral_version": coral_version,
        "prepare": {
            "coral_bin": coral_bin,
            "queries": [TABLES_QUERY, COLUMNS_QUERY, FUNCTIONS_QUERY]
        }
    });
    claim_run_dir(&args.dir)?;
    atomic_write(&args.dir.join("inventory.json"), &inventory_bytes)?;
    write_jsonl(&args.dir.join("samples.jsonl"), &samples)?;
    atomic_write_json(&args.dir.join("manifest.json"), &manifest)?;
    println!(
        "Prepared {} samples from {} tables, {} columns, and {} table functions in {}.",
        samples.len(),
        inventory.tables.len(),
        inventory.columns.len(),
        inventory.functions.len(),
        args.dir.display()
    );
    Ok(true)
}

fn catalog_inventory(
    coral_bin: &Path,
    workspace: &str,
    config_dir: Option<&Path>,
) -> Result<Inventory> {
    Ok(Inventory {
        tables: coral_sql_json(coral_bin, workspace, config_dir, TABLES_QUERY)?,
        columns: coral_sql_json(coral_bin, workspace, config_dir, COLUMNS_QUERY)?,
        functions: coral_sql_json(coral_bin, workspace, config_dir, FUNCTIONS_QUERY)?,
    })
}

fn inventory_bytes(inventory: &Inventory) -> Result<Vec<u8>> {
    serde_json::to_vec_pretty(inventory).context("serializing inventory")
}

fn coral_sql_json<T: DeserializeOwned>(
    coral_bin: &Path,
    workspace: &str,
    config_dir: Option<&Path>,
    sql: &str,
) -> Result<Vec<T>> {
    let mut command = Command::new(coral_bin);
    command.args(["--workspace", workspace, "sql", "--format", "json", sql]);
    set_config_dir(&mut command, config_dir)?;
    let output = command
        .output()
        .with_context(|| format!("running {} sql", coral_bin.display()))?;
    if !output.status.success() {
        bail!(
            "Coral catalog query failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    serde_json::from_slice(&output.stdout).context("parsing Coral catalog JSON")
}

fn validate_inventory(inventory: &Inventory) -> Result<()> {
    if inventory.tables.is_empty() && inventory.functions.is_empty() {
        bail!("live catalog contains no tables or table functions");
    }
    if inventory
        .tables
        .iter()
        .map(|table| table.schema_name.as_str())
        .chain(
            inventory
                .columns
                .iter()
                .map(|column| column.schema_name.as_str()),
        )
        .chain(
            inventory
                .functions
                .iter()
                .map(|function| function.schema_name.as_str()),
        )
        .any(|schema_name| schema_name == "coral")
    {
        bail!("benchmark inventory must not contain coral system surfaces");
    }
    for function in &inventory.functions {
        serde_json::from_str::<Vec<FunctionArgument>>(&function.arguments_json).with_context(
            || {
                format!(
                    "parsing arguments for {}.{}",
                    function.schema_name, function.function_name
                )
            },
        )?;
        serde_json::from_str::<Vec<FunctionResultColumn>>(&function.result_columns_json)
            .with_context(|| {
                format!(
                    "parsing result columns for {}.{}",
                    function.schema_name, function.function_name
                )
            })?;
    }
    Ok(())
}

fn sample_inventory(inventory: &Inventory, count: usize, seed: u64) -> Result<Vec<Sample>> {
    let mut tables_by_schema = BTreeMap::<String, Vec<&TableRow>>::new();
    let mut functions_by_schema = BTreeMap::<String, Vec<&FunctionRow>>::new();
    let mut columns_by_table = BTreeMap::<(String, String), Vec<&ColumnRow>>::new();
    for table in &inventory.tables {
        tables_by_schema
            .entry(table.schema_name.clone())
            .or_default()
            .push(table);
    }
    for function in &inventory.functions {
        functions_by_schema
            .entry(function.schema_name.clone())
            .or_default()
            .push(function);
    }
    for column in &inventory.columns {
        columns_by_table
            .entry((column.schema_name.clone(), column.table_name.clone()))
            .or_default()
            .push(column);
    }

    let schemas = tables_by_schema
        .keys()
        .chain(functions_by_schema.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if schemas.is_empty() {
        bail!("cannot sample an empty catalog");
    }

    let mut rng = SplitMix64::new(seed);
    let mut schema_schedule = Vec::with_capacity(count);
    while schema_schedule.len() < count {
        let mut cycle = schemas.clone();
        shuffle(&mut cycle, &mut rng);
        let remaining = count.saturating_sub(schema_schedule.len());
        schema_schedule.extend(cycle.into_iter().take(remaining));
    }

    let mut samples = Vec::with_capacity(count);
    for (index, schema) in schema_schedule.into_iter().enumerate() {
        let tables: &[&TableRow] = tables_by_schema.get(&schema).map_or(&[], Vec::as_slice);
        let functions: &[&FunctionRow] =
            functions_by_schema.get(&schema).map_or(&[], Vec::as_slice);
        let choose_function = match (tables.is_empty(), functions.is_empty()) {
            (false, false) => rng.index(2) == 1,
            (true, false) => true,
            (false, true) => false,
            (true, true) => bail!("schema {schema} has no sampleable surfaces"),
        };
        let (target, metadata) = if choose_function {
            let function = choose(functions, &mut rng).context("choosing table function")?;
            sample_function(function, &mut rng)?
        } else {
            let table = choose(tables, &mut rng).context("choosing table")?;
            sample_table(table, &columns_by_table, &mut rng)?
        };
        samples.push(Sample {
            format_version: FORMAT_VERSION,
            id: format!("s{:04}", index + 1),
            target,
            metadata,
        });
    }
    Ok(samples)
}

fn sample_table(
    table: &TableRow,
    columns_by_table: &BTreeMap<(String, String), Vec<&ColumnRow>>,
    rng: &mut SplitMix64,
) -> Result<(Target, SampleMetadata)> {
    let parent_target = Target {
        schema_name: table.schema_name.clone(),
        surface_kind: "table".to_string(),
        surface_name: table.table_name.clone(),
        field_role: None,
        field_name: None,
    };
    let parent_metadata = SampleMetadata {
        description: table.description.clone(),
        guide: table.guide.clone(),
        data_type: None,
        required: None,
    };
    if rng.index(2) == 0 {
        return Ok((parent_target, parent_metadata));
    }

    let columns: &[&ColumnRow] = columns_by_table
        .get(&(table.schema_name.clone(), table.table_name.clone()))
        .map_or(&[], Vec::as_slice);
    let filters = table
        .required_filters
        .split(',')
        .map(str::trim)
        .filter(|filter| !filter.is_empty())
        .collect::<Vec<_>>();
    let role_count = usize::from(!columns.is_empty()) + usize::from(!filters.is_empty());
    if role_count == 0 {
        return Ok((parent_target, parent_metadata));
    }
    let choose_filter = !filters.is_empty() && (columns.is_empty() || rng.index(role_count) == 1);
    if choose_filter {
        let filter = choose(&filters, rng).context("choosing table filter")?;
        return Ok((
            Target {
                field_role: Some("table_filter".to_string()),
                field_name: Some((*filter).to_string()),
                ..parent_target
            },
            SampleMetadata {
                description: "Required table filter".to_string(),
                guide: table.guide.clone(),
                data_type: None,
                required: Some(true),
            },
        ));
    }
    let column = choose(columns, rng).context("choosing table column")?;
    Ok((
        Target {
            field_role: Some("table_column".to_string()),
            field_name: Some(column.column_name.clone()),
            ..parent_target
        },
        SampleMetadata {
            description: column.description.clone(),
            guide: table.guide.clone(),
            data_type: Some(column.data_type.clone()),
            required: Some(column.is_required_filter),
        },
    ))
}

fn sample_function(
    function: &FunctionRow,
    rng: &mut SplitMix64,
) -> Result<(Target, SampleMetadata)> {
    let parent_target = Target {
        schema_name: function.schema_name.clone(),
        surface_kind: "table_function".to_string(),
        surface_name: function.function_name.clone(),
        field_role: None,
        field_name: None,
    };
    let parent_metadata = SampleMetadata {
        description: function.description.clone(),
        guide: String::new(),
        data_type: None,
        required: None,
    };
    if rng.index(2) == 0 {
        return Ok((parent_target, parent_metadata));
    }

    let arguments = serde_json::from_str::<Vec<FunctionArgument>>(&function.arguments_json)
        .context("parsing sampled function arguments")?;
    let columns = serde_json::from_str::<Vec<FunctionResultColumn>>(&function.result_columns_json)
        .context("parsing sampled function result columns")?;
    let role_count = usize::from(!arguments.is_empty()) + usize::from(!columns.is_empty());
    if role_count == 0 {
        return Ok((parent_target, parent_metadata));
    }
    let choose_result = !columns.is_empty() && (arguments.is_empty() || rng.index(role_count) == 1);
    if choose_result {
        let column = choose(&columns, rng).context("choosing function result column")?;
        return Ok((
            Target {
                field_role: Some("table_function_result_column".to_string()),
                field_name: Some(column.name.clone()),
                ..parent_target
            },
            SampleMetadata {
                description: column.description.clone(),
                guide: String::new(),
                data_type: Some(column.data_type.clone()),
                required: Some(false),
            },
        ));
    }
    let argument = choose(&arguments, rng).context("choosing function argument")?;
    Ok((
        Target {
            field_role: Some("table_function_argument".to_string()),
            field_name: Some(argument.name.clone()),
            ..parent_target
        },
        SampleMetadata {
            description: "Table function argument".to_string(),
            guide: if argument.values.is_empty() {
                String::new()
            } else {
                format!("Allowed values: {}", argument.values.join(", "))
            },
            data_type: None,
            required: Some(argument.required),
        },
    ))
}

fn generate(args: &GenerateArgs) -> Result<bool> {
    validate_jobs(args.jobs)?;
    if args.batch_size == 0 {
        bail!("--batch-size must be positive");
    }
    let samples = read_jsonl::<Sample>(&args.dir.join("samples.jsonl"))?;
    if samples.is_empty() {
        bail!("samples.jsonl is empty");
    }
    ensure_command(&args.codex_bin, "Codex CLI")?;
    let schema_path = args.dir.join("generator-output-schema.json");
    atomic_write_json(&schema_path, &generator_output_schema())?;
    let batches = samples
        .chunks(args.batch_size)
        .map(<[Sample]>::to_vec)
        .collect::<Vec<_>>();
    let timeout = Duration::from_secs(args.timeout_seconds);
    let outputs = parallel_map(&batches, args.jobs, |batch_index, batch| {
        generate_batch(batch_index, batch, args, &schema_path, timeout)
    });
    let mut questions = Vec::new();
    for output in outputs {
        questions.extend(output?);
    }
    let corpus = build_corpus(&samples, questions)?;
    write_jsonl(&args.dir.join("corpus.jsonl"), &corpus)?;
    update_manifest(
        &args.dir,
        "generate",
        json!({
            "codex_version": command_version(&args.codex_bin)?,
            "model": args.model,
            "jobs": args.jobs,
            "batch_size": args.batch_size,
            "question_count": corpus.len()
        }),
    )?;
    println!(
        "Generated {} corpus questions in {}.",
        corpus.len(),
        args.dir.display()
    );
    Ok(true)
}

fn generate_batch(
    batch_index: usize,
    samples: &[Sample],
    args: &GenerateArgs,
    schema_path: &Path,
    timeout: Duration,
) -> Result<Vec<GeneratedQuestion>> {
    let batch_dir = args
        .dir
        .join("raw/generate")
        .join(format!("batch-{:03}", batch_index + 1));
    fs::create_dir_all(&batch_dir).with_context(|| format!("creating {}", batch_dir.display()))?;
    let prompt_path = batch_dir.join("prompt.txt");
    let output_path = batch_dir.join("output.json");
    let events_path = batch_dir.join("events.jsonl");
    let stderr_path = batch_dir.join("stderr.log");
    let work_dir = batch_dir.join("work");
    fs::create_dir_all(&work_dir).with_context(|| format!("creating {}", work_dir.display()))?;
    let prompt = generator_prompt(samples)?;
    atomic_write(&prompt_path, prompt.as_bytes())?;

    let mut command = Command::new(&args.codex_bin);
    command.args([
        "exec",
        "--ephemeral",
        "--json",
        "--ignore-user-config",
        "--ignore-rules",
        "--skip-git-repo-check",
        "--sandbox",
        "read-only",
        "--output-schema",
    ]);
    command.arg(schema_path);
    command.arg("--output-last-message");
    command.arg(&output_path);
    command.args(["--model", &args.model, "-C"]);
    command.arg(&work_dir);
    command.arg("-");
    let outcome = run_process(
        &mut command,
        Some(&prompt_path),
        &events_path,
        &stderr_path,
        timeout,
    )?;
    require_success(&outcome, "Codex question generator", &stderr_path)?;
    let raw = fs::read_to_string(&output_path)
        .with_context(|| format!("reading {}", output_path.display()))?;
    let generated: GeneratedBatch =
        serde_json::from_str(&raw).context("parsing generator structured output")?;
    validate_generated_batch(samples, &generated.questions)?;
    Ok(generated.questions)
}

fn generator_prompt(samples: &[Sample]) -> Result<String> {
    let samples_json = serde_json::to_string_pretty(samples).context("serializing sample batch")?;
    Ok(format!(
        "Generate exactly three realistic user questions for each catalog target below.\n\
         The target must be useful for answering each question, but do not teach the user Coral's schema, table, function, or field identifiers. Natural provider and domain language is allowed when a real user would use it.\n\
         Use these styles once per sample: natural, identifier_light, field_focused.\n\
         Return only the structured JSON required by the output schema. Preserve every sample_id exactly.\n\n\
         Targets:\n{samples_json}\n"
    ))
}

fn generator_output_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["questions"],
        "properties": {
            "questions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["sample_id", "style", "question", "rationale"],
                    "properties": {
                        "sample_id": {"type": "string"},
                        "style": {
                            "type": "string",
                            "enum": ["natural", "identifier_light", "field_focused"]
                        },
                        "question": {"type": "string", "minLength": 1},
                        "rationale": {"type": "string", "minLength": 1}
                    }
                }
            }
        }
    })
}

fn validate_generated_batch(samples: &[Sample], questions: &[GeneratedQuestion]) -> Result<()> {
    let expected = samples
        .iter()
        .map(|sample| sample.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut counts = BTreeMap::<&str, usize>::new();
    let mut styles = BTreeMap::<&str, BTreeSet<&str>>::new();
    for question in questions {
        if !expected.contains(question.sample_id.as_str()) {
            bail!(
                "generator returned unknown sample_id {}",
                question.sample_id
            );
        }
        *counts.entry(question.sample_id.as_str()).or_default() += 1;
        styles
            .entry(question.sample_id.as_str())
            .or_default()
            .insert(question.style.as_str());
    }
    for sample_id in expected {
        if counts.get(sample_id).copied() != Some(QUESTIONS_PER_SAMPLE) {
            bail!("generator must return exactly three questions for {sample_id}");
        }
        if styles.get(sample_id).map(BTreeSet::len) != Some(QUESTIONS_PER_SAMPLE) {
            bail!("generator must return three distinct styles for {sample_id}");
        }
    }
    Ok(())
}

fn build_corpus(samples: &[Sample], questions: Vec<GeneratedQuestion>) -> Result<Vec<CorpusCase>> {
    let sample_by_id = samples
        .iter()
        .map(|sample| (sample.id.as_str(), sample))
        .collect::<BTreeMap<_, _>>();
    let mut question_number = BTreeMap::<String, usize>::new();
    let mut corpus = Vec::with_capacity(questions.len());
    for question in questions {
        let sample = sample_by_id
            .get(question.sample_id.as_str())
            .with_context(|| format!("missing sample {}", question.sample_id))?;
        let number = question_number
            .entry(question.sample_id.clone())
            .or_default();
        *number += 1;
        corpus.push(CorpusCase {
            format_version: FORMAT_VERSION,
            case_id: format!("{}-q{}", question.sample_id, number),
            sample_id: question.sample_id,
            style: question.style,
            question: question.question,
            rationale: question.rationale,
            target: sample.target.clone(),
            frozen_query: None,
        });
    }
    corpus.sort_by(|left, right| left.case_id.cmp(&right.case_id));
    Ok(corpus)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Trial {
    format_version: u32,
    case_id: String,
    #[serde(default)]
    case_digest: String,
    #[serde(default)]
    collection_digest: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    agent_query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    search_response: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    evaluation: Option<RankEvaluation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    elapsed_millis: u128,
}

fn collect(args: &CollectArgs) -> Result<bool> {
    validate_jobs(args.jobs)?;
    ensure_command(&args.codex_bin, "Codex CLI")?;
    let coral_bin = absolute_path(&args.coral_bin)?;
    ensure_file(&coral_bin, "Coral binary")?;
    let corpus = read_jsonl::<CorpusCase>(&args.dir.join("corpus.jsonl"))?;
    if corpus.is_empty() {
        bail!("corpus.jsonl is empty");
    }
    validate_corpus_case_ids(&corpus)?;
    let codex_version = command_version(&args.codex_bin)?;
    let coral_version = command_version(&coral_bin)?;
    let coral_sha256 = sha256_file(&coral_bin)?;
    let prepared_inventory_sha256 = verified_inventory_sha256(
        &args.dir,
        &coral_bin,
        &args.workspace,
        args.coral_config_dir.as_deref(),
    )?;
    let collection_protocol_sha256 = collection_protocol_sha256();
    let collection_digest = collection_provenance_digest(
        args,
        &coral_bin,
        &codex_version,
        &coral_version,
        &coral_sha256,
        &prepared_inventory_sha256,
        &collection_protocol_sha256,
    )?;
    let trials_path = args.dir.join("trials.jsonl");
    let mut trials_by_id = if args.retry_failed {
        ensure_file(&trials_path, "Existing trials")?;
        read_jsonl::<Trial>(&trials_path)?
            .into_iter()
            .map(|trial| (trial.case_id.clone(), trial))
            .collect::<BTreeMap<_, _>>()
    } else {
        BTreeMap::new()
    };
    let pending = pending_collection_cases(
        &corpus,
        &trials_by_id,
        args.retry_failed,
        &collection_digest,
    )?;
    let timeout = Duration::from_secs(args.timeout_seconds);
    let outputs = parallel_map(&pending, args.jobs, |_, pending_case| {
        collect_case(
            &pending_case.0,
            &pending_case.1,
            &collection_digest,
            args,
            &coral_bin,
            timeout,
        )
    });
    for output in outputs {
        let trial = output?;
        trials_by_id.insert(trial.case_id.clone(), trial);
    }
    let trials = corpus
        .iter()
        .map(|case| {
            trials_by_id
                .remove(&case.case_id)
                .with_context(|| format!("missing trial for {}", case.case_id))
        })
        .collect::<Result<Vec<_>>>()?;
    write_jsonl(&trials_path, &trials)?;
    let collected = freeze_collected_queries(corpus, &trials);
    write_jsonl(&args.dir.join("collected-corpus.jsonl"), &collected)?;
    let successes = trials.iter().filter(|trial| trial.status == "ok").count();
    update_manifest(
        &args.dir,
        "collect",
        json!({
            "codex_version": codex_version,
            "model": args.model,
            "jobs": args.jobs,
            "retry_failed": args.retry_failed,
            "attempted_cases": pending.len(),
            "workspace": args.workspace,
            "coral_version": coral_version,
            "coral_sha256": coral_sha256,
            "collection_digest": collection_digest,
            "inventory_sha256": prepared_inventory_sha256,
            "collection_protocol_version": COLLECTION_PROTOCOL_VERSION,
            "collection_protocol_sha256": collection_protocol_sha256,
            "coral_config_dir": args.coral_config_dir,
            "case_count": trials.len(),
            "successful_cases": successes
        }),
    )?;
    println!(
        "Collected {successes}/{} successful isolated search trials in {}.",
        trials.len(),
        args.dir.display()
    );
    Ok(successes == trials.len())
}

fn collection_provenance_digest(
    args: &CollectArgs,
    coral_bin: &Path,
    codex_version: &str,
    coral_version: &str,
    coral_sha256: &str,
    inventory_sha256: &str,
    collection_protocol_sha256: &str,
) -> Result<String> {
    Ok(sha256_hex(&serde_json::to_vec(&json!({
        "codex_version": codex_version,
        "model": args.model,
        "coral_bin": coral_bin,
        "coral_version": coral_version,
        "coral_sha256": coral_sha256,
        "workspace": args.workspace,
        "coral_config_dir": args.coral_config_dir,
        "inventory_sha256": inventory_sha256,
        "collection_protocol_version": COLLECTION_PROTOCOL_VERSION,
        "collection_protocol_sha256": collection_protocol_sha256,
    }))?))
}

fn pending_collection_cases(
    corpus: &[CorpusCase],
    trials_by_id: &BTreeMap<String, Trial>,
    retry_failed: bool,
    collection_digest: &str,
) -> Result<Vec<(CorpusCase, String)>> {
    let mut pending = Vec::new();
    for case in corpus {
        let case_digest = corpus_case_digest(case)?;
        let reusable = retry_failed
            && trials_by_id.get(&case.case_id).is_some_and(|trial| {
                trial.status == "ok"
                    && trial.case_digest == case_digest
                    && trial.collection_digest == collection_digest
            });
        if !reusable {
            pending.push((case.clone(), case_digest));
        }
    }
    Ok(pending)
}

fn freeze_collected_queries(mut corpus: Vec<CorpusCase>, trials: &[Trial]) -> Vec<CorpusCase> {
    for (case, trial) in corpus.iter_mut().zip(trials) {
        case.frozen_query = (trial.status == "ok")
            .then(|| trial.agent_query.clone())
            .flatten();
    }
    corpus
}

fn collect_case(
    case: &CorpusCase,
    case_digest: &str,
    collection_digest: &str,
    args: &CollectArgs,
    coral_bin: &Path,
    timeout: Duration,
) -> Result<Trial> {
    let case_dir = next_attempt_dir(&args.dir.join("raw/collect").join(&case.case_id));
    fs::create_dir_all(&case_dir).with_context(|| format!("creating {}", case_dir.display()))?;
    let prompt_path = case_dir.join("prompt.txt");
    let events_path = case_dir.join("events.jsonl");
    let stderr_path = case_dir.join("stderr.log");
    let work_dir = case_dir.join("work");
    fs::create_dir_all(&work_dir).with_context(|| format!("creating {}", work_dir.display()))?;
    let prompt = collection_prompt(&case.question);
    atomic_write(&prompt_path, prompt.as_bytes())?;

    let mut command = collection_command(args, coral_bin, &work_dir)?;
    let outcome = run_process(
        &mut command,
        Some(&prompt_path),
        &events_path,
        &stderr_path,
        timeout,
    )?;
    if outcome.timed_out || !outcome.status.is_some_and(|status| status.success()) {
        return Ok(Trial {
            format_version: FORMAT_VERSION,
            case_id: case.case_id.clone(),
            case_digest: case_digest.to_string(),
            collection_digest: collection_digest.to_string(),
            status: if outcome.timed_out {
                "timeout".to_string()
            } else {
                "process_error".to_string()
            },
            agent_query: None,
            requested_limit: None,
            search_response: None,
            evaluation: None,
            error: Some(read_error_tail(&stderr_path)?),
            elapsed_millis: outcome.elapsed.as_millis(),
        });
    }
    let events = fs::read_to_string(&events_path)
        .with_context(|| format!("reading {}", events_path.display()))?;
    match extract_search_call(&events) {
        Ok(call) => {
            let provider_error = catalog_provider_operational_error(&call.response);
            let status = if call.limit != 10 {
                "wrong_limit".to_string()
            } else if provider_error.is_some() {
                "provider_error".to_string()
            } else {
                "ok".to_string()
            };
            let evaluation = provider_error
                .is_none()
                .then(|| evaluate_response(&case.target, &call.response));
            Ok(Trial {
                format_version: FORMAT_VERSION,
                case_id: case.case_id.clone(),
                case_digest: case_digest.to_string(),
                collection_digest: collection_digest.to_string(),
                status,
                agent_query: Some(call.query),
                requested_limit: Some(call.limit),
                search_response: Some(call.response),
                evaluation,
                error: provider_error,
                elapsed_millis: outcome.elapsed.as_millis(),
            })
        }
        Err(error) => Ok(Trial {
            format_version: FORMAT_VERSION,
            case_id: case.case_id.clone(),
            case_digest: case_digest.to_string(),
            collection_digest: collection_digest.to_string(),
            status: "protocol_error".to_string(),
            agent_query: None,
            requested_limit: None,
            search_response: None,
            evaluation: None,
            error: Some(format!("{error:#}")),
            elapsed_millis: outcome.elapsed.as_millis(),
        }),
    }
}

fn next_attempt_dir(case_dir: &Path) -> PathBuf {
    if !case_dir.exists() {
        return case_dir.to_path_buf();
    }
    for attempt in 2_u32.. {
        let candidate = case_dir.join(format!("attempt-{attempt:03}"));
        if !candidate.exists() {
            return candidate;
        }
    }
    unreachable!("u32 attempt space exhausted")
}

fn collection_command(args: &CollectArgs, coral_bin: &Path, work_dir: &Path) -> Result<Command> {
    let coral_command = format!(
        "mcp_servers.coral.command={}",
        serde_json::to_string(&coral_bin.display().to_string())?
    );
    let coral_args = [
        "--disable-tasks",
        "--disable-feedback",
        "--disable-observed-values-search",
        "--workspace",
        args.workspace.as_str(),
        "mcp-stdio",
    ];
    let coral_args = format!(
        "mcp_servers.coral.args={}",
        serde_json::to_string(&coral_args)?
    );
    let mut command = Command::new(&args.codex_bin);
    command.args([
        "exec",
        "--ephemeral",
        "--json",
        "--ignore-user-config",
        "--ignore-rules",
        "--skip-git-repo-check",
        "--sandbox",
        "read-only",
        "--model",
        &args.model,
        "-c",
        "features.memories=false",
        "-c",
        &coral_command,
        "-c",
        &coral_args,
    ]);
    if let Some(config_dir) = args.coral_config_dir.as_deref() {
        command.args([
            "-c",
            &format!(
                "mcp_servers.coral.env.CORAL_CONFIG_DIR={}",
                serde_json::to_string(&absolute_path(config_dir)?.display().to_string())?
            ),
        ]);
    }
    command.arg("-C");
    command.arg(work_dir);
    command.arg("-");
    set_config_dir(&mut command, args.coral_config_dir.as_deref())?;
    Ok(command)
}

struct SearchCall {
    query: String,
    limit: u32,
    response: Value,
}

fn extract_search_call(events: &str) -> Result<SearchCall> {
    let mut tool_ids = BTreeSet::new();
    let mut completed_tools = Vec::new();
    for line in events.lines() {
        let Ok(event) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let event_type = event.get("type").and_then(Value::as_str);
        if !matches!(event_type, Some("item.started" | "item.completed")) {
            continue;
        }
        let Some(item) = event.get("item") else {
            continue;
        };
        if matches!(
            item.get("type").and_then(Value::as_str),
            Some("agent_message" | "reasoning" | "todo_list")
        ) {
            continue;
        }
        let id = item
            .get("id")
            .and_then(Value::as_str)
            .context("tool event is missing item.id")?;
        tool_ids.insert(id.to_string());
        if event_type == Some("item.completed") {
            completed_tools.push(item.clone());
        }
    }
    if tool_ids.len() != 1 {
        bail!(
            "expected exactly one tool invocation, found {}",
            tool_ids.len()
        );
    }
    if completed_tools.len() != 1 {
        bail!(
            "expected the single tool invocation to complete, found {} completed tools",
            completed_tools.len()
        );
    }
    let call = completed_tools
        .first()
        .context("completed tool invocation disappeared")?;
    let server_is_coral = call
        .get("server")
        .and_then(Value::as_str)
        .is_some_and(|server| server.eq_ignore_ascii_case("coral"));
    if call.get("type").and_then(Value::as_str) != Some("mcp_tool_call")
        || !server_is_coral
        || call.get("tool").and_then(Value::as_str) != Some("search")
    {
        bail!("the single tool invocation must be coral.search");
    }
    if call.get("status").and_then(Value::as_str) != Some("completed") {
        bail!("Coral search call did not complete successfully");
    }
    let query = call
        .pointer("/arguments/query")
        .and_then(Value::as_str)
        .context("Coral search call is missing arguments.query")?
        .to_string();
    let limit = call
        .pointer("/arguments/limit")
        .and_then(Value::as_u64)
        .and_then(|limit| u32::try_from(limit).ok())
        .context("Coral search call is missing a valid arguments.limit")?;
    let response = call
        .pointer("/result/structured_content")
        .cloned()
        .context("Coral search call is missing result.structured_content")?;
    Ok(SearchCall {
        query,
        limit,
        response,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReplayRecord {
    format_version: u32,
    case_id: String,
    sample_id: String,
    style: String,
    question: String,
    query: String,
    target: Target,
    limit_10: SearchRun,
    limit_50: SearchRun,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SearchRun {
    limit: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    response: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    evaluation: Option<RankEvaluation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    response_token_count: Option<usize>,
    elapsed_millis: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RankEvaluation {
    #[serde(skip_serializing_if = "Option::is_none")]
    target_rank: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_rank: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    child_rank: Option<usize>,
    child_target: bool,
    censored: bool,
}

fn replay(args: &ReplayArgs) -> Result<bool> {
    validate_jobs(args.jobs)?;
    validate_label(Some(&args.label))?;
    let coral_bin = absolute_path(&args.coral_bin)?;
    ensure_file(&coral_bin, "Coral binary")?;
    let default_corpus_path = args.dir.join("collected-corpus.jsonl");
    let corpus_path = args.corpus.as_deref().unwrap_or(&default_corpus_path);
    let replay_path = artifact_path(&args.dir, "replay", Some(&args.label), "jsonl");
    let raw_replay_dir = args
        .dir
        .join("raw")
        .join(artifact_name("replay", Some(&args.label)));
    refuse_existing_replay(&replay_path, &raw_replay_dir)?;
    let corpus_bytes =
        fs::read(corpus_path).with_context(|| format!("reading {}", corpus_path.display()))?;
    let corpus_sha256 = sha256_hex(&corpus_bytes);
    let corpus = read_jsonl::<CorpusCase>(corpus_path)?;
    if corpus.is_empty() {
        bail!("{} is empty", corpus_path.display());
    }
    validate_corpus_case_ids(&corpus)?;
    let prepared_inventory_sha256 = verified_inventory_sha256(
        &args.dir,
        &coral_bin,
        &args.workspace,
        args.coral_config_dir.as_deref(),
    )?;
    let coral_version = command_version(&coral_bin)?;
    let coral_sha256 = sha256_file(&coral_bin)?;
    let timeout = Duration::from_secs(args.timeout_seconds);
    let (first_case, remaining_cases) = corpus.split_first().context("non-empty replay corpus")?;
    let mut records = vec![replay_case(first_case, args, &coral_bin, timeout)?];
    let outputs = parallel_map(remaining_cases, args.jobs, |_, case| {
        replay_case(case, args, &coral_bin, timeout)
    });
    records.reserve(outputs.len());
    for output in outputs {
        records.push(output?);
    }
    write_jsonl(&replay_path, &records)?;
    let successful = records
        .iter()
        .filter(|record| record.limit_10.error.is_none() && record.limit_50.error.is_none())
        .count();
    update_manifest(
        &args.dir,
        &artifact_name("replay", Some(&args.label)),
        json!({
            "coral_version": coral_version,
            "coral_bin": coral_bin,
            "coral_sha256": coral_sha256,
            "workspace": args.workspace,
            "coral_config_dir": args.coral_config_dir,
            "observed_values_search": args.enable_observed_values_search,
            "jobs": args.jobs,
            "corpus": corpus_path,
            "corpus_sha256": corpus_sha256,
            "inventory_sha256": prepared_inventory_sha256,
            "case_count": records.len(),
            "successful_cases": successful,
            "response_token_encoding": RESPONSE_TOKEN_ENCODING
        }),
    )?;
    println!(
        "Replayed {successful}/{} cases at limits 10 and 50 in {}.",
        records.len(),
        args.dir.display()
    );
    Ok(successful == records.len())
}

fn replay_case(
    case: &CorpusCase,
    args: &ReplayArgs,
    coral_bin: &Path,
    timeout: Duration,
) -> Result<ReplayRecord> {
    let query = case
        .frozen_query
        .as_deref()
        .context("collected corpus case is missing frozen_query")?;
    Ok(ReplayRecord {
        format_version: REPLAY_FORMAT_VERSION,
        case_id: case.case_id.clone(),
        sample_id: case.sample_id.clone(),
        style: case.style.clone(),
        question: case.question.clone(),
        query: query.to_string(),
        target: case.target.clone(),
        limit_10: run_coral_search(case, args, coral_bin, query, 10, timeout)?,
        limit_50: run_coral_search(case, args, coral_bin, query, 50, timeout)?,
    })
}

fn run_coral_search(
    case: &CorpusCase,
    args: &ReplayArgs,
    coral_bin: &Path,
    query: &str,
    limit: u32,
    timeout: Duration,
) -> Result<SearchRun> {
    let run_dir = args
        .dir
        .join("raw")
        .join(artifact_name("replay", Some(&args.label)))
        .join(&case.case_id)
        .join(format!("limit-{limit}"));
    fs::create_dir_all(&run_dir).with_context(|| format!("creating {}", run_dir.display()))?;
    let stdout_path = run_dir.join("response.json");
    let stderr_path = run_dir.join("stderr.log");
    let limit_text = limit.to_string();
    let mut command = Command::new(coral_bin);
    command.args([
        observed_values_search_flag(args.enable_observed_values_search),
        "--workspace",
        &args.workspace,
        "search",
        "--json",
        "--limit",
        &limit_text,
        query,
    ]);
    set_config_dir(&mut command, args.coral_config_dir.as_deref())?;
    let outcome = run_process(&mut command, None, &stdout_path, &stderr_path, timeout)?;
    if outcome.timed_out || !outcome.status.is_some_and(|status| status.success()) {
        return Ok(SearchRun {
            limit,
            response: None,
            evaluation: None,
            error: Some(if outcome.timed_out {
                "search timed out".to_string()
            } else {
                read_error_tail(&stderr_path)?
            }),
            response_token_count: None,
            elapsed_millis: outcome.elapsed.as_millis(),
        });
    }
    let raw = fs::read_to_string(&stdout_path)
        .with_context(|| format!("reading {}", stdout_path.display()))?;
    let response = serde_json::from_str::<Value>(&raw).context("parsing Coral search JSON")?;
    let response_token_count = response_token_count(&response);
    if let Some(error) = catalog_provider_operational_error(&response) {
        return Ok(SearchRun {
            limit,
            response: Some(response),
            evaluation: None,
            error: Some(error),
            response_token_count,
            elapsed_millis: outcome.elapsed.as_millis(),
        });
    }
    let evaluation = evaluate_response(&case.target, &response);
    Ok(SearchRun {
        limit,
        response: Some(response),
        evaluation: Some(evaluation),
        error: None,
        response_token_count,
        elapsed_millis: outcome.elapsed.as_millis(),
    })
}

fn observed_values_search_flag(enabled: bool) -> &'static str {
    if enabled {
        "--enable-observed-values-search"
    } else {
        "--disable-observed-values-search"
    }
}

fn response_token_count(response: &Value) -> Option<usize> {
    serde_json::to_string(response)
        .ok()
        .map(|json| tiktoken_rs::o200k_base_singleton().count_ordinary(&json))
}

fn catalog_provider_operational_error(response: &Value) -> Option<String> {
    let Some(statuses) = response.get("provider_statuses").and_then(Value::as_array) else {
        return Some("search response is missing provider_statuses".to_string());
    };
    let Some(status) = statuses
        .iter()
        .find(|status| status.get("provider").and_then(Value::as_str) == Some("catalog_metadata"))
    else {
        return Some("search response is missing catalog_metadata status".to_string());
    };
    let state = status
        .get("state")
        .and_then(Value::as_str)
        .unwrap_or("unspecified");
    let coverage = status.get("coverage");
    let failed_units = coverage
        .and_then(|coverage| coverage.get("failed_units"))
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let timed_out = coverage
        .and_then(|coverage| coverage.get("timed_out"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let budget_exhausted = coverage
        .and_then(|coverage| coverage.get("budget_exhausted"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let stale_index = coverage
        .and_then(|coverage| coverage.get("stale_index"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if state == "error" || failed_units > 0 || timed_out || budget_exhausted || stale_index {
        Some(format!(
            "catalog provider was not healthy: state={state}, failed_units={failed_units}, timed_out={timed_out}, budget_exhausted={budget_exhausted}, stale_index={stale_index}"
        ))
    } else {
        None
    }
}

fn evaluate_response(target: &Target, response: &Value) -> RankEvaluation {
    let empty = Vec::new();
    let results = response
        .get("results")
        .and_then(Value::as_array)
        .unwrap_or(&empty);
    let child_target = target.is_child();
    let parent_rank = first_rank(results, |result| owner_result_matches(target, result));
    let child_rank = target.field_name.as_ref().and_then(|_| {
        first_rank(results, |result| {
            exact_result_matches(target, result) || field_visible_in_parent(target, result)
        })
    });
    let target_rank = if child_target {
        child_rank
    } else {
        parent_rank
    };
    RankEvaluation {
        target_rank,
        parent_rank,
        child_rank,
        child_target,
        censored: response_is_censored(response),
    }
}

fn first_rank(results: &[Value], matches: impl Fn(&Value) -> bool) -> Option<usize> {
    results.iter().position(matches).map(|index| index + 1)
}

fn exact_result_matches(target: &Target, result: &Value) -> bool {
    if target.field_name.is_none() {
        return owner_result_matches(target, result);
    }
    result.get("kind").and_then(Value::as_str) == Some("column_hint")
        && result
            .pointer("/column_hint/schema_name")
            .and_then(Value::as_str)
            == Some(target.schema_name.as_str())
        && result
            .pointer("/column_hint/surface_kind")
            .and_then(Value::as_str)
            == Some(target.surface_kind.as_str())
        && result
            .pointer("/column_hint/surface_name")
            .and_then(Value::as_str)
            == Some(target.surface_name.as_str())
        && result
            .pointer("/column_hint/column_name")
            .and_then(Value::as_str)
            == target.field_name.as_deref()
        && result
            .pointer("/column_hint/field_role")
            .and_then(Value::as_str)
            == target.field_role.as_deref()
}

fn owner_result_matches(target: &Target, result: &Value) -> bool {
    if result.get("kind").and_then(Value::as_str) != Some("catalog_metadata") {
        return false;
    }
    let Some(item) = result.pointer("/catalog_metadata/item") else {
        return false;
    };
    let qualified_name = format!("{}.{}", target.schema_name, target.surface_name);
    item.get("kind").and_then(Value::as_str) == Some(target.surface_kind.as_str())
        && item.get("name").and_then(Value::as_str) == Some(qualified_name.as_str())
}

fn field_visible_in_parent(target: &Target, result: &Value) -> bool {
    if !owner_result_matches(target, result) {
        return false;
    }
    let Some(field_name) = target.field_name.as_deref() else {
        return false;
    };
    match target.field_role.as_deref() {
        Some("table_column") => object_contains_key(result, "/catalog_metadata/fields", field_name),
        Some("table_filter") => {
            array_contains_string(result, "/catalog_metadata/required_filters", field_name)
        }
        Some("table_function_argument") => {
            object_or_array_contains_name(result, "/catalog_metadata/arguments", field_name)
        }
        Some("table_function_result_column") => {
            object_contains_key(result, "/catalog_metadata/returns", field_name)
        }
        _ => false,
    }
}

fn object_contains_key(result: &Value, pointer: &str, key: &str) -> bool {
    result
        .pointer(pointer)
        .and_then(Value::as_object)
        .is_some_and(|fields| fields.contains_key(key))
}

fn object_or_array_contains_name(result: &Value, pointer: &str, expected: &str) -> bool {
    object_contains_key(result, pointer, expected)
        || array_contains_string(result, pointer, expected)
}

fn array_contains_string(result: &Value, pointer: &str, expected: &str) -> bool {
    result
        .pointer(pointer)
        .and_then(Value::as_array)
        .is_some_and(|values| values.iter().any(|value| value.as_str() == Some(expected)))
}

fn response_is_censored(response: &Value) -> bool {
    response
        .pointer("/truncation/truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || response
            .get("provider_statuses")
            .and_then(Value::as_array)
            .is_some_and(|statuses| {
                statuses.iter().any(|status| {
                    status
                        .pointer("/coverage/has_more")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                })
            })
}

#[derive(Debug, Serialize)]
struct Report {
    format_version: u32,
    response_token_encoding: &'static str,
    case_count: usize,
    limit_10: MetricSummary,
    limit_50: MetricSummary,
    schemas: BTreeMap<String, GroupReport>,
    target_classes: BTreeMap<String, GroupReport>,
    schema_macro_at_10: MacroSummary,
    target_class_macro_at_10: MacroSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    comparison: Option<ComparisonReport>,
}

#[derive(Debug, Serialize)]
struct GroupReport {
    case_count: usize,
    limit_10: MetricSummary,
    limit_50: MetricSummary,
}

#[derive(Debug, Serialize)]
struct MacroSummary {
    group_count: usize,
    target_hit_at_10_rate: f64,
    target_mean_reciprocal_rank: f64,
}

#[derive(Debug, Serialize)]
struct ComparisonReport {
    baseline_label: String,
    candidate_label: String,
    case_count: usize,
    unscored_cases: usize,
    new_hits_at_10: usize,
    lost_hits_at_10: usize,
    rank_improvements: usize,
    rank_regressions: usize,
    unchanged: usize,
    movements: Vec<CaseMovement>,
}

#[derive(Debug, Serialize)]
struct CaseMovement {
    case_id: String,
    change: &'static str,
    baseline_rank: Option<usize>,
    candidate_rank: Option<usize>,
    target: Target,
    query: String,
}

#[derive(Debug, Serialize)]
struct MetricSummary {
    evaluated: usize,
    errors: usize,
    target_hit_at_1: usize,
    target_hit_at_3: usize,
    target_hit_at_5: usize,
    target_hit_at_10: usize,
    target_hit_at_50: usize,
    target_missing: usize,
    target_censored_missing: usize,
    target_mean_reciprocal_rank: f64,
    parent_hit_at_1: usize,
    parent_hit_at_3: usize,
    parent_hit_at_5: usize,
    parent_hit_at_10: usize,
    parent_hit_at_50: usize,
    parent_missing: usize,
    parent_mean_reciprocal_rank: f64,
    child_targets: usize,
    child_parent_hit_at_10: usize,
    child_hit_at_10: usize,
    child_hit_with_parent_at_10: usize,
    observed_value_provider_states: BTreeMap<String, usize>,
    observed_value_max_eligible_units: usize,
    observed_value_cases: usize,
    observed_value_results: usize,
    best_observed_value_rank: Option<usize>,
    tokenized_responses: usize,
    mean_response_tokens: f64,
    p95_response_tokens: usize,
    max_response_tokens: usize,
}

fn report(args: &ReportArgs) -> Result<bool> {
    validate_label(Some(&args.label))?;
    let replay_path = artifact_path(&args.dir, "replay", Some(&args.label), "jsonl");
    let records = read_replay_records(&replay_path)?;
    if records.is_empty() {
        bail!("{} is empty", replay_path.display());
    }
    let schemas = summarize_groups(&records, |record| record.target.schema_name.clone());
    let target_classes = summarize_groups(&records, |record| {
        if record.target.is_child() {
            "child".to_string()
        } else {
            "parent".to_string()
        }
    });
    let comparison = args
        .baseline_label
        .as_deref()
        .map(|baseline_label| {
            validate_label(Some(baseline_label))?;
            validate_replay_context(&args.dir, baseline_label, &args.label)?;
            let baseline_path = artifact_path(&args.dir, "replay", Some(baseline_label), "jsonl");
            let baseline = read_replay_records(&baseline_path)?;
            compare_replays(baseline_label, &args.label, &baseline, &records)
        })
        .transpose()?;
    let report = Report {
        format_version: REPLAY_FORMAT_VERSION,
        response_token_encoding: RESPONSE_TOKEN_ENCODING,
        case_count: records.len(),
        limit_10: summarize(records.iter().map(|record| &record.limit_10)),
        limit_50: summarize(records.iter().map(|record| &record.limit_50)),
        schema_macro_at_10: macro_summary(schemas.values()),
        target_class_macro_at_10: macro_summary(target_classes.values()),
        schemas,
        target_classes,
        comparison,
    };
    let summary_json = artifact_path(&args.dir, "summary", Some(&args.label), "json");
    let summary_markdown = artifact_path(&args.dir, "summary", Some(&args.label), "md");
    refuse_existing_summary(&summary_json, &summary_markdown)?;
    atomic_write_json(&summary_json, &report)?;
    let markdown = render_report(&report);
    atomic_write(&summary_markdown, markdown.as_bytes())?;
    print!("{markdown}");
    Ok(report.limit_10.errors == 0 && report.limit_50.errors == 0)
}

fn summarize_groups(
    records: &[ReplayRecord],
    key: impl Fn(&ReplayRecord) -> String,
) -> BTreeMap<String, GroupReport> {
    let mut grouped = BTreeMap::<String, Vec<&ReplayRecord>>::new();
    for record in records {
        grouped.entry(key(record)).or_default().push(record);
    }
    grouped
        .into_iter()
        .map(|(name, records)| {
            (
                name,
                GroupReport {
                    case_count: records.len(),
                    limit_10: summarize(records.iter().map(|record| &record.limit_10)),
                    limit_50: summarize(records.iter().map(|record| &record.limit_50)),
                },
            )
        })
        .collect()
}

fn macro_summary<'a>(groups: impl Iterator<Item = &'a GroupReport>) -> MacroSummary {
    let groups = groups.collect::<Vec<_>>();
    let group_count = groups.len();
    let (hit_rate_sum, reciprocal_rank_sum) =
        groups.iter().fold((0.0, 0.0), |(hit_sum, mrr_sum), group| {
            let hit_rate = if group.limit_10.evaluated == 0 {
                0.0
            } else {
                count_as_f64(group.limit_10.target_hit_at_10)
                    / count_as_f64(group.limit_10.evaluated)
            };
            (
                hit_sum + hit_rate,
                mrr_sum + group.limit_10.target_mean_reciprocal_rank,
            )
        });
    MacroSummary {
        group_count,
        target_hit_at_10_rate: divide_or_zero(hit_rate_sum, group_count),
        target_mean_reciprocal_rank: divide_or_zero(reciprocal_rank_sum, group_count),
    }
}

fn compare_replays(
    baseline_label: &str,
    candidate_label: &str,
    baseline: &[ReplayRecord],
    candidate: &[ReplayRecord],
) -> Result<ComparisonReport> {
    let baseline_by_id = replay_records_by_id(baseline, baseline_label)?;
    let candidate_by_id = replay_records_by_id(candidate, candidate_label)?;
    if baseline_by_id.keys().ne(candidate_by_id.keys()) {
        bail!("baseline and candidate replay case IDs differ");
    }

    let mut comparison = ComparisonReport {
        baseline_label: baseline_label.to_string(),
        candidate_label: candidate_label.to_string(),
        case_count: candidate.len(),
        unscored_cases: 0,
        new_hits_at_10: 0,
        lost_hits_at_10: 0,
        rank_improvements: 0,
        rank_regressions: 0,
        unchanged: 0,
        movements: Vec::new(),
    };
    for (case_id, baseline) in baseline_by_id {
        let candidate = candidate_by_id
            .get(case_id)
            .context("candidate case disappeared during comparison")?;
        if baseline.query != candidate.query || baseline.target != candidate.target {
            bail!("baseline and candidate differ for case {case_id}");
        }
        let (Some(baseline_evaluation), Some(candidate_evaluation)) = (
            baseline.limit_10.evaluation.as_ref(),
            candidate.limit_10.evaluation.as_ref(),
        ) else {
            comparison.unscored_cases += 1;
            comparison.movements.push(CaseMovement {
                case_id: case_id.to_string(),
                change: "unscored",
                baseline_rank: baseline
                    .limit_10
                    .evaluation
                    .as_ref()
                    .and_then(|evaluation| evaluation.target_rank),
                candidate_rank: candidate
                    .limit_10
                    .evaluation
                    .as_ref()
                    .and_then(|evaluation| evaluation.target_rank),
                target: candidate.target.clone(),
                query: candidate.query.clone(),
            });
            continue;
        };
        let change = match (
            baseline_evaluation.target_rank,
            candidate_evaluation.target_rank,
        ) {
            (None, Some(_)) => {
                comparison.new_hits_at_10 += 1;
                Some("new_hit_at_10")
            }
            (Some(_), None) => {
                comparison.lost_hits_at_10 += 1;
                Some("lost_hit_at_10")
            }
            (Some(baseline), Some(candidate)) if candidate < baseline => {
                comparison.rank_improvements += 1;
                Some("rank_improved")
            }
            (Some(baseline), Some(candidate)) if candidate > baseline => {
                comparison.rank_regressions += 1;
                Some("rank_regressed")
            }
            _ => {
                comparison.unchanged += 1;
                None
            }
        };
        if let Some(change) = change {
            comparison.movements.push(CaseMovement {
                case_id: case_id.to_string(),
                change,
                baseline_rank: baseline_evaluation.target_rank,
                candidate_rank: candidate_evaluation.target_rank,
                target: candidate.target.clone(),
                query: candidate.query.clone(),
            });
        }
    }
    Ok(comparison)
}

fn validate_replay_context(dir: &Path, baseline_label: &str, candidate_label: &str) -> Result<()> {
    let path = dir.join("manifest.json");
    let raw = fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let manifest = serde_json::from_str::<Value>(&raw).context("parsing benchmark manifest")?;
    let baseline_key = artifact_name("replay", Some(baseline_label));
    let candidate_key = artifact_name("replay", Some(candidate_label));
    let baseline = manifest
        .get(&baseline_key)
        .with_context(|| format!("manifest is missing {baseline_key}"))?;
    let candidate = manifest
        .get(&candidate_key)
        .with_context(|| format!("manifest is missing {candidate_key}"))?;
    for field in [
        "corpus_sha256",
        "inventory_sha256",
        "workspace",
        "coral_config_dir",
    ] {
        if baseline.get(field) != candidate.get(field) {
            bail!("baseline and candidate replay {field} differ");
        }
    }
    Ok(())
}

fn replay_records_by_id<'a>(
    records: &'a [ReplayRecord],
    label: &str,
) -> Result<BTreeMap<&'a str, &'a ReplayRecord>> {
    let mut by_id = BTreeMap::new();
    for record in records {
        if by_id.insert(record.case_id.as_str(), record).is_some() {
            bail!("replay {label} contains duplicate case {}", record.case_id);
        }
    }
    Ok(by_id)
}

fn divide_or_zero(numerator: f64, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator / count_as_f64(denominator)
    }
}

fn summarize<'a>(runs: impl Iterator<Item = &'a SearchRun>) -> MetricSummary {
    let mut summary = MetricSummary {
        evaluated: 0,
        errors: 0,
        target_hit_at_1: 0,
        target_hit_at_3: 0,
        target_hit_at_5: 0,
        target_hit_at_10: 0,
        target_hit_at_50: 0,
        target_missing: 0,
        target_censored_missing: 0,
        target_mean_reciprocal_rank: 0.0,
        parent_hit_at_1: 0,
        parent_hit_at_3: 0,
        parent_hit_at_5: 0,
        parent_hit_at_10: 0,
        parent_hit_at_50: 0,
        parent_missing: 0,
        parent_mean_reciprocal_rank: 0.0,
        child_targets: 0,
        child_parent_hit_at_10: 0,
        child_hit_at_10: 0,
        child_hit_with_parent_at_10: 0,
        observed_value_provider_states: BTreeMap::new(),
        observed_value_max_eligible_units: 0,
        observed_value_cases: 0,
        observed_value_results: 0,
        best_observed_value_rank: None,
        tokenized_responses: 0,
        mean_response_tokens: 0.0,
        p95_response_tokens: 0,
        max_response_tokens: 0,
    };
    let mut target_reciprocal_rank_sum = 0.0;
    let mut parent_reciprocal_rank_sum = 0.0;
    let mut response_tokens = Vec::new();
    for run in runs {
        if let Some(response) = run.response.as_ref() {
            record_observed_value_exposure(&mut summary, response);
        }
        let Some(evaluation) = run.evaluation.as_ref() else {
            summary.errors += 1;
            continue;
        };
        summary.evaluated += 1;
        if let Some(token_count) = run
            .response_token_count
            .or_else(|| run.response.as_ref().and_then(response_token_count))
        {
            response_tokens.push(token_count);
        }
        if let Some(rank) = evaluation.target_rank {
            summary.target_hit_at_1 += usize::from(rank <= 1);
            summary.target_hit_at_3 += usize::from(rank <= 3);
            summary.target_hit_at_5 += usize::from(rank <= 5);
            summary.target_hit_at_10 += usize::from(rank <= 10);
            summary.target_hit_at_50 += usize::from(rank <= 50);
            target_reciprocal_rank_sum += 1.0 / count_as_f64(rank);
        } else {
            summary.target_missing += 1;
            summary.target_censored_missing += usize::from(evaluation.censored);
        }
        if let Some(rank) = evaluation.parent_rank {
            summary.parent_hit_at_1 += usize::from(rank <= 1);
            summary.parent_hit_at_3 += usize::from(rank <= 3);
            summary.parent_hit_at_5 += usize::from(rank <= 5);
            summary.parent_hit_at_10 += usize::from(rank <= 10);
            summary.parent_hit_at_50 += usize::from(rank <= 50);
            parent_reciprocal_rank_sum += 1.0 / count_as_f64(rank);
        } else {
            summary.parent_missing += 1;
        }
        if evaluation.child_target {
            summary.child_targets += 1;
            let parent_hit = evaluation.parent_rank.is_some_and(|rank| rank <= 10);
            let child_hit = evaluation.child_rank.is_some_and(|rank| rank <= 10);
            summary.child_parent_hit_at_10 += usize::from(parent_hit);
            summary.child_hit_at_10 += usize::from(child_hit);
            summary.child_hit_with_parent_at_10 += usize::from(parent_hit && child_hit);
        }
    }
    if summary.evaluated > 0 {
        summary.target_mean_reciprocal_rank =
            target_reciprocal_rank_sum / count_as_f64(summary.evaluated);
        summary.parent_mean_reciprocal_rank =
            parent_reciprocal_rank_sum / count_as_f64(summary.evaluated);
    }
    if !response_tokens.is_empty() {
        response_tokens.sort_unstable();
        summary.tokenized_responses = response_tokens.len();
        summary.mean_response_tokens = count_as_f64(response_tokens.iter().copied().sum())
            / count_as_f64(response_tokens.len());
        let p95_index = response_tokens
            .len()
            .saturating_mul(95)
            .div_ceil(100)
            .saturating_sub(1);
        summary.p95_response_tokens = response_tokens.get(p95_index).copied().unwrap_or_default();
        summary.max_response_tokens = response_tokens.last().copied().unwrap_or_default();
    }
    summary
}

fn record_observed_value_exposure(summary: &mut MetricSummary, response: &Value) {
    if let Some(status) = response
        .get("provider_statuses")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|status| status.get("provider").and_then(Value::as_str) == Some("observed_values"))
    {
        let state = status
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("missing");
        *summary
            .observed_value_provider_states
            .entry(state.to_string())
            .or_default() += 1;
        let eligible_units = status
            .pointer("/coverage/eligible_units")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or_default();
        summary.observed_value_max_eligible_units = summary
            .observed_value_max_eligible_units
            .max(eligible_units);
    }

    let mut observed_value_count = 0;
    let mut best_rank = None;
    for (index, result) in response
        .get("results")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
    {
        if result.get("kind").and_then(Value::as_str) == Some("observed_value") {
            observed_value_count += 1;
            best_rank.get_or_insert(index + 1);
        }
    }
    if observed_value_count == 0 {
        return;
    }
    summary.observed_value_cases += 1;
    summary.observed_value_results += observed_value_count;
    let best_rank = best_rank.expect("observed value result has a rank");
    summary.best_observed_value_rank = Some(
        summary
            .best_observed_value_rank
            .map_or(best_rank, |current| current.min(best_rank)),
    );
}

fn render_report(report: &Report) -> String {
    let mut markdown = format!(
        "# Universal Search benchmark\n\n{}",
        render_metric_section(
            "Combined",
            report.case_count,
            &report.limit_10,
            &report.limit_50
        )
    );
    markdown.push_str(&render_macro_section(report));
    markdown.push_str(&render_group_table("By schema", &report.schemas));
    markdown.push_str(&render_group_table(
        "By target class",
        &report.target_classes,
    ));
    if let Some(comparison) = &report.comparison {
        markdown.push_str(&render_comparison(comparison));
    }
    markdown
}

fn render_macro_section(report: &Report) -> String {
    format!(
        "## Equal-weight summaries\n\n\
         Each schema has equal weight in the schema macro. Parent and child cases have equal weight in the target-class macro.\n\n\
         | Grouping | Groups | Target Hit@10 | Target MRR |\n\
         | --- | ---: | ---: | ---: |\n\
         | Schema | {} | {:.1}% | {:.4} |\n\
         | Target class | {} | {:.1}% | {:.4} |\n\n",
        report.schema_macro_at_10.group_count,
        report.schema_macro_at_10.target_hit_at_10_rate * 100.0,
        report.schema_macro_at_10.target_mean_reciprocal_rank,
        report.target_class_macro_at_10.group_count,
        report.target_class_macro_at_10.target_hit_at_10_rate * 100.0,
        report.target_class_macro_at_10.target_mean_reciprocal_rank,
    )
}

fn render_group_table(title: &str, groups: &BTreeMap<String, GroupReport>) -> String {
    let mut markdown = format!(
        "## {title}\n\n\
         | Group | Cases | L10 errors | L10 target Hit@10 | L10 target MRR | L50 target Hit@10 | L50 target MRR |\n\
         | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n"
    );
    for (name, group) in groups {
        writeln!(
            markdown,
            "| {name} | {} | {} | {} | {:.4} | {} | {:.4} |",
            group.case_count,
            group.limit_10.errors,
            group.limit_10.target_hit_at_10,
            group.limit_10.target_mean_reciprocal_rank,
            group.limit_50.target_hit_at_10,
            group.limit_50.target_mean_reciprocal_rank,
        )
        .expect("writing to a String cannot fail");
    }
    markdown.push('\n');
    markdown
}

fn render_comparison(comparison: &ComparisonReport) -> String {
    let mut markdown = format!(
        "## Paired comparison\n\n\
         Candidate `{}` against baseline `{}` using the same case IDs, frozen queries, and exact targets.\n\n\
         | Cases | Unscored | New Hit@10 | Lost Hit@10 | Rank improved | Rank regressed | Unchanged |\n\
         | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n\
         | {} | {} | {} | {} | {} | {} | {} |\n\n",
        comparison.candidate_label,
        comparison.baseline_label,
        comparison.case_count,
        comparison.unscored_cases,
        comparison.new_hits_at_10,
        comparison.lost_hits_at_10,
        comparison.rank_improvements,
        comparison.rank_regressions,
        comparison.unchanged,
    );
    if !comparison.movements.is_empty() {
        markdown.push_str(
            "| Case | Change | Baseline rank | Candidate rank | Target | Frozen query |\n\
             | --- | --- | ---: | ---: | --- | --- |\n",
        );
        for movement in &comparison.movements {
            writeln!(
                markdown,
                "| {} | {} | {} | {} | {} | {} |",
                movement.case_id,
                movement.change,
                rank_label(movement.baseline_rank),
                rank_label(movement.candidate_rank),
                markdown_cell(&target_label(&movement.target)),
                markdown_cell(&movement.query),
            )
            .expect("writing to a String cannot fail");
        }
        markdown.push('\n');
    }
    markdown
}

fn rank_label(rank: Option<usize>) -> String {
    rank.map_or_else(|| "—".to_string(), |rank| rank.to_string())
}

fn target_label(target: &Target) -> String {
    match (&target.field_role, &target.field_name) {
        (Some(role), Some(field)) => format!(
            "{}.{} · {} · {}",
            target.schema_name, target.surface_name, role, field
        ),
        _ => format!("{}.{}", target.schema_name, target.surface_name),
    }
}

fn markdown_cell(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}

fn render_metric_section(
    title: &str,
    case_count: usize,
    limit_10: &MetricSummary,
    limit_50: &MetricSummary,
) -> String {
    format!(
        "## {title}\n\n\
         Cases: {case_count}\n\n\
         ### Target result\n\n\
         The exact requested object. A parent target matches the returned table/function; a child target requires its correct parent and role-specific field entry.\n\n\
         | Limit | Evaluated | Errors | Hit@1 | Hit@3 | Hit@5 | Hit@10 | Hit@50 | Missing | Censored missing | MRR |\n\
         | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n\
         {}\n\
         {}\n\n\
         ### Parent result\n\n\
         The table or table function that owns the requested object. Parent targets own themselves.\n\n\
         | Limit | Evaluated | Hit@1 | Hit@3 | Hit@5 | Hit@10 | Hit@50 | Missing | MRR |\n\
         | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n\
         {}\n\
         {}\n\n\
         ### Child selection\n\n\
         Only cases whose target is a field, filter, argument, or result column.\n\n\
         | Limit | Child targets | Parent Hit@10 | Child Hit@10 | Child selected when parent Hit@10 |\n\
         | ---: | ---: | ---: | ---: | ---: |\n\
         {}\n\
         {}\n\n\
         ### Response size\n\n\
         Compact JSON tokenized with `{}`.\n\n\
         | Limit | Counted | Mean tokens | P95 tokens | Max tokens |\n\
         | ---: | ---: | ---: | ---: | ---: |\n\
         {}\n\
         {}\n\n\
         ### Observed-value exposure\n\n\
         Counts observed-value results competing for the returned result window. Values are retained in the ignored replay payloads but are not copied into this report.\n\n\
         | Limit | Provider states | Max eligible units | Cases with observed values | Result slots | Best rank |\n\
         | ---: | --- | ---: | ---: | ---: | ---: |\n\
         {}\n\
         {}\n\n",
        target_metric_row(10, limit_10),
        target_metric_row(50, limit_50),
        parent_metric_row(10, limit_10),
        parent_metric_row(50, limit_50),
        child_metric_row(10, limit_10),
        child_metric_row(50, limit_50),
        RESPONSE_TOKEN_ENCODING,
        response_token_metric_row(10, limit_10),
        response_token_metric_row(50, limit_50),
        observed_value_metric_row(10, limit_10),
        observed_value_metric_row(50, limit_50)
    )
}

fn observed_value_metric_row(limit: u32, summary: &MetricSummary) -> String {
    format!(
        "| {limit} | {} | {} | {} | {} | {} |",
        provider_state_label(&summary.observed_value_provider_states),
        summary.observed_value_max_eligible_units,
        summary.observed_value_cases,
        summary.observed_value_results,
        rank_label(summary.best_observed_value_rank)
    )
}

fn provider_state_label(states: &BTreeMap<String, usize>) -> String {
    if states.is_empty() {
        return "—".to_string();
    }
    states
        .iter()
        .map(|(state, count)| format!("{state}: {count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn response_token_metric_row(limit: u32, summary: &MetricSummary) -> String {
    format!(
        "| {limit} | {} | {:.1} | {} | {} |",
        summary.tokenized_responses,
        summary.mean_response_tokens,
        summary.p95_response_tokens,
        summary.max_response_tokens
    )
}

fn parent_metric_row(limit: u32, summary: &MetricSummary) -> String {
    format!(
        "| {limit} | {} | {} | {} | {} | {} | {} | {} | {:.4} |",
        summary.evaluated,
        summary.parent_hit_at_1,
        summary.parent_hit_at_3,
        summary.parent_hit_at_5,
        summary.parent_hit_at_10,
        summary.parent_hit_at_50,
        summary.parent_missing,
        summary.parent_mean_reciprocal_rank
    )
}

fn target_metric_row(limit: u32, summary: &MetricSummary) -> String {
    format!(
        "| {limit} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.4} |",
        summary.evaluated,
        summary.errors,
        summary.target_hit_at_1,
        summary.target_hit_at_3,
        summary.target_hit_at_5,
        summary.target_hit_at_10,
        summary.target_hit_at_50,
        summary.target_missing,
        summary.target_censored_missing,
        summary.target_mean_reciprocal_rank
    )
}

fn child_metric_row(limit: u32, summary: &MetricSummary) -> String {
    let conditional = if summary.child_parent_hit_at_10 == 0 {
        0.0
    } else {
        100.0 * count_as_f64(summary.child_hit_with_parent_at_10)
            / count_as_f64(summary.child_parent_hit_at_10)
    };
    format!(
        "| {limit} | {} | {} | {} | {conditional:.1}% |",
        summary.child_targets, summary.child_parent_hit_at_10, summary.child_hit_at_10
    )
}

struct ProcessOutcome {
    status: Option<ExitStatus>,
    timed_out: bool,
    elapsed: Duration,
}

fn run_process(
    command: &mut Command,
    stdin_path: Option<&Path>,
    stdout_path: &Path,
    stderr_path: &Path,
    timeout: Duration,
) -> Result<ProcessOutcome> {
    ensure_parent(stdout_path)?;
    ensure_parent(stderr_path)?;
    command.stdout(Stdio::from(
        File::create(stdout_path).with_context(|| format!("creating {}", stdout_path.display()))?,
    ));
    command.stderr(Stdio::from(
        File::create(stderr_path).with_context(|| format!("creating {}", stderr_path.display()))?,
    ));
    if let Some(path) = stdin_path {
        command.stdin(Stdio::from(
            File::open(path).with_context(|| format!("opening {}", path.display()))?,
        ));
    } else {
        command.stdin(Stdio::null());
    }
    let start = Instant::now();
    let mut child = command.spawn().context("spawning benchmark subprocess")?;
    loop {
        if let Some(status) = child.try_wait().context("polling benchmark subprocess")? {
            return Ok(ProcessOutcome {
                status: Some(status),
                timed_out: false,
                elapsed: start.elapsed(),
            });
        }
        if start.elapsed() >= timeout {
            child
                .kill()
                .context("terminating timed-out benchmark subprocess")?;
            let status = child.wait().context("waiting for terminated subprocess")?;
            return Ok(ProcessOutcome {
                status: Some(status),
                timed_out: true,
                elapsed: start.elapsed(),
            });
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn require_success(outcome: &ProcessOutcome, label: &str, stderr_path: &Path) -> Result<()> {
    if outcome.timed_out {
        bail!("{label} timed out");
    }
    if !outcome.status.is_some_and(|status| status.success()) {
        bail!("{label} failed: {}", read_error_tail(stderr_path)?);
    }
    Ok(())
}

fn parallel_map<T, R>(
    items: &[T],
    jobs: usize,
    operation: impl Fn(usize, &T) -> Result<R> + Sync,
) -> Vec<Result<R>>
where
    T: Sync,
    R: Send,
{
    let next = AtomicUsize::new(0);
    let results = std::sync::Mutex::new(
        std::iter::repeat_with(|| None)
            .take(items.len())
            .collect::<Vec<Option<Result<R>>>>(),
    );
    thread::scope(|scope| {
        for _ in 0..jobs.min(items.len().max(1)) {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(item) = items.get(index) else {
                        break;
                    };
                    let result = operation(index, item);
                    if let Ok(mut locked) = results.lock()
                        && let Some(slot) = locked.get_mut(index)
                    {
                        *slot = Some(result);
                    }
                }
            });
        }
    });
    results
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .into_iter()
        .enumerate()
        .map(|(index, result)| {
            result.unwrap_or_else(|| Err(anyhow::anyhow!("worker did not produce result {index}")))
        })
        .collect()
}

fn validate_jobs(jobs: usize) -> Result<()> {
    if jobs == 0 {
        bail!("--jobs must be positive");
    }
    Ok(())
}

fn collection_prompt(question: &str) -> String {
    format!("{COLLECTION_PROMPT_PREFIX}{question}{COLLECTION_PROMPT_SUFFIX}")
}

fn collection_protocol_sha256() -> String {
    sha256_hex(
        format!(
            "{COLLECTION_PROTOCOL_VERSION}\0{COLLECTION_PROMPT_PREFIX}\0{COLLECTION_PROMPT_SUFFIX}"
        )
        .as_bytes(),
    )
}

fn manifest_inventory_sha256(dir: &Path) -> Result<String> {
    let path = dir.join("manifest.json");
    let raw = fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str::<Value>(&raw)
        .context("parsing benchmark manifest")?
        .get("inventory_sha256")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .context("benchmark manifest is missing inventory_sha256")
}

fn live_inventory_sha256(
    coral_bin: &Path,
    workspace: &str,
    config_dir: Option<&Path>,
) -> Result<String> {
    let inventory = catalog_inventory(coral_bin, workspace, config_dir)?;
    validate_inventory(&inventory)?;
    Ok(sha256_hex(&inventory_bytes(&inventory)?))
}

fn refuse_existing_replay(replay_path: &Path, raw_replay_dir: &Path) -> Result<()> {
    if replay_path.exists() || raw_replay_dir.exists() {
        bail!(
            "replay identifier already exists; choose a new --label instead of overwriting {}",
            replay_path.display()
        );
    }
    Ok(())
}

fn claim_run_dir(dir: &Path) -> Result<()> {
    ensure_parent(dir)?;
    match fs::create_dir(dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            bail!(
                "benchmark run directory already exists; choose a new --dir instead of overwriting {}",
                dir.display()
            )
        }
        Err(error) => Err(error)
            .with_context(|| format!("claiming benchmark run directory {}", dir.display())),
    }
}

fn refuse_existing_summary(json_path: &Path, markdown_path: &Path) -> Result<()> {
    if json_path.exists() || markdown_path.exists() {
        bail!(
            "summary already exists; choose a new replay --label instead of overwriting {}",
            json_path.display()
        );
    }
    Ok(())
}

fn validate_corpus_case_ids(corpus: &[CorpusCase]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for case in corpus {
        case.target
            .validate_shape()
            .with_context(|| format!("invalid target in case {}", case.case_id))?;
        let case_id = case.case_id.as_str();
        if case_id.is_empty()
            || !case_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            bail!(
                "case_id {case_id:?} must be one path-safe component containing only letters, numbers, '-' or '_'"
            );
        }
        if !seen.insert(case_id) {
            bail!("duplicate case_id {case_id:?}");
        }
    }
    Ok(())
}

fn corpus_case_digest(case: &CorpusCase) -> Result<String> {
    Ok(sha256_hex(
        &serde_json::to_vec(case).context("serializing corpus case for retry provenance")?,
    ))
}

fn validate_label(label: Option<&str>) -> Result<()> {
    if label.is_some_and(|label| {
        label.is_empty()
            || !label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    }) {
        bail!("--label must contain only letters, numbers, '-' or '_'");
    }
    Ok(())
}

fn artifact_name(stem: &str, label: Option<&str>) -> String {
    label.map_or_else(|| stem.to_string(), |label| format!("{stem}-{label}"))
}

fn artifact_path(dir: &Path, stem: &str, label: Option<&str>, extension: &str) -> PathBuf {
    dir.join(format!("{}.{}", artifact_name(stem, label), extension))
}

fn read_jsonl<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    raw.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            serde_json::from_str(line)
                .with_context(|| format!("parsing {} line {}", path.display(), index + 1))
        })
        .collect()
}

fn read_replay_records(path: &Path) -> Result<Vec<ReplayRecord>> {
    read_jsonl::<Value>(path)?
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            let version = value
                .get("format_version")
                .and_then(Value::as_u64)
                .context("replay record is missing format_version")?;
            if version != u64::from(REPLAY_FORMAT_VERSION) {
                bail!(
                    "{} line {} uses replay format version {}; expected {}",
                    path.display(),
                    index + 1,
                    version,
                    REPLAY_FORMAT_VERSION
                );
            }
            serde_json::from_value(value)
                .with_context(|| format!("parsing {} line {}", path.display(), index + 1))
        })
        .collect()
}

fn write_jsonl<T: Serialize>(path: &Path, values: &[T]) -> Result<()> {
    let mut bytes = Vec::new();
    for value in values {
        serde_json::to_writer(&mut bytes, value).context("serializing JSONL record")?;
        bytes.push(b'\n');
    }
    atomic_write(path, &bytes)
}

fn atomic_write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value).context("serializing JSON")?;
    bytes.push(b'\n');
    atomic_write(path, &bytes)
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    ensure_parent(path)?;
    let temp_path = path.with_extension(format!("tmp-{}-{}", std::process::id(), unix_nanos()?));
    fs::write(&temp_path, bytes).with_context(|| format!("writing {}", temp_path.display()))?;
    fs::rename(&temp_path, path).with_context(|| format!("replacing {}", path.display()))
}

fn ensure_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("{} has no parent", path.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))
}

fn update_manifest(dir: &Path, stage: &str, value: Value) -> Result<()> {
    let path = dir.join("manifest.json");
    let raw = fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let mut manifest = serde_json::from_str::<Value>(&raw).context("parsing benchmark manifest")?;
    let object = manifest
        .as_object_mut()
        .context("benchmark manifest root must be an object")?;
    object.insert(stage.to_string(), value);
    atomic_write_json(&path, &manifest)
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()
        .context("resolving current directory")?
        .join(path))
}

fn ensure_file(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::metadata(path).with_context(|| format!("reading {}", path.display()))?;
    if !metadata.is_file() {
        bail!("{label} is not a file: {}", path.display());
    }
    Ok(())
}

fn ensure_command(path: &Path, label: &str) -> Result<()> {
    let status = Command::new(path)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .with_context(|| format!("running {label} at {}", path.display()))?;
    if !status.success() {
        bail!("{label} --version failed at {}", path.display());
    }
    Ok(())
}

fn command_version(path: &Path) -> Result<String> {
    let output = Command::new(path)
        .arg("--version")
        .output()
        .with_context(|| format!("running {} --version", path.display()))?;
    if !output.status.success() {
        bail!("{} --version failed", path.display());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn set_config_dir(command: &mut Command, config_dir: Option<&Path>) -> Result<()> {
    if let Some(config_dir) = config_dir {
        command.env("CORAL_CONFIG_DIR", absolute_path(config_dir)?);
    }
    Ok(())
}

fn verified_inventory_sha256(
    run_dir: &Path,
    coral_bin: &Path,
    workspace: &str,
    coral_config_dir: Option<&Path>,
) -> Result<String> {
    let prepared = manifest_inventory_sha256(run_dir)?;
    let live = live_inventory_sha256(coral_bin, workspace, coral_config_dir)?;
    if live != prepared {
        bail!(
            "live catalog inventory differs from the prepared benchmark inventory: prepared={prepared}, live={live}"
        );
    }
    Ok(prepared)
}

fn read_error_tail(path: &Path) -> Result<String> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let lines = raw.lines().rev().take(20).collect::<Vec<_>>();
    Ok(lines.into_iter().rev().collect::<Vec<_>>().join("\n"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("{digest:x}")
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("reading {}", path.display()))?;
        if read == 0 {
            break;
        }
        let chunk = buffer
            .get(..read)
            .context("file read exceeded the hash buffer")?;
        digest.update(chunk);
    }
    let digest = digest.finalize();
    Ok(format!("{digest:x}"))
}

fn count_as_f64(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

fn unix_seconds() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_secs())
}

fn unix_nanos() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_nanos())
}

struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn index(&mut self, upper: usize) -> usize {
        debug_assert!(upper > 0);
        let upper_u64 = u64::try_from(upper).unwrap_or(u64::MAX);
        let threshold = upper_u64.wrapping_neg() % upper_u64;
        loop {
            let value = self.next();
            if value >= threshold {
                return usize::try_from(value % upper_u64).unwrap_or(0);
            }
        }
    }
}

fn shuffle<T>(values: &mut [T], rng: &mut SplitMix64) {
    for index in (1..values.len()).rev() {
        values.swap(index, rng.index(index + 1));
    }
}

fn choose<'a, T>(values: &'a [T], rng: &mut SplitMix64) -> Option<&'a T> {
    if values.is_empty() {
        return None;
    }
    values.get(rng.index(values.len()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::process::Command;

    use serde_json::json;

    use super::{
        CollectArgs, ColumnRow, CorpusCase, FunctionRow, Inventory, RankEvaluation, ReplayRecord,
        SearchRun, TableRow, Target, artifact_path, catalog_provider_operational_error,
        claim_run_dir, collection_command, compare_replays, evaluate_response, extract_search_call,
        observed_values_search_flag, read_replay_records, refuse_existing_replay, sample_inventory,
        set_config_dir, summarize, validate_corpus_case_ids, validate_inventory, validate_label,
    };

    #[test]
    fn seeded_sampler_is_stable_and_schema_balanced() {
        let inventory = fixture_inventory();
        let first = sample_inventory(&inventory, 30, 1729).expect("first sample");
        let second = sample_inventory(&inventory, 30, 1729).expect("second sample");
        assert_eq!(
            serde_json::to_string(&first).expect("serialize first"),
            serde_json::to_string(&second).expect("serialize second")
        );
        let mut counts = BTreeMap::<&str, usize>::new();
        for sample in &first {
            *counts.entry(&sample.target.schema_name).or_default() += 1;
            assert!(
                inventory.tables.iter().any(|table| {
                    table.schema_name == sample.target.schema_name
                        && table.table_name == sample.target.surface_name
                }) || inventory.functions.iter().any(|function| {
                    function.schema_name == sample.target.schema_name
                        && function.function_name == sample.target.surface_name
                })
            );
        }
        assert_eq!(counts.values().min(), counts.values().max());
    }

    #[test]
    fn inventory_rejects_coral_system_surfaces() {
        let mut inventory = fixture_inventory();
        inventory.tables.push(TableRow {
            schema_name: "coral".to_string(),
            table_name: "tables".to_string(),
            description: String::new(),
            guide: String::new(),
            required_filters: String::new(),
        });

        validate_inventory(&inventory).expect_err("system surface must be rejected");
    }

    #[test]
    fn rank_evaluation_reports_target_parent_and_child_separately() {
        let target = Target {
            schema_name: "github".to_string(),
            surface_kind: "table_function".to_string(),
            surface_name: "search_issues".to_string(),
            field_role: Some("table_function_result_column".to_string()),
            field_name: Some("label_names".to_string()),
        };
        let response = json!({
            "results": [
                {
                    "kind": "catalog_metadata",
                    "catalog_metadata": {"item": {
                        "kind": "table_function",
                        "name": "github.search_issues"
                    }, "returns": {"label_names": "Utf8"}}
                },
                {
                    "kind": "column_hint",
                    "column_hint": {
                        "schema_name": "github",
                        "surface_kind": "table_function",
                        "surface_name": "search_issues",
                        "column_name": "label_names",
                        "field_role": "table_function_result_column"
                    }
                }
            ],
            "provider_statuses": [{"coverage": {"has_more": true}}],
            "truncation": {"truncated": true}
        });
        let evaluation = evaluate_response(&target, &response);
        assert_eq!(evaluation.target_rank, Some(1));
        assert_eq!(evaluation.parent_rank, Some(1));
        assert_eq!(evaluation.child_rank, Some(1));
        assert!(evaluation.child_target);
        assert!(evaluation.censored);
    }

    #[test]
    fn child_selection_requires_the_matching_field_role() {
        let target = Target {
            schema_name: "notion".to_string(),
            surface_kind: "table_function".to_string(),
            surface_name: "search_data_source_templates".to_string(),
            field_role: Some("table_function_result_column".to_string()),
            field_name: Some("name".to_string()),
        };
        let response = json!({"results": [{
            "kind": "catalog_metadata",
            "catalog_metadata": {
                "item": {
                    "kind": "table_function",
                    "name": "notion.search_data_source_templates"
                },
                "arguments": ["name"],
                "required_arguments": ["name"]
            }
        }]});

        let evaluation = evaluate_response(&target, &response);

        assert_eq!(evaluation.parent_rank, Some(1));
        assert_eq!(evaluation.target_rank, None);
        assert_eq!(evaluation.child_rank, None);
    }

    #[test]
    fn child_selection_accepts_argument_name_arrays() {
        let target = Target {
            schema_name: "notion".to_string(),
            surface_kind: "table_function".to_string(),
            surface_name: "search_data_source_templates".to_string(),
            field_role: Some("table_function_argument".to_string()),
            field_name: Some("name".to_string()),
        };
        let response = json!({"results": [{
            "kind": "catalog_metadata",
            "catalog_metadata": {
                "item": {
                    "kind": "table_function",
                    "name": "notion.search_data_source_templates"
                },
                "arguments": ["name"],
                "required_arguments": ["name"]
            }
        }]});

        let evaluation = evaluate_response(&target, &response);

        assert_eq!(evaluation.parent_rank, Some(1));
        assert_eq!(evaluation.target_rank, Some(1));
        assert_eq!(evaluation.child_rank, Some(1));
    }

    #[test]
    fn parent_target_uses_the_same_rank_for_target_and_parent() {
        let target = Target {
            schema_name: "github".to_string(),
            surface_kind: "table".to_string(),
            surface_name: "issues".to_string(),
            field_role: None,
            field_name: None,
        };
        let response = json!({"results": [{
            "kind": "catalog_metadata",
            "catalog_metadata": {"item": {
                "kind": "table",
                "name": "github.issues"
            }}
        }]});

        let evaluation = evaluate_response(&target, &response);

        assert_eq!(evaluation.target_rank, Some(1));
        assert_eq!(evaluation.parent_rank, Some(1));
        assert_eq!(evaluation.child_rank, None);
        assert!(!evaluation.child_target);
    }

    #[test]
    fn static_parent_schema_does_not_count_as_child_selection() {
        let target = Target {
            schema_name: "datadog".to_string(),
            surface_kind: "table".to_string(),
            surface_name: "users".to_string(),
            field_role: Some("table_column".to_string()),
            field_name: Some("name".to_string()),
        };
        let response = json!({"results": [{
            "kind": "catalog_metadata",
            "catalog_metadata": {
                "item": {
                    "kind": "table",
                    "name": "datadog.users"
                },
                "table_column_preview": {"columns": [{"column_name": "name"}]}
            }
        }]});

        let evaluation = evaluate_response(&target, &response);

        assert_eq!(evaluation.parent_rank, Some(1));
        assert_eq!(evaluation.target_rank, None);
        assert_eq!(evaluation.child_rank, None);
    }

    #[test]
    fn summary_keeps_target_and_parent_metrics_separate() {
        let run = SearchRun {
            limit: 10,
            response: Some(json!({"results": [{"kind": "catalog_metadata"}]})),
            evaluation: Some(RankEvaluation {
                target_rank: Some(8),
                parent_rank: Some(2),
                child_rank: Some(8),
                child_target: true,
                censored: false,
            }),
            error: None,
            response_token_count: None,
            elapsed_millis: 0,
        };

        let summary = summarize([&run].into_iter());

        assert_eq!(summary.target_hit_at_3, 0);
        assert_eq!(summary.parent_hit_at_3, 1);
        assert_eq!(summary.child_targets, 1);
        assert_eq!(summary.child_parent_hit_at_10, 1);
        assert_eq!(summary.child_hit_at_10, 1);
        assert_eq!(summary.child_hit_with_parent_at_10, 1);
        assert!((summary.parent_mean_reciprocal_rank - 0.5).abs() < f64::EPSILON);
        assert_eq!(summary.tokenized_responses, 1);
        assert!(summary.mean_response_tokens > 0.0);
        assert_eq!(summary.p95_response_tokens, summary.max_response_tokens);
    }

    #[test]
    fn replay_feature_flag_is_explicit_in_both_modes() {
        assert_eq!(
            observed_values_search_flag(true),
            "--enable-observed-values-search"
        );
        assert_eq!(
            observed_values_search_flag(false),
            "--disable-observed-values-search"
        );
    }

    #[test]
    fn summary_reports_observed_value_result_slots_and_best_rank() {
        let run = SearchRun {
            limit: 10,
            response: Some(json!({
                "provider_statuses": [{
                    "provider": "observed_values",
                    "state": "results_found",
                    "coverage": {
                        "eligible_units": 42
                    }
                }],
                "results": [
                    {"kind": "catalog_metadata"},
                    {"kind": "observed_value"},
                    {"kind": "catalog_metadata"},
                    {"kind": "observed_value"}
                ]
            })),
            evaluation: Some(RankEvaluation {
                target_rank: Some(1),
                parent_rank: Some(1),
                child_rank: None,
                child_target: false,
                censored: false,
            }),
            error: None,
            response_token_count: None,
            elapsed_millis: 0,
        };

        let summary = summarize([&run].into_iter());

        assert_eq!(
            summary.observed_value_provider_states.get("results_found"),
            Some(&1)
        );
        assert_eq!(summary.observed_value_max_eligible_units, 42);
        assert_eq!(summary.observed_value_cases, 1);
        assert_eq!(summary.observed_value_results, 2);
        assert_eq!(summary.best_observed_value_rank, Some(2));
    }

    #[test]
    fn paired_comparison_classifies_exact_target_rank_changes() {
        let baseline = vec![
            fixture_replay_record("new", None),
            fixture_replay_record("lost", Some(2)),
            fixture_replay_record("improved", Some(8)),
            fixture_replay_record("regressed", Some(3)),
            fixture_replay_record("unchanged", Some(5)),
        ];
        let candidate = vec![
            fixture_replay_record("new", Some(9)),
            fixture_replay_record("lost", None),
            fixture_replay_record("improved", Some(4)),
            fixture_replay_record("regressed", Some(7)),
            fixture_replay_record("unchanged", Some(5)),
        ];

        let comparison =
            compare_replays("baseline", "candidate", &baseline, &candidate).expect("comparison");

        assert_eq!(comparison.new_hits_at_10, 1);
        assert_eq!(comparison.lost_hits_at_10, 1);
        assert_eq!(comparison.rank_improvements, 1);
        assert_eq!(comparison.rank_regressions, 1);
        assert_eq!(comparison.unchanged, 1);
        assert_eq!(comparison.unscored_cases, 0);
    }

    #[test]
    fn focused_artifacts_are_named_and_validated() {
        assert_eq!(
            artifact_path(
                std::path::Path::new("run"),
                "replay",
                Some("search-regressions"),
                "jsonl"
            ),
            std::path::Path::new("run/replay-search-regressions.jsonl")
        );
        validate_label(Some("search-regressions")).expect("valid label");
        validate_label(Some("../baseline")).expect_err("invalid label");
    }

    #[test]
    fn corpus_case_ids_cannot_escape_the_run_directory() {
        let mut case = fixture_corpus_case("safe-case");
        validate_corpus_case_ids(&[case.clone()]).expect("safe case ID");

        case.case_id = "../outside".to_string();
        validate_corpus_case_ids(&[case]).expect_err("path traversal must fail");
    }

    #[test]
    fn legacy_split_and_acceptable_targets_do_not_affect_the_case() {
        let case = serde_json::from_value::<CorpusCase>(json!({
            "format_version": 1,
            "case_id": "legacy-case",
            "sample_id": "sample",
            "split": "holdout",
            "style": "natural",
            "question": "Which issues are blocked?",
            "rationale": "Find issue relations",
            "target": {
                "schema_name": "linear",
                "surface_kind": "table",
                "surface_name": "issue_relations"
            },
            "acceptable_targets": [{
                "schema_name": "linear",
                "surface_kind": "table",
                "surface_name": "issues"
            }]
        }))
        .expect("legacy corpus case");

        let serialized = serde_json::to_value(case).expect("serialize corpus case");

        assert!(serialized.get("split").is_none());
        assert!(serialized.get("acceptable_targets").is_none());
    }

    #[test]
    fn replay_reader_rejects_old_formats_before_decoding_the_payload() {
        let path = std::env::temp_dir().join(format!(
            "coral-search-bench-old-replay-{}-{}.jsonl",
            std::process::id(),
            super::unix_nanos().expect("clock")
        ));
        std::fs::write(&path, b"{\"format_version\":1}\n").expect("write old replay");

        let error = read_replay_records(&path).expect_err("old replay must fail clearly");
        std::fs::remove_file(&path).expect("remove replay fixture");

        assert!(
            error
                .to_string()
                .contains("uses replay format version 1; expected 3")
        );
    }

    #[test]
    fn config_dir_is_absolute_for_child_processes() {
        let mut command = Command::new("coral");
        set_config_dir(&mut command, Some(Path::new("relative-config")))
            .expect("set config directory");

        let config_dir = command
            .get_envs()
            .find_map(|(name, value)| (name == "CORAL_CONFIG_DIR").then_some(value))
            .flatten()
            .expect("CORAL_CONFIG_DIR");
        assert!(Path::new(config_dir).is_absolute());
    }

    #[test]
    fn collection_passes_config_dir_to_the_mcp_server() {
        let args = CollectArgs {
            dir: "run".into(),
            coral_bin: "coral".into(),
            codex_bin: "codex".into(),
            model: "gpt-5.6-luna".to_string(),
            workspace: "default".to_string(),
            coral_config_dir: Some("relative-config".into()),
            jobs: 1,
            timeout_seconds: 120,
            retry_failed: false,
        };
        let command = collection_command(&args, Path::new("/coral"), Path::new("/work"))
            .expect("collection command");

        assert!(command.get_args().any(|arg| {
            arg.to_string_lossy()
                .starts_with("mcp_servers.coral.env.CORAL_CONFIG_DIR=\"")
        }));
    }

    #[test]
    fn replay_identifier_cannot_overwrite_existing_evidence() {
        let replay_path = std::env::temp_dir().join(format!(
            "coral-search-bench-replay-{}-{}.jsonl",
            std::process::id(),
            super::unix_nanos().expect("clock")
        ));
        std::fs::write(&replay_path, b"evidence").expect("write replay fixture");

        let result = refuse_existing_replay(
            &replay_path,
            std::path::Path::new("definitely-missing-raw-replay-dir"),
        );
        std::fs::remove_file(&replay_path).expect("remove replay fixture");

        assert!(result.is_err());
    }

    #[test]
    fn prepare_cannot_overwrite_existing_run_evidence() {
        let run_dir = std::env::temp_dir().join(format!(
            "coral-search-bench-prepare-{}-{}",
            std::process::id(),
            super::unix_nanos().expect("clock")
        ));
        std::fs::create_dir(&run_dir).expect("create run directory");
        std::fs::write(run_dir.join("manifest.json"), b"existing provenance")
            .expect("write manifest fixture");

        let result = claim_run_dir(&run_dir);
        std::fs::remove_dir_all(&run_dir).expect("remove run directory");

        assert!(result.is_err());
    }

    #[test]
    fn collection_protocol_rejects_additional_tools() {
        let search = json!({
            "type": "item.completed",
            "item": {
                "id": "search",
                "type": "mcp_tool_call",
                "server": "coral",
                "tool": "search",
                "arguments": {"query": "issues", "limit": 10},
                "result": {"structured_content": {"results": []}},
                "status": "completed"
            }
        });
        let shell = json!({
            "type": "item.completed",
            "item": {"id": "shell", "type": "command_execution", "status": "completed"}
        });
        let events = format!("{search}\n{shell}\n");

        assert!(extract_search_call(&events).is_err());
    }

    #[test]
    fn provider_truncation_is_scoreable_but_operational_failure_is_not() {
        let partial = json!({
            "provider_statuses": [{
                "provider": "catalog_metadata",
                "state": "partial",
                "coverage": {
                    "failed_units": 0,
                    "has_more": true,
                    "timed_out": false,
                    "budget_exhausted": false,
                    "stale_index": false
                }
            }]
        });
        assert!(catalog_provider_operational_error(&partial).is_none());

        let failed = json!({
            "provider_statuses": [{
                "provider": "catalog_metadata",
                "state": "partial",
                "coverage": {"failed_units": 1}
            }]
        });
        assert!(catalog_provider_operational_error(&failed).is_some());
    }

    fn fixture_corpus_case(case_id: &str) -> CorpusCase {
        CorpusCase {
            format_version: 1,
            case_id: case_id.to_string(),
            sample_id: "sample".to_string(),
            style: "natural".to_string(),
            question: "Which issues are blocked?".to_string(),
            rationale: "Find issue relations".to_string(),
            target: Target {
                schema_name: "linear".to_string(),
                surface_kind: "table".to_string(),
                surface_name: "issue_relations".to_string(),
                field_role: None,
                field_name: None,
            },
            frozen_query: None,
        }
    }

    fn fixture_replay_record(case_id: &str, target_rank: Option<usize>) -> ReplayRecord {
        let evaluation = RankEvaluation {
            target_rank,
            parent_rank: target_rank,
            child_rank: None,
            child_target: false,
            censored: false,
        };
        let run = SearchRun {
            limit: 10,
            response: Some(json!({"results": []})),
            evaluation: Some(evaluation),
            error: None,
            response_token_count: Some(1),
            elapsed_millis: 0,
        };
        ReplayRecord {
            format_version: super::REPLAY_FORMAT_VERSION,
            case_id: case_id.to_string(),
            sample_id: "sample".to_string(),
            style: "natural".to_string(),
            question: "Which issues are blocked?".to_string(),
            query: "blocked issues".to_string(),
            target: Target {
                schema_name: "linear".to_string(),
                surface_kind: "table".to_string(),
                surface_name: "issue_relations".to_string(),
                field_role: None,
                field_name: None,
            },
            limit_10: run.clone(),
            limit_50: run,
        }
    }

    fn fixture_inventory() -> Inventory {
        Inventory {
            tables: vec![
                TableRow {
                    schema_name: "tables_only".to_string(),
                    table_name: "events".to_string(),
                    description: "Events".to_string(),
                    guide: String::new(),
                    required_filters: "id".to_string(),
                },
                TableRow {
                    schema_name: "mixed".to_string(),
                    table_name: "issues".to_string(),
                    description: "Issues".to_string(),
                    guide: String::new(),
                    required_filters: String::new(),
                },
            ],
            columns: vec![
                ColumnRow {
                    schema_name: "tables_only".to_string(),
                    table_name: "events".to_string(),
                    ordinal_position: 0,
                    column_name: "id".to_string(),
                    data_type: "Utf8".to_string(),
                    is_nullable: false,
                    is_virtual: false,
                    is_required_filter: true,
                    description: "Event ID".to_string(),
                },
                ColumnRow {
                    schema_name: "mixed".to_string(),
                    table_name: "issues".to_string(),
                    ordinal_position: 0,
                    column_name: "title".to_string(),
                    data_type: "Utf8".to_string(),
                    is_nullable: false,
                    is_virtual: false,
                    is_required_filter: false,
                    description: "Issue title".to_string(),
                },
            ],
            functions: vec![
                FunctionRow {
                    schema_name: "functions_only".to_string(),
                    function_name: "search".to_string(),
                    description: "Search".to_string(),
                    arguments_json: "[]".to_string(),
                    result_columns_json: "[]".to_string(),
                    kind: "search".to_string(),
                    search_limits_json: None,
                },
                FunctionRow {
                    schema_name: "mixed".to_string(),
                    function_name: "find".to_string(),
                    description: "Find".to_string(),
                    arguments_json:
                        "[{\"name\":\"q\",\"required\":true,\"values\":[]}]".to_string(),
                    result_columns_json: "[{\"name\":\"id\",\"type\":\"Utf8\",\"nullable\":false,\"description\":\"ID\"}]".to_string(),
                    kind: "search".to_string(),
                    search_limits_json: None,
                },
            ],
        }
    }
}
