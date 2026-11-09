//! SPIKE (coral#2867), throwaway, not product code.
//!
//! Embeds texts with `BAAI/bge-small-en-v1.5` (the author's fp32
//! `onnx/model.onnx`) through `ort` and times it, to check that the pick of
//! coral#2865 holds its Python numbers in Rust. It copies fastembed 0.9.1:
//! truncation at 512 tokens on the right, special tokens added, zero token
//! types, CLS pooling, L2 normalization. One text per `run` call.
//!
//! `coral embed-spike --model-dir <dir> --out <dir> [--threads N]
//!   [--docs <jsonl>] [--timing-queries <jsonl>] [--queries <jsonl>]`
//!
//! Each input line is one JSON string, the text exactly as embedded (prefix
//! included). Writes `<name>.f32` (row-major little-endian f32, 384 per row)
//! per input and `report.json`.

#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    unsafe_code,
    reason = "spike: operator output and getrusage"
)]

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context as _, bail};
use ort::session::Session;
use ort::session::builder::GraphOptimizationLevel;
use ort::value::Tensor;
use serde_json::{Value, json};
use tokenizers::{Tokenizer, TruncationDirection, TruncationParams, TruncationStrategy};

const DIM: usize = 384;
const MAX_TOKENS: usize = 512;
const WARMUP: usize = 3;

struct Args {
    model_dir: PathBuf,
    out: PathBuf,
    threads: usize,
    ep: String,
    docs: Option<PathBuf>,
    timing_queries: Option<PathBuf>,
    queries: Option<PathBuf>,
}

fn parse(args: &[String]) -> anyhow::Result<Args> {
    let mut model_dir = None;
    let mut out = None;
    let mut threads = 1;
    let mut ep = String::from("prefer-cpu");
    let mut docs = None;
    let mut timing_queries = None;
    let mut queries = None;
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let value = it.next().with_context(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--model-dir" => model_dir = Some(PathBuf::from(value)),
            "--out" => out = Some(PathBuf::from(value)),
            "--threads" => threads = value.parse().context("--threads")?,
            "--ep" => ep.clone_from(value),
            "--docs" => docs = Some(PathBuf::from(value)),
            "--timing-queries" => timing_queries = Some(PathBuf::from(value)),
            "--queries" => queries = Some(PathBuf::from(value)),
            other => bail!("unknown flag {other}"),
        }
    }
    Ok(Args {
        model_dir: model_dir.context("--model-dir is required")?,
        out: out.context("--out is required")?,
        threads,
        ep,
        docs,
        timing_queries,
        queries,
    })
}

/// Peak resident set of this process in MB.
fn peak_rss_mb() -> f64 {
    // SAFETY: getrusage writes into the zeroed struct we own.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &raw mut usage) };
    #[allow(clippy::cast_precision_loss, reason = "MB for a report")]
    let raw = usage.ru_maxrss as f64;
    if cfg!(target_os = "macos") {
        raw / 1024.0 / 1024.0
    } else {
        raw / 1024.0
    }
}

/// Current resident set in MB (Linux only).
fn rss_mb() -> Option<f64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kb: f64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb / 1024.0)
}

fn read_texts(path: &Path) -> anyhow::Result<Vec<String>> {
    let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    text.lines()
        .map(|line| serde_json::from_str::<String>(line).context("a line is not a JSON string"))
        .collect()
}

struct Embedder {
    tokenizer: Tokenizer,
    session: Session,
    token_types: bool,
}

impl Embedder {
    fn load(model_dir: &Path, threads: usize, ep: &str) -> anyhow::Result<Self> {
        let mut tokenizer = Tokenizer::from_file(model_dir.join("tokenizer.json"))
            .map_err(|e| anyhow::anyhow!("tokenizer: {e}"))?;
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: MAX_TOKENS,
                strategy: TruncationStrategy::LongestFirst,
                stride: 0,
                direction: TruncationDirection::Right,
            }))
            .map_err(|e| anyhow::anyhow!("truncation: {e}"))?;
        tokenizer.with_padding(None);
        let environment = ort::init().with_name("embed-spike").build()?;
        // `ort` rc.15 sets the automatic EP policy MaxEfficiency on every new
        // builder; "cpu" registers the CPU EP explicitly, as fastembed does.
        let builder = Session::builder(&environment)?;
        let builder = match ep {
            "cpu" => builder
                .with_execution_providers([ort::ep::CPU::default().build()])
                .map_err(|e| anyhow::anyhow!("{e}"))?,
            "prefer-cpu" => builder
                .with_auto_device(ort::session::builder::AutoDevicePolicy::PreferCPU)
                .map_err(|e| anyhow::anyhow!("{e}"))?,
            "max-efficiency" => builder,
            other => bail!("unknown --ep {other}"),
        };
        let session = builder
            .with_optimization_level(GraphOptimizationLevel::All)
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .with_intra_threads(threads)
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .commit_from_file(model_dir.join("onnx").join("model.onnx"))?;
        let token_types = session.inputs().iter().any(|i| i.name() == "token_type_ids");
        Ok(Self {
            tokenizer,
            session,
            token_types,
        })
    }

    /// One text, one `run`: returns the unit vector and the token count.
    fn embed(&mut self, text: &str) -> anyhow::Result<([f32; DIM], usize)> {
        let encoding = self
            .tokenizer
            .encode(text, true)
            .map_err(|e| anyhow::anyhow!("encode: {e}"))?;
        let ids: Vec<i64> = encoding.get_ids().iter().map(|&x| i64::from(x)).collect();
        let mask: Vec<i64> = encoding
            .get_attention_mask()
            .iter()
            .map(|&x| i64::from(x))
            .collect();
        let n = ids.len();
        let shape = [1_usize, n];
        let ids = Tensor::from_array((shape, ids))?;
        let mask = Tensor::from_array((shape, mask))?;
        let outputs = if self.token_types {
            let types = Tensor::from_array((shape, vec![0_i64; n]))?;
            self.session.run(ort::inputs![
                "input_ids" => ids,
                "attention_mask" => mask,
                "token_type_ids" => types,
            ])?
        } else {
            self.session.run(ort::inputs![
                "input_ids" => ids,
                "attention_mask" => mask,
            ])?
        };
        let (shape, data) = outputs[0].try_extract_tensor::<f32>()?;
        if shape.len() != 3 || shape[2] != i64::try_from(DIM)? {
            bail!("unexpected output shape {shape:?}");
        }
        // CLS pooling: token 0 of the one row.
        let mut v = [0.0_f32; DIM];
        v.copy_from_slice(&data[..DIM]);
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in &mut v {
                *x /= norm;
            }
        }
        Ok((v, n))
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Linear-interpolated percentile of sorted values, as the harness computes it.
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        reason = "index of a short list"
    )]
    {
        let k = (sorted.len() - 1) as f64 * p / 100.0;
        let lo = k.floor() as usize;
        let hi = k.ceil() as usize;
        sorted[lo] + (sorted[hi] - sorted[lo]) * (k - lo as f64)
    }
}

fn write_vectors(path: &Path, vectors: &[[f32; DIM]]) -> anyhow::Result<()> {
    let mut file = fs::File::create(path)?;
    let mut bytes = Vec::with_capacity(vectors.len() * DIM * 4);
    for v in vectors {
        for x in v {
            bytes.extend_from_slice(&x.to_le_bytes());
        }
    }
    file.write_all(&bytes)?;
    Ok(())
}

/// Embeds every text once, one per call, timing each call.
fn pass(embedder: &mut Embedder, texts: &[String]) -> anyhow::Result<(Vec<[f32; DIM]>, Value)> {
    let mut vectors = Vec::with_capacity(texts.len());
    let mut per_row = Vec::with_capacity(texts.len());
    let mut tokens = 0_usize;
    let started = Instant::now();
    for (i, text) in texts.iter().enumerate() {
        let t = Instant::now();
        let (v, n) = embedder.embed(text)?;
        per_row.push(ms(t.elapsed()));
        tokens += n;
        vectors.push(v);
        if (i + 1) % 2000 == 0 {
            eprintln!(
                "  {} rows, {:.1} s",
                i + 1,
                started.elapsed().as_secs_f64()
            );
        }
    }
    let seconds = started.elapsed().as_secs_f64();
    per_row.sort_by(f64::total_cmp);
    #[allow(clippy::cast_precision_loss, reason = "report")]
    let rows = texts.len() as f64;
    #[allow(clippy::cast_precision_loss, reason = "report")]
    let tokens_f = tokens as f64;
    Ok((
        vectors,
        json!({
            "rows": texts.len(),
            "seconds": seconds,
            "rows_per_s": rows / seconds,
            "minutes": seconds / 60.0,
            "tokens": tokens,
            "tokens_per_s": tokens_f / seconds,
            "row_ms_p50": percentile(&per_row, 50.0),
            "row_ms_p99": percentile(&per_row, 99.0),
            "row_ms_max": per_row.last().copied().unwrap_or(f64::NAN),
        }),
    ))
}

/// Runs the spike with the arguments after `embed-spike`.
///
/// # Errors
/// Any failure to read inputs, load the model, embed or write outputs.
pub fn run(args: &[String]) -> anyhow::Result<()> {
    let args = parse(args)?;
    fs::create_dir_all(&args.out)?;
    let mut report = serde_json::Map::new();
    report.insert("threads".into(), json!(args.threads));
    report.insert("ep".into(), json!(args.ep));
    report.insert("os".into(), json!(std::env::consts::OS));
    report.insert("arch".into(), json!(std::env::consts::ARCH));
    report.insert("ort_crate".into(), json!("2.0.0-rc.15"));
    report.insert("rss_before_model_mb".into(), json!(rss_mb()));
    report.insert("peak_rss_before_model_mb".into(), json!(peak_rss_mb()));

    let started = Instant::now();
    let mut embedder = Embedder::load(&args.model_dir, args.threads, &args.ep)?;
    let load = started.elapsed();
    report.insert("load_s".into(), json!(load.as_secs_f64()));
    report.insert("ort_build_info".into(), json!(ort::info()));
    report.insert("rss_after_load_mb".into(), json!(rss_mb()));
    report.insert("peak_rss_after_load_mb".into(), json!(peak_rss_mb()));
    let t = Instant::now();
    embedder.embed("List the open pull requests of a repository.")?;
    report.insert("first_call_s".into(), json!(t.elapsed().as_secs_f64()));
    eprintln!("load {:.3} s", load.as_secs_f64());

    if let Some(path) = &args.timing_queries {
        let texts = read_texts(path)?;
        for text in texts.iter().take(WARMUP) {
            embedder.embed(text)?;
        }
        let mut latencies = Vec::with_capacity(texts.len());
        let mut vectors = Vec::with_capacity(texts.len());
        for text in &texts {
            let t = Instant::now();
            let (v, _) = embedder.embed(text)?;
            latencies.push(ms(t.elapsed()));
            vectors.push(v);
        }
        write_vectors(&args.out.join("timing_queries.f32"), &vectors)?;
        latencies.sort_by(f64::total_cmp);
        #[allow(clippy::cast_precision_loss, reason = "report")]
        let mean = latencies.iter().sum::<f64>() / latencies.len() as f64;
        let q = json!({
            "n": latencies.len(),
            "p50_ms": percentile(&latencies, 50.0),
            "p99_ms": percentile(&latencies, 99.0),
            "mean_ms": mean,
        });
        eprintln!("timing queries {q}");
        report.insert("query".into(), q);
    }
    if let Some(path) = &args.docs {
        let texts = read_texts(path)?;
        eprintln!("docs: {} rows", texts.len());
        let (vectors, stats) = pass(&mut embedder, &texts)?;
        write_vectors(&args.out.join("docs.f32"), &vectors)?;
        eprintln!("docs {stats}");
        report.insert("index".into(), stats);
    }
    if let Some(path) = &args.queries {
        let texts = read_texts(path)?;
        let (vectors, stats) = pass(&mut embedder, &texts)?;
        write_vectors(&args.out.join("queries.f32"), &vectors)?;
        report.insert("queries_pass".into(), stats);
    }
    report.insert("peak_rss_mb".into(), json!(peak_rss_mb()));
    report.insert("rss_end_mb".into(), json!(rss_mb()));
    let report = Value::Object(report);
    fs::write(
        args.out.join("report.json"),
        serde_json::to_string_pretty(&report)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
