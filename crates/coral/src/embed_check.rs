//! `coral embed-check`, hidden: loads the embedding model and times query
//! embedding, so you can measure a platform with the shipped binary (#3051).

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use coral_embed::{Embed as _, MANIFEST, Model};

/// Queries embedded before timing starts.
const WARMUP: usize = 3;

/// Short queries in the bench's style, embedded in turn.
const QUERIES: [&str; 8] = [
    "add public SSH key to GitHub account",
    "list the open pull requests of a repository",
    "cancel running custom assistant session",
    "damaged order refund customer response",
    "send a message to a Slack channel",
    "who is on call this week",
    "create a calendar event with a video call link",
    "get the latest deployment status",
];

/// `coral embed-check`'s flags.
#[derive(Debug, clap::Args)]
pub(crate) struct EmbedCheckArgs {
    /// The folder that holds the model's files under the manifest's names.
    #[arg(long)]
    model_dir: PathBuf,
    /// Timed query embeddings.
    #[arg(long, default_value_t = 200, value_parser = clap::value_parser!(u32).range(1..))]
    runs: u32,
}

/// Loads the model from `--model-dir` and prints the load time and the
/// query embedding time.
pub(crate) fn run(args: &EmbedCheckArgs) -> anyhow::Result<()> {
    println!("model {}", MANIFEST.model_id);
    println!("embedder_revision {}", MANIFEST.embedder_revision());
    let started = Instant::now();
    let model = Model::load(&args.model_dir)
        .with_context(|| format!("load the model from {}", args.model_dir.display()))?;
    println!("load_ms {:.1}", millis(started.elapsed()));

    let started = Instant::now();
    let vector = model.embed_query(QUERIES[0])?;
    println!("first_query_ms {:.1}", millis(started.elapsed()));
    let norm = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    println!("dimension {} norm {norm:.6}", vector.len());

    for query in QUERIES.iter().cycle().take(WARMUP) {
        model.embed_query(query)?;
    }
    let mut times = Vec::new();
    for query in QUERIES.iter().cycle().take(usize::try_from(args.runs)?) {
        let started = Instant::now();
        model.embed_query(query)?;
        times.push(millis(started.elapsed()));
    }
    times.sort_by(f64::total_cmp);
    println!(
        "query_ms p50 {:.2} p99 {:.2} max {:.2} runs {}",
        percentile(&times, 50),
        percentile(&times, 99),
        percentile(&times, 100),
        times.len()
    );
    Ok(())
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

/// The nearest-rank percentile of sorted values. `--runs` is at least 1, so
/// the list is never empty.
fn percentile(sorted: &[f64], percent: usize) -> f64 {
    let rank = (sorted.len() * percent).div_ceil(100).max(1);
    *sorted
        .get(rank - 1)
        .expect("a rank in 1..=len of a list that is not empty")
}
