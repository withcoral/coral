//! PROTOTYPE (#2538 entry size spike). Throwaway code: never merge.
//!
//! Question: how much memory does one cache entry of #2538 take (parsed graph,
//! TypeScript names, render cache), and what does each part cost to build?
//!
//! Reads each Generation's pinned source document from `<dir>/<generation
//! id>.input` (kind in `.kind`) and materializes it with this build's code, so
//! the graph is the one main would store. `<id>.json`, the graph an older
//! build stored, is only weighed for comparison.
//!
//! ```sh
//! cargo run --release -p coral-catalog \
//!   --example prototype_2538_entry_size -- <dir> <generation id>...
//! ```

use std::alloc::System;
use std::time::{Duration, Instant};

use cap::Cap;
use coral_catalog::OperationAvailability;
use coral_catalog::prototype_2538::{
    declares_input, materialize_stored, names, parse_graph, validate_graph,
};

#[global_allocator]
static ALLOCATOR: Cap<System> = Cap::new(System, usize::MAX);

/// Operations timed one by one, cold and warm.
const SAMPLE: usize = 200;

fn heap() -> usize {
    ALLOCATOR.allocated()
}

fn rss_bytes() -> usize {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps");
    String::from_utf8(out.stdout)
        .expect("ps output")
        .trim()
        .parse::<usize>()
        .expect("rss")
        * 1024
}

fn mb(bytes: usize) -> String {
    format!("{:.1}", bytes as f64 / 1_000_000.0)
}

fn ms(duration: Duration) -> String {
    format!("{:.1}", duration.as_secs_f64() * 1000.0)
}

fn us(duration: Duration) -> String {
    format!("{:.0}", duration.as_secs_f64() * 1_000_000.0)
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let index = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[index]
}

struct Row {
    id: String,
    stored_json_bytes: u64,
    materialize: Duration,
    json_bytes: usize,
    operations: usize,
    parse: Duration,
    validate: Duration,
    graph_bytes: usize,
    names_time: Duration,
    names_bytes: usize,
    project_all: Duration,
    failed: usize,
    renders: usize,
    renders_bytes: usize,
    cold: Vec<Duration>,
    warm: Vec<Duration>,
    drop_time: Duration,
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let first = args.next().expect("graph dir or --rss");
    if first == "--rss" {
        // A server's view: load one OpenAPI graph JSON and name it, nothing else.
        let path = args.next().expect("graph JSON path");
        let rss_before = rss_bytes();
        let heap_before = heap();
        let json = std::fs::read_to_string(path).expect("graph JSON");
        let graph = parse_graph(&json);
        validate_graph(&graph);
        let names = coral_catalog::prototype_2538::openapi_names(&graph);
        drop(json);
        println!(
            "graph + names: heap {} MB; RSS {} MB → {} MB (+{} MB)",
            mb(heap() - heap_before),
            mb(rss_before),
            mb(rss_bytes()),
            mb(rss_bytes().saturating_sub(rss_before)),
        );
        drop(names);
        drop(graph);
        return;
    }
    let dir = std::path::PathBuf::from(first);
    let ids = args.collect::<Vec<_>>();

    let rss_start = rss_bytes();
    let heap_start = heap();
    let mut rows = Vec::new();
    // Keep every entry alive to see the total for several Generations.
    let mut kept = Vec::new();
    for id in &ids {
        let read = |extension: &str| {
            let text = std::fs::read_to_string(dir.join(format!("{id}.{extension}")))
                .expect("input file");
            // psql adds one newline after the value.
            text.strip_suffix('\n').map(str::to_string).unwrap_or(text)
        };
        let started = Instant::now();
        let (graph_json, operations) = materialize_stored(&read("kind"), &read("input"), &read("config"));
        let materialize = started.elapsed();
        let available = operations
            .iter()
            .filter(|operation| operation.availability() == OperationAvailability::Available)
            .collect::<Vec<_>>();

        let before_graph = heap();
        if let Ok(dump) = std::env::var("PROTO_DUMP_DIR") {
            std::fs::write(format!("{dump}/{id}.main.json"), &graph_json).expect("dump");
        }
        let json = graph_json.clone();
        let json_bytes = json.len();
        let started = Instant::now();
        let graph = parse_graph(&json);
        let parse = started.elapsed();
        drop(json);
        let graph_bytes = heap() - before_graph;
        let started = Instant::now();
        validate_graph(&graph);
        let validate = started.elapsed();

        let before_names = heap();
        let started = Instant::now();
        let names = names(&graph, available[0]);
        let names_time = started.elapsed();
        let names_bytes = heap() - before_names;

        let step = (available.len() / SAMPLE).max(1);
        let sample = available.iter().step_by(step).collect::<Vec<_>>();
        let mut cold = sample
            .iter()
            .map(|operation| {
                names.clear_renders();
                let started = Instant::now();
                let _ = declares_input(&graph, &names, operation);
                started.elapsed()
            })
            .collect::<Vec<_>>();
        cold.sort();

        names.clear_renders();
        let before_renders = heap();
        let started = Instant::now();
        let failed = available
            .iter()
            .filter(|operation| declares_input(&graph, &names, operation).is_none())
            .count();
        let project_all = started.elapsed();
        let renders_bytes = heap() - before_renders;
        let renders = names.render_count();

        let mut warm = sample
            .iter()
            .map(|operation| {
                let started = Instant::now();
                let _ = declares_input(&graph, &names, operation);
                started.elapsed()
            })
            .collect::<Vec<_>>();
        warm.sort();

        // Time one drop of a fresh, cold entry, as each request pays today.
        let drop_time = {
            let graph = parse_graph(&graph_json);
            let names = coral_catalog::prototype_2538::names(&graph, available[0]);
            let started = Instant::now();
            drop(names);
            drop(graph);
            started.elapsed()
        };

        rows.push(Row {
            id: id.clone(),
            stored_json_bytes: std::fs::metadata(dir.join(format!("{id}.json")))
                .map_or(0, |metadata| metadata.len()),
            materialize,
            json_bytes,
            operations: available.len(),
            parse,
            validate,
            graph_bytes,
            names_time,
            names_bytes,
            project_all,
            failed,
            renders,
            renders_bytes,
            cold,
            warm,
            drop_time,
        });
        drop(operations);
        drop(graph_json);
        kept.push((graph, names));
    }
    let heap_all = heap() - heap_start;
    let rss_all = rss_bytes();

    println!(
        "| Generation | Ops | Stored JSON MB (old build) | JSON MB | Graph MB | Names MB | Renders (count) | Renders MB | Entry MB, warm |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|");
    for row in &rows {
        println!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            &row.id[..8],
            row.operations,
            mb(usize::try_from(row.stored_json_bytes).expect("size")),
            mb(row.json_bytes),
            mb(row.graph_bytes),
            mb(row.names_bytes),
            row.renders,
            mb(row.renders_bytes),
            mb(row.graph_bytes + row.names_bytes + row.renders_bytes),
        );
    }
    println!();
    println!(
        "| Generation | Materialize s | Parse ms | Validate ms | Names ms | Drop ms | Project all ms (failed) | Hit cold p50/p90/max µs | Hit warm p50/p90/max µs |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|");
    for row in &rows {
        println!(
            "| {} | {:.1} | {} | {} | {} | {} | {} ({}) | {}/{}/{} | {}/{}/{} |",
            &row.id[..8],
            row.materialize.as_secs_f64(),
            ms(row.parse),
            ms(row.validate),
            ms(row.names_time),
            ms(row.drop_time),
            ms(row.project_all),
            row.failed,
            us(percentile(&row.cold, 0.5)),
            us(percentile(&row.cold, 0.9)),
            us(*row.cold.last().expect("sample")),
            us(percentile(&row.warm, 0.5)),
            us(percentile(&row.warm, 0.9)),
            us(*row.warm.last().expect("sample")),
        );
    }
    println!();
    println!(
        "All entries kept, warm: heap {} MB; RSS {} MB → {} MB (+{} MB)",
        mb(heap_all),
        mb(rss_start),
        mb(rss_all),
        mb(rss_all.saturating_sub(rss_start)),
    );
    drop(kept);
}
