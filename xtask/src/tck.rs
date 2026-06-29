//! Reporting helpers for the virtual graph openCypher baseline fixture.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

const DEFAULT_BASELINE_FIXTURE: &str =
    "crates/coral-engine/tests/fixtures/virtual_graph/opencypher_read_baseline.json";

#[derive(Debug, clap::Args)]
pub(crate) struct Args {
    /// Path to the openCypher-style baseline fixture.
    #[arg(long, default_value = DEFAULT_BASELINE_FIXTURE)]
    fixture: PathBuf,

    /// Emit machine-readable JSON instead of a text summary.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Deserialize)]
struct Suite {
    #[serde(rename = "suite")]
    name: String,
    minimum_feature_counts: BTreeMap<String, usize>,
    scenarios: Vec<Scenario>,
}

#[derive(Debug, Deserialize)]
struct Scenario {
    id: String,
    feature: String,
    expected: Expectation,
}

#[derive(Debug, Deserialize)]
struct Expectation {
    kind: String,
}

#[derive(Debug, Serialize)]
struct Report {
    suite: String,
    scenario_count: usize,
    expected_error_count: usize,
    feature_counts: BTreeMap<String, usize>,
    minimum_feature_counts: BTreeMap<String, usize>,
    feature_floor_violations: Vec<FeatureFloorViolation>,
}

#[derive(Debug, Serialize)]
struct FeatureFloorViolation {
    feature: String,
    minimum: usize,
    actual: usize,
}

pub(crate) fn run(args: &Args) -> Result<bool> {
    let report = load_report(&args.fixture)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_text_report(&report);
    }
    Ok(feature_floors_satisfied(&report))
}

fn load_report(path: &Path) -> Result<Report> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let suite: Suite = serde_json::from_str(&raw).with_context(|| {
        format!(
            "parsing virtual graph openCypher baseline fixture {}",
            path.display()
        )
    })?;

    let mut ids = BTreeSet::new();
    let mut feature_counts = BTreeMap::<String, usize>::new();
    let mut expected_error_count = 0;
    for scenario in &suite.scenarios {
        if !ids.insert(scenario.id.as_str()) {
            anyhow::bail!("duplicate openCypher baseline scenario id: {}", scenario.id);
        }
        *feature_counts.entry(scenario.feature.clone()).or_default() += 1;
        if scenario.expected.kind == "error" {
            expected_error_count += 1;
        }
    }

    let feature_floor_violations =
        feature_floor_violations(&feature_counts, &suite.minimum_feature_counts);

    Ok(Report {
        suite: suite.name,
        scenario_count: suite.scenarios.len(),
        expected_error_count,
        feature_counts,
        minimum_feature_counts: suite.minimum_feature_counts,
        feature_floor_violations,
    })
}

fn feature_floors_satisfied(report: &Report) -> bool {
    report.feature_floor_violations.is_empty()
}

fn feature_floor_violations(
    feature_counts: &BTreeMap<String, usize>,
    minimum_feature_counts: &BTreeMap<String, usize>,
) -> Vec<FeatureFloorViolation> {
    minimum_feature_counts
        .iter()
        .filter_map(|(feature, minimum)| {
            let actual = feature_counts.get(feature).copied().unwrap_or_default();
            (actual < *minimum).then(|| FeatureFloorViolation {
                feature: feature.clone(),
                minimum: *minimum,
                actual,
            })
        })
        .collect()
}

fn print_text_report(report: &Report) {
    println!("suite: {}", report.suite);
    println!("scenarios: {}", report.scenario_count);
    println!("expected errors: {}", report.expected_error_count);
    println!("features:");
    for (feature, count) in &report.feature_counts {
        let minimum = report
            .minimum_feature_counts
            .get(feature)
            .copied()
            .unwrap_or_default();
        println!("  {feature}: {count} (floor {minimum})");
    }
    if !report.feature_floor_violations.is_empty() {
        println!("floor violations:");
        for violation in &report.feature_floor_violations {
            println!(
                "  {}: {} < {}",
                violation.feature, violation.actual, violation.minimum
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_counts_current_baseline() {
        let report = load_report(&workspace_root().join(DEFAULT_BASELINE_FIXTURE))
            .expect("baseline fixture should parse");

        assert_eq!(report.suite, "coral-opencypher-read-baseline");
        assert_eq!(report.scenario_count, 41);
        assert_eq!(report.expected_error_count, 1);
        assert_eq!(report.feature_counts.get("Where"), Some(&9));
        assert!(report.feature_floor_violations.is_empty());
        assert!(feature_floors_satisfied(&report));
    }

    fn workspace_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask should live under workspace root")
            .to_path_buf()
    }
}
