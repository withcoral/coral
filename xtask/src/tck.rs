//! Reporting helpers for virtual graph compatibility baseline fixtures.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

const DEFAULT_BASELINE_FIXTURE: &str =
    "crates/coral-engine/tests/fixtures/virtual_graph/opencypher_read_baseline.json";
const DEFAULT_UPSTREAM_SCENARIO_FLOOR: usize = 1_615;
const DEFAULT_UPSTREAM_READ_CANDIDATE_FLOOR: usize = 1_294;

#[derive(Debug, clap::Args)]
pub(crate) struct Args {
    /// Path to a virtual graph compatibility baseline fixture.
    #[arg(long, default_value = DEFAULT_BASELINE_FIXTURE)]
    fixture: PathBuf,

    /// Emit machine-readable JSON instead of a text summary.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, clap::Args)]
pub(crate) struct UpstreamArgs {
    /// Path to an upstream openCypher checkout's tck/features directory.
    #[arg(long)]
    features_dir: PathBuf,

    /// Path to Coral's curated Cypher compatibility baseline fixture.
    #[arg(long, default_value = DEFAULT_BASELINE_FIXTURE)]
    coral_baseline_fixture: PathBuf,

    /// Minimum upstream scenario definitions expected at the pinned TCK version.
    #[arg(long, default_value_t = DEFAULT_UPSTREAM_SCENARIO_FLOOR)]
    minimum_scenarios: usize,

    /// Minimum upstream read-candidate scenario definitions expected.
    #[arg(long, default_value_t = DEFAULT_UPSTREAM_READ_CANDIDATE_FLOOR)]
    minimum_read_candidate_scenarios: usize,

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
    undeclared_features: Vec<String>,
}

#[derive(Debug, Serialize)]
struct FeatureFloorViolation {
    feature: String,
    minimum: usize,
    actual: usize,
}

#[derive(Debug, Serialize)]
struct UpstreamReport {
    suite: String,
    source_features_dir: String,
    feature_file_count: usize,
    scenario_count: usize,
    read_candidate_scenario_count: usize,
    coral_baseline_scenario_count: usize,
    coral_baseline_total_basis_points: usize,
    coral_baseline_read_candidate_basis_points: usize,
    minimum_scenarios: usize,
    minimum_read_candidate_scenarios: usize,
    scenario_floor_violation: Option<ScenarioFloorViolation>,
    read_candidate_floor_violation: Option<ScenarioFloorViolation>,
    category_counts: BTreeMap<String, UpstreamCount>,
    feature_group_counts: BTreeMap<String, UpstreamFeatureGroup>,
    uncategorized_feature_groups: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ScenarioFloorViolation {
    minimum: usize,
    actual: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
struct UpstreamCount {
    feature_files: usize,
    scenarios: usize,
}

#[derive(Debug, Serialize)]
struct UpstreamFeatureGroup {
    category: String,
    feature_files: usize,
    scenarios: usize,
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

pub(crate) fn run_upstream(args: &UpstreamArgs) -> Result<bool> {
    let report = load_upstream_report(
        &args.features_dir,
        &args.coral_baseline_fixture,
        args.minimum_scenarios,
        args.minimum_read_candidate_scenarios,
    )?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_upstream_text_report(&report);
    }
    Ok(upstream_floors_satisfied(&report))
}

fn load_report(path: &Path) -> Result<Report> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let suite: Suite = serde_json::from_str(&raw).with_context(|| {
        format!(
            "parsing virtual graph compatibility baseline fixture {}",
            path.display()
        )
    })?;

    let mut ids = BTreeSet::new();
    let mut feature_counts = BTreeMap::<String, usize>::new();
    let mut expected_error_count = 0;
    for scenario in &suite.scenarios {
        if !ids.insert(scenario.id.as_str()) {
            anyhow::bail!(
                "duplicate virtual graph baseline scenario id: {}",
                scenario.id
            );
        }
        *feature_counts.entry(scenario.feature.clone()).or_default() += 1;
        if scenario.expected.kind == "error" {
            expected_error_count += 1;
        }
    }

    let feature_floor_violations =
        feature_floor_violations(&feature_counts, &suite.minimum_feature_counts);
    let undeclared_features = undeclared_features(&feature_counts, &suite.minimum_feature_counts);

    Ok(Report {
        suite: suite.name,
        scenario_count: suite.scenarios.len(),
        expected_error_count,
        feature_counts,
        minimum_feature_counts: suite.minimum_feature_counts,
        feature_floor_violations,
        undeclared_features,
    })
}

fn load_upstream_report(
    features_dir: &Path,
    coral_baseline_fixture: &Path,
    minimum_scenarios: usize,
    minimum_read_candidate_scenarios: usize,
) -> Result<UpstreamReport> {
    let coral_baseline = load_report(coral_baseline_fixture)?;
    let mut feature_file_count = 0;
    let mut scenario_count = 0;
    let mut read_candidate_scenario_count = 0;
    let mut category_counts = BTreeMap::<String, UpstreamCount>::new();
    let mut feature_group_counts = BTreeMap::<String, UpstreamFeatureGroup>::new();
    let mut uncategorized_feature_groups = BTreeSet::<String>::new();

    for entry in walkdir::WalkDir::new(features_dir).follow_links(false) {
        let entry = entry.with_context(|| format!("walking {}", features_dir.display()))?;
        if !entry.file_type().is_file()
            || entry.path().extension().is_none_or(|ext| ext != "feature")
        {
            continue;
        }

        let relative = entry.path().strip_prefix(features_dir).with_context(|| {
            format!(
                "stripping {} from {}",
                features_dir.display(),
                entry.path().display()
            )
        })?;
        let feature_group = feature_group(relative)?;
        let category = classify_feature_group(&feature_group);
        let scenarios = count_feature_scenarios(entry.path())?;

        feature_file_count += 1;
        scenario_count += scenarios;
        if category.is_read_candidate() {
            read_candidate_scenario_count += scenarios;
        }

        let category_name = category.name().to_string();
        let category_entry = category_counts.entry(category_name.clone()).or_default();
        category_entry.feature_files += 1;
        category_entry.scenarios += scenarios;

        match feature_group_counts.get_mut(&feature_group) {
            Some(existing) => {
                existing.feature_files += 1;
                existing.scenarios += scenarios;
            }
            None => {
                feature_group_counts.insert(
                    feature_group.clone(),
                    UpstreamFeatureGroup {
                        category: category_name,
                        feature_files: 1,
                        scenarios,
                    },
                );
            }
        }

        if matches!(category, UpstreamCategory::Uncategorized) {
            uncategorized_feature_groups.insert(feature_group);
        }
    }

    let scenario_floor_violation =
        (scenario_count < minimum_scenarios).then_some(ScenarioFloorViolation {
            minimum: minimum_scenarios,
            actual: scenario_count,
        });
    let read_candidate_floor_violation = (read_candidate_scenario_count
        < minimum_read_candidate_scenarios)
        .then_some(ScenarioFloorViolation {
            minimum: minimum_read_candidate_scenarios,
            actual: read_candidate_scenario_count,
        });

    Ok(UpstreamReport {
        suite: "coral-opencypher-upstream-tck-inventory".to_string(),
        source_features_dir: features_dir.display().to_string(),
        feature_file_count,
        scenario_count,
        read_candidate_scenario_count,
        coral_baseline_scenario_count: coral_baseline.scenario_count,
        coral_baseline_total_basis_points: percentage_basis_points(
            coral_baseline.scenario_count,
            scenario_count,
        ),
        coral_baseline_read_candidate_basis_points: percentage_basis_points(
            coral_baseline.scenario_count,
            read_candidate_scenario_count,
        ),
        minimum_scenarios,
        minimum_read_candidate_scenarios,
        scenario_floor_violation,
        read_candidate_floor_violation,
        category_counts,
        feature_group_counts,
        uncategorized_feature_groups: uncategorized_feature_groups.into_iter().collect(),
    })
}

fn feature_floors_satisfied(report: &Report) -> bool {
    report.feature_floor_violations.is_empty() && report.undeclared_features.is_empty()
}

fn upstream_floors_satisfied(report: &UpstreamReport) -> bool {
    report.scenario_floor_violation.is_none()
        && report.read_candidate_floor_violation.is_none()
        && report.uncategorized_feature_groups.is_empty()
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

fn undeclared_features(
    feature_counts: &BTreeMap<String, usize>,
    minimum_feature_counts: &BTreeMap<String, usize>,
) -> Vec<String> {
    feature_counts
        .keys()
        .filter(|feature| !minimum_feature_counts.contains_key(*feature))
        .cloned()
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
    if !report.undeclared_features.is_empty() {
        println!("undeclared features:");
        for feature in &report.undeclared_features {
            println!("  {feature}");
        }
    }
}

fn print_upstream_text_report(report: &UpstreamReport) {
    println!("suite: {}", report.suite);
    println!("features dir: {}", report.source_features_dir);
    println!("feature files: {}", report.feature_file_count);
    println!("upstream scenarios: {}", report.scenario_count);
    println!(
        "read-candidate scenarios: {}",
        report.read_candidate_scenario_count
    );
    println!(
        "Coral curated baseline scenarios: {} ({}% of total, {}% of read candidates)",
        report.coral_baseline_scenario_count,
        format_basis_points(report.coral_baseline_total_basis_points),
        format_basis_points(report.coral_baseline_read_candidate_basis_points)
    );
    println!("categories:");
    for (category, count) in &report.category_counts {
        println!(
            "  {category}: {} scenarios across {} files",
            count.scenarios, count.feature_files
        );
    }
    if let Some(violation) = &report.scenario_floor_violation {
        println!(
            "scenario floor violation: {} < {}",
            violation.actual, violation.minimum
        );
    }
    if let Some(violation) = &report.read_candidate_floor_violation {
        println!(
            "read-candidate floor violation: {} < {}",
            violation.actual, violation.minimum
        );
    }
    if !report.uncategorized_feature_groups.is_empty() {
        println!("uncategorized feature groups:");
        for group in &report.uncategorized_feature_groups {
            println!("  {group}");
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UpstreamCategory {
    ProductExcludedMutation,
    ProductExcludedProcedure,
    ReadCandidateClause,
    ReadCandidateExpression,
    ReadCandidateUseCase,
    Uncategorized,
}

impl UpstreamCategory {
    fn name(self) -> &'static str {
        match self {
            UpstreamCategory::ProductExcludedMutation => "product_excluded_mutation",
            UpstreamCategory::ProductExcludedProcedure => "product_excluded_procedure",
            UpstreamCategory::ReadCandidateClause => "read_candidate_clause",
            UpstreamCategory::ReadCandidateExpression => "read_candidate_expression",
            UpstreamCategory::ReadCandidateUseCase => "read_candidate_use_case",
            UpstreamCategory::Uncategorized => "uncategorized",
        }
    }

    fn is_read_candidate(self) -> bool {
        matches!(
            self,
            UpstreamCategory::ReadCandidateClause
                | UpstreamCategory::ReadCandidateExpression
                | UpstreamCategory::ReadCandidateUseCase
        )
    }
}

fn classify_feature_group(group: &str) -> UpstreamCategory {
    match group {
        "clauses/call" => UpstreamCategory::ProductExcludedProcedure,
        "clauses/create" | "clauses/delete" | "clauses/merge" | "clauses/remove"
        | "clauses/set" => UpstreamCategory::ProductExcludedMutation,
        _ if group.starts_with("clauses/") => UpstreamCategory::ReadCandidateClause,
        _ if group.starts_with("expressions/") => UpstreamCategory::ReadCandidateExpression,
        _ if group.starts_with("useCases/") => UpstreamCategory::ReadCandidateUseCase,
        _ => UpstreamCategory::Uncategorized,
    }
}

fn feature_group(path: &Path) -> Result<String> {
    let mut components = path.components();
    let first = components
        .next()
        .and_then(|component| component.as_os_str().to_str())
        .context("feature file path should include a top-level group")?;
    let second = components
        .next()
        .and_then(|component| component.as_os_str().to_str())
        .context("feature file path should include a second-level group")?;
    Ok(format!("{first}/{second}"))
}

fn count_feature_scenarios(path: &Path) -> Result<usize> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("reading upstream TCK feature {}", path.display()))?;
    Ok(raw
        .lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            trimmed.starts_with("Scenario:") || trimmed.starts_with("Scenario Outline:")
        })
        .count())
}

fn percentage_basis_points(numerator: usize, denominator: usize) -> usize {
    numerator
        .saturating_mul(10_000)
        .saturating_add(denominator / 2)
        .checked_div(denominator)
        .unwrap_or_default()
}

fn format_basis_points(basis_points: usize) -> String {
    format!("{}.{:02}", basis_points / 100, basis_points % 100)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_counts_current_baseline() {
        let report = load_report(&workspace_root().join(DEFAULT_BASELINE_FIXTURE))
            .expect("baseline fixture should parse");

        assert_eq!(report.suite, "coral-opencypher-read-baseline");
        assert_eq!(report.scenario_count, 100);
        assert_eq!(report.expected_error_count, 11);
        assert_eq!(report.feature_counts.get("Aggregation"), Some(&5));
        assert_eq!(report.feature_counts.get("GraphMetadata"), Some(&5));
        assert_eq!(report.feature_counts.get("ListExpressions"), Some(&9));
        assert_eq!(report.feature_counts.get("LiteralExpressions"), Some(&2));
        assert_eq!(report.feature_counts.get("MathematicalFunctions"), Some(&5));
        assert_eq!(report.feature_counts.get("NullSemantics"), Some(&3));
        assert_eq!(report.feature_counts.get("OptionalMatch"), Some(&4));
        assert_eq!(report.feature_counts.get("Parameters"), Some(&2));
        assert_eq!(report.feature_counts.get("PathValues"), Some(&4));
        assert_eq!(report.feature_counts.get("ScalarExpressions"), Some(&23));
        assert_eq!(report.feature_counts.get("TypeConversion"), Some(&3));
        assert_eq!(report.feature_counts.get("VariableLengthPaths"), Some(&4));
        assert_eq!(report.feature_counts.get("Where"), Some(&11));
        assert_eq!(report.feature_counts.get("With"), Some(&6));
        assert!(report.feature_floor_violations.is_empty());
        assert!(report.undeclared_features.is_empty());
        assert!(feature_floors_satisfied(&report));
    }

    #[test]
    fn report_counts_graphql_baseline() {
        let report =
            load_report(&workspace_root().join(
                "crates/coral-engine/tests/fixtures/virtual_graph/graphql_read_baseline.json",
            ))
            .expect("GraphQL baseline fixture should parse");

        assert_eq!(report.suite, "coral-graphql-read-baseline");
        assert_eq!(report.scenario_count, 12);
        assert_eq!(report.expected_error_count, 1);
        assert_eq!(report.feature_counts.get("RootSelection"), Some(&2));
        assert_eq!(report.feature_counts.get("ScalarFilters"), Some(&2));
        assert!(report.feature_floor_violations.is_empty());
        assert!(report.undeclared_features.is_empty());
        assert!(feature_floors_satisfied(&report));
    }

    #[test]
    fn upstream_report_classifies_feature_groups() {
        let root = test_dir("upstream-report-classifies-feature-groups");
        write_feature(
            &root.join("clauses/match/Match.feature"),
            "Feature: Match\n  Scenario: Match nodes\n  Scenario Outline: Match outline\n",
        );
        write_feature(
            &root.join("clauses/create/Create.feature"),
            "Feature: Create\n  Scenario: Create node\n",
        );
        write_feature(
            &root.join("clauses/call/Call.feature"),
            "Feature: Call\n  Scenario: Call procedure\n",
        );
        write_feature(
            &root.join("expressions/string/String.feature"),
            "Feature: String\n  Scenario: String predicate\n",
        );
        write_feature(
            &root.join("useCases/triadicSelection/TriadicSelection.feature"),
            "Feature: Triadic selection\n  Scenario: Triadic selection\n",
        );
        write_feature(
            &root.join("expressions/aggregation/Aggregation.feature"),
            "Feature: Aggregation placeholder\n",
        );

        let report = load_upstream_report(
            &root,
            &workspace_root().join(DEFAULT_BASELINE_FIXTURE),
            6,
            4,
        )
        .expect("upstream report should parse");

        assert_eq!(report.feature_file_count, 6);
        assert_eq!(report.scenario_count, 6);
        assert_eq!(report.read_candidate_scenario_count, 4);
        assert_eq!(report.coral_baseline_scenario_count, 100);
        assert_eq!(
            report
                .category_counts
                .get("read_candidate_clause")
                .map(|count| count.scenarios),
            Some(2)
        );
        assert_eq!(
            report
                .category_counts
                .get("read_candidate_expression")
                .map(|count| count.scenarios),
            Some(1)
        );
        assert_eq!(
            report
                .category_counts
                .get("read_candidate_use_case")
                .map(|count| count.scenarios),
            Some(1)
        );
        assert_eq!(
            report
                .category_counts
                .get("product_excluded_mutation")
                .map(|count| count.scenarios),
            Some(1)
        );
        assert_eq!(
            report
                .category_counts
                .get("product_excluded_procedure")
                .map(|count| count.scenarios),
            Some(1)
        );
        assert!(upstream_floors_satisfied(&report));
    }

    #[test]
    fn upstream_report_flags_uncategorized_feature_groups() {
        let root = test_dir("upstream-report-flags-uncategorized-feature-groups");
        write_feature(
            &root.join("newSurface/example/Example.feature"),
            "Feature: Example\n  Scenario: New surface\n",
        );

        let report = load_upstream_report(
            &root,
            &workspace_root().join(DEFAULT_BASELINE_FIXTURE),
            1,
            0,
        )
        .expect("upstream report should parse");

        assert_eq!(
            report.uncategorized_feature_groups,
            vec!["newSurface/example"]
        );
        assert!(!upstream_floors_satisfied(&report));
    }

    #[test]
    fn percentage_basis_points_rounds_to_nearest() {
        assert_eq!(percentage_basis_points(45, 1_615), 279);
        assert_eq!(percentage_basis_points(45, 1_294), 348);
        assert_eq!(percentage_basis_points(45, 0), 0);
    }

    fn workspace_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask should live under workspace root")
            .to_path_buf()
    }

    fn test_dir(name: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("coral-xtask-{name}-{}", uuid::Uuid::new_v4()));
        if root.exists() {
            fs::remove_dir_all(&root).expect("stale test directory should be removable");
        }
        fs::create_dir_all(&root).expect("test directory should be created");
        root
    }

    fn write_feature(path: &Path, body: &str) {
        fs::create_dir_all(path.parent().expect("feature path should have parent"))
            .expect("feature parent should be created");
        fs::write(path, body).expect("feature file should be written");
    }
}
