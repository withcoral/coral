//! Reporting helpers for virtual graph compatibility baseline fixtures.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use coral_engine::virtual_graph::{
    GraphqlCapability, GraphqlCapabilitySurface, graphql_read_capability_surface,
};
use serde::{Deserialize, Serialize};

const DEFAULT_BASELINE_FIXTURE: &str =
    "crates/coral-engine/tests/fixtures/virtual_graph/opencypher_read_baseline.json";
const DEFAULT_GRAPHQL_BASELINE_FIXTURE: &str =
    "crates/coral-engine/tests/fixtures/virtual_graph/graphql_read_baseline.json";
const DEFAULT_UPSTREAM_SCENARIO_FLOOR: usize = 1_615;
const DEFAULT_UPSTREAM_READ_CANDIDATE_FLOOR: usize = 1_294;
const GRAPHQL_SCHEMA_COVERAGE_OVERALL_FLOOR_BASIS_POINTS: usize = 10_000;
const GRAPHQL_SCHEMA_COVERAGE_CATEGORY_FLOORS: &[(&str, usize)] = &[
    ("Aggregates", 17),
    ("BooleanCombinators", 4),
    ("Directives", 2),
    ("ElementIdOperators", 18),
    ("IdentityFields", 6),
    ("IdentityOperators", 10),
    ("MetaFields", 2),
    ("NullOrders", 2),
    ("OrderDirections", 2),
    ("RejectionPaths", 15),
    ("RootFieldForms", 3),
    ("RowModifiers", 5),
    ("ScalarOperators", 18),
    ("Traversal", 6),
];
const GRAPHQL_SCHEMA_COVERAGE_ACKNOWLEDGED_UNCOVERED: &[&str] = &[];

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

#[derive(Debug, clap::Args)]
pub(crate) struct GraphqlSchemaCoverageArgs {
    /// Path to Coral's curated GraphQL read compatibility baseline fixture.
    #[arg(long, default_value = DEFAULT_GRAPHQL_BASELINE_FIXTURE)]
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
    query: String,
    expected: Expectation,
}

#[derive(Debug, Deserialize)]
struct Expectation {
    kind: String,
    #[serde(default)]
    contains: Option<String>,
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

#[derive(Debug, Serialize)]
struct GraphqlSchemaCoverageReport {
    suite: String,
    scenario_count: usize,
    accepted_scenario_count: usize,
    error_scenario_count: usize,
    overall: GraphqlCoverageSummary,
    alias_spellings: GraphqlCoverageSummary,
    categories: BTreeMap<String, GraphqlCategoryCoverage>,
    uncovered: Vec<GraphqlUncoveredCapability>,
    overall_floor_basis_points: usize,
    category_covered_floors: BTreeMap<String, usize>,
    acknowledged_uncovered: Vec<String>,
    floor_violations: Vec<GraphqlCoverageFloorViolation>,
    unacknowledged_uncovered: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct GraphqlCoverageSummary {
    covered: usize,
    total: usize,
    basis_points: usize,
}

#[derive(Clone, Debug, Serialize)]
struct GraphqlCategoryCoverage {
    covered: usize,
    total: usize,
    basis_points: usize,
    uncovered: Vec<GraphqlUncoveredCapability>,
}

#[derive(Clone, Debug, Serialize)]
struct GraphqlUncoveredCapability {
    id: String,
    category: String,
    capability: String,
    tag: String,
    reason: String,
}

#[derive(Debug, Serialize)]
struct GraphqlCoverageFloorViolation {
    metric: String,
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

pub(crate) fn run_graphql_schema_coverage(args: &GraphqlSchemaCoverageArgs) -> Result<bool> {
    let report = load_graphql_schema_coverage_report(&args.fixture)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_graphql_schema_coverage_text_report(&report);
    }
    Ok(graphql_schema_coverage_floors_satisfied(&report))
}

fn load_report(path: &Path) -> Result<Report> {
    let suite = load_suite(path)?;

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

fn load_suite(path: &Path) -> Result<Suite> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| {
        format!(
            "parsing virtual graph compatibility baseline fixture {}",
            path.display()
        )
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

fn load_graphql_schema_coverage_report(path: &Path) -> Result<GraphqlSchemaCoverageReport> {
    let suite = load_suite(path)?;
    let surface = graphql_read_capability_surface();
    let denominator = graphql_capability_denominator(&surface);
    let alias_denominator = graphql_alias_denominator(&surface);
    let mut covered = empty_category_sets(&denominator);
    let mut covered_aliases = BTreeSet::<String>::new();
    let mut ids = BTreeSet::new();
    let mut accepted_scenario_count = 0;
    let mut error_scenario_count = 0;

    for scenario in &suite.scenarios {
        if !ids.insert(scenario.id.as_str()) {
            anyhow::bail!(
                "duplicate virtual graph baseline scenario id: {}",
                scenario.id
            );
        }

        if scenario.expected.kind == "error" {
            error_scenario_count += 1;
            cover_graphql_rejection_path(&surface, scenario, &mut covered);
        } else {
            accepted_scenario_count += 1;
            cover_graphql_query_capabilities(
                &surface,
                scenario,
                &mut covered,
                &mut covered_aliases,
            );
        }
    }

    let uncovered = graphql_uncovered_capabilities(&denominator, &covered);
    let categories = graphql_category_reports(&denominator, &covered);
    let total = denominator.values().map(BTreeSet::len).sum();
    let covered_total = covered.values().map(BTreeSet::len).sum();
    let alias_total = alias_denominator.len();
    let alias_covered = covered_aliases.len();
    let category_covered_floors = GRAPHQL_SCHEMA_COVERAGE_CATEGORY_FLOORS
        .iter()
        .map(|(category, floor)| ((*category).to_string(), *floor))
        .collect::<BTreeMap<_, _>>();
    let acknowledged_uncovered = GRAPHQL_SCHEMA_COVERAGE_ACKNOWLEDGED_UNCOVERED
        .iter()
        .map(|capability| (*capability).to_string())
        .collect::<Vec<_>>();
    let unacknowledged_uncovered = graphql_unacknowledged_uncovered(&uncovered);
    let floor_violations = graphql_schema_coverage_floor_violations(
        covered_total,
        &categories,
        &category_covered_floors,
    );

    Ok(GraphqlSchemaCoverageReport {
        suite: suite.name,
        scenario_count: suite.scenarios.len(),
        accepted_scenario_count,
        error_scenario_count,
        overall: GraphqlCoverageSummary {
            covered: covered_total,
            total,
            basis_points: percentage_basis_points(covered_total, total),
        },
        alias_spellings: GraphqlCoverageSummary {
            covered: alias_covered,
            total: alias_total,
            basis_points: percentage_basis_points(alias_covered, alias_total),
        },
        categories,
        uncovered,
        overall_floor_basis_points: GRAPHQL_SCHEMA_COVERAGE_OVERALL_FLOOR_BASIS_POINTS,
        category_covered_floors,
        acknowledged_uncovered,
        floor_violations,
        unacknowledged_uncovered,
    })
}

fn graphql_capability_denominator(
    surface: &GraphqlCapabilitySurface,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut denominator = BTreeMap::new();
    insert_canonical_capabilities(
        &mut denominator,
        "ScalarOperators",
        &surface.scalar_operators,
    );
    insert_canonical_capabilities(
        &mut denominator,
        "IdentityOperators",
        &surface.identity_operators,
    );
    insert_canonical_capabilities(
        &mut denominator,
        "ElementIdOperators",
        &surface.element_id_operators,
    );
    insert_named_capabilities(&mut denominator, "Aggregates", &surface.aggregates);
    insert_canonical_capabilities(
        &mut denominator,
        "BooleanCombinators",
        &surface.boolean_combinators,
    );
    insert_named_capabilities(&mut denominator, "Directives", &surface.directives);
    insert_canonical_capabilities(
        &mut denominator,
        "OrderDirections",
        &surface.order_directions,
    );
    insert_canonical_capabilities(&mut denominator, "NullOrders", &surface.null_orders);
    insert_canonical_capabilities(&mut denominator, "RowModifiers", &surface.row_modifiers);
    insert_named_capabilities(&mut denominator, "IdentityFields", &surface.identity_fields);
    insert_named_capabilities(&mut denominator, "Traversal", &surface.traversal);
    insert_named_capabilities(&mut denominator, "MetaFields", &surface.meta_fields);
    insert_named_capabilities(
        &mut denominator,
        "RootFieldForms",
        &surface.root_field_forms,
    );
    denominator.insert(
        "RejectionPaths".to_string(),
        surface
            .rejection_paths
            .iter()
            .map(|path| path.id.to_string())
            .collect(),
    );
    denominator
}

fn graphql_alias_denominator(surface: &GraphqlCapabilitySurface) -> BTreeSet<String> {
    let mut aliases = BTreeSet::new();
    insert_alias_capabilities(
        &mut aliases,
        "ScalarOperators",
        &surface.scalar_operator_aliases,
    );
    insert_alias_capabilities(
        &mut aliases,
        "IdentityOperators",
        &surface.identity_operator_aliases,
    );
    insert_alias_capabilities(
        &mut aliases,
        "ElementIdOperators",
        &surface.element_id_operator_aliases,
    );
    insert_alias_capabilities(
        &mut aliases,
        "BooleanCombinators",
        &surface.boolean_combinator_aliases,
    );
    insert_alias_capabilities(
        &mut aliases,
        "OrderDirections",
        &surface.order_direction_aliases,
    );
    insert_alias_capabilities(&mut aliases, "NullOrders", &surface.null_order_aliases);
    insert_alias_capabilities(&mut aliases, "RowModifiers", &surface.row_modifier_aliases);
    aliases
}

fn insert_canonical_capabilities(
    denominator: &mut BTreeMap<String, BTreeSet<String>>,
    category: &str,
    capabilities: &[GraphqlCapability],
) {
    denominator.insert(
        category.to_string(),
        capabilities
            .iter()
            .map(|capability| capability.canonical.to_string())
            .collect(),
    );
}

fn insert_named_capabilities(
    denominator: &mut BTreeMap<String, BTreeSet<String>>,
    category: &str,
    capabilities: &[&str],
) {
    denominator.insert(
        category.to_string(),
        capabilities
            .iter()
            .map(|capability| (*capability).to_string())
            .collect(),
    );
}

fn insert_alias_capabilities(
    aliases: &mut BTreeSet<String>,
    category: &str,
    alias_map: &BTreeMap<&'static str, &'static str>,
) {
    aliases.extend(
        alias_map
            .iter()
            .map(|(alias, canonical)| format!("{category}:{alias}->{canonical}")),
    );
}

fn empty_category_sets(
    denominator: &BTreeMap<String, BTreeSet<String>>,
) -> BTreeMap<String, BTreeSet<String>> {
    denominator
        .keys()
        .map(|category| (category.clone(), BTreeSet::new()))
        .collect()
}

fn cover_graphql_query_capabilities(
    surface: &GraphqlCapabilitySurface,
    scenario: &Scenario,
    covered: &mut BTreeMap<String, BTreeSet<String>>,
    covered_aliases: &mut BTreeSet<String>,
) {
    let query = scenario.query.as_str();
    cover_operator_capabilities(
        covered,
        covered_aliases,
        "ScalarOperators",
        &surface.scalar_operators,
        &surface.scalar_operator_aliases,
        |alias| contains_graphql_name(query, alias),
    );
    cover_operator_capabilities(
        covered,
        covered_aliases,
        "IdentityOperators",
        &surface.identity_operators,
        &surface.identity_operator_aliases,
        |alias| contains_field_operator(query, "_id", alias),
    );
    cover_operator_capabilities(
        covered,
        covered_aliases,
        "ElementIdOperators",
        &surface.element_id_operators,
        &surface.element_id_operator_aliases,
        |alias| contains_field_operator(query, "_elementId", alias),
    );
    cover_named_when(covered, "Aggregates", &surface.aggregates, |name| {
        contains_graphql_name(query, name)
    });
    cover_operator_capabilities(
        covered,
        covered_aliases,
        "BooleanCombinators",
        &surface.boolean_combinators,
        &surface.boolean_combinator_aliases,
        |alias| contains_graphql_name(query, alias),
    );
    cover_named_when(covered, "Directives", &surface.directives, |name| {
        query.contains(&format!("@{name}"))
    });
    cover_operator_capabilities(
        covered,
        covered_aliases,
        "OrderDirections",
        &surface.order_directions,
        &surface.order_direction_aliases,
        |alias| contains_graphql_name(query, alias),
    );
    cover_operator_capabilities(
        covered,
        covered_aliases,
        "NullOrders",
        &surface.null_orders,
        &surface.null_order_aliases,
        |alias| contains_graphql_name(query, alias),
    );
    cover_operator_capabilities(
        covered,
        covered_aliases,
        "RowModifiers",
        &surface.row_modifiers,
        &surface.row_modifier_aliases,
        |alias| contains_graphql_name(query, alias),
    );
    cover_identity_field_capabilities(query, covered);
    cover_traversal_capabilities(scenario, covered);
    cover_meta_field_capabilities(query, covered);
    cover_root_field_form_capabilities(query, covered);
}

fn cover_graphql_rejection_path(
    surface: &GraphqlCapabilitySurface,
    scenario: &Scenario,
    covered: &mut BTreeMap<String, BTreeSet<String>>,
) {
    let Some(expected) = scenario.expected.contains.as_deref() else {
        return;
    };
    for path in &surface.rejection_paths {
        if expected.contains(path.stable_substring) {
            cover(covered, "RejectionPaths", path.id);
        }
    }
}

fn cover_operator_capabilities(
    covered: &mut BTreeMap<String, BTreeSet<String>>,
    covered_aliases: &mut BTreeSet<String>,
    category: &str,
    capabilities: &[GraphqlCapability],
    aliases: &BTreeMap<&'static str, &'static str>,
    contains_alias: impl Fn(&str) -> bool,
) {
    for capability in capabilities {
        if capability.aliases.iter().any(|alias| contains_alias(alias)) {
            cover(covered, category, capability.canonical);
        }
    }
    covered_aliases.extend(
        aliases
            .iter()
            .filter(|(alias, _)| contains_alias(alias))
            .map(|(alias, canonical)| format!("{category}:{alias}->{canonical}")),
    );
}

fn cover_named_when(
    covered: &mut BTreeMap<String, BTreeSet<String>>,
    category: &str,
    capabilities: &[&str],
    predicate: impl Fn(&str) -> bool,
) {
    for capability in capabilities {
        if predicate(capability) {
            cover(covered, category, capability);
        }
    }
}

fn cover_identity_field_capabilities(
    query: &str,
    covered: &mut BTreeMap<String, BTreeSet<String>>,
) {
    if contains_graphql_name(query, "_id") {
        cover(covered, "IdentityFields", "_id.select");
    }
    if contains_field_operator_any(query, "_id") {
        cover(covered, "IdentityFields", "_id.filter");
    }
    if query.contains("field: _id") {
        cover(covered, "IdentityFields", "_id.order");
    }
    if contains_graphql_name(query, "_elementId") {
        cover(covered, "IdentityFields", "_elementId.select");
    }
    if contains_field_operator_any(query, "_elementId") {
        cover(covered, "IdentityFields", "_elementId.filter");
    }
    if query.contains("field: _elementId") {
        cover(covered, "IdentityFields", "_elementId.order");
    }
}

fn cover_traversal_capabilities(
    scenario: &Scenario,
    covered: &mut BTreeMap<String, BTreeSet<String>>,
) {
    let query = scenario.query.as_str();
    if query.contains("out_") {
        cover(covered, "Traversal", "out");
    }
    if query.contains("in_") {
        cover(covered, "Traversal", "in");
    }
    if query.contains("any_") {
        cover(covered, "Traversal", "any");
    }
    if contains_graphql_name(query, "_edge") {
        cover(covered, "Traversal", "_edge");
    }
    if contains_graphql_name(query, "relationshipWhere") {
        cover(covered, "Traversal", "relationshipWhere");
    }
    if scenario.feature == "RelationshipExistence" {
        cover(covered, "Traversal", "existence");
    }
}

fn cover_meta_field_capabilities(query: &str, covered: &mut BTreeMap<String, BTreeSet<String>>) {
    if contains_graphql_name(query, "__typename") && !query.contains("_edge { edgeType: __typename")
    {
        cover(covered, "MetaFields", "node.__typename");
    }
    if query.contains("_edge") && contains_graphql_name(query, "__typename") {
        cover(covered, "MetaFields", "edge.__typename");
    }
}

fn cover_root_field_form_capabilities(
    query: &str,
    covered: &mut BTreeMap<String, BTreeSet<String>>,
) {
    if contains_graphql_name(query, "Person") {
        cover(covered, "RootFieldForms", "exact-label");
    }
    if query.contains("{ person(") || query.contains("{ person ") {
        cover(covered, "RootFieldForms", "singular-alias");
    }
    if contains_graphql_name(query, "persons") || contains_graphql_name(query, "Persons") {
        cover(covered, "RootFieldForms", "plural-alias");
    }
}

fn cover(covered: &mut BTreeMap<String, BTreeSet<String>>, category: &str, capability: &str) {
    covered
        .entry(category.to_string())
        .or_default()
        .insert(capability.to_string());
}

fn contains_field_operator_any(query: &str, field: &str) -> bool {
    query
        .match_indices(field)
        .filter(|(start, _)| graphql_name_boundary(query, *start, field.len()))
        .any(|(start, _)| {
            let Some(tail) = query.get(start + field.len()..) else {
                return false;
            };
            let Some(tail) = tail.trim_start().strip_prefix(':') else {
                return false;
            };
            tail.trim_start().starts_with('{')
        })
}

fn contains_field_operator(query: &str, field: &str, operator: &str) -> bool {
    query
        .match_indices(field)
        .filter(|(start, _)| graphql_name_boundary(query, *start, field.len()))
        .any(|(start, _)| {
            let Some(tail) = query.get(start + field.len()..) else {
                return false;
            };
            let Some(tail) = tail.trim_start().strip_prefix(':') else {
                return false;
            };
            let tail = tail.trim_start();
            let Some(object) = tail.strip_prefix('{') else {
                return false;
            };
            let end = object.find('}').unwrap_or(object.len());
            object
                .get(..end)
                .is_some_and(|candidate| contains_graphql_name(candidate, operator))
        })
}

fn contains_graphql_name(query: &str, name: &str) -> bool {
    query
        .match_indices(name)
        .any(|(start, _)| graphql_name_boundary(query, start, name.len()))
}

fn graphql_name_boundary(query: &str, start: usize, len: usize) -> bool {
    let bytes = query.as_bytes();
    let before_is_name = start
        .checked_sub(1)
        .and_then(|index| bytes.get(index))
        .is_some_and(|byte| is_graphql_name_byte(*byte));
    let after_is_name = bytes
        .get(start + len)
        .is_some_and(|byte| is_graphql_name_byte(*byte));
    !before_is_name && !after_is_name
}

fn is_graphql_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn graphql_uncovered_capabilities(
    denominator: &BTreeMap<String, BTreeSet<String>>,
    covered: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<GraphqlUncoveredCapability> {
    let mut uncovered = Vec::new();
    for (category, capabilities) in denominator {
        let covered_category = covered.get(category);
        for capability in capabilities {
            if covered_category.is_some_and(|covered| covered.contains(capability)) {
                continue;
            }
            let (tag, reason) = graphql_uncovered_tag(category, capability);
            uncovered.push(GraphqlUncoveredCapability {
                id: format!("{category}:{capability}"),
                category: category.clone(),
                capability: capability.clone(),
                tag: tag.to_string(),
                reason: reason.to_string(),
            });
        }
    }
    uncovered
}

fn graphql_category_reports(
    denominator: &BTreeMap<String, BTreeSet<String>>,
    covered: &BTreeMap<String, BTreeSet<String>>,
) -> BTreeMap<String, GraphqlCategoryCoverage> {
    denominator
        .iter()
        .map(|(category, capabilities)| {
            let covered_count = covered.get(category).map_or(0, BTreeSet::len);
            let uncovered = capabilities
                .iter()
                .filter(|capability| {
                    !covered
                        .get(category)
                        .is_some_and(|covered| covered.contains(*capability))
                })
                .map(|capability| {
                    let (tag, reason) = graphql_uncovered_tag(category, capability);
                    GraphqlUncoveredCapability {
                        id: format!("{category}:{capability}"),
                        category: category.clone(),
                        capability: capability.clone(),
                        tag: tag.to_string(),
                        reason: reason.to_string(),
                    }
                })
                .collect::<Vec<_>>();
            (
                category.clone(),
                GraphqlCategoryCoverage {
                    covered: covered_count,
                    total: capabilities.len(),
                    basis_points: percentage_basis_points(covered_count, capabilities.len()),
                    uncovered,
                },
            )
        })
        .collect()
}

fn graphql_uncovered_tag(category: &str, capability: &str) -> (&'static str, &'static str) {
    match (category, capability) {
        (
            "Aggregates",
            "_collectDistinct" | "_sumDistinct" | "_avgDistinct" | "_medianDistinct"
            | "_minDistinct" | "_maxDistinct",
        ) => (
            "needs fixture expansion",
            "distinct aggregate coverage needs duplicate deterministic aggregate inputs",
        ),
        ("Aggregates", "_stDev" | "_stDevP") => (
            "needs fixture expansion",
            "standard deviation coverage needs a richer numeric distribution",
        ),
        ("NullOrders", _) => (
            "needs fixture expansion",
            "null ordering is only behaviorally visible with nullable fixture values",
        ),
        _ => (
            "addable now",
            "the current GraphQL baseline fixture can exercise this capability",
        ),
    }
}

fn graphql_unacknowledged_uncovered(uncovered: &[GraphqlUncoveredCapability]) -> Vec<String> {
    let acknowledged = GRAPHQL_SCHEMA_COVERAGE_ACKNOWLEDGED_UNCOVERED
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    uncovered
        .iter()
        .filter(|capability| !acknowledged.contains(capability.id.as_str()))
        .map(|capability| capability.id.clone())
        .collect()
}

fn graphql_schema_coverage_floor_violations(
    covered_total: usize,
    categories: &BTreeMap<String, GraphqlCategoryCoverage>,
    category_floors: &BTreeMap<String, usize>,
) -> Vec<GraphqlCoverageFloorViolation> {
    let mut violations = Vec::new();
    let overall_basis_points = percentage_basis_points(
        covered_total,
        categories.values().map(|category| category.total).sum(),
    );
    if overall_basis_points < GRAPHQL_SCHEMA_COVERAGE_OVERALL_FLOOR_BASIS_POINTS {
        violations.push(GraphqlCoverageFloorViolation {
            metric: "overall".to_string(),
            minimum: GRAPHQL_SCHEMA_COVERAGE_OVERALL_FLOOR_BASIS_POINTS,
            actual: overall_basis_points,
        });
    }
    for (category, floor) in category_floors {
        let actual = categories
            .get(category)
            .map(|coverage| coverage.covered)
            .unwrap_or_default();
        if actual < *floor {
            violations.push(GraphqlCoverageFloorViolation {
                metric: category.clone(),
                minimum: *floor,
                actual,
            });
        }
    }
    violations
}

fn feature_floors_satisfied(report: &Report) -> bool {
    report.feature_floor_violations.is_empty() && report.undeclared_features.is_empty()
}

fn upstream_floors_satisfied(report: &UpstreamReport) -> bool {
    report.scenario_floor_violation.is_none()
        && report.read_candidate_floor_violation.is_none()
        && report.uncategorized_feature_groups.is_empty()
}

fn graphql_schema_coverage_floors_satisfied(report: &GraphqlSchemaCoverageReport) -> bool {
    report.floor_violations.is_empty() && report.unacknowledged_uncovered.is_empty()
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

fn print_graphql_schema_coverage_text_report(report: &GraphqlSchemaCoverageReport) {
    println!("suite: {}", report.suite);
    println!("scenarios: {}", report.scenario_count);
    println!("accepted scenarios: {}", report.accepted_scenario_count);
    println!("error scenarios: {}", report.error_scenario_count);
    println!(
        "overall schema coverage: {}/{} ({}%)",
        report.overall.covered,
        report.overall.total,
        format_basis_points(report.overall.basis_points)
    );
    println!(
        "alias spelling coverage: {}/{} ({}%)",
        report.alias_spellings.covered,
        report.alias_spellings.total,
        format_basis_points(report.alias_spellings.basis_points)
    );
    println!("categories:");
    for (category, coverage) in &report.categories {
        println!(
            "  {category}: {}/{} ({}%)",
            coverage.covered,
            coverage.total,
            format_basis_points(coverage.basis_points)
        );
    }
    if !report.uncovered.is_empty() {
        println!("uncovered:");
        for capability in &report.uncovered {
            println!(
                "  [{}] {}:{} - {}",
                capability.tag, capability.category, capability.capability, capability.reason
            );
        }
    }
    if !report.floor_violations.is_empty() {
        println!("floor violations:");
        for violation in &report.floor_violations {
            println!(
                "  {}: {} < {}",
                violation.metric, violation.actual, violation.minimum
            );
        }
    }
    if !report.unacknowledged_uncovered.is_empty() {
        println!("unacknowledged uncovered:");
        for capability in &report.unacknowledged_uncovered {
            println!("  {capability}");
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
        assert_eq!(report.scenario_count, 719);
        assert_eq!(report.expected_error_count, 79);
        assert_eq!(report.feature_counts.get("Aggregation"), Some(&29));
        assert_eq!(report.feature_counts.get("CollectSubquery"), Some(&3));
        assert_eq!(report.feature_counts.get("CountSubquery"), Some(&10));
        assert_eq!(report.feature_counts.get("ExistsSubquery"), Some(&13));
        assert_eq!(report.feature_counts.get("GraphMetadata"), Some(&20));
        assert_eq!(report.feature_counts.get("ListExpressions"), Some(&40));
        assert_eq!(report.feature_counts.get("LiteralExpressions"), Some(&15));
        assert_eq!(report.feature_counts.get("MapExpressions"), Some(&25));
        assert_eq!(
            report.feature_counts.get("MathematicalFunctions"),
            Some(&30)
        );
        assert_eq!(report.feature_counts.get("NullSemantics"), Some(&14));
        assert_eq!(report.feature_counts.get("OptionalMatch"), Some(&34));
        assert_eq!(report.feature_counts.get("Parameters"), Some(&14));
        assert_eq!(report.feature_counts.get("PatternComprehension"), Some(&15));
        assert_eq!(report.feature_counts.get("PathValues"), Some(&19));
        assert_eq!(report.feature_counts.get("ReturnDistinct"), Some(&3));
        assert_eq!(report.feature_counts.get("ReturnProjection"), Some(&12));
        assert_eq!(report.feature_counts.get("ScalarExpressions"), Some(&86));
        assert_eq!(report.feature_counts.get("Temporal"), Some(&103));
        assert_eq!(report.feature_counts.get("TypeConversion"), Some(&17));
        assert_eq!(report.feature_counts.get("Union"), Some(&17));
        assert_eq!(report.feature_counts.get("Unwind"), Some(&38));
        assert_eq!(report.feature_counts.get("VariableLengthPaths"), Some(&16));
        assert_eq!(report.feature_counts.get("Where"), Some(&30));
        assert_eq!(report.feature_counts.get("With"), Some(&51));
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
        assert_eq!(report.scenario_count, 164);
        assert_eq!(report.expected_error_count, 19);
        assert_eq!(report.feature_counts.get("Aggregation"), Some(&20));
        assert_eq!(report.feature_counts.get("ErrorHandling"), Some(&19));
        assert_eq!(report.feature_counts.get("RootSelection"), Some(&4));
        assert_eq!(report.feature_counts.get("ScalarFilters"), Some(&18));
        assert_eq!(report.feature_counts.get("Temporal"), Some(&14));
        assert!(report.feature_floor_violations.is_empty());
        assert!(report.undeclared_features.is_empty());
        assert!(feature_floors_satisfied(&report));
    }

    #[test]
    fn schema_coverage_graphql() {
        let report =
            load_graphql_schema_coverage_report(&workspace_root().join(
                "crates/coral-engine/tests/fixtures/virtual_graph/graphql_read_baseline.json",
            ))
            .expect("GraphQL schema coverage report should build");

        assert_eq!(report.suite, "coral-graphql-read-baseline");
        assert_eq!(report.scenario_count, 164);
        assert_eq!(report.accepted_scenario_count, 145);
        assert_eq!(report.error_scenario_count, 19);
        assert_eq!(report.overall.covered, 110);
        assert_eq!(report.overall.total, 110);
        assert_eq!(report.overall.basis_points, 10_000);
        assert_eq!(report.alias_spellings.covered, 64);
        assert_eq!(report.alias_spellings.total, 137);
        assert_eq!(
            report
                .categories
                .get("Aggregates")
                .map(|coverage| (coverage.covered, coverage.total)),
            Some((17, 17))
        );
        assert_eq!(
            report
                .categories
                .get("ElementIdOperators")
                .map(|coverage| (coverage.covered, coverage.total)),
            Some((18, 18))
        );
        assert_eq!(
            report
                .categories
                .get("IdentityOperators")
                .map(|coverage| (coverage.covered, coverage.total)),
            Some((10, 10))
        );
        assert_eq!(
            report
                .categories
                .get("NullOrders")
                .map(|coverage| (coverage.covered, coverage.total)),
            Some((2, 2))
        );
        assert_eq!(
            report
                .categories
                .get("RejectionPaths")
                .map(|coverage| (coverage.covered, coverage.total)),
            Some((15, 15))
        );
        assert_eq!(
            report
                .categories
                .get("ScalarOperators")
                .map(|coverage| (coverage.covered, coverage.total)),
            Some((18, 18))
        );
        assert_eq!(report.overall_floor_basis_points, 10_000);
        assert_eq!(report.category_covered_floors.get("Aggregates"), Some(&17));
        assert_eq!(
            report.category_covered_floors.get("ElementIdOperators"),
            Some(&18)
        );
        assert_eq!(
            report.category_covered_floors.get("IdentityOperators"),
            Some(&10)
        );
        assert_eq!(report.category_covered_floors.get("NullOrders"), Some(&2));
        assert_eq!(
            report.category_covered_floors.get("RejectionPaths"),
            Some(&15)
        );
        assert_eq!(
            report.category_covered_floors.get("ScalarOperators"),
            Some(&18)
        );
        assert!(report.floor_violations.is_empty());
        assert!(report.unacknowledged_uncovered.is_empty());
        assert!(graphql_schema_coverage_floors_satisfied(&report));
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
        assert_eq!(report.coral_baseline_scenario_count, 719);
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
