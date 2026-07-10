use std::collections::{BTreeSet, HashSet};

use coral_engine::{QuerySource, RuntimeSourceComponent, UdfRuntimeDefinition};

use crate::bootstrap::AppError;

pub(crate) type SqlPublishTargets = HashSet<SqlPublishTarget>;

pub(crate) fn initial_sql_publish_targets(selected_sources: &[QuerySource]) -> SqlPublishTargets {
    source_sql_publish_targets(selected_sources)
}

pub(crate) fn record_sql_publish_target(
    function: &UdfRuntimeDefinition,
    publish_targets: &mut SqlPublishTargets,
) -> Result<(), AppError> {
    let target = SqlPublishTarget::new(
        &function.publish.table_function.schema,
        &function.publish.table_function.name,
    );
    if !publish_targets.insert(target.clone()) {
        return Err(AppError::FailedPrecondition(format!(
            "function publish target '{}' is installed more than once",
            target.display_name()
        )));
    }
    Ok(())
}

pub(crate) fn source_sql_publish_targets_for_schemas(
    selected_sources: &[QuerySource],
    schemas: &BTreeSet<String>,
) -> SqlPublishTargets {
    let mut targets = HashSet::new();
    for source in selected_sources {
        for component in source.components() {
            if schemas.contains(component.source_name()) {
                record_source_component_sql_targets(component, &mut targets);
            }
        }
    }
    targets
}

pub(crate) fn unchecked_source_publish_schemas(
    function: &UdfRuntimeDefinition,
    checked: &BTreeSet<String>,
) -> BTreeSet<String> {
    let schema = &function.publish.table_function.schema;
    if checked.contains(schema) {
        BTreeSet::new()
    } else {
        BTreeSet::from([schema.clone()])
    }
}

fn source_sql_publish_targets(selected_sources: &[QuerySource]) -> SqlPublishTargets {
    let mut targets = HashSet::new();
    for source in selected_sources {
        for component in source.components() {
            record_source_component_sql_targets(component, &mut targets);
        }
    }
    targets
}

fn record_source_component_sql_targets(
    component: &RuntimeSourceComponent,
    targets: &mut SqlPublishTargets,
) {
    match component {
        RuntimeSourceComponent::Http(manifest) => {
            for table in &manifest.tables {
                targets.insert(SqlPublishTarget::new(&manifest.common.name, table.name()));
            }
            for function in &manifest.functions {
                targets.insert(SqlPublishTarget::new(&manifest.common.name, &function.name));
            }
        }
        RuntimeSourceComponent::File(manifest) => {
            for table in &manifest.tables {
                targets.insert(SqlPublishTarget::new(&manifest.common.name, table.name()));
            }
        }
        RuntimeSourceComponent::Mcp(manifest) => {
            for table in &manifest.tables {
                targets.insert(SqlPublishTarget::new(
                    &manifest.common.name,
                    &table.common.name,
                ));
            }
            for function in &manifest.functions {
                targets.insert(SqlPublishTarget::new(
                    &manifest.common.name,
                    &function.common.name,
                ));
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SqlPublishTarget {
    schema: String,
    name: String,
}

impl SqlPublishTarget {
    fn new(schema: &str, name: &str) -> Self {
        Self {
            schema: schema.to_ascii_lowercase(),
            name: name.to_ascii_lowercase(),
        }
    }

    fn display_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use coral_engine::{
        RuntimeSourcePackage, UdfRuntimeImplementation, UdfRuntimePublish,
        UdfRuntimeTableFunctionPublish,
    };
    use coral_spec::parse_source_manifest_yaml;

    use super::*;

    fn functions_source() -> QuerySource {
        http_source("functions", "review_queue")
    }

    fn http_source(schema: &str, table: &str) -> QuerySource {
        let manifest = parse_source_manifest_yaml(&format!(
            r"
name: {schema}
version: 0.1.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: {table}
    description: Existing table
    request:
      method: GET
      path: /{table}
    response: {{}}
    columns:
      - name: id
        type: Int64
"
        ))
        .expect("source manifest");
        QuerySource::new(manifest, BTreeMap::new(), BTreeMap::new())
    }

    fn multi_schema_source() -> QuerySource {
        let primary = http_source("logical", "primary_table");
        let secondary = http_source("secondary", "review_queue");
        QuerySource::from_runtime_components(
            RuntimeSourcePackage {
                source_name: "logical".to_string(),
                authored_version: None,
                description: String::new(),
                declared_inputs: Vec::new(),
                test_queries: Vec::new(),
                components: primary
                    .components()
                    .iter()
                    .chain(secondary.components())
                    .cloned()
                    .collect(),
            },
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("multi-schema source")
    }

    fn runtime_function() -> UdfRuntimeDefinition {
        UdfRuntimeDefinition {
            name: "review_queue".to_string(),
            description: String::new(),
            arguments: Vec::new(),
            implementation: UdfRuntimeImplementation::CoralSql {
                query: "select 1 as id".to_string(),
            },
            publish: UdfRuntimePublish {
                table_function: UdfRuntimeTableFunctionPublish {
                    schema: "functions".to_string(),
                    name: "review_queue".to_string(),
                    description: String::new(),
                },
            },
            result_columns: Vec::new(),
        }
    }

    #[test]
    fn functions_schema_still_checks_source_publish_targets() {
        let targets = initial_sql_publish_targets(&[functions_source()]);

        assert!(targets.contains(&SqlPublishTarget::new("functions", "review_queue")));
    }

    #[test]
    fn functions_schema_is_not_treated_as_prechecked() {
        assert_eq!(
            unchecked_source_publish_schemas(&runtime_function(), &BTreeSet::new()),
            BTreeSet::from(["functions".to_string()])
        );
    }

    #[test]
    fn schema_filter_checks_secondary_source_components() {
        let targets = source_sql_publish_targets_for_schemas(
            &[multi_schema_source()],
            &BTreeSet::from(["secondary".to_string()]),
        );

        assert!(targets.contains(&SqlPublishTarget::new("secondary", "review_queue")));
        assert!(!targets.contains(&SqlPublishTarget::new("logical", "primary_table")));
    }
}
