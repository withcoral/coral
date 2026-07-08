use std::collections::{BTreeSet, HashSet};

use coral_engine::{QuerySource, RuntimeSourceComponent, UdfRuntimeDefinition};
use coral_spec::FunctionSpec;

use crate::bootstrap::AppError;

pub(crate) type SqlPublishTargets = HashSet<SqlPublishTarget>;

pub(crate) fn initial_sql_publish_targets(
    spec: &FunctionSpec,
    selected_sources: &[QuerySource],
) -> SqlPublishTargets {
    if spec.schema() == "functions" {
        HashSet::new()
    } else {
        source_sql_publish_targets(selected_sources)
    }
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
    let schema_sources = selected_sources
        .iter()
        .filter(|source| schemas.contains(source.source_name()))
        .cloned()
        .collect::<Vec<_>>();
    if schema_sources.is_empty() {
        return HashSet::new();
    }
    source_sql_publish_targets(&schema_sources)
}

pub(crate) fn unchecked_source_publish_schemas(
    function: &UdfRuntimeDefinition,
    checked: &BTreeSet<String>,
) -> BTreeSet<String> {
    let schema = &function.publish.table_function.schema;
    if schema != "functions" && !checked.contains(schema) {
        BTreeSet::from([schema.clone()])
    } else {
        BTreeSet::new()
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
