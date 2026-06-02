//! Composite runtime source registration for app-assembled component packages.

use std::collections::BTreeSet;

use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;

use crate::backends::{
    BackendRegistration, BackendRegistrationContext, CompiledBackendSource, RegisteredSourceTable,
    SourceTableFunctions,
};

struct CompositeCompiledSource {
    source_name: String,
    components: Vec<Box<dyn CompiledBackendSource>>,
}

pub(crate) fn compile_source(
    source_name: String,
    components: Vec<Box<dyn CompiledBackendSource>>,
) -> Box<dyn CompiledBackendSource> {
    Box::new(CompositeCompiledSource {
        source_name,
        components,
    })
}

#[async_trait]
impl CompiledBackendSource for CompositeCompiledSource {
    fn schema_name(&self) -> &str {
        &self.source_name
    }

    fn source_name(&self) -> &str {
        &self.source_name
    }

    async fn register(
        &self,
        ctx: &SessionContext,
        registration_context: &BackendRegistrationContext,
    ) -> datafusion::error::Result<BackendRegistration> {
        let mut tables = Vec::new();
        let mut table_names = BTreeSet::new();
        let mut table_functions = SourceTableFunctions::new();
        let mut registered_functions = Vec::new();
        let mut inputs = Vec::new();
        let mut input_keys = BTreeSet::new();

        for component in &self.components {
            let registration = component.register(ctx, registration_context).await?;
            let (component_tables, component_table_functions, component_source) =
                registration.into_parts();

            for table in component_tables {
                push_unique_table(&self.source_name, &mut table_names, &mut tables, table)?;
            }
            for (name, function) in component_table_functions {
                if table_functions.insert(name.clone(), function).is_some() {
                    return Err(DataFusionError::Execution(format!(
                        "source '{}' registered duplicate table function '{name}'",
                        self.source_name
                    )));
                }
            }
            registered_functions.extend(component_source.table_functions);
            for input in component_source.inputs {
                if input_keys.insert(input.key.clone()) {
                    inputs.push(input);
                }
            }
        }

        Ok(BackendRegistration::new(
            self.source_name.clone(),
            tables,
            table_functions,
            registered_functions,
            inputs,
        ))
    }
}

fn push_unique_table(
    source_name: &str,
    table_names: &mut BTreeSet<String>,
    tables: &mut Vec<RegisteredSourceTable>,
    table: RegisteredSourceTable,
) -> datafusion::error::Result<()> {
    let name = table.metadata.table_name.clone();
    if table_names.insert(name.clone()) {
        tables.push(table);
        return Ok(());
    }

    Err(DataFusionError::Execution(format!(
        "source '{source_name}' registered duplicate table '{name}'"
    )))
}
