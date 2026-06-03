//! Composite runtime source registration for app-assembled component packages.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;

use crate::backends::{
    BackendRegistration, BackendRegistrationContext, CompiledBackendSource, RegisteredSource,
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
        let mut tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        let mut table_functions = SourceTableFunctions::new();
        let mut registered_tables = Vec::new();
        let mut registered_functions = Vec::new();
        let mut inputs = Vec::new();
        let mut input_keys = BTreeSet::new();

        for component in &self.components {
            let registration = component.register(ctx, registration_context).await?;
            for (name, table) in registration.tables {
                if tables.insert(name.clone(), table).is_some() {
                    return Err(DataFusionError::Execution(format!(
                        "source '{}' registered duplicate table '{name}'",
                        self.source_name
                    )));
                }
            }
            for (name, function) in registration.table_functions {
                if table_functions.insert(name.clone(), function).is_some() {
                    return Err(DataFusionError::Execution(format!(
                        "source '{}' registered duplicate table function '{name}'",
                        self.source_name
                    )));
                }
            }
            registered_tables.extend(registration.source.tables);
            registered_functions.extend(registration.source.table_functions);
            for input in registration.source.inputs {
                if input_keys.insert(input.key.clone()) {
                    inputs.push(input);
                }
            }
        }

        Ok(BackendRegistration {
            tables,
            table_functions,
            source: RegisteredSource {
                schema_name: self.source_name.clone(),
                tables: registered_tables,
                table_functions: registered_functions,
                inputs,
            },
        })
    }
}
