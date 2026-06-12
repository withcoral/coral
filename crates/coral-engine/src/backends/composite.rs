//! Composite runtime source registration for app-assembled component packages.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;

use crate::backends::{
    BackendRegistration, BackendRegistrationContext, CompiledBackendSource, RegisteredSource,
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

    fn validate_runtime_capabilities(&self) -> datafusion::error::Result<()> {
        for component in &self.components {
            component.validate_runtime_capabilities()?;
        }
        Ok(())
    }

    async fn register(
        &self,
        ctx: &SessionContext,
        registration_context: &BackendRegistrationContext,
    ) -> datafusion::error::Result<BackendRegistration> {
        let mut tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        let mut registered_tables = Vec::new();
        let mut registered_functions = Vec::new();
        let mut function_keys = BTreeSet::new();
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
            for function in registration.source.table_functions {
                let key = (function.schema_name.clone(), function.function_name.clone());
                if !function_keys.insert(key) {
                    return Err(DataFusionError::Execution(format!(
                        "source '{}' registered duplicate table function '{}.{}'",
                        self.source_name, function.schema_name, function.function_name
                    )));
                }
                registered_functions.push(function);
            }
            registered_tables.extend(registration.source.tables);
            for input in registration.source.inputs {
                if input_keys.insert(input.key.clone()) {
                    inputs.push(input);
                }
            }
        }

        Ok(BackendRegistration {
            tables,
            source: RegisteredSource {
                schema_name: self.source_name.clone(),
                tables: registered_tables,
                table_functions: registered_functions,
                inputs,
            },
        })
    }
}
