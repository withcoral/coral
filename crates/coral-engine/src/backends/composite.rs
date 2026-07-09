//! Composite runtime source registration for app-assembled component packages.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;

use crate::backends::{
    BackendCatalogRegistration, BackendRegistration, BackendRegistrationContext,
    BackendSchemaRegistration, CompiledBackendSource, RegisteredInput, RegisteredSource,
    RegisteredTable, RegisteredTableFunction,
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
        let mut schemas: BTreeMap<String, CompositeSchemaRegistration> = BTreeMap::new();
        let mut catalogs: Vec<BackendCatalogRegistration> = Vec::new();

        for component in &self.components {
            let registration = component.register(ctx, registration_context).await?;
            catalogs.extend(registration.catalogs);
            for schema in registration.schemas {
                let schema_name = schema.source.schema_name.clone();
                let target = schemas
                    .entry(schema_name.clone())
                    .or_insert_with(|| CompositeSchemaRegistration::new(schema_name.clone()));
                for (name, table) in schema.tables {
                    if target.tables.insert(name.clone(), table).is_some() {
                        return Err(DataFusionError::Execution(format!(
                            "source '{}' schema '{schema_name}' registered duplicate table '{name}'",
                            self.source_name
                        )));
                    }
                }
                for function in schema.source.table_functions {
                    let function_name = function.function_name.clone();
                    if !target.function_keys.insert(function_name.clone()) {
                        return Err(DataFusionError::Execution(format!(
                            "source '{}' schema '{schema_name}' registered duplicate table function '{function_name}'",
                            self.source_name
                        )));
                    }
                    target.registered_functions.push(function);
                }
                target.registered_tables.extend(schema.source.tables);
                for input in schema.source.inputs {
                    if target.input_keys.insert(input.key.clone()) {
                        target.inputs.push(input);
                    }
                }
            }
        }

        Ok(BackendRegistration {
            schemas: schemas
                .into_values()
                .map(CompositeSchemaRegistration::into_registration)
                .collect(),
            catalogs,
        })
    }
}

struct CompositeSchemaRegistration {
    schema_name: String,
    tables: HashMap<String, Arc<dyn TableProvider>>,
    function_keys: BTreeSet<String>,
    registered_tables: Vec<RegisteredTable>,
    registered_functions: Vec<RegisteredTableFunction>,
    inputs: Vec<RegisteredInput>,
    input_keys: BTreeSet<String>,
}

impl CompositeSchemaRegistration {
    fn new(schema_name: String) -> Self {
        Self {
            schema_name,
            tables: HashMap::new(),
            function_keys: BTreeSet::new(),
            registered_tables: Vec::new(),
            registered_functions: Vec::new(),
            inputs: Vec::new(),
            input_keys: BTreeSet::new(),
        }
    }

    fn into_registration(self) -> BackendSchemaRegistration {
        BackendSchemaRegistration {
            tables: self.tables,
            source: RegisteredSource {
                catalog_name: None,
                schema_name: self.schema_name,
                catalog_name: None,
                tables: self.registered_tables,
                table_functions: self.registered_functions,
                inputs: self.inputs,
            },
        }
    }
}
