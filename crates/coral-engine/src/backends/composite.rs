//! Composite runtime source registration for app-assembled component packages.

use std::collections::BTreeMap;

use async_trait::async_trait;
use datafusion::prelude::SessionContext;

use crate::backends::{
    BackendRegistration, BackendRegistrationContext, CatalogPublication, CompiledBackendSource,
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
        let mut publications = BTreeMap::<String, CatalogPublication>::new();

        for component in &self.components {
            let registration = component.register(ctx, registration_context).await?;
            for publication in registration.catalog_publications {
                match publications.entry(publication.catalog_name.clone()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(publication);
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().merge(publication)?;
                    }
                }
            }
        }

        Ok(BackendRegistration::from_publications(
            publications.into_values().collect(),
        ))
    }
}
