//! Source-scoped catalog staging and decoration.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::datasource::TableProvider;

use super::{
    BackendCatalogRegistration, BackendRegistration, BackendSchemaRegistration,
    DatabaseColumnFetcher, RegisteredSource, SourceQualifiedName,
};
use crate::runtime::schema_provider::StaticSchemaProvider;
use crate::{CoreError, QuerySource, SourceDecorator, SourceTables};
use coral_spec::SqlObjectName;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogPublication {
    ExtendExisting,
    InstallNew,
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogTarget {
    pub(crate) catalog_name: String,
    pub(crate) publication: CatalogPublication,
}

pub(crate) struct DeclaredCatalogDraft {
    target: CatalogTarget,
    tables: BTreeMap<SqlObjectName, Arc<dyn TableProvider>>,
    source: RegisteredSource,
}

pub(crate) struct ProviderDiscoveredCatalogDraft {
    target: CatalogTarget,
    provider: Arc<dyn CatalogProvider>,
    source: RegisteredSource,
    column_fetcher: Option<Arc<dyn DatabaseColumnFetcher>>,
}

pub(crate) struct CatalogRegistration {
    pub(crate) target: CatalogTarget,
    pub(crate) provider: Arc<dyn CatalogProvider>,
    pub(crate) source: RegisteredSource,
    pub(crate) column_fetcher: Option<Arc<dyn DatabaseColumnFetcher>>,
}

pub(crate) struct CatalogPreparation<'a> {
    source: &'a QuerySource,
    decorators: &'a mut [Box<dyn SourceDecorator>],
    declared: Vec<DeclaredCatalogDraft>,
    provider_discovered: Vec<ProviderDiscoveredCatalogDraft>,
}

impl<'a> CatalogPreparation<'a> {
    pub(crate) fn new(
        source: &'a QuerySource,
        decorators: &'a mut [Box<dyn SourceDecorator>],
    ) -> Self {
        Self {
            source,
            decorators,
            declared: Vec::new(),
            provider_discovered: Vec::new(),
        }
    }

    pub(crate) fn stage_backend_registration(
        &mut self,
        registration: BackendRegistration,
    ) -> Result<(), CoreError> {
        for schema in registration.schemas {
            self.stage_declared(Self::declared_draft(schema)?)?;
        }
        for catalog in registration.catalogs {
            self.stage_provider_discovered(Self::provider_discovered_draft(catalog)?)?;
        }
        Ok(())
    }

    pub(crate) fn stage_declared(&mut self, draft: DeclaredCatalogDraft) -> Result<(), CoreError> {
        if draft.target.publication != CatalogPublication::ExtendExisting {
            return Err(CoreError::InvalidInput(
                "declared v3 catalog drafts must extend the default catalog".to_string(),
            ));
        }
        self.declared.push(draft);
        Ok(())
    }

    pub(crate) fn stage_provider_discovered(
        &mut self,
        draft: ProviderDiscoveredCatalogDraft,
    ) -> Result<(), CoreError> {
        if draft.target.publication != CatalogPublication::InstallNew {
            return Err(CoreError::InvalidInput(
                "provider-discovered catalog drafts must install a new catalog".to_string(),
            ));
        }
        self.provider_discovered.push(draft);
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Vec<CatalogRegistration>, CoreError> {
        self.ensure_provider_discovered_support()?;

        let mut providers = BTreeMap::new();
        for draft in &self.declared {
            for (sql_name, provider) in &draft.tables {
                if providers
                    .insert(sql_name.clone(), Arc::clone(provider))
                    .is_some()
                {
                    return Err(CoreError::InvalidInput(format!(
                        "source '{}' staged duplicate table identity '{sql_name}'",
                        self.source.source_name()
                    )));
                }
            }
        }

        let expected_identities = providers.keys().cloned().collect::<BTreeSet<_>>();
        let mut tables = SourceTables::new(providers);
        if !self.declared.is_empty() {
            for decorator in self.decorators {
                tables = decorator
                    .decorate_source(self.source, tables)
                    .map_err(|error| decorator_error(decorator.name(), &error))?;
            }
        }
        let actual_identities = tables.iter().map(|(name, _)| name.clone()).collect();
        if expected_identities != actual_identities {
            return Err(CoreError::FailedPrecondition(format!(
                "source '{}' decoration changed its canonical table identity set",
                self.source.source_name()
            )));
        }

        let mut decorated = tables.into_inner();
        let mut registrations =
            Vec::with_capacity(self.declared.len() + self.provider_discovered.len());
        for draft in self.declared {
            let mut schema_tables = HashMap::with_capacity(draft.tables.len());
            for sql_name in draft.tables.keys() {
                let provider = decorated.remove(sql_name).ok_or_else(|| {
                    CoreError::FailedPrecondition(format!(
                        "source '{}' decoration omitted table identity '{sql_name}'",
                        self.source.source_name()
                    ))
                })?;
                schema_tables.insert(sql_name.name().to_string(), provider);
            }
            let schema_name = draft.source.qualified_name.name().to_string();
            let provider = Arc::new(MemoryCatalogProvider::new());
            provider
                .register_schema(
                    &schema_name,
                    Arc::new(StaticSchemaProvider::new(schema_tables)),
                )
                .map_err(|error| CoreError::FailedPrecondition(error.to_string()))?;
            registrations.push(CatalogRegistration {
                target: draft.target,
                provider,
                source: draft.source,
                column_fetcher: None,
            });
        }
        debug_assert!(decorated.is_empty());

        registrations.extend(self.provider_discovered.into_iter().map(|draft| {
            CatalogRegistration {
                target: draft.target,
                provider: draft.provider,
                source: draft.source,
                column_fetcher: draft.column_fetcher,
            }
        }));
        Ok(registrations)
    }

    fn ensure_provider_discovered_support(&self) -> Result<(), CoreError> {
        if self.provider_discovered.is_empty() {
            return Ok(());
        }
        if let Some(decorator) = self
            .decorators
            .iter()
            .find(|decorator| !decorator.supports_provider_discovered_catalogs())
        {
            return Err(CoreError::FailedPrecondition(format!(
                "source '{}' registers provider-discovered catalogs, which source decorator '{}' does not support",
                self.source.source_name(),
                decorator.name()
            )));
        }
        Ok(())
    }

    fn declared_draft(
        registration: BackendSchemaRegistration,
    ) -> Result<DeclaredCatalogDraft, CoreError> {
        let BackendSchemaRegistration { tables, source } = registration;
        let SourceQualifiedName::Schema(schema_name) = &source.qualified_name else {
            return Err(CoreError::InvalidInput(
                "declared backend registration must publish a schema".to_string(),
            ));
        };
        let tables = tables
            .into_iter()
            .map(|(table_name, provider)| {
                let sql_name = SqlObjectName::try_new("datafusion", schema_name, table_name)
                    .map_err(|error| CoreError::InvalidInput(error.to_string()))?;
                Ok((sql_name, provider))
            })
            .collect::<Result<_, CoreError>>()?;
        Ok(DeclaredCatalogDraft {
            target: CatalogTarget {
                catalog_name: "datafusion".to_string(),
                publication: CatalogPublication::ExtendExisting,
            },
            tables,
            source,
        })
    }

    fn provider_discovered_draft(
        registration: BackendCatalogRegistration,
    ) -> Result<ProviderDiscoveredCatalogDraft, CoreError> {
        let BackendCatalogRegistration {
            catalog,
            source,
            column_fetcher,
        } = registration;
        let SourceQualifiedName::Catalog(catalog_name) = &source.qualified_name else {
            return Err(CoreError::InvalidInput(
                "provider-discovered backend registration must publish a catalog".to_string(),
            ));
        };
        Ok(ProviderDiscoveredCatalogDraft {
            target: CatalogTarget {
                catalog_name: catalog_name.clone(),
                publication: CatalogPublication::InstallNew,
            },
            provider: catalog,
            source,
            column_fetcher: Some(column_fetcher),
        })
    }
}

fn decorator_error(name: &str, error: &crate::SourceDecoratorError) -> CoreError {
    match error {
        crate::SourceDecoratorError::InvalidInput(detail) => {
            CoreError::InvalidInput(format!("source decorator '{name}': {detail}"))
        }
        crate::SourceDecoratorError::FailedPrecondition(detail) => {
            CoreError::FailedPrecondition(format!("source decorator '{name}': {detail}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::datatypes::Schema;
    use datafusion::catalog::MemoryCatalogProvider;
    use datafusion::catalog::empty::EmptyTable;
    use datafusion::datasource::TableProvider;

    use super::{
        CatalogPreparation, CatalogPublication, CatalogTarget, DeclaredCatalogDraft,
        ProviderDiscoveredCatalogDraft,
    };
    use crate::backends::{RegisteredSource, SourceQualifiedName};
    use crate::{
        QuerySource, RuntimeSourcePackage, SourceDecorator, SourceDecoratorError, SourceTables,
    };
    use coral_spec::SqlObjectName;

    struct CountingDecorator {
        calls: Arc<AtomicUsize>,
        fail: bool,
        supports_provider_discovered: bool,
    }

    impl SourceDecorator for CountingDecorator {
        fn name(&self) -> &'static str {
            "counting"
        }

        fn supports_provider_discovered_catalogs(&self) -> bool {
            self.supports_provider_discovered
        }

        fn decorate_source(
            &mut self,
            _source: &QuerySource,
            tables: SourceTables,
        ) -> Result<SourceTables, SourceDecoratorError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(SourceDecoratorError::failed_precondition(
                    "decoration failed",
                ));
            }
            Ok(tables)
        }
    }

    #[test]
    fn provider_discovered_support_fails_before_decoration() {
        let source = query_source();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut decorators: Vec<Box<dyn SourceDecorator>> = vec![Box::new(CountingDecorator {
            calls: Arc::clone(&calls),
            fail: false,
            supports_provider_discovered: false,
        })];
        let mut preparation = CatalogPreparation::new(&source, &mut decorators);
        preparation
            .stage_provider_discovered(provider_discovered_draft())
            .expect("stage provider-discovered catalog");

        let Err(error) = preparation.finish() else {
            panic!("unsupported provider discovery must fail closed");
        };

        assert!(error.to_string().contains("does not support"), "{error}");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn decoration_failure_returns_no_catalog_registrations() {
        let source = query_source();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut decorators: Vec<Box<dyn SourceDecorator>> = vec![Box::new(CountingDecorator {
            calls: Arc::clone(&calls),
            fail: true,
            supports_provider_discovered: false,
        })];
        let mut preparation = CatalogPreparation::new(&source, &mut decorators);
        preparation
            .stage_declared(declared_draft("github", "issues"))
            .expect("stage declared catalog");

        let Err(error) = preparation.finish() else {
            panic!("decoration failure must not produce registrations");
        };

        assert!(error.to_string().contains("decoration failed"), "{error}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    fn query_source() -> QuerySource {
        QuerySource::from_runtime_components(
            RuntimeSourcePackage {
                source_name: "github".to_string(),
                authored_version: None,
                description: String::new(),
                declared_inputs: Vec::new(),
                test_queries: Vec::new(),
                identity_requirements: None,
                catalogs: Vec::new(),
            },
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("query source")
    }

    fn declared_draft(schema_name: &str, table_name: &str) -> DeclaredCatalogDraft {
        let sql_name = SqlObjectName::try_new("datafusion", schema_name, table_name)
            .expect("canonical SQL name");
        let provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::new(Schema::empty())));
        DeclaredCatalogDraft {
            target: CatalogTarget {
                catalog_name: "datafusion".to_string(),
                publication: CatalogPublication::ExtendExisting,
            },
            tables: BTreeMap::from([(sql_name, provider)]),
            source: RegisteredSource {
                qualified_name: SourceQualifiedName::Schema(schema_name.to_string()),
                tables: Vec::new(),
                table_functions: Vec::new(),
                inputs: Vec::new(),
            },
        }
    }

    fn provider_discovered_draft() -> ProviderDiscoveredCatalogDraft {
        ProviderDiscoveredCatalogDraft {
            target: CatalogTarget {
                catalog_name: "github".to_string(),
                publication: CatalogPublication::InstallNew,
            },
            provider: Arc::new(MemoryCatalogProvider::new()),
            source: RegisteredSource {
                qualified_name: SourceQualifiedName::Catalog("github".to_string()),
                tables: Vec::new(),
                table_functions: Vec::new(),
                inputs: Vec::new(),
            },
            column_fetcher: None,
        }
    }
}
