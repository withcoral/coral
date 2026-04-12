//! Live-eval row injection using a needles file.

use std::path::{Path, PathBuf};

use coral_engine::{SourceDecorator, SourceDecoratorError, SourceTables};

mod error;
mod loader;
mod provider;

use error::NeedleError;
use loader::NeedleGroups;
use provider::{NeedleTableProvider, build_needle_batches};

/// Optional path to a YAML needles file for live-eval row injection.
pub const CORAL_NEEDLES_FILE: &str = "CORAL_NEEDLES_FILE";

/// Cloneable app-owned config for live-eval row injection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NeedleInjectionConfig {
    path: PathBuf,
}

impl NeedleInjectionConfig {
    #[must_use]
    /// Builds injection config from a YAML needles file path.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    #[must_use]
    /// Returns the configured needles file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    /// Builds a fresh decorator for one runtime registration pass.
    pub fn source_decorator(&self) -> Box<dyn SourceDecorator> {
        Box::new(NeedleDecorator::new(self.path.clone()))
    }
}

#[derive(Debug)]
struct NeedleDecorator {
    path: PathBuf,
    groups: Option<NeedleGroups>,
}

impl NeedleDecorator {
    fn new(path: PathBuf) -> Self {
        Self { path, groups: None }
    }

    fn groups_mut(&mut self) -> Result<&mut NeedleGroups, SourceDecoratorError> {
        self.groups.as_mut().ok_or_else(|| {
            SourceDecoratorError::failed_precondition(
                "needle decorator used before prepare completed",
            )
        })
    }
}

impl SourceDecorator for NeedleDecorator {
    fn name(&self) -> &'static str {
        "needle_injection"
    }

    fn prepare(&mut self) -> Result<(), SourceDecoratorError> {
        self.groups = Some(
            loader::load_needle_groups(&self.path).map_err(NeedleError::into_source_decorator)?,
        );
        Ok(())
    }

    fn decorate_source(
        &mut self,
        schema_name: &str,
        mut tables: SourceTables,
    ) -> Result<SourceTables, SourceDecoratorError> {
        let groups = self.groups_mut()?;
        if groups.is_empty() {
            return Ok(tables);
        }

        for (name, provider) in &mut tables {
            let Some(rows) = groups.take(schema_name, name) else {
                continue;
            };
            let batches = build_needle_batches(&rows, &provider.schema())
                .map_err(NeedleError::into_source_decorator)?;
            if !batches.is_empty() {
                *provider = std::sync::Arc::new(NeedleTableProvider::new(
                    std::sync::Arc::clone(provider),
                    batches,
                ));
            }
        }

        Ok(tables)
    }

    fn finish(&mut self) -> Result<(), SourceDecoratorError> {
        let groups = self.groups.take().ok_or_else(|| {
            SourceDecoratorError::failed_precondition(
                "needle decorator finish called before prepare completed",
            )
        })?;
        groups
            .ensure_all_consumed()
            .map_err(NeedleError::into_source_decorator)
    }
}
