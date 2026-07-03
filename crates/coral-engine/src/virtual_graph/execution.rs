//! Virtual graph execution results: the `GraphExecution` (a `SqlTranslation`
//! paired with the engine `QueryExecution` from running one plan) and
//! `GraphQueryPlan` (a `SqlTranslation` paired with the `QueryPlan` from an
//! explain) wrappers. Each exposes the translated `DataFusion` SQL, its
//! diagnostics, and the underlying execution or plan. Result containers only —
//! they bundle lowering output with downstream engine output and run nothing.

use super::{Diagnostic, SqlTranslation};
use crate::{QueryExecution, QueryPlan};

/// Materialized result of executing one virtual graph plan.
#[derive(Debug, Clone)]
pub struct GraphExecution {
    translation: SqlTranslation,
    execution: QueryExecution,
}

/// Explain-plan result for one virtual graph plan.
#[derive(Debug, Clone)]
pub struct GraphQueryPlan {
    translation: SqlTranslation,
    plan: QueryPlan,
}

impl GraphExecution {
    /// Builds one virtual graph execution result.
    #[must_use]
    pub fn new(translation: SqlTranslation, execution: QueryExecution) -> Self {
        Self {
            translation,
            execution,
        }
    }

    /// Returns the SQL translation metadata.
    #[must_use]
    pub fn translation(&self) -> &SqlTranslation {
        &self.translation
    }

    /// Returns the translated `DataFusion` SQL.
    #[must_use]
    pub fn translated_sql(&self) -> &str {
        self.translation.sql()
    }

    /// Returns non-fatal virtual graph diagnostics.
    #[must_use]
    pub fn diagnostics(&self) -> &[Diagnostic] {
        self.translation.diagnostics()
    }

    /// Returns the underlying SQL execution result.
    #[must_use]
    pub fn execution(&self) -> &QueryExecution {
        &self.execution
    }
}

impl GraphQueryPlan {
    /// Builds one virtual graph explain result.
    #[must_use]
    pub fn new(translation: SqlTranslation, plan: QueryPlan) -> Self {
        Self { translation, plan }
    }

    /// Returns the SQL translation metadata.
    #[must_use]
    pub fn translation(&self) -> &SqlTranslation {
        &self.translation
    }

    /// Returns the translated `DataFusion` SQL.
    #[must_use]
    pub fn translated_sql(&self) -> &str {
        self.translation.sql()
    }

    /// Returns non-fatal virtual graph diagnostics.
    #[must_use]
    pub fn diagnostics(&self) -> &[Diagnostic] {
        self.translation.diagnostics()
    }

    /// Returns the underlying SQL query plan.
    #[must_use]
    pub fn plan(&self) -> &QueryPlan {
        &self.plan
    }
}
