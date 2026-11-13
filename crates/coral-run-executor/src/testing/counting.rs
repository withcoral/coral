//! A compiler client that counts the calls it passes on.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use coral_v8::{
    AnalyzedSource, AuthoredToolCompilation, BuiltinTools, CompiledProgram, CoralDeclarations,
    LinkedToolSource, SourceReferences,
};

use crate::{AnalysisError, CompileError, CompilerClient};

/// The calls a [`CountingCompiler`] passed on.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompilerCalls {
    /// The number of sources each `analyze` carried, in call order.
    pub analyze: Vec<usize>,
    /// The number of `compile` calls.
    pub compile: usize,
    /// The number of `prepare` calls.
    pub prepare: usize,
}

/// A [`CompilerClient`] that counts the calls it passes to another.
pub struct CountingCompiler {
    inner: Arc<dyn CompilerClient>,
    calls: Mutex<CompilerCalls>,
}

impl CountingCompiler {
    /// Counts the calls it passes to `inner`.
    #[must_use]
    pub fn new(inner: Arc<dyn CompilerClient>) -> Self {
        Self {
            inner,
            calls: Mutex::default(),
        }
    }

    /// The calls passed on since the last take, which it forgets.
    pub fn take(&self) -> CompilerCalls {
        std::mem::take(&mut *self.calls.lock().expect("compiler calls"))
    }

    fn record(&self, record: impl FnOnce(&mut CompilerCalls)) {
        record(&mut self.calls.lock().expect("compiler calls"));
    }
}

#[tonic::async_trait]
impl CompilerClient for CountingCompiler {
    async fn analyze(
        &self,
        time_left: Duration,
        sources: &[AnalyzedSource],
    ) -> Result<Vec<SourceReferences>, AnalysisError> {
        self.record(|calls| calls.analyze.push(sources.len()));
        self.inner.analyze(time_left, sources).await
    }

    async fn compile(
        &self,
        time_left: Duration,
        source: &str,
        linked_tools: &[LinkedToolSource],
        builtin_tools: &BuiltinTools,
        callable: Option<&[String]>,
    ) -> Result<CompiledProgram, CompileError> {
        self.record(|calls| calls.compile += 1);
        self.inner
            .compile(time_left, source, linked_tools, builtin_tools, callable)
            .await
    }

    async fn prepare(
        &self,
        time_left: Duration,
        source: &str,
        builtin_tools: &BuiltinTools,
        declarations: Option<&CoralDeclarations>,
    ) -> Result<AuthoredToolCompilation, CompileError> {
        self.record(|calls| calls.prepare += 1);
        self.inner
            .prepare(time_left, source, builtin_tools, declarations)
            .await
    }
}
