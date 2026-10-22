//! PROTOTYPE (#2538 entry size spike). Throwaway code: never merge.
//!
//! Opens the crate-private steps of [`crate::StoredDeclarationFields`] so
//! `examples/prototype_2538_entry_size.rs` can time and weigh each one.

use crate::schema::TypeScriptNames;
use crate::{ExecutionBinding, MaterializedOperation, SchemaGraph};

// Compiles only if a shared cache can hold the parts of one entry: the graph
// as it is, and the names behind a lock (or with a `Mutex` render cache).
const _: () = {
    const fn send_sync<T: Send + Sync>() {}
    const fn send<T: Send>() {}
    send_sync::<SchemaGraph>();
    send::<TypeScriptNames>();
    send::<crate::StoredDeclarationFields>();
};

/// Materializes one stored source document with this build's code. Returns
/// the schema graph JSON that publication would store, and the Operations.
///
/// # Panics
///
/// When the input does not decode or materialization rejects it.
#[must_use]
pub fn materialize_stored(
    kind: &str,
    pinned: &str,
    configuration: &str,
) -> (String, Vec<MaterializedOperation>) {
    let kind = crate::ProviderSetupInputKind::from_storage(kind).expect("input kind");
    let input = match crate::ProviderSetupInput::from_storage(kind, pinned).expect("pinned input")
    {
        crate::ProviderSetupInput::OpenApi(document) => crate::ProviderSetupInput::OpenApi(
            document
                .restore_configuration(configuration, 1)
                .expect("configuration"),
        ),
        crate::ProviderSetupInput::Mcp(snapshot) => crate::ProviderSetupInput::Mcp(
            snapshot
                .restore_configuration(configuration, 1)
                .expect("configuration"),
        ),
    };
    let outcome = match &input {
        crate::ProviderSetupInput::OpenApi(document) => crate::openapi::materialize(document, &[]),
        crate::ProviderSetupInput::Mcp(snapshot) => crate::mcp::materialize(snapshot, &[]),
    };
    match outcome {
        crate::MaterializationOutcome::Ready(catalog) => (
            catalog.schema_graph().to_storage_json().expect("graph JSON"),
            catalog.operations().to_vec(),
        ),
        crate::MaterializationOutcome::Rejected(diagnostic) => {
            panic!("materialization rejected: {diagnostic:?}")
        }
    }
}

/// Parses stored graph JSON, without the validation.
///
/// # Panics
///
/// When the JSON is not a schema graph.
#[must_use]
pub fn parse_graph(json: &str) -> SchemaGraph {
    serde_json::from_str(json).expect("stored schema graph JSON")
}

/// Runs the validation that `SchemaGraph::from_storage_json` runs.
///
/// # Panics
///
/// When the graph is not valid.
pub fn validate_graph(graph: &SchemaGraph) {
    graph.validate().expect("valid schema graph");
}

/// One Provider's TypeScript names, as `StoredDeclarationFields` keeps them.
pub struct Names(TypeScriptNames);

impl Names {
    /// Empties the render cache.
    pub fn clear_renders(&self) {
        self.0.prototype_clear_renders();
    }

    /// Counts the kept renders.
    #[must_use]
    pub fn render_count(&self) -> usize {
        self.0.prototype_render_count()
    }
}

/// Names every schema of the graph for the protocol of `operation`.
///
/// # Panics
///
/// For a built-in Operation, which this spike does not measure.
#[must_use]
pub fn names(graph: &SchemaGraph, operation: &MaterializedOperation) -> Names {
    Names(match operation.execution_binding() {
        ExecutionBinding::Http(_) => crate::openapi::typescript_names(graph),
        ExecutionBinding::Mcp(_) => crate::mcp::typescript_names(graph),
        ExecutionBinding::Builtin(_) => panic!("built-in Generations are out of scope"),
    })
}

/// Names every schema of an OpenAPI graph.
#[must_use]
pub fn openapi_names(graph: &SchemaGraph) -> Names {
    Names(crate::openapi::typescript_names(graph))
}

/// Runs the projection that one search hit runs, `declares_input`.
/// `None` when the projection fails.
#[must_use]
pub fn declares_input(
    graph: &SchemaGraph,
    names: &Names,
    operation: &MaterializedOperation,
) -> Option<bool> {
    match operation.execution_binding() {
        ExecutionBinding::Http(binding) => {
            crate::openapi::declares_input(operation.input_schema(), binding, graph, &names.0).ok()
        }
        ExecutionBinding::Mcp(binding) => {
            crate::mcp::declares_input(operation.input_schema(), binding, graph, &names.0).ok()
        }
        ExecutionBinding::Builtin(_) => Some(true),
    }
}
