mod derive;
mod model;
mod names;
mod pagination;
mod runtime;
mod sync;

pub use derive::generate_projection_catalog;
pub use model::*;
pub(super) use pagination::pagination_query_param_names;
pub use runtime::{
    mcp_projection_arg_specs, projection_arg_specs, projection_column_specs,
    projection_filter_specs, request_spec_for_projection,
};
pub use sync::{ProjectionInputSyncMode, sync_projection_catalog, sync_projection_inputs};
