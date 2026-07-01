mod derive;
mod model;
mod names;
mod pagination;
mod runtime;

pub use derive::generate_projection_catalog;
pub use model::*;
pub use runtime::{
    manifest_data_type_name, mcp_projection_arg_specs, projection_arg_specs,
    projection_arg_specs_with_pagination, projection_column_specs,
    projection_column_specs_with_pagination, projection_filter_specs,
    projection_filter_specs_with_pagination, request_spec_for_projection,
    request_spec_for_projection_with_pagination,
};
