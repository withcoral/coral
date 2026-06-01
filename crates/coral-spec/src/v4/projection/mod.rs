mod generator;
mod model;
mod naming;
mod specs;
mod types;

pub use generator::generate_projection_catalog;
pub use model::{
    Projection, ProjectionCatalog, ProjectionColumn, ProjectionInput, ProjectionKind,
    ProjectionVisibility, SqlInputExposure,
};
pub use specs::{
    projection_arg_specs, projection_column_specs, projection_filter_specs,
    request_spec_for_projection,
};
pub use types::manifest_data_type_name;
