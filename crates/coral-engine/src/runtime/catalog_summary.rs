//! Catalog summary construction for discovery surfaces.

use crate::runtime::query;
use crate::{CatalogInfo, CoreError, QueryRuntimeConfig, QuerySource};

pub(crate) async fn list_catalog_summaries(
    sources: &[QuerySource],
    runtime: QueryRuntimeConfig,
    schema_filter: Option<&str>,
) -> Result<CatalogInfo, CoreError> {
    Ok(query::build_runtime_with_options(
        sources,
        runtime,
        query::RuntimeBuildOptions::catalog_summary(),
    )
    .await?
    .catalog_info(schema_filter))
}
