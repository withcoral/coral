use std::collections::BTreeMap;
use std::ops::Deref;

use serde::{Deserialize, Serialize, Serializer};

use crate::backends::mcp::{McpOffsetPaginationSpec, McpPaginationSpec};
use crate::v4::ir::SemanticIr;
use crate::v4::operation_metadata::ValidatedSurfacePlan;
use crate::{PaginationMode, PaginationSpec, Result};

/// Imported facts and the execution-policy opinions inferred from them.
#[derive(Debug, Clone)]
pub struct ImportedSurface {
    pub semantic_ir: SemanticIr,
    pub operation_metadata: OperationMetadataCatalog,
}

impl ImportedSurface {
    pub fn validated_plan(&self) -> Result<ValidatedSurfacePlan> {
        ValidatedSurfacePlan::new(self.semantic_ir.clone(), self.operation_metadata.clone())
    }
}

impl Deref for ImportedSurface {
    type Target = SemanticIr;

    fn deref(&self) -> &Self::Target {
        &self.semantic_ir
    }
}

/// Complete inferred execution policy for one imported surface.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationMetadataCatalog {
    pub artifact_schema_version: u32,
    pub source_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generator_version: Option<String>,
    pub operations: BTreeMap<String, OperationMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OperationMetadata {
    Rest {
        #[serde(serialize_with = "serialize_rest_operation_pagination")]
        pagination: PaginationSpec,
        lookup_keys: Vec<String>,
    },
    Mcp {
        pagination: McpOperationPagination,
    },
}

fn serialize_rest_operation_pagination<S>(
    pagination: &PaginationSpec,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if rest_pagination_is_canonical_none(pagination) {
        #[derive(Serialize)]
        struct DisabledPagination {
            mode: PaginationMode,
        }

        DisabledPagination {
            mode: PaginationMode::None,
        }
        .serialize(serializer)
    } else {
        pagination.serialize(serializer)
    }
}

pub(super) fn rest_pagination_is_canonical_none(pagination: &PaginationSpec) -> bool {
    pagination.mode == PaginationMode::None
        && pagination.page_size.is_none()
        && pagination.cursor_param.is_none()
        && pagination.cursor_body_path.is_empty()
        && pagination.response_cursor_path.is_empty()
        && pagination.response_cursor_header.is_none()
        && pagination.page_param.is_none()
        && pagination.page_start == 0
        && pagination.page_step == 1
        && pagination.offset_param.is_none()
        && pagination.offset_start == 0
        && pagination.offset_step.is_none()
        && !pagination.link_header_require_results
        && pagination.next_url_header.is_none()
        && pagination.max_pages.is_none()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct McpOperationPagination {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<McpPaginationSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<McpOffsetPaginationSpec>,
}
