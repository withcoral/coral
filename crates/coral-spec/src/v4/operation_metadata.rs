use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::backends::mcp::{McpOffsetPaginationSpec, McpPaginationSpec};
use crate::v4::ir::{
    IrExecutionAttachment, IrInputLocation, IrOperation, IrScalarType, IrTypeShape,
    OutputCardinality, SemanticIr,
};
use crate::v4::manifest::SurfaceType;
use crate::v4::projections::{ProjectionCatalog, ProjectionKind, SqlInputExposure};
use crate::{ManifestError, PaginationMode, PaginationSpec, Result};

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

fn rest_pagination_is_canonical_none(pagination: &PaginationSpec) -> bool {
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

/// Structurally validated pairing of imported facts and effective policy.
#[derive(Debug, Clone, Serialize)]
pub struct ValidatedSurfacePlan {
    semantic_ir: SemanticIr,
    operation_metadata: OperationMetadataCatalog,
}

impl<'de> Deserialize<'de> for ValidatedSurfacePlan {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct UnvalidatedPlan {
            semantic_ir: SemanticIr,
            operation_metadata: OperationMetadataCatalog,
        }

        let plan = UnvalidatedPlan::deserialize(deserializer)?;
        Self::new(plan.semantic_ir, plan.operation_metadata).map_err(D::Error::custom)
    }
}

impl ValidatedSurfacePlan {
    pub fn new(
        semantic_ir: SemanticIr,
        operation_metadata: OperationMetadataCatalog,
    ) -> Result<Self> {
        validate_semantic_ir_structure(&semantic_ir)?;
        validate_operation_metadata_structure(&semantic_ir, &operation_metadata)?;
        Ok(Self {
            semantic_ir,
            operation_metadata,
        })
    }

    #[must_use]
    pub fn semantic_ir(&self) -> &SemanticIr {
        &self.semantic_ir
    }

    #[must_use]
    pub fn operation_metadata(&self) -> &OperationMetadataCatalog {
        &self.operation_metadata
    }

    #[must_use]
    /// Returns metadata for an operation in this validated plan.
    ///
    /// # Panics
    ///
    /// Panics when `operation_id` is not part of the paired semantic IR.
    pub fn metadata_for_operation(&self, operation_id: &str) -> &OperationMetadata {
        self.operation_metadata
            .operations
            .get(operation_id)
            .expect("validated plan contains metadata for every operation")
    }

    #[must_use]
    /// Returns effective REST pagination for a REST operation.
    ///
    /// # Panics
    ///
    /// Panics when the operation is absent or is not a REST operation.
    pub fn rest_pagination(&self, operation_id: &str) -> &PaginationSpec {
        match self.metadata_for_operation(operation_id) {
            OperationMetadata::Rest { pagination, .. } => pagination,
            OperationMetadata::Mcp { .. } => panic!("REST operation has MCP metadata"),
        }
    }

    #[must_use]
    /// Returns effective cursor and offset pagination for an MCP operation.
    ///
    /// # Panics
    ///
    /// Panics when the operation is absent or is not an MCP operation.
    pub fn mcp_pagination(
        &self,
        operation_id: &str,
    ) -> (Option<&McpPaginationSpec>, Option<&McpOffsetPaginationSpec>) {
        match self.metadata_for_operation(operation_id) {
            OperationMetadata::Mcp { pagination } => {
                (pagination.cursor.as_ref(), pagination.offset.as_ref())
            }
            OperationMetadata::Rest { .. } => panic!("MCP operation has REST metadata"),
        }
    }

    #[must_use]
    pub fn input_is_lookup_key(&self, operation_id: &str, input_name: &str) -> bool {
        matches!(
            self.metadata_for_operation(operation_id),
            OperationMetadata::Rest { lookup_keys, .. }
                if lookup_keys.iter().any(|candidate| candidate == input_name)
        )
    }

    #[must_use]
    /// Pagination only ever owns query (REST) or tool-arg (MCP) inputs; an
    /// input in another location that shares a pagination parameter's name is
    /// not owned.
    pub fn pagination_owns_input(
        &self,
        operation: &IrOperation,
        input_name: &str,
        location: IrInputLocation,
    ) -> bool {
        match self.metadata_for_operation(&operation.id) {
            OperationMetadata::Rest { pagination, .. } => {
                location == IrInputLocation::Query
                    && rest_pagination_owned_inputs(operation, pagination)
                        .is_ok_and(|owned| owned.contains(input_name))
            }
            OperationMetadata::Mcp { pagination } => {
                location == IrInputLocation::ToolArg
                    && (pagination
                        .cursor
                        .as_ref()
                        .is_some_and(|cursor| cursor.cursor_arg == input_name)
                        || pagination.offset.as_ref().is_some_and(|offset| {
                            offset.limit_arg == input_name || offset.offset_arg == input_name
                        }))
            }
        }
    }
}

/// Validates the fact graph independently from inferred operation policy.
///
/// Keeping this boundary public lets artifact loaders attribute corrupt semantic IR to the
/// materialization even when a complete operation-metadata override is selected.
pub fn validate_semantic_ir_structure(semantic_ir: &SemanticIr) -> Result<()> {
    let mut types = BTreeSet::new();
    for ty in &semantic_ir.types {
        if ty.id.trim().is_empty() || ty.id != ty.id.trim() || !types.insert(ty.id.as_str()) {
            return Err(ManifestError::validation(format!(
                "semantic IR type id '{}' is blank, padded, or repeated",
                ty.id
            )));
        }
    }

    for ty in &semantic_ir.types {
        match &ty.shape {
            IrTypeShape::Object { fields } => {
                let mut field_names = BTreeSet::new();
                for field in fields {
                    if field.name.trim().is_empty()
                        || field.name != field.name.trim()
                        || !field_names.insert(field.name.as_str())
                    {
                        return Err(ManifestError::validation(format!(
                            "semantic IR type '{}' has a blank, padded, or repeated field name '{}'",
                            ty.id, field.name
                        )));
                    }
                    validate_type_ref(
                        &types,
                        &field.type_ref,
                        &format!("semantic IR type '{}' field '{}'", ty.id, field.name),
                        false,
                    )?;
                }
            }
            IrTypeShape::List { item_type_ref } => validate_type_ref(
                &types,
                item_type_ref,
                &format!("semantic IR list type '{}' item", ty.id),
                false,
            )?,
            IrTypeShape::Map { value_type_ref } => validate_type_ref(
                &types,
                value_type_ref,
                &format!("semantic IR map type '{}' value", ty.id),
                false,
            )?,
            IrTypeShape::Scalar(_) | IrTypeShape::Enum { .. } | IrTypeShape::Json => {}
        }
    }

    let mut operations = BTreeMap::new();
    for operation in &semantic_ir.operations {
        if operation.id.trim().is_empty() || operation.id != operation.id.trim() {
            return Err(ManifestError::validation(
                "semantic IR operation id must not be blank or padded",
            ));
        }
        if operations
            .insert(operation.id.as_str(), operation)
            .is_some()
        {
            return Err(ManifestError::validation(format!(
                "semantic IR operation '{}' is repeated",
                operation.id
            )));
        }
        validate_ir_operation(semantic_ir.surface_type, operation, &types)?;
    }

    Ok(())
}

/// Validates complete operation policy against an already-formed semantic IR catalog.
pub fn validate_operation_metadata_structure(
    semantic_ir: &SemanticIr,
    metadata: &OperationMetadataCatalog,
) -> Result<()> {
    let operations = semantic_ir
        .operations
        .iter()
        .map(|operation| (operation.id.as_str(), operation))
        .collect::<BTreeMap<_, _>>();

    for operation_id in metadata.operations.keys() {
        if !operations.contains_key(operation_id.as_str()) {
            return Err(ManifestError::validation(format!(
                "operation metadata references unknown operation '{operation_id}'"
            )));
        }
    }
    for (operation_id, operation) in operations {
        let effective = metadata.operations.get(operation_id).ok_or_else(|| {
            ManifestError::validation(format!(
                "operation metadata is missing operation '{operation_id}'"
            ))
        })?;
        validate_operation_metadata(operation, effective)?;
    }
    Ok(())
}

fn validate_type_ref(
    types: &BTreeSet<&str>,
    type_ref: &str,
    owner: &str,
    allow_none: bool,
) -> Result<()> {
    if type_ref.trim().is_empty() || type_ref != type_ref.trim() {
        return Err(ManifestError::validation(format!(
            "{owner} has a blank or padded type reference"
        )));
    }
    if types.contains(type_ref) || type_ref == "json" || (allow_none && type_ref == "none") {
        return Ok(());
    }
    Err(ManifestError::validation(format!(
        "{owner} references missing type '{type_ref}'"
    )))
}

fn validate_ir_operation(
    surface_type: SurfaceType,
    operation: &IrOperation,
    types: &BTreeSet<&str>,
) -> Result<()> {
    let expected_surface = match operation.execution {
        IrExecutionAttachment::Rest(_) => SurfaceType::OpenApi,
        IrExecutionAttachment::Mcp(_) => SurfaceType::Mcp,
    };
    if expected_surface != surface_type {
        return Err(ManifestError::validation(format!(
            "operation '{}' execution type does not match the semantic IR surface type",
            operation.id
        )));
    }

    let mut inputs = BTreeSet::new();
    for input in &operation.inputs {
        if input.name.trim().is_empty() || input.name != input.name.trim() {
            return Err(ManifestError::validation(format!(
                "operation '{}' has a blank or padded input name",
                operation.id
            )));
        }
        if !inputs.insert((input.location, input.name.as_str())) {
            return Err(ManifestError::validation(format!(
                "operation '{}' input '{}' at {:?} is repeated",
                operation.id, input.name, input.location
            )));
        }
    }

    if let IrExecutionAttachment::Rest(rest) = &operation.execution {
        let mut bindings = BTreeSet::new();
        for binding in &rest.parameters {
            if binding.input_name.trim().is_empty()
                || binding.wire_name.trim().is_empty()
                || binding.input_name != binding.input_name.trim()
                || binding.wire_name != binding.wire_name.trim()
            {
                return Err(ManifestError::validation(format!(
                    "operation '{}' has a blank or padded REST binding",
                    operation.id
                )));
            }
            if !bindings.insert((binding.location, binding.wire_name.as_str())) {
                return Err(ManifestError::validation(format!(
                    "operation '{}' REST binding '{}' at {:?} is repeated",
                    operation.id, binding.wire_name, binding.location
                )));
            }
            if !operation
                .inputs
                .iter()
                .any(|input| input.location == binding.location && input.name == binding.input_name)
            {
                return Err(ManifestError::validation(format!(
                    "operation '{}' REST binding '{}' references missing input '{}'",
                    operation.id, binding.wire_name, binding.input_name
                )));
            }
        }
        if let Some(request_body) = &rest.request_body {
            validate_type_ref(
                types,
                &request_body.type_ref,
                &format!("operation '{}' REST request body", operation.id),
                false,
            )?;
        }
    }
    validate_type_ref(
        types,
        &operation.output.type_ref,
        &format!("operation '{}' output", operation.id),
        operation.output.cardinality == OutputCardinality::None,
    )?;
    if let Some(entity) = &operation.entity {
        validate_type_ref(
            types,
            &entity.type_ref,
            &format!("operation '{}' entity", operation.id),
            false,
        )?;
    }
    if operation
        .output
        .row_path
        .iter()
        .any(|segment| segment.trim().is_empty())
    {
        return Err(ManifestError::validation(format!(
            "operation '{}' output row path contains an empty segment",
            operation.id
        )));
    }
    Ok(())
}

fn validate_operation_metadata(
    operation: &IrOperation,
    metadata: &OperationMetadata,
) -> Result<()> {
    match (&operation.execution, metadata) {
        (
            IrExecutionAttachment::Rest(_),
            OperationMetadata::Rest {
                pagination,
                lookup_keys,
            },
        ) => {
            if pagination.mode == PaginationMode::None
                && !rest_pagination_is_canonical_none(pagination)
            {
                return Err(ManifestError::validation(format!(
                    "operation '{}' pagination.mode=none cannot define other pagination settings",
                    operation.id
                )));
            }
            pagination.validated("operation_metadata", &operation.id)?;
            let pagination_inputs = validate_rest_pagination(operation, pagination)?;
            validate_lookup_keys(operation, lookup_keys, &pagination_inputs)
        }
        (IrExecutionAttachment::Mcp(_), OperationMetadata::Mcp { pagination }) => {
            validate_mcp_pagination(operation, pagination)
        }
        (IrExecutionAttachment::Rest(_), OperationMetadata::Mcp { .. }) => {
            Err(ManifestError::validation(format!(
                "REST operation '{}' has MCP operation metadata",
                operation.id
            )))
        }
        (IrExecutionAttachment::Mcp(_), OperationMetadata::Rest { .. }) => {
            Err(ManifestError::validation(format!(
                "MCP operation '{}' has REST operation metadata",
                operation.id
            )))
        }
    }
}

fn validate_rest_pagination(
    operation: &IrOperation,
    pagination: &PaginationSpec,
) -> Result<BTreeSet<String>> {
    let owned = rest_pagination_owned_inputs(operation, pagination)?;
    for path in [
        pagination.cursor_body_path.as_slice(),
        pagination.response_cursor_path.as_slice(),
        pagination
            .page_size
            .as_ref()
            .map_or(&[][..], |page_size| page_size.body_path.as_slice()),
    ] {
        if path.iter().any(|segment| segment.trim().is_empty()) {
            return Err(ManifestError::validation(format!(
                "operation '{}' pagination path contains an empty segment",
                operation.id
            )));
        }
    }
    let uses_body = !pagination.cursor_body_path.is_empty()
        || pagination
            .page_size
            .as_ref()
            .is_some_and(|page_size| !page_size.body_path.is_empty());
    let IrExecutionAttachment::Rest(rest) = &operation.execution else {
        unreachable!("REST pagination validated only for REST operations")
    };
    if uses_body && rest.request_body.is_none() {
        return Err(ManifestError::validation(format!(
            "operation '{}' pagination references a request body that is not present",
            operation.id
        )));
    }
    Ok(owned)
}

fn rest_pagination_owned_inputs(
    operation: &IrOperation,
    pagination: &PaginationSpec,
) -> Result<BTreeSet<String>> {
    let mut owned = BTreeSet::new();
    for (wire_name, role, expected) in [
        (
            pagination.page_param.as_deref(),
            "page_param",
            ExpectedInputType::Numeric,
        ),
        (
            pagination.offset_param.as_deref(),
            "offset_param",
            ExpectedInputType::Numeric,
        ),
        (
            pagination.cursor_param.as_deref(),
            "cursor_param",
            ExpectedInputType::String,
        ),
        (
            pagination
                .page_size
                .as_ref()
                .and_then(|page_size| page_size.query_param.as_deref()),
            "page_size.query_param",
            ExpectedInputType::Numeric,
        ),
    ] {
        let Some(wire_name) = wire_name else { continue };
        let input = rest_query_input_for_wire_name(operation, wire_name, role)?;
        if !expected.accepts(input.data_type) {
            return Err(ManifestError::validation(format!(
                "operation '{}' pagination.{role} references input '{}' with incompatible type",
                operation.id, input.name
            )));
        }
        if matches!(expected, ExpectedInputType::String) && input.required {
            return Err(ManifestError::validation(format!(
                "operation '{}' pagination.{role} input '{}' must be optional",
                operation.id, input.name
            )));
        }
        owned.insert(input.name.clone());
    }
    Ok(owned)
}

fn rest_query_input_for_wire_name<'a>(
    operation: &'a IrOperation,
    wire_name: &str,
    role: &str,
) -> Result<&'a crate::v4::IrOperationInput> {
    let IrExecutionAttachment::Rest(rest) = &operation.execution else {
        unreachable!("REST binding lookup only occurs for REST operations")
    };
    let bindings = rest
        .parameters
        .iter()
        .filter(|binding| {
            binding.location == IrInputLocation::Query && binding.wire_name == wire_name
        })
        .collect::<Vec<_>>();
    let [binding] = bindings.as_slice() else {
        return Err(ManifestError::validation(format!(
            "operation '{}' pagination.{role} must reference exactly one query binding named '{wire_name}'",
            operation.id
        )));
    };
    operation
        .inputs
        .iter()
        .find(|input| input.location == IrInputLocation::Query && input.name == binding.input_name)
        .ok_or_else(|| {
            ManifestError::validation(format!(
                "operation '{}' pagination.{role} references missing query input '{}'",
                operation.id, binding.input_name
            ))
        })
}

#[derive(Clone, Copy)]
enum ExpectedInputType {
    Numeric,
    String,
}

impl ExpectedInputType {
    fn accepts(self, data_type: IrScalarType) -> bool {
        match self {
            Self::Numeric => matches!(data_type, IrScalarType::Integer | IrScalarType::Number),
            Self::String => matches!(data_type, IrScalarType::String | IrScalarType::Id),
        }
    }
}

fn validate_lookup_keys(
    operation: &IrOperation,
    lookup_keys: &[String],
    pagination_inputs: &BTreeSet<String>,
) -> Result<()> {
    let mut unique = BTreeSet::new();
    for name in lookup_keys {
        if name.trim().is_empty() || name != name.trim() || !unique.insert(name.as_str()) {
            return Err(ManifestError::validation(format!(
                "operation '{}' has invalid or repeated lookup key '{name}'",
                operation.id
            )));
        }
        if !operation
            .inputs
            .iter()
            .any(|input| input.location == IrInputLocation::Query && input.name == *name)
        {
            return Err(ManifestError::validation(format!(
                "operation '{}' lookup key '{}' does not name a query input",
                operation.id, name
            )));
        }
        if pagination_inputs.contains(name) {
            return Err(ManifestError::validation(format!(
                "operation '{}' input '{}' cannot be both pagination-owned and a lookup key",
                operation.id, name
            )));
        }
    }
    Ok(())
}

fn validate_mcp_pagination(
    operation: &IrOperation,
    pagination: &McpOperationPagination,
) -> Result<()> {
    if pagination.cursor.is_some() && pagination.offset.is_some() {
        return Err(ManifestError::validation(format!(
            "operation '{}' has both MCP cursor and offset pagination",
            operation.id
        )));
    }
    if let Some(cursor) = &pagination.cursor {
        validate_max_pages(operation, cursor.max_pages)?;
        if cursor.response_cursor_path.is_empty()
            || cursor
                .response_cursor_path
                .iter()
                .any(|segment| segment.trim().is_empty())
        {
            return Err(ManifestError::validation(format!(
                "operation '{}' MCP response cursor path must not be empty",
                operation.id
            )));
        }
        require_mcp_input(
            operation,
            &cursor.cursor_arg,
            ExpectedInputType::String,
            "cursor",
        )?;
    }
    if let Some(offset) = &pagination.offset {
        validate_max_pages(operation, offset.max_pages)?;
        if offset.default_limit == 0
            || offset.max_limit == 0
            || offset.default_limit > offset.max_limit
            || offset.limit_arg == offset.offset_arg
        {
            return Err(ManifestError::validation(format!(
                "operation '{}' MCP offset pagination is invalid",
                operation.id
            )));
        }
        require_mcp_input(
            operation,
            &offset.limit_arg,
            ExpectedInputType::Numeric,
            "limit",
        )?;
        require_mcp_input(
            operation,
            &offset.offset_arg,
            ExpectedInputType::Numeric,
            "offset",
        )?;
    }
    Ok(())
}

fn require_mcp_input(
    operation: &IrOperation,
    name: &str,
    expected: ExpectedInputType,
    role: &str,
) -> Result<()> {
    let input = operation
        .inputs
        .iter()
        .find(|input| input.location == IrInputLocation::ToolArg && input.name == name)
        .ok_or_else(|| {
            ManifestError::validation(format!(
                "operation '{}' MCP pagination {role} references missing tool arg '{name}'",
                operation.id
            ))
        })?;
    if input.required || !expected.accepts(input.data_type) {
        return Err(ManifestError::validation(format!(
            "operation '{}' MCP pagination {role} tool arg '{name}' must be optional and have the correct type",
            operation.id
        )));
    }
    Ok(())
}

fn validate_max_pages(operation: &IrOperation, max_pages: Option<usize>) -> Result<()> {
    if max_pages == Some(0) {
        return Err(ManifestError::validation(format!(
            "operation '{}' MCP pagination max_pages must be greater than 0",
            operation.id
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionInputSyncMode {
    RecomputeInputExposure,
    PreserveExistingExposure,
}

pub fn sync_projection_inputs(
    plan: &ValidatedSurfacePlan,
    projections: &mut ProjectionCatalog,
    mode: ProjectionInputSyncMode,
) {
    let operations = plan
        .semantic_ir()
        .operations
        .iter()
        .map(|operation| (operation.id.as_str(), operation))
        .collect::<BTreeMap<_, _>>();
    for projection in &mut projections.projections {
        let Some(operation) = operations.get(projection.operation_id.as_str()) else {
            continue;
        };
        let default_exposure = match projection.kind {
            ProjectionKind::Table => SqlInputExposure::Filter,
            ProjectionKind::TableFunction { .. } => SqlInputExposure::FunctionArg,
        };
        for input in &mut projection.inputs {
            let pagination_owned =
                plan.pagination_owns_input(operation, &input.wire_name, input.source_location);
            match mode {
                ProjectionInputSyncMode::RecomputeInputExposure => {
                    input.sql_exposure =
                        input_exposure(input.source_location, default_exposure, pagination_owned);
                }
                ProjectionInputSyncMode::PreserveExistingExposure if pagination_owned => {
                    input.sql_exposure = SqlInputExposure::Internal;
                }
                ProjectionInputSyncMode::PreserveExistingExposure => {}
            }
            input.lookup_key = matches!(operation.execution, IrExecutionAttachment::Rest(_))
                && input.sql_exposure == SqlInputExposure::Filter
                && plan.input_is_lookup_key(&operation.id, &input.wire_name);
        }
    }
}

fn input_exposure(
    location: IrInputLocation,
    default_exposure: SqlInputExposure,
    pagination_owned: bool,
) -> SqlInputExposure {
    match location {
        IrInputLocation::Query | IrInputLocation::ToolArg if pagination_owned => {
            SqlInputExposure::Internal
        }
        IrInputLocation::Path | IrInputLocation::Query | IrInputLocation::ToolArg => {
            default_exposure
        }
        IrInputLocation::Header | IrInputLocation::Cookie | IrInputLocation::Body => {
            SqlInputExposure::Internal
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parse_source_manifest_yaml;
    use crate::v4::{
        OperationMetadata, ProjectionInputSyncMode, SqlInputExposure, generate_projection_catalog,
        import_openapi_surface, sync_projection_inputs,
    };

    fn imported() -> (crate::v4::V4SourceManifest, super::ImportedSurface) {
        let manifest = parse_source_manifest_yaml(
            r"
name: demo
dsl_version: 4
surface:
  type: openapi
  file: /tmp/openapi.yaml
  base_url: https://api.example.com
",
        )
        .expect("manifest")
        .as_v4()
        .expect("v4")
        .clone();
        let imported = import_openapi_surface(
            &manifest,
            &manifest.surface,
            br"
openapi: 3.0.3
paths:
  /items:
    get:
      operationId: items/list
      parameters:
        - {name: page, in: query, schema: {type: integer, default: 1}}
        - {name: per_page, in: query, schema: {type: integer, default: 25, maximum: 100}}
        - {name: state, in: query, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {type: object}}
",
        )
        .expect("import");
        (manifest, imported)
    }

    #[test]
    fn semantic_ir_serialization_contains_facts_not_inferred_policy() {
        let (_manifest, imported) = imported();
        let yaml = serde_yaml::to_string(&imported.semantic_ir).expect("semantic IR YAML");

        assert!(
            !yaml.contains("pagination:"),
            "unexpected policy in IR: {yaml}"
        );
        assert!(
            !yaml.contains("lookup_keys:"),
            "unexpected policy in IR: {yaml}"
        );
        assert!(matches!(
            imported.operation_metadata.operations.values().next(),
            Some(OperationMetadata::Rest { pagination, lookup_keys })
                if pagination.page_param.as_deref() == Some("page")
                    && lookup_keys == &["state"]
        ));
    }

    #[test]
    fn disabled_rest_pagination_serializes_only_its_mode() {
        let metadata = OperationMetadata::Rest {
            pagination: crate::PaginationSpec::default(),
            lookup_keys: Vec::new(),
        };

        let yaml = serde_yaml::to_string(&metadata).expect("operation metadata YAML");
        let value: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("metadata value");
        let pagination = value
            .get("pagination")
            .and_then(serde_yaml::Value::as_mapping)
            .expect("pagination mapping");

        assert_eq!(pagination.len(), 1, "unexpected pagination fields: {yaml}");
        assert_eq!(
            pagination.get("mode"),
            Some(&serde_yaml::Value::String("none".to_string()))
        );

        let decoded: OperationMetadata = serde_yaml::from_str(&yaml).expect("round trip");
        assert!(matches!(
            decoded,
            OperationMetadata::Rest { pagination, .. }
                if pagination.mode == crate::PaginationMode::None
                    && pagination.page_step == 1
        ));
    }

    #[test]
    fn plan_rejects_settings_for_disabled_rest_pagination() {
        let (_manifest, mut imported) = imported();
        let OperationMetadata::Rest { pagination, .. } = imported
            .operation_metadata
            .operations
            .values_mut()
            .next()
            .expect("metadata")
        else {
            panic!("REST metadata")
        };
        *pagination = crate::PaginationSpec {
            page_param: Some("page".to_string()),
            ..crate::PaginationSpec::default()
        };

        let error = imported
            .validated_plan()
            .expect_err("disabled pagination settings must fail");

        assert!(
            error
                .to_string()
                .contains("pagination.mode=none cannot define other pagination settings"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn plan_requires_complete_exact_operation_metadata() {
        let (_manifest, imported) = imported();
        let mut missing = imported.operation_metadata.clone();
        missing.operations.clear();
        let error = super::ValidatedSurfacePlan::new(imported.semantic_ir.clone(), missing)
            .expect_err("missing metadata must fail");
        assert!(error.to_string().contains("is missing operation"));

        let mut extra = imported.operation_metadata.clone();
        let value = extra.operations.values().next().expect("metadata").clone();
        extra.operations.insert("unknown".to_string(), value);
        let error = super::ValidatedSurfacePlan::new(imported.semantic_ir, extra)
            .expect_err("unknown metadata must fail");
        assert!(error.to_string().contains("unknown operation"));
    }

    #[test]
    fn plan_rejects_dangling_operation_output_type_reference() {
        let (_manifest, mut imported) = imported();
        imported
            .semantic_ir
            .operations
            .first_mut()
            .expect("operation")
            .output
            .type_ref = "missing_type".to_string();

        let error = imported
            .validated_plan()
            .expect_err("dangling output must fail");

        assert!(
            error
                .to_string()
                .contains("output references missing type 'missing_type'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn plan_rejects_dangling_nested_type_reference() {
        let (_manifest, mut imported) = imported();
        imported.semantic_ir.types.push(crate::v4::IrType {
            id: "dangling_list".to_string(),
            shape: crate::v4::IrTypeShape::List {
                item_type_ref: "missing_item_type".to_string(),
            },
            nullable: false,
            description: String::new(),
        });

        let error = imported
            .validated_plan()
            .expect_err("dangling field must fail");

        assert!(
            error
                .to_string()
                .contains("references missing type 'missing_item_type'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn plan_rejects_lookup_keys_owned_by_pagination() {
        let (_manifest, mut imported) = imported();
        let OperationMetadata::Rest { lookup_keys, .. } = imported
            .operation_metadata
            .operations
            .values_mut()
            .next()
            .expect("metadata")
        else {
            panic!("REST metadata")
        };
        lookup_keys.push("page".to_string());

        let error = imported.validated_plan().expect_err("overlap must fail");
        assert!(
            error
                .to_string()
                .contains("both pagination-owned and a lookup key")
        );
    }

    #[test]
    fn projection_override_identity_survives_policy_reconciliation() {
        let (manifest, imported) = imported();
        let plan = imported.validated_plan().expect("plan");
        let mut catalog = generate_projection_catalog(&manifest, &plan).expect("projections");
        let projection = catalog.projections.first_mut().expect("projection");
        projection.name = "authored_items".to_string();
        projection.guide = "Keep this guide".to_string();
        projection.inputs.iter_mut().for_each(|input| {
            input.sql_exposure = SqlInputExposure::FunctionArg;
        });

        sync_projection_inputs(
            &plan,
            &mut catalog,
            ProjectionInputSyncMode::PreserveExistingExposure,
        );

        let projection = catalog.projections.first().expect("projection");
        assert_eq!(projection.name, "authored_items");
        assert_eq!(projection.guide, "Keep this guide");
        assert_eq!(
            projection
                .inputs
                .iter()
                .find(|input| input.wire_name == "page")
                .expect("page")
                .sql_exposure,
            SqlInputExposure::Internal
        );
        assert_eq!(
            projection
                .inputs
                .iter()
                .find(|input| input.wire_name == "state")
                .expect("state")
                .sql_exposure,
            SqlInputExposure::FunctionArg
        );
    }
}
