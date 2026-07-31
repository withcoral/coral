//! Validation of inferred REST/MCP execution policy against an
//! already-validated semantic IR.

use std::collections::{BTreeMap, BTreeSet};

use crate::v4::ir::{
    IrExecutionAttachment, IrInputLocation, IrOperation, IrOperationOutput, IrScalarType, IrType,
    IrTypeShape, OutputCardinality, SemanticIr,
};
use crate::v4::operation_metadata::model::{
    McpOperationPagination, OperationMetadata, OperationMetadataCatalog,
    rest_pagination_is_canonical_none,
};
use crate::{ManifestError, PaginationMode, PaginationSpec, Result};

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
    let types = semantic_ir
        .types
        .iter()
        .map(|ty| (ty.id.as_str(), ty))
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
        validate_operation_metadata(operation, effective, &types)?;
    }
    Ok(())
}

fn validate_operation_metadata(
    operation: &IrOperation,
    metadata: &OperationMetadata,
    types: &BTreeMap<&str, &IrType>,
) -> Result<()> {
    match (&operation.execution, metadata) {
        (
            IrExecutionAttachment::Rest(_),
            OperationMetadata::Rest {
                row_path,
                pagination,
                lookup_keys,
            },
        ) => {
            validate_row_path(operation, row_path)?;
            resolve_output_row_type_ref(&operation.output, row_path, types).map_err(|error| {
                ManifestError::validation(format!(
                    "operation '{}' output row path is invalid: {error}",
                    operation.id
                ))
            })?;
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
        (
            IrExecutionAttachment::Mcp(_),
            OperationMetadata::Mcp {
                row_path,
                pagination,
            },
        ) => {
            validate_row_path(operation, row_path)?;
            resolve_output_row_type_ref(&operation.output, row_path, types).map_err(|error| {
                ManifestError::validation(format!(
                    "operation '{}' output row path is invalid: {error}",
                    operation.id
                ))
            })?;
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

fn validate_row_path(operation: &IrOperation, row_path: &[String]) -> Result<()> {
    if row_path
        .iter()
        .any(|segment| segment.trim().is_empty() || segment != segment.trim())
    {
        return Err(ManifestError::validation(format!(
            "operation '{}' output row path contains a blank or padded segment",
            operation.id
        )));
    }
    Ok(())
}

/// Resolves the type of the rows an operation yields once its row path is
/// applied, or the declared output type when the path is empty.
pub(crate) fn resolve_output_row_type_ref<'a>(
    output: &'a IrOperationOutput,
    row_path: &[String],
    types: &BTreeMap<&str, &'a IrType>,
) -> std::result::Result<&'a str, String> {
    if row_path.is_empty() {
        return Ok(&output.type_ref);
    }

    // A row path selects from the response root, but a declared list is already
    // the rows: its `type_ref` describes one of them. Traversing from there
    // would check the path against a single row's fields while the runtime
    // applies it to the enclosing array, where it selects nothing.
    if output.cardinality == OutputCardinality::List {
        return Err("row path must be empty when the response root is already a list".to_string());
    }

    let mut type_ref = output.type_ref.as_str();
    for segment in row_path {
        let ty = types
            .get(type_ref)
            .ok_or_else(|| format!("type '{type_ref}' is missing"))?;
        let IrTypeShape::Object { fields } = &ty.shape else {
            return Err(format!(
                "segment '{segment}' traverses non-object type '{type_ref}'"
            ));
        };
        let field = fields
            .iter()
            .find(|field| field.name == *segment)
            .ok_or_else(|| format!("type '{type_ref}' has no field '{segment}'"))?;
        type_ref = &field.type_ref;
    }

    let ty = types
        .get(type_ref)
        .ok_or_else(|| format!("type '{type_ref}' is missing"))?;
    let IrTypeShape::List { item_type_ref } = &ty.shape else {
        return Err(format!("terminal type '{type_ref}' is not a list"));
    };
    if item_type_ref != "json" && !types.contains_key(item_type_ref.as_str()) {
        return Err(format!("list item type '{item_type_ref}' is missing"));
    }
    Ok(item_type_ref)
}

fn validate_rest_pagination(
    operation: &IrOperation,
    pagination: &PaginationSpec,
) -> Result<BTreeSet<String>> {
    let owned = rest_pagination_owned_inputs(operation, pagination)?;
    for path in [
        pagination.cursor_body_path.as_slice(),
        pagination.response_cursor_path.as_slice(),
        pagination.next_url_path.as_slice(),
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

pub(super) fn rest_pagination_owned_inputs(
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
        // Discovery pagination must request the initial page; a nonzero start
        // would silently skip rows on every fetch.
        if offset.offset_start != 0 {
            return Err(ManifestError::validation(format!(
                "operation '{}' MCP offset pagination offset_start must be 0",
                operation.id
            )));
        }
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
