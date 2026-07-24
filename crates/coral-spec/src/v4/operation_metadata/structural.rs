//! Fact-graph validation of the semantic IR, independent from any inferred
//! operation policy.

use std::collections::{BTreeMap, BTreeSet};

use crate::v4::ir::{
    IrExecutionAttachment, IrInputLocation, IrOperation, IrTypeShape, OutputCardinality, SemanticIr,
};
use crate::v4::manifest::SurfaceType;
use crate::{ManifestError, Result};

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
        // Runtime lowering only renders execution-appropriate locations, so an
        // input elsewhere would surface as a SQL argument that never reaches
        // the wire.
        let location_matches_execution = match &operation.execution {
            IrExecutionAttachment::Rest(_) => input.location != IrInputLocation::ToolArg,
            IrExecutionAttachment::Mcp(_) => input.location == IrInputLocation::ToolArg,
        };
        if !location_matches_execution {
            return Err(ManifestError::validation(format!(
                "operation '{}' input '{}' at {:?} does not match its execution type",
                operation.id, input.name, input.location
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
    Ok(())
}
