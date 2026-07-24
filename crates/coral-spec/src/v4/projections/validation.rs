//! Read-only compatibility validation for a materialized projection catalog
//! and the effective operation policy selected at load time.

use std::collections::BTreeMap;

use crate::v4::ir::IrExecutionAttachment;
use crate::v4::operation_metadata::ValidatedSurfacePlan;
use crate::v4::projections::{ProjectionCatalog, SqlInputExposure};
use crate::{ManifestError, Result};

pub fn validate_projection_compatibility(
    plan: &ValidatedSurfacePlan,
    projections: &ProjectionCatalog,
) -> Result<()> {
    let operations = plan
        .semantic_ir()
        .operations
        .iter()
        .map(|operation| (operation.id.as_str(), operation))
        .collect::<BTreeMap<_, _>>();
    for projection in &projections.projections {
        let Some(operation) = operations.get(projection.operation_id.as_str()) else {
            return Err(ManifestError::validation(format!(
                "projection '{}' references missing operation '{}'",
                projection.name, projection.operation_id
            )));
        };
        for input in &projection.inputs {
            if !operation.inputs.iter().any(|operation_input| {
                operation_input.location == input.source_location
                    && operation_input.name == input.wire_name
            }) {
                return Err(ManifestError::validation(format!(
                    "projection '{}' input '{}' does not match a {:?} input named '{}' on operation '{}'",
                    projection.name,
                    input.name,
                    input.source_location,
                    input.wire_name,
                    operation.id
                )));
            }

            if plan.pagination_owns_input(operation, &input.wire_name, input.source_location)
                && input.sql_exposure != SqlInputExposure::Internal
            {
                return Err(ManifestError::validation(format!(
                    "projection '{}' input '{}' on operation '{}' is owned by pagination but has sql_exposure '{}'; pagination-owned inputs must be internal",
                    projection.name,
                    input.name,
                    operation.id,
                    sql_exposure_name(input.sql_exposure)
                )));
            }

            if !input.lookup_key {
                continue;
            }
            if !matches!(operation.execution, IrExecutionAttachment::Rest(_)) {
                return Err(ManifestError::validation(format!(
                    "projection '{}' input '{}' on operation '{}' has lookup_key=true, but lookup keys are only valid for REST inputs",
                    projection.name, input.name, operation.id
                )));
            }
            if input.sql_exposure != SqlInputExposure::Filter {
                return Err(ManifestError::validation(format!(
                    "projection '{}' input '{}' on operation '{}' has lookup_key=true with sql_exposure '{}'; lookup keys must be exposed as filters",
                    projection.name,
                    input.name,
                    operation.id,
                    sql_exposure_name(input.sql_exposure)
                )));
            }
            if !plan.input_is_lookup_key(&operation.id, &input.wire_name) {
                return Err(ManifestError::validation(format!(
                    "projection '{}' input '{}' on operation '{}' has lookup_key=true, but wire input '{}' is not authorised by the operation metadata lookup-key allowlist",
                    projection.name, input.name, operation.id, input.wire_name
                )));
            }
        }
    }
    Ok(())
}

const fn sql_exposure_name(exposure: SqlInputExposure) -> &'static str {
    match exposure {
        SqlInputExposure::Filter => "filter",
        SqlInputExposure::FunctionArg => "function_arg",
        SqlInputExposure::Internal => "internal",
    }
}
