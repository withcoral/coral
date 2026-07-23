//! Reconciliation of authored or generated projection inputs with the
//! validated surface plan's effective operation policy.

use std::collections::BTreeMap;

use crate::v4::ir::{IrExecutionAttachment, IrInputLocation};
use crate::v4::operation_metadata::ValidatedSurfacePlan;
use crate::v4::projections::{ProjectionCatalog, ProjectionKind, SqlInputExposure};
use crate::{ManifestError, Result};

use super::derive::derived_projection_columns;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionInputSyncMode {
    RecomputeInputExposure,
    PreserveExistingExposure,
}

pub fn sync_projection_inputs(
    plan: &ValidatedSurfacePlan,
    projections: &mut ProjectionCatalog,
    mode: ProjectionInputSyncMode,
) -> Result<()> {
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
            // An input the operation does not declare would keep its SQL
            // exposure while runtime lowering silently drops it from the wire
            // request, so reject it here where the mismatch is actionable.
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
    Ok(())
}

/// Synchronizes every projection field derived from effective operation
/// metadata. Authored projection overrides retain their explicit columns.
pub fn sync_projection_catalog(
    plan: &ValidatedSurfacePlan,
    projections: &mut ProjectionCatalog,
    mode: ProjectionInputSyncMode,
) -> Result<()> {
    sync_projection_inputs(plan, projections, mode)?;
    if mode != ProjectionInputSyncMode::RecomputeInputExposure {
        return Ok(());
    }

    for projection in &mut projections.projections {
        let Some(operation) = plan
            .semantic_ir()
            .operations
            .iter()
            .find(|operation| operation.id == projection.operation_id)
        else {
            continue;
        };
        if matches!(operation.execution, IrExecutionAttachment::Rest(_)) {
            projection.columns = derived_projection_columns(plan, operation);
        }
    }
    Ok(())
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
