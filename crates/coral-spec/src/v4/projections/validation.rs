//! Read-only compatibility validation for a materialized projection catalog
//! and the effective operation policy selected at load time.

use std::collections::BTreeMap;

use crate::v4::ir::{IrExecutionAttachment, IrInputLocation, IrTypeShape};
use crate::v4::operation_metadata::ValidatedSurfacePlan;
use crate::v4::projections::{
    Projection, ProjectionCatalog, ProjectionKind, ProjectionVisibility, SqlInputExposure,
};
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
        if matches!(operation.execution, IrExecutionAttachment::Rest(_)) {
            validate_projection_columns(plan, projection, &operation.id)?;
        }
        let public_exposure = public_exposure_for_kind(&projection.kind);
        for input in &projection.inputs {
            let Some(operation_input) = operation.inputs.iter().find(|operation_input| {
                operation_input.location == input.source_location
                    && operation_input.name == input.wire_name
            }) else {
                return Err(ManifestError::validation(format!(
                    "projection '{}' input '{}' does not match a {:?} input named '{}' on operation '{}'",
                    projection.name,
                    input.name,
                    input.source_location,
                    input.wire_name,
                    operation.id
                )));
            };

            let pagination_owned =
                plan.pagination_owns_input(operation, &input.wire_name, input.source_location);
            if pagination_owned && input.sql_exposure != SqlInputExposure::Internal {
                return Err(ManifestError::validation(format!(
                    "projection '{}' input '{}' on operation '{}' is owned by pagination but has sql_exposure '{}'; pagination-owned inputs must be internal",
                    projection.name,
                    input.name,
                    operation.id,
                    sql_exposure_name(input.sql_exposure)
                )));
            }

            if input.lookup_key {
                if !matches!(operation.execution, IrExecutionAttachment::Rest(_)) {
                    return Err(ManifestError::validation(format!(
                        "projection '{}' input '{}' on operation '{}' has lookup_key=true, but lookup keys are only valid for REST inputs",
                        projection.name, input.name, operation.id
                    )));
                }
                // The metadata allowlist is keyed by wire name alone, and only
                // query parameters can enter it. A header, cookie, or body
                // input sharing an allowlisted query name is never lowered into
                // a request, so a dependent join must not bind to it.
                if input.source_location != IrInputLocation::Query {
                    return Err(ManifestError::validation(format!(
                        "projection '{}' input '{}' on operation '{}' has lookup_key=true with source location {:?}; lookup keys are only valid for query inputs",
                        projection.name, input.name, operation.id, input.source_location
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

            // Runtime package assembly reads filters from table projections and
            // arguments from table function projections, so the other exposure
            // is unbindable: the input silently disappears from every request.
            if input.sql_exposure != SqlInputExposure::Internal
                && input.sql_exposure != public_exposure
            {
                return Err(ManifestError::validation(format!(
                    "projection '{}' input '{}' on operation '{}' has sql_exposure '{}'; {} projections expose non-internal inputs as {}",
                    projection.name,
                    input.name,
                    operation.id,
                    sql_exposure_name(input.sql_exposure),
                    projection_kind_name(&projection.kind),
                    public_exposure_plural(public_exposure)
                )));
            }

            // Internal inputs are dropped by request lowering, so internalizing
            // an input the provider requires makes every request incomplete.
            // The generator hides such projections instead of publishing them.
            if input.sql_exposure == SqlInputExposure::Internal
                && operation_input.required
                && !pagination_owned
                && projection.visibility == ProjectionVisibility::Published
            {
                return Err(ManifestError::validation(format!(
                    "projection '{}' input '{}' on operation '{}' is required by the operation but has sql_exposure 'internal'; published projections cannot internalize required inputs",
                    projection.name, input.name, operation.id
                )));
            }
        }
    }
    Ok(())
}

/// Checks that a REST projection's columns describe the rows the effective
/// operation policy actually yields.
///
/// The projection catalog is a snapshot: an operation-metadata override that
/// moves the row path elsewhere leaves the materialized columns pointing at
/// fields the new rows do not have. Rather than reconcile the snapshot at load
/// time, reject the combination and let the user regenerate or override the
/// catalog too.
fn validate_projection_columns(
    plan: &ValidatedSurfacePlan,
    projection: &Projection,
    operation_id: &str,
) -> Result<()> {
    let row_type_ref = plan.rest_output_type_ref(operation_id);
    let Some(row_type) = plan
        .semantic_ir()
        .types
        .iter()
        .find(|ty| ty.id == row_type_ref)
    else {
        return Ok(());
    };
    // Only an object row type names its fields. Scalar, list, and opaque JSON
    // rows are projected whole, and their columns carry no source path.
    let IrTypeShape::Object { fields } = &row_type.shape else {
        return Ok(());
    };
    for column in &projection.columns {
        let Some(field_name) = column.source_path.first() else {
            continue;
        };
        if !fields.iter().any(|field| field.name == *field_name) {
            return Err(ManifestError::validation(format!(
                "projection '{}' column '{}' reads field '{field_name}', but the rows operation '{operation_id}' yields have type '{row_type_ref}', which has no such field",
                projection.name, column.name
            )));
        }
    }
    Ok(())
}

const fn public_exposure_for_kind(kind: &ProjectionKind) -> SqlInputExposure {
    match kind {
        ProjectionKind::Table => SqlInputExposure::Filter,
        ProjectionKind::TableFunction { .. } => SqlInputExposure::FunctionArg,
    }
}

const fn projection_kind_name(kind: &ProjectionKind) -> &'static str {
    match kind {
        ProjectionKind::Table => "table",
        ProjectionKind::TableFunction { .. } => "table function",
    }
}

const fn public_exposure_plural(exposure: SqlInputExposure) -> &'static str {
    match exposure {
        SqlInputExposure::Filter => "filters",
        SqlInputExposure::FunctionArg => "function arguments",
        SqlInputExposure::Internal => "internal inputs",
    }
}

const fn sql_exposure_name(exposure: SqlInputExposure) -> &'static str {
    match exposure {
        SqlInputExposure::Filter => "filter",
        SqlInputExposure::FunctionArg => "function_arg",
        SqlInputExposure::Internal => "internal",
    }
}
