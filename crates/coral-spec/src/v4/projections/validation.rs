//! Read-only compatibility validation for a materialized projection catalog
//! and the effective operation policy selected at load time.

use std::collections::BTreeMap;

use crate::v4::ir::{IrExecutionAttachment, IrInputLocation, IrOperation, IrType, IrTypeShape};
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
    let types = plan
        .semantic_ir()
        .types
        .iter()
        .map(|ty| (ty.id.as_str(), ty))
        .collect::<BTreeMap<_, _>>();
    for projection in &projections.projections {
        let Some(operation) = operations.get(projection.operation_id.as_str()) else {
            return Err(ManifestError::validation(format!(
                "projection '{}' references missing operation '{}'",
                projection.name, projection.operation_id
            )));
        };
        validate_projection_columns(plan, &types, projection, &operation.id)?;
        validate_projection_inputs(plan, projection, operation)?;
    }
    Ok(())
}

/// Checks that a projection's inputs bind to the operation's, and that each is
/// exposed to SQL in a way request lowering can actually honour.
fn validate_projection_inputs(
    plan: &ValidatedSurfacePlan,
    projection: &Projection,
    operation: &IrOperation,
) -> Result<()> {
    let public_exposure = public_exposure_for_kind(&projection.kind);
    for input in &projection.inputs {
        let Some(operation_input) = operation.inputs.iter().find(|operation_input| {
            operation_input.location == input.source_location
                && operation_input.name == input.wire_name
        }) else {
            return Err(ManifestError::validation(format!(
                "projection '{}' input '{}' does not match a {:?} input named '{}' on operation '{}'",
                projection.name, input.name, input.source_location, input.wire_name, operation.id
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
        if input.sql_exposure != SqlInputExposure::Internal && input.sql_exposure != public_exposure
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
    Ok(())
}

/// Checks that a projection's columns describe the rows the effective
/// operation policy actually yields.
///
/// The projection catalog is a snapshot: an operation-metadata override that
/// moves the row path elsewhere leaves the materialized columns pointing at
/// fields the new rows do not have. Rather than reconcile the snapshot at load
/// time, reject the combination and let the user regenerate or override the
/// catalog too.
fn validate_projection_columns(
    plan: &ValidatedSurfacePlan,
    types: &BTreeMap<&str, &IrType>,
    projection: &Projection,
    operation_id: &str,
) -> Result<()> {
    let row_type_ref = plan.output_row_type_ref(operation_id);
    // Only an object row type names its fields. Every other shape is projected
    // whole, as a single column with no source path — including a `json` row,
    // which the semantic IR carries no entry for at all.
    let row_fields = types
        .get(row_type_ref)
        .and_then(|row_type| match &row_type.shape {
            IrTypeShape::Object { fields } => Some(fields.as_slice()),
            IrTypeShape::Scalar(_)
            | IrTypeShape::List { .. }
            | IrTypeShape::Map { .. }
            | IrTypeShape::Enum { .. }
            | IrTypeShape::Json => None,
        });
    for column in &projection.columns {
        let Some(field_name) = column.source_path.first() else {
            continue;
        };
        // A source path against a row that names nothing is not merely stale:
        // it resolves to null on every row, silently.
        let Some(fields) = row_fields else {
            return Err(ManifestError::validation(format!(
                "projection '{}' column '{}' reads field '{field_name}', but the rows operation '{operation_id}' yields have type '{row_type_ref}', which is not an object and names no fields",
                projection.name, column.name
            )));
        };
        let Some(field) = fields.iter().find(|field| field.name == *field_name) else {
            return Err(ManifestError::validation(format!(
                "projection '{}' column '{}' reads field '{field_name}', but the rows operation '{operation_id}' yields have type '{row_type_ref}', which has no such field",
                projection.name, column.name
            )));
        };
        // Generated and authored columns may both nest, and runtime follows
        // every segment it is given.
        let nested = column.source_path.get(1..).unwrap_or_default();
        if let Err(reason) = walk_source_path(&field.type_ref, nested, types) {
            return Err(ManifestError::validation(format!(
                "projection '{}' column '{}' reads source path '{}', but {reason}",
                projection.name,
                column.name,
                column.source_path.join(".")
            )));
        }
    }
    Ok(())
}

/// Follows the segments below a column's first, stopping as soon as the type it
/// has reached can no longer say anything about the rest.
///
/// `get_path_value` reads a segment that parses as an integer only as an array
/// index, never as a key, so whether a segment is numeric decides as much as the
/// shape it lands on does. Established top-level projection columns can contain
/// a numeric *first* segment for a resource whose field really is named `0`, and
/// those columns already resolve to null; rejecting them here would invalidate
/// existing generated catalogs. The nested scalar-leaf generator omits numeric
/// object keys, so the rule applies below the first segment to reject authored
/// overrides and stale generated artifacts that runtime cannot read.
fn walk_source_path(
    type_ref: &str,
    segments: &[String],
    types: &BTreeMap<&str, &IrType>,
) -> std::result::Result<(), String> {
    let mut type_ref = type_ref;
    for segment in segments {
        // A type the IR does not carry describes nothing, exactly like `json`.
        let Some(ty) = types.get(type_ref) else {
            return Ok(());
        };
        let numeric = segment.parse::<usize>().is_ok();
        type_ref = match &ty.shape {
            IrTypeShape::Object { fields } => {
                if numeric {
                    return Err(numeric_segment_error(type_ref, segment));
                }
                let Some(field) = fields.iter().find(|field| field.name == *segment) else {
                    return Err(format!("type '{type_ref}' has no field '{segment}'"));
                };
                field.type_ref.as_str()
            }
            // Any key selects a map value, so only a numeric one is ruled out.
            IrTypeShape::Map { value_type_ref } => {
                if numeric {
                    return Err(numeric_segment_error(type_ref, segment));
                }
                value_type_ref.as_str()
            }
            // An array is the one shape a numeric segment does address.
            IrTypeShape::List { item_type_ref } => {
                if !numeric {
                    return Err(format!(
                        "type '{type_ref}' is a list, so segment '{segment}' must be a numeric index"
                    ));
                }
                item_type_ref.as_str()
            }
            // Opaque payloads are unknowable at import time, not known-wrong.
            IrTypeShape::Json => return Ok(()),
            IrTypeShape::Scalar(_) | IrTypeShape::Enum { .. } => {
                return Err(format!(
                    "type '{type_ref}' names no fields, so segment '{segment}' cannot be selected"
                ));
            }
        };
    }
    Ok(())
}

fn numeric_segment_error(type_ref: &str, segment: &str) -> String {
    format!(
        "type '{type_ref}' is keyed by name, but segment '{segment}' is read as an array index and so selects nothing"
    )
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
