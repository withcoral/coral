//! Canonical conversions between the coral-spec scalar vocabulary and
//! Arrow/DataFusion types.
//!
//! This module is the home for `ManifestDataType`-to-Arrow *type-level*
//! policies, so that when more than one lowering exists (column schemas
//! want real Arrow types; string-shaped parameter binding wants plain
//! strings) the policies sit adjacent and reviewable. It holds column-schema
//! lowering, SQL parameter binding lowering, Arrow-to-manifest inference, and
//! type-claim compatibility. Value-level `ManifestDataType` switches still live
//! with their backends (`convert_items` in `backends/shared/mapping.rs` builds
//! Arrow arrays per variant, and `coerce_filter_value` / `coerce_call_arg_value`
//! in the MCP backend coerce JSON values). All of those matches are
//! wildcard-free, so adding a `ManifestDataType` variant breaks each of them
//! loudly.

use coral_spec::ManifestDataType;
use datafusion::arrow::datatypes::{DataType, TimeUnit};

/// Lowers a manifest data type into the Arrow type used for table and
/// result-column schemas.
///
/// This is the column-schema policy: `Timestamp` becomes a real
/// microsecond-precision UTC Arrow timestamp, and `Json` is stored as text.
pub(crate) fn arrow_column_type(data_type: ManifestDataType) -> DataType {
    match data_type {
        ManifestDataType::Utf8 | ManifestDataType::Json => DataType::Utf8,
        ManifestDataType::Int64 => DataType::Int64,
        ManifestDataType::Boolean => DataType::Boolean,
        ManifestDataType::Float64 => DataType::Float64,
        ManifestDataType::Timestamp => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        }
    }
}

/// Lowers a manifest data type into the Arrow type used while binding SQL
/// parameter placeholders.
///
/// This is the parameter-binding policy: `Json` and `Timestamp` are declared
/// manifest types, but they bind through `DataFusion` as string-shaped values.
pub(crate) fn arrow_parameter_type(data_type: ManifestDataType) -> DataType {
    match data_type {
        ManifestDataType::Utf8 | ManifestDataType::Json | ManifestDataType::Timestamp => {
            DataType::Utf8
        }
        ManifestDataType::Int64 => DataType::Int64,
        ManifestDataType::Boolean => DataType::Boolean,
        ManifestDataType::Float64 => DataType::Float64,
    }
}

/// Returns whether a manifest type binds through `DataFusion` SQL parameters as
/// one of the string-shaped Arrow values.
pub(crate) fn parameter_binding_is_string_shaped(data_type: ManifestDataType) -> bool {
    is_string_family(&arrow_parameter_type(data_type))
}

/// Resolves a DataFusion/Arrow inferred parameter type into Coral manifest
/// spelling when the type can be expressed in function metadata.
pub(crate) fn manifest_data_type_for_arrow(data_type: &DataType) -> Option<ManifestDataType> {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some(ManifestDataType::Utf8),
        DataType::Int64 => Some(ManifestDataType::Int64),
        DataType::Float64 => Some(ManifestDataType::Float64),
        DataType::Boolean => Some(ManifestDataType::Boolean),
        DataType::Timestamp(_, _) => Some(ManifestDataType::Timestamp),
        _ => None,
    }
}

/// Whether a declared manifest type is compatible with one Arrow-inferred
/// claim for the same SQL placeholder.
pub(crate) fn manifest_claim_accepts_arrow(declared: ManifestDataType, arrow: &DataType) -> bool {
    let declared_arrow = arrow_parameter_type(declared);
    declared_arrow == *arrow
        || (is_string_family(&declared_arrow) && is_string_family(arrow))
        || (declared == ManifestDataType::Float64 && matches!(arrow, DataType::Int64))
        || (declared == ManifestDataType::Timestamp && matches!(arrow, DataType::Timestamp(_, _)))
}

/// Whether an Arrow type is one of the string representations
/// (`Utf8`, `LargeUtf8`, `Utf8View`).
///
/// `DataFusion` may materialize any of the three for logically-string data,
/// so string-typed checks must accept the whole family.
pub(crate) fn is_string_family(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [ManifestDataType; 6] = [
        ManifestDataType::Utf8,
        ManifestDataType::Json,
        ManifestDataType::Int64,
        ManifestDataType::Float64,
        ManifestDataType::Boolean,
        ManifestDataType::Timestamp,
    ];

    fn assert_all_manifest_data_types_covered(data_type: ManifestDataType) {
        match data_type {
            ManifestDataType::Utf8
            | ManifestDataType::Json
            | ManifestDataType::Int64
            | ManifestDataType::Float64
            | ManifestDataType::Boolean
            | ManifestDataType::Timestamp => {}
        }
    }

    #[test]
    fn every_variant_accepts_its_own_lowerings() {
        for data_type in ALL {
            assert_all_manifest_data_types_covered(data_type);

            assert!(
                manifest_claim_accepts_arrow(data_type, &arrow_parameter_type(data_type)),
                "{data_type} should accept its parameter lowering"
            );
            assert!(
                manifest_claim_accepts_arrow(data_type, &arrow_column_type(data_type)),
                "{data_type} should accept its column lowering"
            );
        }
    }

    #[test]
    fn every_lowering_resolves_to_some_manifest_type() {
        for data_type in ALL {
            assert_all_manifest_data_types_covered(data_type);

            assert!(
                manifest_data_type_for_arrow(&arrow_parameter_type(data_type)).is_some(),
                "{data_type} parameter lowering should resolve to some manifest type"
            );
            assert!(
                manifest_data_type_for_arrow(&arrow_column_type(data_type)).is_some(),
                "{data_type} column lowering should resolve to some manifest type"
            );
        }
    }

    #[test]
    fn string_shaped_lowerings_resolve_to_utf8_not_their_semantic_type() {
        assert_eq!(
            manifest_data_type_for_arrow(&arrow_parameter_type(ManifestDataType::Json)),
            Some(ManifestDataType::Utf8)
        );
        assert_eq!(
            manifest_data_type_for_arrow(&arrow_parameter_type(ManifestDataType::Timestamp)),
            Some(ManifestDataType::Utf8)
        );
        assert_eq!(
            manifest_data_type_for_arrow(&arrow_column_type(ManifestDataType::Timestamp)),
            Some(ManifestDataType::Timestamp)
        );
        assert_eq!(
            manifest_data_type_for_arrow(&DataType::LargeUtf8),
            Some(ManifestDataType::Utf8)
        );
        assert_eq!(
            manifest_data_type_for_arrow(&DataType::Utf8View),
            Some(ManifestDataType::Utf8)
        );
    }

    #[test]
    fn string_shaped_parameter_binding_follows_parameter_lowering() {
        for data_type in ALL {
            assert_eq!(
                parameter_binding_is_string_shaped(data_type),
                is_string_family(&arrow_parameter_type(data_type)),
                "{data_type} should follow its parameter lowering"
            );
        }

        assert!(parameter_binding_is_string_shaped(ManifestDataType::Utf8));
        assert!(parameter_binding_is_string_shaped(ManifestDataType::Json));
        assert!(parameter_binding_is_string_shaped(
            ManifestDataType::Timestamp
        ));
        assert!(!parameter_binding_is_string_shaped(ManifestDataType::Int64));
        assert!(!parameter_binding_is_string_shaped(
            ManifestDataType::Float64
        ));
        assert!(!parameter_binding_is_string_shaped(
            ManifestDataType::Boolean
        ));
    }
}
