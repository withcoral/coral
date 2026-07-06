//! Canonical conversions between the coral-spec scalar vocabulary and
//! Arrow/DataFusion types.
//!
//! This module is the home for `ManifestDataType`-to-Arrow *type-level*
//! policies, so that when more than one lowering exists (column schemas
//! want real Arrow types; string-shaped parameter binding wants plain
//! strings) the policies sit adjacent and reviewable. Today it holds the
//! column-schema lowering; value-level `ManifestDataType` switches still
//! live with their backends (`convert_items` in `backends/shared/mapping.rs`
//! builds Arrow arrays per variant, and `coerce_filter_value` /
//! `coerce_call_arg_value` in the MCP backend coerce JSON values). All of
//! those matches are wildcard-free, so adding a `ManifestDataType` variant
//! breaks each of them loudly.

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
