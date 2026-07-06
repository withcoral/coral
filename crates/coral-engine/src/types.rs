//! Canonical conversions between the coral-spec scalar vocabulary and
//! Arrow/DataFusion types.
//!
//! Every `ManifestDataType`-to-Arrow lowering lives here so that the
//! policies stay adjacent and reviewable. There is deliberately more than
//! one lowering: column schemas want real Arrow types, while SQL parameter
//! binding lowers string-shaped types to plain strings. When adding a
//! `ManifestDataType` variant, the exhaustive matches in this module are
//! the engine-side checklist.

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
