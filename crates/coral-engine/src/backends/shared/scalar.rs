//! Scalar conversions shared by provider request boundaries.

use coral_spec::ManifestDataType;
use datafusion::arrow::datatypes::DataType;
use datafusion::scalar::ScalarValue;

pub(crate) fn timestamp_to_rfc3339(value: &ScalarValue) -> Option<String> {
    if !matches!(
        value,
        ScalarValue::TimestampSecond(_, _)
            | ScalarValue::TimestampMillisecond(_, _)
            | ScalarValue::TimestampMicrosecond(_, _)
            | ScalarValue::TimestampNanosecond(_, _)
    ) {
        return None;
    }

    value
        .cast_to(&crate::types::arrow_data_type(ManifestDataType::Timestamp))
        .ok()?
        .cast_to(&DataType::Utf8)
        .ok()?
        .try_as_str()
        .flatten()
        .map(str::to_owned)
}
