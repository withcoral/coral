//! Canonical internal identity for native provider rows.

use arrow::array::{
    Array, BooleanArray, Decimal128Array, Decimal256Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeStringArray, StringArray, StringViewArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use coral_spec::ManifestDataType;
use sha2::{Digest as _, Sha256};

use crate::sources::universal_search::{
    ResolvedUniversalSearchResultField, ResolvedUniversalSearchRoute,
};
use crate::workspaces::WorkspaceName;

const TAG_VERSION: u8 = 0x01;
const TAG_WORKSPACE: u8 = 0x02;
const TAG_SOURCE_NAME: u8 = 0x03;
const TAG_INSTALLATION_REVISION: u8 = 0x04;
const TAG_AUTHORED_ROUTE: u8 = 0x05;
const TAG_INFERRED_V4_OPERATION: u8 = 0x08;
const TAG_ENTITY_TYPE: u8 = 0x09;
const TAG_ABSENT: u8 = 0x0a;
const TAG_IDENTITY_FIELDS: u8 = 0x0b;
const TAG_PROVIDER_ID: u8 = 0x0c;
const TAG_URL: u8 = 0x0d;

const TAG_TEXT: u8 = 0x20;
const TAG_BOOL: u8 = 0x21;
const TAG_I8: u8 = 0x22;
const TAG_I16: u8 = 0x23;
const TAG_I32: u8 = 0x24;
const TAG_I64: u8 = 0x25;
const TAG_U8: u8 = 0x26;
const TAG_U16: u8 = 0x27;
const TAG_U32: u8 = 0x28;
const TAG_U64: u8 = 0x29;
const TAG_DECIMAL128: u8 = 0x2a;
const TAG_DECIMAL256: u8 = 0x2b;
const TAG_TIMESTAMP: u8 = 0x2c;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(in crate::search) struct NativeIdentity([u8; 32]);

impl NativeIdentity {
    pub(in crate::search) fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

pub(super) fn identity_for_row(
    workspace: &WorkspaceName,
    route: &ResolvedUniversalSearchRoute,
    batch: &RecordBatch,
    row: usize,
    provider_id: Option<&str>,
    url: Option<&str>,
) -> Option<NativeIdentity> {
    let prefix = identity_prefix(workspace, route)?;
    if !route.result.identity_fields.is_empty() {
        let mut hasher = prefix.clone();
        append_component(&mut hasher, TAG_IDENTITY_FIELDS, &[])?;
        let valid =
            route.result.identity_fields.iter().all(|field| {
                append_identity_field(&mut hasher, batch, row, field).unwrap_or(false)
            });
        if valid {
            return Some(finish(hasher));
        }
    }
    if let Some(provider_id) = provider_id {
        let mut hasher = prefix.clone();
        append_component(&mut hasher, TAG_PROVIDER_ID, provider_id.as_bytes())?;
        return Some(finish(hasher));
    }
    if let Some(url) = url {
        let mut hasher = prefix;
        append_component(&mut hasher, TAG_URL, url.as_bytes())?;
        return Some(finish(hasher));
    }
    None
}

fn identity_prefix(
    workspace: &WorkspaceName,
    route: &ResolvedUniversalSearchRoute,
) -> Option<Sha256> {
    let mut hasher = Sha256::new();
    append_component(&mut hasher, TAG_VERSION, b"native/v1")?;
    append_component(&mut hasher, TAG_WORKSPACE, workspace.as_str().as_bytes())?;
    append_component(&mut hasher, TAG_SOURCE_NAME, route.source_name.as_bytes())?;
    append_component(
        &mut hasher,
        TAG_INSTALLATION_REVISION,
        route.installation_revision.as_bytes(),
    )?;
    match route.authored_route_id.as_deref() {
        Some(route_id) => append_component(&mut hasher, TAG_AUTHORED_ROUTE, route_id.as_bytes())?,
        None => append_component(
            &mut hasher,
            TAG_INFERRED_V4_OPERATION,
            route.target.operation_id.as_bytes(),
        )?,
    }
    match route.result.entity_type.as_deref() {
        Some(entity_type) => {
            append_component(&mut hasher, TAG_ENTITY_TYPE, entity_type.as_bytes())?;
        }
        None => append_component(&mut hasher, TAG_ABSENT, &[])?,
    }
    Some(hasher)
}

fn append_identity_field(
    hasher: &mut Sha256,
    batch: &RecordBatch,
    row: usize,
    field: &ResolvedUniversalSearchResultField,
) -> Option<bool> {
    let column = unique_column(batch, &field.column_name)?;
    if !manifest_type_matches(column.data_type(), field.data_type) {
        return Some(false);
    }
    Some(append_array_value(hasher, column.as_ref(), row))
}

fn unique_column<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a arrow::array::ArrayRef> {
    let schema = batch.schema();
    let mut matches = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name() == name)
        .map(|(index, _)| index);
    let index = matches.next()?;
    if matches.next().is_some() {
        return None;
    }
    batch.columns().get(index)
}

fn manifest_type_matches(data_type: &DataType, expected: ManifestDataType) -> bool {
    match expected {
        ManifestDataType::Utf8 => matches!(
            data_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ),
        ManifestDataType::Int64 => matches!(data_type, DataType::Int64),
        ManifestDataType::Boolean => matches!(data_type, DataType::Boolean),
        ManifestDataType::Timestamp => matches!(data_type, DataType::Timestamp(_, _)),
        ManifestDataType::Float64 | ManifestDataType::Json => false,
    }
}

macro_rules! append_primitive {
    ($hasher:expr, $array:expr, $row:expr, $array_type:ty, $tag:expr) => {{
        $array
            .as_any()
            .downcast_ref::<$array_type>()
            .and_then(|array| append_component($hasher, $tag, &array.value($row).to_be_bytes()))
            .is_some()
    }};
}

fn append_array_value(hasher: &mut Sha256, array: &dyn Array, row: usize) -> bool {
    if row >= array.len() || array.is_null(row) {
        return false;
    }
    match array.data_type() {
        DataType::Utf8 => append_downcast_value::<StringArray>(hasher, array, row, TAG_TEXT),
        DataType::LargeUtf8 => {
            append_downcast_value::<LargeStringArray>(hasher, array, row, TAG_TEXT)
        }
        DataType::Utf8View => {
            append_downcast_value::<StringViewArray>(hasher, array, row, TAG_TEXT)
        }
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .and_then(|array| append_component(hasher, TAG_BOOL, &[u8::from(array.value(row))]))
            .is_some(),
        DataType::Int8 => append_primitive!(hasher, array, row, Int8Array, TAG_I8),
        DataType::Int16 => append_primitive!(hasher, array, row, Int16Array, TAG_I16),
        DataType::Int32 => append_primitive!(hasher, array, row, Int32Array, TAG_I32),
        DataType::Int64 => append_primitive!(hasher, array, row, Int64Array, TAG_I64),
        DataType::UInt8 => append_primitive!(hasher, array, row, UInt8Array, TAG_U8),
        DataType::UInt16 => append_primitive!(hasher, array, row, UInt16Array, TAG_U16),
        DataType::UInt32 => append_primitive!(hasher, array, row, UInt32Array, TAG_U32),
        DataType::UInt64 => append_primitive!(hasher, array, row, UInt64Array, TAG_U64),
        DataType::Decimal128(_, scale) => array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .and_then(|array| {
                let mut payload = Vec::with_capacity(17);
                payload.push((*scale).cast_unsigned());
                payload.extend_from_slice(&array.value(row).to_be_bytes());
                append_component(hasher, TAG_DECIMAL128, &payload)
            })
            .is_some(),
        DataType::Decimal256(_, scale) => array
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .and_then(|array| {
                let mut payload = Vec::with_capacity(33);
                payload.push((*scale).cast_unsigned());
                payload.extend_from_slice(&array.value(row).to_be_bytes());
                append_component(hasher, TAG_DECIMAL256, &payload)
            })
            .is_some(),
        DataType::Timestamp(unit, timezone) => {
            append_timestamp(hasher, array, row, *unit, timezone.as_deref())
        }
        _ => false,
    }
}

fn append_downcast_value<T>(hasher: &mut Sha256, array: &dyn Array, row: usize, tag: u8) -> bool
where
    T: Array + StringValue + 'static,
{
    array
        .as_any()
        .downcast_ref::<T>()
        .and_then(|array| append_component(hasher, tag, array.string_value(row).as_bytes()))
        .is_some()
}

trait StringValue {
    fn string_value(&self, row: usize) -> &str;
}

impl StringValue for StringArray {
    fn string_value(&self, row: usize) -> &str {
        self.value(row)
    }
}

impl StringValue for LargeStringArray {
    fn string_value(&self, row: usize) -> &str {
        self.value(row)
    }
}

impl StringValue for StringViewArray {
    fn string_value(&self, row: usize) -> &str {
        self.value(row)
    }
}

fn append_timestamp(
    hasher: &mut Sha256,
    array: &dyn Array,
    row: usize,
    unit: TimeUnit,
    timezone: Option<&str>,
) -> bool {
    let raw = match unit {
        TimeUnit::Second => timestamp_value::<TimestampSecondArray>(array, row),
        TimeUnit::Millisecond => timestamp_value::<TimestampMillisecondArray>(array, row),
        TimeUnit::Microsecond => timestamp_value::<TimestampMicrosecondArray>(array, row),
        TimeUnit::Nanosecond => timestamp_value::<TimestampNanosecondArray>(array, row),
    };
    let Some(raw) = raw else {
        return false;
    };
    let multiplier = match unit {
        TimeUnit::Second => 1_000_000_000_i128,
        TimeUnit::Millisecond => 1_000_000_i128,
        TimeUnit::Microsecond => 1_000_i128,
        TimeUnit::Nanosecond => 1_i128,
    };
    let mut payload = Vec::with_capacity(25 + timezone.map_or(0, str::len));
    payload.extend_from_slice(&(i128::from(raw) * multiplier).to_be_bytes());
    match timezone {
        Some(timezone) => {
            payload.push(1);
            let Ok(length) = u64::try_from(timezone.len()) else {
                return false;
            };
            payload.extend_from_slice(&length.to_be_bytes());
            payload.extend_from_slice(timezone.as_bytes());
        }
        None => payload.push(0),
    }
    append_component(hasher, TAG_TIMESTAMP, &payload).is_some()
}

fn timestamp_value<T>(array: &dyn Array, row: usize) -> Option<i64>
where
    T: Array + TimestampValue + 'static,
{
    array
        .as_any()
        .downcast_ref::<T>()
        .map(|array| array.timestamp_value(row))
}

trait TimestampValue {
    fn timestamp_value(&self, row: usize) -> i64;
}

macro_rules! impl_timestamp_value {
    ($($array_type:ty),+ $(,)?) => {
        $(
            impl TimestampValue for $array_type {
                fn timestamp_value(&self, row: usize) -> i64 {
                    self.value(row)
                }
            }
        )+
    };
}

impl_timestamp_value!(
    TimestampSecondArray,
    TimestampMillisecondArray,
    TimestampMicrosecondArray,
    TimestampNanosecondArray,
);

fn append_component(hasher: &mut Sha256, tag: u8, value: &[u8]) -> Option<()> {
    let length = u64::try_from(value.len()).ok()?;
    hasher.update([tag]);
    hasher.update(length.to_be_bytes());
    hasher.update(value);
    Some(())
}

fn finish(hasher: Sha256) -> NativeIdentity {
    NativeIdentity(hasher.finalize().into())
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        BinaryArray, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray,
        StringArray, StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    };
    use arrow::datatypes::{Int64Type, i256};
    use sha2::{Digest as _, Sha256};

    use super::{append_array_value, finish};

    fn digest(array: &dyn arrow::array::Array) -> Option<[u8; 32]> {
        let mut hasher = Sha256::new();
        append_array_value(&mut hasher, array, 0).then(|| finish(hasher).as_bytes())
    }

    #[test]
    fn logical_types_and_framed_text_do_not_collide() {
        assert_ne!(
            digest(&Int64Array::from(vec![1])),
            digest(&UInt64Array::from(vec![1]))
        );
        assert_ne!(
            digest(&Int64Array::from(vec![1])),
            digest(&StringArray::from(vec!["1"]))
        );

        let mut left = Sha256::new();
        assert!(append_array_value(
            &mut left,
            &StringArray::from(vec!["ab"]),
            0
        ));
        assert!(append_array_value(
            &mut left,
            &StringArray::from(vec!["c"]),
            0
        ));
        let mut right = Sha256::new();
        assert!(append_array_value(
            &mut right,
            &StringArray::from(vec!["a"]),
            0
        ));
        assert!(append_array_value(
            &mut right,
            &StringArray::from(vec!["bc"]),
            0
        ));
        assert_ne!(finish(left), finish(right));

        let integer_digests = [
            digest(&Int8Array::from(vec![1])),
            digest(&Int16Array::from(vec![1])),
            digest(&Int32Array::from(vec![1])),
            digest(&Int64Array::from(vec![1])),
            digest(&UInt8Array::from(vec![1])),
            digest(&UInt16Array::from(vec![1])),
            digest(&UInt32Array::from(vec![1])),
            digest(&UInt64Array::from(vec![1])),
        ];
        assert_eq!(
            integer_digests
                .into_iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            8
        );
    }

    #[test]
    fn strings_preserve_exact_unicode_without_storage_or_normalization_drift() {
        assert_eq!(
            digest(&StringArray::from(vec!["é"])),
            digest(&StringViewArray::from(vec!["é"]))
        );
        assert_ne!(
            digest(&StringArray::from(vec!["é"])),
            digest(&StringArray::from(vec!["e\u{301}"]))
        );
        assert_eq!(
            digest(&StringArray::from(vec!["é"])),
            Some([
                150, 45, 189, 33, 200, 91, 78, 134, 48, 205, 221, 235, 237, 48, 217, 191, 78, 201,
                111, 37, 223, 75, 116, 157, 232, 254, 169, 141, 251, 177, 66, 243,
            ])
        );
    }

    #[test]
    fn decimals_include_width_scale_sign_and_unscaled_value() {
        let decimal128 = arrow::array::Decimal128Array::from(vec![12_345_i128])
            .with_precision_and_scale(10, 2)
            .expect("decimal128");
        let other_scale = arrow::array::Decimal128Array::from(vec![12_345_i128])
            .with_precision_and_scale(10, 3)
            .expect("decimal128 scale");
        let negative = arrow::array::Decimal128Array::from(vec![-12_345_i128])
            .with_precision_and_scale(10, 2)
            .expect("negative decimal128");
        let decimal256 = arrow::array::Decimal256Array::from(vec![i256::from_i128(12_345)])
            .with_precision_and_scale(40, 2)
            .expect("decimal256");

        assert_ne!(digest(&decimal128), digest(&other_scale));
        assert_ne!(digest(&decimal128), digest(&negative));
        assert_ne!(digest(&decimal128), digest(&decimal256));
    }

    #[test]
    fn timestamps_normalize_units_but_preserve_explicit_timezone_metadata() {
        let seconds = TimestampSecondArray::from(vec![2]).with_timezone("UTC");
        let millis = TimestampMillisecondArray::from(vec![2_000]).with_timezone("UTC");
        let micros = TimestampMicrosecondArray::from(vec![2_000_000]).with_timezone("UTC");
        let nanos = TimestampNanosecondArray::from(vec![2_000_000_000]).with_timezone("UTC");
        assert_eq!(digest(&seconds), digest(&millis));
        assert_eq!(digest(&seconds), digest(&micros));
        assert_eq!(digest(&seconds), digest(&nanos));
        assert_ne!(
            digest(&seconds),
            digest(&TimestampSecondArray::from(vec![2]).with_timezone("+00:00"))
        );
        assert_ne!(
            digest(&seconds),
            digest(&TimestampSecondArray::from(vec![2]))
        );
        assert!(digest(&TimestampSecondArray::from(vec![i64::MAX])).is_some());
    }

    #[test]
    fn null_float_binary_and_nested_values_invalidate_identity() {
        let nested = ListArray::from_iter_primitive::<Int64Type, _, _>([Some(vec![Some(1)])]);
        assert_eq!(digest(&Int64Array::from(vec![None])), None);
        assert_eq!(digest(&Float64Array::from(vec![1.0])), None);
        assert_eq!(digest(&BinaryArray::from(vec![b"one".as_slice()])), None);
        assert_eq!(digest(&nested), None);
    }
}
