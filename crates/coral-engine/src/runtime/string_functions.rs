//! Coral-specific string scalar functions used by virtual graph SQL lowering.

use std::any::Any;
use std::sync::Arc;

use arrow::array::types::IntervalMonthDayNanoType;
use arrow::array::{Array, ArrayRef, Int64Builder, ListBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field, IntervalUnit};
use datafusion::common::cast::{
    as_interval_mdn_array, as_large_string_array, as_string_array, as_string_view_array,
};
use datafusion::common::{Result as DataFusionResult, exec_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(crate) fn register_string_functions(
    registry: &mut dyn FunctionRegistry,
) -> DataFusionResult<()> {
    registry.register_udf(Arc::new(ScalarUDF::from(StringIndices::new())))?;
    registry.register_udf(Arc::new(ScalarUDF::from(DurationToIso::new())))?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct StringIndices {
    signature: Signature,
}

impl StringIndices {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StringIndices {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "coral_string_indices"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(indices_return_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let [source, pattern] = args.args.as_slice() else {
            return exec_err!("coral_string_indices expects exactly two arguments");
        };
        let source = source.to_array_of_size(args.number_rows)?;
        let pattern = pattern.to_array_of_size(args.number_rows)?;
        Ok(ColumnarValue::Array(string_indices_array(
            &source,
            &pattern,
            args.number_rows,
        )?))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct DurationToIso {
    signature: Signature,
}

impl DurationToIso {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Interval(IntervalUnit::MonthDayNano)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for DurationToIso {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "coral_duration_to_iso"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let [duration] = args.args.as_slice() else {
            return exec_err!("coral_duration_to_iso expects exactly one argument");
        };
        let duration = duration.to_array_of_size(args.number_rows)?;
        Ok(ColumnarValue::Array(duration_to_iso_array(
            &duration,
            args.number_rows,
        )?))
    }
}

fn indices_return_type() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)))
}

fn string_indices_array(
    source: &ArrayRef,
    pattern: &ArrayRef,
    rows: usize,
) -> DataFusionResult<ArrayRef> {
    let mut builder = ListBuilder::new(Int64Builder::new());
    for row in 0..rows {
        let Some(source) = string_value(source.as_ref(), row)? else {
            builder.append(false);
            continue;
        };
        let Some(pattern) = string_value(pattern.as_ref(), row)? else {
            builder.append(false);
            continue;
        };
        for index in string_indices(source, pattern)? {
            builder.values().append_value(index);
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}

fn string_value(array: &dyn Array, index: usize) -> DataFusionResult<Option<&str>> {
    if array.is_null(index) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Utf8 => Ok(Some(as_string_array(array)?.value(index))),
        DataType::Utf8View => Ok(Some(as_string_view_array(array)?.value(index))),
        DataType::LargeUtf8 => Ok(Some(as_large_string_array(array)?.value(index))),
        data_type => exec_err!("coral_string_indices expects string arguments, got {data_type}"),
    }
}

fn string_indices(source: &str, pattern: &str) -> DataFusionResult<Vec<i64>> {
    if pattern.is_empty() {
        return Ok(Vec::new());
    }

    let mut indices = Vec::new();
    for (char_index, (byte_index, _)) in source.char_indices().enumerate() {
        if source
            .get(byte_index..)
            .is_some_and(|tail| tail.starts_with(pattern))
        {
            indices.push(i64::try_from(char_index).map_err(|error| {
                datafusion::common::DataFusionError::Internal(format!(
                    "string index overflow: {error}"
                ))
            })?);
        }
    }
    Ok(indices)
}

fn duration_to_iso_array(duration: &ArrayRef, rows: usize) -> DataFusionResult<ArrayRef> {
    let duration = as_interval_mdn_array(duration.as_ref())?;
    let mut builder = StringBuilder::new();
    for row in 0..rows {
        if duration.is_null(row) {
            builder.append_null();
            continue;
        }
        let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(duration.value(row));
        builder.append_value(duration_to_iso_string(months, days, nanos));
    }
    Ok(Arc::new(builder.finish()))
}

fn duration_to_iso_string(months: i32, days: i32, nanos: i64) -> String {
    if months == 0 && days == 0 && nanos == 0 {
        return "PT0S".to_string();
    }

    let mut output = String::from("P");
    let years = months / 12;
    let month_remainder = months % 12;
    if years != 0 {
        output.push_str(&years.to_string());
        output.push('Y');
    }
    if month_remainder != 0 {
        output.push_str(&month_remainder.to_string());
        output.push('M');
    }
    if days != 0 {
        output.push_str(&days.to_string());
        output.push('D');
    }
    if nanos != 0 {
        output.push('T');
        let hours = nanos / 3_600_000_000_000;
        let hour_remainder = nanos % 3_600_000_000_000;
        let minutes = hour_remainder / 60_000_000_000;
        let minute_remainder = hour_remainder % 60_000_000_000;
        append_duration_time_component(&mut output, hours, 'H');
        append_duration_time_component(&mut output, minutes, 'M');
        append_duration_seconds(&mut output, minute_remainder, hours == 0 && minutes == 0);
    }
    output
}

fn append_duration_time_component(output: &mut String, value: i64, suffix: char) {
    if value != 0 {
        output.push_str(&value.to_string());
        output.push(suffix);
    }
}

fn append_duration_seconds(output: &mut String, nanos: i64, force: bool) {
    let whole = nanos / 1_000_000_000;
    let fractional = nanos % 1_000_000_000;
    if whole == 0 && fractional == 0 && !force {
        return;
    }
    if fractional == 0 {
        output.push_str(&whole.to_string());
        output.push('S');
        return;
    }

    if whole < 0 || fractional < 0 {
        output.push('-');
    }
    output.push_str(&whole.abs().to_string());
    output.push('.');
    let mut fractional = format!("{:09}", fractional.abs());
    while fractional.ends_with('0') {
        fractional.pop();
    }
    output.push_str(&fractional);
    output.push('S');
}

#[cfg(test)]
mod tests {
    use super::{duration_to_iso_string, string_indices};

    #[test]
    fn string_indices_returns_zero_based_character_positions() {
        assert_eq!(
            string_indices("banana", "ana").expect("indices should compute"),
            vec![1, 3]
        );
        assert_eq!(
            string_indices("éclairé", "é").expect("indices should compute"),
            vec![0, 6]
        );
    }

    #[test]
    fn string_indices_handles_empty_or_missing_patterns() {
        assert!(
            string_indices("abc", "")
                .expect("indices should compute")
                .is_empty()
        );
        assert!(
            string_indices("abc", "z")
                .expect("indices should compute")
                .is_empty()
        );
    }

    #[test]
    fn duration_to_iso_formats_pinned_temporal6_rows() {
        for (months, days, nanos, expected) in [
            (149, 14, 58_390_000_000_001, "P12Y5M14DT16H13M10.000000001S"),
            (149, -14, 57_600_000_000_000, "P12Y5M-14DT16H"),
            (0, 0, 660_000_000_000, "PT11M"),
            (0, 0, 1_999_000_000, "PT1.999S"),
            (0, 0, -60_001_000_000, "PT-1M-0.001S"),
            (0, 1, 1_000_000, "P1DT0.001S"),
            (0, 0, 0, "PT0S"),
        ] {
            assert_eq!(duration_to_iso_string(months, days, nanos), expected);
        }
    }
}
