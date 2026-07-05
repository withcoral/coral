//! Coral-specific string and temporal scalar functions used by virtual graph SQL lowering.

use std::any::Any;
use std::sync::Arc;

use arrow::array::temporal_conversions::{
    NANOSECONDS_IN_DAY, date32_to_datetime, time64ns_to_time, time64us_to_time,
    timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_s_to_datetime,
    timestamp_us_to_datetime,
};
use arrow::array::types::IntervalMonthDayNanoType;
use arrow::array::{
    Array, ArrayRef, Int64Builder, IntervalMonthDayNanoBuilder, ListBuilder, StringBuilder,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};
use chrono::{Datelike, Months, NaiveDate, NaiveDateTime, NaiveTime};
use datafusion::common::cast::{
    as_date32_array, as_interval_mdn_array, as_large_string_array, as_string_array,
    as_string_view_array, as_time64_microsecond_array, as_time64_nanosecond_array,
    as_timestamp_microsecond_array, as_timestamp_millisecond_array, as_timestamp_nanosecond_array,
    as_timestamp_second_array,
};
use datafusion::common::{DataFusionError, Result as DataFusionResult, exec_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(crate) fn register_string_functions(
    registry: &mut dyn FunctionRegistry,
) -> DataFusionResult<()> {
    registry.register_udf(Arc::new(ScalarUDF::from(StringIndices::new())))?;
    registry.register_udf(Arc::new(ScalarUDF::from(DurationToIso::new())))?;
    registry.register_udf(Arc::new(ScalarUDF::from(ZonedDateTimeToIso::new())))?;
    registry.register_udf(Arc::new(ScalarUDF::from(DurationPart::new())))?;
    registry.register_udf(Arc::new(ScalarUDF::from(DurationBetween::new(
        "coral_duration_between",
        DurationBetweenMode::Full,
    ))))?;
    registry.register_udf(Arc::new(ScalarUDF::from(DurationBetween::new(
        "coral_duration_in_months",
        DurationBetweenMode::MonthsOnly,
    ))))?;
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct ZonedDateTimeToIso {
    signature: Signature,
}

impl ZonedDateTimeToIso {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ZonedDateTimeToIso {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "coral_zoneddatetime_to_iso"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let [timestamp, timezone] = args.args.as_slice() else {
            return exec_err!("coral_zoneddatetime_to_iso expects exactly two arguments");
        };
        let timestamp = timestamp.to_array_of_size(args.number_rows)?;
        let timezone = timezone.to_array_of_size(args.number_rows)?;
        Ok(ColumnarValue::Array(zoned_datetime_to_iso_array(
            &timestamp,
            &timezone,
            args.number_rows,
        )?))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct DurationPart {
    signature: Signature,
}

impl DurationPart {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::Interval(IntervalUnit::MonthDayNano),
                    DataType::Utf8,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for DurationPart {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "coral_duration_part"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let [duration, unit] = args.args.as_slice() else {
            return exec_err!("coral_duration_part expects exactly two arguments");
        };
        let duration = duration.to_array_of_size(args.number_rows)?;
        let unit = unit.to_array_of_size(args.number_rows)?;
        Ok(ColumnarValue::Array(duration_part_array(
            &duration,
            &unit,
            args.number_rows,
        )?))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum DurationBetweenMode {
    Full,
    MonthsOnly,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct DurationBetween {
    name: &'static str,
    mode: DurationBetweenMode,
    signature: Signature,
}

impl DurationBetween {
    fn new(name: &'static str, mode: DurationBetweenMode) -> Self {
        Self {
            name,
            mode,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DurationBetween {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Interval(IntervalUnit::MonthDayNano))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let [start, end] = args.args.as_slice() else {
            return exec_err!("{} expects exactly two arguments", self.name);
        };
        let start = start.to_array_of_size(args.number_rows)?;
        let end = end.to_array_of_size(args.number_rows)?;
        Ok(ColumnarValue::Array(duration_between_array(
            &start,
            &end,
            args.number_rows,
            self.mode,
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
            indices.push(
                i64::try_from(char_index)
                    .map_err(|error| internal_error(format!("string index overflow: {error}")))?,
            );
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

fn zoned_datetime_to_iso_array(
    timestamp: &ArrayRef,
    timezone: &ArrayRef,
    rows: usize,
) -> DataFusionResult<ArrayRef> {
    if !matches!(timestamp.data_type(), DataType::Timestamp(_, Some(_))) {
        return exec_err!(
            "coral_zoneddatetime_to_iso expects a zoned timestamp argument, got {}",
            timestamp.data_type()
        );
    }
    let timestamp_text = cast(timestamp.as_ref(), &DataType::Utf8)?;
    let mut builder = StringBuilder::new();
    for row in 0..rows {
        if timestamp.is_null(row) {
            builder.append_null();
            continue;
        }
        let Some(timestamp_text) = string_value(timestamp_text.as_ref(), row)? else {
            builder.append_null();
            continue;
        };
        let Some(timezone) = string_value(timezone.as_ref(), row)? else {
            builder.append_null();
            continue;
        };
        builder.append_value(zoned_datetime_to_iso_string(timestamp_text, timezone));
    }
    Ok(Arc::new(builder.finish()))
}

fn zoned_datetime_to_iso_string(timestamp_text: &str, timezone: &str) -> String {
    if is_fixed_offset_timezone(timezone) {
        timestamp_text.to_string()
    } else {
        format!("{timestamp_text}[{timezone}]")
    }
}

fn is_fixed_offset_timezone(timezone: &str) -> bool {
    if timezone.eq_ignore_ascii_case("z") {
        return true;
    }
    let mut chars = timezone.chars();
    let Some(sign @ ('+' | '-')) = chars.next() else {
        return false;
    };
    let rest = chars.collect::<String>();
    let Some((hours, minutes)) = rest.split_once(':') else {
        return false;
    };
    matches!(sign, '+' | '-')
        && hours.len() == 2
        && minutes.len() == 2
        && hours.chars().all(|value| value.is_ascii_digit())
        && minutes.chars().all(|value| value.is_ascii_digit())
}

fn duration_part_array(
    duration: &ArrayRef,
    unit: &ArrayRef,
    rows: usize,
) -> DataFusionResult<ArrayRef> {
    let duration = as_interval_mdn_array(duration.as_ref())?;
    let mut builder = Int64Builder::new();
    for row in 0..rows {
        if duration.is_null(row) {
            builder.append_null();
            continue;
        }
        let Some(unit) = string_value(unit.as_ref(), row)? else {
            builder.append_null();
            continue;
        };
        let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(duration.value(row));
        builder.append_value(duration_part_value(months, days, nanos, unit)?);
    }
    Ok(Arc::new(builder.finish()))
}

fn duration_part_value(months: i32, days: i32, nanos: i64, unit: &str) -> DataFusionResult<i64> {
    let months = i64::from(months);
    let days = i64::from(days);
    match unit {
        "years" => Ok(months / 12),
        "quarters" => Ok(months / 3),
        "months" => Ok(months),
        "weeks" => Ok(days / 7),
        "days" => Ok(days),
        "hours" => Ok(nanos / 3_600_000_000_000),
        "minutes" => Ok(nanos / 60_000_000_000),
        "seconds" => Ok(nanos / 1_000_000_000),
        "milliseconds" => Ok(nanos / 1_000_000),
        "microseconds" => Ok(nanos / 1_000),
        "nanoseconds" => Ok(nanos),
        "quartersOfYear" => Ok((months / 3) % 4),
        "monthsOfQuarter" => Ok(months % 3),
        "monthsOfYear" => Ok(months % 12),
        "daysOfWeek" => Ok(days % 7),
        "minutesOfHour" => Ok((nanos / 60_000_000_000) % 60),
        "secondsOfMinute" => Ok((nanos / 1_000_000_000) % 60),
        "millisecondsOfSecond" => Ok((nanos / 1_000_000) % 1_000),
        "microsecondsOfSecond" => Ok((nanos / 1_000) % 1_000_000),
        "nanosecondsOfSecond" => Ok(nanos % 1_000_000_000),
        _ => exec_err!("coral_duration_part does not support component {unit:?}"),
    }
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

fn duration_between_array(
    start: &ArrayRef,
    end: &ArrayRef,
    rows: usize,
    mode: DurationBetweenMode,
) -> DataFusionResult<ArrayRef> {
    let mut builder = IntervalMonthDayNanoBuilder::with_capacity(rows);
    for row in 0..rows {
        let Some(start) = temporal_input_value(start.as_ref(), row)? else {
            builder.append_null();
            continue;
        };
        let Some(end) = temporal_input_value(end.as_ref(), row)? else {
            builder.append_null();
            continue;
        };
        let parts = duration_between(start, end, mode)?;
        builder.append_value(IntervalMonthDayNanoType::make_value(
            parts.months,
            parts.days,
            parts.nanos,
        ));
    }
    Ok(Arc::new(builder.finish()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DurationParts {
    months: i32,
    days: i32,
    nanos: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemporalInput {
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Time(NaiveTime),
}

impl TemporalInput {
    fn date(self) -> Option<NaiveDate> {
        match self {
            Self::Date(date) => Some(date),
            Self::DateTime(datetime) => Some(datetime.date()),
            Self::Time(_) => None,
        }
    }

    fn anchored_datetime(self, peer: Self) -> DataFusionResult<NaiveDateTime> {
        match self {
            Self::Date(date) => Ok(date.and_time(NaiveTime::MIN)),
            Self::DateTime(datetime) => Ok(datetime),
            Self::Time(time) => Ok(peer.date().unwrap_or(epoch_date()?).and_time(time)),
        }
    }
}

fn temporal_input_value(
    array: &dyn Array,
    index: usize,
) -> DataFusionResult<Option<TemporalInput>> {
    if array.is_null(index) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Date32 => {
            let datetime =
                date32_to_datetime(as_date32_array(array)?.value(index)).ok_or_else(|| {
                    internal_error("coral_duration_between received out-of-range Date32 value")
                })?;
            Ok(Some(TemporalInput::Date(datetime.date())))
        }
        DataType::Timestamp(TimeUnit::Second, None) => Ok(Some(TemporalInput::DateTime(
            timestamp_s_to_datetime(as_timestamp_second_array(array)?.value(index)).ok_or_else(
                || {
                    internal_error(
                        "coral_duration_between received out-of-range timestamp-second value",
                    )
                },
            )?,
        ))),
        DataType::Timestamp(TimeUnit::Millisecond, None) => Ok(Some(TemporalInput::DateTime(
            timestamp_ms_to_datetime(as_timestamp_millisecond_array(array)?.value(index))
                .ok_or_else(|| {
                    internal_error(
                        "coral_duration_between received out-of-range timestamp-millisecond value",
                    )
                })?,
        ))),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Ok(Some(TemporalInput::DateTime(
            timestamp_us_to_datetime(as_timestamp_microsecond_array(array)?.value(index))
                .ok_or_else(|| {
                    internal_error(
                        "coral_duration_between received out-of-range timestamp-microsecond value",
                    )
                })?,
        ))),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Ok(Some(TemporalInput::DateTime(
            timestamp_ns_to_datetime(as_timestamp_nanosecond_array(array)?.value(index))
                .ok_or_else(|| {
                    internal_error(
                        "coral_duration_between received out-of-range timestamp-nanosecond value",
                    )
                })?,
        ))),
        DataType::Time64(TimeUnit::Microsecond) => Ok(Some(TemporalInput::Time(
            time64us_to_time(as_time64_microsecond_array(array)?.value(index)).ok_or_else(
                || {
                    internal_error(
                        "coral_duration_between received out-of-range time-microsecond value",
                    )
                },
            )?,
        ))),
        DataType::Time64(TimeUnit::Nanosecond) => Ok(Some(TemporalInput::Time(
            time64ns_to_time(as_time64_nanosecond_array(array)?.value(index)).ok_or_else(|| {
                internal_error("coral_duration_between received out-of-range time-nanosecond value")
            })?,
        ))),
        data_type => exec_err!(
            "coral_duration_between expects date, timestamp, or time arguments, got {data_type}"
        ),
    }
}

fn duration_between(
    start: TemporalInput,
    end: TemporalInput,
    mode: DurationBetweenMode,
) -> DataFusionResult<DurationParts> {
    let start_datetime = start.anchored_datetime(end)?;
    let end_datetime = end.anchored_datetime(start)?;
    let months = whole_months_between(start_datetime, end_datetime);
    if matches!(mode, DurationBetweenMode::MonthsOnly) {
        return Ok(DurationParts {
            months: i32::try_from(months).map_err(|error| {
                internal_error(format!("duration month component overflow: {error}"))
            })?,
            days: 0,
            nanos: 0,
        });
    }

    let candidate = checked_add_signed_months(start_datetime, months)?;
    let remainder_nanos = (end_datetime - candidate)
        .num_nanoseconds()
        .ok_or_else(|| {
            internal_error("duration day/time remainder exceeded chrono nanosecond range")
        })?;
    Ok(DurationParts {
        months: i32::try_from(months).map_err(|error| {
            internal_error(format!("duration month component overflow: {error}"))
        })?,
        days: i32::try_from(remainder_nanos / NANOSECONDS_IN_DAY)
            .map_err(|error| internal_error(format!("duration day component overflow: {error}")))?,
        nanos: remainder_nanos % NANOSECONDS_IN_DAY,
    })
}

fn whole_months_between(start: NaiveDateTime, end: NaiveDateTime) -> i64 {
    // Neo4j DurationValue.between follows java.time LocalDate.until(MONTHS):
    // pack proleptic-month and day-of-month before dividing by 32, so a
    // chrono-clamped arrival day before the start day is not a whole month.
    let packed_start = packed_date_for_java_until_months(start.date());
    let packed_end = packed_date_for_java_until_months(end.date());
    (packed_end - packed_start) / 32
}

fn packed_date_for_java_until_months(date: NaiveDate) -> i64 {
    (i64::from(date.year()) * 12 + i64::from(date.month0())) * 32 + i64::from(date.day())
}

fn checked_add_signed_months(
    datetime: NaiveDateTime,
    months: i64,
) -> DataFusionResult<NaiveDateTime> {
    let month_count = u32::try_from(months.unsigned_abs())
        .map_err(|error| internal_error(format!("duration month component overflow: {error}")))?;
    let month_delta = Months::new(month_count);
    let shifted = if months >= 0 {
        datetime.checked_add_months(month_delta)
    } else {
        datetime.checked_sub_months(month_delta)
    };
    shifted.ok_or_else(|| internal_error("duration month advance is out of range"))
}

fn epoch_date() -> DataFusionResult<NaiveDate> {
    NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or_else(|| internal_error("chrono could not construct Unix epoch date"))
}

fn internal_error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Internal(message.into())
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
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

    use super::{
        DurationBetweenMode, TemporalInput, duration_between, duration_part_value,
        duration_to_iso_string, string_indices, zoned_datetime_to_iso_string,
    };

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

    #[test]
    fn zoned_datetime_to_iso_appends_only_named_timezone_brackets() {
        assert_eq!(
            zoned_datetime_to_iso_string("2020-06-01T09:00:00+01:00", "Europe/London"),
            "2020-06-01T09:00:00+01:00[Europe/London]"
        );
        assert_eq!(
            zoned_datetime_to_iso_string("2020-06-01T09:00:00+01:00", "+01:00"),
            "2020-06-01T09:00:00+01:00"
        );
        assert_eq!(
            zoned_datetime_to_iso_string("2020-06-01T08:00:00Z", "+00:00"),
            "2020-06-01T08:00:00Z"
        );
        assert_eq!(
            zoned_datetime_to_iso_string("2020-06-01T08:00:00Z", "Z"),
            "2020-06-01T08:00:00Z"
        );
    }

    #[test]
    fn duration_part_value_matches_pinned_component_formulas() {
        let tck_duration = (16, 10, 3_661_111_111_111);
        for (unit, expected) in [
            ("years", 1),
            ("quarters", 5),
            ("months", 16),
            ("weeks", 1),
            ("days", 10),
            ("hours", 1),
            ("minutes", 61),
            ("seconds", 3_661),
            ("milliseconds", 3_661_111),
            ("microseconds", 3_661_111_111),
            ("nanoseconds", 3_661_111_111_111),
            ("quartersOfYear", 1),
            ("monthsOfQuarter", 1),
            ("monthsOfYear", 4),
            ("daysOfWeek", 3),
            ("minutesOfHour", 1),
            ("secondsOfMinute", 1),
            ("millisecondsOfSecond", 111),
            ("microsecondsOfSecond", 111_111),
            ("nanosecondsOfSecond", 111_111_111),
        ] {
            assert_eq!(
                duration_part_value(tck_duration.0, tck_duration.1, tck_duration.2, unit)
                    .expect("duration part should compute"),
                expected,
                "{unit}"
            );
        }

        assert_eq!(
            duration_part_value(14, 0, 0, "years").expect("years should compute"),
            1
        );
        assert_eq!(
            duration_part_value(14, 0, 0, "months").expect("months should compute"),
            14
        );
        assert_eq!(
            duration_part_value(14, 0, 0, "monthsOfYear").expect("monthsOfYear should compute"),
            2
        );

        assert_eq!(
            duration_part_value(-14, -10, -3_661_111_111_111, "monthsOfYear")
                .expect("negative month component should compute"),
            -2
        );
        assert_eq!(
            duration_part_value(-14, -10, -3_661_111_111_111, "secondsOfMinute")
                .expect("negative second component should compute"),
            -1
        );
        assert_eq!(
            duration_part_value(0, 0, 0, "nanosecondsOfSecond")
                .expect("zero duration component should compute"),
            0
        );
    }

    #[test]
    fn duration_between_matches_pinned_temporal10_rows() {
        assert_between_iso(
            date(1984, 10, 11),
            date(2015, 6, 24),
            DurationBetweenMode::Full,
            "P30Y8M13D",
        );
        assert_between_iso(
            date(1984, 10, 11),
            datetime("2016-07-21T21:45:22.142"),
            DurationBetweenMode::Full,
            "P31Y9M10DT21H45M22.142S",
        );
        assert_between_iso(
            date(1984, 10, 11),
            date(2015, 6, 24),
            DurationBetweenMode::MonthsOnly,
            "P30Y8M",
        );
        assert_between_iso(
            datetime("2015-07-21T21:40:32.142"),
            date(2015, 6, 24),
            DurationBetweenMode::Full,
            "P-27DT-21H-40M-32.142S",
        );
        assert_between_iso(
            time("14:30:00"),
            datetime("2016-07-21T21:45:22.142"),
            DurationBetweenMode::Full,
            "PT7H15M22.142S",
        );
    }

    #[test]
    fn duration_between_pins_documented_month_end_and_negative_cases() {
        // These month-end rows are not present in upstream Temporal10. They document
        // java.time LocalDate.until(MONTHS), which Neo4j DurationValue.between
        // wraps: month counts are computed from packed proleptic-month/day values,
        // then chrono's checked_add_months supplies the TCK-consistent anchor.
        assert_between_iso(
            date(2020, 1, 31),
            date(2020, 2, 29),
            DurationBetweenMode::Full,
            "P29D",
        );
        assert_between_iso(
            date(2020, 1, 31),
            date(2020, 3, 30),
            DurationBetweenMode::Full,
            "P1M30D",
        );
        assert_between_iso(
            date(2020, 1, 31),
            date(2020, 4, 30),
            DurationBetweenMode::Full,
            "P2M30D",
        );
        assert_between_iso(
            date(2015, 6, 24),
            date(1984, 10, 11),
            DurationBetweenMode::Full,
            "P-30Y-8M-13D",
        );
    }

    fn assert_between_iso(
        start: TemporalInput,
        end: TemporalInput,
        mode: DurationBetweenMode,
        expected: &str,
    ) {
        let parts = duration_between(start, end, mode).expect("duration should compute");
        assert_eq!(
            duration_to_iso_string(parts.months, parts.days, parts.nanos),
            expected
        );
    }

    fn date(year: i32, month: u32, day: u32) -> TemporalInput {
        TemporalInput::Date(NaiveDate::from_ymd_opt(year, month, day).expect("valid test date"))
    }

    fn datetime(text: &str) -> TemporalInput {
        TemporalInput::DateTime(
            NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
                .expect("valid test datetime"),
        )
    }

    fn time(text: &str) -> TemporalInput {
        TemporalInput::Time(
            NaiveTime::parse_from_str(text, "%H:%M:%S%.f").expect("valid test time"),
        )
    }
}
