//! Coral-specific string scalar functions used by virtual graph SQL lowering.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Builder, ListBuilder};
use arrow::datatypes::{DataType, Field};
use datafusion::common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion::common::{Result as DataFusionResult, exec_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(crate) fn register_string_functions(
    registry: &mut dyn FunctionRegistry,
) -> DataFusionResult<()> {
    registry.register_udf(Arc::new(ScalarUDF::from(StringIndices::new())))?;
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

#[cfg(test)]
mod tests {
    use super::string_indices;

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
}
