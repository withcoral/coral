use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};

use crate::runtime::dependent_join::logical::BindingKey;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct Tuple {
    values: Vec<BindingValue>,
}

impl Tuple {
    pub(crate) fn new(values: Vec<BindingValue>) -> Self {
        Self { values }
    }

    pub(crate) fn values(&self) -> &[BindingValue] {
        &self.values
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum BindingValue {
    String(String),
    Int64(i64),
    Boolean(bool),
}

impl BindingValue {
    pub(crate) fn to_wire_string(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Int64(value) => value.to_string(),
            Self::Boolean(value) => value.to_string(),
        }
    }
}

pub(crate) struct BindingProjector {
    keys: Arc<[BindingKey]>,
}

impl BindingProjector {
    pub(crate) fn new(keys: Arc<[BindingKey]>) -> Self {
        Self { keys }
    }

    pub(crate) fn project(&self, batch: &RecordBatch, row: usize) -> Result<Option<Tuple>> {
        let mut values = Vec::with_capacity(self.keys.len());

        for key in self.keys.iter() {
            let index = batch
                .schema()
                .index_of(&key.resolver_binding_name)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            let array = batch.column(index);

            if array.is_null(row) {
                return Ok(None);
            }

            values.push(extract_binding_value(array.as_ref(), row)?);
        }

        Ok(Some(Tuple::new(values)))
    }
}

fn extract_binding_value(array: &dyn Array, row: usize) -> Result<BindingValue> {
    match array.data_type() {
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| coercion_error(array.data_type()))?;
            Ok(BindingValue::String(array.value(row).to_string()))
        }
        DataType::Int64 => {
            let array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| coercion_error(array.data_type()))?;
            Ok(BindingValue::Int64(array.value(row)))
        }
        DataType::Boolean => {
            let array = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| coercion_error(array.data_type()))?;
            Ok(BindingValue::Boolean(array.value(row)))
        }
        other => Err(coercion_error(other)),
    }
}

fn coercion_error(data_type: &DataType) -> DataFusionError {
    DataFusionError::Execution(format!(
        "dependent join binding value has unsupported Arrow type {data_type}"
    ))
}
