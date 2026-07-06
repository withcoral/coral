//! Partition extraction and pruning helpers for file-backed sources.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, Operator};

use coral_spec::backends::file::{FilePartitionDataType, PartitionColumnSpec, PartitionPathSpec};

use super::listing::parse_bool;

fn arrow_type(data_type: FilePartitionDataType) -> DataType {
    crate::types::arrow_column_type(data_type.into())
}

fn scalar_from_path(
    data_type: FilePartitionDataType,
    name: &str,
    raw: &str,
) -> Result<ScalarValue> {
    let decoded = urlencoding::decode(raw).map_err(|error| {
        DataFusionError::Execution(format!("partition '{name}' has invalid encoding: {error}"))
    })?;
    let decoded = decoded.as_ref();

    match data_type {
        FilePartitionDataType::Utf8 | FilePartitionDataType::Json => {
            Ok(ScalarValue::Utf8(Some(decoded.to_string())))
        }
        FilePartitionDataType::Int64 => decoded
            .parse::<i64>()
            .map(|value| ScalarValue::Int64(Some(value)))
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "partition '{name}' value '{decoded}' is not Int64: {error}",
                ))
            }),
        FilePartitionDataType::Boolean => {
            parse_bool(decoded).map(|value| ScalarValue::Boolean(Some(value)))
        }
        FilePartitionDataType::Float64 => {
            let value = decoded.parse::<f64>().map_err(|error| {
                DataFusionError::Execution(format!(
                    "partition '{name}' value '{decoded}' is not Float64: {error}",
                ))
            })?;
            if !value.is_finite() {
                return Err(DataFusionError::Execution(format!(
                    "partition '{name}' value '{decoded}' is not finite",
                )));
            }
            Ok(ScalarValue::Float64(Some(value)))
        }
    }
}

fn boolean_scalar(data_type: FilePartitionDataType, value: bool) -> Option<ScalarValue> {
    (data_type == FilePartitionDataType::Boolean).then_some(ScalarValue::Boolean(Some(value)))
}

fn cast_literal(data_type: FilePartitionDataType, expr: &Expr) -> Option<ScalarValue> {
    let literal = literal_scalar(expr)?;
    literal
        .cast_to(&arrow_type(data_type))
        .ok()
        .filter(|value| !value.is_null())
}

#[derive(Debug, Clone)]
struct PartitionColumn {
    name: String,
    data_type: FilePartitionDataType,
    path: PartitionPathSpec,
}

impl PartitionColumn {
    fn from_spec(spec: &PartitionColumnSpec) -> Self {
        Self {
            name: spec.name.clone(),
            data_type: spec.data_type,
            path: spec.path.clone(),
        }
    }

    fn arrow_pair(&self) -> (String, DataType) {
        (self.name.clone(), arrow_type(self.data_type))
    }

    fn arrow_field(&self) -> FieldRef {
        Arc::new(Field::new(&self.name, arrow_type(self.data_type), true))
    }
}

#[derive(Debug, Clone)]
pub(super) struct PartitionColumns {
    columns: Vec<PartitionColumn>,
}

impl PartitionColumns {
    pub(super) fn try_new(specs: &[PartitionColumnSpec]) -> Result<Self> {
        let columns = specs
            .iter()
            .map(PartitionColumn::from_spec)
            .collect::<Vec<_>>();
        let mut seen = HashSet::with_capacity(columns.len());
        for column in &columns {
            if !seen.insert(column.name.as_str()) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate partition '{}'",
                    column.name
                )));
            }
        }
        Ok(Self { columns })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub(super) fn len(&self) -> usize {
        self.columns.len()
    }

    pub(super) fn hive_arrow_columns(&self) -> Vec<(String, DataType)> {
        self.columns
            .iter()
            .filter(|partition| partition.path.is_hive())
            .map(PartitionColumn::arrow_pair)
            .collect()
    }

    pub(super) fn arrow_fields(&self) -> Vec<FieldRef> {
        self.columns
            .iter()
            .map(PartitionColumn::arrow_field)
            .collect()
    }

    pub(super) fn names(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(|partition| partition.name.as_str())
    }

    pub(super) fn contains_expr_column(&self, expr: &Expr) -> bool {
        expr_column_name(expr).is_some_and(|name| self.index_of_name(name).is_some())
    }

    fn index_of_expr_column(&self, expr: &Expr) -> Option<usize> {
        self.index_of_name(expr_column_name(expr)?)
    }

    fn index_of_name(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|partition| partition.name == name)
    }

    fn get(&self, index: usize) -> Option<&PartitionColumn> {
        self.columns.get(index)
    }
}

#[derive(Debug, Clone)]
pub(super) struct PartitionValues {
    values: Vec<ScalarValue>,
}

impl PartitionValues {
    pub(super) fn into_scalars(self) -> Vec<ScalarValue> {
        self.values
    }
}

#[derive(Debug, Default)]
pub(super) struct PartitionFilterConstraints {
    allowed: Vec<Option<HashSet<ScalarValue>>>,
}

impl PartitionFilterConstraints {
    fn new(partitions: &PartitionColumns) -> Self {
        Self {
            allowed: vec![None; partitions.len()],
        }
    }

    fn constrain(&mut self, index: usize, values: HashSet<ScalarValue>) {
        let Some(slot) = self.allowed.get_mut(index) else {
            return;
        };
        match slot {
            Some(existing) => existing.retain(|value| values.contains(value)),
            None => *slot = Some(values),
        }
    }

    pub(super) fn matches(&self, values: &PartitionValues) -> bool {
        self.allowed
            .iter()
            .zip(values.values.iter())
            .all(|(allowed, actual)| allowed.as_ref().is_none_or(|set| set.contains(actual)))
    }
}

pub(super) fn filter_references_partition(expr: &Expr, partitions: &PartitionColumns) -> bool {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            filter_references_partition(binary.left.as_ref(), partitions)
                || filter_references_partition(binary.right.as_ref(), partitions)
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            partitions.contains_expr_column(binary.left.as_ref())
                || partitions.contains_expr_column(binary.right.as_ref())
        }
        Expr::InList(in_list) if !in_list.negated => {
            partitions.contains_expr_column(in_list.expr.as_ref())
        }
        Expr::Column(col) => partitions.index_of_name(col.name()).is_some(),
        Expr::IsTrue(inner) | Expr::Not(inner) | Expr::IsFalse(inner) => {
            partitions.contains_expr_column(inner.as_ref())
        }
        _ => false,
    }
}

pub(super) fn filter_is_supported_partition_filter(
    expr: &Expr,
    partitions: &PartitionColumns,
) -> bool {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            filter_is_supported_partition_filter(binary.left.as_ref(), partitions)
                && filter_is_supported_partition_filter(binary.right.as_ref(), partitions)
        }
        _ => partition_constraint(expr, partitions).is_some(),
    }
}

pub(super) fn partition_filter_constraints(
    filters: &[Expr],
    partitions: &PartitionColumns,
) -> PartitionFilterConstraints {
    let mut constraints = PartitionFilterConstraints::new(partitions);
    for filter in filters {
        collect_partition_filter_constraints(filter, partitions, &mut constraints);
    }
    constraints
}

fn collect_partition_filter_constraints(
    expr: &Expr,
    partitions: &PartitionColumns,
    constraints: &mut PartitionFilterConstraints,
) {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_partition_filter_constraints(binary.left.as_ref(), partitions, constraints);
            collect_partition_filter_constraints(binary.right.as_ref(), partitions, constraints);
        }
        _ => {
            if let Some(constraint) = partition_constraint(expr, partitions) {
                constraints.constrain(constraint.index, constraint.values);
            }
        }
    }
}

struct PartitionConstraint {
    index: usize,
    values: HashSet<ScalarValue>,
}

fn partition_constraint(expr: &Expr, partitions: &PartitionColumns) -> Option<PartitionConstraint> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            extract_partition_equality(binary.left.as_ref(), binary.right.as_ref(), partitions)
                .or_else(|| {
                    extract_partition_equality(
                        binary.right.as_ref(),
                        binary.left.as_ref(),
                        partitions,
                    )
                })
                .map(|(index, value)| PartitionConstraint {
                    index,
                    values: HashSet::from([value]),
                })
        }
        Expr::InList(in_list) if !in_list.negated => {
            let index = partitions.index_of_expr_column(in_list.expr.as_ref())?;
            let partition = partitions.get(index)?;
            let values = in_list
                .list
                .iter()
                .map(|expr| cast_literal(partition.data_type, expr))
                .collect::<Option<HashSet<_>>>()?;
            Some(PartitionConstraint { index, values })
        }
        Expr::Column(col) => boolean_partition_constraint(partitions, col.name(), true),
        Expr::IsTrue(inner) => {
            boolean_partition_constraint(partitions, expr_column_name(inner.as_ref())?, true)
        }
        Expr::Not(inner) | Expr::IsFalse(inner) => {
            boolean_partition_constraint(partitions, expr_column_name(inner.as_ref())?, false)
        }
        _ => None,
    }
}

fn boolean_partition_constraint(
    partitions: &PartitionColumns,
    name: &str,
    value: bool,
) -> Option<PartitionConstraint> {
    let index = partitions.index_of_name(name)?;
    let partition = partitions.get(index)?;
    Some(PartitionConstraint {
        index,
        values: HashSet::from([boolean_scalar(partition.data_type, value)?]),
    })
}

fn extract_partition_equality(
    left: &Expr,
    right: &Expr,
    partitions: &PartitionColumns,
) -> Option<(usize, ScalarValue)> {
    let index = partitions.index_of_expr_column(left)?;
    let partition = partitions.get(index)?;
    Some((index, cast_literal(partition.data_type, right)?))
}

fn expr_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(col) => Some(col.name()),
        Expr::Cast(cast) => expr_column_name(cast.expr.as_ref()),
        Expr::TryCast(cast) => expr_column_name(cast.expr.as_ref()),
        _ => None,
    }
}

pub(super) fn partition_values_for_path(
    table_path: &ListingTableUrl,
    path: &object_store::path::Path,
    partitions: &PartitionColumns,
) -> Result<PartitionValues> {
    if partitions.is_empty() {
        return Ok(PartitionValues { values: vec![] });
    }

    let parent_segments = relative_parent_segments(table_path, path);
    let mut values = Vec::with_capacity(partitions.len());

    for (position, partition) in partitions.columns.iter().enumerate() {
        let raw_value = match &partition.path {
            PartitionPathSpec::Hive => {
                let Some(segment) = parent_segments.get(position) else {
                    return Err(DataFusionError::Execution(format!(
                        "{path} does not match partitioned table layout {table_path}: missing hive partition '{}' at segment {position}",
                        partition.name.as_str()
                    )));
                };
                let Some((name, raw_value)) = segment.split_once('=') else {
                    return Err(DataFusionError::Execution(format!(
                        "{path} does not match partitioned table layout {table_path}: segment '{segment}' is not hive-style for partition '{}'",
                        partition.name.as_str()
                    )));
                };
                if name != partition.name.as_str() {
                    return Err(DataFusionError::Execution(format!(
                        "{path} does not match partitioned table layout {table_path}: expected hive partition '{}' at segment {position}, got '{name}'",
                        partition.name.as_str()
                    )));
                }
                raw_value
            }
            PartitionPathSpec::Segment { index } => {
                let Some(raw_value) = parent_segments.get(*index).map(String::as_str) else {
                    return Err(DataFusionError::Execution(format!(
                        "{path} does not match partitioned table layout {table_path}: missing positional partition '{}' at segment {index}",
                        partition.name.as_str()
                    )));
                };
                raw_value
            }
        };

        values.push(scalar_from_path(
            partition.data_type,
            &partition.name,
            raw_value,
        )?);
    }

    Ok(PartitionValues { values })
}

fn relative_parent_segments(
    table_path: &ListingTableUrl,
    path: &object_store::path::Path,
) -> Vec<String> {
    let mut segments = table_path
        .strip_prefix(path)
        .map(|segments| segments.map(ToString::to_string).collect::<Vec<_>>())
        .unwrap_or_default();
    segments.pop();
    segments
}

fn literal_scalar(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(value, _) => Some(value.clone()),
        Expr::Cast(cast) => literal_scalar(cast.expr.as_ref()),
        Expr::TryCast(cast) => literal_scalar(cast.expr.as_ref()),
        _ => None,
    }
}
