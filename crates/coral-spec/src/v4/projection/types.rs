use crate::ManifestDataType;

use super::super::ir::IrScalarType;

pub(super) fn manifest_type(scalar: IrScalarType) -> ManifestDataType {
    match scalar {
        IrScalarType::String | IrScalarType::Id => ManifestDataType::Utf8,
        IrScalarType::Integer => ManifestDataType::Int64,
        IrScalarType::Number => ManifestDataType::Float64,
        IrScalarType::Boolean => ManifestDataType::Boolean,
        IrScalarType::Timestamp => ManifestDataType::Timestamp,
        IrScalarType::Json => ManifestDataType::Json,
    }
}

pub fn manifest_data_type_name(data_type: ManifestDataType) -> &'static str {
    match data_type {
        ManifestDataType::Utf8 => "Utf8",
        ManifestDataType::Int64 => "Int64",
        ManifestDataType::Boolean => "Boolean",
        ManifestDataType::Float64 => "Float64",
        ManifestDataType::Timestamp => "Timestamp",
        ManifestDataType::Json => "Json",
    }
}
