use serde::{Deserialize, Serialize};

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::OpenApiParameterLocation;
use crate::{
    DetailHintSpec, ManifestDataType, PaginationSpec, SearchLimitsSpec, SourceTableFunctionKind,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionCatalog {
    pub artifact_schema_version: u32,
    pub source_name: String,
    pub generator_version: String,
    pub projections: Vec<Projection>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Projection {
    pub name: String,
    pub kind: ProjectionKind,
    pub description: String,
    pub guide: String,
    pub surface_id: String,
    pub operation_id: String,
    pub visibility: ProjectionVisibility,
    pub inputs: Vec<ProjectionInput>,
    pub columns: Vec<ProjectionColumn>,
    pub pagination: PaginationSpec,
    pub search_limits: Option<SearchLimitsSpec>,
    pub detail_hints: Vec<DetailHintSpec>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum ProjectionKind {
    Table,
    TableFunction {
        function_kind: SourceTableFunctionKind,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionVisibility {
    Published,
    Hidden,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionInput {
    pub name: String,
    pub sql_exposure: SqlInputExposure,
    pub source_location: OpenApiParameterLocation,
    pub wire_name: String,
    pub required: bool,
    pub data_type: ManifestDataType,
    pub default_value: Option<String>,
    pub description: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SqlInputExposure {
    Filter,
    FunctionArg,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionColumn {
    pub name: String,
    pub data_type: ManifestDataType,
    pub source_path: Vec<String>,
    pub nullable: bool,
    pub description: String,
}

#[cfg(test)]
mod tests {
    use super::{
        Projection, ProjectionCatalog, ProjectionKind, ProjectionVisibility, SqlInputExposure,
    };
    use crate::v4::{PROJECTION_GENERATOR_VERSION, V4_ARTIFACT_SCHEMA_VERSION};
    use crate::{ManifestDataType, PaginationSpec, SearchLimitsSpec, SourceTableFunctionKind};

    #[test]
    fn projection_catalog_yaml_uses_editor_friendly_enum_shapes() {
        let catalog = ProjectionCatalog {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: "demo".to_string(),
            generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
            projections: vec![Projection {
                name: "search_issues".to_string(),
                kind: ProjectionKind::TableFunction {
                    function_kind: SourceTableFunctionKind::Search,
                },
                description: String::new(),
                guide: String::new(),
                surface_id: "rest".to_string(),
                operation_id: "issues/search".to_string(),
                visibility: ProjectionVisibility::Published,
                inputs: Vec::new(),
                columns: Vec::new(),
                pagination: PaginationSpec::default(),
                search_limits: Some(SearchLimitsSpec {
                    default_top_k: 30,
                    max_top_k: 100,
                    max_calls_per_query: 100,
                }),
                detail_hints: Vec::new(),
                diagnostics: Vec::new(),
            }],
            diagnostics: Vec::new(),
        };

        let yaml = serde_yaml::to_string(&catalog).expect("serialize projection catalog");
        assert!(
            !yaml.contains('!'),
            "projection catalog should not use YAML local tags: {yaml}"
        );
        assert!(
            yaml.contains("type: table_function"),
            "missing projection kind tag: {yaml}"
        );
        assert!(
            yaml.contains("function_kind: search"),
            "missing function kind: {yaml}"
        );

        serde_yaml::from_str::<ProjectionCatalog>(&yaml)
            .expect("projection catalog should round-trip");
    }

    #[test]
    fn projection_input_unit_enums_remain_plain_scalars() {
        let exposure =
            serde_yaml::to_string(&SqlInputExposure::Filter).expect("serialize exposure");
        let data_type =
            serde_yaml::to_string(&ManifestDataType::Utf8).expect("serialize data type");

        assert_eq!(exposure.trim(), "filter");
        assert_eq!(data_type.trim(), "Utf8");
    }
}
