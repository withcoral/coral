use serde::{Deserialize, Serialize};

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::IrInputLocation;
use crate::{DetailHintSpec, ManifestDataType, SearchLimitsSpec, SourceTableFunctionKind};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionCatalog {
    pub artifact_schema_version: u32,
    pub source_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generator_version: Option<String>,
    pub projections: Vec<Projection>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Projection {
    pub name: String,
    #[serde(default)]
    pub namespace: String,
    pub kind: ProjectionKind,
    pub description: String,
    pub guide: String,
    pub surface_id: String,
    pub operation_id: String,
    pub visibility: ProjectionVisibility,
    pub inputs: Vec<ProjectionInput>,
    pub columns: Vec<ProjectionColumn>,
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
    pub source_location: IrInputLocation,
    pub wire_name: String,
    pub required: bool,
    pub data_type: ManifestDataType,
    pub default_value: Option<String>,
    pub description: String,
    /// Whether this filter input is a complete exact lookup: the API returns
    /// every row matching an equality value, so dependent joins may bind to
    /// it. Meaningless (and false) for non-filter exposures.
    #[serde(default)]
    pub lookup_key: bool,
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
    use crate::{ManifestDataType, SearchLimitsSpec, SourceTableFunctionKind};

    #[test]
    fn projection_catalog_yaml_uses_editor_friendly_enum_shapes() {
        let catalog = ProjectionCatalog {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: "demo".to_string(),
            generator_version: Some(PROJECTION_GENERATOR_VERSION.to_string()),
            projections: vec![Projection {
                name: "search_issues".to_string(),
                namespace: "demo".to_string(),
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
        assert!(
            !yaml.contains("pagination:"),
            "projection catalog should not serialize pagination: {yaml}"
        );

        serde_yaml::from_str::<ProjectionCatalog>(&yaml)
            .expect("projection catalog should round-trip");
    }

    #[test]
    fn projection_deserializes_legacy_catalogs_without_namespace() {
        let raw = format!(
            r"
artifact_schema_version: {V4_ARTIFACT_SCHEMA_VERSION}
source_name: demo
generator_version: {PROJECTION_GENERATOR_VERSION}
projections:
  - name: search_issues
    kind:
      type: table_function
      value:
        function_kind: search
    description: ''
    guide: ''
    surface_id: rest
    operation_id: issues/search
    visibility: published
    inputs: []
    columns: []
    search_limits: null
    detail_hints: []
    diagnostics: []
diagnostics: []
"
        );

        let catalog: ProjectionCatalog =
            serde_yaml::from_str(&raw).expect("legacy projection catalog should deserialize");
        assert_eq!(
            catalog.projections.first().expect("projection").namespace,
            ""
        );
    }

    #[test]
    fn projection_catalog_deserializes_without_generator_version() {
        let raw = format!(
            r"
artifact_schema_version: {V4_ARTIFACT_SCHEMA_VERSION}
source_name: demo
projections: []
diagnostics: []
"
        );

        let catalog: ProjectionCatalog =
            serde_yaml::from_str(&raw).expect("projection override catalog should deserialize");

        assert_eq!(catalog.generator_version, None);
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
