use serde::{Deserialize, Serialize};

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::IrInputLocation;
use crate::{
    CollectionEncoding, DetailHintSpec, ManifestDataType, SearchLimitsSpec, SourceTableFunctionKind,
};

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
    pub kind: ProjectionKind,
    pub description: String,
    pub guide: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub require_guide_read: bool,
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
    /// Set when this input takes a list of values rather than one, and how
    /// that list is encoded onto the wire.
    ///
    /// `data_type` is [`ManifestDataType::Utf8`] whenever this is set, even
    /// though the conventional value is JSON array text. `Json` would reject
    /// the bare single-value form at plan time, because a `Json` argument's
    /// literal must parse as JSON before the value source runs. The list shape
    /// lives here, not in `data_type`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection_encoding: Option<CollectionEncoding>,
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
    /// Excludes this column from observed-value indexing when true.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub do_not_index: bool,
}

#[cfg(test)]
mod tests {
    use super::{
        Projection, ProjectionCatalog, ProjectionColumn, ProjectionInput, ProjectionKind,
        ProjectionVisibility, SqlInputExposure,
    };
    use crate::v4::ir::IrInputLocation;
    use crate::v4::{PROJECTION_GENERATOR_VERSION, V4_ARTIFACT_SCHEMA_VERSION};
    use crate::{CollectionEncoding, ManifestDataType, SearchLimitsSpec, SourceTableFunctionKind};

    #[test]
    fn projection_catalog_yaml_uses_editor_friendly_enum_shapes() {
        let catalog = ProjectionCatalog {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: "demo".to_string(),
            generator_version: Some(PROJECTION_GENERATOR_VERSION.to_string()),
            projections: vec![Projection {
                name: "search_issues".to_string(),
                kind: ProjectionKind::TableFunction {
                    function_kind: SourceTableFunctionKind::Search,
                },
                description: String::new(),
                guide: "Use search_issues for lookups.".to_string(),
                require_guide_read: true,
                operation_id: "issues/search".to_string(),
                visibility: ProjectionVisibility::Published,
                inputs: Vec::new(),
                columns: vec![ProjectionColumn {
                    name: "internal_note".to_string(),
                    data_type: ManifestDataType::Utf8,
                    source_path: vec!["internalNote".to_string()],
                    nullable: true,
                    description: String::new(),
                    do_not_index: true,
                }],
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
        assert!(
            yaml.contains("do_not_index: true"),
            "projection catalog should serialize explicit indexing policy: {yaml}"
        );
        assert!(
            yaml.contains("require_guide_read: true"),
            "required guide-read policy should serialize: {yaml}"
        );
        assert!(!yaml.contains("surface_id:"), "surface ID leaked: {yaml}");

        let decoded = serde_yaml::from_str::<ProjectionCatalog>(&yaml)
            .expect("projection catalog should round-trip");
        assert!(
            decoded
                .projections
                .first()
                .expect("projection")
                .columns
                .first()
                .expect("column")
                .do_not_index,
            "projection column policy should survive round-trip"
        );
        assert!(
            decoded
                .projections
                .first()
                .expect("projection")
                .require_guide_read
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
    fn projection_ignores_unknown_future_fields() {
        let raw = format!(
            r#"
artifact_schema_version: {V4_ARTIFACT_SCHEMA_VERSION}
source_name: demo
projections:
  - name: items
    kind:
      type: table
    description: ""
    guide: ""
    operation_id: items/list
    visibility: published
    inputs: []
    columns: []
    search_limits: null
    detail_hints: []
    diagnostics: []
    future_sql_metadata:
      revision: 2
diagnostics: []
"#
        );

        let catalog: ProjectionCatalog =
            serde_yaml::from_str(&raw).expect("unknown future fields should be advisory");

        assert_eq!(
            catalog.projections.first().expect("projection").name,
            "items"
        );
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

    #[test]
    fn projection_input_collection_encoding_round_trips_as_a_plain_scalar() {
        let input = ProjectionInput {
            name: "select".to_string(),
            sql_exposure: SqlInputExposure::Filter,
            source_location: IrInputLocation::Query,
            wire_name: "$select".to_string(),
            required: false,
            data_type: ManifestDataType::Json,
            collection_encoding: Some(CollectionEncoding::Comma),
            default_value: None,
            description: String::new(),
            lookup_key: false,
        };

        let yaml = serde_yaml::to_string(&input).expect("serialize input");
        assert!(
            yaml.contains("collection_encoding: comma"),
            "unexpected serialized input: {yaml}"
        );
        assert!(
            !yaml.contains('!'),
            "projection input should not use YAML local tags: {yaml}"
        );

        let decoded: ProjectionInput = serde_yaml::from_str(&yaml).expect("deserialize input");
        assert_eq!(decoded.collection_encoding, Some(CollectionEncoding::Comma));
    }

    #[test]
    fn projection_input_omits_collection_encoding_for_scalars() {
        let input = ProjectionInput {
            name: "state".to_string(),
            sql_exposure: SqlInputExposure::Filter,
            source_location: IrInputLocation::Query,
            wire_name: "state".to_string(),
            required: false,
            data_type: ManifestDataType::Utf8,
            collection_encoding: None,
            default_value: None,
            description: String::new(),
            lookup_key: false,
        };

        let yaml = serde_yaml::to_string(&input).expect("serialize input");
        assert!(
            !yaml.contains("collection_encoding"),
            "scalar inputs should not carry the key: {yaml}"
        );

        let decoded: ProjectionInput = serde_yaml::from_str(&yaml).expect("deserialize input");
        assert_eq!(decoded.collection_encoding, None);
    }
}
