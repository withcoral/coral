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
    pub catalog_name: String,
    pub schema_name: String,
    #[serde(flatten)]
    pub sql_identity: ProjectionSqlIdentity,
    pub kind: ProjectionKind,
    pub description: String,
    pub guide: String,
    pub operation_id: String,
    pub visibility: ProjectionVisibility,
    pub inputs: Vec<ProjectionInput>,
    pub columns: Vec<ProjectionColumn>,
    pub search_limits: Option<SearchLimitsSpec>,
    pub detail_hints: Vec<DetailHintSpec>,
    pub diagnostics: Vec<Diagnostic>,
}

impl Projection {
    #[must_use]
    pub fn relation_name(&self) -> &str {
        self.sql_identity.relation_name()
    }

    pub(crate) fn set_relation_name(&mut self, name: String) {
        self.sql_identity.set_relation_name(name);
    }

    #[must_use]
    pub fn sql_reference(&self) -> String {
        format!(
            "{}.{}.{}",
            self.catalog_name,
            self.schema_name,
            self.relation_name()
        )
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum ProjectionSqlIdentity {
    Table { table_name: String },
    TableFunction { function_name: String },
}

impl ProjectionSqlIdentity {
    #[must_use]
    pub fn relation_name(&self) -> &str {
        match self {
            Self::Table { table_name } => table_name,
            Self::TableFunction { function_name } => function_name,
        }
    }

    fn set_relation_name(&mut self, name: String) {
        match self {
            Self::Table { table_name } => *table_name = name,
            Self::TableFunction { function_name } => *function_name = name,
        }
    }
}

impl<'de> Deserialize<'de> for ProjectionSqlIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct PersistedIdentity {
            #[serde(default)]
            table_name: Option<String>,
            #[serde(default)]
            function_name: Option<String>,
        }

        let identity = PersistedIdentity::deserialize(deserializer)?;
        match (identity.table_name, identity.function_name) {
            (Some(table_name), None) => Ok(Self::Table { table_name }),
            (None, Some(function_name)) => Ok(Self::TableFunction { function_name }),
            (Some(_), Some(_)) => Err(serde::de::Error::custom(
                "projection SQL identity must not define both table_name and function_name",
            )),
            (None, None) => Err(serde::de::Error::custom(
                "projection SQL identity must define table_name or function_name",
            )),
        }
    }
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
    /// Excludes this column from observed-value indexing when true.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub do_not_index: bool,
}

#[cfg(test)]
mod tests {
    use super::{
        Projection, ProjectionCatalog, ProjectionColumn, ProjectionKind, ProjectionSqlIdentity,
        ProjectionVisibility, SqlInputExposure,
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
                catalog_name: "demo".to_string(),
                schema_name: "issues".to_string(),
                sql_identity: ProjectionSqlIdentity::TableFunction {
                    function_name: "search".to_string(),
                },
                kind: ProjectionKind::TableFunction {
                    function_kind: SourceTableFunctionKind::Search,
                },
                description: String::new(),
                guide: String::new(),
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
            yaml.contains("catalog_name: demo")
                && yaml.contains("schema_name: issues")
                && yaml.contains("function_name: search")
                && !yaml.contains("table_name:"),
            "projection SQL identity is not canonical: {yaml}"
        );
        assert!(
            !yaml.contains("pagination:"),
            "projection catalog should not serialize pagination: {yaml}"
        );
        assert!(
            yaml.contains("do_not_index: true"),
            "projection catalog should serialize explicit indexing policy: {yaml}"
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
  - catalog_name: demo
    schema_name: public
    table_name: items
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
            catalog
                .projections
                .first()
                .expect("projection")
                .relation_name(),
            "items"
        );
    }

    #[test]
    fn projection_catalog_rejects_ambiguous_or_missing_sql_identity() {
        for identity in [
            "    table_name: items\n    function_name: search_items\n",
            "",
        ] {
            let raw = format!(
                r#"
artifact_schema_version: {V4_ARTIFACT_SCHEMA_VERSION}
source_name: demo
projections:
  - catalog_name: demo
    schema_name: public
{identity}    kind:
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
diagnostics: []
"#
            );

            serde_yaml::from_str::<ProjectionCatalog>(&raw)
                .expect_err("ambiguous or missing projection SQL identity must be rejected");
        }
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
