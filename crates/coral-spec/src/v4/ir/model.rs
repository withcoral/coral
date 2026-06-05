use serde::{Deserialize, Serialize};

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::rest::RestExecutionAttachment;
use crate::v4::manifest::SurfaceType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticIr {
    pub artifact_schema_version: u32,
    pub source_name: String,
    pub surface_id: String,
    pub surface_type: SurfaceType,
    pub importer_version: String,
    pub operations: Vec<IrOperation>,
    pub types: Vec<IrType>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrOperation {
    pub id: String,
    pub method_name: String,
    pub description: String,
    pub deprecated: bool,
    pub read_only: bool,
    pub inputs: Vec<IrOperationInput>,
    pub output: IrOperationOutput,
    pub entity: Option<IrEntityCandidate>,
    pub execution: IrExecutionAttachment,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrOperationInput {
    pub name: String,
    pub location: IrInputLocation,
    pub required: bool,
    pub data_type: IrScalarType,
    pub default_value: Option<String>,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrOperationOutput {
    pub cardinality: OutputCardinality,
    pub type_ref: String,
    pub row_path: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrEntityCandidate {
    pub name: String,
    pub type_ref: String,
    pub identity_fields: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutputCardinality {
    None,
    Singleton,
    List,
    WrappedList,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrType {
    pub id: String,
    pub shape: IrTypeShape,
    pub nullable: bool,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum IrTypeShape {
    Scalar(IrScalarType),
    Object { fields: Vec<IrField> },
    List { item_type_ref: String },
    Map { value_type_ref: String },
    Enum { values: Vec<String> },
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrField {
    pub name: String,
    pub type_ref: String,
    pub required: bool,
    pub nullable: bool,
    pub description: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IrScalarType {
    String,
    Integer,
    Number,
    Boolean,
    Id,
    Timestamp,
    Json,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum IrInputLocation {
    Path,
    Query,
    Header,
    Cookie,
    Body,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum HttpMethod {
    Get,
    Head,
    Options,
    Post,
    Put,
    Patch,
    Delete,
    Trace,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum IrExecutionAttachment {
    Rest(RestExecutionAttachment),
}

#[cfg(test)]
mod tests {
    use super::{
        HttpMethod, IrExecutionAttachment, IrInputLocation, IrOperation, IrOperationInput,
        IrOperationOutput, IrScalarType, IrType, IrTypeShape, OutputCardinality, SemanticIr,
    };
    use crate::PaginationSpec;
    use crate::v4::diagnostics::Diagnostic;
    use crate::v4::ir::rest::{RestExecutionAttachment, RestResponseAttachment};
    use crate::v4::manifest::SurfaceType;
    use crate::v4::{OPENAPI_IMPORTER_VERSION, V4_ARTIFACT_SCHEMA_VERSION};

    #[test]
    fn semantic_ir_yaml_uses_editor_friendly_enum_shapes() {
        let ir = SemanticIr {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: "demo".to_string(),
            surface_id: "rest".to_string(),
            surface_type: SurfaceType::OpenApi,
            importer_version: OPENAPI_IMPORTER_VERSION.to_string(),
            operations: vec![IrOperation {
                id: "list_issues".to_string(),
                method_name: "GET".to_string(),
                description: String::new(),
                deprecated: false,
                read_only: true,
                inputs: Vec::new(),
                output: IrOperationOutput {
                    cardinality: OutputCardinality::List,
                    type_ref: "issue".to_string(),
                    row_path: Vec::new(),
                },
                entity: None,
                execution: IrExecutionAttachment::Rest(RestExecutionAttachment {
                    method: HttpMethod::Get,
                    path_template: "/issues".to_string(),
                    parameters: Vec::new(),
                    request_body: None,
                    response: RestResponseAttachment {
                        status_code: 200,
                        media_type: "application/json".to_string(),
                        response: crate::ResponseSpec::default(),
                    },
                    pagination: PaginationSpec::default(),
                }),
                diagnostics: Vec::new(),
            }],
            types: vec![
                IrType {
                    id: "issue".to_string(),
                    shape: IrTypeShape::Object { fields: Vec::new() },
                    nullable: false,
                    description: String::new(),
                },
                IrType {
                    id: "issue_id".to_string(),
                    shape: IrTypeShape::Scalar(IrScalarType::String),
                    nullable: false,
                    description: String::new(),
                },
            ],
            diagnostics: vec![Diagnostic::warning(
                "TEST",
                "diagnostic",
                "rest".to_string(),
                None,
            )],
        };

        let yaml = serde_yaml::to_string(&ir).expect("serialize semantic IR");
        assert!(
            !yaml.contains('!'),
            "semantic IR should not use YAML local tags: {yaml}"
        );
        assert!(yaml.contains("type: rest"), "missing rest tag: {yaml}");
        assert!(yaml.contains("type: object"), "missing object tag: {yaml}");
        assert!(yaml.contains("type: scalar"), "missing scalar tag: {yaml}");

        serde_yaml::from_str::<SemanticIr>(&yaml).expect("semantic IR should round-trip");
    }

    #[test]
    fn semantic_ir_yaml_rejects_legacy_local_tags() {
        let legacy_yaml = format!(
            r#"
artifact_schema_version: {V4_ARTIFACT_SCHEMA_VERSION}
source_name: demo
surface_id: rest
surface_type: openapi
importer_version: {OPENAPI_IMPORTER_VERSION}
operations: []
types:
  - id: issue
    shape: !Object
      fields: []
    nullable: false
    description: ""
diagnostics: []
"#
        );

        let error = serde_yaml::from_str::<SemanticIr>(&legacy_yaml)
            .expect_err("semantic IR should reject legacy local tags");
        assert!(
            error.to_string().contains("shape") || error.to_string().contains("type"),
            "unexpected legacy local tag error: {error}"
        );
    }

    #[test]
    fn input_location_serialization_preserves_artifact_shape() {
        let input = IrOperationInput {
            name: "owner".to_string(),
            location: IrInputLocation::Path,
            required: true,
            data_type: IrScalarType::String,
            default_value: None,
            description: String::new(),
        };

        let yaml = serde_yaml::to_string(&input).expect("serialize input");
        assert!(
            yaml.contains("location: path"),
            "unexpected serialized input: {yaml}"
        );
        assert!(
            !yaml.contains("OpenApi"),
            "Rust type names must not leak into artifacts: {yaml}"
        );

        let decoded: IrOperationInput = serde_yaml::from_str(&yaml).expect("deserialize input");
        assert_eq!(decoded.location, IrInputLocation::Path);
    }
}
