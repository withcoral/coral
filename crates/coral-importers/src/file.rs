use std::collections::BTreeMap;

use coral_capabilities::{
    Capability, EffectProfile, FileArtifactRef, FileFormatDescriptor, FileScanBinding,
    InvocationSchema, OutputContract, ProviderOrigin, ProviderOriginKind, ShapeHints, SourceId,
    UpstreamBinding,
};
use coral_spec::{FileInterface, SourceFileFormatDescriptor, SourceSpec};

use crate::hash::sha256_hex;
use crate::{
    ImportedInterface, ImporterError, ProviderSnapshotArtifact, RawInterfaceInput, Result,
};

pub(super) fn import_file(
    source_id: &SourceId,
    _spec: &SourceSpec,
    interface: &FileInterface,
    raw_inputs: &BTreeMap<String, RawInterfaceInput>,
) -> Result<ImportedInterface> {
    let raw = raw_inputs
        .get(&interface.id)
        .ok_or_else(|| ImporterError::MissingRawInput(interface.id.clone()))?;
    let RawInterfaceInput::FileListing { schema } = raw else {
        return Err(ImporterError::Parse {
            interface_id: interface.id.clone(),
            message: "expected file listing metadata".to_string(),
        });
    };
    let mut capabilities = Vec::new();
    let file_refs = interface
        .files
        .iter()
        .enumerate()
        .map(|(index, path)| {
            let id = format!("file_{index}");
            FileArtifactRef {
                source_local_path: format!("interfaces/{}/files/{id}", interface.id),
                display_name: path
                    .file_name()
                    .map(|name| name.to_string_lossy().to_string()),
                id,
            }
        })
        .collect::<Vec<_>>();
    let operation_id = "read_files".to_string();
    let provider_ref = format!("interfaces/{}/provider-snapshot.yaml#/files", interface.id);
    let mut capability = Capability::new(
        source_id.clone(),
        interface.id.clone(),
        operation_id,
        ProviderOrigin {
            kind: ProviderOriginKind::FileRelation,
            snapshot_ref: provider_ref,
            provider_name: "files".to_string(),
            tags: Vec::new(),
        },
        UpstreamBinding::FileRead(FileScanBinding {
            file_refs: file_refs.clone(),
            format: match interface.format {
                SourceFileFormatDescriptor::Json => FileFormatDescriptor::Json,
                SourceFileFormatDescriptor::Jsonl => FileFormatDescriptor::Jsonl,
                SourceFileFormatDescriptor::Parquet => FileFormatDescriptor::Parquet,
                SourceFileFormatDescriptor::Csv => FileFormatDescriptor::Csv,
            },
            schema_ref: Some(format!(
                "interfaces/{}/provider-snapshot.yaml#/schema",
                interface.id
            )),
        }),
    );
    capability.display.title = "Read files".to_string();
    capability.effect_profile = EffectProfile::read();
    capability.shape_hints = ShapeHints::root_list();
    capability.output_contract = OutputContract::Single {
        schema: InvocationSchema::new(schema.clone()),
    };
    capabilities.push(capability);
    Ok(ImportedInterface {
        snapshot: ProviderSnapshotArtifact {
            artifact_schema_version: 1,
            source_id: source_id.clone(),
            interface_id: interface.id.clone(),
            interface_type: "file".to_string(),
            importer_version: "file-read-v1".to_string(),
            source_document_sha256: sha256_hex(
                interface
                    .files
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>()
                    .join("\n")
                    .as_bytes(),
            ),
            snapshot: serde_json::json!({
                "files": file_refs,
                "format": format!("{:?}", interface.format).to_ascii_lowercase(),
                "schema": schema,
            }),
            diagnostics: Vec::new(),
        },
        capabilities,
    })
}
