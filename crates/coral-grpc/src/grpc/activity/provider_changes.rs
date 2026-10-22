//! The wire projection of what an `updateProvider` call changes. Coral
//! works out the changes; Reef only renders them.
use super::wire;
use coral_catalog::{
    EntryChange, EntryStatus, FieldChange, ProviderChanges, ProviderSourceOutput, SourceRead,
};
use wire::approval_provider_field_change::Change;

pub(super) fn changes(changes: ProviderChanges) -> wire::ApprovalProviderChanges {
    match changes {
        ProviderChanges::ProviderMissing => wire::ApprovalProviderChanges {
            provider_missing: true,
            fields: Vec::new(),
        },
        ProviderChanges::Fields(fields) => wire::ApprovalProviderChanges {
            provider_missing: false,
            fields: fields.into_iter().map(field).collect(),
        },
    }
}

fn field(change: FieldChange) -> wire::ApprovalProviderFieldChange {
    let (field, change) = match change {
        FieldChange::Value {
            field,
            saved,
            requested,
        } => (
            field,
            Change::Value(wire::ApprovalProviderValueChange {
                saved_json: saved.to_string(),
                requested_json: requested.to_string(),
            }),
        ),
        FieldChange::Entries { field, entries } => (
            field,
            Change::Entries(wire::ApprovalProviderEntriesChange {
                entries: entries.into_iter().map(entry).collect(),
            }),
        ),
        FieldChange::Source { saved, requested } => (
            "source",
            Change::Source(wire::ApprovalProviderSourceChange {
                saved: Some(source(saved)),
                unreadable: matches!(requested, SourceRead::Unreadable),
                requested: match requested {
                    SourceRead::Again | SourceRead::Unreadable => None,
                    SourceRead::New(requested) => Some(source(requested)),
                },
            }),
        ),
    };
    wire::ApprovalProviderFieldChange {
        field: field.to_owned(),
        change: Some(change),
    }
}

fn entry(entry: EntryChange) -> wire::ApprovalProviderEntryChange {
    wire::ApprovalProviderEntryChange {
        key: entry.key,
        status: match entry.status {
            EntryStatus::Added => wire::ApprovalProviderEntryStatus::Added,
            EntryStatus::Removed => wire::ApprovalProviderEntryStatus::Removed,
            EntryStatus::Changed => wire::ApprovalProviderEntryStatus::Changed,
            EntryStatus::Kept => wire::ApprovalProviderEntryStatus::Kept,
        }
        .into(),
        // JSON text is never empty, so empty means absent.
        saved_json: entry
            .saved
            .map(|value| value.to_string())
            .unwrap_or_default(),
        requested_json: entry
            .requested
            .map(|value| value.to_string())
            .unwrap_or_default(),
    }
}

fn source(source: ProviderSourceOutput) -> wire::ApprovalProviderSource {
    let (kind, url) = match source {
        ProviderSourceOutput::OpenapiDocument => (
            wire::ApprovalProviderSourceKind::OpenapiDocument,
            String::new(),
        ),
        ProviderSourceOutput::OpenapiUrl { url } => {
            (wire::ApprovalProviderSourceKind::OpenapiUrl, url)
        }
        ProviderSourceOutput::McpUrl { url } => (wire::ApprovalProviderSourceKind::McpUrl, url),
    };
    wire::ApprovalProviderSource {
        kind: kind.into(),
        url,
    }
}
