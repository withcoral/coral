//! What an `updateProvider` call changes, beside its arguments. Each sent
//! field replaces its saved value whole, so the arguments alone hide what a
//! call removes: an Owner who reviews the call reads each sent field's saved
//! value next to the requested one. Both sides read in `getProvider`'s
//! shapes, so neither holds a secret or a fixed credential value.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::{Map, Value};

use super::{SourceChange, UpdateProviderArguments};
use crate::builtin::{
    BuiltinCredentialRoute, ConfigurationOutput, ProviderSourceArgument, ProviderSourceOutput,
};
use crate::{AuthenticationConfiguration, ProviderConfiguration};

/// The Provider an update applies to, as saved when the review reads it.
#[derive(Debug, Clone)]
pub struct SavedProvider {
    /// Where Coral last read the Provider from.
    pub source: ProviderSourceOutput,
    /// Whose Account Invocations use.
    pub credential_route: BuiltinCredentialRoute,
    /// The saved Provider Configuration.
    pub configuration: ProviderConfiguration,
}

/// What one `updateProvider` call changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderChanges {
    /// No Provider holds the namespace: the call fails and changes nothing.
    ProviderMissing,
    /// Each field the call sends, in the order its arguments list them.
    Fields(Vec<FieldChange>),
}

/// One sent field, with its saved value beside the requested one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldChange {
    /// A value the call replaces whole.
    Value {
        /// The argument, such as `displayName`.
        field: &'static str,
        /// The saved value.
        saved: Value,
        /// The value the call saves.
        requested: Value,
    },
    /// A map the call replaces whole, entry by entry in key order.
    Entries {
        /// The argument, such as `requestHeaders`.
        field: &'static str,
        /// Every key either side holds.
        entries: Vec<EntryChange>,
    },
    /// Where Coral reads the Provider from.
    Source {
        /// The saved source.
        saved: ProviderSourceOutput,
        /// What the call reads.
        requested: SourceRead,
    },
}

/// What an update reads the Provider from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceRead {
    /// The saved source URL, read again.
    Again,
    /// The saved source again, which is an uploaded document with no URL to
    /// read: the call fails and changes nothing.
    Unreadable,
    /// A new source.
    New(ProviderSourceOutput),
}

/// One key of a map the call replaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryChange {
    /// The key, such as a header name.
    pub key: String,
    /// What the call does to it.
    pub status: EntryStatus,
    /// The saved value; absent for an added key.
    pub saved: Option<Value>,
    /// The value the call saves; absent for a removed key.
    pub requested: Option<Value>,
}

/// What a call does to one key of a map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryStatus {
    /// The call adds it.
    Added,
    /// The call removes it.
    Removed,
    /// The call gives it another value.
    Changed,
    /// The call sends it with its saved value.
    Kept,
}

impl UpdateProviderArguments {
    /// What the call changes in `saved`, the Provider as saved now; `None`
    /// when no Provider holds the namespace.
    #[must_use]
    pub fn changes(&self, saved: Option<SavedProvider>) -> ProviderChanges {
        let Some(saved) = saved else {
            return ProviderChanges::ProviderMissing;
        };
        let mut fields = Vec::new();
        match &self.source {
            SourceChange::Keep => {}
            SourceChange::Refresh => fields.push(FieldChange::Source {
                requested: match saved.source {
                    ProviderSourceOutput::OpenapiDocument => SourceRead::Unreadable,
                    ProviderSourceOutput::OpenapiUrl { .. }
                    | ProviderSourceOutput::McpUrl { .. } => SourceRead::Again,
                },
                saved: saved.source.clone(),
            }),
            SourceChange::Replace(source) => fields.push(FieldChange::Source {
                saved: saved.source.clone(),
                requested: SourceRead::New(match source {
                    ProviderSourceArgument::OpenApiDocument(_) => {
                        ProviderSourceOutput::OpenapiDocument
                    }
                    ProviderSourceArgument::OpenApiUrl(url) => {
                        ProviderSourceOutput::OpenapiUrl { url: url.clone() }
                    }
                    ProviderSourceArgument::McpUrl(url) => {
                        ProviderSourceOutput::McpUrl { url: url.clone() }
                    }
                }),
            }),
        }
        if let Some(route) = self.credential_route {
            fields.push(FieldChange::Value {
                field: "credentialRoute",
                saved: to_json(&saved.credential_route),
                requested: to_json(&route),
            });
        }
        let requested = get_provider_fields(self.configuration(saved.configuration.clone(), None));
        let saved = get_provider_fields(saved.configuration);
        let choices = &self.choices;
        for (field, sent, map) in [
            ("displayName", choices.display_name.is_some(), false),
            ("servers", choices.servers.is_some(), true),
            ("authentication", choices.authentication.is_some(), false),
            ("requestHeaders", choices.request_headers.is_some(), true),
            (
                "credentialFields",
                choices.credential_fields.is_some(),
                true,
            ),
        ] {
            if !sent {
                continue;
            }
            let saved = saved.get(field).cloned().unwrap_or(Value::Null);
            let requested = requested.get(field).cloned().unwrap_or(Value::Null);
            fields.push(if map {
                FieldChange::Entries {
                    field,
                    entries: entries(saved, requested),
                }
            } else {
                FieldChange::Value {
                    field,
                    saved,
                    requested,
                }
            });
        }
        ProviderChanges::Fields(fields)
    }
}

/// A configuration in `getProvider`'s shapes, keyed by field. A fixed
/// credential value is left out, as `getProvider` leaves it out.
fn get_provider_fields(configuration: ProviderConfiguration) -> Map<String, Value> {
    let ProviderConfiguration {
        display_name,
        servers,
        authentication,
        request_headers,
        credential_fields,
    } = configuration;
    let configured = authentication.is_some();
    let output = ConfigurationOutput::new(
        display_name,
        servers,
        authentication.unwrap_or(AuthenticationConfiguration::None),
        request_headers,
        credential_fields,
    );
    let Value::Object(mut fields) = to_json(&output) else {
        return Map::new();
    };
    if !configured {
        fields.insert("authentication".to_owned(), Value::Null);
    }
    fields
}

fn to_json(value: &impl Serialize) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

/// Every key either map holds, in key order, with what the call does to it.
fn entries(saved: Value, requested: Value) -> Vec<EntryChange> {
    let into_map = |value| match value {
        Value::Object(map) => map.into_iter().collect::<BTreeMap<_, _>>(),
        _ => BTreeMap::new(),
    };
    let mut saved = into_map(saved);
    let mut requested = into_map(requested);
    let keys = saved
        .keys()
        .chain(requested.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    keys.into_iter()
        .map(|key| {
            let saved = saved.remove(&key);
            let requested = requested.remove(&key);
            let status = match (&saved, &requested) {
                (None, _) => EntryStatus::Added,
                (_, None) => EntryStatus::Removed,
                (Some(saved), Some(requested)) if saved == requested => EntryStatus::Kept,
                (Some(_), Some(_)) => EntryStatus::Changed,
            };
            EntryChange {
                key,
                status,
                saved,
                requested,
            }
        })
        .collect()
}
