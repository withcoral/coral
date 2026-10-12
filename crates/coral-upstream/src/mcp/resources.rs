//! Host-only resource URI retention. Tokens carry neither a URI nor authority
//! outside their originating Run, and every lookup also matches the Provider.

use std::collections::HashMap;
use std::sync::Mutex;

use coral_catalog::McpInvocationMethod;
use serde_json::{Value, json};

use crate::Error;

/// Run-local opaque MCP resource references. The Run owner shares this registry
/// across its Invocations and drops it when execution ends. No durable store or
/// cross-Run resource capability is created.
#[derive(Default)]
pub struct McpResources {
    entries: Mutex<Entries>,
}

#[derive(Default)]
struct Entries {
    by_token: HashMap<String, Resource>,
    bytes: usize,
}

struct Resource {
    provider: String,
    uri: String,
}

impl McpResources {
    pub(super) fn resolve(&self, provider: &str, reference: &Value) -> Result<String, Error> {
        let reference = reference
            .as_object()
            .filter(|value| value.len() == 1)
            .ok_or_else(invalid_reference)?;
        let token = reference
            .get("$coralMcpResource")
            .and_then(Value::as_str)
            .ok_or_else(invalid_reference)?;
        let entries = self.entries.lock().map_err(|_poisoned| Error::McpResult)?;
        let resource = entries
            .by_token
            .get(token)
            .filter(|resource| resource.provider == provider)
            .ok_or_else(invalid_reference)?;
        Ok(resource.uri.clone())
    }

    fn insert(&self, provider: &str, uri: &str) -> Result<Value, Error> {
        let mut entries = self.entries.lock().map_err(|_poisoned| Error::McpResult)?;
        // ponytail: one bounded Run-local map; add reclamation only if long Runs
        // need more than 16 MiB of retained URI and token bookkeeping.
        let bytes = entries
            .bytes
            .checked_add(provider.len())
            .and_then(|bytes| bytes.checked_add(uri.len()))
            .and_then(|bytes| bytes.checked_add(128))
            .ok_or(Error::ResultTooLarge)?;
        if bytes > 16 * 1024 * 1024 {
            return Err(Error::ResultTooLarge);
        }
        let token = uuid::Uuid::new_v4().to_string();
        entries.by_token.insert(
            token.clone(),
            Resource {
                provider: provider.to_string(),
                uri: uri.to_string(),
            },
        );
        entries.bytes = bytes;
        Ok(json!({"$coralMcpResource": token}))
    }

    pub(super) fn project(
        &self,
        provider: &str,
        method: McpInvocationMethod,
        data: &mut Value,
    ) -> Result<(), Error> {
        if method == McpInvocationMethod::CallTool
            && let Some(content) = data.get_mut("content").and_then(Value::as_array_mut)
        {
            for block in content {
                match block.get("type").and_then(Value::as_str) {
                    Some("resource_link") => self.replace_uri(provider, block, "resource")?,
                    Some("resource") => self.replace_uri(
                        provider,
                        block.get_mut("resource").ok_or(Error::McpResult)?,
                        "reference",
                    )?,
                    _ => {}
                }
            }
        }
        if method == McpInvocationMethod::ReadResource
            && let Some(contents) = data.get_mut("contents").and_then(Value::as_array_mut)
        {
            for resource in contents {
                self.replace_uri(provider, resource, "reference")?;
            }
        }
        Ok(())
    }

    fn replace_uri(&self, provider: &str, resource: &mut Value, field: &str) -> Result<(), Error> {
        let object = resource.as_object_mut().ok_or(Error::McpResult)?;
        let uri = object.remove("uri").ok_or(Error::McpResult)?;
        let uri = uri
            .as_str()
            .filter(|uri| !uri.is_empty())
            .ok_or(Error::McpResult)?;
        object.insert(field.to_string(), self.insert(provider, uri)?);
        Ok(())
    }
}

fn invalid_reference() -> Error {
    Error::InvalidArguments(
        "resource reference was not created by this Provider in this Run".to_string(),
    )
}
