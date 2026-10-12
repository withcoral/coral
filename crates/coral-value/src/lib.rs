//! Transport-neutral bounded JSON and inline Binary Values shared by host crossings.

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Effective deployment limits, snapshotted by each accepted Run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[expect(
    clippy::struct_field_names,
    reason = "persisted and advertised limits name their byte units explicitly"
)]
pub struct ValueLimits {
    binary_bytes: std::num::NonZeroUsize,
    host_bytes: std::num::NonZeroUsize,
    upstream_bytes: std::num::NonZeroUsize,
    run_output_bytes: std::num::NonZeroUsize,
}

impl Default for ValueLimits {
    fn default() -> Self {
        Self::new(8 << 20, 16 << 20, 16 << 20, 64 << 20).expect("default limits are positive")
    }
}

impl ValueLimits {
    /// Selects finite positive limits in bytes.
    ///
    /// # Errors
    /// Returns an error if any limit is zero.
    pub fn new(
        binary: usize,
        host: usize,
        upstream: usize,
        run: usize,
    ) -> Result<Self, ValueError> {
        let positive = |n| std::num::NonZeroUsize::new(n).ok_or(ValueError::Invalid);
        Ok(Self {
            binary_bytes: positive(binary)?,
            host_bytes: positive(host)?,
            upstream_bytes: positive(upstream)?,
            run_output_bytes: positive(run)?,
        })
    }

    /// Maximum decoded bytes per Binary Value or MCP binary payload.
    #[must_use]
    pub const fn binary_bytes(self) -> usize {
        self.binary_bytes.get()
    }
    /// Maximum encoded JSON bytes at a V8-host crossing.
    #[must_use]
    pub const fn host_bytes(self) -> usize {
        self.host_bytes.get()
    }
    /// Maximum bytes per upstream request or result.
    #[must_use]
    pub const fn upstream_bytes(self) -> usize {
        self.upstream_bytes.get()
    }
    /// Maximum cumulative durable Program output bytes.
    #[must_use]
    pub const fn run_output_bytes(self) -> usize {
        self.run_output_bytes.get()
    }

    /// Validates JSON and every nested Binary Value at a host crossing.
    ///
    /// # Errors
    /// Rejects invalid tags, metadata, base64, or exceeded bounds.
    pub fn validate_json(self, encoded: &str) -> Result<Value, ValueError> {
        if encoded.len() > self.host_bytes() {
            return Err(ValueError::TooLarge);
        }
        let value = serde_json::from_str(encoded).map_err(|_error| ValueError::Invalid)?;
        self.validate(&value)?;
        Ok(value)
    }

    /// Revalidates tagged values recursively without changing ordinary JSON data.
    ///
    /// # Errors
    /// Rejects malformed or oversized inline binary payloads.
    pub fn validate(self, value: &Value) -> Result<(), ValueError> {
        match value {
            Value::Object(fields) => {
                if fields.get("_tag").and_then(Value::as_str) == Some("BinaryValue") {
                    decode_binary(value, self.binary_bytes())?;
                } else {
                    // MCP retains its native block representation rather than a Binary Value.
                    match fields.get("type").and_then(Value::as_str) {
                        Some("image" | "audio")
                            if fields.get("data").is_some_and(Value::is_string)
                                && fields.get("mimeType").is_some_and(Value::is_string) =>
                        {
                            decode_base64(
                                fields
                                    .get("data")
                                    .and_then(Value::as_str)
                                    .ok_or(ValueError::Invalid)?,
                                self.binary_bytes(),
                            )?;
                        }
                        Some("resource") => {
                            if let Some(resource) = fields.get("resource")
                                && resource.get("blob").is_some_and(Value::is_string)
                                && (resource.get("uri").is_some_and(Value::is_string)
                                    || resource
                                        .get("reference")
                                        .and_then(|reference| reference.get("$coralMcpResource"))
                                        .is_some_and(Value::is_string))
                            {
                                self.validate_mcp_resource(resource)?;
                            }
                        }
                        _ => {}
                    }
                    for value in fields.values() {
                        self.validate(value)?;
                    }
                }
            }
            Value::Array(values) => {
                for value in values {
                    self.validate(value)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Bounds a native MCP resource payload in an explicitly known resource context.
    /// Generic JSON objects with `uri` and `blob` fields are not resource values.
    ///
    /// # Errors
    /// Rejects malformed or oversized inline base64 blobs.
    pub fn validate_mcp_resource(self, resource: &Value) -> Result<(), ValueError> {
        if let Some(blob) = resource.get("blob") {
            decode_base64(
                blob.as_str().ok_or(ValueError::Invalid)?,
                self.binary_bytes(),
            )?;
        }
        Ok(())
    }
}

/// Safe, content-free validation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ValueError {
    /// The value is not interoperable JSON or has a malformed binary extension.
    #[error("invalid Program Value")]
    Invalid,
    /// The complete value exceeds a finite limit.
    #[error("value exceeds its limit")]
    TooLarge,
}

/// Decodes the exact closed Binary Value contract, revalidating all metadata.
///
/// # Errors
/// Rejects unknown fields, noncanonical base64, invalid metadata, or oversized data.
pub fn decode_binary(value: &Value, maximum: usize) -> Result<Vec<u8>, ValueError> {
    let fields = value.as_object().ok_or(ValueError::Invalid)?;
    if fields.keys().any(|key| {
        !matches!(
            key.as_str(),
            "_tag" | "mimeType" | "encoding" | "data" | "byteLength" | "name"
        )
    }) || fields.get("_tag").and_then(Value::as_str) != Some("BinaryValue")
        || fields.get("encoding").and_then(Value::as_str) != Some("base64")
    {
        return Err(ValueError::Invalid);
    }
    let media = fields
        .get("mimeType")
        .and_then(Value::as_str)
        .ok_or(ValueError::Invalid)?;
    if !valid_mime_essence(media) {
        return Err(ValueError::Invalid);
    }
    if let Some(name) = fields.get("name") {
        let name = name.as_str().ok_or(ValueError::Invalid)?;
        if name.len() > 255
            || matches!(name, "." | "..")
            || name
                .chars()
                .any(|c| c.is_control() || matches!(c, '/' | '\\'))
        {
            return Err(ValueError::Invalid);
        }
    }
    let data = fields
        .get("data")
        .and_then(Value::as_str)
        .ok_or(ValueError::Invalid)?;
    let decoded = decode_base64(data, maximum)?;
    if fields.get("byteLength").and_then(Value::as_u64) != u64::try_from(decoded.len()).ok() {
        return Err(ValueError::Invalid);
    }
    Ok(decoded)
}

/// Constructs a canonical Binary Value from exact bytes.
///
/// # Errors
/// Rejects an invalid MIME essence or oversized data.
pub fn binary_value(bytes: &[u8], mime_type: &str, maximum: usize) -> Result<Value, ValueError> {
    if bytes.len() > maximum {
        return Err(ValueError::TooLarge);
    }
    if !valid_mime_essence(mime_type) {
        return Err(ValueError::Invalid);
    }
    Ok(
        json!({"_tag":"BinaryValue", "encoding":"base64", "mimeType":mime_type, "data":base64::engine::general_purpose::STANDARD.encode(bytes), "byteLength":bytes.len()}),
    )
}

fn valid_mime_essence(value: &str) -> bool {
    value.parse::<mime::Mime>().is_ok_and(|mime| {
        mime.essence_str() == value && !value.contains('*') && value == value.to_ascii_lowercase()
    })
}

fn decode_base64(data: &str, maximum: usize) -> Result<Vec<u8>, ValueError> {
    if data.len() / 4 * 3 > maximum.saturating_add(2) {
        return Err(ValueError::TooLarge);
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|_error| ValueError::Invalid)?;
    if bytes.len() > maximum {
        return Err(ValueError::TooLarge);
    }
    if base64::engine::general_purpose::STANDARD.encode(&bytes) != data {
        return Err(ValueError::Invalid);
    }
    Ok(bytes)
}
