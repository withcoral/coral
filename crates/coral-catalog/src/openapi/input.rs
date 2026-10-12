use crate::Error;

/// Maximum exact OpenAPI input size accepted by the synchronous profile.
pub const OPENAPI_DOCUMENT_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Exact UTF-8 OpenAPI input accepted for durable Provider Setup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenApiDocument {
    source: String,
    pub(super) configuration: Option<Box<crate::ProviderConfiguration>>,
    pub(super) configuration_revision: i64,
}

impl OpenApiDocument {
    /// Accepts exact bounded nonempty UTF-8 input for later materialization.
    ///
    /// # Errors
    ///
    /// Returns [`Error::OpenApiDocumentTooLarge`] for oversized input, or
    /// [`Error::InvalidInput`] for empty or null-bearing input.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.len() > OPENAPI_DOCUMENT_MAX_BYTES {
            return Err(Error::OpenApiDocumentTooLarge);
        }
        if value.is_empty() || value.contains('\0') {
            return Err(Error::InvalidInput(
                "OpenAPI input must be non-empty and contain no null character".into(),
            ));
        }
        Ok(Self {
            source: value.to_string(),
            configuration: None,
            configuration_revision: 0,
        })
    }

    /// Returns the exact accepted input.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.source
    }
}
