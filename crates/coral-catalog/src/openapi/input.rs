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

    /// Accepts a document file: UTF-8 text, or gzipped UTF-8 text such as the
    /// `.json.gz` document Anthropic's SDKs publish. The size limit applies to
    /// the unzipped text.
    ///
    /// # Errors
    ///
    /// Returns [`Error::OpenApiDocumentNotText`] for corrupt gzip or other
    /// non-UTF-8 input, or the errors of [`Self::parse`].
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, Error> {
        let text = String::from_utf8(gunzip(bytes)?)
            .map_err(|_invalid_utf8| Error::OpenApiDocumentNotText)?;
        Self::parse(&text)
    }

    /// Returns the exact accepted input.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.source
    }
}

/// Unzips a gzip file, detected by its magic bytes, and returns any other
/// input unchanged.
fn gunzip(bytes: Vec<u8>) -> Result<Vec<u8>, Error> {
    use std::io::Read as _;
    const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];
    if !bytes.starts_with(&GZIP_MAGIC) {
        return Ok(bytes);
    }
    let mut text = Vec::new();
    flate2::read::MultiGzDecoder::new(bytes.as_slice())
        .take(
            u64::try_from(OPENAPI_DOCUMENT_MAX_BYTES)
                .unwrap_or(u64::MAX)
                .saturating_add(1),
        )
        .read_to_end(&mut text)
        .map_err(|_corrupt| Error::OpenApiDocumentNotText)?;
    if text.len() > OPENAPI_DOCUMENT_MAX_BYTES {
        return Err(Error::OpenApiDocumentTooLarge);
    }
    Ok(text)
}
