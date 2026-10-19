/// Catalog materialization or persistence failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An OpenAPI document exceeds the accepted byte budget.
    #[error("OpenAPI document exceeds the size limit")]
    OpenApiDocumentTooLarge,
    /// An OpenAPI document is neither UTF-8 text nor gzipped UTF-8 text.
    #[error("OpenAPI document is not text")]
    OpenApiDocumentNotText,
    /// Caller-supplied catalog input was invalid.
    #[error("invalid catalog input: {0}")]
    InvalidInput(String),
    /// A persisted row does not decode to the closed catalog model.
    #[error("database contains corrupt Catalog data: {0}")]
    CorruptData(String),
    /// The persistence Adapter rejected the operation.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
}
