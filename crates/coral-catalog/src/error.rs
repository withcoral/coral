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

impl Error {
    /// The database error this one is, if any. A module that wraps this
    /// error asks here, so it never lists another module's variants.
    #[must_use]
    pub const fn database_error(&self) -> Option<&coral_db::Error> {
        match self {
            Self::Database(error) => Some(error),
            // Name every variant so a new one that holds a database error
            // is found here.
            Self::OpenApiDocumentTooLarge
            | Self::OpenApiDocumentNotText
            | Self::InvalidInput(_)
            | Self::CorruptData(_) => None,
        }
    }
}

impl Error {
    /// The database error behind this one, by value, or this error back
    /// when it holds none. [`Self::database_error`] says which it is.
    ///
    /// # Errors
    ///
    /// Returns the error itself when no database error is behind it.
    pub fn into_database_error(self) -> Result<coral_db::Error, Self> {
        match self {
            Self::Database(error) => Ok(error),
            other => {
                debug_assert!(other.database_error().is_none());
                Err(other)
            }
        }
    }
}
