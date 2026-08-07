use std::fmt;

/// A complete SQL identity shared by tables and table functions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SqlObjectName {
    catalog_name: String,
    schema_name: String,
    name: String,
}

impl SqlObjectName {
    /// Creates a complete SQL object name from normalized coordinates.
    pub fn new(
        catalog_name: impl Into<String>,
        schema_name: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            catalog_name: catalog_name.into(),
            schema_name: schema_name.into(),
            name: name.into(),
        }
    }

    /// Returns the SQL catalog coordinate.
    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    /// Returns the SQL schema coordinate.
    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    /// Returns the bare relation or function coordinate.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Display for SqlObjectName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}.{}.{}",
            self.catalog_name, self.schema_name, self.name
        )
    }
}
