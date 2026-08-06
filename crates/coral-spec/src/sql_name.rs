use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

/// Complete, validated SQL identity for a table or table function.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct SqlObjectName {
    catalog_name: String,
    schema_name: String,
    name: String,
}

#[derive(Deserialize)]
struct SerializedSqlObjectName {
    catalog_name: String,
    schema_name: String,
    name: String,
}

impl<'de> Deserialize<'de> for SqlObjectName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let serialized = SerializedSqlObjectName::deserialize(deserializer)?;
        Self::try_new(
            serialized.catalog_name,
            serialized.schema_name,
            serialized.name,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl SqlObjectName {
    /// Constructs a complete SQL name after validating every identifier.
    pub fn try_new(
        catalog_name: impl Into<String>,
        schema_name: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Self, SqlObjectNameError> {
        let catalog_name = catalog_name.into();
        let schema_name = schema_name.into();
        let name = name.into();
        validate_coordinate(SqlObjectNameCoordinate::Catalog, &catalog_name)?;
        validate_coordinate(SqlObjectNameCoordinate::Schema, &schema_name)?;
        validate_coordinate(SqlObjectNameCoordinate::Name, &name)?;
        Ok(Self {
            catalog_name,
            schema_name,
            name,
        })
    }

    #[must_use]
    /// Returns the catalog coordinate.
    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    #[must_use]
    /// Returns the schema coordinate.
    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    #[must_use]
    /// Returns the bare relation or function coordinate.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Display for SqlObjectName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_coordinate(formatter, &self.catalog_name)?;
        formatter.write_str(".")?;
        fmt_coordinate(formatter, &self.schema_name)?;
        formatter.write_str(".")?;
        fmt_coordinate(formatter, &self.name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlObjectNameCoordinate {
    Catalog,
    Schema,
    Name,
}

impl fmt::Display for SqlObjectNameCoordinate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Catalog => "catalog_name",
            Self::Schema => "schema_name",
            Self::Name => "name",
        })
    }
}

/// Validation failure for one coordinate of a [`SqlObjectName`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("SQL object {coordinate} {reason}: '{value}'")]
pub struct SqlObjectNameError {
    coordinate: SqlObjectNameCoordinate,
    value: String,
    reason: &'static str,
}

fn validate_coordinate(
    coordinate: SqlObjectNameCoordinate,
    value: &str,
) -> Result<(), SqlObjectNameError> {
    if value.trim().is_empty() {
        return Err(SqlObjectNameError {
            coordinate,
            value: value.to_string(),
            reason: "must not be empty",
        });
    }
    if value.chars().any(char::is_control) {
        return Err(SqlObjectNameError {
            coordinate,
            value: value.to_string(),
            reason: "must not contain control characters",
        });
    }
    Ok(())
}

fn fmt_coordinate(formatter: &mut fmt::Formatter<'_>, value: &str) -> fmt::Result {
    let mut chars = value.chars();
    let can_render_bare = chars
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric());
    if can_render_bare {
        formatter.write_str(value)
    } else {
        write!(formatter, "\"{}\"", value.replace('"', "\"\""))
    }
}

#[cfg(test)]
mod tests {
    use super::SqlObjectName;

    #[test]
    fn complete_sql_name_round_trips_with_named_coordinates() {
        let name =
            SqlObjectName::try_new("github_v4", "issues", "list_for_repo").expect("valid SQL name");
        let yaml = serde_yaml::to_string(&name).expect("serialize SQL name");

        assert!(yaml.contains("catalog_name: github_v4"));
        assert!(yaml.contains("schema_name: issues"));
        assert!(yaml.contains("name: list_for_repo"));
        assert_eq!(
            serde_yaml::from_str::<SqlObjectName>(&yaml).expect("deserialize SQL name"),
            name
        );
    }

    #[test]
    fn complete_sql_name_rejects_empty_or_control_character_coordinates() {
        for (catalog, schema, name, coordinate) in [
            ("", "issues", "list", "catalog_name"),
            ("github_v4", "\n", "list", "schema_name"),
            ("github_v4", "issues", "list\0for_repo", "name"),
        ] {
            let error = SqlObjectName::try_new(catalog, schema, name).expect_err("invalid name");
            assert!(error.to_string().contains(coordinate), "{error}");
        }
    }

    #[test]
    fn deserialization_cannot_bypass_coordinate_validation() {
        let error = serde_yaml::from_str::<SqlObjectName>(
            "catalog_name: github_v4\nschema_name: issues\nname: \"\\0\"\n",
        )
        .expect_err("invalid serialized SQL name");

        assert!(
            error.to_string().contains("name must not contain control"),
            "{error}"
        );
    }

    #[test]
    fn complete_sql_name_quotes_non_bare_coordinates_for_display() {
        let name = SqlObjectName::try_new("source-name", "public", "player.stats")
            .expect("quoted SQL name");

        assert_eq!(name.to_string(), "\"source-name\".public.\"player.stats\"");
    }
}
