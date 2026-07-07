use std::{fmt, str::FromStr};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ToolName {
    Sql,
    ListCatalog,
    SearchCatalog,
    DescribeTable,
    ListColumns,
    Feedback,
}

impl ToolName {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Sql => "sql",
            Self::ListCatalog => "list_catalog",
            Self::SearchCatalog => "search_catalog",
            Self::DescribeTable => "describe_table",
            Self::ListColumns => "list_columns",
            Self::Feedback => "feedback",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct UnknownToolName;

impl FromStr for ToolName {
    type Err = UnknownToolName;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "sql" => Ok(Self::Sql),
            "list_catalog" => Ok(Self::ListCatalog),
            "search_catalog" => Ok(Self::SearchCatalog),
            "describe_table" => Ok(Self::DescribeTable),
            "list_columns" => Ok(Self::ListColumns),
            "feedback" => Ok(Self::Feedback),
            _ => Err(UnknownToolName),
        }
    }
}

impl fmt::Display for ToolName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str((*self).as_str())
    }
}
