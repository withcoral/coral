use std::{fmt, str::FromStr};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ToolName {
    Sql,
    AddFunction,
    Search,
    ListCatalog,
    Describe,
    ListColumns,
    StartTask,
    EndTask,
    Feedback,
}

impl ToolName {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Sql => "sql",
            Self::AddFunction => "add_function",
            Self::Search => "search",
            Self::ListCatalog => "list_catalog",
            Self::Describe => "describe",
            Self::ListColumns => "list_columns",
            Self::StartTask => "start_task",
            Self::EndTask => "end_task",
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
            "add_function" => Ok(Self::AddFunction),
            "search" => Ok(Self::Search),
            "list_catalog" => Ok(Self::ListCatalog),
            "describe" => Ok(Self::Describe),
            "list_columns" => Ok(Self::ListColumns),
            "start_task" => Ok(Self::StartTask),
            "end_task" => Ok(Self::EndTask),
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
