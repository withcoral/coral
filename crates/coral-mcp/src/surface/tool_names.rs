#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ToolName {
    Sql,
    ListCatalog,
    SearchCatalog,
    DescribeTable,
    ListColumns,
    OpenEpisode,
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
            Self::OpenEpisode => "open_episode",
            Self::Feedback => "feedback",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "sql" => Some(Self::Sql),
            "list_catalog" => Some(Self::ListCatalog),
            "search_catalog" => Some(Self::SearchCatalog),
            "describe_table" => Some(Self::DescribeTable),
            "list_columns" => Some(Self::ListColumns),
            "open_episode" => Some(Self::OpenEpisode),
            "feedback" => Some(Self::Feedback),
            _ => None,
        }
    }
}
