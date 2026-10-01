use super::source_names::connected_source_names_text;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolDescriptionContext {
    pub(crate) visible_table_count: usize,
    pub(crate) visible_function_count: usize,
    connected_source_names: Vec<String>,
}

impl ToolDescriptionContext {
    pub(crate) fn new(
        visible_table_count: usize,
        visible_function_count: usize,
        mut connected_source_names: Vec<String>,
    ) -> Self {
        connected_source_names.sort();
        connected_source_names.dedup();
        Self {
            visible_table_count,
            visible_function_count,
            connected_source_names,
        }
    }

    pub(crate) fn connected_sources_sentence(&self) -> String {
        connected_source_names_text(&self.connected_source_names).map_or_else(
            || "No connected user sources are currently configured.".to_string(),
            |names| format!("Connected sources/schemas include: {names}."),
        )
    }
}
