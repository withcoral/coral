//! Catalog search snapshot built from app-owned engine catalog views.

use coral_engine::{
    CatalogInfo, ColumnInfo, TableFunctionArgumentInfo, TableFunctionInfo,
    TableFunctionResultColumnInfo, TableInfo,
};
use coral_spec::SourceTableFunctionKind;
use sha2::{Digest as _, Sha256};

use crate::catalog::model::RuntimeRelationOwners;
use crate::search::catalog::sqlite_index::{
    CatalogIndexDocument, CatalogIndexDocumentKind, CatalogIndexSnapshot,
};
use crate::search::result::{SearchFieldRole, SearchSurfaceKind};

const CATALOG_SEARCH_SNAPSHOT_VERSION: &str = "catalog-search-snapshot-v4";

#[derive(Debug, Clone)]
pub(crate) struct CatalogSearchSnapshot {
    pub(crate) documents: Vec<CatalogDocument>,
    pub(crate) fingerprint: String,
}

impl CatalogSearchSnapshot {
    #[cfg(test)]
    pub(crate) fn from_catalog(catalog: &CatalogInfo) -> Self {
        Self::from_catalog_with_runtime_relation_owners(catalog, &RuntimeRelationOwners::new())
    }

    pub(crate) fn from_catalog_with_runtime_relation_owners(
        catalog: &CatalogInfo,
        runtime_relation_owners: &RuntimeRelationOwners,
    ) -> Self {
        let runtime_relation_owners =
            normalized_runtime_relation_owners(catalog, runtime_relation_owners);
        let mut documents = catalog_documents(catalog);
        for document in &mut documents {
            document.owner_source_name = runtime_relation_owners
                .get(&(document.catalog_name.clone(), document.schema_name.clone()))
                .cloned()
                .unwrap_or_else(|| document.source_name.clone());
        }
        documents.sort_by(|left, right| left.doc_id.cmp(&right.doc_id));
        let fingerprint = catalog_snapshot_fingerprint(catalog, &runtime_relation_owners);
        Self {
            documents,
            fingerprint,
        }
    }

    pub(crate) fn fingerprint_catalog_with_runtime_relation_owners(
        catalog: &CatalogInfo,
        runtime_relation_owners: &RuntimeRelationOwners,
    ) -> String {
        catalog_snapshot_fingerprint(
            catalog,
            &normalized_runtime_relation_owners(catalog, runtime_relation_owners),
        )
    }

    pub(crate) fn index_snapshot(&self) -> CatalogIndexSnapshot {
        CatalogIndexSnapshot {
            documents: self
                .documents
                .iter()
                .map(CatalogDocument::index_document)
                .collect(),
            fingerprint: self.fingerprint.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogDocument {
    pub(crate) doc_id: String,
    pub(crate) doc_kind: CatalogDocumentKind,
    pub(crate) owner_source_name: String,
    pub(crate) source_name: String,
    pub(crate) catalog_name: Option<String>,
    pub(crate) schema_name: String,
    pub(crate) surface_kind: Option<SearchSurfaceKind>,
    pub(crate) surface_name: String,
    pub(crate) field_name: String,
    pub(crate) field_role: Option<SearchFieldRole>,
    pub(crate) qualified_name: String,
    pub(crate) title: String,
    pub(crate) description: String,
    pub(crate) searchable_text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogDocumentKind {
    CatalogTable,
    CatalogTableFunction,
    ColumnHint,
}

impl CatalogDocument {
    fn index_document(&self) -> CatalogIndexDocument {
        CatalogIndexDocument {
            doc_id: self.doc_id.clone(),
            doc_kind: catalog_document_kind_to_index(self.doc_kind),
            owner_source_name: self.owner_source_name.clone(),
            source_name: self.source_name.clone(),
            surface_kind: surface_kind_as_str(self.surface_kind).to_string(),
            surface_name: self.surface_name.clone(),
            field_name: self.field_name.clone(),
            field_role: field_role_as_str(self.field_role).to_string(),
            qualified_name: self.qualified_name.clone(),
            title: self.title.clone(),
            description: self.description.clone(),
            searchable_text: self.searchable_text.clone(),
            payload_json: serde_json::json!({
                "catalog_name": self.catalog_name,
                "schema_name": self.schema_name,
            })
            .to_string(),
        }
    }
}

fn catalog_document_kind_to_index(kind: CatalogDocumentKind) -> CatalogIndexDocumentKind {
    match kind {
        CatalogDocumentKind::CatalogTable => CatalogIndexDocumentKind::CatalogTable,
        CatalogDocumentKind::CatalogTableFunction => CatalogIndexDocumentKind::CatalogTableFunction,
        CatalogDocumentKind::ColumnHint => CatalogIndexDocumentKind::ColumnHint,
    }
}

pub(crate) fn surface_kind_as_str(surface_kind: Option<SearchSurfaceKind>) -> &'static str {
    match surface_kind {
        Some(SearchSurfaceKind::Table) => "table",
        Some(SearchSurfaceKind::TableFunction) => "table_function",
        None => "",
    }
}

pub(crate) fn surface_kind_from_str(value: &str) -> Option<SearchSurfaceKind> {
    match value {
        "table" => Some(SearchSurfaceKind::Table),
        "table_function" => Some(SearchSurfaceKind::TableFunction),
        _ => None,
    }
}

pub(crate) fn field_role_as_str(field_role: Option<SearchFieldRole>) -> &'static str {
    match field_role {
        Some(SearchFieldRole::TableColumn) => "table_column",
        Some(SearchFieldRole::TableFilter) => "table_filter",
        Some(SearchFieldRole::TableFunctionArgument) => "table_function_argument",
        Some(SearchFieldRole::TableFunctionResultColumn) => "table_function_result_column",
        None => "",
    }
}

pub(crate) fn field_role_from_str(value: &str) -> Option<SearchFieldRole> {
    match value {
        "table_column" => Some(SearchFieldRole::TableColumn),
        "table_filter" => Some(SearchFieldRole::TableFilter),
        "table_function_argument" => Some(SearchFieldRole::TableFunctionArgument),
        "table_function_result_column" => Some(SearchFieldRole::TableFunctionResultColumn),
        _ => None,
    }
}

fn catalog_documents(catalog: &CatalogInfo) -> Vec<CatalogDocument> {
    let mut documents = Vec::new();
    for table in &catalog.tables {
        table_documents(table, &mut documents);
    }
    for function in &catalog.table_functions {
        table_function_documents(function, &mut documents);
    }
    documents
}

fn normalized_runtime_relation_owners(
    catalog: &CatalogInfo,
    runtime_relation_owners: &RuntimeRelationOwners,
) -> RuntimeRelationOwners {
    let mut normalized = runtime_relation_owners.clone();
    for (catalog_name, schema_name) in catalog
        .tables
        .iter()
        .map(|table| (table.catalog_name.as_ref(), table.schema_name.as_str()))
        .chain(catalog.table_functions.iter().map(|function| {
            (
                function.catalog_name.as_ref(),
                function.schema_name.as_str(),
            )
        }))
    {
        let source_name = catalog_name.map_or(schema_name, String::as_str);
        normalized
            .entry((catalog_name.cloned(), schema_name.to_string()))
            .or_insert_with(|| source_name.to_string());
    }
    normalized
}

fn table_documents(table: &TableInfo, documents: &mut Vec<CatalogDocument>) {
    let qualified_name = sql_reference(
        table.catalog_name.as_deref(),
        &table.schema_name,
        &table.table_name,
    );
    let source_name = relation_source_name(table.catalog_name.as_deref(), &table.schema_name);
    documents.push(CatalogDocument {
        doc_id: format!("catalog:table:{qualified_name}"),
        doc_kind: CatalogDocumentKind::CatalogTable,
        owner_source_name: source_name.clone(),
        source_name,
        catalog_name: table.catalog_name.clone(),
        schema_name: table.schema_name.clone(),
        surface_kind: Some(SearchSurfaceKind::Table),
        surface_name: table.table_name.clone(),
        field_name: String::new(),
        field_role: None,
        qualified_name: qualified_name.clone(),
        title: table.table_name.clone(),
        description: table.description.clone(),
        searchable_text: join_search_text([
            catalog_name_text(table.catalog_name.as_deref()),
            table.schema_name.as_str(),
            table.table_name.as_str(),
            qualified_name.as_str(),
            table.description.as_str(),
            table.guide.as_str(),
            table.required_filters.join(" ").as_str(),
        ]),
    });

    for column in &table.columns {
        table_column_document(table, column, documents);
    }
    for filter in &table.required_filters {
        table_required_filter_document(table, filter, documents);
    }
}

fn table_column_document(
    table: &TableInfo,
    column: &ColumnInfo,
    documents: &mut Vec<CatalogDocument>,
) {
    let surface_qualified_name = sql_reference(
        table.catalog_name.as_deref(),
        &table.schema_name,
        &table.table_name,
    );
    let source_name = relation_source_name(table.catalog_name.as_deref(), &table.schema_name);
    documents.push(CatalogDocument {
        doc_id: format!("column:table:{surface_qualified_name}:{}", column.name),
        doc_kind: CatalogDocumentKind::ColumnHint,
        owner_source_name: source_name.clone(),
        source_name,
        catalog_name: table.catalog_name.clone(),
        schema_name: table.schema_name.clone(),
        surface_kind: Some(SearchSurfaceKind::Table),
        surface_name: table.table_name.clone(),
        field_name: column.name.clone(),
        field_role: Some(SearchFieldRole::TableColumn),
        qualified_name: format!("{surface_qualified_name}.{}", column.name),
        title: column.name.clone(),
        description: column.description.clone(),
        searchable_text: join_search_text([
            catalog_name_text(table.catalog_name.as_deref()),
            table.schema_name.as_str(),
            table.table_name.as_str(),
            column.name.as_str(),
            column.data_type.as_str(),
            column.description.as_str(),
        ]),
    });
}

fn table_required_filter_document(
    table: &TableInfo,
    filter: &str,
    documents: &mut Vec<CatalogDocument>,
) {
    let surface_qualified_name = sql_reference(
        table.catalog_name.as_deref(),
        &table.schema_name,
        &table.table_name,
    );
    let source_name = relation_source_name(table.catalog_name.as_deref(), &table.schema_name);
    documents.push(CatalogDocument {
        doc_id: format!("filter:table:{surface_qualified_name}:{filter}"),
        doc_kind: CatalogDocumentKind::ColumnHint,
        owner_source_name: source_name.clone(),
        source_name,
        catalog_name: table.catalog_name.clone(),
        schema_name: table.schema_name.clone(),
        surface_kind: Some(SearchSurfaceKind::Table),
        surface_name: table.table_name.clone(),
        field_name: filter.to_string(),
        field_role: Some(SearchFieldRole::TableFilter),
        qualified_name: format!("{surface_qualified_name}.{filter}"),
        title: filter.to_string(),
        description: "Required table filter".to_string(),
        searchable_text: join_search_text([
            catalog_name_text(table.catalog_name.as_deref()),
            table.schema_name.as_str(),
            table.table_name.as_str(),
            filter,
            "required table filter",
        ]),
    });
}

fn table_function_documents(function: &TableFunctionInfo, documents: &mut Vec<CatalogDocument>) {
    let qualified_name = sql_reference(
        function.catalog_name.as_deref(),
        &function.schema_name,
        &function.function_name,
    );
    let source_name = relation_source_name(function.catalog_name.as_deref(), &function.schema_name);
    let source_native_search_keywords = if function.kind == SourceTableFunctionKind::Search {
        "source native search provider route fanout"
    } else {
        ""
    };
    let arguments = function
        .arguments
        .iter()
        .map(|argument| argument.name.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    let result_columns = function
        .result_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    documents.push(CatalogDocument {
        doc_id: format!("catalog:function:{qualified_name}"),
        doc_kind: CatalogDocumentKind::CatalogTableFunction,
        owner_source_name: source_name.clone(),
        source_name,
        catalog_name: function.catalog_name.clone(),
        schema_name: function.schema_name.clone(),
        surface_kind: Some(SearchSurfaceKind::TableFunction),
        surface_name: function.function_name.clone(),
        field_name: String::new(),
        field_role: None,
        qualified_name: qualified_name.clone(),
        title: function.function_name.clone(),
        description: function.description.clone(),
        searchable_text: join_search_text([
            catalog_name_text(function.catalog_name.as_deref()),
            function.schema_name.as_str(),
            function.function_name.as_str(),
            qualified_name.as_str(),
            function.description.as_str(),
            function.guide.as_str(),
            function.kind.as_str(),
            source_native_search_keywords,
            arguments.as_str(),
            result_columns.as_str(),
        ]),
    });

    for argument in &function.arguments {
        table_function_argument_document(function, argument, documents);
    }
    for column in &function.result_columns {
        table_function_result_column_document(function, column, documents);
    }
}

fn table_function_argument_document(
    function: &TableFunctionInfo,
    argument: &TableFunctionArgumentInfo,
    documents: &mut Vec<CatalogDocument>,
) {
    let surface_qualified_name = sql_reference(
        function.catalog_name.as_deref(),
        &function.schema_name,
        &function.function_name,
    );
    let source_name = relation_source_name(function.catalog_name.as_deref(), &function.schema_name);
    let values = argument.values.join(" ");
    documents.push(CatalogDocument {
        doc_id: format!(
            "argument:function:{surface_qualified_name}:{}",
            argument.name
        ),
        doc_kind: CatalogDocumentKind::ColumnHint,
        owner_source_name: source_name.clone(),
        source_name,
        catalog_name: function.catalog_name.clone(),
        schema_name: function.schema_name.clone(),
        surface_kind: Some(SearchSurfaceKind::TableFunction),
        surface_name: function.function_name.clone(),
        field_name: argument.name.clone(),
        field_role: Some(SearchFieldRole::TableFunctionArgument),
        qualified_name: format!("{surface_qualified_name}.{}", argument.name),
        title: argument.name.clone(),
        description: "Table function argument".to_string(),
        searchable_text: join_search_text([
            catalog_name_text(function.catalog_name.as_deref()),
            function.schema_name.as_str(),
            function.function_name.as_str(),
            argument.name.as_str(),
            values.as_str(),
            "table function argument",
        ]),
    });
}

fn table_function_result_column_document(
    function: &TableFunctionInfo,
    column: &TableFunctionResultColumnInfo,
    documents: &mut Vec<CatalogDocument>,
) {
    let surface_qualified_name = sql_reference(
        function.catalog_name.as_deref(),
        &function.schema_name,
        &function.function_name,
    );
    let source_name = relation_source_name(function.catalog_name.as_deref(), &function.schema_name);
    documents.push(CatalogDocument {
        doc_id: format!(
            "result_column:function:{surface_qualified_name}:{}",
            column.name
        ),
        doc_kind: CatalogDocumentKind::ColumnHint,
        owner_source_name: source_name.clone(),
        source_name,
        catalog_name: function.catalog_name.clone(),
        schema_name: function.schema_name.clone(),
        surface_kind: Some(SearchSurfaceKind::TableFunction),
        surface_name: function.function_name.clone(),
        field_name: column.name.clone(),
        field_role: Some(SearchFieldRole::TableFunctionResultColumn),
        qualified_name: format!("{surface_qualified_name}.{}", column.name),
        title: column.name.clone(),
        description: column.description.clone(),
        searchable_text: join_search_text([
            catalog_name_text(function.catalog_name.as_deref()),
            function.schema_name.as_str(),
            function.function_name.as_str(),
            column.name.as_str(),
            column.data_type.as_str(),
            column.description.as_str(),
            "table function result column",
        ]),
    });
}

fn sql_reference(catalog_name: Option<&str>, schema_name: &str, surface_name: &str) -> String {
    catalog_name.map_or_else(
        || format!("{schema_name}.{surface_name}"),
        |catalog_name| format!("{catalog_name}.{schema_name}.{surface_name}"),
    )
}

fn relation_source_name(catalog_name: Option<&str>, schema_name: &str) -> String {
    catalog_name.unwrap_or(schema_name).to_string()
}

fn catalog_name_text(catalog_name: Option<&str>) -> &str {
    catalog_name.unwrap_or_default()
}

fn join_search_text<const N: usize>(parts: [&str; N]) -> String {
    parts
        .into_iter()
        .filter(|part| !part.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn catalog_snapshot_fingerprint(
    catalog: &CatalogInfo,
    runtime_relation_owners: &RuntimeRelationOwners,
) -> String {
    let mut hasher = Sha256::new();
    update_hash(&mut hasher, CATALOG_SEARCH_SNAPSHOT_VERSION);

    for ((catalog_name, schema_name), owner_source_name) in runtime_relation_owners {
        update_hash(&mut hasher, "runtime_relation_owner");
        update_hash(&mut hasher, catalog_name.as_deref().unwrap_or_default());
        update_hash(&mut hasher, schema_name);
        update_hash(&mut hasher, owner_source_name);
    }

    let mut tables = catalog.tables.iter().collect::<Vec<_>>();
    tables.sort_by(|left, right| {
        (
            left.catalog_name.as_deref().unwrap_or_default(),
            left.schema_name.as_str(),
            left.table_name.as_str(),
        )
            .cmp(&(
                right.catalog_name.as_deref().unwrap_or_default(),
                right.schema_name.as_str(),
                right.table_name.as_str(),
            ))
    });
    for table in tables {
        update_hash(&mut hasher, "table");
        update_hash(
            &mut hasher,
            table.catalog_name.as_deref().unwrap_or_default(),
        );
        update_hash(&mut hasher, &table.schema_name);
        update_hash(&mut hasher, &table.table_name);
        update_hash(&mut hasher, &table.description);
        update_hash(&mut hasher, &table.guide);
        let mut columns = table.columns.iter().collect::<Vec<_>>();
        columns.sort_by(|left, right| {
            (left.ordinal_position, left.name.as_str())
                .cmp(&(right.ordinal_position, right.name.as_str()))
        });
        for column in columns {
            update_column_hash(&mut hasher, column);
        }
        let mut required_filters = table.required_filters.iter().collect::<Vec<_>>();
        required_filters.sort();
        for filter in required_filters {
            update_hash(&mut hasher, "required_filter");
            update_hash(&mut hasher, filter);
        }
    }

    let mut functions = catalog.table_functions.iter().collect::<Vec<_>>();
    functions.sort_by(|left, right| {
        (
            left.catalog_name.as_deref().unwrap_or_default(),
            left.schema_name.as_str(),
            left.function_name.as_str(),
        )
            .cmp(&(
                right.catalog_name.as_deref().unwrap_or_default(),
                right.schema_name.as_str(),
                right.function_name.as_str(),
            ))
    });
    for function in functions {
        update_hash(&mut hasher, "table_function");
        update_hash(
            &mut hasher,
            function.catalog_name.as_deref().unwrap_or_default(),
        );
        update_hash(&mut hasher, &function.schema_name);
        update_hash(&mut hasher, &function.function_name);
        update_hash(&mut hasher, &function.description);
        update_hash(&mut hasher, &function.guide);
        update_hash(&mut hasher, function.kind.as_str());
        let search_limits_json = function
            .search_limits
            .as_ref()
            .map(|limits| serde_json::to_string(limits).expect("search limits json"));
        update_hash(
            &mut hasher,
            search_limits_json.as_deref().unwrap_or_default(),
        );
        let mut arguments = function.arguments.iter().collect::<Vec<_>>();
        arguments.sort_by(|left, right| left.name.cmp(&right.name));
        for argument in arguments {
            update_hash(&mut hasher, "argument");
            update_hash(&mut hasher, &argument.name);
            update_hash_bool(&mut hasher, argument.required);
            for value in &argument.values {
                update_hash(&mut hasher, value);
            }
        }
        let mut result_columns = function.result_columns.iter().collect::<Vec<_>>();
        result_columns.sort_by(|left, right| left.name.cmp(&right.name));
        for column in result_columns {
            update_hash(&mut hasher, "result_column");
            update_hash(&mut hasher, &column.name);
            update_hash(&mut hasher, &column.data_type);
            update_hash_bool(&mut hasher, column.nullable);
            update_hash(&mut hasher, &column.description);
        }
    }

    format!("{:x}", hasher.finalize())
}

fn update_column_hash(hasher: &mut Sha256, column: &ColumnInfo) {
    update_hash(hasher, "column");
    update_hash(hasher, &column.name);
    update_hash(hasher, &column.data_type);
    update_hash_bool(hasher, column.nullable);
    update_hash_bool(hasher, column.is_virtual);
    update_hash_bool(hasher, column.is_required_filter);
    update_hash(hasher, &column.description);
    update_hash(hasher, &column.ordinal_position.to_string());
}

fn update_hash_bool(hasher: &mut Sha256, value: bool) {
    update_hash(hasher, if value { "true" } else { "false" });
}

fn update_hash(hasher: &mut Sha256, value: &str) {
    hasher.update(value.as_bytes());
    hasher.update([0]);
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use coral_engine::{CatalogInfo, TableInfo};

    use super::{CatalogDocumentKind, CatalogIndexDocumentKind, CatalogSearchSnapshot};

    #[test]
    fn snapshot_fingerprint_changes_with_catalog_content() {
        let first = CatalogSearchSnapshot::from_catalog(&catalog_with_table("messages"));
        let second = CatalogSearchSnapshot::from_catalog(&catalog_with_table("tasks"));

        assert_ne!(first.fingerprint, second.fingerprint);
    }

    #[test]
    fn snapshot_fingerprint_and_documents_include_installed_source_ownership() {
        let catalog = catalog_with_table("messages");
        let first = CatalogSearchSnapshot::from_catalog_with_runtime_relation_owners(
            &catalog,
            &BTreeMap::from([((None, "fixture".to_string()), "owner_a".to_string())]),
        );
        let second = CatalogSearchSnapshot::from_catalog_with_runtime_relation_owners(
            &catalog,
            &BTreeMap::from([((None, "fixture".to_string()), "owner_b".to_string())]),
        );

        assert_ne!(first.fingerprint, second.fingerprint);
        assert!(
            first
                .documents
                .iter()
                .all(|document| document.owner_source_name == "owner_a")
        );
        assert!(
            first
                .index_snapshot()
                .documents
                .iter()
                .all(|document| document.owner_source_name == "owner_a")
        );
    }

    #[test]
    fn snapshot_indexes_tables_and_columns() {
        let snapshot = CatalogSearchSnapshot::from_catalog(&catalog_with_table("messages"));

        assert!(
            snapshot
                .documents
                .iter()
                .any(|doc| doc.doc_kind == CatalogDocumentKind::CatalogTable
                    && doc.surface_name == "messages")
        );
        assert!(snapshot.documents.iter().any(|doc| doc.doc_kind
            == CatalogDocumentKind::ColumnHint
            && doc.field_name == "title"));
    }

    #[test]
    fn catalog_search_qualified_identity_uses_catalog_schema_and_leaf() {
        let mut catalog = catalog_with_table("messages");
        let catalog_table = catalog.tables.first_mut().expect("catalog table");
        catalog_table.catalog_name = Some("github_v4".to_string());
        catalog_table.schema_name = "issues".to_string();
        let owners = BTreeMap::from([(
            (Some("github_v4".to_string()), "issues".to_string()),
            "installed_github".to_string(),
        )]);

        let snapshot =
            CatalogSearchSnapshot::from_catalog_with_runtime_relation_owners(&catalog, &owners);
        let table = snapshot
            .documents
            .iter()
            .find(|document| document.doc_kind == CatalogDocumentKind::CatalogTable)
            .expect("table document");
        assert_eq!(table.doc_id, "catalog:table:github_v4.issues.messages");
        assert_eq!(table.qualified_name, "github_v4.issues.messages");
        assert_eq!(table.owner_source_name, "installed_github");
        assert_eq!(table.source_name, "github_v4");
        assert!(table.searchable_text.contains("github_v4"));

        let indexed = snapshot
            .index_snapshot()
            .documents
            .into_iter()
            .find(|document| document.doc_kind == CatalogIndexDocumentKind::CatalogTable)
            .expect("indexed table");
        let payload: serde_json::Value =
            serde_json::from_str(&indexed.payload_json).expect("identity payload");
        assert_eq!(
            payload.pointer("/catalog_name"),
            Some(&serde_json::json!("github_v4"))
        );
        assert_eq!(
            payload.pointer("/schema_name"),
            Some(&serde_json::json!("issues"))
        );
    }

    fn catalog_with_table(table_name: &str) -> CatalogInfo {
        CatalogInfo {
            tables: vec![TableInfo {
                catalog_name: None,
                schema_name: "fixture".to_string(),
                table_name: table_name.to_string(),
                description: format!("Fixture {table_name}"),
                guide: String::new(),
                columns: vec![coral_engine::ColumnInfo {
                    name: "title".to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: true,
                    is_virtual: false,
                    is_required_filter: false,
                    description: "Message title".to_string(),
                    ordinal_position: 0,
                }],
                required_filters: Vec::new(),
            }],
            table_functions: Vec::new(),
        }
    }
}
