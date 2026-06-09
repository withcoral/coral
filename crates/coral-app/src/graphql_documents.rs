//! GraphQL operation document artifact helpers.

use std::path::{Path, PathBuf};

use coral_capabilities::{GraphqlOperationBinding, GraphqlOperationKind, GraphqlVariableBinding};

pub(crate) const GENERATED_GRAPHQL_OPERATIONS_DIR: &str = "generated-graphql-operations";

pub(crate) fn operation_document_path(
    materialized_dir: &Path,
    interface_id: &str,
    binding: &GraphqlOperationBinding,
) -> Result<PathBuf, String> {
    Ok(materialized_dir
        .join("interfaces")
        .join(interface_id)
        .join(GENERATED_GRAPHQL_OPERATIONS_DIR)
        .join(operation_document_filename(binding)?))
}

pub(crate) fn operation_document_filename(
    binding: &GraphqlOperationBinding,
) -> Result<String, String> {
    let filename = binding
        .document_ref
        .rsplit('/')
        .next()
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| {
            format!(
                "GraphQL binding '{}' has an empty document_ref",
                binding.operation_name
            )
        })?;
    if filename == "."
        || filename == ".."
        || filename.contains('\\')
        || !filename.ends_with(".graphql")
    {
        return Err(format!(
            "GraphQL binding '{}' has invalid document_ref '{}'",
            binding.operation_name, binding.document_ref
        ));
    }
    Ok(filename.to_string())
}

pub(crate) fn render_operation_document(
    binding: &GraphqlOperationBinding,
) -> Result<String, String> {
    let operation_kind = operation_kind_keyword(binding.graphql_operation_kind)?;
    let root_field = binding.response_path.first().ok_or_else(|| {
        format!(
            "GraphQL binding '{}' has no response_path root field",
            binding.operation_name
        )
    })?;
    let variable_definitions = binding
        .variable_bindings
        .iter()
        .filter_map(variable_definition)
        .collect::<Vec<_>>();
    let variable_definitions = if variable_definitions.is_empty() {
        String::new()
    } else {
        format!("({})", variable_definitions.join(", "))
    };
    let field_arguments = binding
        .variable_bindings
        .iter()
        .map(|binding| format!("{}: ${}", binding.variable_name, binding.variable_name))
        .collect::<Vec<_>>();
    let field_arguments = if field_arguments.is_empty() {
        String::new()
    } else {
        format!("({})", field_arguments.join(", "))
    };
    let selection_set = binding
        .selection_set
        .as_deref()
        .filter(|selection_set| !selection_set.trim().is_empty())
        .map(|selection_set| format!(" {{ {selection_set} }}"))
        .unwrap_or_default();
    Ok(format!(
        "{operation_kind} {}{} {{ {}{}{} }}",
        binding.operation_name, variable_definitions, root_field, field_arguments, selection_set
    ))
}

fn operation_kind_keyword(kind: GraphqlOperationKind) -> Result<&'static str, String> {
    match kind {
        GraphqlOperationKind::Subscription => {
            Err("GraphQL subscriptions are not invokable in this runtime".to_string())
        }
        kind => Ok(kind.as_keyword()),
    }
}

fn variable_definition(binding: &GraphqlVariableBinding) -> Option<String> {
    Some(format!(
        "${}: {}",
        binding.variable_name,
        binding.graphql_type.as_deref()?
    ))
}
