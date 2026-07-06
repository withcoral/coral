use std::collections::BTreeSet;

use graphql_parser::query::{Directive, FragmentDefinition};

use super::{GraphqlCompileContext, compile_boolean, unsupported};
use crate::CoreError;

pub(super) fn selection_is_included(
    directives: &[Directive<'_, String>],
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<bool, CoreError> {
    let path = path.into();
    let mut included = true;
    let mut seen_directives = BTreeSet::new();
    for (index, directive) in directives.iter().enumerate() {
        let directive_path = format!("{path}[{index}]");
        if !seen_directives.insert(directive.name.clone()) {
            return Err(unsupported(
                format!("{directive_path}.name"),
                format!("GraphQL directive '@{}' is repeated", directive.name),
            ));
        }
        match directive.name.as_str() {
            "include" => {
                if !compile_directive_if_argument(directive, &directive_path, context)? {
                    included = false;
                }
            }
            "skip" => {
                if compile_directive_if_argument(directive, &directive_path, context)? {
                    included = false;
                }
            }
            _ => {
                return Err(unsupported(
                    format!("{directive_path}.name"),
                    format!("unsupported GraphQL directive '@{}'", directive.name),
                ));
            }
        }
    }
    Ok(included)
}

pub(super) fn validate_graphql_directive_syntax(
    directives: &[Directive<'_, String>],
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    let mut seen_directives = BTreeSet::new();
    for (index, directive) in directives.iter().enumerate() {
        let directive_path = format!("{path}[{index}]");
        if !seen_directives.insert(directive.name.clone()) {
            return Err(unsupported(
                format!("{directive_path}.name"),
                format!("GraphQL directive '@{}' is repeated", directive.name),
            ));
        }
        match directive.name.as_str() {
            "include" | "skip" => {
                validate_directive_if_argument_syntax(directive, &directive_path)?;
            }
            _ => {
                return Err(unsupported(
                    format!("{directive_path}.name"),
                    format!("unsupported GraphQL directive '@{}'", directive.name),
                ));
            }
        }
    }
    Ok(())
}

fn validate_directive_if_argument_syntax(
    directive: &Directive<'_, String>,
    path: &str,
) -> Result<(), CoreError> {
    let [(name, _)] = directive.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!(
                "GraphQL @{} directive requires exactly one 'if' argument",
                directive.name
            ),
        ));
    };
    if name != "if" {
        return Err(unsupported(
            format!("{path}.arguments[0]"),
            format!(
                "GraphQL @{} directive requires an 'if' argument",
                directive.name
            ),
        ));
    }
    Ok(())
}

pub(super) fn fragment_definition_is_included(
    fragment: &FragmentDefinition<'_, String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<bool, CoreError> {
    selection_is_included(
        &fragment.directives,
        format!("fragment.{}.directives", fragment.name),
        context,
    )
}

fn compile_directive_if_argument(
    directive: &Directive<'_, String>,
    path: &str,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<bool, CoreError> {
    let [(name, value)] = directive.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!(
                "GraphQL @{} directive requires exactly one 'if' argument",
                directive.name
            ),
        ));
    };
    if name != "if" {
        return Err(unsupported(
            format!("{path}.arguments[0]"),
            format!(
                "GraphQL @{} directive requires an 'if' argument",
                directive.name
            ),
        ));
    }
    compile_boolean(
        value,
        format!("{path}.arguments.if"),
        &format!("@{} if argument", directive.name),
        context,
    )
}
