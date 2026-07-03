//! CST recovery and pre-parse normalization for the openCypher frontend: uses
//! `decypher`'s lossless CST to recover source detail the high-level AST drops —
//! variable-only function arguments (`count(n)`, `id(n)`, `type(r)`), list and
//! pattern-comprehension/reduce/filter sources, inline property values, and
//! `UNWIND` expression text — and runs the pre-parse text-rewriting passes
//! (compact `COUNT` subqueries, static `range`, string-predicate functions,
//! `ORDER BY` null placement) plus expression span helpers. Stateless
//! `pub(super)` helpers split out of `cypher.rs`.

use std::borrow::Cow;
use std::collections::BTreeMap;

use decypher::ast::clause::SortItem;
use decypher::ast::expr::{Expression, Literal as CypherLiteral};
use decypher::ast::query::Query;
use decypher::ast::visit;
use decypher::cst::AstNode as _;
use decypher::syntax::{SyntaxKind, SyntaxNode};

use crate::CoreError;

use super::{
    CollectionFilterCall, FunctionArgumentSources, INTERNAL_STATIC_RANGE_FUNCTION,
    INTERNAL_STRING_CONTAINS_FUNCTION, INTERNAL_STRING_ENDS_WITH_FUNCTION,
    INTERNAL_STRING_STARTS_WITH_FUNCTION, InlinePropertyValueSource, ListComprehensionSource,
    NullOrder, OrderNullPlacementNormalization, PatternComprehensionSource, StaticListFunctionKind,
    StaticListFunctionSource, StaticReduceSource, VariableFunctionArgument, unsupported,
};

pub(super) fn collect_variable_function_arguments(
    cypher: &str,
) -> BTreeMap<(usize, usize), VariableFunctionArgument> {
    // decypher's high-level AST currently drops variable-only function
    // arguments such as count(n), id(n), and type(r); the lossless CST keeps
    // them by span.
    let parse = decypher::parse_cst(cypher);
    let tree = parse.tree();
    tree.syntax()
        .descendants()
        .filter(|node| node.kind() == SyntaxKind::FUNCTION_INVOCATION)
        .filter_map(|node| variable_function_argument_from_cst(&node))
        .collect()
}

pub(super) fn collect_function_argument_sources(
    cypher: &str,
) -> BTreeMap<(usize, usize), FunctionArgumentSources> {
    // decypher's high-level AST can omit variable-only function arguments.
    // Keep the lossless argument text available for static expression folders
    // that need to recover repeated item-variable arguments such as atan2(x, x).
    let parse = decypher::parse_cst(cypher);
    let tree = parse.tree();
    tree.syntax()
        .descendants()
        .filter(|node| node.kind() == SyntaxKind::FUNCTION_INVOCATION)
        .filter_map(|node| function_argument_sources_from_cst(&node))
        .collect()
}

pub(super) fn collect_collection_filter_calls(
    cypher: &str,
) -> BTreeMap<(usize, usize), CollectionFilterCall> {
    // decypher's high-level AST currently lowers all/any/none/single(...)
    // filter expressions as normal function calls and drops the collection
    // expression. Recover the filter header from the lossless CST by span.
    let parse = decypher::parse_cst(cypher);
    let tree = parse.tree();
    tree.syntax()
        .descendants()
        .filter(|node| node.kind() == SyntaxKind::FUNCTION_INVOCATION)
        .filter_map(|node| collection_filter_call_from_cst(cypher, &node))
        .collect()
}

pub(super) fn collect_list_comprehension_sources(
    cypher: &str,
) -> BTreeMap<(usize, usize), ListComprehensionSource> {
    // decypher's high-level AST currently drops the source collection from
    // list comprehensions. Recover the `variable IN collection` header from
    // the lossless CST by span, then keep lowering tied to the typed AST for
    // the optional filter and identity map expression.
    let parse = decypher::parse_cst(cypher);
    let tree = parse.tree();
    tree.syntax()
        .descendants()
        .filter(|node| node.kind() == SyntaxKind::LIST_COMPREHENSION)
        .filter_map(|node| list_comprehension_source_from_cst(cypher, &node))
        .collect()
}

pub(super) fn collect_pattern_comprehension_sources(
    cypher: &str,
) -> BTreeMap<(usize, usize), PatternComprehensionSource> {
    // decypher currently recognizes pattern comprehensions but does not expose
    // the relationship pattern in the typed AST. Recover the lossless source
    // and lower it through the same scoped MATCH path as COLLECT subqueries.
    let parse = decypher::parse_cst(cypher);
    let tree = parse.tree();
    tree.syntax()
        .descendants()
        .filter(|node| node.kind() == SyntaxKind::PATTERN_COMPREHENSION)
        .filter_map(|node| pattern_comprehension_source_from_cst(cypher, &node))
        .collect()
}

pub(super) fn collect_static_reduce_sources(
    cypher: &str,
) -> BTreeMap<(usize, usize), StaticReduceSource> {
    // decypher's high-level AST does not expose reduce(acc = init, x IN list | expr)
    // as three regular function arguments. Recover the header and reducer
    // expression from the lossless CST, then parse each expression fragment
    // through the normal expression compiler.
    let parse = decypher::parse_cst(cypher);
    let tree = parse.tree();
    tree.syntax()
        .descendants()
        .filter(|node| node.kind() == SyntaxKind::FUNCTION_INVOCATION)
        .filter_map(|node| static_reduce_source_from_cst(cypher, &node))
        .collect()
}

pub(super) fn collect_static_list_function_sources(
    cypher: &str,
) -> BTreeMap<(usize, usize), StaticListFunctionSource> {
    // Legacy Cypher list functions filter(...) and extract(...) have
    // comprehension-like headers that are not represented as ordinary function
    // arguments in the typed AST. Recover their source parts from the lossless
    // CST and compile them through the same static list-comprehension folder.
    let parse = decypher::parse_cst(cypher);
    let tree = parse.tree();
    tree.syntax()
        .descendants()
        .filter(|node| node.kind() == SyntaxKind::FUNCTION_INVOCATION)
        .filter_map(|node| static_list_function_source_from_cst(cypher, &node))
        .collect()
}

pub(super) fn collect_unwind_expression_sources(cypher: &str) -> BTreeMap<(usize, usize), String> {
    // decypher's high-level AST can under-represent UNWIND expressions such as
    // `UNWIND ['a'] + $extra AS value`. Recover the full source expression from
    // the lossless CST and parse it through Coral's expression-fragment path.
    let parse = decypher::parse_cst(cypher);
    let tree = parse.tree();
    tree.syntax()
        .descendants()
        .filter(|node| node.kind() == SyntaxKind::UNWIND_CLAUSE)
        .filter_map(|node| unwind_expression_source_from_cst(cypher, &node))
        .collect()
}

pub(super) fn collect_inline_property_value_sources(
    cypher: &str,
) -> BTreeMap<usize, InlinePropertyValueSource> {
    // decypher's high-level AST can under-represent map-entry values inside
    // pattern property maps, for example `source.team` may surface as only the
    // variable `source`. Recover the full value text from the lossless CST and
    // reparse it only when the typed AST span was truncated.
    let parse = decypher::parse_cst(cypher);
    let tree = parse.tree();
    tree.syntax()
        .descendants()
        .filter(|node| node.kind() == SyntaxKind::MAP_ENTRY)
        .filter_map(|node| inline_property_value_source_from_cst(cypher, &node))
        .collect()
}

fn inline_property_value_source_from_cst(
    cypher: &str,
    node: &SyntaxNode,
) -> Option<(usize, InlinePropertyValueSource)> {
    let range = node.text_range();
    let start: usize = range.start().into();
    let end: usize = range.end().into();
    let source = cypher.get(start..end)?;
    let colon = find_top_level_character(source, ':')?;
    let value_start = skip_ascii_whitespace(source, colon + 1);
    let value_end = trim_ascii_whitespace_end(source, source.len());
    if value_start >= value_end {
        return None;
    }
    let value = source.get(value_start..value_end)?;
    let absolute_value_start = start + value_start;
    let absolute_value_end = start + value_end;
    Some((
        absolute_value_start,
        InlinePropertyValueSource {
            source: value.to_string(),
            end: absolute_value_end,
        },
    ))
}

fn unwind_expression_source_from_cst(
    cypher: &str,
    node: &SyntaxNode,
) -> Option<((usize, usize), String)> {
    let range = node.text_range();
    let start: usize = range.start().into();
    let end: usize = range.end().into();
    let source = cypher.get(start..end)?;
    let expression = parse_unwind_expression_source(source)?;
    Some(((start, end), expression.to_string()))
}

fn parse_unwind_expression_source(source: &str) -> Option<&str> {
    const UNWIND_KEYWORD: &str = "UNWIND";
    let source = source.trim();
    if !source
        .get(..UNWIND_KEYWORD.len())
        .is_some_and(|value| value.eq_ignore_ascii_case(UNWIND_KEYWORD))
        || !keyword_has_boundaries(source, 0, UNWIND_KEYWORD.len())
    {
        return None;
    }
    let after_unwind = source.get(UNWIND_KEYWORD.len()..)?.trim();
    let as_index = find_top_level_keyword(after_unwind, "AS")?;
    let expression = after_unwind.get(..as_index)?.trim();
    (!expression.is_empty()).then_some(expression)
}

fn list_comprehension_source_from_cst(
    cypher: &str,
    node: &SyntaxNode,
) -> Option<((usize, usize), ListComprehensionSource)> {
    let range = node.text_range();
    let start: usize = range.start().into();
    let end: usize = range.end().into();
    let source = cypher.get(start..end)?;
    let value = parse_list_comprehension_source(source)?;
    Some(((start, end), value))
}

fn parse_list_comprehension_source(source: &str) -> Option<ListComprehensionSource> {
    let inner = source.strip_prefix('[')?.strip_suffix(']')?.trim();
    let in_index = find_top_level_keyword(inner, "IN")?;
    let variable = parse_collection_filter_variable(inner.get(..in_index)?.trim())?;
    let after_in = inner.get(in_index + "IN".len()..)?.trim();
    let where_index = find_top_level_keyword(after_in, "WHERE");
    let map_index = find_top_level_character(after_in, '|');
    let end_index = [where_index, map_index]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(after_in.len());
    let collection_source = after_in.get(..end_index)?.trim();
    if collection_source.is_empty() {
        return None;
    }
    let filter_source = where_index.and_then(|where_index| {
        let filter_end = map_index.unwrap_or(after_in.len());
        if where_index >= filter_end {
            return None;
        }
        after_in
            .get(where_index + "WHERE".len()..filter_end)
            .map(str::trim)
            .filter(|source| !source.is_empty())
            .map(str::to_string)
    });
    Some(ListComprehensionSource {
        variable,
        collection_source: collection_source.to_string(),
        filter_source,
        has_map: map_index.is_some(),
    })
}

fn pattern_comprehension_source_from_cst(
    cypher: &str,
    node: &SyntaxNode,
) -> Option<((usize, usize), PatternComprehensionSource)> {
    let range = node.text_range();
    let start: usize = range.start().into();
    let end: usize = range.end().into();
    let source = cypher.get(start..end)?;
    let value = parse_pattern_comprehension_source(source)?;
    Some(((start, end), value))
}

fn parse_pattern_comprehension_source(source: &str) -> Option<PatternComprehensionSource> {
    let inner = source.strip_prefix('[')?.strip_suffix(']')?.trim();
    let map_index = find_top_level_character(inner, '|')?;
    let left = inner.get(..map_index)?.trim();
    let map_source = inner.get(map_index + '|'.len_utf8()..)?.trim();
    if left.is_empty() || map_source.is_empty() {
        return None;
    }
    let where_index = find_top_level_keyword(left, "WHERE");
    let (pattern_source, where_source) = match where_index {
        Some(where_index) => (
            left.get(..where_index)?.trim(),
            left.get(where_index + "WHERE".len()..)?.trim(),
        ),
        None => (left, ""),
    };
    if pattern_source.is_empty() || where_index.is_some() && where_source.is_empty() {
        return None;
    }
    let collect_query_source = if where_source.is_empty() {
        format!("MATCH {pattern_source} RETURN {map_source}")
    } else {
        format!("MATCH {pattern_source} WHERE {where_source} RETURN {map_source}")
    };
    let count_query_source = if where_source.is_empty() {
        format!("MATCH {pattern_source} RETURN 1")
    } else {
        format!("MATCH {pattern_source} WHERE {where_source} RETURN 1")
    };
    Some(PatternComprehensionSource {
        collect_query_source,
        count_query_source,
    })
}

pub(super) fn collect_compact_exists_pattern_queries(
    cypher: &str,
) -> BTreeMap<(usize, usize), String> {
    // decypher's high-level AST treats `WHERE` inside `EXISTS { pattern WHERE ... }`
    // as a clause child, which makes the builder classify the expression as a
    // regular subquery and lose the compact pattern. Recover that compact form
    // from the lossless CST and lower it through the same scoped MATCH path as
    // `EXISTS { MATCH pattern WHERE ... }`.
    let parse = decypher::parse_cst(cypher);
    let tree = parse.tree();
    tree.syntax()
        .descendants()
        .filter(|node| node.kind() == SyntaxKind::EXISTS_SUBQUERY)
        .filter_map(|node| compact_exists_pattern_query_from_cst(cypher, &node))
        .collect()
}

pub(super) fn normalize_order_null_placements(cypher: &str) -> OrderNullPlacementNormalization<'_> {
    let (placements, removals) = collect_order_null_placement_sites(cypher);
    if removals.is_empty() {
        return OrderNullPlacementNormalization {
            cypher: Cow::Borrowed(cypher),
            placements,
        };
    }

    let mut normalized = String::with_capacity(cypher.len());
    let mut cursor = 0usize;
    for (start, end) in removals {
        if let Some(prefix) = cypher.get(cursor..start) {
            normalized.push_str(prefix);
        }
        cursor = end;
    }
    if let Some(suffix) = cypher.get(cursor..) {
        normalized.push_str(suffix);
    }

    OrderNullPlacementNormalization {
        cypher: Cow::Owned(normalized),
        placements,
    }
}

fn collect_order_null_placement_sites(
    cypher: &str,
) -> (Vec<Option<NullOrder>>, Vec<(usize, usize)>) {
    const ORDER_KEYWORD: &str = "ORDER";
    const BY_KEYWORD: &str = "BY";

    let mut placements = Vec::new();
    let mut removals = Vec::new();
    let mut index = 0usize;
    let mut depth = 0usize;
    let mut in_string = false;
    let mut in_escaped_identifier = false;

    while index < cypher.len() {
        let Some(rest) = cypher.get(index..) else {
            break;
        };
        let Some(character) = rest.chars().next() else {
            break;
        };
        let character_len = character.len_utf8();

        if in_string {
            if character == '\'' {
                let next_index = index + character_len;
                if cypher
                    .get(next_index..)
                    .is_some_and(|value| value.starts_with('\''))
                {
                    index = next_index + '\''.len_utf8();
                    continue;
                }
                in_string = false;
            }
            index += character_len;
            continue;
        }

        if in_escaped_identifier {
            if character == '`' {
                let next_index = index + character_len;
                if cypher
                    .get(next_index..)
                    .is_some_and(|value| value.starts_with('`'))
                {
                    index = next_index + '`'.len_utf8();
                    continue;
                }
                in_escaped_identifier = false;
            }
            index += character_len;
            continue;
        }

        if keyword_at(cypher, index, ORDER_KEYWORD)
            && previous_non_whitespace(cypher, index) != Some('.')
        {
            let after_order = skip_ascii_whitespace(cypher, index + ORDER_KEYWORD.len());
            if keyword_at(cypher, after_order, BY_KEYWORD) {
                let after_by = skip_ascii_whitespace(cypher, after_order + BY_KEYWORD.len());
                index = collect_order_items_until_clause_end(
                    cypher,
                    after_by,
                    depth,
                    &mut placements,
                    &mut removals,
                );
                continue;
            }
        }

        match character {
            '\'' => in_string = true,
            '`' => in_escaped_identifier = true,
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }

        index += character_len;
    }

    (placements, removals)
}

fn collect_order_items_until_clause_end(
    cypher: &str,
    start: usize,
    baseline_depth: usize,
    placements: &mut Vec<Option<NullOrder>>,
    removals: &mut Vec<(usize, usize)>,
) -> usize {
    let mut item_start = skip_ascii_whitespace(cypher, start);
    let mut index = item_start;
    let mut depth = baseline_depth;
    let mut in_string = false;
    let mut in_escaped_identifier = false;

    while index < cypher.len() {
        let Some(rest) = cypher.get(index..) else {
            break;
        };
        let Some(character) = rest.chars().next() else {
            break;
        };
        let character_len = character.len_utf8();

        if in_string {
            if character == '\'' {
                let next_index = index + character_len;
                if cypher
                    .get(next_index..)
                    .is_some_and(|value| value.starts_with('\''))
                {
                    index = next_index + '\''.len_utf8();
                    continue;
                }
                in_string = false;
            }
            index += character_len;
            continue;
        }

        if in_escaped_identifier {
            if character == '`' {
                let next_index = index + character_len;
                if cypher
                    .get(next_index..)
                    .is_some_and(|value| value.starts_with('`'))
                {
                    index = next_index + '`'.len_utf8();
                    continue;
                }
                in_escaped_identifier = false;
            }
            index += character_len;
            continue;
        }

        if depth == baseline_depth {
            if matches!(character, ')' | ']' | '}') {
                push_order_null_placement_item(cypher, item_start, index, placements, removals);
                return index;
            }
            if character == ',' {
                push_order_null_placement_item(cypher, item_start, index, placements, removals);
                item_start = skip_ascii_whitespace(cypher, index + character_len);
                index = item_start;
                continue;
            }
            if order_clause_end_keyword_at(cypher, index) {
                push_order_null_placement_item(cypher, item_start, index, placements, removals);
                return index;
            }
        }

        match character {
            '\'' => in_string = true,
            '`' => in_escaped_identifier = true,
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }

        index += character_len;
    }

    push_order_null_placement_item(cypher, item_start, cypher.len(), placements, removals);
    cypher.len()
}

fn push_order_null_placement_item(
    cypher: &str,
    start: usize,
    end: usize,
    placements: &mut Vec<Option<NullOrder>>,
    removals: &mut Vec<(usize, usize)>,
) {
    let start = skip_ascii_whitespace(cypher, start);
    let end = trim_ascii_whitespace_end(cypher, end);
    if start >= end {
        return;
    }
    if let Some((remove_start, remove_end, nulls)) =
        find_order_item_null_placement(cypher, start, end)
    {
        placements.push(Some(nulls));
        removals.push((remove_start, remove_end));
    } else {
        placements.push(None);
    }
}

fn find_order_item_null_placement(
    cypher: &str,
    start: usize,
    end: usize,
) -> Option<(usize, usize, NullOrder)> {
    const NULLS_KEYWORD: &str = "NULLS";

    let mut index = start;
    let mut depth = 0usize;
    let mut in_string = false;
    let mut in_escaped_identifier = false;

    while index < end {
        let rest = cypher.get(index..end)?;
        let character = rest.chars().next()?;
        let character_len = character.len_utf8();

        if in_string {
            if character == '\'' {
                let next_index = index + character_len;
                if cypher
                    .get(next_index..end)
                    .is_some_and(|value| value.starts_with('\''))
                {
                    index = next_index + '\''.len_utf8();
                    continue;
                }
                in_string = false;
            }
            index += character_len;
            continue;
        }

        if in_escaped_identifier {
            if character == '`' {
                let next_index = index + character_len;
                if cypher
                    .get(next_index..end)
                    .is_some_and(|value| value.starts_with('`'))
                {
                    index = next_index + '`'.len_utf8();
                    continue;
                }
                in_escaped_identifier = false;
            }
            index += character_len;
            continue;
        }

        match character {
            '\'' => {
                in_string = true;
                index += character_len;
                continue;
            }
            '`' => {
                in_escaped_identifier = true;
                index += character_len;
                continue;
            }
            '(' | '[' | '{' => {
                depth += 1;
                index += character_len;
                continue;
            }
            ')' | ']' | '}' => {
                depth = depth.saturating_sub(1);
                index += character_len;
                continue;
            }
            _ => {}
        }

        if depth == 0
            && keyword_at(cypher, index, NULLS_KEYWORD)
            && previous_non_whitespace(cypher, index) != Some('.')
        {
            let after_nulls = skip_ascii_whitespace(cypher, index + NULLS_KEYWORD.len());
            if let Some((placement_end, nulls)) = parse_null_placement_keyword(cypher, after_nulls)
                && cypher
                    .get(placement_end..end)
                    .is_some_and(|tail| tail.trim().is_empty())
            {
                let remove_start = include_preceding_ascii_whitespace(cypher, start, index);
                return Some((remove_start, placement_end, nulls));
            }
        }

        index += character_len;
    }

    None
}

fn parse_null_placement_keyword(source: &str, index: usize) -> Option<(usize, NullOrder)> {
    const FIRST_KEYWORD: &str = "FIRST";
    const LAST_KEYWORD: &str = "LAST";

    if keyword_at(source, index, FIRST_KEYWORD) {
        Some((index + FIRST_KEYWORD.len(), NullOrder::First))
    } else if keyword_at(source, index, LAST_KEYWORD) {
        Some((index + LAST_KEYWORD.len(), NullOrder::Last))
    } else {
        None
    }
}

pub(super) fn collect_order_null_placements_for_query(
    query: &Query,
    placements: &[Option<NullOrder>],
) -> Result<BTreeMap<(usize, usize), NullOrder>, CoreError> {
    #[derive(Default)]
    struct SortItemCollector<'ast> {
        items: Vec<&'ast SortItem>,
    }

    impl<'ast> visit::Visit<'ast> for SortItemCollector<'ast> {
        fn visit_sort_item(&mut self, node: &'ast SortItem) {
            self.items.push(node);
            visit::walk_sort_item(self, node);
        }
    }

    let mut collector = SortItemCollector::default();
    visit::Visit::visit_query(&mut collector, query);
    if collector.items.len() != placements.len() {
        if placements.iter().any(Option::is_some) {
            return Err(CoreError::internal(format!(
                "recovered {} Cypher ORDER BY null placements for {} parsed sort items",
                placements.len(),
                collector.items.len()
            )));
        }
        return Ok(BTreeMap::new());
    }

    let mut by_expression_span = BTreeMap::new();
    for (item, nulls) in collector.items.into_iter().zip(placements.iter().copied()) {
        let Some(nulls) = nulls else {
            continue;
        };
        let Some(span) = expression_span(&item.expression) else {
            return Err(unsupported(
                "order.nulls",
                "NULLS FIRST/LAST currently requires a sort expression with source span",
            ));
        };
        by_expression_span.insert((span.start, span.end), nulls);
    }
    Ok(by_expression_span)
}

pub(super) fn expression_span(expression: &Expression) -> Option<decypher::error::Span> {
    match expression {
        Expression::Literal(literal) => literal_span(literal),
        Expression::Variable(variable) => Some(variable.name.span),
        Expression::Parameter(parameter) => Some(parameter.span),
        Expression::PropertyLookup { span, .. }
        | Expression::NodeLabels { span, .. }
        | Expression::BinaryOp { span, .. }
        | Expression::UnaryOp { span, .. }
        | Expression::Comparison { span, .. }
        | Expression::ListIndex { span, .. }
        | Expression::ListSlice { span, .. }
        | Expression::In { span, .. }
        | Expression::IsNull { span, .. }
        | Expression::CountStar { span } => Some(*span),
        Expression::FunctionCall(function) => Some(function.span),
        Expression::Case(case_expression) => Some(case_expression.span),
        Expression::ListComprehension(comprehension) => Some(comprehension.span),
        Expression::PatternComprehension(comprehension) => Some(comprehension.span),
        Expression::All(filter)
        | Expression::Any(filter)
        | Expression::None(filter)
        | Expression::Single(filter) => Some(filter.span),
        Expression::Parenthesized(inner) => expression_span(inner),
        Expression::Pattern(pattern) => Some(pattern.span),
        Expression::Exists(exists) => Some(exists.span),
        Expression::CountSubquery(count) => Some(count.span),
        Expression::CollectSubquery(collect) => Some(collect.span),
        Expression::MapProjection(projection) => Some(projection.span),
    }
}

fn literal_span(literal: &CypherLiteral) -> Option<decypher::error::Span> {
    match literal {
        CypherLiteral::String(string) => Some(string.span),
        CypherLiteral::List(list) => Some(list.span),
        CypherLiteral::Map(map) => Some(map.span),
        CypherLiteral::Number(_) | CypherLiteral::Boolean(_) | CypherLiteral::Null => None,
    }
}

pub(super) fn normalize_compact_count_subqueries(cypher: &str) -> Cow<'_, str> {
    const COUNT_KEYWORD: &str = "COUNT";

    let mut rewrites = Vec::new();
    let mut index = 0usize;
    let mut in_string = false;
    let mut in_escaped_identifier = false;

    while index < cypher.len() {
        let Some(rest) = cypher.get(index..) else {
            break;
        };
        let Some(character) = rest.chars().next() else {
            break;
        };
        let character_len = character.len_utf8();

        if in_string {
            if character == '\'' {
                let next_index = index + character_len;
                if cypher
                    .get(next_index..)
                    .is_some_and(|value| value.starts_with('\''))
                {
                    index = next_index + '\''.len_utf8();
                    continue;
                }
                in_string = false;
            }
            index += character_len;
            continue;
        }

        if in_escaped_identifier {
            if character == '`' {
                let next_index = index + character_len;
                if cypher
                    .get(next_index..)
                    .is_some_and(|value| value.starts_with('`'))
                {
                    index = next_index + '`'.len_utf8();
                    continue;
                }
                in_escaped_identifier = false;
            }
            index += character_len;
            continue;
        }

        match character {
            '\'' => {
                in_string = true;
                index += character_len;
                continue;
            }
            '`' => {
                in_escaped_identifier = true;
                index += character_len;
                continue;
            }
            _ => {}
        }

        if rest
            .get(..COUNT_KEYWORD.len())
            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(COUNT_KEYWORD))
            && keyword_has_boundaries(cypher, index, COUNT_KEYWORD.len())
        {
            let after_keyword = skip_ascii_whitespace(cypher, index + COUNT_KEYWORD.len());
            if cypher
                .get(after_keyword..)
                .is_some_and(|value| value.starts_with('{'))
                && let Some(close) = find_matching_brace(cypher, after_keyword)
            {
                let body_start = after_keyword + '{'.len_utf8();
                if let Some(body) = cypher.get(body_start..close)
                    && compact_count_body_should_normalize(body)
                {
                    rewrites.push((body_start, close, format!(" MATCH {} FINISH ", body.trim())));
                }
                index = close + '}'.len_utf8();
                continue;
            }
        }

        index += character_len;
    }

    if rewrites.is_empty() {
        return Cow::Borrowed(cypher);
    }

    let mut normalized = String::with_capacity(cypher.len() + rewrites.len() * 13);
    let mut cursor = 0usize;
    for (start, end, replacement) in rewrites {
        if let Some(prefix) = cypher.get(cursor..start) {
            normalized.push_str(prefix);
        }
        normalized.push_str(&replacement);
        cursor = end;
    }
    if let Some(suffix) = cypher.get(cursor..) {
        normalized.push_str(suffix);
    }
    Cow::Owned(normalized)
}

pub(super) fn normalize_static_range_functions(cypher: &str) -> Cow<'_, str> {
    const RANGE_KEYWORD: &str = "range";

    let mut rewrites = Vec::new();
    let mut index = 0usize;
    let mut in_string = false;
    let mut in_escaped_identifier = false;

    while index < cypher.len() {
        let Some(rest) = cypher.get(index..) else {
            break;
        };
        let Some(character) = rest.chars().next() else {
            break;
        };
        let character_len = character.len_utf8();

        if in_string {
            if character == '\'' {
                let next_index = index + character_len;
                if cypher
                    .get(next_index..)
                    .is_some_and(|value| value.starts_with('\''))
                {
                    index = next_index + '\''.len_utf8();
                    continue;
                }
                in_string = false;
            }
            index += character_len;
            continue;
        }

        if in_escaped_identifier {
            if character == '`' {
                let next_index = index + character_len;
                if cypher
                    .get(next_index..)
                    .is_some_and(|value| value.starts_with('`'))
                {
                    index = next_index + '`'.len_utf8();
                    continue;
                }
                in_escaped_identifier = false;
            }
            index += character_len;
            continue;
        }

        match character {
            '\'' => {
                in_string = true;
                index += character_len;
                continue;
            }
            '`' => {
                in_escaped_identifier = true;
                index += character_len;
                continue;
            }
            _ => {}
        }

        if rest
            .get(..RANGE_KEYWORD.len())
            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(RANGE_KEYWORD))
            && keyword_has_boundaries(cypher, index, RANGE_KEYWORD.len())
            && previous_non_whitespace(cypher, index) != Some('.')
        {
            let after_keyword = skip_ascii_whitespace(cypher, index + RANGE_KEYWORD.len());
            if cypher
                .get(after_keyword..)
                .is_some_and(|value| value.starts_with('('))
            {
                rewrites.push((index, index + RANGE_KEYWORD.len()));
                index = after_keyword + '('.len_utf8();
                continue;
            }
        }

        index += character_len;
    }

    if rewrites.is_empty() {
        return Cow::Borrowed(cypher);
    }

    let mut normalized =
        String::with_capacity(cypher.len() + rewrites.len() * INTERNAL_STATIC_RANGE_FUNCTION.len());
    let mut cursor = 0usize;
    for (start, end) in rewrites {
        if let Some(prefix) = cypher.get(cursor..start) {
            normalized.push_str(prefix);
        }
        normalized.push_str(INTERNAL_STATIC_RANGE_FUNCTION);
        cursor = end;
    }
    if let Some(suffix) = cypher.get(cursor..) {
        normalized.push_str(suffix);
    }
    Cow::Owned(normalized)
}

pub(super) fn normalize_string_predicate_functions(cypher: &str) -> Cow<'_, str> {
    const FUNCTIONS: [(&str, &str); 3] = [
        ("contains", INTERNAL_STRING_CONTAINS_FUNCTION),
        ("startsWith", INTERNAL_STRING_STARTS_WITH_FUNCTION),
        ("endsWith", INTERNAL_STRING_ENDS_WITH_FUNCTION),
    ];

    let mut rewrites = Vec::new();
    let mut index = 0usize;
    let mut in_string = false;
    let mut in_escaped_identifier = false;

    while index < cypher.len() {
        let Some(rest) = cypher.get(index..) else {
            break;
        };
        let Some(character) = rest.chars().next() else {
            break;
        };
        let character_len = character.len_utf8();

        if in_string {
            if character == '\'' {
                let next_index = index + character_len;
                if cypher
                    .get(next_index..)
                    .is_some_and(|value| value.starts_with('\''))
                {
                    index = next_index + '\''.len_utf8();
                    continue;
                }
                in_string = false;
            }
            index += character_len;
            continue;
        }

        if in_escaped_identifier {
            if character == '`' {
                let next_index = index + character_len;
                if cypher
                    .get(next_index..)
                    .is_some_and(|value| value.starts_with('`'))
                {
                    index = next_index + '`'.len_utf8();
                    continue;
                }
                in_escaped_identifier = false;
            }
            index += character_len;
            continue;
        }

        match character {
            '\'' => {
                in_string = true;
                index += character_len;
                continue;
            }
            '`' => {
                in_escaped_identifier = true;
                index += character_len;
                continue;
            }
            _ => {}
        }

        let Some((keyword, replacement)) = FUNCTIONS.iter().find(|(keyword, _)| {
            rest.get(..keyword.len())
                .is_some_and(|candidate| candidate.eq_ignore_ascii_case(keyword))
                && keyword_has_boundaries(cypher, index, keyword.len())
                && previous_non_whitespace(cypher, index) != Some('.')
        }) else {
            index += character_len;
            continue;
        };

        let after_keyword = skip_ascii_whitespace(cypher, index + keyword.len());
        if cypher
            .get(after_keyword..)
            .is_some_and(|value| value.starts_with('('))
        {
            rewrites.push((index, index + keyword.len(), *replacement));
            index = after_keyword + '('.len_utf8();
            continue;
        }

        index += character_len;
    }

    if rewrites.is_empty() {
        return Cow::Borrowed(cypher);
    }

    let additional_capacity: usize = rewrites
        .iter()
        .map(|(start, end, replacement)| replacement.len().saturating_sub(end - start))
        .sum();
    let mut normalized = String::with_capacity(cypher.len() + additional_capacity);
    let mut cursor = 0usize;
    for (start, end, replacement) in rewrites {
        if let Some(prefix) = cypher.get(cursor..start) {
            normalized.push_str(prefix);
        }
        normalized.push_str(replacement);
        cursor = end;
    }
    if let Some(suffix) = cypher.get(cursor..) {
        normalized.push_str(suffix);
    }
    Cow::Owned(normalized)
}

fn compact_count_body_should_normalize(body: &str) -> bool {
    let trimmed = body.trim_start();
    if trimmed.starts_with('(') {
        return true;
    }
    let Some(equals_index) = find_top_level_character(trimmed, '=') else {
        return false;
    };
    let Some(path_variable) = trimmed.get(..equals_index).map(str::trim) else {
        return false;
    };
    if parse_collection_filter_variable(path_variable).is_none() {
        return false;
    }
    trimmed
        .get(equals_index + '='.len_utf8()..)
        .is_some_and(|pattern| pattern.trim_start().starts_with('('))
}

fn skip_ascii_whitespace(source: &str, mut index: usize) -> usize {
    while let Some(rest) = source.get(index..) {
        let Some(character) = rest.chars().next() else {
            return index;
        };
        if !character.is_ascii_whitespace() {
            return index;
        }
        index += character.len_utf8();
    }
    index
}

fn previous_non_whitespace(source: &str, index: usize) -> Option<char> {
    source
        .get(..index)?
        .chars()
        .rev()
        .find(|character| !character.is_whitespace())
}

fn find_matching_brace(source: &str, open: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut in_escaped_identifier = false;
    let mut index = open;

    while index < source.len() {
        let rest = source.get(index..)?;
        let character = rest.chars().next()?;
        let character_len = character.len_utf8();

        if in_string {
            if character == '\'' {
                let next_index = index + character_len;
                if source.get(next_index..)?.starts_with('\'') {
                    index = next_index + '\''.len_utf8();
                    continue;
                }
                in_string = false;
            }
            index += character_len;
            continue;
        }

        if in_escaped_identifier {
            if character == '`' {
                let next_index = index + character_len;
                if source.get(next_index..)?.starts_with('`') {
                    index = next_index + '`'.len_utf8();
                    continue;
                }
                in_escaped_identifier = false;
            }
            index += character_len;
            continue;
        }

        match character {
            '\'' => in_string = true,
            '`' => in_escaped_identifier = true,
            '{' => depth += 1,
            '}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }

        index += character_len;
    }

    None
}

fn compact_exists_pattern_query_from_cst(
    cypher: &str,
    node: &SyntaxNode,
) -> Option<((usize, usize), String)> {
    let has_pattern = node
        .children()
        .any(|child| child.kind() == SyntaxKind::NODE_PATTERN);
    let has_where = node
        .children()
        .any(|child| child.kind() == SyntaxKind::WHERE_CLAUSE);
    if !has_pattern || !has_where {
        return None;
    }

    let range = node.text_range();
    let start: usize = range.start().into();
    let end: usize = range.end().into();
    let source = cypher.get(start..end)?;
    let inner = compact_exists_inner_source(source)?;
    let where_index = find_top_level_keyword(inner, "WHERE")?;
    let pattern_source = inner.get(..where_index)?.trim();
    let where_source = inner.get(where_index + "WHERE".len()..)?.trim();
    if pattern_source.is_empty() || where_source.is_empty() {
        return None;
    }
    Some((
        (start, end),
        format!("MATCH {pattern_source} WHERE {where_source} FINISH"),
    ))
}

fn compact_exists_inner_source(source: &str) -> Option<&str> {
    if let Some(open) = source.find('{') {
        let close = source.rfind('}')?;
        if close <= open {
            return None;
        }
        return source.get(open + 1..close).map(str::trim);
    }

    let exists_end = "EXISTS".len();
    let after_exists = source.get(exists_end..)?.trim_start();
    if !after_exists.starts_with('(') {
        return None;
    }
    let open = source.len() - after_exists.len();
    let close = source.rfind(')')?;
    if close <= open {
        return None;
    }
    source.get(open + 1..close).map(str::trim)
}

fn collection_filter_call_from_cst(
    cypher: &str,
    node: &SyntaxNode,
) -> Option<((usize, usize), CollectionFilterCall)> {
    let range = node.text_range();
    let start: usize = range.start().into();
    let end: usize = range.end().into();
    let source = cypher.get(start..end)?;
    let call = parse_collection_filter_call_source(source)?;
    Some(((start, end), call))
}

fn parse_collection_filter_call_source(source: &str) -> Option<CollectionFilterCall> {
    let open = source.find('(')?;
    let close = source.rfind(')')?;
    if close <= open {
        return None;
    }
    let function_name = source.get(..open)?.trim();
    if !matches!(
        function_name.to_ascii_lowercase().as_str(),
        "all" | "any" | "none" | "single"
    ) {
        return None;
    }

    let inner = source.get(open + 1..close)?.trim();
    let in_index = find_top_level_keyword(inner, "IN")?;
    let variable = parse_collection_filter_variable(inner.get(..in_index)?.trim())?;
    let after_in = inner.get(in_index + "IN".len()..)?.trim();
    let (collection_source, has_predicate) =
        if let Some(where_index) = find_top_level_keyword(after_in, "WHERE") {
            (after_in.get(..where_index)?.trim(), true)
        } else {
            (after_in, false)
        };
    if collection_source.is_empty() {
        return None;
    }
    Some(CollectionFilterCall {
        variable,
        collection_source: collection_source.to_string(),
        has_predicate,
    })
}

fn static_reduce_source_from_cst(
    cypher: &str,
    node: &SyntaxNode,
) -> Option<((usize, usize), StaticReduceSource)> {
    let range = node.text_range();
    let start: usize = range.start().into();
    let end: usize = range.end().into();
    let source = cypher.get(start..end)?;
    let reduce = parse_static_reduce_source(source)?;
    Some(((start, end), reduce))
}

fn parse_static_reduce_source(source: &str) -> Option<StaticReduceSource> {
    let open = source.find('(')?;
    let close = source.rfind(')')?;
    if close <= open {
        return None;
    }
    let function_name = source.get(..open)?.trim();
    if !function_name.eq_ignore_ascii_case("reduce") {
        return None;
    }

    let inner = source.get(open + 1..close)?.trim();
    let comma = find_top_level_character(inner, ',')?;
    let accumulator_source = inner.get(..comma)?.trim();
    let source_and_expression = inner.get(comma + 1..)?.trim();

    let equals = find_top_level_character(accumulator_source, '=')?;
    let accumulator_variable =
        parse_collection_filter_variable(accumulator_source.get(..equals)?.trim())?;
    let initial_source = accumulator_source.get(equals + 1..)?.trim();
    if initial_source.is_empty() {
        return None;
    }

    let pipe = find_top_level_character(source_and_expression, '|')?;
    let collection_header = source_and_expression.get(..pipe)?.trim();
    let expression_source = source_and_expression.get(pipe + 1..)?.trim();
    if expression_source.is_empty() {
        return None;
    }

    let in_index = find_top_level_keyword(collection_header, "IN")?;
    let item_variable =
        parse_collection_filter_variable(collection_header.get(..in_index)?.trim())?;
    let collection_source = collection_header.get(in_index + "IN".len()..)?.trim();
    if collection_source.is_empty() {
        return None;
    }

    Some(StaticReduceSource {
        accumulator_variable,
        initial_source: initial_source.to_string(),
        item_variable,
        collection_source: collection_source.to_string(),
        expression_source: expression_source.to_string(),
    })
}

fn static_list_function_source_from_cst(
    cypher: &str,
    node: &SyntaxNode,
) -> Option<((usize, usize), StaticListFunctionSource)> {
    let range = node.text_range();
    let start: usize = range.start().into();
    let end: usize = range.end().into();
    let source = cypher.get(start..end)?;
    let value = parse_static_list_function_source(source)?;
    Some(((start, end), value))
}

fn parse_static_list_function_source(source: &str) -> Option<StaticListFunctionSource> {
    let open = source.find('(')?;
    let close = source.rfind(')')?;
    if close <= open {
        return None;
    }
    let function_name = source.get(..open)?.trim();
    let kind = if function_name.eq_ignore_ascii_case("filter") {
        StaticListFunctionKind::Filter
    } else if function_name.eq_ignore_ascii_case("extract") {
        StaticListFunctionKind::Extract
    } else {
        return None;
    };

    let inner = source.get(open + 1..close)?.trim();
    match kind {
        StaticListFunctionKind::Filter => parse_static_filter_function_source(inner),
        StaticListFunctionKind::Extract => parse_static_extract_function_source(inner),
    }
}

fn parse_static_filter_function_source(inner: &str) -> Option<StaticListFunctionSource> {
    let in_index = find_top_level_keyword(inner, "IN")?;
    let variable = parse_collection_filter_variable(inner.get(..in_index)?.trim())?;
    let after_in = inner.get(in_index + "IN".len()..)?.trim();
    let where_index = find_top_level_keyword(after_in, "WHERE")?;
    let collection_source = after_in.get(..where_index)?.trim();
    let filter_source = after_in.get(where_index + "WHERE".len()..)?.trim();
    if collection_source.is_empty() || filter_source.is_empty() {
        return None;
    }
    Some(StaticListFunctionSource {
        kind: StaticListFunctionKind::Filter,
        variable,
        collection_source: collection_source.to_string(),
        filter_source: Some(filter_source.to_string()),
        map_source: None,
    })
}

fn parse_static_extract_function_source(inner: &str) -> Option<StaticListFunctionSource> {
    let in_index = find_top_level_keyword(inner, "IN")?;
    let variable = parse_collection_filter_variable(inner.get(..in_index)?.trim())?;
    let after_in = inner.get(in_index + "IN".len()..)?.trim();
    let pipe_index = find_top_level_character(after_in, '|')?;
    let collection_source = after_in.get(..pipe_index)?.trim();
    let map_source = after_in.get(pipe_index + 1..)?.trim();
    if collection_source.is_empty() || map_source.is_empty() {
        return None;
    }
    Some(StaticListFunctionSource {
        kind: StaticListFunctionKind::Extract,
        variable,
        collection_source: collection_source.to_string(),
        filter_source: None,
        map_source: Some(map_source.to_string()),
    })
}

pub(super) fn parse_collection_filter_variable(source: &str) -> Option<String> {
    if source.is_empty() {
        return None;
    }
    if let Some(stripped) = source
        .strip_prefix('`')
        .and_then(|value| value.strip_suffix('`'))
    {
        return Some(stripped.replace("``", "`"));
    }
    let mut characters = source.chars();
    let first = characters.next()?;
    if matches!(
        source.to_ascii_lowercase().as_str(),
        "null" | "true" | "false"
    ) {
        return None;
    }
    (first == '_' || first.is_ascii_alphabetic())
        .then_some(())
        .filter(|()| {
            characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
        })
        .map(|()| source.to_string())
}

fn split_top_level_arguments(source: &str) -> Option<Vec<&str>> {
    if source.trim().is_empty() {
        return Some(Vec::new());
    }

    let mut arguments = Vec::new();
    let mut depth = 0usize;
    let mut in_string = false;
    let mut in_escaped_identifier = false;
    let mut start = 0usize;
    let mut index = 0usize;

    while index < source.len() {
        let rest = source.get(index..)?;
        let character = rest.chars().next()?;
        let character_len = character.len_utf8();

        if in_string {
            if character == '\'' {
                let next_index = index + character_len;
                if source.get(next_index..)?.starts_with('\'') {
                    index = next_index + '\''.len_utf8();
                    continue;
                }
                in_string = false;
            }
            index += character_len;
            continue;
        }

        if in_escaped_identifier {
            if character == '`' {
                let next_index = index + character_len;
                if source.get(next_index..)?.starts_with('`') {
                    index = next_index + '`'.len_utf8();
                    continue;
                }
                in_escaped_identifier = false;
            }
            index += character_len;
            continue;
        }

        match character {
            '\'' => in_string = true,
            '`' => in_escaped_identifier = true,
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth = depth.checked_sub(1)?,
            ',' if depth == 0 => {
                arguments.push(source.get(start..index)?.trim());
                start = index + character_len;
            }
            _ => {}
        }

        index += character_len;
    }

    if depth != 0 || in_string || in_escaped_identifier {
        return None;
    }

    arguments.push(source.get(start..)?.trim());
    arguments
        .iter()
        .all(|argument| !argument.is_empty())
        .then_some(arguments)
}

fn find_top_level_keyword(source: &str, keyword: &str) -> Option<usize> {
    let keyword_len = keyword.len();
    let mut depth = 0usize;
    let mut in_string = false;
    let mut in_escaped_identifier = false;
    let mut index = 0usize;

    while index < source.len() {
        let rest = source.get(index..)?;
        let character = rest.chars().next()?;
        let character_len = character.len_utf8();

        if in_string {
            if character == '\'' {
                let next_index = index + character_len;
                if source.get(next_index..)?.starts_with('\'') {
                    index = next_index + '\''.len_utf8();
                    continue;
                }
                in_string = false;
            }
            index += character_len;
            continue;
        }

        if in_escaped_identifier {
            if character == '`' {
                let next_index = index + character_len;
                if source.get(next_index..)?.starts_with('`') {
                    index = next_index + '`'.len_utf8();
                    continue;
                }
                in_escaped_identifier = false;
            }
            index += character_len;
            continue;
        }

        match character {
            '\'' => in_string = true,
            '`' => in_escaped_identifier = true,
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }

        if depth == 0
            && rest.len() >= keyword_len
            && rest
                .get(..keyword_len)
                .is_some_and(|candidate| candidate.eq_ignore_ascii_case(keyword))
            && keyword_has_boundaries(source, index, keyword_len)
        {
            return Some(index);
        }

        index += character_len;
    }
    None
}

fn find_top_level_character(source: &str, target: char) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut in_escaped_identifier = false;
    let mut index = 0usize;

    while index < source.len() {
        let rest = source.get(index..)?;
        let character = rest.chars().next()?;
        let character_len = character.len_utf8();

        if in_string {
            if character == '\'' {
                let next_index = index + character_len;
                if source.get(next_index..)?.starts_with('\'') {
                    index = next_index + '\''.len_utf8();
                    continue;
                }
                in_string = false;
            }
            index += character_len;
            continue;
        }

        if in_escaped_identifier {
            if character == '`' {
                let next_index = index + character_len;
                if source.get(next_index..)?.starts_with('`') {
                    index = next_index + '`'.len_utf8();
                    continue;
                }
                in_escaped_identifier = false;
            }
            index += character_len;
            continue;
        }

        match character {
            '\'' => in_string = true,
            '`' => in_escaped_identifier = true,
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }

        if depth == 0 && character == target {
            return Some(index);
        }

        index += character_len;
    }
    None
}

fn keyword_has_boundaries(source: &str, index: usize, keyword_len: usize) -> bool {
    let before = source
        .get(..index)
        .and_then(|prefix| prefix.chars().next_back());
    let after = source
        .get(index + keyword_len..)
        .and_then(|suffix| suffix.chars().next());
    !before.is_some_and(is_identifier_continue) && !after.is_some_and(is_identifier_continue)
}

fn keyword_at(source: &str, index: usize, keyword: &str) -> bool {
    source
        .get(index..index + keyword.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(keyword))
        && keyword_has_boundaries(source, index, keyword.len())
}

fn order_clause_end_keyword_at(source: &str, index: usize) -> bool {
    const CLAUSE_END_KEYWORDS: &[&str] = &[
        "CALL", "CREATE", "DELETE", "FINISH", "LIMIT", "MATCH", "MERGE", "OPTIONAL", "REMOVE",
        "RETURN", "SET", "SKIP", "UNION", "UNWIND", "WHERE", "WITH",
    ];

    CLAUSE_END_KEYWORDS
        .iter()
        .any(|keyword| keyword_at(source, index, keyword))
}

fn trim_ascii_whitespace_end(source: &str, mut end: usize) -> usize {
    while let Some(prefix) = source.get(..end) {
        let Some(character) = prefix.chars().next_back() else {
            return end;
        };
        if !character.is_ascii_whitespace() {
            return end;
        }
        end -= character.len_utf8();
    }
    end
}

fn include_preceding_ascii_whitespace(source: &str, lower_bound: usize, mut index: usize) -> usize {
    while index > lower_bound {
        let Some(prefix) = source.get(lower_bound..index) else {
            return index;
        };
        let Some(character) = prefix.chars().next_back() else {
            return index;
        };
        if !character.is_ascii_whitespace() {
            return index;
        }
        index -= character.len_utf8();
    }
    index
}

fn is_identifier_continue(character: char) -> bool {
    character == '_' || character.is_ascii_alphanumeric()
}

fn variable_function_argument_from_cst(
    node: &SyntaxNode,
) -> Option<((usize, usize), VariableFunctionArgument)> {
    let range = node.text_range();
    let start: usize = range.start().into();
    let end: usize = range.end().into();
    let source = node.text().to_string();
    let open = source.find('(')?;
    let close = source.rfind(')')?;
    if close <= open {
        return variable_function_argument_from_children(node, start, end);
    }
    let Some(arguments) = split_top_level_arguments(source.get(open + 1..close)?) else {
        return variable_function_argument_from_children(node, start, end);
    };
    let mut variables = arguments
        .iter()
        .enumerate()
        .filter_map(|(index, argument)| {
            parse_variable_function_argument_source(argument).map(|variable| (index, variable))
        });
    let (index, variable) = variables.next()?;
    if variables.next().is_some() {
        return None;
    }
    Some((
        (start, end),
        VariableFunctionArgument {
            variable,
            index,
            count: arguments.len(),
        },
    ))
}

fn function_argument_sources_from_cst(
    node: &SyntaxNode,
) -> Option<((usize, usize), FunctionArgumentSources)> {
    let range = node.text_range();
    let start: usize = range.start().into();
    let end: usize = range.end().into();
    let source = node.text().to_string();
    let open = source.find('(')?;
    let close = source.rfind(')')?;
    if close <= open {
        return None;
    }
    let arguments = split_top_level_arguments(source.get(open + 1..close)?)?;
    Some((
        (start, end),
        FunctionArgumentSources {
            arguments: arguments.into_iter().map(str::to_string).collect(),
        },
    ))
}

fn parse_variable_function_argument_source(source: &str) -> Option<String> {
    let source = source.trim();
    let source = source
        .get(.."DISTINCT".len())
        .filter(|candidate| candidate.eq_ignore_ascii_case("DISTINCT"))
        .filter(|_| keyword_has_boundaries(source, 0, "DISTINCT".len()))
        .and_then(|_| source.get("DISTINCT".len()..))
        .map_or(source, str::trim_start);
    parse_collection_filter_variable(source)
}

fn variable_function_argument_from_children(
    node: &SyntaxNode,
    start: usize,
    end: usize,
) -> Option<((usize, usize), VariableFunctionArgument)> {
    let mut variables = node
        .children()
        .filter(|child| child.kind() == SyntaxKind::VARIABLE)
        .filter_map(|child| {
            let source = child.text().to_string();
            parse_collection_filter_variable(source.trim())
        });
    let variable = variables.next()?;
    if variables.next().is_some() {
        return None;
    }
    Some((
        (start, end),
        VariableFunctionArgument {
            variable,
            index: 0,
            count: 0,
        },
    ))
}
