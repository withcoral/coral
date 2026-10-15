use serde::Deserialize;
use sourcemap::{SourceMap, SourceMapBuilder};

use crate::Error;

const COMPILER_SOURCE_NAME: &str = "program.ts";
const EMITTED_FILE_NAME: &str = "program.js";
const PROGRAM_SOURCE_NAME: &str = "Program";
const SAVED_FUNCTION_SOURCE_PREFIX: &str = "Saved Function coral.functions.";
// Keep one extra frame so the error representation can report truncation.
const RUNTIME_STACK_MAX_FRAMES: usize = 17;

/// One compiler-input line's exact authored origin.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuthoredSourceMapping {
    generated_line: u32,
    generated_column: u32,
    source: String,
    line: u32,
    column: u32,
}

/// Rewrites TypeScript's private-wrapper map into the authored-source map that
/// is retained with a Program package.
pub(crate) fn normalize(
    raw_source_map: &str,
    authored_mappings: &[AuthoredSourceMapping],
) -> Result<String, Error> {
    let compiler_map = SourceMap::from_slice(raw_source_map.as_bytes())
        .map_err(|error| invalid_source_map(error.to_string()))?;
    validate_compiler_map(&compiler_map)?;

    validate_authored_mappings(authored_mappings)?;
    let mut builder = SourceMapBuilder::new(Some(EMITTED_FILE_NAME));
    for token in compiler_map.tokens() {
        let authored =
            authored_mapping_at(authored_mappings, token.get_src_line(), token.get_src_col());
        match authored {
            Some(authored) => {
                let source_column = token
                    .get_src_col()
                    .checked_sub(authored.generated_column)
                    .and_then(|offset| authored.column.checked_add(offset))
                    .ok_or_else(|| invalid_source_map("authored column is out of range"))?;
                builder.add(
                    token.get_dst_line(),
                    token.get_dst_col(),
                    authored.line,
                    source_column,
                    Some(&authored.source),
                    None,
                    token.is_range(),
                );
            }
            None => {
                // An explicit unmapped token prevents greatest-lower-bound
                // lookup from attributing a later private wrapper frame to the
                // preceding authored line.
                builder.add(
                    token.get_dst_line(),
                    token.get_dst_col(),
                    0,
                    0,
                    None,
                    None,
                    token.is_range(),
                );
            }
        }
    }

    let source_map = builder.into_sourcemap();
    validate_authored_map(&source_map)?;
    let mut encoded = Vec::new();
    source_map
        .to_writer(&mut encoded)
        .map_err(|error| invalid_source_map(error.to_string()))?;
    String::from_utf8(encoded).map_err(|error| invalid_source_map(error.to_string()))
}

fn validate_authored_mappings(authored_mappings: &[AuthoredSourceMapping]) -> Result<(), Error> {
    let mut previous = None;
    for mapping in authored_mappings {
        validate_authored_source_name(&mapping.source)?;
        let generated = (mapping.generated_line, mapping.generated_column);
        if previous.is_some_and(|previous| previous >= generated) {
            return Err(invalid_source_map(
                "authored source mappings are not strictly ordered",
            ));
        }
        previous = Some(generated);
    }
    Ok(())
}

fn authored_mapping_at(
    mappings: &[AuthoredSourceMapping],
    generated_line: u32,
    generated_column: u32,
) -> Option<&AuthoredSourceMapping> {
    let index = mappings.partition_point(|mapping| {
        (mapping.generated_line, mapping.generated_column) <= (generated_line, generated_column)
    });
    index
        .checked_sub(1)
        .and_then(|index| mappings.get(index))
        .filter(|mapping| mapping.generated_line == generated_line)
}

pub(crate) fn decode_authored(value: &str) -> Result<SourceMap, Error> {
    let source_map = SourceMap::from_slice(value.as_bytes())
        .map_err(|error| invalid_source_map(error.to_string()))?;
    validate_authored_map(&source_map)?;
    Ok(source_map)
}

pub(crate) fn authored_frames(
    raw_stack: &str,
    source_map: &SourceMap,
) -> Vec<crate::ProgramErrorFrame> {
    let mut frames = Vec::new();
    for line in raw_stack.lines() {
        let Some((generated_line, generated_column)) = generated_location(line) else {
            continue;
        };
        let Some((source, line, column)) =
            authored_position(source_map, generated_line, generated_column)
        else {
            continue;
        };
        let frame = crate::ProgramErrorFrame {
            source: source.to_string(),
            line,
            column,
        };
        if frames.last() != Some(&frame) {
            frames.push(frame);
        }
        if frames.len() == RUNTIME_STACK_MAX_FRAMES {
            break;
        }
    }

    frames
}

/// Maps one one-based emitted position to the authored location it came from.
///
/// Returns `None` for a private wrapper position, which has no authored source.
pub(crate) fn authored_location(
    source_map: &SourceMap,
    generated_line: u32,
    generated_column: u32,
) -> Option<crate::AuthoredLocation> {
    let (source, line, column) = authored_position(
        source_map,
        generated_line.checked_sub(1)?,
        generated_column.checked_sub(1)?,
    )?;
    let source = if source == PROGRAM_SOURCE_NAME {
        crate::AuthoredSource::Program
    } else {
        crate::AuthoredSource::SavedFunction {
            path: source
                .strip_prefix(SAVED_FUNCTION_SOURCE_PREFIX)?
                .to_string(),
        }
    };
    Some(crate::AuthoredLocation {
        source,
        line,
        column,
    })
}

// Zero-based emitted position to its authored source and one-based position.
fn authored_position(
    source_map: &SourceMap,
    generated_line: u32,
    generated_column: u32,
) -> Option<(&str, u32, u32)> {
    let token = source_map.lookup_token(generated_line, generated_column)?;
    if token.get_dst_line() != generated_line {
        return None;
    }
    Some((
        token.get_source()?,
        token.get_src_line().saturating_add(1),
        token.get_src_col().saturating_add(1),
    ))
}

fn validate_compiler_map(source_map: &SourceMap) -> Result<(), Error> {
    if source_map.get_file() != Some(EMITTED_FILE_NAME)
        || source_map
            .get_source_root()
            .is_some_and(|root| !root.is_empty())
        || source_map.get_source_count() != 1
        || source_map.get_source(0) != Some(COMPILER_SOURCE_NAME)
        || source_map.get_source_contents(0).is_some()
    {
        return Err(invalid_source_map(
            "compiler map does not match the pinned TypeScript profile",
        ));
    }
    Ok(())
}

fn validate_authored_map(source_map: &SourceMap) -> Result<(), Error> {
    if source_map.get_file() != Some(EMITTED_FILE_NAME) || source_map.get_source_root().is_some() {
        return Err(invalid_source_map(
            "authored map does not target the private emitted file",
        ));
    }
    for source_id in 0..source_map.get_source_count() {
        let source = source_map
            .get_source(source_id)
            .ok_or_else(|| invalid_source_map("authored source name is missing"))?;
        validate_authored_source_name(source)?;
        if source_map.get_source_contents(source_id).is_some() {
            return Err(invalid_source_map(
                "authored source content must not be embedded",
            ));
        }
    }
    Ok(())
}

fn validate_authored_source_name(source: &str) -> Result<(), Error> {
    let valid = source == PROGRAM_SOURCE_NAME
        || source
            .strip_prefix(SAVED_FUNCTION_SOURCE_PREFIX)
            .is_some_and(|path| {
                !path.is_empty()
                    && path.len() <= 256
                    && !path
                        .chars()
                        .any(|character| character.is_control() || matches!(character, '/' | '\\'))
            });
    if valid {
        Ok(())
    } else {
        Err(invalid_source_map("authored source name is invalid"))
    }
}

fn generated_location(line: &str) -> Option<(u32, u32)> {
    let marker = format!("{EMITTED_FILE_NAME}:");
    let tail = line.rsplit_once(&marker)?.1;
    let mut fields = tail.split(':');
    let line = fields.next()?.parse::<u32>().ok()?.checked_sub(1)?;
    let column_text = fields
        .next()?
        .split(|character: char| !character.is_ascii_digit())
        .next()?;
    let column = column_text.parse::<u32>().ok()?.checked_sub(1)?;
    Some((line, column))
}

fn invalid_source_map(message: impl Into<String>) -> Error {
    Error::InvalidSourceMap(message.into())
}
