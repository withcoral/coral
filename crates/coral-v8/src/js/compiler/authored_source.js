// Counts lines by ECMAScript line terminators, as TypeScript's emitter and V8
// do: CRLF once, and each CR, LF, U+2028 or U+2029 otherwise.
function sourceLineCount(text) {
  return ts.computeLineStarts(text).length;
}

function programAuthoredMappings(body, generatedLineOffset = 1) {
  const mappings = [];
  if (body.length > 0) {
    // A trailing CR joins the wrapper's next LF into one CRLF, so the empty
    // authored line after it never reaches the compiled source.
    const lineCount = sourceLineCount(body) - (body.endsWith('\r') ? 1 : 0);
    for (let index = 0; index < lineCount; index += 1) {
      mappings.push({
        generatedLine: generatedLineOffset + index,
        generatedColumn: 0,
        source: 'Program',
        line: index,
        column: 0,
      });
    }
  }
  return mappings;
}

// Every UTF-16 code unit except a line terminator becomes a space, so removed
// syntax leaves the code after it at its authored line and column.
function blankedText(text) {
  return text.replace(/[^\n\r\u2028\u2029]/g, ' ');
}

// Copies `source` with each edit's span blanked and its private line, if any,
// on generated lines of its own before the span. Edits are in source order
// and do not overlap. Every other generated line holds one authored line, or
// the rest of one, at its authored columns, so one column-zero mapping per
// line is exact and TypeScript's own map supplies the column. Private lines
// stay unmapped. A private line must hold no line terminator.
function editedSource(source, edits) {
  const lineStarts = ts.computeLineStarts(source);
  let text = '';
  let cursor = 0;
  const insertions = [];
  for (const edit of edits) {
    text += source.slice(cursor, edit.start);
    if (edit.line !== undefined) {
      const position = ts.computeLineAndCharacterOfPosition(lineStarts, edit.start);
      insertions.push(position);
      const column = position.character;
      text += `${column > 0 ? '\n' : ''}${edit.line}\n${' '.repeat(column)}`;
    }
    text += blankedText(source.slice(edit.start, edit.end));
    cursor = edit.end;
  }
  text += source.slice(cursor);

  const mappings = [];
  let generatedLine = 0;
  let next = 0;
  // A final line terminator leaves an empty last line that holds no code.
  for (let line = 0; line < lineStarts.length && lineStarts[line] < source.length; line += 1) {
    const mapAuthoredLine = () => {
      mappings.push({ generatedLine, generatedColumn: 0, line, column: 0 });
    };
    if (insertions[next]?.line !== line || insertions[next].character > 0) {
      mapAuthoredLine();
    }
    for (; insertions[next]?.line === line; next += 1) {
      generatedLine += insertions[next].character > 0 ? 2 : 1;
      mapAuthoredLine();
    }
    generatedLine += 1;
  }
  return { text, mappings };
}

// The authored Program body as it compiles: with `edits`, at body offsets,
// applied, and one mapping per authored line relative to the body's first
// generated line.
function compiledProgramBody(body, edits) {
  if (edits.length === 0) {
    return { text: body, mappings: programAuthoredMappings(body, 0) };
  }
  const edited = editedSource(body, edits);
  return {
    text: edited.text,
    mappings: edited.mappings.map((mapping) => ({ ...mapping, source: 'Program' })),
  };
}

const PROGRAM_WRAPPER_PREFIX = '(async () => {\n';

function wrapProgramBody(body) {
  return {
    source: `${PROGRAM_WRAPPER_PREFIX}${body}\n})();\n`,
    authoredMappings: programAuthoredMappings(body),
  };
}

function unwrapParentheses(node) {
  while (ts.isParenthesizedExpression(node)) {
    node = node.expression;
  }
  return node;
}

function hasExpectedProgramShape(sourceFile) {
  if (sourceFile.statements.length !== 1) return false;

  const statement = sourceFile.statements[0];
  if (!ts.isExpressionStatement(statement)) return false;

  const call = statement.expression;
  if (!ts.isCallExpression(call) || call.arguments.length !== 0) return false;

  const entry = unwrapParentheses(call.expression);
  if (!ts.isArrowFunction(entry)) return false;
  if (entry.parameters.length !== 0 || !ts.isBlock(entry.body)) return false;

  return entry.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.AsyncKeyword) === true;
}

function authoredMappingAt(mappings, line, column) {
  if (mappings === undefined) return undefined;
  let matched;
  for (const mapping of mappings) {
    if (mapping.generatedLine > line) break;
    if (mapping.generatedLine === line && mapping.generatedColumn <= column) {
      matched = mapping;
    }
  }
  return matched;
}

function diagnosticPayload(diagnostic, authoredMappings) {
  let line;
  let column;
  if (diagnostic.file && diagnostic.start !== undefined) {
    const position = diagnostic.file.getLineAndCharacterOfPosition(diagnostic.start);
    const authored = authoredMappingAt(authoredMappings, position.line, position.character);
    if (authored) {
      line = authored.line + 1;
      column = authored.column + position.character - authored.generatedColumn + 1;
    } else if (authoredMappings === undefined) {
      line = position.line + 1;
      column = position.character + 1;
    }
  }
  return {
    category: ts.DiagnosticCategory[diagnostic.category].toLowerCase(),
    code: diagnostic.code,
    message: ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n'),
    line,
    column,
  };
}

function authoredReferenceLocation(sourceFile, node, authoredMappings) {
  const position = sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile));
  const authored = authoredMappingAt(authoredMappings, position.line, position.character);
  return {
    line: (authored?.line ?? position.line) + 1,
    column: authored
      ? authored.column + position.character - authored.generatedColumn + 1
      : position.character + 1,
  };
}
