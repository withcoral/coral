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

function wrapProgramBody(body) {
  return {
    source: `(async () => {\n${body}\n})();\n`,
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
