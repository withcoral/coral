// A literal `coral.addTool({ source: name })` or `coral.updateTool` call
// saves the function `name` names: a function the calling
// source declares at its top level. Admission extracts its module, the
// declaration and every top-level declaration statement it reaches, and
// replaces the `source` value with a private carrier of that text. Only the Program host reads a carrier, so no
// authored value reaches the Tool as a source.

const SAVE_FUNCTION_SOURCE_GLOBAL = '__coralSavedFunctionSource';

// The Built-in Tools that take a function as `source`. The Program host
// in runtime/program_host_bootstrap.js lists them too; change both.
const SAVING_TOOLS = new Set(['addTool', 'updateTool']);

// The saving Tool `access` names, or undefined.
function savingTool(access) {
  return access?.root === 'lexical' &&
    access.segments.length === 1 &&
    SAVING_TOOLS.has(access.segments[0])
    ? access.segments[0]
    : undefined;
}

// Whether every segment from `coral` to `node` is a property name.
function isLiteralCoralChain(node) {
  let current = unwrapTransparentExpression(node);
  while (ts.isPropertyAccessExpression(current)) {
    current = unwrapTransparentExpression(current.expression);
  }
  return ts.isIdentifier(current);
}

// The call whose callee is `node`, through transparent wrappers.
function calleeCall(node) {
  const callee = outermostTransparentExpression(node);
  return ts.isCallExpression(callee.parent) && callee.parent.expression === callee
    ? callee.parent
    : undefined;
}

function isDeclarationStatement(statement) {
  return (
    ts.isVariableStatement(statement) ||
    ts.isFunctionDeclaration(statement) ||
    ts.isClassDeclaration(statement) ||
    ts.isInterfaceDeclaration(statement) ||
    ts.isTypeAliasDeclaration(statement) ||
    ts.isEnumDeclaration(statement) ||
    ts.isModuleDeclaration(statement)
  );
}

// The statement of `container` that holds `node`, if any.
function topLevelStatement(node, container) {
  let current = node;
  while (current.parent !== undefined && current.parent !== container) {
    current = current.parent;
  }
  return current.parent === container ? current : undefined;
}

function isFunctionInitializer(initializer) {
  if (initializer === undefined) return false;
  const value = unwrapTransparentExpression(initializer);
  return ts.isArrowFunction(value) || ts.isFunctionExpression(value);
}

// The top-level statements that declare the function `symbol` names, or
// undefined unless it is a top-level function declaration or a top-level
// `const` bound to an arrow or function expression.
function savedFunctionStatements(symbol, sourceFile, container) {
  const declarations = (symbol?.declarations ?? []).filter(
    (declaration) => declaration.getSourceFile() === sourceFile,
  );
  if (declarations.length === 0) return undefined;
  if (
    declarations.every(
      (declaration) => ts.isFunctionDeclaration(declaration) && declaration.parent === container,
    ) &&
    declarations.some((declaration) => declaration.body !== undefined)
  ) {
    return declarations;
  }
  const [declaration] = declarations;
  if (
    declarations.length === 1 &&
    ts.isVariableDeclaration(declaration) &&
    ts.isIdentifier(declaration.name) &&
    ts.isVariableDeclarationList(declaration.parent) &&
    (declaration.parent.flags & ts.NodeFlags.Const) !== 0 &&
    ts.isVariableStatement(declaration.parent.parent) &&
    declaration.parent.parent.parent === container &&
    isFunctionInitializer(declaration.initializer)
  ) {
    return [declaration.parent.parent];
  }
  return undefined;
}

function referencedSymbol(node, checker) {
  return ts.isShorthandPropertyAssignment(node.parent) && node.parent.name === node
    ? checker.getShorthandAssignmentValueSymbol(node.parent)
    : checker.getSymbolAtLocation(node);
}

// `roots` and every top-level declaration statement they reach through any
// reference, runtime or type, in source order. A reference that reaches a
// name declared inside another kind of top-level statement, such as a `var`
// in a block, is returned instead: copying that statement would run it.
function declarationClosure(roots, sourceFile, container, checker) {
  const included = new Set(roots);
  const pending = [...roots];
  let unreachable;
  while (pending.length > 0) {
    const visit = (node) => {
      if (unreachable !== undefined) return;
      if (ts.isIdentifier(node) && identifierIsReference(node)) {
        for (const declaration of referencedSymbol(node, checker)?.declarations ?? []) {
          if (declaration.getSourceFile() !== sourceFile) continue;
          const statement = topLevelStatement(declaration, container);
          if (statement === undefined || included.has(statement)) continue;
          if (!isDeclarationStatement(statement)) {
            unreachable = node;
            return;
          }
          included.add(statement);
          pending.push(statement);
        }
      }
      ts.forEachChild(node, visit);
    };
    visit(pending.pop());
    if (unreachable !== undefined) return { unreachable };
  }
  return { statements: [...included].toSorted((left, right) => left.pos - right.pos) };
}

function isLineTerminator(character) {
  return /[\n\r\u2028\u2029]/u.test(character);
}

// One statement as written: from the start of its line, when only
// indentation precedes it, through its own leading and same-line trailing
// comments, without `export` or `default`, which the saved module states once.
function statementText(statement, sourceFile) {
  const text = sourceFile.text;
  const ownComment = (range) =>
    statement.pos === 0 || isLineTerminator(text.slice(statement.pos, range.pos));
  const leading = (ts.getLeadingCommentRanges(text, statement.pos) ?? []).find(ownComment);
  let start = leading?.pos ?? statement.getStart(sourceFile);
  let lineStart = start;
  while (lineStart > 0 && /[ \t]/u.test(text[lineStart - 1])) lineStart -= 1;
  if (lineStart === 0 || isLineTerminator(text[lineStart - 1])) start = lineStart;
  const trailing = (ts.getTrailingCommentRanges(text, statement.end) ?? []).at(-1);
  const end = trailing?.end ?? statement.end;

  let copied = '';
  let cursor = start;
  for (const modifier of statement.modifiers ?? []) {
    if (
      modifier.kind === ts.SyntaxKind.ExportKeyword ||
      modifier.kind === ts.SyntaxKind.DefaultKeyword
    ) {
      copied += text.slice(cursor, modifier.getStart(sourceFile));
      cursor = ts.skipTrivia(text, modifier.end);
    }
  }
  return copied + text.slice(cursor, end);
}

function savedModuleText(statements, name, sourceFile) {
  const declarations = statements.map((statement) => statementText(statement, sourceFile));
  return `${declarations.join('\n\n')}\n\nexport default ${name};\n`;
}

// A string literal on one generated line: JSON escapes every line terminator
// but U+2028 and U+2029.
function singleLineStringLiteral(text) {
  return JSON.stringify(text)
    .replace(/\u2028/gu, '\\u2028')
    .replace(/\u2029/gu, '\\u2029');
}

function sourceValueRefusal(value, scope) {
  if (
    ts.isStringLiteral(value) ||
    ts.isNoSubstitutionTemplateLiteral(value) ||
    ts.isTemplateExpression(value)
  ) {
    return `source must name a function declared at the top level of ${scope}, not a string`;
  }
  if (
    ts.isArrowFunction(value) ||
    ts.isFunctionExpression(value) ||
    ts.isMethodDeclaration(value) ||
    ts.isAccessor(value)
  ) {
    return `source must name a function declared at the top level of ${scope}, not an inline function`;
  }
  return `source must name a function declared at the top level of ${scope}`;
}

function isSourceProperty(property) {
  const name = property.name;
  return (
    name !== undefined &&
    (ts.isIdentifier(name) || ts.isStringLiteral(name)) &&
    name.text === 'source'
  );
}

// The edits that carry each saved function's module to its literal
// saving call in `sourceFile`, or the first refusal.
// `container` holds the source's top-level statements, `sourceName` and
// `scope` name it in a refusal, and `carrier` names the private function that
// wraps each module.
function savedFunctionExtraction({
  sourceFile,
  program,
  container,
  authoredMappings,
  sourceName,
  scope,
  carrier,
}) {
  const checker = program.getTypeChecker();
  const edits = [];
  let refusal;
  function refuse(message, node) {
    const { line, column } = authoredReferenceLocation(sourceFile, node, authoredMappings);
    refusal = `${sourceName}:${line}:${column}: ${message}`;
    return false;
  }
  function carry(property, valueNode, statements, name) {
    const literal = singleLineStringLiteral(savedModuleText(statements, name, sourceFile));
    const shorthand = ts.isShorthandPropertyAssignment(property);
    edits.push({
      start: valueNode.getStart(sourceFile),
      end: valueNode.end,
      line: `${shorthand ? 'source: ' : ''}${carrier}(${literal})`,
    });
  }
  // A shorthand `source` names itself; any other value must be a name.
  function extractSource(property) {
    let name;
    let symbol;
    if (ts.isShorthandPropertyAssignment(property)) {
      name = property.name;
      symbol = checker.getShorthandAssignmentValueSymbol(property);
    } else if (ts.isPropertyAssignment(property)) {
      name = unwrapTransparentExpression(property.initializer);
      if (!ts.isIdentifier(name)) return refuse(sourceValueRefusal(name, scope), name);
      symbol = checker.getSymbolAtLocation(name);
    } else {
      return refuse(sourceValueRefusal(property, scope), property);
    }
    const statements = savedFunctionStatements(symbol, sourceFile, container);
    if (statements === undefined) {
      return refuse(
        `${name.text} is not a function declared at the top level of ${scope}; declare it there with function or const`,
        name,
      );
    }
    const closure = declarationClosure(statements, sourceFile, container, checker);
    if (closure.unreachable !== undefined) {
      return refuse(
        `${closure.unreachable.text} is not declared at the top level of ${scope}; declare it there, outside any block or loop`,
        closure.unreachable,
      );
    }
    carry(
      property,
      ts.isShorthandPropertyAssignment(property) ? property.name : property.initializer,
      closure.statements,
      name.text,
    );
    return true;
  }

  walkRuntimeNodes(sourceFile, (node) => {
    if (
      !(ts.isPropertyAccessExpression(node) || ts.isElementAccessExpression(node)) ||
      parentContinuesCoralPath(node)
    ) {
      return true;
    }
    const tool = savingTool(coralAccess(node, checker, sourceFile));
    if (tool === undefined) return true;
    const call = calleeCall(node);
    if (!isLiteralCoralChain(node) || call === undefined) {
      return refuse(
        `Call coral.${tool} by its literal path, so it can take a function as source`,
        node,
      );
    }
    const input =
      call.arguments.length === 1 ? unwrapTransparentExpression(call.arguments[0]) : undefined;
    const sources =
      input !== undefined && ts.isObjectLiteralExpression(input)
        ? input.properties.filter(isSourceProperty)
        : [];
    if (sources.length === 0) {
      return refuse(`Pass coral.${tool} one object literal with source`, call);
    }
    if (sources.length > 1) {
      return refuse('Pass source once', sources[1]);
    }
    // A later spread or computed key may replace the carried source.
    const replacing = input.properties
      .slice(input.properties.indexOf(sources[0]) + 1)
      .find(
        (property) =>
          ts.isSpreadAssignment(property) ||
          (property.name !== undefined && ts.isComputedPropertyName(property.name)),
      );
    if (replacing !== undefined) {
      return refuse(
        'Put source after every spread and computed key, so none replaces it',
        replacing,
      );
    }
    return sources.every(extractSource);
  });
  return refusal === undefined
    ? { edits: edits.toSorted((left, right) => left.start - right.start) }
    : { refusal };
}

// A carrier name no authored source declares, so no authored binding
// shadows it.
function savedFunctionCarrierName(sourceFiles) {
  const used = new Set(sourceFiles.flatMap((sourceFile) => [...identifierNames(sourceFile)]));
  for (let suffix = 1; ; suffix += 1) {
    const name = `${SAVE_FUNCTION_SOURCE_GLOBAL}_${suffix}`;
    if (!used.has(name)) return name;
  }
}

// Private source that takes the host's carrier constructor before any
// authored code runs, so no Program can reach it.
function savedFunctionCarrierDeclaration(carrier) {
  return `const ${carrier} = globalThis.${SAVE_FUNCTION_SOURCE_GLOBAL}; Reflect.deleteProperty(globalThis, "${SAVE_FUNCTION_SOURCE_GLOBAL}");`;
}

function programExtraction(sourceFile, program, authoredMappings, carrier) {
  const entry = unwrapParentheses(sourceFile.statements[0].expression.expression);
  const extraction = savedFunctionExtraction({
    sourceFile,
    program,
    container: entry.body,
    authoredMappings,
    sourceName: 'Program',
    scope: 'this Program',
    carrier,
  });
  if (extraction.refusal !== undefined) return extraction;
  const offset = PROGRAM_WRAPPER_PREFIX.length;
  return {
    edits: extraction.edits.map((edit) => ({
      ...edit,
      start: edit.start - offset,
      end: edit.end - offset,
    })),
  };
}

function savedFunctionModuleExtraction(sourceFile, program, pathSegments, carrier) {
  return savedFunctionExtraction({
    sourceFile,
    program,
    container: sourceFile,
    authoredMappings: undefined,
    sourceName: `Private Tool coral.private.${pathSegments.join('.')}`,
    scope: 'this Private Tool',
    carrier,
  });
}
