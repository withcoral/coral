function modifiersWithoutExport(modifiers) {
  return modifiers?.filter(
    (modifier) =>
      modifier.kind !== ts.SyntaxKind.ExportKeyword &&
      modifier.kind !== ts.SyntaxKind.DefaultKeyword,
  );
}

// The parser's leaf tokens of `node`, in source order. A context-free scanner
// cannot tell a template continuation or a regular expression from other
// tokens, so it would pair the wrong spans; the parse tree already has.
function syntaxTokens(sourceFile, node) {
  const tokens = [];
  const visit = (current) => {
    if (
      current.kind >= ts.SyntaxKind.FirstJSDocNode &&
      current.kind <= ts.SyntaxKind.LastJSDocNode
    ) {
      return;
    }
    const children = current.getChildren(sourceFile);
    if (children.length > 0) {
      children.forEach(visit);
    } else if (current.kind !== ts.SyntaxKind.EndOfFileToken && current.getWidth(sourceFile) > 0) {
      tokens.push({
        kind: current.kind,
        text: current.getText(sourceFile),
        position: current.getStart(sourceFile),
      });
    }
  };
  visit(node);
  return tokens;
}

function equivalentPrintedToken(left, right) {
  if (left.kind !== right.kind) return false;
  return left.kind !== ts.SyntaxKind.Identifier || left.text === right.text;
}

// Saved Function linking changes only each retained statement's export prefix.
// Align the unchanged token suffix so printer whitespace and blank-line removal
// cannot move runtime frames away from the original authored token positions.
// The printer also ends every statement it prints with a semicolon, including
// ones the author left to automatic semicolon insertion; only such an inserted
// semicolon is skipped. Any other difference stops the alignment, so earlier
// tokens stay unmapped rather than guessed.
function printedStatementMappings(block, sourceFile, authored) {
  const printedFile = ts.createSourceFile(
    'printed_saved_function.ts',
    block,
    sourceFile.languageVersion,
    true,
    ts.ScriptKind.TS,
  );
  const printedTokens = syntaxTokens(printedFile, printedFile);
  const authoredTokens = syntaxTokens(sourceFile, authored);
  let printedIndex = printedTokens.length - 1;
  let authoredIndex = authoredTokens.length - 1;

  const mappings = [];
  while (printedIndex >= 0 && authoredIndex >= 0) {
    const printed = printedTokens[printedIndex];
    const original = authoredTokens[authoredIndex];
    if (!equivalentPrintedToken(printed, original)) {
      if (printed.kind !== ts.SyntaxKind.SemicolonToken) break;
      printedIndex -= 1;
      continue;
    }
    const generated = printedFile.getLineAndCharacterOfPosition(printed.position);
    const authoredPosition = sourceFile.getLineAndCharacterOfPosition(original.position);
    mappings.push({
      generatedLine: generated.line,
      generatedColumn: generated.character,
      line: authoredPosition.line,
      column: authoredPosition.character,
    });
    printedIndex -= 1;
    authoredIndex -= 1;
  }
  return mappings.toReversed();
}

function savedFunctionRuntimeBody(source, target) {
  const sourceFile = ts.createSourceFile(
    'saved_function.ts',
    source,
    target,
    true,
    ts.ScriptKind.TS,
  );
  if (
    sourceFile.parseDiagnostics.some(
      (diagnostic) => diagnostic.category === ts.DiagnosticCategory.Error,
    )
  ) {
    throw new Error('Saved Function source has syntax errors under the current TypeScript Profile');
  }
  const capabilityError = forbiddenCapabilityError(sourceFile);
  if (capabilityError !== undefined) {
    throw new Error(capabilityError);
  }

  const statements = [];
  function retainStatement(emitted, authored) {
    statements.push({ emitted, authored });
  }
  let defaultExpression;

  for (const statement of sourceFile.statements) {
    const isDefault =
      statement.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.DefaultKeyword) ===
      true;
    const isExported =
      statement.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword) ===
      true;

    if (ts.isFunctionDeclaration(statement) && isDefault && isExported) {
      if (!statement.body) {
        throw new Error('Saved Function default export must have a runtime implementation');
      }
      if (statement.name) {
        retainStatement(
          ts.factory.updateFunctionDeclaration(
            statement,
            modifiersWithoutExport(statement.modifiers),
            statement.asteriskToken,
            statement.name,
            statement.typeParameters,
            statement.parameters,
            statement.type,
            statement.body,
          ),
          statement,
        );
        defaultExpression = ts.factory.createIdentifier(statement.name.text);
      } else {
        const defaultIdentifier = ts.factory.createUniqueName('__coralSavedFunctionDefaultExport');
        retainStatement(
          ts.factory.createVariableStatement(
            undefined,
            ts.factory.createVariableDeclarationList(
              [
                ts.factory.createVariableDeclaration(
                  defaultIdentifier,
                  undefined,
                  undefined,
                  ts.factory.createFunctionExpression(
                    modifiersWithoutExport(statement.modifiers),
                    statement.asteriskToken,
                    undefined,
                    statement.typeParameters,
                    statement.parameters,
                    statement.type,
                    statement.body,
                  ),
                ),
              ],
              ts.NodeFlags.Const,
            ),
          ),
          statement,
        );
        defaultExpression = defaultIdentifier;
      }
      continue;
    }
    if (ts.isExportAssignment(statement) && !statement.isExportEquals) {
      const defaultIdentifier = ts.factory.createUniqueName('__coralSavedFunctionDefaultExport');
      retainStatement(
        ts.factory.createVariableStatement(
          undefined,
          ts.factory.createVariableDeclarationList(
            [
              ts.factory.createVariableDeclaration(
                defaultIdentifier,
                undefined,
                undefined,
                statement.expression,
              ),
            ],
            ts.NodeFlags.Const,
          ),
        ),
        statement,
      );
      defaultExpression = defaultIdentifier;
      continue;
    }
    if (
      ts.isExportDeclaration(statement) &&
      !statement.moduleSpecifier &&
      statement.exportClause &&
      ts.isNamedExports(statement.exportClause) &&
      statement.exportClause.elements.length === 0
    ) {
      continue;
    }
    if (
      ts.isExportDeclaration(statement) &&
      !statement.moduleSpecifier &&
      !statement.isTypeOnly &&
      statement.exportClause &&
      ts.isNamedExports(statement.exportClause) &&
      statement.exportClause.elements.length === 1 &&
      statement.exportClause.elements[0].name.text === 'default' &&
      !statement.exportClause.elements[0].isTypeOnly
    ) {
      const element = statement.exportClause.elements[0];
      defaultExpression = ts.factory.createIdentifier((element.propertyName || element.name).text);
      continue;
    }
    if (isDefault || isExported || ts.isExportDeclaration(statement)) {
      throw new Error('Saved Function source has an unsupported export shape');
    }
    retainStatement(statement, statement);
  }
  if (defaultExpression === undefined) {
    throw new Error('Saved Function source has no runtime default export');
  }

  const printer = ts.createPrinter({ newLine: ts.NewLineKind.LineFeed });
  const blocks = [];
  const mappings = [];
  let generatedLineOffset = 0;
  for (const { emitted, authored } of statements) {
    const block = printer.printNode(ts.EmitHint.Unspecified, emitted, sourceFile);
    for (const mapping of printedStatementMappings(block, sourceFile, authored)) {
      mappings.push({
        ...mapping,
        generatedLine: mapping.generatedLine + generatedLineOffset,
      });
    }
    blocks.push(block);
    generatedLineOffset += sourceLineCount(block);
  }
  const privateReturn = printer.printNode(
    ts.EmitHint.Unspecified,
    ts.factory.createReturnStatement(
      ts.factory.createArrowFunction(
        undefined,
        undefined,
        [],
        undefined,
        ts.factory.createToken(ts.SyntaxKind.EqualsGreaterThanToken),
        defaultExpression,
      ),
    ),
    sourceFile,
  );
  blocks.push(privateReturn);
  return { text: blocks.join('\n'), mappings };
}

function linkedProgramSource(body, savedFunctions, target) {
  const pathKeys = new Set();
  for (const savedFunction of savedFunctions) {
    if (
      !Array.isArray(savedFunction.pathSegments) ||
      savedFunction.pathSegments.length === 0 ||
      savedFunction.pathSegments.some((segment) => typeof segment !== 'string') ||
      typeof savedFunction.source !== 'string'
    ) {
      throw new Error('Saved Function closure contains an invalid source entry');
    }
    const key = savedFunction.pathSegments.join('.');
    if (pathKeys.has(key)) {
      throw new Error('Saved Function closure contains a duplicate path');
    }
    pathKeys.add(key);
  }
  for (const key of pathKeys) {
    for (const other of pathKeys) {
      if (key !== other && other.startsWith(`${key}.`)) {
        throw new Error('Saved Function closure contains a callable namespace conflict');
      }
    }
  }

  const modules = savedFunctions.map((savedFunction, index) => ({
    ...savedFunction,
    index,
    body: savedFunctionRuntimeBody(savedFunction.source, target),
  }));
  const linkedPaths = JSON.stringify(
    savedFunctions.map((savedFunction) => savedFunction.pathSegments),
  );
  const sourceParts = [];
  const authoredMappings = [];
  let generatedLine = 0;
  function append(text) {
    // A CR ending the previous part and this LF form one CRLF terminator.
    const joinsCrLf = sourceParts.at(-1)?.endsWith('\r') && text.startsWith('\n');
    sourceParts.push(text);
    generatedLine += sourceLineCount(text) - 1 - (joinsCrLf ? 1 : 0);
  }

  append(`(async () => {
    let __coralFunctions = Object.freeze(Object.create(null));
    const __coralHostDescriptors = Object.getOwnPropertyDescriptors(
      globalThis.coral ?? Object.create(null),
    );
    Reflect.deleteProperty(__coralHostDescriptors, "functions");
    const coral = Object.create(null);
    Object.defineProperties(coral, __coralHostDescriptors);
    Object.defineProperty(coral, "functions", {
      get() { return __coralFunctions; },
      enumerable: true,
      configurable: false,
    });
    Object.preventExtensions(coral);

    const __coralLinkedPaths = ${linkedPaths};
    const __coralSavedFunctionDefaults = Object.create(null);
    for (let __coralIndex = 0; __coralIndex < __coralLinkedPaths.length; __coralIndex += 1) {
      Object.defineProperty(__coralSavedFunctionDefaults, __coralIndex, {
        value: undefined,
        enumerable: false,
        writable: true,
        configurable: false,
      });
    }
    Object.preventExtensions(__coralSavedFunctionDefaults);
    const __coralFunctionRoot = Object.create(null);
    const __coralFunctionNamespaces = [__coralFunctionRoot];
    for (let __coralIndex = 0; __coralIndex < __coralLinkedPaths.length; __coralIndex += 1) {
      const __coralPath = __coralLinkedPaths[__coralIndex];
      let __coralFunctionParent = __coralFunctionRoot;
      for (const __coralSegment of __coralPath.slice(0, -1)) {
        if (!Object.hasOwn(__coralFunctionParent, __coralSegment)) {
          const __coralNext = Object.create(null);
          Object.defineProperty(__coralFunctionParent, __coralSegment, {
            value: __coralNext,
            enumerable: true,
            writable: false,
            configurable: false,
          });
          __coralFunctionNamespaces.push(__coralNext);
        }
        __coralFunctionParent = __coralFunctionParent[__coralSegment];
      }
      Object.defineProperty(
        __coralFunctionParent,
        __coralPath[__coralPath.length - 1],
        {
          get() {
            const __coralDefault = __coralSavedFunctionDefaults[__coralIndex];
            return __coralDefault === undefined ? undefined : __coralDefault();
          },
          enumerable: true,
          configurable: false,
        },
      );
    }
    for (const __coralNamespace of __coralFunctionNamespaces.toReversed()) {
      Object.freeze(__coralNamespace);
    }
    __coralFunctions = __coralFunctionRoot;
`);

  for (const module of modules) {
    append(`    __coralSavedFunctionDefaults[${module.index}] = await (async () => {\n`);
    const savedFunctionStart = generatedLine;
    append(module.body.text);
    for (const mapping of module.body.mappings) {
      authoredMappings.push({
        ...mapping,
        generatedLine: savedFunctionStart + mapping.generatedLine,
        source: `Saved Function coral.functions.${module.pathSegments.join('.')}`,
      });
    }
    append(`
    })();
    if (
      typeof __coralSavedFunctionDefaults[${module.index}] !== "function"
      || typeof __coralSavedFunctionDefaults[${module.index}]() !== "function"
    ) {
      throw new TypeError("Saved Function default export is not callable");
    }
`);
  }

  append(`    return await (async () => {\n`);
  const programStart = generatedLine;
  append(body);
  authoredMappings.push(...programAuthoredMappings(body, programStart));
  append(`
    })();
  })();
`);
  return { source: sourceParts.join(''), authoredMappings };
}
