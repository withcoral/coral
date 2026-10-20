// Every UTF-16 code unit except a line terminator becomes a space, so removed
// export syntax leaves the code after it at its authored line and column.
function blankedText(text) {
  return text.replace(/[^\n\r\u2028\u2029]/g, ' ');
}

// Identifier names as the parser reads them, so an escaped spelling such as
// `\u005f_x` and the plain `__x` count as one name.
function identifierNames(sourceFile) {
  const names = new Set();
  const visit = (node) => {
    if (ts.isIdentifier(node)) names.add(node.text);
    ts.forEachChild(node, visit);
  };
  visit(sourceFile);
  return names;
}

// Copies the authored source with each edit's span blanked and its private
// line, if any, on generated lines of its own before the span. Every other
// generated line holds one authored line, or the rest of one, at its authored
// columns, so one column-zero mapping per line is exact and TypeScript's own
// map supplies the column. Private lines and the return stay unmapped.
function editedSavedFunctionBody(sourceFile, edits, defaultName) {
  const source = sourceFile.text;
  let text = '';
  let cursor = 0;
  const insertions = [];
  for (const edit of edits) {
    text += source.slice(cursor, edit.start);
    if (edit.line !== undefined) {
      const position = sourceFile.getLineAndCharacterOfPosition(edit.start);
      insertions.push(position);
      const column = position.character;
      text += `${column > 0 ? '\n' : ''}${edit.line}\n${' '.repeat(column)}`;
    }
    text += blankedText(source.slice(edit.start, edit.end));
    cursor = edit.end;
  }
  // The line break ends any trailing comment, and the semicolon ends any
  // unterminated last statement.
  text += `${source.slice(cursor)}\n;return () => ${defaultName};`;

  const lineStarts = ts.computeLineStarts(source);
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

  const usedNames = identifierNames(sourceFile);
  function privateName() {
    for (let suffix = 1; ; suffix += 1) {
      const name = `__coralSavedFunctionDefaultExport_${suffix}`;
      if (!usedNames.has(name)) {
        usedNames.add(name);
        return name;
      }
    }
  }
  // Authored spans to blank, in source order. An edit's `line` is private
  // source placed on its own line before the span.
  const edits = [];
  const shebang = /^#![^\n\r\u2028\u2029]*/.exec(source);
  if (shebang) edits.push({ start: 0, end: shebang[0].length });
  // A removed export declaration still ends the statement before it.
  const removeStatement = (statement) => {
    edits.push({ start: statement.getStart(sourceFile), end: statement.end, line: ';' });
  };
  let defaultName;

  for (const statement of sourceFile.statements) {
    const exportModifiers =
      statement.modifiers?.filter(
        (modifier) =>
          modifier.kind === ts.SyntaxKind.ExportKeyword ||
          modifier.kind === ts.SyntaxKind.DefaultKeyword,
      ) ?? [];
    const isDefault = exportModifiers.some(
      (modifier) => modifier.kind === ts.SyntaxKind.DefaultKeyword,
    );
    const isExported = exportModifiers.some(
      (modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword,
    );

    if (ts.isFunctionDeclaration(statement) && isDefault && isExported) {
      if (!statement.body) {
        throw new Error('Saved Function default export must have a runtime implementation');
      }
      for (const modifier of exportModifiers) {
        edits.push({ start: modifier.getStart(sourceFile), end: modifier.end });
      }
      if (statement.name) {
        defaultName = statement.name.text;
      } else {
        // Name the declaration after `function` or its `*`, where a line
        // break cannot change the parse.
        defaultName = privateName();
        const nameStart = (
          statement.asteriskToken ??
          statement
            .getChildren(sourceFile)
            .find((child) => child.kind === ts.SyntaxKind.FunctionKeyword)
        ).end;
        edits.push({ start: nameStart, end: nameStart, line: defaultName });
      }
      continue;
    }
    if (ts.isExportAssignment(statement) && !statement.isExportEquals) {
      // `export default` and `const name =` both take an AssignmentExpression,
      // so automatic semicolon insertion ends the statement at the same token.
      defaultName = privateName();
      edits.push({
        start: statement.getStart(sourceFile),
        end: statement.expression.getStart(sourceFile),
        line: `const ${defaultName} =`,
      });
      continue;
    }
    if (
      ts.isExportDeclaration(statement) &&
      !statement.moduleSpecifier &&
      statement.exportClause &&
      ts.isNamedExports(statement.exportClause) &&
      statement.exportClause.elements.length === 0
    ) {
      removeStatement(statement);
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
      defaultName = (element.propertyName || element.name).text;
      removeStatement(statement);
      continue;
    }
    if (isDefault || isExported || ts.isExportDeclaration(statement)) {
      throw new Error('Saved Function source has an unsupported export shape');
    }
  }
  if (defaultName === undefined) {
    throw new Error('Saved Function source has no runtime default export');
  }
  return editedSavedFunctionBody(sourceFile, edits, defaultName);
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
