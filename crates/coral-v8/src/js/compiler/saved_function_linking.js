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

// The authored source with `edits` applied and private source after it: the
// line break ends any trailing comment, and the semicolon ends any
// unterminated last statement.
function editedSavedFunctionBody(sourceFile, edits, defaultName) {
  const edited = editedSource(sourceFile.text, edits);
  return {
    text: `${edited.text}\n;return () => ${defaultName};`,
    mappings: edited.mappings,
  };
}

// `extraEdits` carry the functions the module saves, at module offsets.
function savedFunctionRuntimeBody(source, target, extraEdits) {
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
  edits.push(...extraEdits);
  edits.sort((left, right) => left.start - right.start);
  return editedSavedFunctionBody(sourceFile, edits, defaultName);
}

const COMPLETE_ROOT_GLOBAL = '__coralCompleteRoot';

// Private source that runs before any authored code: it takes the host's
// root completion, builds `coral.private` from the linked Private Tools, each
// of which a getter reads once its module has initialized, and completes
// `coral` with it and a member for each Built-in Tool in `builtinTools`.
// Module code then sets `__coralSavedFunctionDefaults[index]`.
function coralRootPrelude(builtinTools, savedFunctions) {
  if (
    !Array.isArray(builtinTools) ||
    builtinTools.some(
      (key) => typeof key !== 'string' || !ts.isIdentifierText(key, ts.ScriptTarget.ES2025),
    )
  ) {
    throw new Error('Built-in Tools must be identifiers');
  }
  const linkedPaths = JSON.stringify(
    savedFunctions.map((savedFunction) => savedFunction.pathSegments),
  );
  return `    const __coralCompleteRoot = globalThis.${COMPLETE_ROOT_GLOBAL};
    Reflect.deleteProperty(globalThis, "${COMPLETE_ROOT_GLOBAL}");
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
    if (__coralCompleteRoot !== undefined) {
      __coralCompleteRoot(${JSON.stringify(builtinTools)}, __coralFunctionRoot);
    } else {
      // An execution without the Program host calls no Tool, so its \`coral\`
      // holds only the Private Tools it links.
      Object.defineProperty(globalThis, "coral", {
        value: Object.freeze(Object.defineProperty(Object.create(null), "private", {
          value: __coralFunctionRoot,
          enumerable: true,
        })),
        enumerable: true,
      });
    }`;
}

// `saves` names the private carrier and holds the edits that carry what the
// Program body and each module save.
function linkedProgramSource(body, savedFunctions, builtinTools, target, saves) {
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
    body: savedFunctionRuntimeBody(savedFunction.source, target, saves.moduleEdits[index]),
  }));
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
    ${savedFunctionCarrierDeclaration(saves.carrier)}
${coralRootPrelude(builtinTools, savedFunctions)}
`);

  for (const module of modules) {
    append(`    __coralSavedFunctionDefaults[${module.index}] = await (async () => {\n`);
    const savedFunctionStart = generatedLine;
    append(module.body.text);
    for (const mapping of module.body.mappings) {
      authoredMappings.push({
        ...mapping,
        generatedLine: savedFunctionStart + mapping.generatedLine,
        source: `Saved Function coral.private.${module.pathSegments.join('.')}`,
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
  const programBody = compiledProgramBody(body, saves.programEdits);
  append(programBody.text);
  for (const mapping of programBody.mappings) {
    authoredMappings.push({ ...mapping, generatedLine: programStart + mapping.generatedLine });
  }
  append(`
    })();
  })();
`);
  return { source: sourceParts.join(''), authoredMappings };
}
