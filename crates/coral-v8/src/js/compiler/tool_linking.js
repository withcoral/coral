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
function editedLinkedToolBody(sourceFile, edits, defaultName) {
  const edited = editedSource(sourceFile.text, edits);
  return {
    text: `${edited.text}\n;return () => ${defaultName};`,
    mappings: edited.mappings,
  };
}

// `extraEdits` carry the functions the module saves, at module offsets.
function linkedToolRuntimeBody(source, target, extraEdits) {
  const sourceFile = ts.createSourceFile('linked_tool.ts', source, target, true, ts.ScriptKind.TS);
  if (
    sourceFile.parseDiagnostics.some(
      (diagnostic) => diagnostic.category === ts.DiagnosticCategory.Error,
    )
  ) {
    throw new Error('Tool source has syntax errors under the current TypeScript Profile');
  }
  const capabilityError = forbiddenCapabilityError(sourceFile);
  if (capabilityError !== undefined) {
    throw new Error(capabilityError);
  }

  const usedNames = identifierNames(sourceFile);
  function privateName() {
    for (let suffix = 1; ; suffix += 1) {
      const name = `__coralLinkedToolDefaultExport_${suffix}`;
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
        throw new Error('Tool default export must have a runtime implementation');
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
      throw new Error('Tool source has an unsupported export shape');
    }
  }
  if (defaultName === undefined) {
    throw new Error('Tool source has no runtime default export');
  }
  edits.push(...extraEdits);
  edits.sort((left, right) => left.start - right.start);
  return editedLinkedToolBody(sourceFile, edits, defaultName);
}

const COMPLETE_ROOT_GLOBAL = '__coralCompleteRoot';

// The members of `coral` that hold linked Tools, and how a source names
// each kind.
const LINKED_ROOT_KINDS = Object.freeze({ private: 'Private Tool', shared: 'Shared Tool' });

// Private source that runs before any authored code: it takes the host's
// root completion and completes `coral` with a member for each Built-in Tool
// in `builtinTools`, with `coral.private` and with `coral.shared`, which it
// builds from the linked Private and Shared Tools. The host makes each
// callable, outside this script, so the innermost frame of this script at a
// call is the authored call site. A call performs the Tool, whose body is the
// default export of its module once that module has initialized. Module code
// then sets `__coralLinkedToolDefaults[index]`.
function coralRootPrelude(builtinTools, linkedTools) {
  if (
    !Array.isArray(builtinTools) ||
    builtinTools.some(
      (key) => typeof key !== 'string' || !ts.isIdentifierText(key, ts.ScriptTarget.ES2025),
    )
  ) {
    throw new Error('Built-in Tools must be identifiers');
  }
  const linkedPaths = JSON.stringify(
    linkedTools.map((linkedTool) => ({
      root: linkedTool.root,
      path: linkedTool.pathSegments,
    })),
  );
  return `    const __coralCompleteRoot = globalThis.${COMPLETE_ROOT_GLOBAL};
    Reflect.deleteProperty(globalThis, "${COMPLETE_ROOT_GLOBAL}");
    const __coralLinkedPaths = ${linkedPaths};
    const __coralLinkedToolDefaults = Object.create(null);
    for (let __coralIndex = 0; __coralIndex < __coralLinkedPaths.length; __coralIndex += 1) {
      Object.defineProperty(__coralLinkedToolDefaults, __coralIndex, {
        value: undefined,
        enumerable: false,
        writable: true,
        configurable: false,
      });
    }
    Object.preventExtensions(__coralLinkedToolDefaults);
    const __coralBuildLinkedRoot = (__coralRoot, __coralLinkedTool) => {
      const __coralToolRoot = Object.create(null);
      const __coralToolNamespaces = [__coralToolRoot];
      for (let __coralIndex = 0; __coralIndex < __coralLinkedPaths.length; __coralIndex += 1) {
        if (__coralLinkedPaths[__coralIndex].root !== __coralRoot) continue;
        const __coralPath = __coralLinkedPaths[__coralIndex].path;
        const __coralCallPath = "coral." + __coralRoot + "." + __coralPath.join(".");
        let __coralToolParent = __coralToolRoot;
        for (const __coralSegment of __coralPath.slice(0, -1)) {
          if (!Object.hasOwn(__coralToolParent, __coralSegment)) {
            const __coralNext = Object.create(null);
            Object.defineProperty(__coralToolParent, __coralSegment, {
              value: __coralNext,
              enumerable: true,
              writable: false,
              configurable: false,
            });
            __coralToolNamespaces.push(__coralNext);
          }
          __coralToolParent = __coralToolParent[__coralSegment];
        }
        const __coralBody = () => {
          const __coralDefault = __coralLinkedToolDefaults[__coralIndex];
          if (__coralDefault === undefined) {
            throw new TypeError(__coralCallPath + " is called before its module initialized");
          }
          return __coralDefault();
        };
        Object.defineProperty(
          __coralToolParent,
          __coralPath[__coralPath.length - 1],
          {
            value: __coralLinkedTool(__coralCallPath, __coralBody),
            enumerable: true,
            writable: false,
            configurable: false,
          },
        );
      }
      for (const __coralNamespace of __coralToolNamespaces.toReversed()) {
        Object.freeze(__coralNamespace);
      }
      return __coralToolRoot;
    };
    const __coralBuildLinkedTools = (__coralLinkedTool) => ({
      private: __coralBuildLinkedRoot("private", __coralLinkedTool),
      shared: __coralBuildLinkedRoot("shared", __coralLinkedTool),
    });
    if (__coralCompleteRoot !== undefined) {
      __coralCompleteRoot(${JSON.stringify(builtinTools)}, __coralBuildLinkedTools);
    } else {
      // An execution without the Program host calls no Tool, so its \`coral\`
      // holds only the Private and Shared Tools it links, and calling one fails.
      const __coralLinked = __coralBuildLinkedTools((__coralCallPath) => () =>
        Promise.reject(new TypeError(__coralCallPath + " is called without the Program host")),
      );
      Object.defineProperty(globalThis, "coral", {
        value: Object.freeze(Object.defineProperties(Object.create(null), {
          private: { value: __coralLinked.private, enumerable: true },
          shared: { value: __coralLinked.shared, enumerable: true },
        })),
        enumerable: true,
      });
    }`;
}

// `saves` names the private carrier and holds the edits that carry what the
// Program body and each module save.
function linkedProgramSource(body, linkedTools, builtinTools, target, saves) {
  const pathKeys = { private: new Set(), shared: new Set() };
  for (const linkedTool of linkedTools) {
    if (
      !Object.hasOwn(LINKED_ROOT_KINDS, linkedTool.root) ||
      !Array.isArray(linkedTool.pathSegments) ||
      linkedTool.pathSegments.length === 0 ||
      linkedTool.pathSegments.some((segment) => typeof segment !== 'string') ||
      typeof linkedTool.source !== 'string'
    ) {
      throw new Error('Linked Tool closure contains an invalid source entry');
    }
    const key = linkedTool.pathSegments.join('.');
    if (pathKeys[linkedTool.root].has(key)) {
      throw new Error('Linked Tool closure contains a duplicate path');
    }
    pathKeys[linkedTool.root].add(key);
  }
  for (const keys of Object.values(pathKeys)) {
    for (const key of keys) {
      for (const other of keys) {
        if (key !== other && other.startsWith(`${key}.`)) {
          throw new Error('Linked Tool closure contains a callable namespace conflict');
        }
      }
    }
  }

  const modules = linkedTools.map((linkedTool, index) => ({
    ...linkedTool,
    index,
    body: linkedToolRuntimeBody(linkedTool.source, target, saves.moduleEdits[index]),
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
    ${privateToolCarrierDeclaration(saves.carrier)}
${coralRootPrelude(builtinTools, linkedTools)}
`);

  for (const module of modules) {
    append(`    __coralLinkedToolDefaults[${module.index}] = await (async () => {\n`);
    const linkedToolStart = generatedLine;
    append(module.body.text);
    for (const mapping of module.body.mappings) {
      authoredMappings.push({
        ...mapping,
        generatedLine: linkedToolStart + mapping.generatedLine,
        source: `${LINKED_ROOT_KINDS[module.root]} coral.${module.root}.${module.pathSegments.join('.')}`,
      });
    }
    append(`
    })();
    if (
      typeof __coralLinkedToolDefaults[${module.index}] !== "function"
      || typeof __coralLinkedToolDefaults[${module.index}]() !== "function"
    ) {
      throw new TypeError("Tool default export is not callable");
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
