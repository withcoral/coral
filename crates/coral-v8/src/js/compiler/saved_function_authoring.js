function hasModifierInAncestors(node, kind, stop) {
  let current = node;
  while (current && current !== stop) {
    if (current.modifiers?.some((modifier) => modifier.kind === kind) === true) {
      return true;
    }
    current = current.parent;
  }
  return false;
}

function hasTypeOnlyDefaultExport(sourceFile) {
  return sourceFile.statements.some(
    (statement) =>
      ts.isExportDeclaration(statement) &&
      statement.exportClause &&
      ts.isNamedExports(statement.exportClause) &&
      statement.exportClause.elements.some(
        (element) =>
          element.name.text === 'default' && (statement.isTypeOnly || element.isTypeOnly),
      ),
  );
}

function validateDefaultCallable(program, sourceFile) {
  if (hasTypeOnlyDefaultExport(sourceFile)) {
    return 'Saved Function default export must be a runtime value';
  }
  const checker = program.getTypeChecker();
  const moduleSymbol = checker.getSymbolAtLocation(sourceFile);
  if (!moduleSymbol) {
    return 'Saved Function source must be a TypeScript module with one default export';
  }
  const exports = checker.getExportsOfModule(moduleSymbol);
  if (exports.length !== 1 || exports[0].escapedName !== 'default') {
    return 'Saved Function source must export exactly one default callable';
  }
  let defaultExport = exports[0];
  if (defaultExport.flags & ts.SymbolFlags.Alias) {
    defaultExport = checker.getAliasedSymbol(defaultExport);
  }
  const declaration =
    defaultExport.declarations?.find(
      (candidate) => !ts.isFunctionDeclaration(candidate) || candidate.body !== undefined,
    ) ||
    defaultExport.valueDeclaration ||
    defaultExport.declarations?.[0] ||
    sourceFile;
  if (
    declaration === sourceFile ||
    hasModifierInAncestors(declaration, ts.SyntaxKind.DeclareKeyword, sourceFile) ||
    (ts.isFunctionDeclaration(declaration) && !declaration.body)
  ) {
    return 'Saved Function default export must have a runtime implementation';
  }
  const type = checker.getTypeOfSymbolAtLocation(defaultExport, declaration);
  if (checker.getSignaturesOfType(type, ts.SignatureKind.Call).length === 0) {
    return 'Saved Function default export must be callable';
  }
  return undefined;
}

const UNRESOLVED_DECLARATION_DIAGNOSTIC_CODES = new Set([
  2304, // Cannot find name.
  2307, // Cannot find module.
  2503, // Cannot find namespace.
  2552, // Cannot find name; did you mean ...
  2580, // Cannot find name (host-specific common names).
  2688, // Cannot find type definition file.
  2694, // Namespace has no exported member.
]);

function analyzeDeclaration(declaration, fileName, compilerOptions) {
  const outputs = new Map();
  const declarationName = fileName.replace(/\.tsx?$/u, '.d.ts');
  const declarationOptions = {
    ...compilerOptions,
    noEmit: true,
    skipLibCheck: false,
  };
  const { program, sourceFile } = virtualTypeScriptProgram(
    declarationName,
    declaration,
    declarationOptions,
    outputs,
  );
  if (!sourceFile) {
    throw new Error('declaration validation source is missing');
  }
  const unresolvedDiagnostics = program
    .getSemanticDiagnostics(sourceFile)
    .filter((diagnostic) => UNRESOLVED_DECLARATION_DIAGNOSTIC_CODES.has(diagnostic.code));
  if (unresolvedDiagnostics.length > 0) {
    return { unresolvedDiagnostics, declarationFields: [] };
  }
  try {
    return {
      unresolvedDiagnostics,
      declarationFields: projectDeclarationFields(program, sourceFile),
    };
  } catch {
    return {
      unresolvedDiagnostics,
      declarationFields: undefined,
    };
  }
}

function declarationFieldPath(parent, name) {
  if (ts.isIdentifier(name)) return `${parent}.${name.text}`;
  if (ts.isStringLiteral(name)) return `${parent}[${JSON.stringify(name.text)}]`;
  if (ts.isNumericLiteral(name)) return `${parent}[${name.text}]`;
  return undefined;
}

function projectDeclarationFields(program, sourceFile) {
  const checker = program.getTypeChecker();
  const moduleSymbol = checker.getSymbolAtLocation(sourceFile);
  if (!moduleSymbol) throw new Error('declaration module symbol is missing');
  const exported = checker
    .getExportsOfModule(moduleSymbol)
    .find((symbol) => symbol.escapedName === 'default');
  if (!exported) throw new Error('declaration default export is missing');
  const defaultSymbol =
    exported.flags & ts.SymbolFlags.Alias ? checker.getAliasedSymbol(exported) : exported;
  const declaration = defaultSymbol.valueDeclaration || defaultSymbol.declarations?.[0];
  if (!declaration) throw new Error('declaration default export has no declaration');
  const callable = checker.getTypeOfSymbolAtLocation(defaultSymbol, declaration);
  const signatures = checker.getSignaturesOfType(callable, ts.SignatureKind.Call);
  if (signatures.length === 0) throw new Error('declaration default export is not callable');

  const fields = new Map();
  const MAX_DEPTH = 64;
  const MAX_FIELDS = 4096;
  const MAX_PATH_UNITS = 1024;
  const MAX_TEXT_UNITS = 16 * 1024;
  const MAX_TOTAL_UNITS = 2 * 1024 * 1024;
  let projectedUnits = 0;

  function recordDeclarationField(path, nodes, type) {
    if (!path) return;
    if (path.length > MAX_PATH_UNITS) {
      throw new Error('declaration field path exceeds projection bound');
    }
    const parts = [path];
    for (const node of nodes) {
      if (node.name) parts.push(node.name.getText(sourceFile));
      for (const tag of ts.getJSDocCommentsAndTags(node)) {
        parts.push(tag.getText(sourceFile));
      }
    }
    parts.push(checker.typeToString(type, sourceFile, ts.TypeFormatFlags.NoTruncation));
    for (const node of type.symbol?.declarations || []) {
      if (node.getSourceFile() === sourceFile && ts.isEnumDeclaration(node)) {
        parts.push(node.getText(sourceFile));
      }
    }
    const text = [...new Set(parts.filter((part) => part.length > 0))].join('\n');
    const previous = fields.get(path);
    const merged =
      previous === undefined ? text : [...new Set(`${previous}\n${text}`.split('\n'))].join('\n');
    if (merged.length > MAX_TEXT_UNITS || (previous === undefined && fields.size >= MAX_FIELDS)) {
      throw new Error('declaration fields exceed projection bound');
    }
    projectedUnits += path.length + merged.length;
    if (previous !== undefined) projectedUnits -= path.length + previous.length;
    if (projectedUnits > MAX_TOTAL_UNITS) {
      throw new Error('declaration fields exceed projection bound');
    }
    fields.set(path, merged);
  }

  function visitType(type, path, resolving = new Set(), depth = 0) {
    if (depth > MAX_DEPTH || resolving.has(type)) return;
    const nextResolving = new Set(resolving).add(type);
    const visit = (child, childPath = path) =>
      visitType(child, childPath, nextResolving, depth + 1);
    if (type.flags & ts.TypeFlags.TypeParameter) {
      const constraint = checker.getBaseConstraintOfType(type);
      if (constraint) visit(constraint);
    } else if (type.isUnionOrIntersection()) {
      for (const branch of type.types) visit(branch);
    } else if (checker.isArrayType(type) || checker.isTupleType(type)) {
      const element = checker.getIndexTypeOfType(type, ts.IndexKind.Number);
      if (element) {
        recordDeclarationField(`${path}[]`, [], element);
        visit(element, `${path}[]`);
      }
    } else if (type.flags & ts.TypeFlags.Object) {
      // A nested callable is a field, not another input/result root. Do not
      // expand standard-library members into authored declaration evidence.
      if (checker.getSignaturesOfType(type, ts.SignatureKind.Call).length > 0) return;
      for (const property of checker.getPropertiesOfType(type)) {
        const nodes = (property.declarations || []).filter(
          (node) => node.getSourceFile() === sourceFile,
        );
        if (
          nodes.some(
            (node) =>
              (node.name && ts.isPrivateIdentifier(node.name)) ||
              ts.getCombinedModifierFlags(node) &
                (ts.ModifierFlags.Private | ts.ModifierFlags.Protected),
          )
        )
          continue;
        const name = property.getName();
        if (name.startsWith('__@')) continue;
        if (nodes.length === 0 && property.declarations?.length > 0) continue;
        const childPath =
          nodes[0]?.name?.text === name
            ? declarationFieldPath(path, nodes[0].name)
            : ts.isIdentifierText(name, sourceFile.languageVersion)
              ? `${path}.${name}`
              : `${path}[${JSON.stringify(name)}]`;
        if (childPath === undefined) continue;
        const child = checker.getTypeOfSymbolAtLocation(property, sourceFile);
        recordDeclarationField(childPath, nodes, child);
        visit(child, childPath);
      }
      for (const info of checker.getIndexInfosOfType(type)) {
        if (info.keyType.flags & ts.TypeFlags.ESSymbolLike) continue;
        if (info.declaration && info.declaration.getSourceFile() !== sourceFile) continue;
        const childPath = `${path}[key]`;
        recordDeclarationField(childPath, info.declaration ? [info.declaration] : [], info.type);
        visit(info.type, childPath);
      }
    }
  }

  for (const signature of signatures) {
    for (const [index, parameter] of signature.getParameters().entries()) {
      const node = parameter.valueDeclaration || parameter.declarations?.[0];
      if (!node) continue;
      const path = ts.isIdentifier(node.name) ? `input.${node.name.text}` : `input[${index}]`;
      const type = checker.getTypeOfSymbolAtLocation(parameter, node);
      recordDeclarationField(node.dotDotDotToken ? `${path}[]` : path, [node], type);
      visitType(type, path);
    }
    const result = checker.getReturnTypeOfSignature(signature);
    const node = signature.getDeclaration();
    recordDeclarationField('result', node ? [node] : [], result);
    visitType(checker.getAwaitedType(result) || result, 'result');
  }
  return [...fields]
    .map(([path, text]) => ({ path, text }))
    .toSorted((left, right) => left.path.localeCompare(right.path));
}

function utf8Length(text) {
  let length = 0;
  for (const character of text) {
    const code = character.codePointAt(0);
    length += code < 0x80 ? 1 : code < 0x800 ? 2 : code < 0x10000 ? 3 : 4;
  }
  return length;
}

// Emits one declaration and checks that it names nothing outside itself.
function emitSavedFunctionDeclaration(program, fileName, compilerOptions) {
  const outputs = new Map();
  const emitResult = program.emit(
    undefined,
    (outputName, text) => outputs.set(normalizedFileName(outputName), text),
    undefined,
    true,
  );
  const blockingDiagnostics = emitResult.diagnostics.filter(
    (diagnostic) => diagnostic.category === ts.DiagnosticCategory.Error,
  );
  const declarationName = normalizedFileName(fileName).replace(/\.tsx?$/u, '.d.ts');
  const declarationText = outputs.get(declarationName);
  if (blockingDiagnostics.length > 0 || declarationText === undefined) {
    return { declarationText, blockingDiagnostics };
  }
  return {
    declarationText,
    blockingDiagnostics,
    analysis: analyzeDeclaration(declarationText, fileName, compilerOptions),
  };
}

// Why a typed declaration cannot be saved, as advisory diagnostics, or
// undefined when it can. A name the declaration cannot resolve is located in
// the declaration, not the source, so it is reported at the export.
function typedDeclarationFailure(emitted, sourceFile, declarationMaxBytes) {
  const exported = sourceFile.statements.find(
    (statement) =>
      ts.isExportAssignment(statement) ||
      statement.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.DefaultKeyword),
  );
  const atExport = (diagnostic) =>
    diagnosticPayload({
      ...diagnostic,
      file: sourceFile,
      start: exported?.getStart(sourceFile),
    });
  if (emitted.blockingDiagnostics.length > 0 || emitted.declarationText === undefined) {
    return emitted.blockingDiagnostics.map((diagnostic) => diagnosticPayload(diagnostic));
  }
  if (emitted.analysis.unresolvedDiagnostics.length > 0) {
    return emitted.analysis.unresolvedDiagnostics.map(atExport);
  }
  if (utf8Length(emitted.declarationText) > declarationMaxBytes) {
    return [
      atExport({
        category: ts.DiagnosticCategory.Error,
        code: DECLARATION_TOO_LARGE_DIAGNOSTIC_CODE,
        messageText: `The typed declaration exceeds ${declarationMaxBytes} bytes, so its result is saved as any. Return fewer fields, or annotate the return type.`,
      }),
    ];
  }
  return undefined;
}

function savedFunctionDeclarationResult(emitted, diagnostics, references) {
  const { declarationText, blockingDiagnostics, analysis } = emitted;
  if (blockingDiagnostics.length > 0 || declarationText === undefined) {
    return {
      declarationText: null,
      validationError:
        declarationText === undefined ? 'TypeScript emitted no Saved Function declaration' : null,
      blockingDiagnostics: blockingDiagnostics.map((diagnostic) => diagnosticPayload(diagnostic)),
      diagnostics,
      references,
    };
  }
  const unresolved = analysis.unresolvedDiagnostics;
  return {
    declarationText: unresolved.length === 0 ? declarationText : null,
    declarationFields: analysis.declarationFields,
    declarationProjectionError:
      analysis.declarationFields === undefined ? 'declaration_field_projection_failed' : null,
    validationError:
      unresolved.length === 0
        ? null
        : 'Saved Function declaration contains unresolved public type names',
    blockingDiagnostics: unresolved.map((diagnostic) => diagnosticPayload(diagnostic)),
    diagnostics,
    references,
  };
}

// Prepares one Saved Function, typed by `declarations` when they are given.
function prepareSavedFunction(source, declarations, fileName, targetName) {
  const compilerOptions = {
    ...PROGRAM_COMPILER_OPTIONS,
    target: resolveTypeScriptTarget(targetName),
    module: ts.ModuleKind.ESNext,
    isolatedModules: false,
    declaration: true,
    emitDeclarationOnly: true,
    sourceMap: false,
    skipLibCheck: true,
    types: [],
  };
  const checked = typedVirtualProgram(fileName, source, compilerOptions, declarations);
  const { program, sourceFile } = checked;

  const syntacticDiagnostics = program.getSyntacticDiagnostics(sourceFile);
  if (
    syntacticDiagnostics.some((diagnostic) => diagnostic.category === ts.DiagnosticCategory.Error)
  ) {
    return {
      declarationText: null,
      validationError: null,
      blockingDiagnostics: syntacticDiagnostics.map((diagnostic) => diagnosticPayload(diagnostic)),
      diagnostics: [],
      references: [],
    };
  }
  const capabilityError = forbiddenCapabilityError(sourceFile, undefined, program);
  if (capabilityError !== undefined) {
    return {
      declarationText: null,
      validationError: capabilityError,
      blockingDiagnostics: [],
      diagnostics: [],
      references: [],
    };
  }
  const pathAnalysis = savedFunctionPathAnalysis(sourceFile, program);
  if (pathAnalysis.validationError !== undefined) {
    return {
      declarationText: null,
      validationError: null,
      referenceError: pathAnalysis.validationError,
      blockingDiagnostics: [],
      diagnostics: [],
      references: [],
    };
  }
  const validationError = validateDefaultCallable(program, sourceFile);
  if (validationError !== undefined) {
    return {
      declarationText: null,
      validationError,
      blockingDiagnostics: [],
      diagnostics: checked.advisoryDiagnostics(),
      references: pathAnalysis.references,
    };
  }

  const emitted = emitSavedFunctionDeclaration(program, fileName, compilerOptions);
  const failure =
    declarations === null
      ? undefined
      : typedDeclarationFailure(emitted, sourceFile, declarations.declarationMaxBytes);
  if (failure === undefined) {
    return savedFunctionDeclarationResult(
      emitted,
      checked.advisoryDiagnostics(),
      pathAnalysis.references,
    );
  }
  // Typing never fails a save. A typed declaration that cannot be saved is
  // emitted again untyped, as it was before callers supplied declarations,
  // and the diagnostics say why.
  const untyped = typedVirtualProgram(fileName, source, compilerOptions, null);
  return savedFunctionDeclarationResult(
    emitSavedFunctionDeclaration(untyped.program, fileName, compilerOptions),
    checked.advisoryDiagnostics(failure),
    pathAnalysis.references,
  );
}
