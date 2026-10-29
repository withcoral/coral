function authoredImportNode(sourceFile) {
  if (
    sourceFile.referencedFiles.length > 0 ||
    sourceFile.typeReferenceDirectives.length > 0 ||
    sourceFile.libReferenceDirectives.length > 0
  ) {
    return sourceFile;
  }
  let found;
  function visit(node) {
    if (found !== undefined) return;
    if (
      ts.isImportDeclaration(node) ||
      ts.isImportEqualsDeclaration(node) ||
      ts.isImportTypeNode(node) ||
      (ts.isCallExpression(node) && node.expression.kind === ts.SyntaxKind.ImportKeyword) ||
      (ts.isMetaProperty(node) && node.keywordToken === ts.SyntaxKind.ImportKeyword)
    ) {
      found = node;
      return;
    }
    ts.forEachChild(node, visit);
  }
  visit(sourceFile);
  return found;
}

function forbiddenAmbientReference(program, sourceFile) {
  if (!program) return undefined;
  const checker = program.getTypeChecker();
  function isLocallyDeclared(node) {
    return (
      checker
        .getSymbolAtLocation(node)
        ?.declarations?.some((declaration) => declaration.getSourceFile() === sourceFile) === true
    );
  }
  function isAmbientGlobalThis(node) {
    return ts.isIdentifier(node) && node.text === 'globalThis' && !isLocallyDeclared(node);
  }
  let found;
  function visit(node) {
    if (found !== undefined || ts.isTypeNode(node)) return;
    if (
      ts.isPropertyAccessExpression(node) &&
      isAmbientGlobalThis(node.expression) &&
      FORBIDDEN_AMBIENT_GLOBALS.has(node.name.text)
    ) {
      found = { node: node.name, capability: node.name.text };
      return;
    }
    if (
      ts.isElementAccessExpression(node) &&
      isAmbientGlobalThis(node.expression) &&
      ts.isStringLiteral(node.argumentExpression) &&
      FORBIDDEN_AMBIENT_GLOBALS.has(node.argumentExpression.text)
    ) {
      found = { node: node.argumentExpression, capability: node.argumentExpression.text };
      return;
    }
    if (ts.isIdentifier(node) && FORBIDDEN_AMBIENT_GLOBALS.has(node.text)) {
      const parent = node.parent;
      const isLabel =
        (ts.isLabeledStatement(parent) ||
          ts.isBreakStatement(parent) ||
          ts.isContinueStatement(parent)) &&
        parent.label === node;
      const isPropertyName =
        (ts.isPropertyAccessExpression(parent) && parent.name === node) ||
        ((ts.isPropertyAssignment(parent) ||
          ts.isPropertyDeclaration(parent) ||
          ts.isPropertySignature(parent) ||
          ts.isMethodDeclaration(parent) ||
          ts.isMethodSignature(parent) ||
          ts.isGetAccessorDeclaration(parent) ||
          ts.isSetAccessorDeclaration(parent)) &&
          parent.name === node);
      if (!isLabel && !isPropertyName) {
        if (!isLocallyDeclared(node)) {
          found = { node, capability: node.text };
          return;
        }
      }
    }
    ts.forEachChild(node, visit);
  }
  visit(sourceFile);
  return found;
}

// Private compiler-binding checks and Saved Function analysis must agree about
// which AST nodes can produce runtime references. Returning false stops the walk.
function walkRuntimeNodes(root, inspectNode) {
  let stopped = false;
  function visit(node) {
    if (stopped) return;
    if (
      (node.flags & ts.NodeFlags.Ambient) !== 0 ||
      ts.isInterfaceDeclaration(node) ||
      ts.isTypeAliasDeclaration(node)
    )
      return;
    if (
      (ts.isFunctionDeclaration(node) ||
        ts.isMethodDeclaration(node) ||
        ts.isConstructorDeclaration(node) ||
        ts.isGetAccessorDeclaration(node) ||
        ts.isSetAccessorDeclaration(node)) &&
      node.body === undefined
    )
      return;
    if (ts.isExpressionWithTypeArguments(node)) {
      const heritage = node.parent;
      const owner = heritage?.parent;
      if (
        ts.isHeritageClause(heritage) &&
        heritage.token === ts.SyntaxKind.ExtendsKeyword &&
        (ts.isClassDeclaration(owner) || ts.isClassExpression(owner))
      ) {
        visit(node.expression);
      }
      return;
    }
    if (ts.isTypeNode(node)) return;
    if (inspectNode(node) === false) {
      stopped = true;
      return;
    }
    ts.forEachChild(node, visit);
  }
  visit(root);
}

function privateCompilerBindingReference(sourceFile, program) {
  if (!program) return undefined;
  const checker = program.getTypeChecker();
  let found;
  walkRuntimeNodes(sourceFile, (node) => {
    if (ts.isIdentifier(node) && node.text.startsWith('__coral') && identifierIsReference(node)) {
      const symbol = ts.isShorthandPropertyAssignment(node.parent)
        ? checker.getShorthandAssignmentValueSymbol(node.parent)
        : checker.getSymbolAtLocation(node);
      const hasRuntimeLocal =
        symbol?.declarations?.some((declaration) =>
          declarationCreatesRuntimeBinding(declaration, sourceFile),
        ) === true;
      if (!hasRuntimeLocal) {
        found = node;
        return false;
      }
    }
    return true;
  });
  return found;
}

function sourceMappingDirectivePosition(sourceFile) {
  const scanner = ts.createScanner(
    sourceFile.languageVersion,
    false,
    ts.LanguageVariant.Standard,
    sourceFile.text,
  );
  while (scanner.scan() !== ts.SyntaxKind.EndOfFileToken) {
    if (
      scanner.getToken() === ts.SyntaxKind.SingleLineCommentTrivia &&
      /^\/\/[#@]\s*source(?:Mapping)?URL\s*=/u.test(scanner.getTokenText())
    ) {
      return scanner.getTokenPos();
    }
  }
  return undefined;
}

function forbiddenCapabilityError(sourceFile, authoredMappings, program) {
  const importNode = authoredImportNode(sourceFile);
  const ambientReference = forbiddenAmbientReference(program, sourceFile);
  const privateBinding = privateCompilerBindingReference(sourceFile, program);
  const sourceDirectivePosition = sourceMappingDirectivePosition(sourceFile);
  const node = importNode ?? ambientReference?.node ?? privateBinding;
  const position = node?.getStart(sourceFile) ?? sourceDirectivePosition;
  if (position === undefined) return undefined;

  const generated = sourceFile.getLineAndCharacterOfPosition(position);
  const authored = authoredMappingAt(authoredMappings, generated.line, generated.character);
  const line = authored ? authored.line : generated.line;
  const column = authored
    ? Math.max(0, authored.column + generated.character - authored.generatedColumn)
    : generated.character;
  const capability =
    importNode !== undefined
      ? 'authored imports'
      : ambientReference !== undefined
        ? `ambient ${ambientReference.capability}`
        : privateBinding !== undefined
          ? 'private compiler bindings'
          : 'authored source mapping directives';
  return `${capability} are not available at ${line + 1}:${column + 1}`;
}
function unwrapTransparentExpression(node) {
  let current = node;
  while (
    ts.isParenthesizedExpression(current) ||
    ts.isAsExpression(current) ||
    ts.isTypeAssertionExpression(current) ||
    ts.isNonNullExpression(current) ||
    ts.isSatisfiesExpression(current)
  ) {
    current = current.expression;
  }
  return current;
}

function declarationCreatesRuntimeBinding(declaration, sourceFile) {
  if (declaration.getSourceFile() !== sourceFile) return false;
  if (
    (declaration.flags & ts.NodeFlags.Ambient) !== 0 ||
    ts.isInterfaceDeclaration(declaration) ||
    ts.isTypeAliasDeclaration(declaration) ||
    ts.isTypeParameterDeclaration(declaration) ||
    (ts.isFunctionDeclaration(declaration) && declaration.body === undefined)
  ) {
    return false;
  }
  return true;
}

function isHostCoralIdentifier(node, checker, sourceFile) {
  if (!ts.isIdentifier(node) || node.text !== 'coral') return false;
  const symbol = ts.isShorthandPropertyAssignment(node.parent)
    ? checker.getShorthandAssignmentValueSymbol(node.parent)
    : checker.getSymbolAtLocation(node);
  return (
    symbol?.declarations?.some((declaration) =>
      declarationCreatesRuntimeBinding(declaration, sourceFile),
    ) !== true
  );
}

function isHostGlobalThisIdentifier(node, checker, sourceFile) {
  if (!ts.isIdentifier(node) || node.text !== 'globalThis') return false;
  const symbol = checker.getSymbolAtLocation(node);
  return (
    symbol?.declarations?.some((declaration) =>
      declarationCreatesRuntimeBinding(declaration, sourceFile),
    ) !== true
  );
}

function constantStringValue(node, checker, sourceFile, seen = new Set()) {
  const current = unwrapTransparentExpression(node);
  if (ts.isStringLiteral(current)) return current.text;
  if (!ts.isIdentifier(current)) return undefined;
  const symbol = checker.getSymbolAtLocation(current);
  if (!symbol || seen.has(symbol)) return undefined;
  seen.add(symbol);
  const declaration = symbol.declarations?.find(
    (candidate) =>
      ts.isVariableDeclaration(candidate) &&
      candidate.getSourceFile() === sourceFile &&
      ts.isIdentifier(candidate.name) &&
      candidate.initializer !== undefined &&
      (candidate.parent.flags & ts.NodeFlags.Const) !== 0,
  );
  return declaration
    ? constantStringValue(declaration.initializer, checker, sourceFile, seen)
    : undefined;
}

function globalThisExpressionSegments(node, checker, sourceFile, seen = new Set()) {
  const current = unwrapTransparentExpression(node);
  if (isHostGlobalThisIdentifier(current, checker, sourceFile)) return [];
  if (ts.isPropertyAccessExpression(current) || ts.isElementAccessExpression(current)) {
    const parent = globalThisExpressionSegments(current.expression, checker, sourceFile, seen);
    if (parent === undefined) return undefined;
    const segment = ts.isPropertyAccessExpression(current)
      ? current.name.text
      : constantStringValue(current.argumentExpression, checker, sourceFile);
    return [...parent, segment];
  }
  if (!ts.isIdentifier(current)) return undefined;
  const symbol = checker.getSymbolAtLocation(current);
  if (!symbol || seen.has(symbol)) return undefined;
  seen.add(symbol);
  for (const declaration of symbol.declarations || []) {
    if (
      ts.isVariableDeclaration(declaration) &&
      declaration.getSourceFile() === sourceFile &&
      ts.isIdentifier(declaration.name) &&
      declaration.initializer
    ) {
      return globalThisExpressionSegments(declaration.initializer, checker, sourceFile, seen);
    }
    if (
      ts.isBindingElement(declaration) &&
      declaration.getSourceFile() === sourceFile &&
      ts.isObjectBindingPattern(declaration.parent)
    ) {
      const variable = declaration.parent.parent;
      if (ts.isVariableDeclaration(variable) && variable.initializer) {
        const parent = globalThisExpressionSegments(
          variable.initializer,
          checker,
          sourceFile,
          seen,
        );
        if (parent === undefined) return undefined;
        const property = declaration.propertyName || declaration.name;
        const segment =
          ts.isIdentifier(property) || ts.isStringLiteral(property) ? property.text : undefined;
        return [...parent, segment];
      }
    }
  }
  return undefined;
}

function coralAccess(node, checker, sourceFile) {
  const segments = [];
  let current = unwrapTransparentExpression(node);
  while (ts.isPropertyAccessExpression(current) || ts.isElementAccessExpression(current)) {
    if (ts.isPropertyAccessExpression(current)) {
      segments.unshift(current.name.text);
    } else {
      segments.unshift(constantStringValue(current.argumentExpression, checker, sourceFile));
    }
    current = unwrapTransparentExpression(current.expression);
  }
  if (!ts.isIdentifier(current)) return undefined;
  if (isHostCoralIdentifier(current, checker, sourceFile)) {
    return { root: 'lexical', segments };
  }
  const globalSegments = globalThisExpressionSegments(current, checker, sourceFile);
  if (globalSegments !== undefined) {
    const complete = [...globalSegments, ...segments];
    if (complete[0] === 'coral') {
      return { root: 'global', segments: complete.slice(1) };
    }
    if (complete[0] === undefined && complete.includes('functions')) {
      return { root: 'computed_global', segments: complete };
    }
  }
  return undefined;
}

function propertyPath(node, checker, sourceFile) {
  const access = coralAccess(node, checker, sourceFile);
  if (
    access?.root === 'lexical' &&
    access.segments[0] === 'functions' &&
    access.segments.every((segment) => segment !== undefined)
  ) {
    return access.segments.slice(1);
  }
  return undefined;
}

function parentContinuesCoralPath(node) {
  let current = node;
  while (
    current.parent &&
    (ts.isParenthesizedExpression(current.parent) ||
      ts.isAsExpression(current.parent) ||
      ts.isTypeAssertionExpression(current.parent) ||
      ts.isNonNullExpression(current.parent) ||
      ts.isSatisfiesExpression(current.parent)) &&
    current.parent.expression === current
  ) {
    current = current.parent;
  }
  return (
    (ts.isPropertyAccessExpression(current.parent) ||
      ts.isElementAccessExpression(current.parent)) &&
    current.parent.expression === current
  );
}

function identifierIsReference(node) {
  const parent = node.parent;
  if (ts.isPropertyAccessExpression(parent) && parent.name === node) return false;
  if (
    (ts.isPropertyAssignment(parent) ||
      ts.isPropertyDeclaration(parent) ||
      ts.isPropertySignature(parent) ||
      ts.isMethodDeclaration(parent) ||
      ts.isMethodSignature(parent) ||
      ts.isGetAccessorDeclaration(parent) ||
      ts.isSetAccessorDeclaration(parent) ||
      ts.isVariableDeclaration(parent) ||
      ts.isParameter(parent) ||
      ts.isFunctionDeclaration(parent) ||
      ts.isFunctionExpression(parent) ||
      ts.isClassDeclaration(parent) ||
      ts.isClassExpression(parent) ||
      ts.isInterfaceDeclaration(parent) ||
      ts.isTypeAliasDeclaration(parent) ||
      ts.isBindingElement(parent)) &&
    parent.name === node
  ) {
    return false;
  }
  return !(
    (ts.isLabeledStatement(parent) ||
      ts.isBreakStatement(parent) ||
      ts.isContinueStatement(parent)) &&
    parent.label === node
  );
}

function savedFunctionPathAnalysis(sourceFile, program, authoredMappings) {
  const checker = program.getTypeChecker();
  const references = new Map();
  let validationError;
  function reject(message, node) {
    validationError = {
      message,
      ...authoredReferenceLocation(sourceFile, node, authoredMappings),
    };
    return false;
  }
  walkRuntimeNodes(sourceFile, (node) => {
    if (
      isHostCoralIdentifier(node, checker, sourceFile) &&
      identifierIsReference(node) &&
      !parentContinuesCoralPath(node)
    ) {
      return reject('Coral namespace aliases are not supported', node);
    }
    if (ts.isPropertyAccessExpression(node) || ts.isElementAccessExpression(node)) {
      const access = coralAccess(node, checker, sourceFile);
      if (
        (access?.root === 'global' &&
          (access.segments[0] === 'functions' ||
            (access.segments.length > 0 && access.segments[0] === undefined))) ||
        access?.root === 'computed_global'
      ) {
        return reject('Saved Function paths must be directly named through coral.functions', node);
      }
      if (
        ts.isElementAccessExpression(node) &&
        access?.root === 'lexical' &&
        (access.segments[0] === 'functions' || access.segments[0] === undefined)
      ) {
        return reject('Saved Function paths must be directly named without computed access', node);
      }
    }
    if (ts.isPropertyAccessExpression(node)) {
      const path = propertyPath(node, checker, sourceFile);
      if (path !== undefined && !parentContinuesCoralPath(node)) {
        if (path.length === 0) {
          return reject('Saved Function namespace aliases are not supported', node);
        }
        const joined = path.join('.');
        if (!references.has(joined)) {
          references.set(joined, {
            path: joined,
            ...authoredReferenceLocation(sourceFile, node, authoredMappings),
          });
        }
      }
    }
    return true;
  });
  return {
    references: [...references.values()].toSorted((left, right) =>
      left.path.localeCompare(right.path),
    ),
    validationError,
  };
}

function referenceAnalysisProgram(source, fileName, targetName) {
  const compilerOptions = {
    ...PROGRAM_COMPILER_OPTIONS,
    target: resolveTypeScriptTarget(targetName),
    noLib: true,
    types: [],
  };
  const { program, sourceFile } = virtualTypeScriptProgram(fileName, source, compilerOptions);
  if (!sourceFile) throw new Error('Saved Function analysis source is missing');
  return { program, sourceFile };
}

function analyzeSavedFunctionPaths(body, fileName, targetName) {
  const wrapped = wrapProgramBody(body);
  const { program, sourceFile } = referenceAnalysisProgram(wrapped.source, fileName, targetName);
  if (!hasExpectedProgramShape(sourceFile)) {
    return {
      references: [],
      validationError: {
        message: 'Program source is not one async-function body',
        line: 1,
        column: 1,
      },
    };
  }

  return savedFunctionPathAnalysis(sourceFile, program, wrapped.authoredMappings);
}

function savedFunctionDependencyAnalysis(sourceFile, program) {
  const syntaxError = sourceFile.parseDiagnostics.find(
    (diagnostic) => diagnostic.category === ts.DiagnosticCategory.Error,
  );
  if (syntaxError !== undefined) {
    const position =
      syntaxError.start === undefined
        ? { line: 0, character: 0 }
        : sourceFile.getLineAndCharacterOfPosition(syntaxError.start);
    return {
      references: [],
      validationError: {
        message: 'Saved Function source has syntax errors under the current TypeScript Profile',
        line: position.line + 1,
        column: position.character + 1,
      },
    };
  }
  return savedFunctionPathAnalysis(sourceFile, program);
}

function analyzeSavedFunctionDependencies(source, fileName, targetName) {
  const { program, sourceFile } = referenceAnalysisProgram(source, fileName, targetName);
  return savedFunctionDependencyAnalysis(sourceFile, program);
}

// The number of segments under `coral` that name a Tool's Call Root:
// `providers.<namespace>` for an Owner's Provider, `workspace` for the
// Built-in Provider. Undefined for any other root.
function toolCallRootLength(segments) {
  if (segments[0] === 'providers') return 2;
  if (segments[0] === 'workspace') return 1;
  return undefined;
}

// Each maximal literal `coral.providers` or `coral.workspace` chain, relative
// to `coral` and cut before its first computed segment, where it starts, and
// whether it is called. A cut chain is not called: its computed segment may
// name the Tool. Aliased roots are not followed.
function toolReferenceChains(sourceFile, program) {
  const checker = program.getTypeChecker();
  const chains = [];
  walkRuntimeNodes(sourceFile, (node) => {
    if (
      (ts.isPropertyAccessExpression(node) || ts.isElementAccessExpression(node)) &&
      !parentContinuesCoralPath(node)
    ) {
      const access = coralAccess(node, checker, sourceFile);
      if (access?.root === 'lexical' && toolCallRootLength(access.segments) !== undefined) {
        const computed = access.segments.indexOf(undefined);
        const segments = access.segments.slice(0, computed === -1 ? undefined : computed);
        const called =
          computed === -1 && ts.isCallExpression(node.parent) && node.parent.expression === node;
        if (segments.length > 1) {
          chains.push({ segments, start: node.getStart(sourceFile), called });
        }
      }
    }
    return true;
  });
  return chains;
}

// Every chain prefix that could be a Call Path: a Call Root and at least one
// identifier segment.
function candidateToolPaths(chains) {
  const paths = new Set();
  for (const { segments } of chains) {
    const rootLength = toolCallRootLength(segments);
    for (let length = 2; length <= segments.length; length += 1) {
      if (!ts.isIdentifierText(segments[length - 1], ts.ScriptTarget.ES2025)) break;
      if (length > rootLength) {
        paths.add(`coral.${segments.slice(0, length).join('.')}`);
      }
    }
  }
  return [...paths].toSorted();
}

// The Saved Functions and Tools one Saved Function module names, from
// one analysis: what its typed preparation needs.
function analyzeSavedFunctionCallPaths(source, fileName, targetName) {
  const { program, sourceFile } = referenceAnalysisProgram(source, fileName, targetName);
  const dependencies = savedFunctionDependencyAnalysis(sourceFile, program);
  if (dependencies.validationError !== undefined) return dependencies;
  const chains = toolReferenceChains(sourceFile, program);
  return {
    ...dependencies,
    tools: { callPaths: candidateToolPaths(chains), chains },
  };
}
