// Authoring-only types for Middleware. Middleware reaches Provider Operations
// and Emissions through its own privileged `coral`; it has no Saved Functions.
const MIDDLEWARE_HOST_DECLARATION_FILE_NAME = "coral_middleware.d.ts";
const MIDDLEWARE_HOST_DECLARATION = `
declare namespace coral {
  const providers: any;
  /** Emits a Program Value attributed to this Middleware execution. */
  function emit(value: unknown): Promise<void>;
  class OperationError extends Error {
    private constructor();
    readonly code: string;
    readonly traceId?: string;
  }
}
declare namespace CoralMiddleware {
  /** Workspace Operation Access State. */
  type OperationPolicy = "enabled" | "approval_required" | "disabled";
  /** Mutable state private to this Middleware and one invocation scope. */
  type Context = Record<string, any>;
  interface RunContexts {
    /** This Middleware's context for the whole Run. */
    readonly run: Context;
  }
  interface FunctionContexts extends RunContexts {
    /** This Middleware's context for the current Saved Function call. */
    readonly function: Context;
  }
  interface OperationContexts extends RunContexts {
    /** This Middleware's context for the nearest enclosing Saved Function call. */
    readonly function?: Context;
    /** This Middleware's context for this Operation Invocation. */
    readonly operation: Context;
  }
  /** Immutable facts about the submitted Program. */
  interface ProgramMetadata {
    readonly runId?: string;
    readonly linkedFunctions: readonly string[];
  }
  interface RunInvocation {
    readonly kind: "run";
    readonly program: ProgramMetadata;
    readonly contexts: RunContexts;
  }
  interface FunctionInvocation {
    readonly kind: "function";
    /** Call Path, such as coral.functions.finance.tax. */
    readonly path: string;
    /** Positional arguments. Later Middleware and the call see edits. */
    args: unknown[];
    readonly contexts: FunctionContexts;
  }
  interface OperationInvocation {
    readonly kind: "operation";
    /** Call Path, such as coral.providers.linear.listIssues. */
    readonly path: string;
    /** The Operation's input object. Later Middleware and the call see edits. */
    args: any;
    /** The Run's policy for this Operation. */
    readonly originalPolicy: OperationPolicy;
    /** The policy applied immediately before execution. */
    currentPolicy: OperationPolicy;
    readonly contexts: OperationContexts;
  }
  /** A Program or Saved Function call to coral.emit. It has no policy. */
  interface EmitInvocation {
    readonly kind: "operation";
    readonly path: "coral.emit";
    /** The emitted value. Later Middleware and the Emission see edits. */
    args: unknown;
    readonly contexts: OperationContexts;
  }
  type Invocation = RunInvocation | FunctionInvocation | OperationInvocation | EmitInvocation;
  /** Runs the wrapped invocation. Call it at most once and await or return it. */
  type Next = () => Promise<unknown>;
  type Middleware = (ctx: Invocation, next: Next) => unknown;
}
`;

// Middleware module initialization must not have effects: it may only declare
// types, functions, classes without static effects, and inert constants.
function inertExpression(node) {
  const current = unwrapTransparentExpression(node);
  if (
    ts.isStringLiteral(current)
    || ts.isNumericLiteral(current)
    || ts.isBigIntLiteral(current)
    || ts.isNoSubstitutionTemplateLiteral(current)
    || ts.isRegularExpressionLiteral(current)
    || ts.isArrowFunction(current)
    || ts.isFunctionExpression(current)
    || ts.isIdentifier(current)
    || current.kind === ts.SyntaxKind.TrueKeyword
    || current.kind === ts.SyntaxKind.FalseKeyword
    || current.kind === ts.SyntaxKind.NullKeyword
  ) {
    return true;
  }
  if (
    ts.isPrefixUnaryExpression(current)
    && (current.operator === ts.SyntaxKind.MinusToken
      || current.operator === ts.SyntaxKind.PlusToken)
  ) {
    const operand = unwrapTransparentExpression(current.operand);
    return ts.isNumericLiteral(operand) || ts.isBigIntLiteral(operand);
  }
  if (ts.isArrayLiteralExpression(current)) {
    return current.elements.every((element) =>
      !ts.isSpreadElement(element) && inertExpression(element)
    );
  }
  if (ts.isObjectLiteralExpression(current)) {
    return current.properties.every((property) => {
      if (property.name && ts.isComputedPropertyName(property.name)) {
        const key = unwrapTransparentExpression(property.name.expression);
        if (!ts.isStringLiteral(key) && !ts.isNumericLiteral(key)) return false;
      }
      if (ts.isPropertyAssignment(property)) return inertExpression(property.initializer);
      return ts.isShorthandPropertyAssignment(property)
        || ts.isMethodDeclaration(property)
        || ts.isGetAccessorDeclaration(property)
        || ts.isSetAccessorDeclaration(property);
    });
  }
  return false;
}

function inertClass(node) {
  if (ts.getDecorators?.(node)?.length > 0) return false;
  if (node.heritageClauses?.some((clause) =>
    clause.token === ts.SyntaxKind.ExtendsKeyword
  )) {
    return false;
  }
  return node.members.every((member) => {
    if (ts.isClassStaticBlockDeclaration(member)) return false;
    if (ts.getDecorators?.(member)?.length > 0) return false;
    if (member.name && ts.isComputedPropertyName(member.name)) {
      const key = unwrapTransparentExpression(member.name.expression);
      if (!ts.isStringLiteral(key) && !ts.isNumericLiteral(key)) return false;
    }
    const isStatic = member.modifiers?.some((modifier) =>
      modifier.kind === ts.SyntaxKind.StaticKeyword
    ) === true;
    if (ts.isPropertyDeclaration(member) && isStatic && member.initializer) {
      return inertExpression(member.initializer);
    }
    return true;
  });
}

function middlewareTopLevelEffect(sourceFile) {
  for (const statement of sourceFile.statements) {
    if (
      ts.isInterfaceDeclaration(statement)
      || ts.isTypeAliasDeclaration(statement)
      || ts.isEmptyStatement(statement)
      || (statement.flags & ts.NodeFlags.Ambient) !== 0
      || statement.modifiers?.some((modifier) =>
        modifier.kind === ts.SyntaxKind.DeclareKeyword
      ) === true
      || ts.isFunctionDeclaration(statement)
    ) {
      continue;
    }
    if (ts.isClassDeclaration(statement) && inertClass(statement)) continue;
    if (
      ts.isVariableStatement(statement)
      && statement.declarationList.declarations.every((declaration) =>
        ts.isIdentifier(declaration.name)
        && (declaration.initializer === undefined || inertExpression(declaration.initializer))
      )
    ) {
      continue;
    }
    if (
      ts.isExportAssignment(statement)
      && !statement.isExportEquals
      && inertExpression(statement.expression)
    ) {
      continue;
    }
    if (ts.isExportDeclaration(statement) && !statement.moduleSpecifier) continue;
    return statement;
  }
  return undefined;
}

// Middleware cannot reference Saved Functions or reach the Program's coral.
function middlewareNamespaceMisuse(sourceFile, program) {
  const analysis = savedFunctionPathAnalysis(sourceFile, program);
  if (analysis.validationError !== undefined) {
    return analysis.validationError.message.replace(
      "Saved Function",
      "Middleware Saved Function",
    ) + ` at ${analysis.validationError.line}:${analysis.validationError.column}`;
  }
  if (analysis.references.length > 0) {
    const reference = analysis.references[0];
    return `Middleware cannot reference Saved Functions at ${reference.line}:${reference.column}`;
  }
  const checker = program.getTypeChecker();
  let misuse;
  walkRuntimeNodes(sourceFile, (node) => {
    if (ts.isPropertyAccessExpression(node) || ts.isElementAccessExpression(node)) {
      const access = coralAccess(node, checker, sourceFile);
      const member = access?.segments[0];
      if (
        access !== undefined
        && (member === "functions" || access.root === "global")
      ) {
        const location = authoredReferenceLocation(sourceFile, node);
        misuse = member === "functions"
          ? `Middleware cannot reference Saved Functions at ${location.line}:${location.column}`
          : `Middleware must use its own coral namespace at ${location.line}:${location.column}`;
        return false;
      }
    }
    return true;
  });
  return misuse;
}

function middlewareValidationError(sourceFile, program) {
  const capabilityError = forbiddenCapabilityError(sourceFile, undefined, program);
  if (capabilityError !== undefined) return capabilityError;
  const misuse = middlewareNamespaceMisuse(sourceFile, program);
  if (misuse !== undefined) return misuse;
  const effect = middlewareTopLevelEffect(sourceFile);
  if (effect !== undefined) {
    const location = authoredReferenceLocation(sourceFile, effect);
    return `Middleware module top-level code must not have effects at ${location.line}:${location.column}`;
  }
  return undefined;
}

function middlewareProgram(source, fileName, targetName) {
  const compilerOptions = {
    ...PROGRAM_COMPILER_OPTIONS,
    target: resolveTypeScriptTarget(targetName),
    module: ts.ModuleKind.ESNext,
    isolatedModules: false,
    noEmit: true,
    sourceMap: false,
    skipLibCheck: true,
    types: [],
  };
  return virtualTypeScriptProgram(
    fileName,
    source,
    compilerOptions,
    new Map(),
    {[MIDDLEWARE_HOST_DECLARATION_FILE_NAME]: MIDDLEWARE_HOST_DECLARATION},
  );
}

function prepareMiddleware(source, fileName, targetName) {
  const {program, sourceFile} = middlewareProgram(source, fileName, targetName);
  if (!sourceFile) throw new Error("Middleware virtual source is missing");
  const syntacticDiagnostics = program.getSyntacticDiagnostics(sourceFile);
  if (syntacticDiagnostics.some((diagnostic) =>
    diagnostic.category === ts.DiagnosticCategory.Error
  )) {
    return {
      validationError: null,
      blockingDiagnostics: syntacticDiagnostics.map((diagnostic) =>
        diagnosticPayload(diagnostic)
      ),
      diagnostics: [],
    };
  }
  const validationError = middlewareValidationError(sourceFile, program)
    ?? validateDefaultCallable(program, sourceFile, "Middleware");
  return {
    validationError: validationError ?? null,
    blockingDiagnostics: [],
    diagnostics: validationError === undefined
      ? program.getSemanticDiagnostics(sourceFile).map((diagnostic) =>
        diagnosticPayload(diagnostic)
      )
      : [],
  };
}
