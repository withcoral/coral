const PROGRAM_COMPILER_OPTIONS = Object.freeze({
  target: ts.ScriptTarget.ES2025,
  module: ts.ModuleKind.ESNext,
  strict: true,
  alwaysStrict: true,
  isolatedModules: true,
  useDefineForClassFields: true,
  newLine: ts.NewLineKind.LineFeed,
  sourceMap: true,
  inlineSourceMap: false,
  inlineSources: false,
  removeComments: false,
  importHelpers: false,
  noEmitHelpers: false,
});

// Authoring-only types for the host root. Callers supply the declarations of
// the Tools and Private Tools a source names; a root without them is
// any. Tools type `coral.providers` and the Built-in Tools on `coral`
// together.
const CORAL_HOST_DECLARATION_FILE_NAME = 'coral.d.ts';
// Coral's diagnostic codes are outside TypeScript's range.
// A called Tool path that names no Tool the caller can call.
const UNKNOWN_TOOL_DIAGNOSTIC_CODE = 100001;
// A typed Private Tool declaration saved untyped because it would exceed
// the declaration limit.
const DECLARATION_TOO_LARGE_DIAGNOSTIC_CODE = 100002;

// `coral` is a constant of an object type, not a namespace of constants, so
// `coral.private` type-checks although `private` is a strict-mode reserved
// word. A namespace of types alone merges with it, so `coral.ToolError` also
// names the error's type. Its members are exactly the Program host's: any
// other member is an error where the source names it.
function coralHostDeclaration({ builtins, providers, privateTools, sharedTools }) {
  return `
declare namespace coral {
  /**
   * Rejects a Tool call that produced no declared success. When Coral
   * rejects the upstream's response, \`data\` and \`metadata\` hold it, and
   * \`status\` its HTTP status. Without \`data\`, it carries only a Coral
   * failure code, and the call may or may not have taken effect.
   */
  interface ToolError extends Error {
    readonly code: string;
    readonly traceId?: string;
    readonly status?: number;
    readonly data?: unknown;
    readonly metadata?: unknown;
    /** For \`tool_not_found\`, up to 3 Call Paths you can call, nearest first. */
    readonly suggestions?: readonly string[];
  }
}

declare const coral: {
  ${builtins}
  readonly providers: ${providers};
  readonly private: ${privateTools};
  readonly shared: ${sharedTools};
  readonly emit: (value: unknown) => Promise<void>;
  readonly ToolError: abstract new (...args: never) => coral.ToolError;
};

declare const console: {
  readonly debug: (...values: unknown[]) => void;
  readonly info: (...values: unknown[]) => void;
  readonly log: (...values: unknown[]) => void;
  readonly warn: (...values: unknown[]) => void;
  readonly error: (...values: unknown[]) => void;
};
`;
}

function callableNode() {
  return { type: undefined, children: new Map() };
}

function insertCallable(root, segments, type) {
  let node = root;
  for (const segment of segments) {
    if (!node.children.has(segment)) node.children.set(segment, callableNode());
    node = node.children.get(segment);
  }
  node.type = type;
}

// An open namespace is any past its known members, so a path that literal
// analysis cannot see, such as one reached through an alias, is untyped
// rather than wrong.
function renderCallableNode(node, open) {
  const members = [...node.children].map(
    ([segment, child]) =>
      `readonly ${JSON.stringify(segment)}: ${renderCallableNode(child, open)};`,
  );
  if (open) members.push('readonly [segment: string]: any;');
  const object = `{ ${members.join(' ')} }`;
  if (node.type === undefined) return object;
  return node.children.size === 0 ? node.type : `(${node.type}) & ${object}`;
}

// Each supplied declaration is an ambient module named by its Call Path, so
// the named types of two callables never collide and a diagnostic that names
// the module names the Call Path. `export {}` keeps those names private, so
// an emitted Private Tool declaration inlines them or fails to name them.
// A Call Path cannot contain `-`, so module resolution never finds the file
// that declares it and loads it a second time.
// `callable` types each callable from the type of its declared default.
function typedRoot(sources, prefix, entries, callable = (declared) => declared) {
  if (entries == null) return undefined;
  const tree = callableNode();
  for (const { pathSegments, declaration } of entries) {
    const callPath = `${prefix}.${pathSegments.join('.')}`;
    const module = JSON.stringify(callPath);
    sources[`declaration-${callPath}.d.ts`] =
      `declare module ${module} {\n${declaration}\nexport {};\n}\n`;
    insertCallable(tree, pathSegments, callable(`typeof import(${module}).default`));
  }
  return tree;
}

// A Private Tool call is a Tool Invocation, so it returns a Promise of what
// the function it declares returns, whether or not that function is async.
function privateToolCallable(declared) {
  return `(${declared} extends (...args: infer A) => infer R ? (...args: A) => Promise<Awaited<R>> : never)`;
}

// The called chains whose first prefix past their Call Root is neither a
// declared Call Path nor a namespace of one: they name nothing the caller can
// call. Their open namespaces make them any, so one Coral diagnostic replaces
// every cascading TypeScript one. A resolved sibling must not make a callable
// namespace look absent. A chain that is not called may be a namespace whose
// Tools an alias reaches, so it is not reported. With `namespaceCalls`, a
// called chain that ends at a namespace is reported too; typing reports
// that call as not callable itself.
function unknownTools(tools, { namespaceCalls = false } = {}) {
  const resolved = new Set();
  const namespaces = new Set();
  for (const { pathSegments } of tools.declarations) {
    resolved.add(pathSegments.join('.'));
    for (let length = 1; length < pathSegments.length; length += 1) {
      namespaces.add(pathSegments.slice(0, length).join('.'));
    }
  }
  const unknown = [];
  for (const { segments, start, line, column, called } of tools.chains) {
    if (!called) continue;
    const shortest = shortestToolChain(toolCallRootLength(segments));
    for (let length = shortest; length <= segments.length; length += 1) {
      const key = segments.slice(0, length).join('.');
      if (resolved.has(key)) break;
      if (namespaces.has(key) && (length < segments.length || !namespaceCalls)) continue;
      unknown.push({ start, line, column, segments, callPath: `coral.${segments.join('.')}` });
      break;
    }
  }
  return unknown;
}

// The called chains that name no Tool among `callPaths`, the complete
// Call Paths the caller can call: the rule of the unknown-Tool diagnostic,
// which Program Admission applies before a Run is created. A call to a
// namespace of a Tool fails with `tool_not_found` when it runs, so it is
// unknown too.
function unknownToolCalls(chains, callPaths) {
  const declarations = callPaths.map((callPath) => ({
    pathSegments: callPath.split('.').slice(1),
  }));
  return unknownTools({ chains, declarations }, { namespaceCalls: true }).map(
    ({ callPath, line, column }) => ({
      callPath,
      line,
      column,
    }),
  );
}

// The declaration files that type `coral`, and the Tool chains that name
// nothing the caller can call. Each of `builtinTools` is a member of `coral`,
// typed by its declaration or any without one. Without `declarations`, or
// without one root's, that root is any.
function coralSurface(builtinTools, declarations) {
  const sources = {};
  // Tool paths are relative to `coral`, so one tree holds both
  // `providers` and the Built-in Tools.
  const tools = typedRoot(sources, 'coral', declarations?.tools?.declarations);
  const privateTools = typedRoot(
    sources,
    'coral.private',
    declarations?.privateTools,
    privateToolCallable,
  );
  // A Shared Tool call is a Tool Invocation too.
  const sharedTools = typedRoot(
    sources,
    'coral.shared',
    declarations?.sharedTools,
    privateToolCallable,
  );
  const builtins = builtinTools.map((key) => {
    const declared = tools?.children.get(key);
    return `readonly ${JSON.stringify(key)}: ${declared === undefined ? 'any' : renderCallableNode(declared, true)};`;
  });
  sources[CORAL_HOST_DECLARATION_FILE_NAME] = coralHostDeclaration({
    builtins: builtins.join('\n  '),
    providers:
      tools === undefined
        ? 'any'
        : renderCallableNode(tools.children.get('providers') ?? callableNode(), true),
    privateTools: privateTools === undefined ? 'any' : renderCallableNode(privateTools, false),
    sharedTools: sharedTools === undefined ? 'any' : renderCallableNode(sharedTools, false),
  });
  // A member of `coral` that is no Built-in Tool is a TypeScript error
  // already, so it needs no Coral diagnostic too.
  const builtinKeys = new Set(builtinTools);
  const unknown =
    tools === undefined
      ? []
      : unknownTools(declarations.tools).filter(
          ({ segments }) => segments[0] === 'providers' || builtinKeys.has(segments[0]),
        );
  return { sources, unknownTools: unknown };
}

function diagnosticSortPosition(value) {
  return value ?? Number.MAX_SAFE_INTEGER;
}

function sortedDiagnostics(diagnostics) {
  return diagnostics.toSorted(
    (left, right) =>
      diagnosticSortPosition(left.line) - diagnosticSortPosition(right.line) ||
      diagnosticSortPosition(left.column) - diagnosticSortPosition(right.column),
  );
}

let typeScriptLibrarySources;
// Library files never change, so every program shares one parsed and bound
// copy, as the TypeScript language service does.
const librarySourceFiles = new Map();

function configureTypeScriptLibraries(librarySources) {
  typeScriptLibrarySources = librarySources;
}

function resolveTypeScriptTarget(targetName) {
  const target = ts.ScriptTarget[targetName];
  if (typeof target !== 'number') {
    throw new Error(`unknown TypeScript ScriptTarget: ${targetName}`);
  }
  return target;
}

function normalizedFileName(fileName) {
  return fileName.replaceAll('\\', '/').split('/').pop();
}

function librarySourceFile(fileName, normalized, languageVersion) {
  const version =
    typeof languageVersion === 'object' ? languageVersion.languageVersion : languageVersion;
  const key = `${version}:${normalized}`;
  let file = librarySourceFiles.get(key);
  if (file === undefined) {
    file = ts.createSourceFile(
      fileName,
      typeScriptLibrarySources[normalized],
      languageVersion,
      true,
    );
    librarySourceFiles.set(key, file);
  }
  return file;
}

function compilerHost(sourceFileName, source, compilerOptions, outputs, ambientSources) {
  const sourceNames = new Set([sourceFileName, normalizedFileName(sourceFileName)]);
  const supportingSource = (normalized) =>
    ambientSources[normalized] ?? typeScriptLibrarySources[normalized];
  return {
    getSourceFile(fileName, languageVersion) {
      const normalized = normalizedFileName(fileName);
      if (
        !sourceNames.has(fileName) &&
        !sourceNames.has(normalized) &&
        ambientSources[normalized] === undefined &&
        typeScriptLibrarySources[normalized] !== undefined
      ) {
        return librarySourceFile(fileName, normalized, languageVersion);
      }
      const text =
        sourceNames.has(fileName) || sourceNames.has(normalized)
          ? source
          : supportingSource(normalized);
      if (text === undefined) return undefined;
      return ts.createSourceFile(fileName, text, languageVersion, true);
    },
    getDefaultLibFileName() {
      return 'lib.es2025.d.ts';
    },
    writeFile(fileName, text) {
      outputs.set(normalizedFileName(fileName), text);
    },
    getCurrentDirectory() {
      return '/';
    },
    getCanonicalFileName(fileName) {
      return fileName;
    },
    useCaseSensitiveFileNames() {
      return true;
    },
    getNewLine() {
      return '\n';
    },
    fileExists(fileName) {
      const normalized = normalizedFileName(fileName);
      return (
        sourceNames.has(fileName) ||
        sourceNames.has(normalized) ||
        supportingSource(normalized) !== undefined
      );
    },
    readFile(fileName) {
      const normalized = normalizedFileName(fileName);
      if (sourceNames.has(fileName) || sourceNames.has(normalized)) return source;
      return supportingSource(normalized);
    },
    directoryExists() {
      return true;
    },
    getDirectories() {
      return [];
    },
  };
}

function virtualTypeScriptProgram(
  sourceFileName,
  source,
  compilerOptions,
  outputs = new Map(),
  ambientSources = {},
) {
  const host = compilerHost(sourceFileName, source, compilerOptions, outputs, ambientSources);
  const program = ts.createProgram(
    [sourceFileName, ...Object.keys(ambientSources)],
    compilerOptions,
    host,
  );
  return {
    host,
    program,
    sourceFile: program.getSourceFile(sourceFileName),
  };
}

// One source typed by `declarations`, or untyped without them. `coral` holds
// `builtinTools` either way.
function typedVirtualProgram(fileName, source, compilerOptions, builtinTools, declarations) {
  const surface = coralSurface(builtinTools, declarations);
  const typed = virtualTypeScriptProgram(
    fileName,
    source,
    compilerOptions,
    undefined,
    surface.sources,
  );
  const { program, sourceFile } = typed;
  if (!sourceFile) throw new Error('typed virtual source is missing');
  const unknownToolDiagnostics = surface.unknownTools.map(({ start, callPath }) =>
    diagnosticPayload({
      file: sourceFile,
      start,
      category: ts.DiagnosticCategory.Error,
      code: UNKNOWN_TOOL_DIAGNOSTIC_CODE,
      messageText: `${callPath} names no Tool that you can call.`,
    }),
  );
  return {
    ...typed,
    advisoryDiagnostics(extra = []) {
      return sortedDiagnostics([
        ...program
          .getSemanticDiagnostics(sourceFile)
          .map((diagnostic) => diagnosticPayload(diagnostic)),
        ...unknownToolDiagnostics,
        ...extra,
      ]);
    },
  };
}
