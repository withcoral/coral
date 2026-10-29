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
// the Tools and Saved Functions a source names; a root without them is
// any. Tools type `coral.providers` and `coral.workspace` together.
const CORAL_HOST_DECLARATION_FILE_NAME = 'coral.d.ts';
// Coral's diagnostic codes are outside TypeScript's range.
// A called `coral.providers` or `coral.workspace` path that names no
// Tool the caller can call.
const UNKNOWN_TOOL_DIAGNOSTIC_CODE = 100001;
// A typed Saved Function declaration saved untyped because it would exceed
// the declaration limit.
const DECLARATION_TOO_LARGE_DIAGNOSTIC_CODE = 100002;

function coralHostDeclaration(providersType, workspaceType, functionsType) {
  return `
declare namespace coral {
  const providers: ${providersType};
  const workspace: ${workspaceType};
  const functions: ${functionsType};
  function emit(value: unknown): Promise<void>;
  /**
   * Rejects a Tool call that produced no declared success. When Coral
   * rejects the upstream's response, \`data\` and \`metadata\` hold it, and
   * \`status\` its HTTP status. Without \`data\`, it carries only a Coral
   * failure code, and the call may or may not have taken effect.
   */
  class ToolError extends Error {
    private constructor();
    readonly code: string;
    readonly traceId?: string;
    readonly status?: number;
    readonly data?: unknown;
    readonly metadata?: unknown;
  }
}

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
// an emitted Saved Function declaration inlines them or fails to name them.
// A Call Path cannot contain `-`, so module resolution never finds the file
// that declares it and loads it a second time.
function typedRoot(sources, prefix, entries) {
  if (entries == null) return undefined;
  const tree = callableNode();
  for (const { pathSegments, declaration } of entries) {
    const callPath = `${prefix}.${pathSegments.join('.')}`;
    const module = JSON.stringify(callPath);
    sources[`declaration-${callPath}.d.ts`] =
      `declare module ${module} {\n${declaration}\nexport {};\n}\n`;
    insertCallable(tree, pathSegments, `typeof import(${module}).default`);
  }
  return tree;
}

// The called chains whose first prefix past `coral.<root>` is neither a
// declared Call Path nor a namespace of one: they name nothing the caller can
// call. Their open namespaces make them any, so one Coral diagnostic replaces
// every cascading TypeScript one. A resolved sibling must not make a callable
// namespace look absent. A chain that is not called may be a namespace whose
// Tools an alias reaches, so it is not reported.
function unknownTools(tools) {
  const resolved = new Set();
  const namespaces = new Set();
  for (const { pathSegments } of tools.declarations) {
    resolved.add(pathSegments.join('.'));
    for (let length = 1; length < pathSegments.length; length += 1) {
      namespaces.add(pathSegments.slice(0, length).join('.'));
    }
  }
  const unknown = [];
  for (const { segments, start, called } of tools.chains) {
    if (!called) continue;
    for (let length = 2; length <= segments.length; length += 1) {
      const key = segments.slice(0, length).join('.');
      if (resolved.has(key)) break;
      if (namespaces.has(key)) continue;
      unknown.push({ start, callPath: `coral.${segments.join('.')}` });
      break;
    }
  }
  return unknown;
}

// The declaration files that type the `coral` namespace, and the Tool
// chains that name nothing the caller can call. Without `declarations`, or
// without one root's, that root is any.
function coralSurface(declarations) {
  const sources = {};
  // Tool paths are relative to `coral`, so one tree holds both
  // `providers` and `workspace`.
  const tools = typedRoot(sources, 'coral', declarations?.tools?.declarations);
  const functions = typedRoot(sources, 'coral.functions', declarations?.functions);
  const toolRoot = (root) =>
    tools === undefined
      ? 'any'
      : renderCallableNode(tools.children.get(root) ?? callableNode(), true);
  sources[CORAL_HOST_DECLARATION_FILE_NAME] = coralHostDeclaration(
    toolRoot('providers'),
    toolRoot('workspace'),
    functions === undefined ? 'any' : renderCallableNode(functions, false),
  );
  return {
    sources,
    unknownTools: tools === undefined ? [] : unknownTools(declarations.tools),
  };
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

// One source typed by `declarations`, or untyped without them.
function typedVirtualProgram(fileName, source, compilerOptions, declarations) {
  const surface = coralSurface(declarations);
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
