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

// Authoring-only types for the host root. Provider Operations and Saved
// Functions stay untyped here: their declarations live outside this isolate.
const CORAL_HOST_DECLARATION_FILE_NAME = "coral.d.ts";
const CORAL_HOST_DECLARATION = `
declare namespace coral {
  const providers: any;
  const functions: any;
  function emit(value: unknown): Promise<void>;
  /**
   * Rejects an Operation call that produced no declared success. When the
   * upstream answered, \`data\` and \`metadata\` hold its complete outcome, and
   * \`status\` its HTTP status. Without \`data\`, Coral received no response,
   * and the call may or may not have taken effect.
   */
  class OperationError extends Error {
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

let typeScriptLibrarySources;

function configureTypeScriptLibraries(librarySources) {
  typeScriptLibrarySources = librarySources;
}

function resolveTypeScriptTarget(targetName) {
  const target = ts.ScriptTarget[targetName];
  if (typeof target !== "number") {
    throw new Error(`unknown TypeScript ScriptTarget: ${targetName}`);
  }
  return target;
}

function normalizedFileName(fileName) {
  return fileName.replaceAll("\\", "/").split("/").pop();
}

function compilerHost(
  sourceFileName,
  source,
  compilerOptions,
  outputs,
  ambientSources,
) {
  const sourceNames = new Set([sourceFileName, normalizedFileName(sourceFileName)]);
  const supportingSource = (normalized) =>
    ambientSources[normalized] ?? typeScriptLibrarySources[normalized];
  return {
    getSourceFile(fileName, languageVersion) {
      const normalized = normalizedFileName(fileName);
      const text = sourceNames.has(fileName) || sourceNames.has(normalized)
        ? source
        : supportingSource(normalized);
      if (text === undefined) return undefined;
      return ts.createSourceFile(fileName, text, languageVersion, true);
    },
    getDefaultLibFileName() {
      return "lib.es2025.d.ts";
    },
    writeFile(fileName, text) {
      outputs.set(normalizedFileName(fileName), text);
    },
    getCurrentDirectory() {
      return "/";
    },
    getCanonicalFileName(fileName) {
      return fileName;
    },
    useCaseSensitiveFileNames() {
      return true;
    },
    getNewLine() {
      return "\n";
    },
    fileExists(fileName) {
      const normalized = normalizedFileName(fileName);
      return sourceNames.has(fileName) || sourceNames.has(normalized)
        || supportingSource(normalized) !== undefined;
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
  const host = compilerHost(
    sourceFileName,
    source,
    compilerOptions,
    outputs,
    ambientSources,
  );
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
