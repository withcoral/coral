const FORBIDDEN_AMBIENT_GLOBALS = new Set([
  ...globalThis.__coralForbiddenAmbientGlobals,
  'eval',
  'Function',
]);
Reflect.deleteProperty(globalThis, '__coralForbiddenAmbientGlobals');

function compilerResultJson(result) {
  return JSON.stringify(result);
}

globalThis.__coralTypeScriptVersion = ts.version;
globalThis.__coralConfigureCompiler = function configureCompiler(librarySourcesJson) {
  configureTypeScriptLibraries(JSON.parse(librarySourcesJson));
};
globalThis.__coralCompile = function compileVirtualProgram(
  body,
  builtinToolsJson,
  fileName,
  targetName,
) {
  return compilerResultJson(
    compileProgram(body, JSON.parse(builtinToolsJson), fileName, targetName),
  );
};
globalThis.__coralPreparePrivateTool = function prepareVirtualPrivateTool(
  source,
  builtinToolsJson,
  declarationsJson,
  fileName,
  targetName,
) {
  return compilerResultJson(
    preparePrivateTool(
      source,
      JSON.parse(builtinToolsJson),
      JSON.parse(declarationsJson),
      fileName,
      targetName,
    ),
  );
};
globalThis.__coralAnalyzePrivateToolCallPaths = function analyzeVirtualPrivateToolCallPaths(
  source,
  fileName,
  targetName,
) {
  return compilerResultJson(analyzePrivateToolCallPaths(source, fileName, targetName));
};
globalThis.__coralAnalyzePrivateToolPaths = function analyzeVirtualProgramPaths(
  source,
  fileName,
  targetName,
) {
  return compilerResultJson(analyzePrivateToolPaths(source, fileName, targetName));
};
globalThis.__coralUnknownToolCalls = function unknownVirtualToolCalls(chainsJson, callPathsJson) {
  return compilerResultJson(unknownToolCalls(JSON.parse(chainsJson), JSON.parse(callPathsJson)));
};
globalThis.__coralAnalyzePrivateToolDependencies =
  function analyzeVirtualPrivateToolDependencies(source, fileName, targetName) {
    return compilerResultJson(analyzePrivateToolDependencies(source, fileName, targetName));
  };
globalThis.__coralCompileLinkedProgram = function compileVirtualLinkedProgram(
  body,
  privateToolsJson,
  builtinToolsJson,
  fileName,
  targetName,
) {
  return compilerResultJson(
    compileLinkedProgram(
      body,
      JSON.parse(privateToolsJson),
      JSON.parse(builtinToolsJson),
      fileName,
      targetName,
    ),
  );
};
