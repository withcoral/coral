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
globalThis.__coralPrepareAuthoredTool = function prepareVirtualAuthoredTool(
  source,
  builtinToolsJson,
  declarationsJson,
  fileName,
  targetName,
) {
  return compilerResultJson(
    prepareAuthoredTool(
      source,
      JSON.parse(builtinToolsJson),
      JSON.parse(declarationsJson),
      fileName,
      targetName,
    ),
  );
};
globalThis.__coralAnalyzeAuthoredToolCallPaths = function analyzeVirtualAuthoredToolCallPaths(
  source,
  fileName,
  targetName,
) {
  return compilerResultJson(analyzeAuthoredToolCallPaths(source, fileName, targetName));
};
globalThis.__coralAnalyzeProgramPaths = function analyzeVirtualProgramPaths(
  source,
  fileName,
  targetName,
) {
  return compilerResultJson(analyzeProgramPaths(source, fileName, targetName));
};
globalThis.__coralUnknownToolCalls = function unknownVirtualToolCalls(chainsJson, callPathsJson) {
  return compilerResultJson(unknownToolCalls(JSON.parse(chainsJson), JSON.parse(callPathsJson)));
};
globalThis.__coralAnalyzeAuthoredToolDependencies = function analyzeVirtualAuthoredToolDependencies(
  source,
  fileName,
  targetName,
) {
  return compilerResultJson(analyzeAuthoredToolDependencies(source, fileName, targetName));
};
globalThis.__coralCompileLinkedProgram = function compileVirtualLinkedProgram(
  body,
  linkedToolsJson,
  builtinToolsJson,
  fileName,
  targetName,
) {
  return compilerResultJson(
    compileLinkedProgram(
      body,
      JSON.parse(linkedToolsJson),
      JSON.parse(builtinToolsJson),
      fileName,
      targetName,
    ),
  );
};
