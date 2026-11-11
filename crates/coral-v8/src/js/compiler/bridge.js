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
globalThis.__coralAnalyzeProgram = function analyzeVirtualProgram(body, fileName, targetName) {
  return compilerResultJson(analysisResult(analyzeProgram(body, fileName, targetName)));
};
globalThis.__coralAnalyzeAuthoredTool = function analyzeVirtualAuthoredTool(
  source,
  fileName,
  targetName,
) {
  return compilerResultJson(analysisResult(analyzeAuthoredTool(source, fileName, targetName)));
};
globalThis.__coralUnknownToolCalls = function unknownVirtualToolCalls(
  kind,
  source,
  callPathsJson,
  fileName,
  targetName,
) {
  return compilerResultJson(
    unknownToolCalls(kind, source, JSON.parse(callPathsJson), fileName, targetName),
  );
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
