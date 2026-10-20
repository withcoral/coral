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
globalThis.__coralCompile = function compileVirtualProgram(body, fileName, targetName) {
  return compilerResultJson(compileProgram(body, fileName, targetName));
};
globalThis.__coralPrepareSavedFunction = function prepareVirtualSavedFunction(
  source,
  declarationsJson,
  fileName,
  targetName,
) {
  return compilerResultJson(
    prepareSavedFunction(source, JSON.parse(declarationsJson), fileName, targetName),
  );
};
globalThis.__coralAnalyzeSavedFunctionCallPaths = function analyzeVirtualSavedFunctionCallPaths(
  source,
  fileName,
  targetName,
) {
  return compilerResultJson(analyzeSavedFunctionCallPaths(source, fileName, targetName));
};
globalThis.__coralAnalyzeSavedFunctionPaths = function analyzeVirtualProgramPaths(
  source,
  fileName,
  targetName,
) {
  return compilerResultJson(analyzeSavedFunctionPaths(source, fileName, targetName));
};
globalThis.__coralAnalyzeSavedFunctionDependencies =
  function analyzeVirtualSavedFunctionDependencies(source, fileName, targetName) {
    return compilerResultJson(analyzeSavedFunctionDependencies(source, fileName, targetName));
  };
globalThis.__coralCompileLinkedProgram = function compileVirtualLinkedProgram(
  body,
  savedFunctionsJson,
  fileName,
  targetName,
) {
  return compilerResultJson(
    compileLinkedProgram(body, JSON.parse(savedFunctionsJson), fileName, targetName),
  );
};
