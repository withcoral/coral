function compileProgramSource(
  source,
  fileName,
  compilerOptions,
  authoredMappings,
  validationError,
) {
  const parsed = ts.createSourceFile(
    fileName,
    source,
    compilerOptions.target,
    true,
    ts.ScriptKind.TS,
  );
  const wrapperValid = hasExpectedProgramShape(parsed);
  const result = ts.transpileModule(source, {
    fileName,
    reportDiagnostics: true,
    compilerOptions,
  });

  return {
    outputText: result.outputText,
    sourceMapText: result.sourceMapText,
    authoredMappings,
    wrapperValid,
    validationError,
    diagnostics: (result.diagnostics || []).map((diagnostic) =>
      diagnosticPayload(diagnostic, authoredMappings),
    ),
  };
}

function compileProgram(body, fileName, targetName) {
  const target = resolveTypeScriptTarget(targetName);
  const wrapped = wrapProgramBody(body);
  const compilerOptions = { ...PROGRAM_COMPILER_OPTIONS, target };
  const { program, sourceFile } = virtualTypeScriptProgram(
    fileName,
    wrapped.source,
    compilerOptions,
  );
  if (!sourceFile) throw new Error('Program virtual source is missing');
  const validationError = forbiddenCapabilityError(sourceFile, wrapped.authoredMappings, program);
  return compileProgramSource(
    wrapped.source,
    fileName,
    compilerOptions,
    wrapped.authoredMappings,
    validationError,
  );
}

function compileLinkedProgram(body, savedFunctions, fileName, targetName) {
  const target = resolveTypeScriptTarget(targetName);
  if (!Array.isArray(savedFunctions)) {
    throw new Error('Saved Function closure must be an array');
  }
  const wrapped = wrapProgramBody(body);
  const compilerOptions = { ...PROGRAM_COMPILER_OPTIONS, target };
  const { program, sourceFile: ordinaryProgram } = virtualTypeScriptProgram(
    fileName,
    wrapped.source,
    compilerOptions,
  );
  if (!ordinaryProgram) throw new Error('Program virtual source is missing');
  if (!hasExpectedProgramShape(ordinaryProgram)) {
    return {
      outputText: '',
      sourceMapText: null,
      authoredMappings: [],
      wrapperValid: false,
      validationError: null,
      diagnostics: [],
    };
  }
  let validationError = forbiddenCapabilityError(
    ordinaryProgram,
    wrapped.authoredMappings,
    program,
  );
  for (let index = 0; validationError === undefined && index < savedFunctions.length; index += 1) {
    const savedFunction = savedFunctions[index];
    if (typeof savedFunction?.source !== 'string') {
      throw new Error('Saved Function closure contains an invalid source entry');
    }
    const savedFunctionFileName = `saved_function_${index}.ts`;
    const { program: savedFunctionProgram, sourceFile: savedFunctionFile } =
      virtualTypeScriptProgram(savedFunctionFileName, savedFunction.source, compilerOptions);
    if (!savedFunctionFile) {
      throw new Error('Saved Function virtual source is missing');
    }
    validationError = forbiddenCapabilityError(savedFunctionFile, undefined, savedFunctionProgram);
  }
  const linked = linkedProgramSource(body, savedFunctions, target);
  return compileProgramSource(
    linked.source,
    fileName,
    compilerOptions,
    linked.authoredMappings,
    validationError,
  );
}
