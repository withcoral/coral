function compileProgramSource(
  source,
  fileName,
  compilerOptions,
  authoredMappings,
  validationError,
  saveRefusal,
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
    saveRefusal,
    diagnostics: (result.diagnostics || []).map((diagnostic) =>
      diagnosticPayload(diagnostic, authoredMappings),
    ),
  };
}

function hasSyntaxErrors(sourceFile) {
  return sourceFile.parseDiagnostics.some(
    (diagnostic) => diagnostic.category === ts.DiagnosticCategory.Error,
  );
}

// The Program body runs in an inner function, so no authored declaration
// shadows a name the private source before it reads.
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
  if (!hasExpectedProgramShape(sourceFile)) {
    return compileProgramSource(
      wrapped.source,
      fileName,
      compilerOptions,
      wrapped.authoredMappings,
      validationError,
    );
  }
  const carrier = savedFunctionCarrierName([sourceFile]);
  const extraction =
    validationError === undefined && !hasSyntaxErrors(sourceFile)
      ? programExtraction(sourceFile, program, wrapped.authoredMappings, carrier)
      : { edits: [] };
  const compiled = compiledProgramBody(body, extraction.edits ?? []);
  return compileProgramSource(
    `(async () => { ${savedFunctionCarrierDeclaration(carrier)} return await (async () => {\n${compiled.text}\n})(); })();\n`,
    fileName,
    compilerOptions,
    compiled.mappings.map((mapping) => ({ ...mapping, generatedLine: mapping.generatedLine + 1 })),
    validationError,
    extraction.refusal,
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
  const modules = savedFunctions.map((savedFunction, index) => {
    if (typeof savedFunction?.source !== 'string') {
      throw new Error('Saved Function closure contains an invalid source entry');
    }
    const { program: moduleProgram, sourceFile } = virtualTypeScriptProgram(
      `saved_function_${index}.ts`,
      savedFunction.source,
      compilerOptions,
    );
    if (!sourceFile) {
      throw new Error('Saved Function virtual source is missing');
    }
    return { program: moduleProgram, sourceFile, pathSegments: savedFunction.pathSegments };
  });
  let validationError = forbiddenCapabilityError(
    ordinaryProgram,
    wrapped.authoredMappings,
    program,
  );
  for (let index = 0; validationError === undefined && index < modules.length; index += 1) {
    validationError = forbiddenCapabilityError(
      modules[index].sourceFile,
      undefined,
      modules[index].program,
    );
  }
  const carrier = savedFunctionCarrierName([
    ordinaryProgram,
    ...modules.map((module) => module.sourceFile),
  ]);
  const saves = { carrier, programEdits: [], moduleEdits: modules.map(() => []) };
  let saveRefusal;
  if (validationError === undefined && !hasSyntaxErrors(ordinaryProgram)) {
    const extraction = programExtraction(
      ordinaryProgram,
      program,
      wrapped.authoredMappings,
      carrier,
    );
    saveRefusal = extraction.refusal;
    saves.programEdits = extraction.edits ?? [];
    for (let index = 0; saveRefusal === undefined && index < modules.length; index += 1) {
      const { sourceFile, program: moduleProgram, pathSegments } = modules[index];
      if (hasSyntaxErrors(sourceFile) || !Array.isArray(pathSegments)) continue;
      const moduleExtraction = savedFunctionModuleExtraction(
        sourceFile,
        moduleProgram,
        pathSegments,
        carrier,
      );
      saveRefusal = moduleExtraction.refusal;
      saves.moduleEdits[index] = moduleExtraction.edits ?? [];
    }
  }
  if (saveRefusal !== undefined) {
    return {
      outputText: '',
      sourceMapText: null,
      authoredMappings: [],
      wrapperValid: true,
      validationError,
      saveRefusal,
      diagnostics: [],
    };
  }
  const linked = linkedProgramSource(body, savedFunctions, target, saves);
  return compileProgramSource(
    linked.source,
    fileName,
    compilerOptions,
    linked.authoredMappings,
    validationError,
  );
}
