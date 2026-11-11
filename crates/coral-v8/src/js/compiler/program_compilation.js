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
// shadows a name the private source before it reads. `builtinTools` are the
// keys of the Built-in Tools the Program calls on `coral`.
function compileProgram(body, builtinTools, fileName, targetName) {
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
  const carrier = privateToolCarrierName([sourceFile]);
  const extraction =
    validationError === undefined && !hasSyntaxErrors(sourceFile)
      ? programExtraction(sourceFile, program, wrapped.authoredMappings, carrier)
      : { edits: [] };
  const compiled = compiledProgramBody(body, extraction.edits ?? []);
  const prefix = `(async () => { ${privateToolCarrierDeclaration(carrier)}\n${coralRootPrelude(builtinTools, [])}\nreturn await (async () => {\n`;
  const bodyLine = sourceLineCount(prefix) - 1;
  return compileProgramSource(
    `${prefix}${compiled.text}\n})(); })();\n`,
    fileName,
    compilerOptions,
    compiled.mappings.map((mapping) => ({
      ...mapping,
      generatedLine: mapping.generatedLine + bodyLine,
    })),
    validationError,
    extraction.refusal,
  );
}

// Why a stored source the Program links cannot be called, or undefined:
// one saved before every Tool took one object (ADR 0034). Coral never
// rewrites a stored source, so its author saves or publishes it again.
function linkedToolInputRefusal({ program, sourceFile, pathSegments, root }) {
  if (hasSyntaxErrors(sourceFile) || !Array.isArray(pathSegments)) return undefined;
  let shape;
  try {
    const checker = program.getTypeChecker();
    shape = toolInputShapeError(checker, defaultExportSignatures(checker, sourceFile));
  } catch {
    // A source without one default callable fails where it is compiled.
    return undefined;
  }
  if (shape?.node === undefined) return undefined;
  const at = sourceFile.getLineAndCharacterOfPosition(shape.node.getStart(sourceFile));
  const shared = root === 'shared';
  const fix = shared
    ? 'A Workspace Owner must publish a new revision.'
    : 'Save it again with coral.updateTool.';
  return `${shared ? 'Shared' : 'Private'} Tool coral.${shared ? 'shared' : 'private'}.${pathSegments.join('.')}:${at.line + 1}:${at.character + 1}: ${shape.reason}. ${fix}`;
}

function compileLinkedProgram(body, linkedTools, builtinTools, fileName, targetName) {
  const target = resolveTypeScriptTarget(targetName);
  if (!Array.isArray(linkedTools)) {
    throw new Error('Linked Tool closure must be an array');
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
  const modules = linkedTools.map((linkedTool, index) => {
    if (typeof linkedTool?.source !== 'string') {
      throw new Error('Linked Tool closure contains an invalid source entry');
    }
    const { program: moduleProgram, sourceFile } = virtualTypeScriptProgram(
      `private_tool_${index}.ts`,
      linkedTool.source,
      compilerOptions,
    );
    if (!sourceFile) {
      throw new Error('Linked Tool virtual source is missing');
    }
    return {
      program: moduleProgram,
      sourceFile,
      pathSegments: linkedTool.pathSegments,
      root: linkedTool.root,
    };
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
  const linkedToolRefusal =
    validationError === undefined
      ? modules.map(linkedToolInputRefusal).find((refusal) => refusal !== undefined)
      : undefined;
  if (linkedToolRefusal !== undefined) {
    return {
      outputText: '',
      sourceMapText: null,
      authoredMappings: [],
      wrapperValid: true,
      validationError: null,
      linkedToolRefusal,
      diagnostics: [],
    };
  }
  const carrier = privateToolCarrierName([
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
      const { sourceFile, program: moduleProgram, pathSegments, root } = modules[index];
      if (hasSyntaxErrors(sourceFile) || !Array.isArray(pathSegments)) continue;
      const moduleExtraction = privateToolModuleExtraction(
        sourceFile,
        moduleProgram,
        pathSegments,
        carrier,
        root,
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
  const linked = linkedProgramSource(body, linkedTools, builtinTools, target, saves);
  return compileProgramSource(
    linked.source,
    fileName,
    compilerOptions,
    linked.authoredMappings,
    validationError,
  );
}
