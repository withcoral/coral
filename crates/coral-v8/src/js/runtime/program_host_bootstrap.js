'use strict';
(() => {
  const invoke = globalThis.__coralToolInvoke;
  const appendEmission = globalThis.__coralProgramEmit;
  const appendLog = globalThis.__coralProgramLog;
  Reflect.deleteProperty(globalThis, '__coralToolInvoke');
  Reflect.deleteProperty(globalThis, '__coralProgramEmit');
  Reflect.deleteProperty(globalThis, '__coralProgramLog');

  const nativeStringify = JSON.stringify;
  const nativeString = String;
  const NativeError = Error;
  const NativeTypeError = TypeError;
  const defineProperties = Object.defineProperties;
  const setPrototypeOf = Object.setPrototypeOf;
  const freeze = Object.freeze;
  const create = Object.create;
  const keys = Object.keys;
  const isArray = Array.isArray;

  class ToolError extends NativeError {
    // oxlint-disable-next-line constructor-super -- Programs must not construct a ToolError.
    constructor() {
      throw new NativeTypeError('ToolError cannot be constructed');
    }
  }
  Object.defineProperty(ToolError.prototype, 'name', { value: 'ToolError' });
  Object.freeze(ToolError.prototype);
  Object.freeze(ToolError);

  // A literal `coral.workspace.saveFunction` call passes its source in a
  // carrier that admission writes and the compiled wrapper takes before any
  // authored code runs. A carrier has no prototype, so no Program reaches
  // this class, and a private field marks it, so no Program forges one.
  class SavedFunctionSource {
    #text;
    constructor(text) {
      this.#text = text;
      setPrototypeOf(this, null);
      freeze(this);
    }

    static textOf(value) {
      return typeof value === 'object' && value !== null && #text in value
        ? value.#text
        : undefined;
    }
  }
  Object.defineProperty(globalThis, '__coralSavedFunctionSource', {
    value: (text) => new SavedFunctionSource(nativeString(text)),
    enumerable: false,
    writable: false,
    configurable: true,
  });

  // The arguments of `coral.workspace.saveFunction` as the Tool reads them:
  // `source` only when a carrier holds it, so no authored value reaches the
  // Tool as a source. Undefined unless the input is an object.
  const SAVE_FUNCTION_PATH = 'coral.workspace.saveFunction';
  const saveFunctionArguments = (input) => {
    if (typeof input !== 'object' || input === null || isArray(input)) return undefined;
    const encoded = create(null);
    for (const key of keys(input)) {
      if (key !== 'source') encoded[key] = input[key];
    }
    const text = SavedFunctionSource.textOf(input.source);
    if (text !== undefined) encoded.source = text;
    return encoded;
  };

  const callablePath = (path) =>
    new Proxy(function coralToolPath() {}, {
      get(_target, property) {
        if (typeof property !== 'string' || property === 'then') {
          return undefined;
        }
        return callablePath(`${path}.${property}`);
      },
      apply(_target, _receiver, args) {
        if (args.length > 1) {
          return invoke(path);
        }
        let encoded = '{}';
        if (args.length === 1) {
          try {
            const input = path === SAVE_FUNCTION_PATH ? saveFunctionArguments(args[0]) : args[0];
            if (input === undefined) return invoke(path);
            encoded = nativeStringify(input);
          } catch {
            return invoke(path);
          }
          if (typeof encoded !== 'string') {
            return invoke(path);
          }
        }
        return invoke(path, encoded);
      },
    });

  const emit = (value) => {
    let encoded;
    try {
      encoded = nativeStringify(value);
    } catch {
      return appendEmission();
    }
    if (typeof encoded !== 'string') {
      return appendEmission();
    }
    return appendEmission(encoded);
  };

  const logMessage = (values) => {
    let message = '';
    for (let index = 0; index < values.length; index += 1) {
      if (index > 0) message += ' ';
      message += nativeString(values[index]);
    }
    return message;
  };
  const writeLog = (level, values) => {
    appendLog(level, logMessage(values));
  };
  const console = Object.create(null);
  for (const name of ['debug', 'info', 'log', 'warn', 'error']) {
    Object.defineProperty(console, name, {
      value: (...values) => writeLog(name === 'log' ? 'info' : name, values),
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
  Object.preventExtensions(console);
  Object.defineProperty(globalThis, 'console', {
    value: console,
    enumerable: true,
    writable: false,
    configurable: false,
  });

  // `providers` and `workspace` start the two kinds of Provider Call Root; the
  // host resolves the Provider from the complete Call Path. Any other member
  // is undefined, so a misspelled one throws where the Program uses it.
  const coral = Object.create(null);
  Object.defineProperties(coral, {
    ToolError: {
      value: ToolError,
      enumerable: true,
      writable: false,
      configurable: false,
    },
    providers: {
      value: callablePath('coral.providers'),
      enumerable: true,
      writable: false,
      configurable: false,
    },
    workspace: {
      value: callablePath('coral.workspace'),
      enumerable: true,
      writable: false,
      configurable: false,
    },
    emit: {
      value: emit,
      enumerable: true,
      writable: false,
      configurable: false,
    },
  });
  Object.preventExtensions(coral);
  Object.defineProperty(globalThis, 'coral', {
    value: coral,
    enumerable: true,
    writable: false,
    configurable: false,
  });

  // Returned only to the host; Programs cannot mint a ToolError. An
  // upstream rejection carries the complete outcome the Program did not
  // receive, and a rejected Call Path that names no Tool carries the nearest
  // ones that do.
  return (code, message, traceId, stack, outcome, suggestions) => {
    const error = new NativeError(message);
    setPrototypeOf(error, ToolError.prototype);
    defineProperties(error, {
      code: { __proto__: null, value: code },
      stack: { __proto__: null, value: stack, writable: true, configurable: true },
    });
    if (traceId !== undefined) {
      defineProperties(error, { traceId: { __proto__: null, value: traceId } });
    }
    if (suggestions !== undefined) {
      defineProperties(error, {
        suggestions: { __proto__: null, value: freeze(suggestions) },
      });
    }
    if (outcome !== undefined) {
      if (outcome.status !== undefined) {
        defineProperties(error, { status: { __proto__: null, value: outcome.status } });
      }
      defineProperties(error, {
        data: { __proto__: null, value: outcome.data },
        metadata: { __proto__: null, value: outcome.metadata },
      });
    }
    return error;
  };
})();
