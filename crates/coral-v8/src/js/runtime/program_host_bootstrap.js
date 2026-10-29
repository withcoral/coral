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

  class ToolError extends NativeError {
    // oxlint-disable-next-line constructor-super -- Programs must not construct a ToolError.
    constructor() {
      throw new NativeTypeError('ToolError cannot be constructed');
    }
  }
  Object.defineProperty(ToolError.prototype, 'name', { value: 'ToolError' });
  Object.freeze(ToolError.prototype);
  Object.freeze(ToolError);

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
            encoded = nativeStringify(args[0]);
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
  // upstream rejection carries the complete outcome the Program did not receive.
  return (code, message, traceId, stack, outcome) => {
    const error = new NativeError(message);
    setPrototypeOf(error, ToolError.prototype);
    defineProperties(error, {
      code: { __proto__: null, value: code },
      stack: { __proto__: null, value: stack, writable: true, configurable: true },
    });
    if (traceId !== undefined) {
      defineProperties(error, { traceId: { __proto__: null, value: traceId } });
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
