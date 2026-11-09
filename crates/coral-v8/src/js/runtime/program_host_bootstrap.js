'use strict';
(() => {
  const invoke = globalThis.__coralToolInvoke;
  const perform = globalThis.__coralToolPerform;
  const complete = globalThis.__coralToolComplete;
  const runBody = globalThis.__coralToolRunBody;
  const appendEmission = globalThis.__coralProgramEmit;
  const appendLog = globalThis.__coralProgramLog;
  Reflect.deleteProperty(globalThis, '__coralToolInvoke');
  Reflect.deleteProperty(globalThis, '__coralToolPerform');
  Reflect.deleteProperty(globalThis, '__coralToolComplete');
  Reflect.deleteProperty(globalThis, '__coralToolRunBody');
  Reflect.deleteProperty(globalThis, '__coralProgramEmit');
  Reflect.deleteProperty(globalThis, '__coralProgramLog');

  const nativeStringify = JSON.stringify;
  const nativeParse = JSON.parse;
  const reflectApply = Reflect.apply;
  const nativeString = String;
  const NativeError = Error;
  const NativeTypeError = TypeError;
  const defineProperties = Object.defineProperties;
  const setPrototypeOf = Object.setPrototypeOf;
  const freeze = Object.freeze;
  const create = Object.create;
  const keys = Object.keys;
  const isArray = Array.isArray;
  const NativeProxy = Proxy;
  const NativePromise = Promise;
  const nativePromiseResolve = Promise.resolve;

  class ToolError extends NativeError {
    // oxlint-disable-next-line constructor-super -- Programs must not construct a ToolError.
    constructor() {
      throw new NativeTypeError('ToolError cannot be constructed');
    }
  }
  Object.defineProperty(ToolError.prototype, 'name', { value: 'ToolError' });
  Object.freeze(ToolError.prototype);
  Object.freeze(ToolError);

  // A literal call to a saving Tool passes its source in a
  // carrier that admission writes and the compiled wrapper takes before any
  // authored code runs. A carrier has no prototype, so no Program reaches
  // this class, and a private field marks it, so no Program forges one.
  class PrivateToolSource {
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
  Object.defineProperty(globalThis, '__coralPrivateToolSource', {
    value: (text) => new PrivateToolSource(nativeString(text)),
    enumerable: false,
    writable: false,
    configurable: true,
  });

  // The arguments of a saving Tool as the Tool reads them: `source` only
  // when a carrier holds it, so no authored value reaches the Tool as a
  // source. Undefined unless the input is an object.
  const savingArguments = (input) => {
    if (typeof input !== 'object' || input === null || isArray(input)) return undefined;
    const encoded = create(null);
    for (const key of keys(input)) {
      // A copied `toJSON` would replace what this encodes, source included.
      if (key !== 'source' && key !== 'toJSON') encoded[key] = input[key];
    }
    const text = PrivateToolSource.textOf(input.source);
    if (text !== undefined) encoded.source = text;
    return encoded;
  };

  const callablePath = (path) =>
    new NativeProxy(function coralToolPath() {}, {
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
            // Plain comparisons, so no prototype a Program can replace
            // decides which Tool takes a carried source. Admission lists
            // the same Tools in SAVING_TOOLS; change both.
            const saving = path === 'coral.addTool' || path === 'coral.updateTool';
            const input = saving ? savingArguments(args[0]) : args[0];
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

  // The text a thrown value reaches the caller with: an Error's message, or
  // the value itself. Reading either can run Program code, which may throw.
  const thrownMessage = (thrown) => {
    try {
      if (typeof thrown === 'object' && thrown !== null && typeof thrown.message === 'string') {
        return thrown.message;
      }
      return nativeString(thrown);
    } catch {
      return 'Tool failed';
    }
  };

  // One call to a Private or Shared Tool, which Coral performs by running `body()`,
  // the linked module's default export, in this Program: one Invocation.
  // The call crosses to Coral and back, so its arguments and result are
  // copied as JSON, and it settles only once its completion is durable.
  // Admission and approval come first; the body runs only once permitted.
  // The body runs as the body of `handle`, so every Tool call it makes,
  // after any await, is that call's own; this function's own awaits stay
  // the caller's. A returned thenable is resolved there too, so its `then`
  // runs as the body.
  const performTool = async (callPath, body, args) => {
    let encoded;
    try {
      encoded = nativeStringify(args);
    } catch {
      return perform(callPath);
    }
    if (typeof encoded !== 'string') return perform(callPath);
    // Before any await, so the caller's frame names the call site.
    const permitted = perform(callPath, encoded);
    const handle = await permitted;
    let returned;
    try {
      const value = await runBody(handle, () =>
        reflectApply(nativePromiseResolve, NativePromise, [
          reflectApply(body(), undefined, nativeParse(encoded)),
        ]),
      );
      try {
        returned = value === undefined ? 'null' : nativeStringify(value);
      } catch {
        returned = undefined;
      }
    } catch (thrown) {
      return complete(handle, undefined, thrownMessage(thrown), thrown);
    }
    return complete(handle, typeof returned === 'string' ? returned : undefined);
  };

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

  // The Program host's own members. The compiled Program completes `coral`
  // before any authored code runs: it adds a member for each Built-in Tool
  // it was compiled with, such as `coral.search`, and `private` and `shared`,
  // which hold the Private and Shared Tools it links, built with the call
  // that performs each. Then
  // no member can be added, so a misspelled one is undefined and throws
  // where the Program uses it.
  const coral = create(null);
  Object.defineProperties(coral, {
    ToolError: {
      value: ToolError,
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
    providers: {
      value: callablePath('coral.providers'),
      enumerable: true,
      writable: false,
      configurable: false,
    },
  });
  Object.defineProperty(globalThis, 'coral', {
    value: coral,
    enumerable: true,
    writable: false,
    configurable: false,
  });
  Object.defineProperty(globalThis, '__coralCompleteRoot', {
    value: (builtinTools, buildLinkedTools) => {
      const linked = buildLinkedTools(
        (callPath, body) =>
          (...args) =>
            performTool(callPath, body, args),
      );
      for (const key of builtinTools) {
        Object.defineProperty(coral, key, {
          value: callablePath(`coral.${key}`),
          enumerable: true,
          writable: false,
          configurable: false,
        });
      }
      for (const root of ['private', 'shared']) {
        Object.defineProperty(coral, root, {
          value: linked[root],
          enumerable: true,
          writable: false,
          configurable: false,
        });
      }
      Object.preventExtensions(coral);
    },
    enumerable: false,
    writable: false,
    configurable: true,
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
