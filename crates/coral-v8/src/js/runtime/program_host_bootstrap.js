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
  const ObjectPrototype = Object.prototype;
  const getPrototypeOf = Object.getPrototypeOf;
  const objectTag = Object.prototype.toString;
  const regExpTest = RegExp.prototype.test;
  const numberIsFinite = Number.isFinite;
  const IDENTIFIER = /^[A-Za-z_$][0-9A-Za-z_$]*$/u;

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

  // Tool data is what JSON holds: null, booleans, finite numbers, strings,
  // arrays and plain objects (ADR 0034). `encodeData` copies a value into
  // fresh containers as it checks it, so each getter runs once and no
  // `toJSON` replaces what it encodes. Anything else is refused, never
  // converted: `notData` then holds where it is, as a path from `root`,
  // and why. Only Coral's own natives are called, since a Program can
  // replace any prototype's methods.
  const notData = { field: '', reason: '' };
  const NOT_DATA = freeze(create(null));
  const refuse = (field, reason) => {
    notData.field = field;
    notData.reason = reason;
    return NOT_DATA;
  };
  const fieldPath = (parent, key) => {
    if (typeof key === 'number') return `${parent}[${key}]`;
    if (!reflectApply(regExpTest, IDENTIFIER, [key])) return `${parent}[${nativeStringify(key)}]`;
    return parent === '' ? key : `${parent}.${key}`;
  };
  // The standard objects an author is likely to pass, and the data to use
  // instead. Another prototype is named by its class.
  const NAMED_OBJECTS = freeze({
    __proto__: null,
    '[object Date]': 'a Date; use a string, such as date.toISOString()',
    '[object Map]': 'a Map; use an object, such as Object.fromEntries(map)',
    '[object Set]': 'a Set; use an array, such as [...set]',
    '[object Promise]': 'a Promise; await it first',
    '[object RegExp]': 'a RegExp; use a string',
    '[object Error]': 'an Error; use its message',
  });
  const notPlain = (value) => {
    const named = NAMED_OBJECTS[reflectApply(objectTag, value, [])];
    if (named !== undefined) return `it is ${named}`;
    const constructor = getPrototypeOf(value).constructor;
    const name = typeof constructor === 'function' ? constructor.name : undefined;
    return typeof name === 'string' && name !== ''
      ? `it is an instance of class ${name}; use a plain object`
      : 'it is not a plain object; use a plain object';
  };
  const copyData = (value, path, ancestors) => {
    switch (typeof value) {
      case 'string':
      case 'boolean':
      case 'undefined':
        return value;
      case 'number':
        if (numberIsFinite(value)) return value;
        throw refuse(path, `it is ${nativeString(value)}; use a finite number or null`);
      case 'bigint':
        throw refuse(path, 'it is a bigint; use a number or a string');
      case 'symbol':
        throw refuse(path, 'it is a symbol; use a string');
      case 'function':
        throw refuse(path, 'it is a function; use data');
      default:
        break;
    }
    if (value === null) return null;
    for (let index = 0; index < ancestors.length; index += 1) {
      if (ancestors[index] === value) throw refuse(path, 'it contains itself');
    }
    ancestors[ancestors.length] = value;
    let copy;
    if (isArray(value)) {
      copy = [];
      const length = value.length;
      for (let index = 0; index < length; index += 1) {
        const element = copyData(value[index], fieldPath(path, index), ancestors);
        copy[index] = element === undefined ? null : element;
      }
    } else {
      const prototype = getPrototypeOf(value);
      if (prototype !== ObjectPrototype && prototype !== null) throw refuse(path, notPlain(value));
      copy = create(null);
      const names = keys(value);
      for (let index = 0; index < names.length; index += 1) {
        const name = names[index];
        const element = copyData(value[name], fieldPath(path, name), ancestors);
        if (element !== undefined) copy[name] = element;
      }
    }
    ancestors.length -= 1;
    return copy;
  };
  // `value` as JSON text, or undefined when it is not data. A getter or
  // proxy that throws, or nesting too deep to walk, leaves it unreadable.
  const encodeData = (value, root) => {
    let copy;
    try {
      copy = copyData(value, root, []);
    } catch (thrown) {
      if (thrown !== NOT_DATA) refuse(root, 'it could not be read as data');
      return undefined;
    }
    return nativeStringify(copy === undefined ? null : copy);
  };
  // What a Tool call's one argument is when it is not an object.
  const argumentKind = (value) => {
    if (value === null) return 'null';
    if (isArray(value)) return 'an array';
    if (value === undefined) return 'undefined';
    return `a ${typeof value}`;
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
          return invoke(path, undefined, '', `pass one object, not ${args.length} arguments`);
        }
        let encoded = '{}';
        if (args.length === 1) {
          let input;
          try {
            // Plain comparisons, so no prototype a Program can replace
            // decides which Tool takes a carried source. Admission lists
            // the same Tools in SAVING_TOOLS; change both.
            const saving = path === 'coral.addTool' || path === 'coral.updateTool';
            input = saving ? savingArguments(args[0]) : args[0];
            if (input === undefined) return invoke(path);
            encoded = nativeStringify(input);
          } catch {
            // A Provider Tool's arguments still cross as `JSON.stringify`
            // writes them; when it cannot, say where.
            if (encodeData(input, '') === undefined) {
              return invoke(path, undefined, notData.field, notData.reason);
            }
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
  // The call crosses to Coral and back, so it takes one object, its
  // argument and result are copied as data, and it settles only once its
  // completion is durable. Arguments that are not data are refused before
  // the call is one.
  // Admission and approval come first; the body runs only once permitted.
  // The body runs as the body of `handle`, so every Tool call it makes,
  // after any await, is that call's own; this function's own awaits stay
  // the caller's. A returned thenable is resolved there too, so its `then`
  // runs as the body.
  const performTool = async (callPath, body, args) => {
    // Every refusal and the permit are asked before any await, so the
    // caller's frame names the call site.
    if (args.length > 1) {
      return perform(callPath, undefined, '', `pass one object, not ${args.length} arguments`);
    }
    const input = args.length === 0 ? {} : args[0];
    if (typeof input !== 'object' || input === null || isArray(input)) {
      return perform(callPath, undefined, '', `pass one object, not ${argumentKind(input)}`);
    }
    const encoded = encodeData(input, '');
    if (encoded === undefined) return perform(callPath, undefined, notData.field, notData.reason);
    const permitted = perform(callPath, encoded);
    const handle = await permitted;
    let returned;
    try {
      const value = await runBody(handle, () =>
        reflectApply(nativePromiseResolve, NativePromise, [
          reflectApply(body(), undefined, [nativeParse(encoded)]),
        ]),
      );
      returned = encodeData(value, 'result');
    } catch (thrown) {
      return complete(handle, undefined, thrownMessage(thrown), thrown);
    }
    if (returned === undefined) {
      return complete(handle, undefined, undefined, undefined, notData.field, notData.reason);
    }
    return complete(handle, returned);
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
