"use strict";

(() => {
  const invoke = globalThis.__coralOperationInvoke;
  const appendEmission = globalThis.__coralProgramEmit;
  const appendLog = globalThis.__coralProgramLog;
  const operationPolicy = globalThis.__coralOperationPolicy;
  const callLocation = globalThis.__coralCallLocation;
  const appendEmissionWithPolicy = globalThis.__coralProgramEmitWithPolicy;
  const middlewareStarted = globalThis.__coralMiddlewareStarted;
  const middlewareContinued = globalThis.__coralMiddlewareContinued;
  const middlewareEnded = globalThis.__coralMiddlewareEnded;
  const currentFrame = globalThis.__coralFrameGet;
  const enterFrame = globalThis.__coralFrameSet;
  const runId = globalThis.__coralRunId;
  for (const name of [
    "__coralOperationInvoke",
    "__coralProgramEmit",
    "__coralProgramLog",
    "__coralOperationPolicy",
    "__coralCallLocation",
    "__coralProgramEmitWithPolicy",
    "__coralMiddlewareStarted",
    "__coralMiddlewareContinued",
    "__coralMiddlewareEnded",
    "__coralFrameGet",
    "__coralFrameSet",
    "__coralRunId",
  ]) {
    Reflect.deleteProperty(globalThis, name);
  }

  const nativeStringify = JSON.stringify;
  const nativeString = String;
  const NativeTypeError = TypeError;
  const defineProperties = Object.defineProperties;
  const defineProperty = Object.defineProperty;
  const setPrototypeOf = Object.setPrototypeOf;
  const freeze = Object.freeze;
  const create = Object.create;
  const preventExtensions = Object.preventExtensions;
  const isArray = Array.isArray;
  const hasOwn = Object.hasOwn;
  const apply = Reflect.apply;
  const nativeParse = JSON.parse;
  const NativeError = Error;
  const NativePromise = Promise;
  const promiseReject = (reason) => NativePromise.reject(reason);

  class OperationError extends NativeError {
    constructor() {
      throw new NativeTypeError("OperationError cannot be constructed");
    }
  }
  Object.defineProperty(OperationError.prototype, "name", { value: "OperationError" });
  Object.freeze(OperationError.prototype);
  Object.freeze(OperationError);

  // Encodes one logical argument object, or returns undefined so the host
  // rejects the call with its own invalid-input failure.
  const encodeArguments = (args) => {
    if (args.length > 1) return undefined;
    if (args.length === 0) return "{}";
    let encoded;
    try {
      encoded = nativeStringify(args[0]);
    } catch (_error) {
      return undefined;
    }
    return typeof encoded === "string" ? encoded : undefined;
  };

  const callablePath = (path, call) =>
    new Proxy(function coralOperationPath() {}, {
      get(_target, property) {
        if (typeof property !== "string" || property === "then") {
          return undefined;
        }
        return callablePath(`${path}.${property}`, call);
      },
      apply(_target, _receiver, args) {
        return call(path, encodeArguments(args));
      },
    });

  // Middleware runtime. Every execution point runs in an async-context frame
  // that V8 carries through Promise continuations, so the runtime knows
  // whether code runs for the Program, inside one Saved Function call, inside
  // one Middleware execution, or during Middleware module initialization.
  const PROGRAM = 0;
  const MIDDLEWARE = 1;
  const INITIALIZING = 2;
  const TOP_FRAME = freeze({ kind: PROGRAM, call: null });
  const frame = () => currentFrame() ?? TOP_FRAME;
  const withFrame = (next, body) => {
    const previous = currentFrame();
    enterFrame(next);
    try {
      return body();
    } finally {
      enterFrame(previous);
    }
  };

  const middleware = {
    installed: false,
    entries: [],
    run: [],
    function: [],
    operation: [],
    runContexts: [],
    // Null-prototype stores: Program code cannot reach them through accessors
    // it defines on Array.prototype or Object.prototype.
    wrappers: create(null),
  };
  const POLICIES = freeze(["enabled", "approval_required", "disabled"]);
  const isPolicy = (value) =>
    value === POLICIES[0] || value === POLICIES[1] || value === POLICIES[2];
  // A context has no prototype, so Program-defined Object.prototype accessors
  // cannot observe or forge a Middleware's private keys.
  const plainContext = () => create(null);
  const callContext = (scope, index) => {
    let context = scope.contexts[index];
    if (context === undefined) {
      context = plainContext();
      scope.contexts[index] = context;
    }
    return context;
  };

  // Runs one onion. `makeContext(entry)` builds that Middleware's own view of
  // the shared invocation state; `center` runs the wrapped invocation in the
  // invocation's own frame, never with a Middleware's helper privilege.
  const runChain = (entries, scope, target, invocationFrame, makeContext, center) => {
    const dispatch = (position) => {
      if (position === entries.length) {
        return withFrame(invocationFrame, center);
      }
      return execute(entries[position], position);
    };
    const execute = async (entry, position) => {
      const executionId = middlewareStarted(entry.index, scope, target);
      // The helper capability and next() live only as long as this execution.
      const live = { running: true };
      const executionFrame = freeze({
        kind: MIDDLEWARE,
        index: entry.index,
        executionId,
        live,
      });
      let called = false;
      let settled = false;
      const next = () => {
        if (!live.running) {
          return promiseReject(
            new NativeError("Middleware next() cannot run after the Middleware finished"),
          );
        }
        if (called) {
          return promiseReject(
            new NativeError("Middleware next() can be called at most once"),
          );
        }
        called = true;
        return (async () => {
          try {
            return await withFrame(invocationFrame, () => dispatch(position + 1));
          } finally {
            settled = true;
            middlewareContinued(executionId);
          }
        })();
      };
      let result;
      try {
        result = await withFrame(executionFrame, () =>
          entry.callable(makeContext(entry), next));
      } catch (error) {
        live.running = false;
        middlewareEnded(executionId, "failed", error);
        throw error;
      }
      live.running = false;
      if (called && !settled) {
        const error = new NativeError(
          "Middleware finished before its next() continuation settled; await or return next()",
        );
        middlewareEnded(executionId, "failed", error);
        throw error;
      }
      middlewareEnded(executionId, "succeeded", result);
      return result;
    };
    return dispatch(0);
  };

  const contexts = (entry, functionScope, operationScope) => {
    const value = create(null);
    defineProperty(value, "run", {
      value: middleware.runContexts[entry.index],
      enumerable: true,
    });
    if (functionScope !== null) {
      defineProperty(value, "function", {
        value: callContext(functionScope, entry.index),
        enumerable: true,
      });
    }
    if (operationScope !== null) {
      defineProperty(value, "operation", {
        value: callContext(operationScope, entry.index),
        enumerable: true,
      });
    }
    return freeze(value);
  };

  const invocationContext = (kind, path, state, entry, functionScope, operationScope) => {
    const context = create(null);
    defineProperties(context, {
      kind: { value: kind, enumerable: true },
      contexts: {
        value: contexts(entry, functionScope, operationScope),
        enumerable: true,
      },
    });
    if (path !== undefined) {
      defineProperty(context, "path", { value: path, enumerable: true });
    }
    if (hasOwn(state, "args")) {
      defineProperty(context, "args", {
        get: () => state.args,
        set: (value) => {
          state.args = value;
        },
        enumerable: true,
      });
    }
    if (state.originalPolicy !== undefined) {
      defineProperties(context, {
        originalPolicy: { value: state.originalPolicy, enumerable: true },
        currentPolicy: {
          get: () => state.currentPolicy,
          set: (value) => {
            if (!isPolicy(value)) {
              throw new NativeTypeError(
                "currentPolicy must be enabled, approval_required, or disabled",
              );
            }
            state.currentPolicy = value;
          },
          enumerable: true,
        },
      });
    }
    if (state.program !== undefined) {
      defineProperty(context, "program", { value: state.program, enumerable: true });
    }
    return preventExtensions(context);
  };

  const ordinaryOperation = (path, encoded) => {
    if (!middleware.installed) return invoke(path, encoded);
    const current = frame();
    if (current.kind === MIDDLEWARE) {
      return promiseReject(new NativeTypeError(
        "Middleware must call Operations through its own coral.providers",
      ));
    }
    if (current.kind === INITIALIZING) {
      return promiseReject(new NativeTypeError(
        "Middleware module initialization cannot invoke Operations",
      ));
    }
    if (middleware.operation.length === 0 || encoded === undefined) {
      return invoke(path, encoded);
    }
    return interceptOperation(path, encoded, current, callLocation() ?? null);
  };

  // `location` is the Program's call site, captured while its frame is still
  // on the stack; the center reaches the host only after Middleware runs.
  const interceptOperation = async (path, encoded, invocationFrame, location) => {
    const originalPolicy = await operationPolicy(path);
    const operationScope = { contexts: create(null) };
    const functionScope = invocationFrame.call;
    const state = {
      args: nativeParse(encoded),
      originalPolicy,
      currentPolicy: originalPolicy,
    };
    return runChain(
      middleware.operation,
      "operation",
      path,
      invocationFrame,
      (entry) => invocationContext(
        "operation",
        path,
        state,
        entry,
        functionScope,
        operationScope,
      ),
      () => {
        const arguments_ = encodeArguments([state.args]);
        return invoke(path, arguments_, state.currentPolicy, undefined, location);
      },
    );
  };

  const helperProviders = (index) =>
    callablePath("coral.providers", (path, encoded) => {
      const current = frame();
      if (current.kind !== MIDDLEWARE || current.index !== index || !current.live.running) {
        return promiseReject(new NativeTypeError(
          "A Middleware's coral.providers works only while that Middleware runs",
        ));
      }
      return invoke(path, encoded, undefined, current.executionId);
    });

  const wrapFunction = (index, path, callable) => {
    if (!middleware.installed || typeof callable !== "function") return callable;
    const existing = middleware.wrappers[index];
    if (existing !== undefined && existing.callable === callable) {
      return existing.wrapper;
    }
    const target = `coral.functions.${path}`;
    const intercepted = middleware.function.length > 0;
    const wrapper = function coralSavedFunction(...args) {
      const scope = { contexts: create(null) };
      const callFrame = freeze({ kind: PROGRAM, call: scope });
      if (!intercepted) {
        return withFrame(callFrame, () => apply(callable, this, args));
      }
      const receiver = this;
      const state = { args };
      return runChain(
        middleware.function,
        "function",
        target,
        callFrame,
        (entry) => invocationContext("function", target, state, entry, scope, null),
        () => {
          if (!isArray(state.args)) {
            throw new NativeTypeError("Saved Function ctx.args must be an array");
          }
          return apply(callable, receiver, state.args);
        },
      );
    };
    middleware.wrappers[index] = { callable, wrapper };
    return wrapper;
  };

  const startMiddleware = async (configuration, factories, program) => {
    const linkedFunctions = freeze([...configuration.linkedFunctions]);
    for (let index = 0; index < factories.length; index += 1) {
      const scopes = configuration.middleware[index];
      const initializing = freeze({ kind: INITIALIZING });
      const callable = withFrame(initializing, () => {
        const coral = create(null);
        defineProperties(coral, {
          OperationError: { value: OperationError, enumerable: true },
          providers: { value: helperProviders(index), enumerable: true },
          emit: { value: middlewareEmit(index), enumerable: true },
        });
        preventExtensions(coral);
        return factories[index](coral)();
      });
      if (typeof callable !== "function") {
        throw new NativeTypeError("Middleware default export is not callable");
      }
      const entry = freeze({ index, callable });
      middleware.entries.push(entry);
      middleware.runContexts.push(plainContext());
      if (scopes.run) middleware.run.push(entry);
      if (scopes.function) middleware.function.push(entry);
      if (scopes.operation) middleware.operation.push(entry);
    }
    middleware.installed = true;
    const programMetadata = freeze(create(null, {
      runId: { value: runId, enumerable: true },
      linkedFunctions: { value: linkedFunctions, enumerable: true },
    }));
    const state = { program: programMetadata };
    return runChain(
      middleware.run,
      "run",
      undefined,
      TOP_FRAME,
      (entry) => invocationContext("run", undefined, state, entry, null, null),
      () => program(wrapFunction),
    );
  };

  const runtime = freeze(create(null, {
    start: { value: startMiddleware },
  }));

  // Encodes one Emission at call time, so later Program changes to the value
  // cannot change what was emitted. Undefined lets the host reject it.
  const encodeEmission = (value) => {
    try {
      const encoded = nativeStringify(value);
      return typeof encoded === "string" ? encoded : undefined;
    } catch (_error) {
      return undefined;
    }
  };
  const append = (encoded, executionId, location) =>
    encoded === undefined
      ? appendEmission()
      : appendEmission(encoded, executionId, location);

  // Intercepted emits wait for every earlier emit, so Emissions keep the
  // Program's call order even when the Program does not await them.
  let emitTail;
  const inCallOrder = (task) => {
    const previous = emitTail;
    const queued = (async () => {
      if (previous !== undefined) {
        try {
          await previous;
        } catch (_error) {
          // An earlier emit's failure belongs to its own caller.
        }
      }
      return task();
    })();
    emitTail = queued;
    return queued;
  };

  // `coral.emit` is an Operation for Middleware: Operation Middleware wraps it
  // with `ctx.path === "coral.emit"` and the emitted value as `ctx.args`. Its
  // original policy is always `enabled`; Middleware may change the current
  // one, and the center applies the final policy to the final value. The
  // Program's Promise always resolves to undefined.
  const interceptEmit = (encoded, invocationFrame, location) => {
    const operationScope = { contexts: create(null) };
    const state = {
      args: nativeParse(encoded),
      originalPolicy: POLICIES[0],
      currentPolicy: POLICIES[0],
    };
    return runChain(
      middleware.operation,
      "operation",
      "coral.emit",
      invocationFrame,
      (entry) => invocationContext(
        "operation",
        "coral.emit",
        state,
        entry,
        invocationFrame.call,
        operationScope,
      ),
      () => {
        const final = encodeEmission(state.args);
        if (final === undefined || state.currentPolicy === POLICIES[0]) {
          return append(final, undefined, location);
        }
        return appendEmissionWithPolicy(final, state.currentPolicy, location);
      },
    );
  };

  const emit = (value) => {
    const encoded = encodeEmission(value);
    if (!middleware.installed) return append(encoded);
    const current = frame();
    if (current.kind === MIDDLEWARE) {
      return promiseReject(new NativeTypeError(
        "Middleware must emit through its own coral.emit",
      ));
    }
    if (current.kind === INITIALIZING) {
      return promiseReject(new NativeTypeError(
        "Middleware module initialization cannot emit",
      ));
    }
    if (encoded === undefined || middleware.operation.length === 0) return append(encoded);
    const location = callLocation() ?? null;
    return inCallOrder(async () => {
      await interceptEmit(encoded, current, location);
      return undefined;
    });
  };

  // A Middleware's own emit bypasses interception but names its execution,
  // so the Emission stays attributed and observable.
  const middlewareEmit = (index) => (value) => {
    const current = frame();
    if (current.kind !== MIDDLEWARE || current.index !== index || !current.live.running) {
      return promiseReject(new NativeTypeError(
        "A Middleware's coral.emit works only while that Middleware runs",
      ));
    }
    return append(encodeEmission(value), current.executionId);
  };

  const logMessage = (values) => {
    let message = "";
    for (let index = 0; index < values.length; index += 1) {
      if (index > 0) message += " ";
      message += nativeString(values[index]);
    }
    return message;
  };
  const writeLog = (level, values) => {
    appendLog(level, logMessage(values));
  };
  const console = Object.create(null);
  for (const name of ["debug", "info", "log", "warn", "error"]) {
    Object.defineProperty(console, name, {
      value: (...values) => writeLog(name === "log" ? "info" : name, values),
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
  Object.preventExtensions(console);
  Object.defineProperty(globalThis, "console", {
    value: console,
    enumerable: true,
    writable: false,
    configurable: false,
  });

  const coral = Object.create(null);
  Object.defineProperties(coral, {
    OperationError: {
      value: OperationError,
      enumerable: true,
      writable: false,
      configurable: false,
    },
    providers: {
      value: callablePath("coral.providers", ordinaryOperation),
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
  Object.defineProperty(globalThis, "coral", {
    value: coral,
    enumerable: true,
    writable: false,
    configurable: false,
  });

  // Returned only to the host; Programs cannot mint Coral Failures or reach
  // the Middleware runtime.
  const operationError = (code, message, traceId, stack) => {
    const error = new NativeError(message);
    setPrototypeOf(error, OperationError.prototype);
    defineProperties(error, {
      code: { __proto__: null, value: code },
      stack: { __proto__: null, value: stack, writable: true, configurable: true },
    });
    if (traceId !== undefined) {
      defineProperties(error, { traceId: { __proto__: null, value: traceId } });
    }
    return error;
  };
  return [operationError, runtime];
})();
