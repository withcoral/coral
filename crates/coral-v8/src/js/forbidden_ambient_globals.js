'use strict';

// Shared by compiler capability checks and the execution-isolate bootstrap.
// Dynamic code constructors are handled separately because they are intrinsic
// ECMAScript globals that V8 disables rather than globals Coral can remove.
globalThis.__coralForbiddenAmbientGlobals = Object.freeze([
  // Node, Deno, Bun, packages, subprocesses, filesystem, and environment.
  'global',
  'process',
  'Buffer',
  'require',
  'module',
  'exports',
  '__filename',
  '__dirname',
  'Deno',
  'Bun',

  // Browser, network, storage, and credential surfaces.
  'window',
  'document',
  'navigator',
  'location',
  'history',
  'screen',
  'performance',
  'fetch',
  'XMLHttpRequest',
  'WebSocket',
  'EventSource',
  'crypto',
  'localStorage',
  'sessionStorage',
  'indexedDB',
  'caches',
  'Credential',
  'CredentialsContainer',
  'File',
  'FileReader',
  'showOpenFilePicker',

  // Timers, workers, and cross-agent communication.
  'setTimeout',
  'clearTimeout',
  'setInterval',
  'clearInterval',
  'setImmediate',
  'clearImmediate',
  'queueMicrotask',
  'Worker',
  'SharedWorker',
  'ServiceWorker',
  'importScripts',
  'MessageChannel',
  'BroadcastChannel',

  // Programs exchange JSON values. Removing every buffer/view constructor
  // prevents typed arrays from allocating buffers or recovering constructors.
  // The compiler checks authored references but retains its own intrinsics.
  'ArrayBuffer',
  'DataView',
  'Int8Array',
  'Uint8Array',
  'Uint8ClampedArray',
  'Int16Array',
  'Uint16Array',
  'Int32Array',
  'Uint32Array',
  'Float16Array',
  'Float32Array',
  'Float64Array',
  'BigInt64Array',
  'BigUint64Array',

  // Explicitly excluded execution features.
  'WebAssembly',
  'SharedArrayBuffer',
  'Atomics',
]);
