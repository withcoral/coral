'use strict';
(() => {
  const forbiddenGlobals = globalThis.__coralForbiddenAmbientGlobals;
  Reflect.deleteProperty(globalThis, '__coralForbiddenAmbientGlobals');

  for (const name of forbiddenGlobals) {
    Reflect.deleteProperty(globalThis, name);
    if (typeof globalThis[name] !== 'undefined') {
      throw new Error(`failed to remove forbidden global: ${name}`);
    }
  }
})();
