// Returns every name a Program can read without declaring it, sorted: the own
// string keys of the global object and of each prototype above
// Object.prototype. Symbol keys are not names, so they are left out. The probe
// reads no global but `globalThis`, `Object` and `Error`. The runtime's own
// bootstrap needs those three, so the probe runs whenever any other global is
// missing and reports it.
const names: string[] = [];
let object: object | null = globalThis;
while (object !== null && object !== Object.prototype) {
  for (const name of Object.getOwnPropertyNames(object)) {
    if (!names.includes(name)) {
      names.push(name);
    }
  }
  object = Object.getPrototypeOf(object);
}
if (object !== Object.prototype) {
  throw new Error('the global object does not inherit from Object.prototype');
}
return names.toSorted();
