// Returns every name a Program can read without declaring it, sorted: the own
// string keys of the global object and of each prototype above
// Object.prototype. Symbol keys are not names, so they are left out.
const names = new Set<string>();
let object: object | null = globalThis;
while (object !== null && object !== Object.prototype) {
  for (const name of Object.getOwnPropertyNames(object)) {
    names.add(name);
  }
  object = Object.getPrototypeOf(object);
}
if (object !== Object.prototype) {
  throw new Error('the global object does not inherit from Object.prototype');
}
return [...names].toSorted();
