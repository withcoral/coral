// Returns every name a Program can read without declaring it, sorted: the own
// keys of the global object and of each prototype above Object.prototype.
const names = new Set<string>();
let object: object | null = globalThis;
while (object !== null && object !== Object.prototype) {
  for (const key of Reflect.ownKeys(object)) {
    names.add(String(key));
  }
  object = Object.getPrototypeOf(object);
}
if (object !== Object.prototype) {
  throw new Error('the global object does not inherit from Object.prototype');
}
return [...names].toSorted();
