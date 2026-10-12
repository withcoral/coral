// The names Coral keeps Credential Fields under. An owner types labels; these
// turn a label into a name, and never into one another credential holds.

/** A Credential Field name Coral accepts, made from a label: `API key` becomes `api_key`. */
export function nameFor(label: string, taken: string[]) {
  const base = label
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 50)
  if (!base) return null
  const start = /^[a-z]/.test(base) ? base : `field_${base}`
  let name = start
  for (let number = 2; taken.includes(name); number += 1) name = `${start}_${number}`
  return name
}

/** The first `field_N` no credential has, so a new credential never replaces one. */
export function unusedFieldName(taken: string[]) {
  let number = taken.length + 1
  while (taken.includes(`field_${number}`)) number += 1
  return `field_${number}`
}
