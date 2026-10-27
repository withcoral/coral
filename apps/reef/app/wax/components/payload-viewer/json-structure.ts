/**
 * JSON as its retained tokens. Each scalar and key keeps its exact source text, so
 * large integers, number spelling, escapes and duplicate keys survive. Nothing is
 * reserialized; JSON.parse only checks that the source is JSON.
 */
export type JsonNode =
  | { kind: 'object'; start: number; end: number; entries: JsonEntry[] }
  | { kind: 'array'; start: number; end: number; items: JsonNode[] }
  | { kind: 'string' | 'number' | 'literal'; start: number; end: number; raw: string }

/** One property, its key as retained, quotes included. */
export type JsonEntry = { key: string; keyStart: number; value: JsonNode }

/** Past this depth, JSON shows only as retained: walking it could exhaust the stack. */
export const MAX_STRUCTURE_DEPTH = 200

/** The structure of `source`; 'invalid' when it is not JSON, 'deep' past the depth bound. */
export function jsonStructure(source: string): JsonNode | 'invalid' | 'deep' {
  try {
    JSON.parse(source)
  } catch {
    return 'invalid'
  }
  // A cheap bound before the walk: no more nested containers than open brackets.
  let depth = 0
  let deepest = 0
  for (let at = 0; at < source.length; at++) {
    const char = source[at]
    if (char === '"') {
      at++
      while (source[at] !== '"') at += source[at] === '\\' ? 2 : 1
    } else if (char === '{' || char === '[') deepest = Math.max(deepest, ++depth)
    else if (char === '}' || char === ']') depth--
  }
  if (deepest > MAX_STRUCTURE_DEPTH) return 'deep'
  let at = 0
  const space = () => {
    while (at < source.length && ' \t\n\r'.includes(source[at]!)) at++
  }
  const string = () => {
    const start = at
    at++
    while (source[at] !== '"') at += source[at] === '\\' ? 2 : 1
    at++
    return source.slice(start, at)
  }
  const value = (): JsonNode => {
    space()
    const start = at
    const char = source[at]
    if (char === '{') {
      at++
      const entries: JsonEntry[] = []
      space()
      while (source[at] !== '}') {
        space()
        const keyStart = at
        const key = string()
        space()
        at++ // :
        entries.push({ key, keyStart, value: value() })
        space()
        if (source[at] === ',') at++
        space()
      }
      at++
      return { kind: 'object', start, end: at, entries }
    }
    if (char === '[') {
      at++
      const items: JsonNode[] = []
      space()
      while (source[at] !== ']') {
        items.push(value())
        space()
        if (source[at] === ',') at++
        space()
      }
      at++
      return { kind: 'array', start, end: at, items }
    }
    if (char === '"') {
      const raw = string()
      return { kind: 'string', start, end: at, raw }
    }
    while (at < source.length && !' \t\n\r,]}'.includes(source[at]!)) at++
    const raw = source.slice(start, at)
    return { kind: /^[-\d]/.test(raw) ? 'number' : 'literal', start, end: at, raw }
  }
  return value()
}

/** How many values `node` holds, itself included. */
export function valueCount(node: JsonNode): number {
  if (node.kind === 'object')
    return node.entries.reduce((sum, entry) => sum + valueCount(entry.value), 1)
  if (node.kind === 'array') return node.items.reduce((sum, item) => sum + valueCount(item), 1)
  return 1
}
