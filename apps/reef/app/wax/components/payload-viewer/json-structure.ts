/**
 * JSON as its retained tokens. Each scalar and key keeps its exact source text, so
 * large integers, number spelling, escapes and duplicate keys survive. Nothing is
 * reserialized.
 */
export type JsonNode =
  | { kind: 'object'; start: number; end: number; entries: JsonEntry[] }
  | { kind: 'array'; start: number; end: number; items: JsonNode[] }
  | { kind: 'string' | 'number' | 'literal'; start: number; end: number; raw: string }

/** One property, its key as retained, quotes included. */
export type JsonEntry = { key: string; keyStart: number; value: JsonNode }

/** Past this depth, JSON shows only as retained: walking it could exhaust the stack. */
export const MAX_STRUCTURE_DEPTH = 200

type Container = Extract<JsonNode, { kind: 'object' | 'array' }>
type Key = { key: string; keyStart: number }
/** A container still open, and the key its next value takes. */
type OpenContainer = { node: Container; key: Key | null }

// Each matches at `lastIndex` only: a run of plain string characters, a number, a literal,
// and JSON's own whitespace.
// oxlint-disable-next-line no-control-regex -- JSON strings may not hold control characters.
const STRING_RUN = /[^"\\\u0000-\u001f]*/y
const ESCAPE = /\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4})/y
const NUMBER = /-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/y
const LITERAL = /true|false|null/y
const SPACE = /[ \t\n\r]*/y

/**
 * The structure of `source`, read in one pass that checks it against JSON's grammar;
 * 'invalid' when it is not JSON, 'deep' when it is but nests past MAX_STRUCTURE_DEPTH.
 * Open containers sit on a list, not the call stack, so any depth reads safely.
 */
export function jsonStructure(source: string): JsonNode | 'invalid' | 'deep' {
  let at = 0
  /** Whether `pattern` matches at `at`, which then moves past it. */
  const skip = (pattern: RegExp) => {
    pattern.lastIndex = at
    if (!pattern.test(source)) return false
    at = pattern.lastIndex
    return true
  }
  const space = () => {
    // Most values have no space before them.
    const char = source.charCodeAt(at)
    if (char === 32 || char === 9 || char === 10 || char === 13) skip(SPACE)
  }
  /** A string at `at`, quotes included, or null. */
  const string = () => {
    const from = at
    if (source[at] !== '"') return null
    at++
    for (;;) {
      skip(STRING_RUN)
      if (source[at] === '"') return source.slice(from, ++at)
      if (!skip(ESCAPE)) return null
    }
  }
  /** A key and its colon, at `at`, or null. */
  const key = (): Key | null => {
    const keyStart = at
    const text = string()
    space()
    if (text === null || source[at] !== ':') return null
    at++
    return { key: text, keyStart }
  }
  const open: OpenContainer[] = []
  let deepest = 0
  for (;;) {
    space()
    const start = at
    const char = source[at]
    let value: JsonNode
    if (char === '{' || char === '[') {
      at++
      const node: Container =
        char === '{'
          ? { kind: 'object', start, end: start, entries: [] }
          : { kind: 'array', start, end: start, items: [] }
      const container: OpenContainer = { node, key: null }
      open.push(container)
      deepest = Math.max(deepest, open.length)
      space()
      if (source[at] !== (char === '{' ? '}' : ']')) {
        if (char === '{') {
          container.key = key()
          if (!container.key) return 'invalid'
        }
        continue
      }
      at++
      open.pop()
      node.end = at
      value = node
    } else {
      const kind =
        char === '"'
          ? string() !== null && 'string'
          : (skip(NUMBER) && 'number') || (skip(LITERAL) && 'literal')
      if (!kind) return 'invalid'
      value = { kind, start, end: at, raw: source.slice(start, at) }
    }
    for (;;) {
      const parent = open.at(-1)
      if (!parent) {
        space()
        if (at < source.length) return 'invalid'
        return deepest > MAX_STRUCTURE_DEPTH ? 'deep' : value
      }
      if (parent.node.kind === 'object') parent.node.entries.push({ ...parent.key!, value })
      else parent.node.items.push(value)
      space()
      if (source[at] === ',') {
        at++
        space()
        if (parent.node.kind === 'object') {
          parent.key = key()
          if (!parent.key) return 'invalid'
        }
        break
      }
      if (source[at] !== (parent.node.kind === 'object' ? '}' : ']')) return 'invalid'
      at++
      open.pop()
      parent.node.end = at
      value = parent.node
    }
  }
}

/** How many values `node` holds, itself included. */
export function valueCount(node: JsonNode): number {
  if (node.kind === 'object')
    return node.entries.reduce((sum, entry) => sum + valueCount(entry.value), 1)
  if (node.kind === 'array') return node.items.reduce((sum, item) => sum + valueCount(item), 1)
  return 1
}
