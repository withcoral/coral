import { expect, it } from 'vitest'

import { jsonStructure, MAX_STRUCTURE_DEPTH, type JsonNode } from './json-structure'

/** Whether the platform's own parser reads `source` as JSON: the grammar's oracle. */
const parses = (source: string) => {
  try {
    JSON.parse(source)
    return true
  } catch {
    return false
  }
}

// Catches: a structure for text that is not JSON, or none for JSON, at any part of the
// grammar: strings and their escapes, numbers, literals, delimiters, whitespace and what
// follows the value.
it.each([
  ['{"a":1}', true],
  ['  [1, "x", true, false, null, {}, []]\r\n\t', true],
  ['"a\\"b\\\\c\\/d\\b\\f\\n\\r\\t"', true],
  ['"\\u0041\\uD83D\\uDE00"', true],
  ['-0', true],
  ['1.5e+10', true],
  ['12345678901234567890', true],
  ['0E-2', true],
  ['{"a":1,"a":2}', true],
  ['', false],
  [' ', false],
  ['"unclosed', false],
  ['"tab\there"', false],
  ['"line\nbreak"', false],
  ['"\\x"', false],
  ['"\\u12"', false],
  ['"\\uZZZZ"', false],
  ["'single'", false],
  ['01', false],
  ['1.', false],
  ['.5', false],
  ['+1', false],
  ['-', false],
  ['1e', false],
  ['0x1', false],
  ['NaN', false],
  ['Infinity', false],
  ['tru', false],
  ['nul', false],
  ['True', false],
  ['truex', false],
  ['{} x', false],
  ['1 2', false],
  ['{}}', false],
  ['[1]]', false],
  ['{"a" 1}', false],
  ['{"a":1,}', false],
  ['[1,]', false],
  ['[,1]', false],
  ['{,}', false],
  ['[1 2]', false],
  ['{"a":1 "b":2}', false],
  ['{1:2}', false],
  ['{"a":}', false],
  ['[1', false],
  ['{"a":', false],
  ['[}', false],
  ['{]', false],
  ['\u00a01', false],
  ['\f1', false],
  ['\ufeff{}', false],
  ['"\ud800"', true],
  ['"\\uD800"', true],
  ['[[], {}, [{}], {"a":[]}]', true],
])('reads %j as JSON: %s', (source, json) => {
  expect(parses(source)).toBe(json)
  expect(jsonStructure(source) !== 'invalid').toBe(json)
})

/** Each key and scalar of `node`, in order, as its kind and its exact retained text. */
const tokens = (node: JsonNode): string[] =>
  node.kind === 'object'
    ? node.entries.flatMap((entry) => [`key ${entry.key}`, ...tokens(entry.value)])
    : node.kind === 'array'
      ? node.items.flatMap(tokens)
      : [`${node.kind} ${node.raw}`]

// Catches: a parser that reserializes, which respells numbers and escapes and drops a
// duplicate key, or one whose offsets miss the source.
it('keeps every token exactly as retained, at its offset', () => {
  const source =
    ' {"k\\"q":"\\u0041\\\\", "n":[12345678901234567890, -0.0e+0, 1E2], "k\\"q":true, "z":null} '
  const structure = jsonStructure(source) as JsonNode
  expect(tokens(structure)).toEqual([
    'key "k\\"q"',
    'string "\\u0041\\\\"',
    'key "n"',
    'number 12345678901234567890',
    'number -0.0e+0',
    'number 1E2',
    'key "k\\"q"',
    'literal true',
    'key "z"',
    'literal null',
  ])
  const visit = (node: JsonNode) => {
    const text = source.slice(node.start, node.end)
    if (node.kind === 'object') expect([text[0], text.at(-1)]).toEqual(['{', '}'])
    else if (node.kind === 'array') expect([text[0], text.at(-1)]).toEqual(['[', ']'])
    else expect(text).toBe(node.raw)
    if (node.kind === 'object')
      for (const entry of node.entries) {
        expect(source.slice(entry.keyStart, entry.keyStart + entry.key.length)).toBe(entry.key)
        visit(entry.value)
      }
    if (node.kind === 'array') node.items.forEach(visit)
  }
  visit(structure)
  expect(source.slice(structure.start, structure.end)).toBe(source.trim())
})

const nested = (depth: number, inner = '') => '['.repeat(depth) + inner + ']'.repeat(depth)

// Catches: a depth bound that is off by one, a walk that exhausts the stack on deep JSON,
// or deep text that is not JSON shown as if it were.
it.each([
  [nested(MAX_STRUCTURE_DEPTH), 'structure'],
  [nested(MAX_STRUCTURE_DEPTH + 1), 'deep'],
  [nested(100_000), 'deep'],
  [`${nested(MAX_STRUCTURE_DEPTH + 1)}x`, 'invalid'],
  [nested(100_000, '1,'), 'invalid'],
  ['['.repeat(100_000), 'invalid'],
])('reads JSON nested %#', (source, expected) => {
  const structure = jsonStructure(source)
  expect(typeof structure === 'object' ? 'structure' : structure).toBe(expected)
})

// Catches: any text the grammar table above misses. Each one-character change of a JSON
// payload is JSON exactly when the platform's parser says so.
it('agrees with the platform parser on every one-character change', () => {
  const seed = '{"a":[1,-2.5e3,"x\\"\\u00e9",true,null],"b":{"c":false}}'
  const alphabet = ' "\\{}[],:-+.0123456789eEtrufalsn\tx'
  for (let at = 0; at <= seed.length; at++)
    for (const char of alphabet)
      for (const source of [
        seed.slice(0, at) + char + seed.slice(at + 1),
        seed.slice(0, at) + char + seed.slice(at),
        seed.slice(0, at) + seed.slice(at + 1),
      ])
        expect(jsonStructure(source) !== 'invalid', source).toBe(parses(source))
})
