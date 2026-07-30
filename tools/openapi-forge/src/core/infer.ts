/**
 * Inferring schemas from recorded response samples.
 *
 * Providers rarely describe their response bodies in a machine-readable way,
 * but many record real responses — for tests, for SDK type generation, or as
 * documentation examples. Those samples are the most accurate description of a
 * response available, so the schema is read off them.
 *
 * Inference is deliberately shallow. Coral turns only a row object's *direct*
 * properties into typed SQL columns; anything nested becomes a single JSON
 * column regardless of how precisely it was described. Describing three levels
 * is therefore enough to type every column that exists, and describing more
 * would add bytes to the descriptor that nothing reads.
 */

import type { ObjectNode, SchemaNode, ScalarType } from './model.ts'

/**
 * Levels of nesting described.
 *
 * Three is what a wrapped list needs: the envelope, the row array, and the row
 * object's own properties. A property of a row is described as a bare object or
 * array, which is exactly how Coral treats it.
 */
export const DEFAULT_SCHEMA_DEPTH = 3

const UNKNOWN: SchemaNode = { kind: 'unknown' }

export function inferSchema(sample: unknown, maxDepth: number = DEFAULT_SCHEMA_DEPTH): SchemaNode {
  return inferAt(sample, 0, maxDepth)
}

function inferAt(sample: unknown, depth: number, maxDepth: number): SchemaNode {
  if (Array.isArray(sample)) {
    if (depth >= maxDepth) {
      return { kind: 'array', items: UNKNOWN }
    }
    // Samples list variants of the same thing, so the item schema is the
    // union of what every element declares rather than the first element's.
    const items = sample
      .map((element) => inferAt(element, depth + 1, maxDepth))
      .reduce<SchemaNode | undefined>(
        (left, right) => (left === undefined ? right : unify(left, right)),
        undefined,
      )
    return { kind: 'array', items: items ?? UNKNOWN }
  }

  if (sample !== null && typeof sample === 'object') {
    if (depth >= maxDepth) {
      return { kind: 'object', properties: {} }
    }
    const properties: Record<string, SchemaNode> = {}
    for (const [key, value] of Object.entries(sample)) {
      properties[key] = inferAt(value, depth + 1, maxDepth)
    }
    return { kind: 'object', properties }
  }

  return inferScalar(sample)
}

function inferScalar(sample: unknown): SchemaNode {
  switch (typeof sample) {
    case 'string':
      return { kind: 'scalar', type: 'string' }
    case 'boolean':
      return { kind: 'scalar', type: 'boolean' }
    case 'number':
      // A count typed as a float would be a worse answer than a float typed as
      // a count: the former is wrong for every row, the latter for values the
      // sample never showed. Overlays correct the second case.
      return { kind: 'scalar', type: Number.isInteger(sample) ? 'integer' : 'number' }
    default:
      // `null` says a field exists but not what it holds.
      return UNKNOWN
  }
}

/**
 * Merge two schemas for the same position.
 *
 * Disagreement collapses to `unknown` rather than picking a winner. A field
 * seen as a string in one variant and an object in another has no scalar type,
 * and asserting one would produce a column that fails on real data.
 */
export function unify(left: SchemaNode, right: SchemaNode): SchemaNode {
  if (left.kind === 'unknown') {
    return right
  }
  if (right.kind === 'unknown') {
    return left
  }

  if (left.kind === 'scalar' && right.kind === 'scalar') {
    if (left.type === right.type) {
      return left
    }
    // Integers are a subset of numbers, so this pair has a real answer.
    if (isNumeric(left.type) && isNumeric(right.type)) {
      return { kind: 'scalar', type: 'number' }
    }
    return UNKNOWN
  }

  if (left.kind === 'object' && right.kind === 'object') {
    return unifyObjects(left, right)
  }

  if (left.kind === 'array' && right.kind === 'array') {
    return { kind: 'array', items: unify(left.items, right.items) }
  }

  return UNKNOWN
}

function unifyObjects(left: ObjectNode, right: ObjectNode): ObjectNode {
  const properties: Record<string, SchemaNode> = { ...left.properties }
  for (const [key, schema] of Object.entries(right.properties)) {
    const existing = properties[key]
    properties[key] = existing === undefined ? schema : unify(existing, schema)
  }
  return { kind: 'object', properties }
}

function isNumeric(type: ScalarType): boolean {
  return type === 'integer' || type === 'number'
}
