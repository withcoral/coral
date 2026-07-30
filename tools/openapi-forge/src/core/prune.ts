/**
 * Structural pruning of recorded response samples.
 *
 * Providers record samples that enumerate every variant a response can carry,
 * which makes them enormous: Slack's `conversations.history` sample is a single
 * message object 875 KB in size and seventeen levels deep, almost all of it
 * expanded Block Kit. Coral types only the direct properties of a row, so
 * essentially none of that depth is ever read.
 *
 * Pruning trims a sample to a bounded depth while preserving the JSON *type* of
 * every value at the boundary, which is all that typing needs. In practice this
 * takes that 875 KB sample to about 2.4 KB with all 34 message properties and
 * their types intact.
 *
 * This is deliberately structural rather than inferential. The pruned sample is
 * still a JSON document, so schema inference stays in the build stage and its
 * rules can change without re-fetching anything.
 */

export const DEFAULT_MAX_DEPTH = 4

/**
 * Shape signature used to drop repeated array elements.
 *
 * Samples list variants — several message subtypes, several block kinds — and
 * the interesting thing about each is its shape, not its position. Keeping one
 * element per distinct shape keeps every property that can appear.
 */
function shapeSignature(value: unknown): string {
  if (Array.isArray(value)) {
    return 'array'
  }
  if (value !== null && typeof value === 'object') {
    return `object:${Object.keys(value).toSorted().join(',')}`
  }
  return `scalar:${value === null ? 'null' : typeof value}`
}

/**
 * Prune `value` to `maxDepth` levels.
 *
 * At the boundary an object becomes `{}` and an array becomes `[]`. Both still
 * declare their type, so inference can tell "this field is an object" from
 * "this field is an array" — the only distinction that survives into a Coral
 * column anyway, since both become JSON.
 */
export function pruneSample(value: unknown, maxDepth: number = DEFAULT_MAX_DEPTH): unknown {
  return pruneAt(value, 0, maxDepth)
}

function pruneAt(value: unknown, depth: number, maxDepth: number): unknown {
  if (Array.isArray(value)) {
    if (depth >= maxDepth) {
      return []
    }
    const seen = new Set<string>()
    const kept: unknown[] = []
    for (const element of value) {
      const signature = shapeSignature(element)
      if (seen.has(signature)) {
        continue
      }
      seen.add(signature)
      kept.push(pruneAt(element, depth + 1, maxDepth))
    }
    return kept
  }

  if (value !== null && typeof value === 'object') {
    if (depth >= maxDepth) {
      return {}
    }
    const kept: Record<string, unknown> = {}
    for (const [key, property] of Object.entries(value)) {
      kept[key] = pruneAt(property, depth + 1, maxDepth)
    }
    return kept
  }

  return value
}
