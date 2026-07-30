import { describe, expect, it } from 'vitest'

import { pruneSample } from '../src/core/prune.ts'

describe('pruneSample', () => {
  it('keeps every property and its type within the depth limit', () => {
    const sample = {
      ok: true,
      messages: [{ ts: '1.2', text: 'hi', reply_count: 3, edited: { user: 'U1' } }],
      has_more: false,
    }

    expect(pruneSample(sample, 4)).toEqual(sample)
  })

  /** Beyond the limit only the JSON type survives, which is all typing needs. */
  it('replaces over-deep values with an empty value of the same type', () => {
    const sample = { a: { b: { c: { deep: { gone: 1 }, alsoGone: [1, 2] } } } }

    expect(pruneSample(sample, 3)).toEqual({ a: { b: { c: {} } } })
    expect(pruneSample(sample, 4)).toEqual({ a: { b: { c: { deep: {}, alsoGone: [] } } } })
  })

  it('keeps scalars at any depth', () => {
    expect(pruneSample({ a: { b: { c: { d: 'kept' } } } }, 3)).toEqual({ a: { b: { c: {} } } })
    expect(pruneSample({ a: { b: { c: 'kept' } } }, 3)).toEqual({ a: { b: { c: 'kept' } } })
  })

  /**
   * Samples enumerate variants, so one element per distinct shape keeps every
   * property that can appear while dropping the repetition that makes the
   * files enormous.
   */
  it('keeps one array element per distinct shape', () => {
    const sample = {
      messages: [
        { ts: '1', text: 'a' },
        { ts: '2', text: 'b' },
        { ts: '3', text: 'c', subtype: 'bot_message' },
      ],
    }

    expect(pruneSample(sample, 4)).toEqual({
      messages: [
        { ts: '1', text: 'a' },
        { ts: '3', text: 'c', subtype: 'bot_message' },
      ],
    })
  })

  it('treats key order as irrelevant when comparing shapes', () => {
    const sample = {
      items: [
        { a: 1, b: 2 },
        { b: 3, a: 4 },
      ],
    }

    expect(pruneSample(sample, 4)).toEqual({ items: [{ a: 1, b: 2 }] })
  })

  it('distinguishes scalar element types', () => {
    expect(pruneSample({ values: [1, 2, 'three', true, null] }, 4)).toEqual({
      values: [1, 'three', true, null],
    })
  })

  it('is idempotent, so re-pruning a stored sample changes nothing', () => {
    const sample = { ok: true, channels: [{ id: 'C1', purpose: { value: 'x' } }] }
    const once = pruneSample(sample, 3)

    expect(pruneSample(once, 3)).toEqual(once)
  })

  it('handles an empty envelope', () => {
    expect(pruneSample({}, 4)).toEqual({})
    expect(pruneSample([], 4)).toEqual([])
  })
})
