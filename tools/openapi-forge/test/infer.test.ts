import { describe, expect, it } from 'vitest'

import { inferSchema, unify } from '../src/core/infer.ts'

describe('inferSchema', () => {
  it('types the properties of a wrapped-list envelope', () => {
    const schema = inferSchema({
      ok: true,
      channels: [{ id: 'C1', num_members: 4, is_archived: false }],
      response_metadata: { next_cursor: 'abc' },
    })

    expect(schema).toEqual({
      kind: 'object',
      properties: {
        ok: { kind: 'scalar', type: 'boolean' },
        channels: {
          kind: 'array',
          items: {
            kind: 'object',
            properties: {
              id: { kind: 'scalar', type: 'string' },
              num_members: { kind: 'scalar', type: 'integer' },
              is_archived: { kind: 'scalar', type: 'boolean' },
            },
          },
        },
        response_metadata: {
          kind: 'object',
          properties: { next_cursor: { kind: 'scalar', type: 'string' } },
        },
      },
    })
  })

  /**
   * Coral turns a row's nested object into one JSON column however precisely
   * it is described, so the default depth stops exactly where the detail stops
   * being read.
   */
  it('describes a row property as a bare object or array', () => {
    const schema = inferSchema({
      channels: [{ id: 'C1', topic: { value: 'hi' }, previous_names: ['old'] }],
    })

    const items = schema.kind === 'object' ? schema.properties.channels : undefined
    const row = items?.kind === 'array' ? items.items : undefined
    const properties = row?.kind === 'object' ? row.properties : {}

    expect(properties.topic).toEqual({ kind: 'object', properties: {} })
    expect(properties.previous_names).toEqual({ kind: 'array', items: { kind: 'unknown' } })
  })

  it('unions across array elements rather than trusting the first', () => {
    const schema = inferSchema({
      messages: [
        { ts: '1', text: 'a' },
        { ts: '2', subtype: 'bot_message', bot_id: 'B1' },
      ],
    })

    const messages = schema.kind === 'object' ? schema.properties.messages : undefined
    const row = messages?.kind === 'array' ? messages.items : undefined

    expect(row?.kind === 'object' ? Object.keys(row.properties).toSorted() : []).toEqual([
      'bot_id',
      'subtype',
      'text',
      'ts',
    ])
  })

  it('distinguishes integers from other numbers', () => {
    const schema = inferSchema({ count: 4, score: 0.5 })
    const properties = schema.kind === 'object' ? schema.properties : {}

    expect(properties.count).toEqual({ kind: 'scalar', type: 'integer' })
    expect(properties.score).toEqual({ kind: 'scalar', type: 'number' })
  })

  /** A null says the field exists but not what it holds. */
  it('treats null as unknown', () => {
    const schema = inferSchema({ maybe: null })

    expect(schema.kind === 'object' ? schema.properties.maybe : undefined).toEqual({
      kind: 'unknown',
    })
  })

  it('describes an empty array without inventing an item type', () => {
    const schema = inferSchema({ items: [] })

    expect(schema.kind === 'object' ? schema.properties.items : undefined).toEqual({
      kind: 'array',
      items: { kind: 'unknown' },
    })
  })
})

describe('unify', () => {
  it('widens integer and number to number', () => {
    expect(unify({ kind: 'scalar', type: 'integer' }, { kind: 'scalar', type: 'number' })).toEqual({
      kind: 'scalar',
      type: 'number',
    })
  })

  /**
   * Asserting a type here would produce a column that fails on real data, so
   * disagreement collapses rather than picking a winner.
   */
  it('collapses genuinely conflicting scalar types to unknown', () => {
    expect(unify({ kind: 'scalar', type: 'string' }, { kind: 'scalar', type: 'boolean' })).toEqual({
      kind: 'unknown',
    })
  })

  it('collapses a scalar merged with a container to unknown', () => {
    expect(unify({ kind: 'scalar', type: 'string' }, { kind: 'object', properties: {} })).toEqual({
      kind: 'unknown',
    })
  })

  it('lets a known schema win over an unknown one', () => {
    const known = { kind: 'scalar', type: 'string' } as const

    expect(unify({ kind: 'unknown' }, known)).toEqual(known)
    expect(unify(known, { kind: 'unknown' })).toEqual(known)
  })

  it('merges object properties from both sides', () => {
    const merged = unify(
      { kind: 'object', properties: { a: { kind: 'scalar', type: 'string' } } },
      { kind: 'object', properties: { b: { kind: 'scalar', type: 'integer' } } },
    )

    expect(merged).toEqual({
      kind: 'object',
      properties: {
        a: { kind: 'scalar', type: 'string' },
        b: { kind: 'scalar', type: 'integer' },
      },
    })
  })
})
