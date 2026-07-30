import { parse } from 'yaml'
import { describe, expect, it } from 'vitest'

import type { ApiModel, Operation, SchemaNode } from '../src/core/model.ts'
import { assertImportable, emitOpenApi, EmitError, OPENAPI_VERSION } from '../src/core/emit.ts'

function operation(overrides: Partial<Operation> = {}): Operation {
  return {
    id: 'conversations.list',
    operationId: 'conversations/list',
    group: 'conversations',
    path: '/conversations.list',
    method: 'get',
    summary: 'Lists all channels.',
    description: 'Lists all channels.',
    deprecated: false,
    parameters: [],
    response: { kind: 'object', properties: { ok: { kind: 'scalar', type: 'boolean' } } },
    warnings: [],
    ...overrides,
  }
}

function model(operations: Operation[]): ApiModel {
  return {
    api: 'demo',
    title: 'Demo',
    description: 'Demo API.',
    serverUrl: 'https://example.com/api',
    operations,
    warnings: [],
  }
}

function emit(operations: Operation[]): Record<string, never> {
  return parse(emitOpenApi(model(operations), { version: '2026-07-30' })) as Record<string, never>
}

function rowArray(component: string, properties: Record<string, SchemaNode>): SchemaNode {
  return {
    kind: 'object',
    properties: {
      ok: { kind: 'scalar', type: 'boolean' },
      rows: { kind: 'array', items: { kind: 'object', component, properties } },
    },
  }
}

describe('emitOpenApi', () => {
  it('emits a 3.0.3 document with servers, tags and paths', () => {
    const document = emit([operation()]) as never as {
      openapi: string
      servers: { url: string }[]
      tags: { name: string }[]
      paths: Record<string, Record<string, { operationId: string }>>
    }

    expect(document.openapi).toBe(OPENAPI_VERSION)
    expect(document.servers).toEqual([{ url: 'https://example.com/api' }])
    expect(document.tags).toEqual([{ name: 'conversations' }])
    expect(document.paths['/conversations.list']?.get?.operationId).toBe('conversations/list')
  })

  /**
   * Scopes and the rate-limit tier are not part of the descriptor's contract —
   * Coral ignores securitySchemes entirely — but they are what a user hitting
   * missing_scope needs, and the description is where they will look.
   */
  it('folds scopes, rate limit and docs URL into the description', () => {
    const document = emit([
      operation({
        scopes: { bot: ['channels:read'], user: ['channels:read', 'groups:read'] },
        rateLimitTier: 'Tier 2: 20+ per minute',
        docsUrl: 'https://docs.example.com/list',
      }),
    ]) as never as { paths: Record<string, Record<string, { description: string }>> }

    const description = document.paths['/conversations.list']?.get?.description ?? ''
    expect(description).toContain('Requires one of the scopes: channels:read, groups:read.')
    expect(description).toContain('Rate limit: Tier 2: 20+ per minute.')
    expect(description).toContain('See https://docs.example.com/list.')
  })

  it('does not double the full stop on a tier that is already a sentence', () => {
    const document = emit([
      operation({ rateLimitTier: 'Special rate limits apply.' }),
    ]) as never as { paths: Record<string, Record<string, { description: string }>> }

    expect(document.paths['/conversations.list']?.get?.description).toContain(
      'Rate limit: Special rate limits apply.',
    )
    expect(document.paths['/conversations.list']?.get?.description).not.toContain('apply..')
  })

  it('emits parameters with their type, requiredness and default', () => {
    const document = emit([
      operation({
        parameters: [
          {
            name: 'limit',
            in: 'query',
            required: false,
            description: 'How many.',
            schema: { kind: 'scalar', type: 'integer' },
            default: 100,
            example: 20,
          },
        ],
      }),
    ]) as never as {
      paths: Record<string, Record<string, { parameters: Record<string, unknown>[] }>>
    }

    expect(document.paths['/conversations.list']?.get?.parameters?.[0]).toEqual({
      name: 'limit',
      in: 'query',
      required: false,
      schema: { type: 'integer', default: 100 },
      description: 'How many.',
      example: 20,
    })
  })

  /** Two operations returning the same shape should describe it once. */
  it('shares one component between identical row schemas', () => {
    const properties: Record<string, SchemaNode> = { id: { kind: 'scalar', type: 'string' } }
    const document = emit([
      operation({ response: rowArray('Message', properties) }),
      operation({
        id: 'conversations.replies',
        operationId: 'conversations/replies',
        path: '/conversations.replies',
        response: rowArray('Message', { ...properties }),
      }),
    ]) as never as { components: { schemas: Record<string, unknown> } }

    expect(Object.keys(document.components.schemas)).toEqual(['Message'])
  })

  /**
   * Merging two different shapes under one name would give one of them the
   * wrong columns, so they are disambiguated instead.
   */
  it('disambiguates two different shapes that want the same name', () => {
    const document = emit([
      operation({ response: rowArray('Item', { id: { kind: 'scalar', type: 'string' } }) }),
      operation({
        id: 'search.messages',
        operationId: 'search/messages',
        group: 'search',
        path: '/search.messages',
        response: rowArray('Item', { text: { kind: 'scalar', type: 'string' } }),
      }),
    ]) as never as { components: { schemas: Record<string, unknown> } }

    expect(Object.keys(document.components.schemas).toSorted()).toEqual(['Item', 'SearchItem'])
  })

  it('emits an object with no described properties as a bare object', () => {
    const document = emit([
      operation({
        response: { kind: 'object', properties: { topic: { kind: 'object', properties: {} } } },
      }),
    ]) as never as { paths: Record<string, Record<string, never>> }
    const schema = schemaOf(document, '/conversations.list')

    expect(schema.properties.topic).toEqual({ type: 'object' })
  })

  /** A value whose type is unknown gets a schema that asserts nothing. */
  it('emits an unknown value as an empty schema', () => {
    const document = emit([
      operation({ response: { kind: 'object', properties: { maybe: { kind: 'unknown' } } } }),
    ]) as never as { paths: Record<string, Record<string, never>> }
    const schema = schemaOf(document, '/conversations.list')

    expect(schema.properties.maybe).toEqual({})
  })

  it('is deterministic for the same model', () => {
    const operations = [operation(), operation({ path: '/users.list', operationId: 'users/list' })]

    expect(emitOpenApi(model(operations), { version: '2026-07-30' })).toBe(
      emitOpenApi(model(operations), { version: '2026-07-30' }),
    )
  })

  it('rejects two operations claiming the same method and path', () => {
    expect(() => emit([operation(), operation({ operationId: 'conversations/other' })])).toThrow(
      /two operations claim GET \/conversations\.list/,
    )
  })
})

describe('assertImportable', () => {
  const base = {
    openapi: OPENAPI_VERSION,
    paths: { '/a': { get: { operationId: 'a/one' } } },
  }

  it('accepts a well-formed document', () => {
    expect(() => assertImportable({ ...base })).not.toThrow()
  })

  it('rejects a version Coral does not accept', () => {
    expect(() => assertImportable({ ...base, openapi: '3.1.0' })).toThrow(EmitError)
  })

  /**
   * Composition keywords are the quiet failure: schema import cannot read them
   * and row-path inference gives up entirely when it meets one.
   */
  it.each(['allOf', 'oneOf', 'anyOf', 'not'])('rejects %s anywhere in the document', (keyword) => {
    const document = {
      ...base,
      components: { schemas: { Thing: { [keyword]: [{ type: 'object' }] } } },
    }

    expect(() => assertImportable(document)).toThrow(new RegExp(`uses '${keyword}'`))
  })

  it('rejects an external reference', () => {
    const document = {
      ...base,
      paths: { '/a': { get: { operationId: 'a/one', schema: { $ref: 'other.yaml#/Thing' } } } },
    }

    expect(() => assertImportable(document)).toThrow(/external reference/)
  })

  it('rejects a local reference that does not resolve', () => {
    const document = {
      ...base,
      paths: {
        '/a': { get: { operationId: 'a/one', schema: { $ref: '#/components/schemas/Gone' } } },
      },
    }

    expect(() => assertImportable(document)).toThrow(/does not resolve/)
  })

  it('accepts a local reference that resolves', () => {
    const document = {
      ...base,
      paths: {
        '/a': { get: { operationId: 'a/one', schema: { $ref: '#/components/schemas/Thing' } } },
      },
      components: { schemas: { Thing: { type: 'object' } } },
    }

    expect(() => assertImportable(document)).not.toThrow()
  })

  /** A collision is a hard error at import, so it is caught before writing. */
  it('rejects operationIds that collide once normalized', () => {
    const document = {
      ...base,
      paths: {
        '/a': { get: { operationId: 'a/one' } },
        '/b': { get: { operationId: 'a.one' } },
      },
    }

    expect(() => assertImportable(document)).toThrow(/collide once normalized/)
  })
})

function schemaOf(
  document: { paths: Record<string, Record<string, never>> },
  path: string,
): { properties: Record<string, unknown> } {
  const get = document.paths[path]?.get as unknown as {
    responses: Record<string, { content: Record<string, { schema: never }> }>
  }
  return get.responses['200']?.content['application/json']?.schema as unknown as {
    properties: Record<string, unknown>
  }
}
