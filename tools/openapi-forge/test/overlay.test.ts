import { mkdtemp, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import type { ApiModel, Operation, Parameter } from '../src/core/model.ts'
import { applyOverlay, loadOverlay, OverlayError } from '../src/core/overlay.ts'

function parameter(overrides: Partial<Parameter> = {}): Parameter {
  return {
    name: 'limit',
    in: 'query',
    required: false,
    description: 'How many.',
    schema: { kind: 'scalar', type: 'number' },
    default: 100,
    ...overrides,
  }
}

function operation(overrides: Partial<Operation> = {}): Operation {
  return {
    id: 'conversations.list',
    operationId: 'conversations/list',
    group: 'conversations',
    path: '/conversations.list',
    method: 'get',
    summary: 'Lists channels.',
    description: 'Lists channels.',
    deprecated: false,
    parameters: [parameter()],
    response: { kind: 'object', properties: {} },
    warnings: [],
    ...overrides,
  }
}

function model(operations: Operation[]): ApiModel {
  return {
    api: 'slack',
    title: 'Slack',
    description: 'Slack.',
    serverUrl: 'https://slack.com/api',
    operations,
    warnings: [],
  }
}

function parametersOf(result: ApiModel, id: string): Parameter[] {
  return result.operations.find((candidate) => candidate.id === id)?.parameters ?? []
}

describe('applyOverlay', () => {
  it('applies a global parameter override wherever the parameter appears', () => {
    const result = applyOverlay(
      model([operation(), operation({ id: 'users.list', path: '/users.list' })]),
      { parameters: { limit: { type: 'integer' } } },
    )

    for (const id of ['conversations.list', 'users.list']) {
      expect(parametersOf(result, id)[0]?.schema.type, id).toBe('integer')
    }
  })

  /** The narrower statement wins; otherwise a global could never be excepted. */
  it('lets a per-operation override beat the global one', () => {
    const result = applyOverlay(
      model([operation(), operation({ id: 'users.list', path: '/users.list' })]),
      {
        parameters: { limit: { type: 'integer' } },
        operations: { 'users.list': { parameters: { limit: { type: 'number' } } } },
      },
    )

    expect(parametersOf(result, 'conversations.list')[0]?.schema.type).toBe('integer')
    expect(parametersOf(result, 'users.list')[0]?.schema.type).toBe('number')
  })

  it('overrides description, requiredness and default', () => {
    const result = applyOverlay(model([operation()]), {
      parameters: { limit: { description: 'Page size.', required: true, default: 200 } },
    })

    expect(parametersOf(result, 'conversations.list')[0]).toMatchObject({
      description: 'Page size.',
      required: true,
      default: 200,
    })
  })

  it('removes a default when the override is null', () => {
    const result = applyOverlay(model([operation()]), { parameters: { limit: { default: null } } })

    expect(parametersOf(result, 'conversations.list')[0]?.default).toBeUndefined()
  })

  it('drops a parameter', () => {
    const result = applyOverlay(model([operation()]), { parameters: { limit: { drop: true } } })

    expect(parametersOf(result, 'conversations.list')).toEqual([])
  })

  it('drops an operation', () => {
    const result = applyOverlay(
      model([operation(), operation({ id: 'users.list', path: '/users.list' })]),
      { operations: { 'users.list': { drop: true } } },
    )

    expect(result.operations.map((candidate) => candidate.id)).toEqual(['conversations.list'])
  })

  it('overrides operation prose', () => {
    const result = applyOverlay(model([operation()]), {
      operations: { 'conversations.list': { summary: 'Channels.', deprecated: true } },
    })

    expect(result.operations[0]).toMatchObject({ summary: 'Channels.', deprecated: true })
  })

  /**
   * An override exists because upstream was wrong. Once upstream is fixed or
   * renamed, a correction that quietly stops applying is indistinguishable from
   * one still doing its job — so it fails instead.
   */
  it('rejects a global parameter override that matches nothing', () => {
    expect(() =>
      applyOverlay(model([operation()]), { parameters: { cursor: { type: 'string' } } }),
    ).toThrow(/parameters\.cursor/)
  })

  it('rejects an operation override that matches nothing', () => {
    expect(() =>
      applyOverlay(model([operation()]), { operations: { 'chat.postMessage': { summary: 'x' } } }),
    ).toThrow(/operations\.chat\.postMessage/)
  })

  it('rejects a per-operation parameter override that matches nothing', () => {
    expect(() =>
      applyOverlay(model([operation()]), {
        operations: { 'conversations.list': { parameters: { nope: { type: 'string' } } } },
      }),
    ).toThrow(/operations\.conversations\.list\.parameters\.nope/)
  })

  it('reports every unmatched entry at once', () => {
    const error = (() => {
      try {
        applyOverlay(model([operation()]), {
          parameters: { cursor: { type: 'string' } },
          operations: { 'chat.postMessage': { summary: 'x' } },
        })
        return undefined
      } catch (thrown) {
        return thrown as Error
      }
    })()

    expect(error?.message).toContain('parameters.cursor')
    expect(error?.message).toContain('operations.chat.postMessage')
  })

  it('leaves a model with no overlay untouched', () => {
    const source = model([operation()])

    expect(applyOverlay(source, {})).toEqual(source)
  })
})

describe('loadOverlay', () => {
  it('treats a missing file as no corrections', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'forge-overlay-'))

    expect(await loadOverlay(join(dir, 'overlay.yaml'))).toEqual({})
  })

  it('treats an empty file as no corrections', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'forge-overlay-'))
    const path = join(dir, 'overlay.yaml')
    await writeFile(path, '# only a comment\n')

    expect(await loadOverlay(path)).toEqual({})
  })

  it('rejects a document that is not a mapping', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'forge-overlay-'))
    const path = join(dir, 'overlay.yaml')
    await writeFile(path, '- a\n- b\n')

    await expect(loadOverlay(path)).rejects.toThrow(OverlayError)
  })

  it('loads the committed Slack overlay', async () => {
    const overlay = await loadOverlay(
      join(import.meta.dirname, '..', 'apis', 'slack', 'overlay.yaml'),
    )

    expect(overlay.parameters?.limit?.type).toBe('integer')
    expect(overlay.parameters?.limit?.reason).toBeTruthy()
  })
})
