/**
 * End-to-end coverage over the committed Slack snapshot.
 *
 * These assert against real upstream inputs rather than fixtures, so an
 * upstream change that breaks extraction fails here rather than silently
 * producing a thinner descriptor.
 */

import { describe, expect, it } from 'vitest'

import { loadConfig } from '../src/core/config.ts'
import { emitOpenApi } from '../src/core/emit.ts'
import { extractApiModel, singularize, splitMethod } from '../src/adapters/slack/extract.ts'
import { collectWarnings } from '../src/core/model.ts'
import { Snapshot } from '../src/core/snapshot.ts'

const model = await (async () => {
  const config = await loadConfig('slack')
  return extractApiModel(config, await Snapshot.open(config.snapshotDir))
})()

describe('extractApiModel', () => {
  it('extracts every configured method', async () => {
    const config = await loadConfig('slack')

    expect(model.operations.map((operation) => operation.id).toSorted()).toEqual(
      config.methods.toSorted(),
    )
  })

  /**
   * Listed rather than counted, so a new warning fails here rather than
   * scrolling past in build output. Two are arguments Coral cannot represent,
   * two are real gaps in Slack's reference pages found by cross-checking
   * against the SDK's request types, and one is a scope page Slack links to
   * but does not publish.
   */
  it('warns only about the five known discrepancies', () => {
    expect(collectWarnings(model).toSorted()).toEqual([
      'Slack publishes no reference page for these scopes, so they are emitted without a description: identity:read',
      'files.list: @slack/web-api accepts arguments the reference page does not document: count, page',
      'team.accessLogs: @slack/web-api accepts arguments the reference page does not document: before',
      "team.externalTeams.list: argument 'slack_connect_pref_filter' has unsupported type 'array'; omitted",
      "team.externalTeams.list: argument 'workspace_filter' has unsupported type 'array'; omitted",
    ])
  })

  it('gives each operation a group/leaf operationId', () => {
    const history = model.operations.find((operation) => operation.id === 'conversations.history')

    expect(history?.operationId).toBe('conversations/history')
    expect(history?.group).toBe('conversations')
    expect(history?.path).toBe('/conversations.history')
    expect(history?.method).toBe('get')
  })

  /**
   * Cursor pagination needs the query parameter and the nested response cursor
   * together; either alone leaves Coral fetching a single page.
   */
  it('describes both halves of Slack cursor pagination', () => {
    for (const id of ['conversations.list', 'conversations.history', 'users.list']) {
      const operation = model.operations.find((candidate) => candidate.id === id)
      const names = operation?.parameters.map((parameter) => parameter.name) ?? []

      expect(names, `${id} is missing a cursor parameter`).toContain('cursor')

      const response = operation?.response
      const metadata =
        response?.kind === 'object' ? response.properties.response_metadata : undefined
      const cursor = metadata?.kind === 'object' ? metadata.properties.next_cursor : undefined

      expect(cursor, `${id} is missing response_metadata.next_cursor`).toEqual({
        kind: 'scalar',
        type: 'string',
      })
    }
  })

  /**
   * Row-path inference falls back to the sole array in an envelope, so a
   * second one silently costs the relation its rows.
   */
  it('leaves exactly one row array in each list envelope', () => {
    for (const operation of model.operations) {
      if (operation.response.kind !== 'object') {
        continue
      }
      const arrays = Object.entries(operation.response.properties).filter(
        ([, schema]) => schema.kind === 'array',
      )

      expect(arrays.length, `${operation.id} declares ${arrays.length} arrays`).toBeLessThanOrEqual(
        1,
      )
    }
  })

  it('names row components after the singular of their array property', () => {
    const list = model.operations.find((operation) => operation.id === 'conversations.list')
    const channels =
      list?.response.kind === 'object' ? list.response.properties.channels : undefined
    const items = channels?.kind === 'array' ? channels.items : undefined

    expect(items?.kind === 'object' ? items.component : undefined).toBe('Channel')
  })

  it('models the ok and error fields so a failed call is visible', () => {
    for (const operation of model.operations) {
      const properties = operation.response.kind === 'object' ? operation.response.properties : {}

      expect(properties.ok, `${operation.id} has no ok field`).toBeDefined()
    }
  })

  it('produces a descriptor that passes the emitter assertions', () => {
    expect(() => emitOpenApi(model, { version: '2026-07-30' })).not.toThrow()
  })

  it('emits deterministically from the committed snapshot', () => {
    expect(emitOpenApi(model, { version: '2026-07-30' })).toBe(
      emitOpenApi(model, { version: '2026-07-30' }),
    )
  })
})

describe('splitMethod', () => {
  /** Coral splits operationId on its slash, so only the first dot is a group. */
  it('treats only the first segment as the group', () => {
    expect(splitMethod('conversations.history')).toEqual({
      group: 'conversations',
      leaf: 'history',
    })
    expect(splitMethod('admin.apps.approve')).toEqual({ group: 'admin', leaf: 'apps_approve' })
  })

  it('handles a method with no group', () => {
    expect(splitMethod('test')).toEqual({ group: 'test', leaf: 'test' })
  })
})

describe('singularize', () => {
  it.each([
    ['channels', 'channel'],
    ['members', 'member'],
    ['messages', 'message'],
    ['matches', 'match'],
    ['files', 'file'],
    ['replies', 'reply'],
    ['ims', 'im'],
  ])('%s -> %s', (plural, singular) => {
    expect(singularize(plural)).toBe(singular)
  })

  it('leaves a word that is already singular alone', () => {
    expect(singularize('access')).toBe('access')
    expect(singularize('channel')).toBe('channel')
  })
})
