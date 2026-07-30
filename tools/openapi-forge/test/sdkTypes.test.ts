import { readdir, readFile } from 'node:fs/promises'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import { crossCheckArguments, parseSdkTypes } from '../src/adapters/slack/sdkTypes.ts'
import { apiDir } from '../src/core/config.ts'

const COMMON = `
export interface TokenOverridable {
  token?: string;
}
export interface CursorPaginationEnabled {
  limit?: number;
  cursor?: string;
}
export interface Channel {
  channel: string;
}
export type OptionalArgument<T> = T | undefined;
`

function parse(files: Record<string, string>): Map<string, Set<string>> {
  return parseSdkTypes(new Map(Object.entries({ 'common.ts': COMMON, ...files })))
}

describe('parseSdkTypes', () => {
  it('resolves an interface through its heritage clauses', () => {
    const types = parse({
      'conversations.ts': `
// https://docs.slack.dev/reference/methods/conversations.history
export interface ConversationsHistoryArguments extends Channel, TokenOverridable, CursorPaginationEnabled {
  oldest?: string;
}
`,
    })

    expect([...(types.get('conversations.history') ?? [])].toSorted()).toEqual([
      'channel',
      'cursor',
      'limit',
      'oldest',
    ])
  })

  /**
   * The wrapper carries the real composition in its type argument. Discarding
   * it made every argument look undocumented, which is how this was found.
   */
  it('looks inside a generic wrapper such as OptionalArgument', () => {
    const types = parse({
      'conversations.ts': `
// https://docs.slack.dev/reference/methods/conversations.list
export type ConversationsListArguments = OptionalArgument<
  TokenOverridable &
    CursorPaginationEnabled & {
      exclude_archived?: boolean;
      types?: string;
    }
>;
`,
    })

    expect([...(types.get('conversations.list') ?? [])].toSorted()).toEqual([
      'cursor',
      'exclude_archived',
      'limit',
      'types',
    ])
  })

  /** Unions model mutually exclusive arguments; the method accepts either. */
  it('collects both sides of a union', () => {
    const types = parse({
      'conversations.ts': `
export interface Emails { emails: string[]; }
export interface UserIDs { user_ids: string[]; }
// https://docs.slack.dev/reference/methods/conversations.inviteShared
export type ConversationsInviteSharedArguments = Channel & (Emails | UserIDs);
`,
    })

    expect([...(types.get('conversations.inviteshared') ?? [])].toSorted()).toEqual([
      'channel',
      'emails',
      'user_ids',
    ])
  })

  it('omits token, which Coral sends as a header', () => {
    const types = parse({
      'auth.ts': `
// https://docs.slack.dev/reference/methods/auth.test
export interface AuthTestArguments extends TokenOverridable {}
`,
    })

    expect(types.get('auth.test')).toEqual(new Set())
  })

  it('ignores an interface with no documentation URL', () => {
    const types = parse({
      'conversations.ts': 'export interface Helper { thing?: string; }\n',
    })

    expect(types.size).toBe(0)
  })

  it('survives a reference cycle', () => {
    const types = parse({
      'a.ts': `
export interface Loop extends Loop { a?: string; }
// https://docs.slack.dev/reference/methods/x.y
export interface XyArguments extends Loop {}
`,
    })

    expect(types.get('x.y')).toEqual(new Set(['a']))
  })

  it('parses the committed SDK snapshot', async () => {
    const dir = join(apiDir('slack'), 'snapshot', 'sdk')
    const files = new Map<string, string>()
    for (const file of await readdir(dir)) {
      files.set(file, await readFile(join(dir, file), 'utf8'))
    }
    const types = parseSdkTypes(files)

    expect(types.size).toBeGreaterThan(20)
    // A spot check that mixin resolution really ran: these come from
    // CursorPaginationEnabled, not from the interface body.
    expect(types.get('conversations.list')).toContain('cursor')
    expect(types.get('conversations.list')).toContain('limit')
    expect(types.get('users.list')).toContain('cursor')
  })
})

describe('crossCheckArguments', () => {
  const sdk = new Map([['files.list', new Set(['channel', 'count', 'page'])]])

  it('reports arguments the SDK accepts that the page omits', () => {
    expect(crossCheckArguments('files.list', ['channel'], sdk)).toEqual([
      '@slack/web-api accepts arguments the reference page does not document: count, page',
    ])
  })

  it('reports arguments the page documents that the SDK omits', () => {
    expect(crossCheckArguments('files.list', ['channel', 'count', 'page', 'extra'], sdk)).toEqual([
      'the reference page documents arguments @slack/web-api does not accept: extra',
    ])
  })

  it('says nothing when the two agree', () => {
    expect(crossCheckArguments('files.list', ['channel', 'count', 'page'], sdk)).toEqual([])
  })

  /** The SDK is a second opinion, not a requirement. */
  it('says nothing about a method the SDK does not cover', () => {
    expect(crossCheckArguments('team.billableInfo', ['user'], sdk)).toEqual([])
  })

  it('matches methods case-insensitively', () => {
    expect(crossCheckArguments('Files.List', ['channel', 'count', 'page'], sdk)).toEqual([])
  })
})
