import { readdir, readFile } from 'node:fs/promises'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import { parseScopePage, ScopeParseError } from '../src/adapters/slack/scopes.ts'
import { apiDir } from '../src/core/config.ts'

const SCOPES_DIR = join(apiDir('slack'), 'snapshot', 'scopes')

const PAGE = `Source: https://docs.slack.dev/reference/scopes/channels.read

# channels:read scope

View basic information about public channels in a workspace

## Facts

**Supported token types**

[\`Bot\`](/authentication/tokens#bot)

[\`User\`](/authentication/tokens#user)

**Compatible API methods**

[\`conversations.info\`](/reference/methods/conversations.info)
`

describe('parseScopePage', () => {
  it('reads the scope, its description and its token classes', () => {
    expect(parseScopePage(PAGE)).toEqual({
      name: 'channels:read',
      description: 'View basic information about public channels in a workspace',
      tokenClasses: ['bot', 'user'],
    })
  })

  /** A page with no prose must not swallow the section heading below it. */
  it('yields an empty description rather than the next heading', () => {
    const page = PAGE.replace('View basic information about public channels in a workspace\n', '')

    expect(parseScopePage(page).description).toBe('')
  })

  it('rejects a page with no scope heading', () => {
    expect(() => parseScopePage('# something else\n')).toThrow(ScopeParseError)
  })

  it('parses every scope page in the snapshot', async () => {
    const files = await readdir(SCOPES_DIR)

    expect(files.length).toBeGreaterThan(20)
    for (const file of files) {
      const facts = parseScopePage(await readFile(join(SCOPES_DIR, file), 'utf8'))

      expect(facts.name, `${file} has no scope name`).not.toBe('')
      expect(facts.description, `${file} has no description`).not.toBe('')
      expect(facts.tokenClasses.length, `${file} lists no token types`).toBeGreaterThan(0)
    }
  })

  /**
   * The scope pages state token support independently of the method pages, so
   * they confirm the bot/user split rather than restating it.
   */
  it('agrees with the method pages that search:read is user-only', async () => {
    const facts = parseScopePage(await readFile(join(SCOPES_DIR, 'search.read.md'), 'utf8'))

    expect(facts.tokenClasses).not.toContain('bot')
    expect(facts.tokenClasses).toContain('user')
  })
})
