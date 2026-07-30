import { readFile } from 'node:fs/promises'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import { DocsParseError, parseDocsPage, parseSourceUrl } from '../src/adapters/slack/docs.ts'
import { apiDir } from '../src/core/config.ts'

const SERVER_URL = 'https://slack.com/api'
const DOCS_DIR = join(apiDir('slack'), 'snapshot', 'docs')

async function page(slug: string): Promise<string> {
  return readFile(join(DOCS_DIR, `${slug}.md`), 'utf8')
}

describe('parseDocsPage', () => {
  it('reads the request line, description, scopes and rate-limit tier', async () => {
    const facts = parseDocsPage(await page('conversations.list'), SERVER_URL)

    expect(facts.method).toBe('conversations.list')
    expect(facts.path).toBe('/conversations.list')
    expect(facts.httpMethod).toBe('get')
    expect(facts.description).toBe('Lists all channels in a Slack team.')
    expect(facts.scopes.bot).toEqual(['channels:read', 'groups:read', 'im:read', 'mpim:read'])
    expect(facts.rateLimitTier).toBe('Tier 2: 20+ per minute')
  })

  it('reads argument names, types, requiredness, defaults and examples', async () => {
    const facts = parseDocsPage(await page('conversations.list'), SERVER_URL)
    const byName = new Map(facts.parameters.map((parameter) => [parameter.name, parameter]))

    expect(byName.get('cursor')).toMatchObject({
      in: 'query',
      required: false,
      schema: { kind: 'scalar', type: 'string' },
    })
    expect(byName.get('limit')).toMatchObject({
      required: false,
      schema: { kind: 'scalar', type: 'number' },
      default: 100,
      example: 20,
    })
    expect(byName.get('exclude_archived')?.default).toBe(false)
    expect(byName.get('types')?.default).toBe('public_channel')
  })

  it('marks required arguments', async () => {
    const facts = parseDocsPage(await page('conversations.history'), SERVER_URL)
    const channel = facts.parameters.find((parameter) => parameter.name === 'channel')

    expect(channel?.required).toBe(true)
    expect(channel?.description).toBe('Conversation ID to fetch history for.')
  })

  /**
   * Coral sends credentials as a manifest header, so emitting `token` would put
   * a required, unfillable argument on every generated relation.
   */
  it('omits the token argument', async () => {
    for (const slug of ['conversations.list', 'users.info', 'auth.test']) {
      const facts = parseDocsPage(await page(slug), SERVER_URL)

      expect(facts.parameters.map((parameter) => parameter.name)).not.toContain('token')
    }
  })

  /** Boolean arguments render a stray `0` between description and default. */
  it('does not mistake rendering artefacts for a description', async () => {
    const facts = parseDocsPage(await page('conversations.history'), SERVER_URL)
    const inclusive = facts.parameters.find((parameter) => parameter.name === 'inclusive')

    expect(inclusive?.description).toMatch(/^Include messages with/)
    expect(inclusive?.description).not.toMatch(/^0/)
  })

  it('flattens documentation links to their label', async () => {
    const facts = parseDocsPage(await page('conversations.history'), SERVER_URL)
    const cursor = facts.parameters.find((parameter) => parameter.name === 'cursor')

    expect(cursor?.description).toContain('See pagination for more detail.')
    expect(cursor?.description).not.toContain('](')
  })

  it('reports a method Slack documents as POST rather than assuming GET', async () => {
    const facts = parseDocsPage(await page('auth.test'), SERVER_URL)

    expect(facts.httpMethod).toBe('post')
  })

  it('sorts parameters so the descriptor does not churn', async () => {
    const facts = parseDocsPage(await page('conversations.list'), SERVER_URL)
    const names = facts.parameters.map((parameter) => parameter.name)

    expect(names).toEqual(names.toSorted((left, right) => left.localeCompare(right)))
  })

  it('parses every page in the snapshot without warnings', async () => {
    const slugs = [
      'auth.test',
      'conversations.history',
      'conversations.info',
      'conversations.list',
      'conversations.members',
      'conversations.replies',
      'search.messages',
      'users.conversations',
      'users.info',
      'users.list',
    ]

    for (const slug of slugs) {
      const facts = parseDocsPage(await page(slug), SERVER_URL)

      expect(facts.warnings, `${slug} produced warnings`).toEqual([])
      expect(facts.method.toLowerCase(), `${slug} method mismatch`).toBe(slug)
      expect(facts.description, `${slug} has no description`).not.toBe('')
    }
  })

  it('rejects a page whose request URL is not under the configured server', async () => {
    const markdown = (await page('users.info')).replace(
      'https://slack.com/api/users.info',
      'https://example.com/users.info',
    )

    expect(() => parseDocsPage(markdown, SERVER_URL)).toThrow(DocsParseError)
  })

  it('rejects a page with no request line', () => {
    expect(() => parseDocsPage('# nothing here\n', SERVER_URL)).toThrow(/no HTTP request line/)
  })
})

describe('parseSourceUrl', () => {
  it('reads the canonical page URL', async () => {
    expect(parseSourceUrl(await page('users.list'))).toBe(
      'https://docs.slack.dev/reference/methods/users.list',
    )
  })
})
