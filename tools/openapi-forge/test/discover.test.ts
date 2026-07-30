import { describe, expect, it } from 'vitest'

import {
  joinIndexes,
  parseMethodIndex,
  parseSampleIndex,
  selectScope,
} from '../src/adapters/slack/discover.ts'

const SITEMAP = `# Slack docs

- https://docs.slack.dev/apis/web-api.md
- https://docs.slack.dev/reference/methods/conversations.history.md
- https://docs.slack.dev/reference/methods/chat.postmessage.md
- https://docs.slack.dev/reference/methods/users.list.md
- https://docs.slack.dev/reference/block-kit/blocks/section-block.md
`

const SAMPLE_INDEX = JSON.stringify([
  { name: 'chat.postMessage.json', type: 'file' },
  { name: 'conversations.history.json', type: 'file' },
  { name: 'rtm.start.json', type: 'file' },
  { name: 'nested', type: 'dir' },
  { name: 'README.md', type: 'file' },
])

describe('parseMethodIndex', () => {
  it('extracts method slugs and ignores other documentation pages', () => {
    expect(parseMethodIndex(SITEMAP)).toEqual([
      'chat.postmessage',
      'conversations.history',
      'users.list',
    ])
  })

  it('deduplicates repeated entries', () => {
    expect(parseMethodIndex(`${SITEMAP}${SITEMAP}`)).toHaveLength(3)
  })
})

describe('parseSampleIndex', () => {
  it('keeps JSON files only, without their extension', () => {
    expect(parseSampleIndex(SAMPLE_INDEX)).toEqual([
      'chat.postMessage',
      'conversations.history',
      'rtm.start',
    ])
  })
})

describe('joinIndexes', () => {
  /**
   * The sitemap lowercases slugs, so the sample filenames are the only place
   * the real casing survives — and the real casing is what the request path
   * needs.
   */
  it('recovers real method casing from the sample filenames', () => {
    const { methods } = joinIndexes(parseMethodIndex(SITEMAP), parseSampleIndex(SAMPLE_INDEX))
    const postMessage = methods.find((method) => method.slug === 'chat.postmessage')

    expect(postMessage?.name).toBe('chat.postMessage')
    expect(postMessage?.docsUrl).toBe(
      'https://docs.slack.dev/reference/methods/chat.postmessage.md',
    )
    expect(postMessage?.sampleUrl).toMatch(/json-logs\/samples\/api\/chat\.postMessage\.json$/)
  })

  it('keeps a documented method that has no recorded sample', () => {
    const { methods } = joinIndexes(parseMethodIndex(SITEMAP), parseSampleIndex(SAMPLE_INDEX))
    const usersList = methods.find((method) => method.slug === 'users.list')

    expect(usersList?.name).toBe('users.list')
    expect(usersList?.sampleUrl).toBeUndefined()
  })

  it('reports samples with no documentation page', () => {
    const { samplesWithoutDocs } = joinIndexes(
      parseMethodIndex(SITEMAP),
      parseSampleIndex(SAMPLE_INDEX),
    )

    expect(samplesWithoutDocs).toEqual(['rtm.start'])
  })
})

describe('selectScope', () => {
  const { methods } = joinIndexes(parseMethodIndex(SITEMAP), parseSampleIndex(SAMPLE_INDEX))

  it('matches configured names case-insensitively', () => {
    const { selected, missing } = selectScope(methods, ['chat.postmessage', 'users.list'])

    expect(selected.map((method) => method.name)).toEqual(['chat.postMessage', 'users.list'])
    expect(missing).toEqual([])
  })

  /** A configured method that vanishes upstream must not silently disappear. */
  it('reports configured methods that no longer exist', () => {
    const { selected, missing } = selectScope(methods, ['users.list', 'groups.list'])

    expect(selected.map((method) => method.name)).toEqual(['users.list'])
    expect(missing).toEqual(['groups.list'])
  })
})
