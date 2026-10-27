import { describe, expect, it } from 'vitest'

import { reviewToolNodes } from './tree-model'
import type { ReviewToolView, SetupView } from './types'

function tool(
  relativePath: string,
  group: string,
  overrides: Partial<ReviewToolView> = {},
): ReviewToolView {
  return {
    key: relativePath,
    relativePath,
    summary: `Does ${relativePath}`,
    group,
    available: true,
    reasons: [],
    path: '',
    ...overrides,
  }
}

/** A setup carrying only what the previewed catalog needs. */
function setupWith(tools: ReviewToolView[], destinations: Record<string, string> = {}): SetupView {
  return {
    configuration: { tools, destinations },
  } as unknown as SetupView
}

describe('reviewToolNodes', () => {
  it('says nothing about a setup that has no configuration yet', () => {
    expect(reviewToolNodes({ configuration: null } as unknown as SetupView)).toEqual([])
  })

  it('lifts tools that cannot run into one group above the rest', () => {
    const nodes = reviewToolNodes(
      setupWith([
        tool('logs.list', 'logs'),
        tool('logs.send', 'logs', { available: false, reasons: ['No server matched.'] }),
      ]),
    )
    expect(nodes.map((node) => node.id)).toEqual(['group:unavailable', 'group:logs'])
    expect(nodes[0].detail).toBe('1 tool')
    expect(nodes[0].children?.map((child) => child.id)).toEqual(['logs.send'])
    // The runnable group keeps only what can run.
    expect(nodes[1].children?.map((child) => child.id)).toEqual(['logs.list'])
  })

  it('leaves the unavailable group out when everything can run', () => {
    const nodes = reviewToolNodes(setupWith([tool('logs.list', 'logs'), tool('logs.send', 'logs')]))
    expect(nodes.map((node) => node.id)).toEqual(['group:logs'])
  })

  it('names the one host a group agrees on', () => {
    const nodes = reviewToolNodes(
      setupWith([tool('logs.list', 'logs'), tool('logs.send', 'logs')], {
        'logs.list': 'https://api.example.test/v1',
        'logs.send': 'https://api.example.test/v1',
      }),
    )
    expect(nodes[0].detail).toBe('2 tools · api.example.test')
  })

  // Two paths on one host count as two destinations, as the rest of setup counts them.
  it.each([
    ['two paths on one host', 'https://api.example.test/v2'],
    ['two hosts', 'https://eu.example.test/v1'],
  ])('counts the destinations a group splits between: %s', (_case, second) => {
    const nodes = reviewToolNodes(
      setupWith([tool('logs.list', 'logs'), tool('logs.send', 'logs')], {
        'logs.list': 'https://api.example.test/v1',
        'logs.send': second,
      }),
    )
    expect(nodes[0].detail).toBe('2 tools · 2 destinations')
  })

  it('states the count alone when no destination is resolved', () => {
    const nodes = reviewToolNodes(setupWith([tool('logs.list', 'logs')]))
    expect(nodes[0].detail).toBe('1 tool')
  })

  it('shows an unparsable destination as it stands rather than failing', () => {
    const nodes = reviewToolNodes(
      setupWith([tool('logs.list', 'logs')], { 'logs.list': 'not a url' }),
    )
    expect(nodes[0].detail).toBe('1 tool · not a url')
  })

  it('carries each summary onto the row that lists it', () => {
    const nodes = reviewToolNodes(setupWith([tool('logs.list', 'logs')]))
    expect(nodes[0].children?.[0].detail).toBe('Does logs.list')
  })

  // Catches: a refresh hiding what it adds or drops, a new tool that
  // cannot run listed as runnable, or a first read showing everything as new.
  it.each([
    ['without live tools', undefined, ['group:unavailable', 'group:logs']],
    [
      'with live tools',
      [
        { relativePath: 'logs.list', summary: 'Does logs.list' },
        { relativePath: 'logs.purge', summary: 'Does logs.purge' },
      ],
      ['group:unavailable', 'refresh:new', 'refresh:removed', 'group:logs'],
    ],
  ])('groups what a refresh changes %s', (_case, previous, groups) => {
    const nodes = reviewToolNodes(
      setupWith([
        tool('logs.list', 'logs'),
        tool('logs.tail', 'logs'),
        tool('logs.send', 'logs', { available: false, reasons: ['No server matched.'] }),
      ]),
      previous,
    )
    expect(nodes.map((node) => node.id)).toEqual(groups)
    const children = (id: string) =>
      nodes.find((node) => node.id === id)?.children?.map((child) => child.id)
    // A new tool that cannot run stays under Cannot run, not New.
    expect(children('group:unavailable')).toEqual(['logs.send'])
    if (!previous) return
    expect(children('refresh:new')).toEqual(['logs.tail'])
    expect(children('refresh:removed')).toEqual(['removed:logs.purge'])
    expect(children('group:logs')).toEqual(['logs.list'])
  })

  // Catches: a provider tag named new sharing an ID with the refresh's New
  // group, so the two open and close together.
  it('keeps a provider group named new apart from New', () => {
    const nodes = reviewToolNodes(setupWith([tool('new.list', 'new'), tool('new.tail', 'new')]), [
      { relativePath: 'new.list', summary: 'Does new.list' },
    ])
    const ids = nodes.map((node) => node.id)
    expect(ids).toEqual(['refresh:new', 'group:new'])
    expect(new Set(ids).size).toBe(ids.length)
  })
})
