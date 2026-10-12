import { describe, expect, it } from 'vitest'

import { reviewOperationNodes } from './tree-model'
import type { ReviewOperationView, SetupView } from './types'

function operation(
  relativePath: string,
  group: string,
  overrides: Partial<ReviewOperationView> = {},
): ReviewOperationView {
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
function setupWith(
  operations: ReviewOperationView[],
  destinations: Record<string, string> = {},
): SetupView {
  return {
    configuration: { operations, destinations },
  } as unknown as SetupView
}

describe('reviewOperationNodes', () => {
  it('says nothing about a setup that has no configuration yet', () => {
    expect(reviewOperationNodes({ configuration: null } as unknown as SetupView)).toEqual([])
  })

  it('lifts operations that cannot run into one group above the rest', () => {
    const nodes = reviewOperationNodes(
      setupWith([
        operation('logs.list', 'logs'),
        operation('logs.send', 'logs', { available: false, reasons: ['No server matched.'] }),
      ]),
    )
    expect(nodes.map((node) => node.id)).toEqual(['group:unavailable', 'group:logs'])
    expect(nodes[0].detail).toBe('1 operation')
    expect(nodes[0].children?.map((child) => child.id)).toEqual(['logs.send'])
    // The runnable group keeps only what can run.
    expect(nodes[1].children?.map((child) => child.id)).toEqual(['logs.list'])
  })

  it('leaves the unavailable group out when everything can run', () => {
    const nodes = reviewOperationNodes(
      setupWith([operation('logs.list', 'logs'), operation('logs.send', 'logs')]),
    )
    expect(nodes.map((node) => node.id)).toEqual(['group:logs'])
  })

  it('names the one host a group agrees on', () => {
    const nodes = reviewOperationNodes(
      setupWith([operation('logs.list', 'logs'), operation('logs.send', 'logs')], {
        'logs.list': 'https://api.example.test/v1',
        'logs.send': 'https://api.example.test/v1',
      }),
    )
    expect(nodes[0].detail).toBe('2 operations · api.example.test')
  })

  // Two paths on one host count as two destinations, as the rest of setup counts them.
  it.each([
    ['two paths on one host', 'https://api.example.test/v2'],
    ['two hosts', 'https://eu.example.test/v1'],
  ])('counts the destinations a group splits between: %s', (_case, second) => {
    const nodes = reviewOperationNodes(
      setupWith([operation('logs.list', 'logs'), operation('logs.send', 'logs')], {
        'logs.list': 'https://api.example.test/v1',
        'logs.send': second,
      }),
    )
    expect(nodes[0].detail).toBe('2 operations · 2 destinations')
  })

  it('states the count alone when no destination is resolved', () => {
    const nodes = reviewOperationNodes(setupWith([operation('logs.list', 'logs')]))
    expect(nodes[0].detail).toBe('1 operation')
  })

  it('shows an unparsable destination as it stands rather than failing', () => {
    const nodes = reviewOperationNodes(
      setupWith([operation('logs.list', 'logs')], { 'logs.list': 'not a url' }),
    )
    expect(nodes[0].detail).toBe('1 operation · not a url')
  })

  it('carries each summary onto the row that lists it', () => {
    const nodes = reviewOperationNodes(setupWith([operation('logs.list', 'logs')]))
    expect(nodes[0].children?.[0].detail).toBe('Does logs.list')
  })
})
