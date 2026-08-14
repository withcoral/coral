import { describe, expect, it } from 'vitest'

import { safeInternalPath } from './safe-path.server'

describe('safeInternalPath', () => {
  it.each([
    ['/', '/'],
    ['/workspaces/analytics', '/workspaces/analytics'],
    ['/workspaces/analytics?tab=mine#top', '/workspaces/analytics?tab=mine#top'],
    [
      '/sources/new?step=oauth&next=%2F%2Fevil.example',
      '/sources/new?step=oauth&next=%2F%2Fevil.example',
    ],
    // Traversal that stays inside the origin is normalized, not rejected: the
    // rule is where the path lands, not how it was spelled.
    ['/workspaces/../traces', '/traces'],
  ])('keeps an internal destination: %s', (value, expected) => {
    expect(safeInternalPath(value)).toBe(expected)
  })

  // The class both predecessors of this helper let through. `URL` normalization
  // turns each of these into a `//`-leading path, which is a scheme-relative URL
  // — so emitting it as `Location` hands the visitor to another host. Every
  // input here starts `/.`, so an input-side `startsWith('//')` check never
  // sees it.
  it.each([
    '/..//evil.example',
    '/..//evil.example/phish',
    '/./..//evil.example',
    '/workspaces/../..//evil.example',
    '/..//evil.example?next=/workspaces',
    '/..//\\evil.example',
    '/.//\\\\evil.example',
  ])('rejects a destination normalization sends off-origin: %s', (value) => {
    expect(safeInternalPath(value)).toBe('/')
  })

  it.each([
    ['a protocol-relative URL', '//evil.example'],
    ['a backslash-relative URL', '/\\evil.example'],
    ['an absolute URL', 'https://evil.example/phish'],
    ['an absolute URL naming the sentinel origin', 'https://coral-ui.invalid/phish'],
    ['a credentialed URL borrowing the sentinel host', 'https://coral-ui.invalid@evil.example/'],
    ['a javascript: URL', 'javascript:alert(1)'],
    ['a data: URL', 'data:text/html,<script>alert(1)</script>'],
    ['a bare relative path', 'workspaces/analytics'],
    ['an empty string', ''],
    ['null', null],
    ['undefined', undefined],
  ])('rejects %s', (_label, value) => {
    expect(safeInternalPath(value)).toBe('/')
  })

  it('rejects a destination past the length bound', () => {
    expect(safeInternalPath(`/${'a'.repeat(2048)}`)).toBe('/')
    expect(safeInternalPath(`/${'a'.repeat(2046)}`)).toBe(`/${'a'.repeat(2046)}`)
  })
})
