import { expect, it } from 'vitest'
import { markupRoute } from '@/test-support/route'
import { BreadcrumbRoot, Breadcrumbs, type Segment } from './breadcrumbs'

const root = { id: 'workspace', type: 'link' as const, text: 'Demo', to: '/workspaces/w' }
function render(items: Segment[], withRoot: boolean, heading = false) {
  const trail = <Breadcrumbs items={items} heading={heading} />
  return markupRoute(withRoot ? <BreadcrumbRoot value={root}>{trail}</BreadcrumbRoot> : trail)
}
const accounts: Segment[] = [
  { id: 'accounts', type: 'link', text: 'Accounts', to: '/workspaces/w/accounts' },
  { id: 'current', type: 'text', text: 'Linear' },
]

it('starts every trail with its root, as a link', () => {
  const html = render(accounts, true)
  expect(html).toContain('href="/workspaces/w"')
  // React hoists the tab title ahead of the trail, so read the order from the nav.
  const nav = html.slice(html.indexOf('<nav'))
  expect(nav.indexOf('Demo')).toBeLessThan(nav.indexOf('Accounts'))
  expect(nav.indexOf('Accounts')).toBeLessThan(nav.indexOf('Linear'))
})

it('shows the root as the title of a page with no trail of its own', () => {
  const html = render([], true)
  expect(html).toContain('Demo')
  expect(html).not.toContain('href="/workspaces/w"')
})

it('leaves a trail outside any root unchanged', () => {
  const html = render(accounts, false)
  expect(html).not.toContain('Demo')
  expect(html).toContain('Accounts')
})

it('titles the page with the current segment only when asked', () => {
  expect(render(accounts, true, true)).toMatch(/<h1[^>]*>Linear<\/h1>/)
  expect(render([], true, true)).toMatch(/<h1[^>]*>Demo<\/h1>/)
  expect(render(accounts, true)).not.toContain('<h1')
})

// Catches: a page that leaves its tab untitled, or names its parents first.
it('titles the browser tab with the trail, most specific first', () => {
  expect(render(accounts, true)).toContain('<title>Linear · Accounts · Demo</title>')
  expect(render([], true)).toContain('<title>Demo</title>')
})
