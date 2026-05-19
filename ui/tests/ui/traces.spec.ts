import { traceHandlers } from './support/trace-handlers'
import { LONG_LINEAR_ISSUES_SPAN_LABEL } from './support/trace-fixtures'
import { expect, test } from './playwright.setup'

test('shows an empty trace stream without contacting a Coral server', async ({ network, page, review }) => {
  network.use(...traceHandlers.empty)

  await review.chapter('Test 1: empty trace stream', 'Mock ListTraces with zero traces')
  await page.goto('/')
  await review.pause()

  await expect(page.getByText('No queries yet')).toBeVisible()
  await expect(page.getByText('Connected')).toBeVisible()
  await expect(page.getByText('0 queries')).toBeVisible()
  await review.pause()
})

test('lists 10 traces, searches one, opens its details, and opens a span inspector', async ({ network, page, review }) => {
  network.use(...traceHandlers.tenTraceDetailFlow)

  await review.chapter('Test 2: ten traces and span details', 'Mock list and detail gRPC-Web responses')
  await page.goto('/')
  await review.pause()

  await expect(page.getByText('10 queries')).toBeVisible()
  await expect(page.getByText(/github\.pull_requests/)).toBeVisible()
  await expect(page.getByText(/slack\.messages/).first()).toBeVisible()
  await expect(page.getByText(/linear\.issues/).first()).toBeVisible()
  await review.pause()

  await review.chapter('Search for one trace', 'Filter ten traces down to the Playwright Linear query')
  await page.getByRole('button', { name: 'Search queries' }).click()
  await page.getByPlaceholder('Search queries...').fill('playwright')

  await expect(page.getByText(/linear\.issues WHERE team_key = 'CORAL' AND title ILIKE '%playwright%'/)).toBeVisible()
  await expect(page.getByText('1 of 10 queries')).toBeVisible()
  await review.pause()

  await review.chapter('Open query details', 'Load the selected trace with ten mocked spans')
  await page.getByText(/linear\.issues WHERE team_key = 'CORAL' AND title ILIKE '%playwright%'/).click()

  await expect(page.getByText('Query details')).toBeVisible()
  await expect(page.getByText(/linear\.issues WHERE team_key = 'CORAL' AND title ILIKE '%playwright%'/)).toBeVisible()
  await expect(page.getByText('API requests')).toBeVisible()
  await expect(page.getByRole('treeitem')).toHaveCount(10)
  await review.pause()

  await review.chapter('Wrap a long span label', 'Open the long Linear span and verify the focus treatment stays contained')
  const longSpanRow = page.getByRole('treeitem').filter({ hasText: LONG_LINEAR_ISSUES_SPAN_LABEL })
  const longSpanButton = longSpanRow.locator(':scope > [role="button"]').first()
  const longSpanLabel = longSpanRow.getByText(LONG_LINEAR_ISSUES_SPAN_LABEL, { exact: true })

  await expect(longSpanRow).toBeVisible()
  await expect(longSpanLabel).toBeVisible()
  expect(await longSpanLabel.evaluate((element) => element.getClientRects().length)).toBeGreaterThan(1)
  const rowBox = await longSpanRow.boundingBox()
  expect(rowBox?.height ?? 0).toBeGreaterThan(38)

  await longSpanButton.focus()
  await expect(longSpanButton).toBeFocused()
  await expect(longSpanButton).toHaveCSS('outline-style', 'solid')
  await expect(longSpanButton).toHaveCSS('outline-offset', '-1px')
  const boxShadow = await longSpanRow.evaluate((element) => getComputedStyle(element).boxShadow)
  expect(boxShadow).toContain('inset')
  await page.screenshot({ path: 'ui/test-results/AOL-2-review.png', fullPage: true })
  await review.pause()

  await review.chapter('Open a span inspector', 'Expand one HTTP span and inspect the captured response body')
  await page.getByRole('button', { name: /GET github\.pull_requests/ }).click()

  await expect(page.getByText('Span details')).toBeVisible()
  await expect(page.getByText('GET github.pull_requests')).toBeVisible()
  await expect(page.getByText('Response body')).toBeVisible()
  await expect(page.getByText('Add MSW Playwright trace fixtures')).toBeVisible()
  await review.pause()
})

test('shows trace storage unavailable errors from TraceService', async ({ network, page, review }) => {
  network.use(...traceHandlers.unavailable)

  await review.chapter('Test 3: TraceService unavailable', 'Mock a gRPC-Web unimplemented response from TraceService')
  await page.goto('/')
  await review.pause()

  await expect(page.getByText('Tracing unavailable')).toBeVisible()
  await expect(page.getByText('Trace storage is not enabled for this Coral server. Enable [local_traces].enabled = true, restart the Coral server, then run a query.').first()).toBeVisible()
  await expect(page.getByText('Disconnected')).toBeVisible()
  await expect(page.getByText('0 queries')).toBeVisible()
  await review.pause()
})
