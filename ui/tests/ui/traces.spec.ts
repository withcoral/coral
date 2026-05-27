import { traceHandlers } from './support/trace-handlers'
import { expect, test } from './playwright.setup'

test('shows an empty trace stream without contacting a Coral server', async ({
  network,
  page,
  review,
}) => {
  network.use(...traceHandlers.empty)

  await review.chapter('Test 1: empty trace stream', 'Mock ListTraces with zero traces')
  await page.goto('/')
  await review.pause()

  await expect(page.getByText('No queries yet')).toBeVisible()
  await expect(page.getByText('Connected')).toBeVisible()
  await expect(page.getByText('0 queries')).toBeVisible()
  await review.pause()
})

test('lists 10 traces, searches one, opens its details, and opens a span inspector', async ({
  network,
  page,
  review,
}) => {
  network.use(...traceHandlers.tenTraceDetailFlow)

  await review.chapter(
    'Test 2: ten traces and span details',
    'Mock list and detail gRPC-Web responses',
  )
  await page.goto('/')
  await review.pause()

  await expect(page.getByText('10 queries')).toBeVisible()
  await expect(page.getByText(/github\.pull_requests/)).toBeVisible()
  await expect(page.getByText(/slack\.messages/).first()).toBeVisible()
  await expect(page.getByText(/linear\.issues/).first()).toBeVisible()
  await review.pause()

  await review.chapter(
    'Search for one trace',
    'Filter ten traces down to the Playwright Linear query',
  )
  await page.getByPlaceholder('Search queries...').fill('playwright')

  await expect(
    page.getByText(/linear\.issues WHERE team_key = 'CORAL' AND title ILIKE '%playwright%'/),
  ).toBeVisible()
  await expect(page.getByText('1 of 10 queries')).toBeVisible()
  await review.pause()

  await review.chapter('Open query details', 'Load the selected trace with ten mocked spans')
  await page
    .getByText(/linear\.issues WHERE team_key = 'CORAL' AND title ILIKE '%playwright%'/)
    .click()

  await expect(page.getByText('Query details')).toBeVisible()
  await expect(
    page.getByText(/linear\.issues WHERE team_key = 'CORAL' AND title ILIKE '%playwright%'/),
  ).toBeVisible()
  await expect(page.getByText('API requests')).toBeVisible()
  await expect(page.getByRole('treeitem')).toHaveCount(14)
  await review.pause()

  await review.chapter(
    'Open a span inspector',
    'Expand one HTTP span and inspect the captured response body',
  )
  await page.getByRole('button', { name: /^GET github\.pull_requests\b/ }).click()
  await page.getByRole('tab', { name: 'Response body' }).click()

  const spanInspector = page.locator('[data-span-inspector="true"]')
  await expect(spanInspector.getByText('GET github.pull_requests')).toBeVisible()
  await expect(
    spanInspector.locator('[data-request-endpoint="true"]', {
      hasText: '/repos/oxide/coral/pulls?state=open&per_page=25',
    }),
  ).toBeVisible()
  await expect(page.getByText('"title": "Add MSW Playwright trace fixtures"')).toBeVisible()
  await expect(page.getByText('Raw body')).toBeVisible()
  await expect(page.getByText('Span attributes')).toBeVisible()
  await page.screenshot({ path: 'test-results/AOL-6-review.png', fullPage: true })
  await review.pause()
})

test('renders trace request and response bodies with JSON, GraphQL, and fallback states', async ({
  network,
  page,
  review,
}) => {
  network.use(...traceHandlers.tenTraceDetailFlow)

  await review.chapter(
    'Open the trace with span details',
    'Load the selected trace so the body viewer states can be inspected',
  )
  await page.goto('/')
  await page.getByPlaceholder('Search queries...').fill('playwright')
  await page
    .getByText(/linear\.issues WHERE team_key = 'CORAL' AND title ILIKE '%playwright%'/)
    .click()

  await expect(page.getByRole('treeitem')).toHaveCount(14)

  await review.chapter(
    'Inspect pretty JSON',
    'Open a structured response body and confirm it is pretty printed',
  )
  await page.getByRole('button', { name: /^GET slack\.conversations\b/ }).click()
  await page.getByRole('tab', { name: 'Response body' }).click()

  await expect(page.getByText('Response body')).toBeVisible()
  await expect(page.getByText('"channels": [')).toBeVisible()
  await expect(page.getByText('"name": "eng-coral"')).toBeVisible()

  await review.chapter(
    'Inspect malformed JSON fallback',
    'Verify raw text stays readable when parsing fails',
  )
  await page.getByRole('button', { name: /^GET github\.issue_previews\b/ }).click()
  await page.getByRole('tab', { name: 'Response body' }).click()

  await expect(page.getByText('Response body')).toBeVisible()
  await expect(page.getByText('{"oops":')).toBeVisible()

  await review.chapter(
    'Inspect GraphQL bodies',
    'Check request metadata, variables, and response data for GraphQL traffic',
  )
  await page.getByRole('button', { name: /^POST linear\.issues\b/ }).click()
  await page.getByRole('tab', { name: 'Request body' }).click()
  const requestPanel = page.getByRole('tabpanel', { name: 'Request body' })
  const responsePanel = page.getByRole('tabpanel', { name: 'Response body' })

  await expect(requestPanel.getByText('GraphQL request')).toBeVisible()
  await expect(requestPanel.getByText('Operation', { exact: true })).toBeVisible()
  await expect(requestPanel.getByText('IssuesSearch', { exact: true })).toBeVisible()
  await expect(requestPanel.getByText('Type', { exact: true })).toBeVisible()
  await expect(requestPanel.getByText('query', { exact: true })).toBeVisible()
  await expect(requestPanel.getByText('Variables', { exact: true }).first()).toBeVisible()
  await expect(requestPanel.getByText('"query": "playwright"')).toBeVisible()
  await expect(requestPanel.getByText('Query', { exact: true })).toBeVisible()
  await expect(requestPanel.locator('pre').filter({ hasText: /^query IssuesSearch/ })).toBeVisible()
  await page.getByRole('tab', { name: 'Response body' }).click()

  await expect(responsePanel.getByText('GraphQL response')).toBeVisible()
  await expect(responsePanel.getByText('Data', { exact: true }).first()).toBeVisible()
  await expect(
    responsePanel.getByText('"title": "Add Playwright coverage for trace stream"'),
  ).toBeVisible()
  await review.pause()
})

test('renders GraphQL detection without /graphql and body absence states', async ({
  network,
  page,
  review,
}) => {
  network.use(...traceHandlers.tenTraceDetailFlow)

  await review.chapter(
    'Open the trace with span details',
    'Load the selected trace so the remaining body viewer states can be inspected',
  )
  await page.goto('/')
  await page.getByPlaceholder('Search queries...').fill('playwright')
  await page
    .getByText(/linear\.issues WHERE team_key = 'CORAL' AND title ILIKE '%playwright%'/)
    .click()

  await expect(page.getByRole('treeitem')).toHaveCount(14)

  await review.chapter(
    'Inspect GraphQL detection without /graphql',
    'Open a GraphQL-shaped body on a non-GraphQL path and confirm the richer rendering still appears',
  )
  await page.getByRole('button', { name: /^POST github\.repository_search\b/ }).click()
  await page.getByRole('tab', { name: 'Request body' }).click()
  const githubRequestPanel = page.getByRole('tabpanel', { name: 'Request body' })
  const githubResponsePanel = page.getByRole('tabpanel', { name: 'Response body' })

  await expect(githubRequestPanel.getByText('GraphQL request')).toBeVisible()
  await expect(githubRequestPanel.getByText('RepositorySearch', { exact: true })).toBeVisible()
  await expect(githubRequestPanel.getByText('Raw body')).toBeVisible()
  await page.getByRole('tab', { name: 'Response body' }).click()

  await expect(githubResponsePanel.getByText('GraphQL response')).toBeVisible()
  await expect(githubResponsePanel.getByText('Errors', { exact: true }).first()).toBeVisible()
  await expect(
    githubResponsePanel.getByText('"message": "GraphQL warnings should still be visible"'),
  ).toBeVisible()

  await review.chapter(
    'Inspect missing and truncated bodies',
    'Confirm the viewer still explains empty and truncated body states',
  )
  await page.getByRole('button', { name: /^POST linear\.issue_request_preview\b/ }).click()
  await page.getByRole('tab', { name: 'Request body' }).click()

  await expect(
    page.getByText('Request body was present (2.0 KB), but content was not captured.'),
  ).toBeVisible()
  await page.getByRole('button', { name: /^GET github\.pull_request_archive\b/ }).click()
  await page.getByRole('tab', { name: 'Response body (truncated)' }).click()

  await expect(
    page.getByText('Response body was truncated (4.0 KB), but no preview was recorded.'),
  ).toBeVisible()
  await review.pause()
})

test('shows trace storage unavailable errors from TraceService', async ({
  network,
  page,
  review,
}) => {
  network.use(...traceHandlers.unavailable)

  await review.chapter(
    'Test 3: TraceService unavailable',
    'Mock a gRPC-Web unimplemented response from TraceService',
  )
  await page.goto('/')
  await review.pause()

  await expect(page.getByText('Tracing unavailable')).toBeVisible()
  await expect(
    page
      .getByText(
        'Trace storage is not enabled for this Coral server. Enable [local_traces].enabled = true, restart the Coral server, then run a query.',
      )
      .first(),
  ).toBeVisible()
  await expect(page.getByText('Disconnected')).toBeVisible()
  await expect(page.getByText('0 queries')).toBeVisible()
  await review.pause()
})
