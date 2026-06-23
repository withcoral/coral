import { sourceLifecycleHandlers } from './support/source-handlers'
import { traceHandlers } from './support/trace-handlers'
import { expect, test } from './playwright.setup'

test('shows route loading while navigating to the traces loader', async ({ network, page }) => {
  network.use(...sourceLifecycleHandlers())

  await page.goto('/sources')
  await expect(page.getByRole('heading', { level: 1, name: 'Sources' })).toBeVisible()

  network.use(...traceHandlers.delayedTraceList)
  await page.getByRole('button', { name: 'Traces' }).click()

  await expect(page.getByRole('progressbar', { name: 'Loading page' })).toBeVisible()
  await expect(page.getByText('10 queries')).toBeVisible()
  await expect(page.getByRole('progressbar', { name: 'Loading page' })).toHaveCount(0)
})

test('shows route loading while navigating to the sources loader', async ({ network, page }) => {
  network.use(...traceHandlers.empty)

  await page.goto('/')
  await expect(page.getByText('No queries yet')).toBeVisible()

  network.use(...sourceLifecycleHandlers({ discoverDelayMs: 650 }))
  await page.getByRole('button', { name: 'Sources' }).click()

  await expect(page.getByRole('progressbar', { name: 'Loading page' })).toBeVisible()
  await expect(page.getByRole('heading', { level: 1, name: 'Sources' })).toBeVisible()
  await expect(page.getByRole('progressbar', { name: 'Loading page' })).toHaveCount(0)
})

test('shows route loading while navigating to trace details', async ({ network, page }) => {
  network.use(...traceHandlers.delayedTraceDetailFlow)

  await page.goto('/')
  await expect(page.getByText('10 queries')).toBeVisible()

  await page
    .getByText(/linear\.issues WHERE team_key = 'CORAL' AND title ILIKE '%playwright%'/)
    .click()

  await expect(page.getByRole('progressbar', { name: 'Loading page' })).toBeVisible()
  await expect(page.getByText('Query details')).toBeVisible()
  await expect(page.getByRole('progressbar', { name: 'Loading page' })).toHaveCount(0)
})
