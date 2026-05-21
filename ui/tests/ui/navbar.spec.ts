import { traceHandlers } from './support/trace-handlers'
import { expect, test } from './playwright.setup'

test('sidebar expands and the brand icon exposes the Query stream tooltip', async ({ network, page, review }, testInfo) => {
  network.use(...traceHandlers.empty)

  await review.chapter('Load the shell', 'Render the empty query stream with the sidebar visible')
  await page.goto('/')

  await expect(page.getByText('No queries yet')).toBeVisible()

  const brandButton = page.getByRole('button', { name: 'Query stream' })
  const sidebar = page.getByRole('navigation', { name: 'Coral' })
  const tracesButton = page.getByRole('button', { name: 'Traces' })

  const expandedWidth = await tracesButton.evaluate((element) => element.getBoundingClientRect().width)
  await expect(brandButton).toHaveAttribute('aria-expanded', 'true')
  await expect(tracesButton).toHaveAttribute('aria-current', 'page')

  await review.chapter('Show the brand tooltip', 'Hover the Coral icon and confirm the exact tooltip copy')
  await brandButton.hover()
  await expect(page.getByText('Query stream', { exact: true })).toBeVisible()
  await review.pause()

  await review.chapter('Collapse and expand the sidebar', 'Toggle the sidebar width and keep the active item selected')
  await brandButton.click()
  await expect(brandButton).toHaveAttribute('aria-expanded', 'false')
  await expect.poll(async () => sidebar.evaluate((element) => element.getBoundingClientRect().width)).toBeLessThan(expandedWidth)

  const collapsedWidth = await sidebar.evaluate((element) => element.getBoundingClientRect().width)

  await brandButton.click()
  await expect(brandButton).toHaveAttribute('aria-expanded', 'true')
  await expect.poll(async () => sidebar.evaluate((element) => element.getBoundingClientRect().width)).toBeGreaterThan(collapsedWidth)
  await expect(tracesButton).toHaveAttribute('aria-current', 'page')
  await expect(tracesButton).toBeDisabled()

  await page.screenshot({ path: testInfo.outputPath('navbar-expanded.png'), fullPage: true })
  await review.pause()
})
