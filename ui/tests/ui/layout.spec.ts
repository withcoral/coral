import { traceHandlers } from './support/trace-handlers'
import { expect, test } from './playwright.setup'

test('sidebar collapses, expands, and exposes the Query stream tooltip', async ({ network, page, review }, testInfo) => {
  network.use(...traceHandlers.empty)

  await review.chapter('Load the shell', 'Render the query stream with the sidebar visible')
  await page.goto('/')

  await expect(page.getByText('No queries yet')).toBeVisible()

  const brandMark = page.getByRole('img', { name: 'Query stream' })
  const sidebar = page.getByRole('navigation', { name: 'Coral' })
  const tracesButton = page.getByRole('button', { name: 'Traces' })
  const tracesLabel = page.getByText('Traces', { exact: true })
  const collapseButton = page.getByRole('button', { name: 'Collapse sidebar' })
  const sidebarWidth = () => sidebar.evaluate((element) => element.getBoundingClientRect().width)

  const expandedWidth = await sidebarWidth()
  await expect(brandMark).toBeVisible()
  await expect(collapseButton).toBeVisible()
  await expect(tracesButton).toHaveAttribute('aria-current', 'page')
  await expect(tracesLabel).toBeVisible()

  await review.chapter('Show the brand tooltip', 'Hover the Coral icon and confirm the exact tooltip copy')
  await brandMark.hover()
  await expect(page.getByText('Query stream', { exact: true })).toBeVisible()
  await review.pause()

  await review.chapter('Collapse the sidebar', 'Use the dedicated toggle button and verify the sidebar narrows')
  await collapseButton.click()
  await expect(page.getByRole('button', { name: 'Expand sidebar' })).toBeVisible()
  await expect.poll(sidebarWidth).toBeLessThan(expandedWidth)
  await expect(tracesLabel).toHaveCount(0)

  await page.screenshot({ path: testInfo.outputPath('layout-collapsed.png'), fullPage: true })
  await review.pause()

  const collapsedWidth = await sidebarWidth()

  await review.chapter('Expand the sidebar', 'Use the dedicated toggle button again and confirm the item stays active')
  await page.getByRole('button', { name: 'Expand sidebar' }).click()
  await expect(page.getByRole('button', { name: 'Collapse sidebar' })).toBeVisible()
  await expect.poll(sidebarWidth).toBeGreaterThan(collapsedWidth)
  await expect(tracesButton).toHaveAttribute('aria-current', 'page')
  await expect(tracesButton).toBeDisabled()

  await page.screenshot({ path: testInfo.outputPath('layout-expanded.png'), fullPage: true })
  await review.pause()
})
