import { traceHandlers } from './support/trace-handlers'
import { expect, test } from './playwright.setup'

const isMacOs = process.platform === 'darwin'
const sidebarToggleShortcut = 'ControlOrMeta+b'
const sidebarToggleHint = isMacOs ? '⌘' : 'Ctrl'
const desktopViewport = { height: 900, width: 1280 }
const smallViewport = { height: 900, width: 960 }
const mobileViewport = { height: 900, width: 600 }

test('sidebar collapses, expands, and exposes the sidebar toggle tooltip', async ({
  network,
  page,
  review,
}, testInfo) => {
  network.use(...traceHandlers.empty)
  await page.setViewportSize(desktopViewport)
  await page.addInitScript(() => {
    if (window.sessionStorage.getItem('layout-test-storage-cleared') === 'true') return

    window.localStorage.removeItem('coral:sidebar-collapsed')
    window.sessionStorage.setItem('layout-test-storage-cleared', 'true')
  })

  await review.chapter('Load the shell', 'Render the query stream with the sidebar visible')
  await page.goto('/')

  await expect(page.getByText('No queries yet')).toBeVisible()

  const brandMark = page.getByRole('img', { name: 'Query stream' })
  const sidebar = page.getByRole('navigation', { name: 'Coral' })
  const tracesButton = page.getByRole('button', { name: 'Traces' })
  const tracesLabel = page.getByText('Traces', { exact: true })
  const brandName = sidebar.getByText('Coral', { exact: true })
  const collapseButton = page.getByRole('button', { name: 'Collapse sidebar' })
  const sidebarWidth = () => sidebar.evaluate((element) => element.getBoundingClientRect().width)

  const expandedWidth = await sidebarWidth()
  await expect(brandMark).toBeVisible()
  await expect(brandName).toBeVisible()
  await expect(collapseButton).toBeVisible()
  await expect(tracesButton).toHaveAttribute('aria-current', 'page')
  await expect(tracesLabel).toBeVisible()

  await review.chapter(
    'Show the sidebar tooltip',
    'Hover the toggle button and confirm the shortcut hint',
  )
  await expect(brandMark).toBeVisible()
  await collapseButton.hover()
  await expect(
    page.getByText(new RegExp(`Collapse sidebar.*${sidebarToggleHint}.*B`)),
  ).toBeVisible()
  await review.pause()

  await review.chapter(
    'Collapse the sidebar with the shortcut',
    'Press mod+b and verify the sidebar narrows',
  )
  await page.keyboard.press(sidebarToggleShortcut)
  await expect(page.getByRole('button', { name: 'Expand sidebar' })).toBeVisible()
  await expect.poll(sidebarWidth).toBeLessThan(expandedWidth)
  await expect(brandName).toHaveCount(0)
  await expect(tracesLabel).toHaveCount(0)

  await review.chapter('Show the collapsed nav item tooltip', 'Hover the icon-only Traces item')
  await tracesButton.hover()
  await expect(tracesLabel).toBeVisible()

  await page.screenshot({ path: testInfo.outputPath('layout-collapsed.png'), fullPage: true })
  await review.pause()

  const collapsedWidth = await sidebarWidth()

  await review.chapter('Reload collapsed sidebar', 'Confirm the collapsed state persists')
  await page.reload()
  await expect(page.getByRole('button', { name: 'Expand sidebar' })).toBeVisible()
  await expect.poll(sidebarWidth).toBeLessThan(expandedWidth)
  await expect(tracesLabel).toHaveCount(0)

  await review.chapter(
    'Expand the sidebar with the shortcut',
    'Press mod+b again and confirm the item stays active',
  )
  await page.keyboard.press(sidebarToggleShortcut)
  await expect(page.getByRole('button', { name: 'Collapse sidebar' })).toBeVisible()
  await expect.poll(sidebarWidth).toBeGreaterThan(collapsedWidth)
  await expect(tracesButton).toHaveAttribute('aria-current', 'page')
  await expect(tracesButton).toBeDisabled()

  await page.screenshot({ path: testInfo.outputPath('layout-expanded.png'), fullPage: true })
  await review.pause()

  await review.chapter('Reload expanded sidebar', 'Confirm the expanded state persists')
  await page.reload()
  await expect(page.getByRole('button', { name: 'Collapse sidebar' })).toBeVisible()
  await expect.poll(sidebarWidth).toBeGreaterThan(collapsedWidth)
  await expect(tracesLabel).toBeVisible()

  await review.chapter(
    'Sync sidebar storage changes',
    'Dispatch storage updates and confirm the visible sidebar state follows them',
  )
  await page.evaluate(() => {
    window.localStorage.setItem('coral:sidebar-collapsed', 'true')
    window.dispatchEvent(
      new StorageEvent('storage', {
        key: 'coral:sidebar-collapsed',
        newValue: 'true',
      }),
    )
  })
  await expect(page.getByRole('button', { name: 'Expand sidebar' })).toBeVisible()
  await expect.poll(sidebarWidth).toBeLessThan(expandedWidth)
  await page.evaluate(() => {
    window.localStorage.setItem('coral:sidebar-collapsed', 'false')
    window.dispatchEvent(
      new StorageEvent('storage', {
        key: 'coral:sidebar-collapsed',
        newValue: 'false',
      }),
    )
  })
  await expect(page.getByRole('button', { name: 'Collapse sidebar' })).toBeVisible()
  await expect.poll(sidebarWidth).toBeGreaterThan(collapsedWidth)

  await review.chapter(
    'Shrink to a small screen',
    'Resize below the sidebar breakpoint and confirm the sidebar collapses automatically',
  )
  await page.setViewportSize(smallViewport)
  await expect.poll(sidebarWidth).toBeLessThan(expandedWidth)
  await expect(page.getByRole('button', { name: 'Expand sidebar' })).toBeVisible()

  await page.screenshot({ path: testInfo.outputPath('layout-small-screen.png'), fullPage: true })
  await review.pause()

  await review.chapter(
    'Shrink to mobile',
    'Resize below the mobile breakpoint and confirm the sidebar toggle is hidden',
  )
  await page.setViewportSize(mobileViewport)
  await expect.poll(sidebarWidth).toBeLessThan(expandedWidth)
  await expect(page.getByRole('button', { name: 'Collapse sidebar' })).toHaveCount(0)
  await expect(page.getByRole('button', { name: 'Expand sidebar' })).toHaveCount(0)

  await page.screenshot({ path: testInfo.outputPath('layout-mobile.png'), fullPage: true })
  await review.pause()

  await review.chapter(
    'Return to desktop',
    'Grow back above the breakpoint and restore the saved expanded state',
  )
  await page.setViewportSize(desktopViewport)
  await expect(page.getByRole('button', { name: 'Collapse sidebar' })).toBeVisible()
  await expect.poll(sidebarWidth).toBeGreaterThan(collapsedWidth)
  await expect(tracesLabel).toBeVisible()
  await review.pause()
})
