import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../support/actors.ts'
import { breadcrumb, dismissOnboarding } from '../../../support/providers.ts'
import { copyNewAgentKey } from '../../../support/agent-key.ts'

test('Management bottom actions are reachable by wheel and keyboard in a short viewport', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const name = `Scroll ${crypto.randomUUID()}`
    await page.goto('/')
    await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
    await page
      .getByRole('navigation', { name: 'Coral', exact: true })
      .getByRole('button', { name: 'Create workspace', exact: true })
      .click()
    await page.getByLabel('Workspace name', { exact: true }).fill(name)
    await page
      .getByRole('dialog')
      .getByRole('button', { name: 'Create workspace', exact: true })
      .click()
    await dismissOnboarding(page)
    await breadcrumb(page, name).waitFor()
    const nav = page.getByRole('navigation', { name: 'Coral', exact: true })
    const confirmation = page.getByRole('dialog', { name: `Archive ${name}`, exact: true })
    const bottom = confirmation.getByRole('button', { name: 'Archive workspace', exact: true })
    await nav.getByRole('button', { name: `Archive ${name}`, exact: true }).click()
    await bottom.waitFor()
    await page.setViewportSize({ width: 1000, height: 220 })
    // The dialog scrolls its own content, so the action must sit inside both
    // the dialog's box and the viewport.
    const visibleInViewport = () =>
      bottom.evaluate((element) => {
        const rect = element.getBoundingClientRect()
        const popup = element.closest('[role="dialog"]')!.getBoundingClientRect()
        return (
          rect.top >= Math.max(0, popup.top) &&
          rect.bottom <= Math.min(window.innerHeight, popup.bottom)
        )
      })
    // Wheel and keyboard scrolls animate. Chromium can drop a key scroll that
    // arrives as the previous animation ends, and scrollTop reaches its target
    // before that end. Wait for the dialog's scrollend before the next input.
    type Scroller = HTMLElement & { scrolled?: Promise<void> }
    const scrollAndSettle = async (scroll: () => Promise<void>) => {
      await confirmation.evaluate((element: Scroller) => {
        element.scrolled = new Promise((resolve, reject) => {
          element.addEventListener('scrollend', () => resolve(), { once: true })
          setTimeout(() => reject(new Error('The dialog did not finish scrolling')), 5000)
        })
      })
      await scroll()
      await confirmation.evaluate((element: Scroller) => element.scrolled)
    }
    const center = async () => {
      const box = (await confirmation.boundingBox())!
      return [box.x + box.width / 2, box.y + box.height / 2] as const
    }
    await step('Wheel to the bottom action of the archive confirmation', async () => {
      // The popup animates to its new maximum height, then scrolls.
      await expect
        .poll(() => confirmation.evaluate((element) => element.scrollHeight > element.clientHeight))
        .toBe(true)
      await expect.poll(visibleInViewport).toBe(false)
      await page.mouse.move(...(await center()))
      await scrollAndSettle(() => page.mouse.wheel(0, 2000))
      expect(await visibleInViewport()).toBe(true)
    })
    await step('Keyboard scrolling also reaches the bottom action', async () => {
      // Keys scroll the focused scroll container. Move focus from the dialog's
      // first button to the dialog, which scrolls itself.
      await confirmation.focus()
      expect(await confirmation.evaluate((element) => element === document.activeElement)).toBe(
        true,
      )
      await scrollAndSettle(() => page.keyboard.press('Home'))
      expect(await visibleInViewport()).toBe(false)
      await scrollAndSettle(() => page.keyboard.press('Control+End'))
      expect(await visibleInViewport()).toBe(true)
      await page.keyboard.press('Escape')
      await expect.poll(() => confirmation.count()).toBe(0)
    })
    const agents = page.getByRole('table', { name: 'Agents', exact: true })
    const filter = page.getByRole('searchbox', { name: 'Filter agents', exact: true })
    await step('Long Agents rows scroll inside the table under its filter', async () => {
      await page.setViewportSize({ width: 1000, height: 700 })
      // Home may have opened another Workspace in the tree; use the new one.
      await page
        .getByRole('navigation', { name: 'Coral', exact: true })
        .getByRole('group', { name, exact: true })
        .getByRole('link', { name: 'Agents', exact: true })
        .click()
      // The Overview has its own Create agent button; act only once Agents has loaded.
      await page.getByRole('heading', { name: 'Agents', exact: true, level: 1 }).waitFor()
      for (let i = 0; i < 6; i++) {
        await page.getByRole('button', { name: 'Create agent', exact: true }).click()
        await page.getByLabel('Agent name', { exact: true }).fill(`Scroll agent ${i}`)
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Create agent', exact: true })
          .click()
        await copyNewAgentKey(page)
        await agents.waitFor()
      }
      await page.setViewportSize({ width: 1000, height: 300 })
      const sidebarTop = await page
        .getByRole('navigation', { name: 'Coral', exact: true })
        .evaluate((element) => element.getBoundingClientRect().top)
      const filterTop = await filter.evaluate((element) => element.getBoundingClientRect().top)
      // Wheel over the first row, so the rows' own scroller takes it.
      const firstRow = (await agents
        .getByRole('rowgroup')
        .last()
        .getByRole('row')
        .first()
        .boundingBox())!
      await page.mouse.move(firstRow.x + firstRow.width / 2, firstRow.y + firstRow.height / 2)
      await page.mouse.wheel(0, 3000)
      await expect
        .poll(() =>
          page
            .getByText(`${actor.label} (You)`, { exact: true })
            .last()
            .evaluate((element) => element.getBoundingClientRect().bottom <= innerHeight),
        )
        .toBe(true)
      // The page itself did not scroll: the filter and the sidebar stay put.
      expect(await filter.evaluate((element) => element.getBoundingClientRect().top)).toBe(
        filterTop,
      )
      expect(await filter.evaluate((element) => element.getBoundingClientRect().top >= 0)).toBe(
        true,
      )
      expect(
        await page
          .getByRole('navigation', { name: 'Coral', exact: true })
          .evaluate((element) => element.getBoundingClientRect().top),
      ).toBe(sidebarTop)
    })
    await step('The find shortcut, ↓ and Enter open one filtered agent', async () => {
      await page.keyboard.press('ControlOrMeta+f')
      await expect
        .poll(() => filter.evaluate((input) => input === document.activeElement))
        .toBe(true)
      await page.keyboard.type('agent 3')
      // Each row links its name once.
      await expect.poll(() => agents.locator('[data-row-link]').count()).toBe(1)
      await page.keyboard.press('ArrowDown')
      await expect
        .poll(() =>
          agents
            .getByRole('link', { name: 'Scroll agent 3', exact: true })
            .evaluate((link) => link === document.activeElement),
        )
        .toBe(true)
      await page.keyboard.press('Enter')
      await breadcrumb(page, 'Scroll agent 3').waitFor()
      await breadcrumb(page).getByRole('link', { name: 'Agents', exact: true }).click()
      await agents.waitFor()
    })
    await step(
      'Agent settings bottom action is reachable without automatic scrolling',
      async () => {
        await page.getByRole('link', { name: 'Scroll agent 0 settings', exact: true }).click()
        await breadcrumb(page, 'Settings').waitFor()
        await page.mouse.move(800, 200)
        await page.mouse.wheel(0, 4000)
        await expect
          .poll(() =>
            page
              .getByRole('button', { name: 'Delete agent', exact: true })
              .evaluate((element) => element.getBoundingClientRect().bottom <= innerHeight),
          )
          .toBe(true)
      },
    )
    await step(
      'Mobile drawer closes on navigation and opens the archive confirmation',
      async () => {
        await page.setViewportSize({ width: 390, height: 360 })
        const drawer = page.getByRole('dialog', { name: 'Navigation', exact: true })
        const openNavigation = page.getByRole('button', { name: 'Open navigation', exact: true })
        await openNavigation.click()
        await drawer.getByRole('link', { name: 'Members', exact: true }).click()
        await breadcrumb(page, 'Members').waitFor()
        await expect.poll(() => page.getByRole('dialog').count()).toBe(0)
        await expect
          .poll(() => openNavigation.evaluate((element) => document.activeElement === element))
          .toBe(true)
        // The drawer closes from its masthead, where the sidebar collapses on a desktop.
        await openNavigation.click()
        await drawer.getByRole('button', { name: 'Close navigation', exact: true }).click()
        await expect.poll(() => page.getByRole('dialog').count()).toBe(0)
        await expect
          .poll(() => openNavigation.evaluate((element) => document.activeElement === element))
          .toBe(true)
        // The confirmation opens from the drawer's Workspace header.
        await openNavigation.click()
        await drawer.getByRole('button', { name: `Archive ${name}`, exact: true }).click()
        await bottom.waitFor()
        await page.mouse.move(195, 180)
        await page.mouse.wheel(0, 3000)
        await expect.poll(visibleInViewport).toBe(true)
        await confirmation.getByRole('button', { name: 'Cancel', exact: true }).click()
        await expect.poll(() => confirmation.count()).toBe(0)
        await page.keyboard.press('Escape')
        await expect.poll(() => page.getByRole('dialog').count()).toBe(0)
        await breadcrumb(page, 'Members').waitFor()
      },
    )
  })
})
