import type { Page } from 'playwright'
import { expect, test } from 'vitest'
import { withOwnerSession, mcpToken } from '../../../support/actors.ts'
import { managementClients } from '../../../support/management.ts'
import { observeRun, programRunner } from '../../../support/providers.ts'

// Keyboard on the full Activity table and its rail, with real Coral history: the
// cursor's reveal beneath pinned headings, a real sparse child window, and detail
// steps that keep the rail mounted. Key ownership and traversal rules are Vitest's.

const TABLE = '[role="table"][aria-label="Activity records"]'

/** The record the cursor row shows, its child ordinal, and whether it shows in full. */
function cursor(page: Page) {
  return page.evaluate((selector) => {
    const table = document.querySelector<HTMLElement>(selector)!
    const row = table.querySelector<HTMLElement>('[data-active]')
    if (!row) return null
    const item = row.parentElement?.closest<HTMLElement>('[data-window-index]')
    // Every heading pinned above the row: the column heading, then each group's own.
    let top = table.querySelector('[role="columnheader"]')!.getBoundingClientRect().bottom
    for (let group = row.parentElement; group && group !== table; group = group.parentElement) {
      const heading = group.firstElementChild
      if (heading !== row && heading?.matches('[data-pinned-heading]'))
        top += heading.getBoundingClientRect().height
    }
    const box = row.getBoundingClientRect()
    const port = table.getBoundingClientRect()
    return {
      key: row.dataset.recordKey!,
      label: row.querySelector('a[href]')?.textContent ?? '',
      index: item ? Number(item.dataset.windowIndex) : null,
      inView: box.top >= top - 1 && box.bottom <= port.top + table.clientHeight + 1,
    }
  }, TABLE)
}
const tableFocused = (page: Page) =>
  page.evaluate((selector) => document.activeElement === document.querySelector(selector), TABLE)
/** Focus leaves every control, as a click on the page background leaves it. */
const toBackground = (page: Page) =>
  page.evaluate(() => (document.activeElement as HTMLElement | null)?.blur())
const record = (page: Page) => new URL(page.url()).searchParams.get('record')

test('The keyboard drives workspace Activity records, sparse child windows and the rail', async (context) => {
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const api = managementClients(actor.accessToken)
    const workspace = await api.workspaces.createWorkspace({
      displayName: 'Keyboard activity',
      requestId: crypto.randomUUID(),
    })
    const id = workspace.name.split('/')[1]
    const token = await mcpToken(page, id)
    const other = programRunner(page, target, id, { accessToken: token, taskIntent: 'Other goal' })
    await observeRun(other, await other.exec('return 1;'))
    const runner = programRunner(page, target, id, {
      accessToken: token,
      taskIntent: 'Keyboard goal',
    })
    // More emissions than one child window holds, then a final value.
    const observed = await observeRun(
      runner,
      await runner.exec('for (let i = 0; i < 230; i++) await coral.emit({ i }); return "done";'),
    )
    const run = `${workspace.name}/runs/${observed.update.run_id}`
    const table = page.locator(TABLE)

    await step('↓ from the page background enters the table on the same press', async () => {
      await page.goto(`/workspaces/${id}/activity`)
      await table.getByRole('link', { name: 'Keyboard goal', exact: true }).waitFor()
      await toBackground(page)
      await page.keyboard.press('ArrowDown')
      expect(await tableFocused(page)).toBe(true)
      expect((await cursor(page))?.label).toBe('Keyboard goal')
    })

    await step('Space expands the Task and its Run; the cursor stays revealed', async () => {
      await page.keyboard.press('Space')
      // Until its children load, an expanded Task is a boundary.
      await table.locator(`[data-record-key="${run}"]`).waitFor()
      await page.keyboard.press('ArrowDown')
      expect((await cursor(page))?.key).toBe(run)
      await page.keyboard.press('Space')
      await table.getByRole('link', { name: 'emission #2', exact: true }).waitFor()
      expect(await tableFocused(page)).toBe(true)
    })

    // Hold every read that moves the Run's window, so the cursor reaches the gap first.
    let release!: () => void
    const held = new Promise<void>((resolve) => (release = resolve))
    let holding = 0
    await page.route('**/activity/observe', async (route) => {
      const branch = route.request().postDataJSON()?.window?.branches?.[run]
      if (branch && (branch.offset > 0 || branch.anchor)) {
        holding++
        await held
      }
      await route.continue().catch(() => {})
    })
    /** Presses ↓ until the cursor stops, or reaches `until`; returns where it rests. */
    const walk = async (until?: string) => {
      let at = await cursor(page)
      for (let press = 0; press < 250; press++) {
        await page.keyboard.press('ArrowDown')
        const next = await cursor(page)
        if (next?.key === at?.key || next?.label === until) return next
        expect(next?.inView).toBe(true)
        at = next
      }
      throw new Error('The cursor never stopped')
    }

    let gap!: NonNullable<Awaited<ReturnType<typeof cursor>>>
    await step('↓ stops at an unloaded child ordinal, without skipping', async () => {
      const stop = (await walk())!
      expect(stop.index).not.toBeNull()
      // The next ordinal is a real gap. Revealing the cursor asks for the window that
      // fills it, which is held; until it lands, ↓ stays.
      expect(
        await table.locator(`[data-window-from="${stop.index! + 1}"]`).count(),
      ).toBeGreaterThan(0)
      await expect.poll(() => holding, { timeout: 10_000 }).toBeGreaterThan(0)
      await page.keyboard.press('ArrowDown')
      expect((await cursor(page))?.key).toBe(stop.key)
      gap = stop
    })

    await step('Once the records arrive, ↓ resumes at the next ordinal', async () => {
      release()
      await table.locator(`[data-window-index="${gap.index! + 1}"]`).waitFor({ timeout: 10_000 })
      await page.keyboard.press('ArrowDown')
      expect((await cursor(page))?.index).toBe(gap.index! + 1)
    })

    await step('↓ crosses later windows to the final value, then leaves the Run', async () => {
      for (let attempt = 0; attempt < 20; attempt++) {
        const stop = (await walk('final value'))!
        if (stop.label === 'final value') break
        // Reveal-driven loading fills the next window; wait for it, never past it.
        await table.locator(`[data-window-index="${stop.index! + 1}"]`).waitFor({ timeout: 10_000 })
      }
      expect((await cursor(page))?.key).toBe(`${run}/final`)
      await page.keyboard.press('ArrowDown')
      const after = await cursor(page)
      expect(after?.key.startsWith(run)).toBe(false)
      expect(after?.label).toBe('Other goal')
    })

    await step('Enter opens the record beside the table; ↑ steps it in the same rail', async () => {
      await page.keyboard.press('ArrowUp')
      await page.keyboard.press('Enter')
      const rail = page.getByRole('dialog', { name: 'Activity details' })
      await rail.waitFor()
      await page.waitForURL(() => record(page) === `${run}/final`)
      expect(await tableFocused(page)).toBe(true)
      // The table keeps the final value revealed, not the Run that its detail shows.
      expect(await cursor(page)).toMatchObject({ key: `${run}/final`, inView: true })
      await rail.evaluate((element) => (element.dataset.reel = 'opened'))
      const entries = await page.evaluate(() => history.length)
      await page.keyboard.press('ArrowUp')
      await page.waitForURL(() => record(page)?.startsWith(`${run}/events/`) === true)
      expect(await rail.getAttribute('data-reel')).toBe('opened')
      expect(await page.evaluate(() => history.length)).toBe(entries)
      expect(await tableFocused(page)).toBe(true)
      // The address changes before the table renders the step's page; the cursor
      // waits for that render rather than resting anywhere else.
      await expect
        .poll(async () => (await cursor(page))?.key, { timeout: 10_000 })
        .toBe(record(page))
    })

    await step('Escape closes the rail onto the record it showed', async () => {
      const shown = record(page)
      await page.keyboard.press('Escape')
      await page.getByRole('dialog', { name: 'Activity details' }).waitFor({ state: 'hidden' })
      expect(record(page)).toBeNull()
      expect(await tableFocused(page)).toBe(true)
      expect((await cursor(page))?.key).toBe(shown)
    })
  })
})

test('The keyboard drives Agent Activity', async (context) => {
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const api = managementClients(actor.accessToken)
    const workspace = await api.workspaces.createWorkspace({
      displayName: 'Keyboard agent',
      requestId: crypto.randomUUID(),
    })
    const issued = await api.agents.createAgent({
      parent: workspace.name,
      displayName: 'Keyboard worker',
      requestId: crypto.randomUUID(),
    })
    const runner = programRunner(page, target, workspace.name.split('/')[1], {
      accessToken: issued.credential,
      taskIntent: 'Agent goal',
    })
    const observed = await observeRun(runner, await runner.exec('return 2;'))
    const run = `${workspace.name}/runs/${observed.update.run_id}`
    const table = page.locator(TABLE)

    await step("The Agent's complete Activity takes the same keys", async () => {
      await page.goto(
        `/${issued.agent!.name.split('/').map(encodeURIComponent).join('/')}?view=activity`,
      )
      await table.getByRole('link', { name: 'Agent goal', exact: true }).waitFor()
      await toBackground(page)
      await page.keyboard.press('ArrowDown')
      expect(await tableFocused(page)).toBe(true)
      expect((await cursor(page))?.label).toBe('Agent goal')
      await page.keyboard.press('Space')
      await table.locator(`[data-record-key="${run}"]`).waitFor()
      await page.keyboard.press('Enter')
      const rail = page.getByRole('dialog', { name: 'Activity details' })
      await rail.waitFor()
      expect(await tableFocused(page)).toBe(true)
      await page.keyboard.press('ArrowDown')
      await page.waitForURL(() => record(page) === run)
      expect((await cursor(page))?.key).toBe(run)
      await page.keyboard.press('Escape')
      await rail.waitFor({ state: 'hidden' })
      expect(await tableFocused(page)).toBe(true)
      expect((await cursor(page))?.key).toBe(run)
    })
  })
})
