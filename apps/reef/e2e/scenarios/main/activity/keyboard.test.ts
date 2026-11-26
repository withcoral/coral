import type { Page } from 'playwright'
import { expect, test } from 'vitest'
import { withOwnerSession, mcpToken } from '../../../support/actors.ts'
import { managementClients } from '../../../support/management.ts'
import { observeRun, programRunner } from '../../../support/providers.ts'

// Keyboard on the Activity waterfall and its rail, with real Coral history: a Run with
// more Events than one child window holds, read in full in the background, its emissions
// as marks, and detail steps that keep the rail mounted. Key ownership and traversal rules
// are Vitest's.

const TABLE = '[role="table"][aria-label="Activity records"]'

/** The record the cursor row shows, and whether it shows in full below the headings. */
function cursor(page: Page) {
  return page.evaluate((selector) => {
    const table = document.querySelector<HTMLElement>(selector)!
    const row = table.querySelector<HTMLElement>('[data-active]')
    if (!row) return null
    const top = table
      .querySelector('[role="columnheader"]')!
      .closest('[role="rowgroup"]')!
      .getBoundingClientRect().bottom
    const box = row.getBoundingClientRect()
    const port = table.getBoundingClientRect()
    return {
      key: row.dataset.recordKey!,
      label: row.querySelector('a[href]')?.textContent ?? '',
      inView: box.top >= top - 1 && box.bottom <= port.top + table.clientHeight + 1,
    }
  }, TABLE)
}
const tableFocused = (page: Page) =>
  page.evaluate((selector) => document.activeElement === document.querySelector(selector), TABLE)
/** Focus leaves every control, as a click on the page background leaves it. */
const toBackground = (page: Page) =>
  page.evaluate(() => (document.activeElement as HTMLElement | null)?.blur())
/** The number in a filler record's name, "Save 12". NaN for any other record. */
const save = (label: string | undefined) => Number(/^Save (\d+)$/.exec(label ?? '')?.[1])
const record = (page: Page) => new URL(page.url()).searchParams.get('record')

test('The keyboard drives the workspace Activity waterfall, its marks and the rail', async (context) => {
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
    // More records than two child windows of the Task hold. Each is a submission Coral
    // rejects: a record of the Task with a row of its own, named by its intent, and no Run,
    // so nothing loads under it.
    const SAVES = 210
    for (let index = 0; index < SAVES; index++) {
      const filled = await other.callTool('exec', {
        task_id: other.taskId(),
        intent: `Save ${index}`,
        program: 'return { invalid syntax;',
        wait_ms: 10_000,
      })
      expect(filled.error).toBeFalsy()
      expect(filled.result?.isError).toBe(true)
    }
    const runner = programRunner(page, target, id, {
      accessToken: token,
      taskIntent: 'Keyboard goal',
    })
    // More emissions than one child window holds, then a return value, which is no record.
    const observed = await observeRun(
      runner,
      await runner.exec('for (let i = 0; i < 230; i++) await coral.emit({ i }); return "done";'),
    )
    const run = `${workspace.name}/runs/${observed.update.run_id}`
    const table = page.locator(TABLE)
    const marks = table.locator(`[data-record-key="${run}"] [data-timeline-mark]`)
    /** The mark the rail shows, which the waterfall draws as the current one. */
    const current = (label: string) =>
      table.locator(`[data-timeline-mark][aria-current="true"][aria-label="${label}"]`)

    await step('The newest goal opens beside the list, and ↓ enters its waterfall', async () => {
      await page.goto(`/workspaces/${id}/activity`)
      await table.getByRole('link', { name: 'Keyboard goal', exact: true }).waitFor()
      await toBackground(page)
      await page.keyboard.press('ArrowDown')
      expect(await tableFocused(page)).toBe(true)
      expect((await cursor(page))?.label).toBe('Keyboard goal')
    })

    await step('The waterfall reads every window of the Run without scrolling', async () => {
      // The Run's start is Event 1, so its 230 emissions are Events 2 to 231: three
      // child windows, each a mark on the Run's row, not a row.
      await expect.poll(() => marks.count(), { timeout: 30_000 }).toBe(230)
      expect(await table.getByRole('row').count()).toBeLessThan(10)
      await page.getByText(/^Loading records… \d/).waitFor({ state: 'detached' })
      // The time breakdown states a value only now that the whole Task is read.
      await page.getByText('Agent model', { exact: true }).waitFor()
    })

    await step('↓ reaches the Run, and → and ← step its marks in the same rail', async () => {
      for (let press = 0; press < 5 && (await cursor(page))?.key !== run; press++)
        await page.keyboard.press('ArrowDown')
      expect(await cursor(page)).toMatchObject({ key: run, inView: true })
      await page.keyboard.press('ArrowRight')
      const rail = page.getByRole('dialog', { name: 'Activity details' })
      await rail.waitFor()
      await page.waitForURL(() => record(page) === `${run}/events/2`)
      expect(await tableFocused(page)).toBe(true)
      await rail.evaluate((element) => (element.dataset.reel = 'opened'))
      const entries = await page.evaluate(() => history.length)
      // The address changes before the waterfall renders the step; a key waits for that.
      await current('emission #2').waitFor()
      await page.keyboard.press('ArrowRight')
      await page.waitForURL(() => record(page) === `${run}/events/3`)
      await current('emission #3').waitFor()
      // ← comes back to the mark before; a step adds no history entry.
      await page.keyboard.press('ArrowLeft')
      await page.waitForURL(() => record(page) === `${run}/events/2`)
      expect(await rail.getAttribute('data-reel')).toBe('opened')
      expect(await page.evaluate(() => history.length)).toBe(entries)
      expect(await tableFocused(page)).toBe(true)
      // The mark the rail shows is the current one, on the Run's row.
      await current('emission #2').waitFor()
      // The last mark, in the Run's last window, is a click away.
      await marks.last().click()
      await page.waitForURL(() => record(page) === `${run}/events/231`)
      expect(await rail.getAttribute('data-reel')).toBe('opened')
    })

    await step('Escape closes the rail onto the Run that holds the mark', async () => {
      // The click on the mark left focus where Escape acts: nothing moves it first.
      await page.keyboard.press('Escape')
      await page.getByRole('dialog', { name: 'Activity details' }).waitFor({ state: 'hidden' })
      // The rail's exit starts at once; the address drops the record when its navigation lands.
      await page.waitForURL(() => record(page) === null)
      expect(await tableFocused(page)).toBe(true)
      expect((await cursor(page))?.key).toBe(run)
      // The goal's waterfall stays open in the address.
      expect(new URL(page.url()).searchParams.getAll('expand')).toContain(
        `${workspace.name}/tasks/${runner.taskId()}`,
      )
    })

    await step(
      'Space closes the goal around the Run and opens it again; Enter opens the Run',
      async () => {
        const row = table.locator(`[data-record-key="${run}"]`)
        // The Run holds only marks, so Space closes the record around it: the goal.
        await page.keyboard.press('Space')
        await row.waitFor({ state: 'detached' })
        expect((await cursor(page))?.label).toBe('Keyboard goal')
        expect(await tableFocused(page)).toBe(true)
        await page.keyboard.press('Space')
        await row.waitFor()
        await page.keyboard.press('ArrowDown')
        expect(await cursor(page)).toMatchObject({ key: run, inView: true })
        await page.keyboard.press('Enter')
        const rail = page.getByRole('dialog', { name: 'Activity details' })
        await rail.waitFor()
        await page.waitForURL(() => record(page) === run)
        expect(await tableFocused(page)).toBe(true)
        await page.keyboard.press('Escape')
        await rail.waitFor({ state: 'hidden' })
        await page.waitForURL(() => record(page) === null)
        expect((await cursor(page))?.key).toBe(run)
      },
    )

    // Hold every read of the other goal's last child window, so the cursor reaches the
    // records that are not loaded before they arrive. The read before it, of the second
    // window, also holds the records of the goal's first Run.
    const otherTask = `${workspace.name}/tasks/${other.taskId()}`
    let release!: () => void
    const held = new Promise<void>((resolve) => (release = resolve))
    const heldWindows: string[] = []
    await page.route(
      (url) =>
        url.searchParams.get('load') === 'page' &&
        Number(url.searchParams.get(`offset.${otherTask}`) ?? 0) >= 200,
      async (route) => {
        heldWindows.push(new URL(route.request().url()).searchParams.get(`offset.${otherTask}`)!)
        await held
        await route.continue().catch(() => {})
      },
    )

    await step('The list opens another goal, its details, and its waterfall', async () => {
      await page
        .getByRole('table', { name: 'Tasks and runs', exact: true })
        .getByRole('link', { name: 'Other goal', exact: true })
        .click()
      await table.getByRole('link', { name: 'Other goal', exact: true }).waitFor()
      expect(await table.getByRole('link', { name: 'Keyboard goal', exact: true }).count()).toBe(0)
      const rail = page.getByRole('dialog', { name: 'Activity details' })
      await rail.getByRole('heading', { name: 'Other goal', exact: true, level: 2 }).waitFor()
      expect(record(page)).toBe(otherTask)
      // Closing the details keeps the goal's waterfall open for the keys below.
      await page.keyboard.press('Escape')
      await rail.waitFor({ state: 'hidden' })
      await page.waitForURL(() => record(page) === null)
      // Until a read returns the records of the goal's Run, a line under the Run says that
      // they load, and ↓ stops there. Vitest covers that stop; this journey waits for them.
      await expect
        .poll(() => table.getByText('Loading records…', { exact: true }).count(), {
          timeout: 10_000,
        })
        .toBe(0)
    })

    /** Presses ↓ until the cursor stops; returns where it rests. Every step stays in view. */
    const walk = async () => {
      let at = await cursor(page)
      for (let press = 0; press < 250; press++) {
        await page.keyboard.press('ArrowDown')
        const next = await cursor(page)
        if (next?.key === at?.key) return next!
        expect(next?.inView).toBe(true)
        at = next
      }
      throw new Error('The cursor never stopped')
    }
    /** The line under the cursor row, which says what the waterfall has not loaded. */
    const underCursor = () =>
      table.evaluate(
        (element) => element.querySelector('[data-active]')?.nextElementSibling?.textContent ?? '',
      )

    let stop!: NonNullable<Awaited<ReturnType<typeof cursor>>>
    await step('↓ stops at a child ordinal that is not loaded, without skipping', async () => {
      await page.locator(TABLE).focus()
      stop = await walk()
      // The goal's last child window is held, so the next ordinal is a real gap.
      expect(await underCursor()).toMatch(/^Loading \d+ records…$/)
      // The Run is the goal's first record, so the last ordinal of the second window, 199,
      // is the filler before it by one.
      expect(save(stop.label)).toBe(198)
      // The read that fills the gap is the one the page asked for.
      await expect.poll(() => heldWindows.length, { timeout: 10_000 }).toBeGreaterThan(0)
      expect(heldWindows[0]).toBe('200')
      await page.keyboard.press('ArrowDown')
      expect((await cursor(page))?.key).toBe(stop.key)
    })

    await step('Once the records arrive, ↓ resumes at the next ordinal', async () => {
      release()
      await table
        .getByRole('link', { name: `Save ${save(stop.label) + 1}`, exact: true })
        .waitFor({ timeout: 10_000 })
      await page.keyboard.press('ArrowDown')
      const next = await cursor(page)
      expect(save(next?.label)).toBe(save(stop.label) + 1)
      expect(next?.inView).toBe(true)
    })

    await step('↓ crosses the later window to the last record, and does not wrap', async () => {
      const last = await walk()
      expect(save(last.label)).toBe(SAVES - 1)
      expect(last.inView).toBe(true)
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
      // The Agent's own scope reads the goal's Run into the waterfall.
      await table.locator(`[data-record-key="${run}"]`).waitFor()
      await page.keyboard.press('Enter')
      const rail = page.getByRole('dialog', { name: 'Activity details' })
      await rail.waitFor()
      expect(await tableFocused(page)).toBe(true)
      await page.keyboard.press('ArrowDown')
      await page.waitForURL(() => record(page) === run)
      // The address changes before the rail renders the step, and Escape in that gap
      // closes onto the Task the rail still shows. Vitest covers the gap; this journey
      // lets the step render first.
      await table.locator(`[data-record-key="${run}"][aria-selected="true"]`).waitFor()
      expect((await cursor(page))?.key).toBe(run)
      await page.keyboard.press('Escape')
      await rail.waitFor({ state: 'hidden' })
      expect(await tableFocused(page)).toBe(true)
      expect((await cursor(page))?.key).toBe(run)
    })
  })
})
