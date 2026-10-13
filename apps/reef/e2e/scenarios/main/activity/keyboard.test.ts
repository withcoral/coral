import { expect, test } from 'vitest'
import type { Locator, Page } from 'playwright'
import { withOwnerSession } from '../../../support/actors.ts'
import { managementClients } from '../../../support/management.ts'
import {
  breadcrumb,
  createWorkspace,
  observeRun,
  programRunner,
} from '../../../support/providers.ts'

// Traversal order, gaps, expansion rules, key ownership, the draft guard, and
// live cursor identity are keyboard.test.tsx cases. This scenario keeps what
// needs a real layout, real focus, and a child window the reader loads.

/** Where the keyboard cursor rests: its label, and its slot in its window. */
const cursorAt = (table: Locator) =>
  table.locator('[role="row"][data-active]').evaluate((row) => {
    const item = row.closest<HTMLElement>('[data-window-index]')
    const bounds = row.getBoundingClientRect()
    const x = bounds.left + bounds.width / 2
    return {
      label: row.querySelector('a')?.textContent ?? '',
      window: item?.closest<HTMLElement>('[data-window]')?.dataset.window ?? null,
      index: item ? Number(item.dataset.windowIndex) : null,
      // Unloaded ordinals follow the cursor directly.
      beforeGap: item?.nextElementSibling?.hasAttribute('data-window-from') ?? false,
      // The row's top and bottom edges belong to it, not to a sticky heading.
      shown: [bounds.top + 1, bounds.bottom - 1].every((y) =>
        row.contains(document.elementFromPoint(x, y)),
      ),
    }
  })

/** Tabs forward until the table itself has focus, as a keyboard user enters it. */
async function tabInto(page: Page, table: Locator) {
  for (let presses = 0; !(await table.evaluate((t) => t === document.activeElement)); presses++) {
    expect(presses, 'Tab reaches the Activity table').toBeLessThan(60)
    await page.keyboard.press('Tab')
  }
}

/** Two Tasks; the newer one holds a search and a Run with more emissions than one window. */
async function recordHistory(execute: (intent: string) => ReturnType<typeof programRunner>) {
  const older = execute(`Keyboard second ${crypto.randomUUID()}`)
  await observeRun(older, await older.exec('return "second";'))
  const intent = `Keyboard first ${crypto.randomUUID()}`
  const newer = execute(intent)
  expect((await newer.search('keyboard')).isError).not.toBe(true)
  await observeRun(
    newer,
    await newer.exec('for (let i = 0; i < 130; i++) coral.emit({ i }); return "first";'),
  )
  return intent
}

async function driveActivity(
  page: Page,
  step: (name: string, body: () => Promise<void>) => Promise<void>,
  path: string,
  intent: string,
) {
  const table = page.getByRole('table', { name: 'Activity records', exact: true })
  const press = async (key: string) => {
    await page.keyboard.press(key)
    return cursorAt(table)
  }
  await step('Open a short Activity page', async () => {
    // A short viewport keeps most emissions out of view.
    await page.setViewportSize({ width: 1440, height: 600 })
    await page.goto(path)
    await table.getByRole('link', { name: intent, exact: true }).waitFor()
    // The table's keys act only while the table itself has focus.
    await tabInto(page, table)
  })
  await step('Space expands the cursor Task and then its Run', async () => {
    expect((await press('ArrowDown')).label).toBe(intent)
    await page.keyboard.press('Space')
    await table.getByRole('link', { name: 'Call one Operation', exact: true }).waitFor()
    for (let presses = 0; (await cursorAt(table)).label !== 'Call one Operation'; presses++) {
      expect(presses).toBeLessThan(3)
      await page.keyboard.press('ArrowDown')
    }
    await page.keyboard.press('Space')
    await table
      .getByRole('link', { name: /^emission #/ })
      .first()
      .waitFor()
    expect(new URL(page.url()).searchParams.getAll('expand')).toHaveLength(2)
  })
  await step('↓ walks every emission in order, below the pinned headings', async () => {
    // The Run's first window ends before its last emissions. The cursor stops
    // there until scrolling loads them; it never skips to the next Task.
    let previous = await cursorAt(table)
    let emissions = 0
    let gaps = 0
    for (let presses = 0; presses < 400; presses++) {
      const at = await press('ArrowDown')
      expect(at.shown, `${at.label} shows in full`).toBe(true)
      if (at.label === previous.label) {
        expect(previous.beforeGap, `${at.label} stops only at a gap`).toBe(true)
        await page.waitForTimeout(50)
      } else if (at.window === previous.window && at.index !== null) {
        expect(at.index, `${at.label} follows ${previous.label}`).toBe(previous.index! + 1)
      }
      if (at.label.startsWith('emission #') && at.label !== previous.label) emissions++
      if (at.beforeGap && at.label !== previous.label) gaps++
      previous = at
      if (!at.window) break
    }
    // The walk left the expanded Task for the older one, past every emission.
    expect(previous.label).toMatch(/^Keyboard second /)
    expect(emissions).toBe(130)
    // The cursor reached the end of a loaded window at least once.
    expect(gaps).toBeGreaterThan(0)
  })
  let older = ''
  let child = ''
  await step('Enter opens the cursor record, and ↓ steps the open detail', async () => {
    older = (await cursorAt(table)).label
    child = (await press('ArrowUp')).label
    await page.keyboard.press('Enter')
    const dialog = page.getByRole('dialog', { name: 'Activity details', exact: true })
    await dialog.waitFor()
    // The drawer itself takes focus, so its arrows step the detail.
    await expect.poll(() => dialog.evaluate((d) => d === document.activeElement)).toBe(true)
    await page.waitForURL((url) => url.searchParams.has('record'))
    const opened = new URL(page.url()).searchParams.get('record')
    // A shorter window makes the detail scroll. The mark shows the dialog stays mounted.
    await page.setViewportSize({ width: 1440, height: 320 })
    expect(
      await dialog.evaluate((element) => {
        element.dataset.reelOpened = 'once'
        element.scrollTop = element.scrollHeight
        return element.scrollTop
      }),
    ).toBeGreaterThan(0)
    await page.keyboard.press('ArrowDown')
    await dialog.getByRole('heading', { name: older, exact: true }).waitFor()
    expect(new URL(page.url()).searchParams.get('record')).not.toBe(opened)
    expect(await dialog.getAttribute('data-reel-opened')).toBe('once')
    expect(await dialog.evaluate((element) => element.scrollTop)).toBe(0)
    // The step replaced the entry Enter added, so Back leaves the detail.
    await page.goBack()
    await page.waitForURL((url) => !url.searchParams.has('record'))
    await page.goForward()
    await dialog.getByRole('heading', { name: older, exact: true }).waitFor()
    await page.setViewportSize({ width: 1440, height: 600 })
  })
  await step(
    'A click on another record keeps the detail open, and ↓ in the table steps it',
    async () => {
      const dialog = page.getByRole('dialog', { name: 'Activity details', exact: true })
      const shown = new URL(page.url()).searchParams.get('record')
      const link = table.getByRole('link', { name: child, exact: true })
      await link.click()
      await page.waitForURL((url) => url.searchParams.get('record') !== shown)
      await dialog.getByRole('heading', { name: older, exact: true }).waitFor({ state: 'detached' })
      expect(await dialog.getAttribute('data-reel-opened')).toBe('once')
      // The table itself takes the keys; the clicked link keeps its own.
      await table.focus()
      await page.keyboard.press('ArrowDown')
      await dialog.getByRole('heading', { name: older, exact: true }).waitFor()
      // The step scrolled the shown record's row into the table's view.
      const row = table.locator('[role="row"][aria-selected="true"]')
      await expect
        .poll(() =>
          row.evaluate((element) => {
            const port = element.closest('[role="table"]')!.getBoundingClientRect()
            const bounds = element.getBoundingClientRect()
            return bounds.top >= port.top && bounds.bottom <= port.bottom
          }),
        )
        .toBe(true)
      expect(await row.locator('a').first().textContent()).toBe(older)
    },
  )
  await step(
    'Escape returns focus to the table, on the last record the detail showed',
    async () => {
      await page.keyboard.press('Escape')
      await page.waitForURL((url) => !url.searchParams.has('record'))
      await expect
        .poll(() => table.evaluate((element) => element === document.activeElement))
        .toBe(true)
      const at = await cursorAt(table)
      expect(at.label).toBe(older)
      expect(at.shown).toBe(true)
      expect((await press('ArrowUp')).label).toBe(child)
    },
  )
}

test('The keyboard drives workspace Activity, its tree, and its detail', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor, target }) => {
    const workspace = await createWorkspace(actor.accessToken, `Keys ${crypto.randomUUID()}`)
    let intent = ''
    await step('Record two Tasks', async () => {
      intent = await recordHistory((taskIntent) =>
        programRunner(page, target, workspace, { taskIntent }),
      )
    })
    await driveActivity(page, step, `/workspaces/${workspace}/activity`, intent)
  })
})

test('The keyboard drives an agent’s Activity, its tree, and its detail', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor, target }) => {
    const workspace = await createWorkspace(actor.accessToken, `Agent keys ${crypto.randomUUID()}`)
    const issued = await managementClients(actor.accessToken).agents.createAgent({
      parent: `workspaces/${workspace}`,
      displayName: 'Keyboard worker',
      requestId: crypto.randomUUID(),
    })
    let intent = ''
    await step('Record two Tasks as the agent', async () => {
      intent = await recordHistory((taskIntent) =>
        programRunner(page, target, workspace, { taskIntent, accessToken: issued.credential }),
      )
    })
    await driveActivity(page, step, `/${issued.agent!.name}`, intent)
  })
})

// Reaching Activity through the sidebar tab, rather than `page.goto`, leaves
// that link focused. Activity's keys act only on the focused table, so the link
// keeps its own keys until Tab brings focus into the table.
test('Activity keys wait for the table: the sidebar keeps its keys, and Tab enters the table', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor, target }) => {
    const workspace = await createWorkspace(actor.accessToken, `First press ${crypto.randomUUID()}`)
    const intent = `First press task ${crypto.randomUUID()}`
    const table = page.getByRole('table', { name: 'Activity records', exact: true })
    let tab: Locator
    await step('Record one Task with a Run', async () => {
      const execute = programRunner(page, target, workspace, { taskIntent: intent })
      await observeRun(execute, await execute.exec('return "ready";'))
    })
    await step('Open the Workspace elsewhere, then click into Activity', async () => {
      await page.goto(`/workspaces/${workspace}`)
      // Hydration first: a click that lands before it opens nothing.
      await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
      tab = page.getByRole('navigation', { name: 'Coral', exact: true }).getByRole('link', {
        name: /^Activity(\. \d+ pending actions?|\. Pending actions unavailable)?$/,
      })
      await tab.click()
      // The Workspace page's Activity glimpse shows the same Task in a table of
      // the same name, so wait for the Activity route itself before the keys.
      await page.waitForURL((url) => url.pathname === `/workspaces/${workspace}/activity`)
      await breadcrumb(page, 'Activity').waitFor()
      await table.getByRole('link', { name: intent, exact: true }).waitFor()
      // The sidebar tab click opened a plain list: no selection, expansion, or anchor.
      expect(new URL(page.url()).search).toBe('')
      expect(await tab.evaluate((link) => link === document.activeElement)).toBe(true)
    })
    await step('↓ and Space leave the focused sidebar link its own keys', async () => {
      await page.keyboard.press('ArrowDown')
      await page.keyboard.press('Space')
      expect(await tab.evaluate((link) => link === document.activeElement)).toBe(true)
      expect(await table.locator('[role="row"][data-active]').count()).toBe(0)
      expect(new URL(page.url()).search).toBe('')
    })
    await step('Tab enters the table, where ↓ then Space expand the Task', async () => {
      await tabInto(page, table)
      const before = await table.getByRole('row').count()
      await page.keyboard.press('ArrowDown')
      expect((await cursorAt(table)).label).toBe(intent)
      await page.keyboard.press('Space')
      await expect.poll(() => table.getByRole('row').count()).toBeGreaterThan(before)
      expect(new URL(page.url()).searchParams.getAll('expand')).toHaveLength(1)
    })
  })
})
