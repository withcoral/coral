import type { Page } from 'playwright'
import { expect, test } from 'vitest'
import { withOwnerSession, mcpToken } from '../../../support/actors.ts'
import { managementClients } from '../../../support/management.ts'
import { observeRun, programRunner } from '../../../support/providers.ts'

// The keyboard on real Coral history: ↓ walks the list's rows and then the open timeline's,
// Enter opens a record, and ↓ in the rail steps through a real Run's Events in Coral's order.
// Key ownership and traversal rules are Vitest's and the stories'.

/** The focused row link: the table that holds it and its text. */
function focused(page: Page) {
  return page.evaluate(() => {
    const link = document.activeElement
    return {
      table: link?.closest('[role="table"]')?.getAttribute('aria-label') ?? null,
      label: link?.textContent ?? '',
    }
  })
}
/** Focus leaves every control, as a click on the page background leaves it. */
const toBackground = (page: Page) =>
  page.evaluate(() => (document.activeElement as HTMLElement | null)?.blur())
const record = (page: Page) => new URL(page.url()).searchParams.get('record')

test('The keyboard walks the Activity list into a timeline and steps the rail through a Run’s Events, for you and for an Agent', async (context) => {
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const api = managementClients(actor.accessToken)
    const workspace = await api.workspaces.createWorkspace({
      displayName: 'Keyboard activity',
      requestId: crypto.randomUUID(),
    })
    const id = workspace.name.split('/')[1]
    const issued = await api.agents.createAgent({
      parent: workspace.name,
      displayName: 'Keyboard worker',
      requestId: crypto.randomUUID(),
    })
    const scopes = [
      {
        label: 'your workspace Activity',
        path: `/workspaces/${id}/activity`,
        token: await mcpToken(page, id),
      },
      {
        label: "an Agent's complete Activity",
        path: `/${issued.agent!.name.split('/').map(encodeURIComponent).join('/')}?view=activity`,
        token: issued.credential,
      },
    ]
    for (const scope of scopes) {
      const goal = `Keyboard goal ${crypto.randomUUID()}`
      const other = `Other goal ${crypto.randomUUID()}`
      const older = programRunner(page, target, id, { accessToken: scope.token, taskIntent: other })
      await observeRun(older, await older.exec('return 1;'))
      const runner = programRunner(page, target, id, { accessToken: scope.token, taskIntent: goal })
      const observed = await observeRun(
        runner,
        await runner.exec('for (let i = 0; i < 3; i++) await coral.emit({ i }); return "done";'),
      )
      const run = `${workspace.name}/runs/${observed.update.run_id}`

      await step(`↓ walks the list and then the open timeline in ${scope.label}`, async () => {
        await page.goto(scope.path)
        // The newest root opens beside the list.
        await page
          .getByRole('table', { name: 'Timeline records', exact: true })
          .getByRole('link', { name: goal, exact: true })
          .waitFor()
        await toBackground(page)
        for (const expected of [
          { table: 'Activity records', label: goal },
          { table: 'Activity records', label: other },
          { table: 'Timeline records', label: goal },
          { table: 'Timeline records', label: 'Call one Tool' },
        ]) {
          await page.keyboard.press('ArrowDown')
          expect(await focused(page)).toEqual(expected)
        }
      })

      await step('Enter opens the Run, and ↓ in the rail steps through its Events', async () => {
        await page.keyboard.press('Enter')
        const rail = page.getByRole('dialog', { name: 'Activity details' })
        await rail.waitFor()
        await page.waitForURL(() => record(page) === run)
        await rail.focus()
        // Each step selects the Run's next record in Coral's order, through its return value.
        let previous = 0
        for (let press = 0; press < 10 && record(page) !== `${run}/final`; press++) {
          const at = record(page)
          await page.keyboard.press('ArrowDown')
          await page.waitForURL(() => record(page) !== at)
          const next = record(page)!
          if (next === `${run}/final`) break
          expect(next.startsWith(`${run}/events/`)).toBe(true)
          const sequence = Number(next.split('/').at(-1))
          expect(sequence).toBeGreaterThan(previous)
          previous = sequence
        }
        expect(record(page)).toBe(`${run}/final`)
        await rail.waitFor()
        await page.keyboard.press('ArrowUp')
        await page.waitForURL(() => record(page) === `${run}/events/${previous}`)
      })

      await step('Escape closes the rail and keeps the open timeline', async () => {
        await page.keyboard.press('Escape')
        await page.getByRole('dialog', { name: 'Activity details' }).waitFor({ state: 'hidden' })
        await page.waitForURL(() => record(page) === null)
        expect(new URL(page.url()).searchParams.get('root')).toBe(
          `${workspace.name}/tasks/${runner.taskId()}`,
        )
      })
    }
  })
})
