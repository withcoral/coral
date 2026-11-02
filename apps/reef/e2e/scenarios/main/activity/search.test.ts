import { expect, test } from 'vitest'
import { authenticatedTarget, loginActor, mcpToken } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { createWorkspace, observeRun, programRunner } from '../../../support/providers.ts'

test('Activity search lists matching Tasks and Runs as you type, follows new matches live, and keeps the search in the address and history', async (context) => {
  const actor = await loginActor(context, authenticatedTarget().authentication.owner)
  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      // A word no other record of this shared Owner holds.
      const word = `zq${crypto.randomUUID().slice(0, 8)}`
      const workspace = await createWorkspace(actor.accessToken, `Search ${word}`)
      const accessToken = await mcpToken(page, workspace)
      const runner = (taskIntent: string) =>
        programRunner(page, target, workspace, { accessToken, taskIntent })
      const run = async (execute: ReturnType<typeof runner>, intent: string) =>
        observeRun(execute, await execute.exec('return 1;', intent))
      const byTask = runner(`Rotate the ${word.toUpperCase()} keys`)
      await run(byTask, 'Read the key list')
      const byRun = runner('Weekly chores')
      await run(byRun, `Export ${word} invoices`)
      await run(byRun, 'Tidy the archive')
      await run(runner('Unrelated work'), 'Unrelated run')

      const table = page.getByRole('table', { name: 'Activity records', exact: true })
      const field = page.getByRole('searchbox', { name: 'Search intent', exact: true })
      const record = (name: string) => table.getByRole('link', { name, exact: true })
      await step('Type a search and see only the matching Task and Run', async () => {
        await page.goto(`/workspaces/${workspace}/activity`)
        await record('Unrelated work').waitFor()
        await field.fill(word)
        await record('Unrelated work').waitFor({ state: 'detached' })
        await record(`Rotate the ${word.toUpperCase()} keys`).waitFor()
        // A Task kept for a Run opens to show only that Run.
        await record('Weekly chores').waitFor()
        await record(`Export ${word} invoices`).waitFor()
        expect(await record('Tidy the archive').count()).toBe(0)
        // Each matching intent marks its match, whatever its case.
        expect((await table.locator('strong').allInnerTexts()).toSorted()).toEqual(
          [word, word.toUpperCase()].toSorted(),
        )
        expect(new URL(page.url()).searchParams.get('q')).toBe(word)
      })
      await step('See a new matching Task arrive while the search stays', async () => {
        await run(runner(`Late ${word} follow-up`), 'Read the follow-up')
        await record(`Late ${word} follow-up`).waitFor()
        expect(await record('Unrelated work').count()).toBe(0)
      })
      await step('Reload and keep the search', async () => {
        await page.reload()
        await expect.poll(() => field.inputValue()).toBe(word)
        await record('Weekly chores').waitFor()
        expect(await record('Unrelated work').count()).toBe(0)
      })
      await step('Go Back to the previous search, then clear it', async () => {
        await field.fill(`${word}-missing`)
        await record('Weekly chores').waitFor({ state: 'detached' })
        await page.goBack()
        await page.waitForURL((url) => url.searchParams.get('q') === word)
        await record('Weekly chores').waitFor()
        await expect.poll(() => field.inputValue()).toBe(word)
        await field.fill('')
        await record('Unrelated work').waitFor()
        expect(new URL(page.url()).searchParams.has('q')).toBe(false)
      })
      await step('Reload a search, then go Back to all activity', async () => {
        await field.fill(word)
        await record('Unrelated work').waitFor({ state: 'detached' })
        await page.reload()
        await record('Weekly chores').waitFor()
        await page.goBack()
        await page.waitForURL((url) => !url.searchParams.has('q'))
        await record('Unrelated work').waitFor()
        await expect.poll(() => field.inputValue()).toBe('')
      })
    },
    { actor: actor.label, storageState: actor.storageState },
  )
})

test('Activity search ignores Unicode case, and opens every Task it lists for its Runs through later pages and live arrivals', async (context) => {
  const actor = await loginActor(context, authenticatedTarget().authentication.owner)
  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      const word = `zq${crypto.randomUUID().slice(0, 8)}`
      const workspace = await createWorkspace(actor.accessToken, `Reveal ${word}`)
      const accessToken = await mcpToken(page, workspace)
      const runner = (taskIntent: string) =>
        programRunner(page, target, workspace, { accessToken, taskIntent })
      const matching = async (index: number) => {
        const execute = runner(`Chores ${index}`)
        await observeRun(execute, await execute.exec('return 1;', `ÉCHÉANCE ${word} ${index}`))
        await observeRun(execute, await execute.exec('return 1;', `Tidy up ${index}`))
      }
      // More than the limit on records you expand, and more than one page.
      const count = 26
      for (let index = 0; index < count; index += 1) await matching(index)

      const table = page.getByRole('table', { name: 'Activity records', exact: true })
      const field = page.getByRole('searchbox', { name: 'Search intent', exact: true })
      const run = (index: number) =>
        table.getByRole('link', { name: `ÉCHÉANCE ${word} ${index}`, exact: true })
      await step('Search in another case and see the Runs of every Task', async () => {
        await page.goto(`/workspaces/${workspace}/activity`)
        await field.fill(`échéance ${word}`)
        for (let index = count - 1; index >= 0; index -= 1) {
          await run(index).scrollIntoViewIfNeeded()
          await run(index).waitFor()
        }
        expect(await table.getByRole('link', { name: 'Tidy up 0', exact: true }).count()).toBe(0)
        expect(await table.locator('strong').first().innerText()).toBe(`ÉCHÉANCE ${word}`)
      })
      await step('See the Runs of a Task that arrives while you search', async () => {
        // Live inserts new records only at the top of the list.
        await run(count - 1).scrollIntoViewIfNeeded()
        await page.mouse.wheel(0, -100_000)
        await matching(count)
        await run(count).waitFor()
      })
      await step('Collapse one Task, keep it collapsed on reload, then open it', async () => {
        const row = table.getByRole('row', { name: /Chores 0/ }).first()
        await row.scrollIntoViewIfNeeded()
        await row.getByRole('button', { name: 'Collapse record' }).click()
        await run(0).waitFor({ state: 'detached' })
        expect(new URL(page.url()).searchParams.has('collapse')).toBe(true)
        await page.reload()
        await run(count).waitFor()
        const closed = table.getByRole('row', { name: /Chores 0/ }).first()
        await closed.scrollIntoViewIfNeeded()
        await closed.getByRole('button', { name: 'Expand record' }).click()
        await run(0).waitFor()
        expect(new URL(page.url()).searchParams.has('collapse')).toBe(false)
      })
    },
    { actor: actor.label, storageState: actor.storageState },
  )
})
