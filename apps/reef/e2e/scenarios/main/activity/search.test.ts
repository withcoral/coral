import { createServer, type ServerResponse } from 'node:http'
import type { Page } from 'playwright'
import { expect, test, type TestContext } from 'vitest'
import { authenticatedTarget, loginActor, mcpToken } from '../../../support/actors.ts'
import { withBrowserSession, type Session } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import { listen, openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  createWorkspace,
  enableTool,
  observeRun,
  programRunner,
} from '../../../support/providers.ts'
import { withOwner } from '../../../support/topology.ts'

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
        await table.hover()
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

/** An Owner's new workspace, whose Programs each wait in the fixture's `lookup` until released. */
async function withHeldRuns(
  context: TestContext,
  journey: (session: {
    page: Page
    step: Session['step']
    word: string
    /** Starts a Program of `lookups` calls, and returns its Run once its first call waits. */
    hold: (taskIntent: string, intent: string, lookups?: number) => Promise<string>
    /** Ends one Run's waiting call. */
    release: () => void
    calls: () => number
    finish: (taskIntent: string, intent: string) => Promise<string>
    /** The Run and its tool calls. */
    records: (run: string) => Promise<string[]>
    open: (run: string) => Promise<void>
    search: (query: string) => Promise<void>
    shown: () => string | null
    address: () => URLSearchParams
  }) => Promise<void>,
) {
  // Each call waits here until the test ends, so its Run stays running.
  const held: ServerResponse[] = []
  let calls = 0
  const upstream = await listen(
    context,
    createServer((_request, response) => {
      calls += 1
      held.push(response)
    }),
  )
  const release = () => held.shift()?.writeHead(200).end('{}')
  context.onTestFinished(() => {
    while (held.length) release()
  })
  await withOwner(context, async ({ page, target, step, accessToken }) => {
    const api = managementClients(accessToken)
    const word = `zq${crypto.randomUUID().slice(0, 8)}`
    const workspace = await createWorkspace(accessToken, `Hold ${word}`)
    const provider = await addProviderFromFile(
      page,
      workspace,
      openApiDocument(
        'Hold fixture',
        { lookup: { summary: 'Look up one record' } },
        { server: upstream.origin },
      ),
      'holdFixture',
    )
    await page.goto(provider)
    await enableTool(page, 'lookup')
    const address = () => new URL(page.url()).searchParams
    const runName = (id: string) => `workspaces/${workspace}/runs/${id}`
    const records = async (run: string) => [
      run,
      ...(await api.activity.browseRunEntries({ name: run, pageSize: 100 })).entries.flatMap(
        ({ record }) => (record.case === 'invocation' ? [record.value.name] : []),
      ),
    ]
    await journey({
      page,
      step,
      word,
      release,
      calls: () => calls,
      records,
      address,
      shown: () => address().get('record'),
      hold: async (taskIntent, intent, lookups = 1) => {
        const before = calls
        // exec returns at once; the Run goes on without it.
        const execute = programRunner(page, target, workspace, { waitMs: 1, taskIntent })
        const accepted = await execute.exec(
          'await coral.providers.holdFixture.lookup({});\n'.repeat(lookups),
          intent,
        )
        expect(accepted.isError).not.toBe(true)
        await expect.poll(() => calls, { timeout: 15_000 }).toBe(before + 1)
        return runName(accepted.structuredContent.run_id)
      },
      finish: async (taskIntent, intent) => {
        const execute = programRunner(page, target, workspace, { taskIntent })
        const { update } = await observeRun(execute, await execute.exec('return 1;', intent))
        return runName(update.run_id)
      },
      open: async (run) => {
        const query = new URLSearchParams({ tree: '1', live: 'latest', record: run })
        await page.goto(`/workspaces/${workspace}/activity?${query}`)
        await page.getByRole('dialog', { name: 'Activity details' }).waitFor()
        // Live holds the Run: it shows the Run's newest record.
        const newest = (await records(run)).at(-1)
        await expect.poll(() => address().get('record'), { timeout: 15_000 }).toBe(newest)
      },
      search: (query) =>
        page.getByRole('searchbox', { name: 'Search intent', exact: true }).fill(query),
    })
  })
}

test.for(['runs', 'ended'] as const)(
  'Live moves from a running Run the search leaves out to a matching Run that %s',
  async (state, context) => {
    await withHeldRuns(context, async (session) => {
      const { step, word, hold, finish, records, open, search, shown, address } = session
      const leftOut = await hold('Unrelated chores', 'Unrelated wait')
      const start = state === 'runs' ? hold : finish
      const matching = await start('Weekly chores', `Export ${word} invoices`)
      await step('Live holds the running Run you open, with no search', () => open(leftOut))
      await step('Search for the other Run, and Live moves to it', async () => {
        await search(word)
        await expect
          .poll(async () => (await records(matching)).includes(shown() ?? ''), { timeout: 15_000 })
          .toBe(true)
        expect(address().get('q')).toBe(word)
        expect(address().get('live')).toBe('latest')
      })
    })
  },
)

test('Live waits while the search matches no Run, then moves to the first that arrives', async (context) => {
  await withHeldRuns(context, async (session) => {
    const { page, step, word, hold, release, calls, records, open, search, shown, address } =
      session
    const leftOut = await hold('Unrelated chores', 'Unrelated wait', 3)
    /** Ends the Run's waiting call, and returns its next one once Coral records it. */
    const nextCall = async () => {
      const count = (await records(leftOut)).length
      release()
      await expect.poll(calls, { timeout: 15_000 }).toBe(count)
      await expect
        .poll(async () => (await records(leftOut)).length, { timeout: 15_000 })
        .toBe(count + 1)
      return (await records(leftOut)).at(-1)
    }
    await step('Live follows the running Run you open, with no search', async () => {
      await open(leftOut)
      const next = await nextCall()
      await expect.poll(shown, { timeout: 15_000 }).toBe(next)
    })
    await step('Search with no match, and Live stops following the open Run', async () => {
      const opened = shown()
      await search(word)
      await page.getByText('No matching activity').waitFor()
      await nextCall()
      await page.waitForTimeout(3_000)
      expect(shown()).toBe(opened)
      expect(address().get('live')).toBe('latest')
    })
    await step('A matching Run starts, and Live moves to it', async () => {
      const matching = await hold('Weekly chores', `Export ${word} invoices`)
      await expect
        .poll(async () => (await records(matching)).includes(shown() ?? ''), { timeout: 15_000 })
        .toBe(true)
    })
  })
})
