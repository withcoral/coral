import { createServer, type ServerResponse } from 'node:http'
import type { Page } from 'playwright'
import { expect, type TestContext } from 'vitest'
import { managementClients } from '../../../support/management.ts'
import { listen, openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  createWorkspace,
  enableTool,
  programRunner,
} from '../../../support/providers.ts'
import { withOwner } from '../../../support/topology.ts'

// A real Run that holds on its tool calls while the drawer follows it. Live mode
// moves the selection to each newest record Coral streams, and the drawer's split
// highlights the line Coral stored for it. The step rules stay in Reef's
// `navigation.test.ts` and `live.stories.tsx`; this proves them against real Coral.

const program = [
  'await coral.providers.followFixture.lookup({})',
  "console.info('between calls')",
  'await coral.providers.followFixture.lookup({})',
  'coral.emit({ done: true })',
  "return 'ok'",
].join('\n')
/** The location Coral stores for a call to `name` on `line` of the program. */
const inProgram = (line: number, name: string) => ({
  document: { case: 'program', value: {} },
  line,
  column: program.split('\n')[line - 1]!.indexOf(name) + 1,
})

/** The source of a record that no call made: nothing to highlight, nothing missing. */
const noCall = { lines: [], unavailable: false }

/** The drawer's highlighted source lines, and whether it says it has no location. */
export async function highlighted(page: Page) {
  const pane = page.getByRole('dialog').getByRole('region', { name: 'Accepted source' })
  await pane.locator('[data-line]').first().waitFor()
  return pane.evaluate((region) => ({
    lines: [...region.querySelectorAll<HTMLElement>('[data-line][data-highlighted]')].map((line) =>
      Number(line.dataset.line),
    ),
    unavailable: (region.textContent ?? '').includes('Source location unavailable'),
  }))
}

export async function followLatest(context: TestContext) {
  // Each lookup waits here until the journey releases it, so the Run stays running.
  const held: ServerResponse[] = []
  const upstream = await listen(
    context,
    createServer((_request, response) => void held.push(response)),
  )
  const release = () =>
    held.shift()!.writeHead(200, { 'content-type': 'application/json' }).end('{"found":true}')
  await withOwner(context, async ({ page, target, step, accessToken }) => {
    const api = managementClients(accessToken)
    const workspace = await createWorkspace(accessToken, `Follow latest ${crypto.randomUUID()}`)
    const provider = await addProviderFromFile(
      page,
      workspace,
      openApiDocument(
        'Follow fixture',
        { lookup: { summary: 'Look up one record' } },
        { server: upstream.origin },
      ),
      'followFixture',
    )
    await page.goto(provider)
    await enableTool(page, 'lookup')
    // exec returns at once; the Run goes on without it.
    const execute = programRunner(page, target, workspace, {
      waitMs: 1,
      taskIntent: `Follow calls ${crypto.randomUUID()}`,
    })
    const accepted = await execute.exec(program)
    expect(accepted.isError).not.toBe(true)
    const run = `workspaces/${workspace}/runs/${accepted.structuredContent.run_id}`
    const task = `workspaces/${workspace}/tasks/${execute.taskId()}`
    /** The Run's tool calls Coral has recorded, in order. */
    const tools = async () =>
      (await api.activity.browseRunEntries({ name: run, pageSize: 100 })).entries.flatMap(
        ({ record }) => (record.case === 'invocation' ? [record.value] : []),
      )

    const table = page.getByRole('table', { name: 'Activity records', includeHidden: true })
    const rail = page.getByRole('dialog', { name: 'Activity details' })
    const selected = rail.getByRole('region', { name: 'Selected record' })
    const toggle = rail.getByRole('button', { name: 'Follow latest record', exact: true })
    const address = () => new URL(page.url()).searchParams
    const selectedKey = () =>
      table.locator('[role=row][aria-selected=true]').first().getAttribute('data-record-key')
    /** Live mode selects `record` in the address and the table, and its header holds the toggle. */
    const follows = async (record: string, source: Awaited<ReturnType<typeof highlighted>>) => {
      await expect.poll(() => address().get('record'), { timeout: 15_000 }).toBe(record)
      await expect.poll(selectedKey).toBe(record)
      await expect.poll(() => highlighted(page)).toEqual(source)
      expect(address().get('live')).toBe('1')
      await expect.poll(() => toggle.getAttribute('aria-pressed')).toBe('true')
      await selected
        .locator('header')
        .getByRole('button', { name: 'Follow latest record' })
        .waitFor()
    }

    await step('Open the running Run with its accepted source', async () => {
      await expect.poll(() => held.length, { timeout: 15_000 }).toBe(1)
      const query = new URLSearchParams({ tree: '1', record: run })
      query.append('expand', task)
      query.append('expand', run)
      await page.goto(`/workspaces/${workspace}/activity?${query}`)
      // The toggle sits beside the close control in the selected record's own header.
      const header = selected.locator('header')
      await header.getByRole('button', { name: 'Follow latest record', exact: true }).waitFor()
      await header.getByRole('button', { name: 'Close details', exact: true }).waitFor()
      expect(await toggle.getAttribute('aria-pressed')).toBe('false')
      expect(address().has('live')).toBe(false)
      // The Run names no call, so its source has no highlight and no missing-location banner.
      await expect.poll(() => highlighted(page)).toEqual(noCall)
    })

    await step('Turn on live mode and select the held tool call', async () => {
      const [first] = await tools()
      expect(first?.sourceLocation).toMatchObject(inProgram(1, 'lookup'))
      await toggle.click()
      await follows(first!.name, { lines: [1], unavailable: false })
    })

    await step('Follow the next tool call Coral streams, at its own line', async () => {
      release()
      await expect.poll(() => held.length, { timeout: 15_000 }).toBe(1)
      const [, second] = await tools()
      expect(second?.sourceLocation).toMatchObject(inProgram(3, 'lookup'))
      await follows(second!.name, { lines: [3], unavailable: false })
    })

    await step('Return to the Run itself once it ends', async () => {
      release()
      // Not its last record: the Run is not a call either, so no highlight and no banner.
      await follows(run, noCall)
    })

    await step('Turn live mode off from the record header', async () => {
      await toggle.click()
      await expect.poll(() => address().has('live')).toBe(false)
      await expect.poll(() => toggle.getAttribute('aria-pressed')).toBe('false')
      expect(address().get('record')).toBe(run)
    })
  })
}
