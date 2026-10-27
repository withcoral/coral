import { createServer } from 'node:http'
import type { Page } from 'playwright'
import { expect, type TestContext } from 'vitest'
import type {
  ActivityEventSummary,
  ActivityInvocation,
} from '../../../generated/coral/management/v1/activity_pb.ts'
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

// A real Run stores where the author's code made each tool call, log and emission.
// The API reads those locations back, and Activity highlights each record's own line in
// the accepted program or in the Saved Function snapshot the Run accepted, even after
// that Saved Function changes. Location rules stay in Reef's `source-location.test.ts`
// and the backend's runtime and repository tests.

const program = [
  'await coral.providers.locationFixture.lookup({})',
  'for (let i = 0; i < 2; i++) {',
  '  await coral.providers.locationFixture.lookup({})',
  '}',
  "console.info('looked up')",
  'coral.emit({ done: true })',
  'await coral.functions.locations.notify()',
  "return 'ok'",
].join('\n')
const snapshot = [
  'export default async () => {',
  '  await coral.providers.locationFixture.lookup({})',
  "  console.warn('accepted-snapshot-notice')",
  '  return 1',
  '}',
].join('\n')
// The same path, saved after the Run: different text, and its calls on other lines.
const replacement = [
  'export default async () => {',
  '  // replacement-source-marker',
  '  // moved down',
  "  console.warn('replacement-notice')",
  '  return 2',
  '}',
].join('\n')
/** Where a call to `name` on `line` starts, as Coral stores it: 1-based, in UTF-16 units. */
const at = (source: string, line: number, name: string) => ({
  line,
  column: source.split('\n')[line - 1]!.indexOf(name) + 1,
})

/** The location Coral stores for a call to `name` on `line` of the accepted program. */
const inProgram = (line: number, name: string) => ({
  document: { case: 'program', value: {} },
  ...at(program, line, name),
})

// The accepted program is the pane's default document, so it carries no name.
const PROGRAM = null
const SNAPSHOT = 'Saved-function snapshot: locations.notify'

/**
 * The open record's accepted-source pane: its document's name, if any, its highlighted lines,
 * whether the first highlighted line sits inside the pane's visible viewport, and
 * whether it is painted differently from an ordinary line.
 */
async function shownSource(page: Page) {
  const pane = page.getByRole('dialog').getByRole('region', { name: 'Accepted source' })
  await pane.locator('[data-line]').first().waitFor()
  const named = pane.getByText(SNAPSHOT, { exact: true })
  const label = (await named.count()) ? await named.innerText() : PROGRAM
  const painted = await pane.evaluate((region) => {
    const lines = [...region.querySelectorAll<HTMLElement>('[data-line]')]
    const highlighted = lines.filter((line) => line.hasAttribute('data-highlighted'))
    const numbers = highlighted.map((line) => Number(line.dataset.line))
    const first = highlighted[0]
    const ordinary = lines.find((line) => !line.hasAttribute('data-highlighted'))
    if (!first || !ordinary) return { lines: numbers, visible: false, distinct: false }
    let viewport = first.parentElement!
    while (viewport !== region && !/auto|scroll/.test(getComputedStyle(viewport).overflowY))
      viewport = viewport.parentElement!
    const box = first.getBoundingClientRect()
    const bounds = viewport.getBoundingClientRect()
    return {
      lines: numbers,
      visible: box.top >= bounds.top && box.bottom <= bounds.bottom && box.height > 0,
      distinct:
        getComputedStyle(first).backgroundColor !== getComputedStyle(ordinary).backgroundColor,
    }
  })
  return { label, ...painted, text: await pane.innerText() }
}

export async function sourceLocations(context: TestContext) {
  const upstream = await listen(
    context,
    createServer((_request, response) => {
      response.writeHead(200, { 'content-type': 'application/json' }).end('{"found":true}')
    }),
  )
  await withOwner(context, async ({ page, target, step, accessToken }) => {
    const api = managementClients(accessToken)
    const workspace = await createWorkspace(accessToken, `Source locations ${crypto.randomUUID()}`)
    const provider = await addProviderFromFile(
      page,
      workspace,
      openApiDocument(
        'Location fixture',
        { lookup: { summary: 'Look up one record' } },
        { server: upstream.origin },
      ),
      'locationFixture',
    )
    await page.goto(provider)
    await enableTool(page, 'lookup')
    const execute = programRunner(page, target, workspace, {
      taskIntent: `Locate calls ${crypto.randomUUID()}`,
    })
    expect(
      (await execute.saveFunction('locations.notify', 'Notify after lookups', snapshot)).isError,
    ).not.toBe(true)
    const observed = await observeRun(execute, await execute.exec(program))
    expect(observed.update.state, JSON.stringify(observed.update.failure)).toBe('succeeded')
    const run = `workspaces/${workspace}/runs/${observed.update.run_id}`
    const task = `workspaces/${workspace}/tasks/${execute.taskId()}`

    type Shown = { name: string; label: string | null; line: number }
    let savedFunction = ''
    let tools: ActivityInvocation[] = []
    let outputs: ActivityEventSummary[] = []
    const inSnapshot = (line: number, name: string) => ({
      document: { case: 'savedFunction', value: savedFunction },
      ...at(snapshot, line, name),
    })
    /** Opens `record` and waits until its own stored line shows highlighted in its document. */
    const highlights = (kind: string, record: Shown) =>
      step(`Highlight the ${kind} at its stored line`, async () => {
        const query = new URLSearchParams()
        query.append('expand', task)
        query.append('expand', run)
        query.set('record', record.name)
        await page.goto(`/workspaces/${workspace}/activity?${query}`)
        await expect
          .poll(async () => {
            const { text: _, ...shown } = await shownSource(page)
            return shown
          })
          .toEqual({ label: record.label, lines: [record.line], visible: true, distinct: true })
        const { text } = await shownSource(page)
        expect(text).not.toContain('Source location unavailable')
        if (record.label === SNAPSHOT) {
          expect(text).toContain('accepted-snapshot-notice')
          expect(text).not.toContain('replacement-source-marker')
        }
      })

    // The program's records first, then the Saved Function's: a failure in one still
    // leaves the other's evidence recorded.
    await step('Read the program’s stored locations back through the management API', async () => {
      const accepted = await api.activity.inspectRun({ name: run, pageSize: 100 })
      expect(accepted.linkedFunctions).toHaveLength(1)
      savedFunction = accepted.linkedFunctions[0]!.savedFunction
      const entries = await api.activity.browseRunEntries({ name: run, pageSize: 100 })
      tools = entries.entries.flatMap(({ record }) =>
        record.case === 'invocation' ? [record.value] : [],
      )
      outputs = entries.entries.flatMap(({ record }) =>
        record.case === 'event' && record.value.kind.startsWith('program.') ? [record.value] : [],
      )
      expect(tools).toHaveLength(4)
      expect(outputs).toHaveLength(3)
      expect({
        tools: tools.slice(0, 3).map((value) => value.sourceLocation),
        outputs: outputs.slice(0, 2).map((value) => value.sourceLocation),
      }).toMatchObject({
        tools: [inProgram(1, 'lookup'), inProgram(3, 'lookup'), inProgram(3, 'lookup')],
        outputs: [inProgram(5, 'info'), inProgram(6, 'emit')],
      })
      // The exact reads carry the same locations as the entries that list them.
      for (const value of tools) {
        const exact = await api.activity.inspectInvocation({ name: value.name })
        expect(exact.invocation?.sourceLocation).toEqual(value.sourceLocation)
      }
      for (const value of outputs) {
        const exact = await api.activity.inspectRunEvent({ run, sequence: value.sequence })
        expect(exact.event?.sourceLocation).toEqual(value.sourceLocation)
      }
    })
    const event = (index: number) => `${run}/events/${outputs[index]!.sequence}`
    await highlights('tool', { name: tools[0]!.name, label: PROGRAM, line: 1 })
    // One call site in a loop: both records keep the line they ran.
    await highlights('first loop call', { name: tools[1]!.name, label: PROGRAM, line: 3 })
    await highlights('second loop call', { name: tools[2]!.name, label: PROGRAM, line: 3 })
    await highlights('log', { name: event(0), label: PROGRAM, line: 5 })
    await highlights('emission', { name: event(1), label: PROGRAM, line: 6 })

    await step('Change the Saved Function; the Run keeps the snapshot it accepted', async () => {
      expect(
        (await execute.saveFunction('locations.notify', 'Replaced after the Run', replacement))
          .isError,
      ).not.toBe(true)
      const accepted = await api.activity.inspectRun({ name: run, pageSize: 1 })
      expect(accepted.linkedFunctions.map(({ path, source }) => ({ path, source }))).toEqual([
        { path: 'locations.notify', source: snapshot },
      ])
    })
    await step('Read the Saved Function’s stored locations back in its snapshot', async () => {
      expect({
        tool: tools[3]!.sourceLocation,
        log: outputs[2]!.sourceLocation,
      }).toMatchObject({ tool: inSnapshot(2, 'lookup'), log: inSnapshot(3, 'warn') })
    })
    await highlights('Saved Function tool', {
      name: tools[3]!.name,
      label: SNAPSHOT,
      line: 2,
    })
    await highlights('Saved Function log', { name: event(2), label: SNAPSHOT, line: 3 })
  })
}
