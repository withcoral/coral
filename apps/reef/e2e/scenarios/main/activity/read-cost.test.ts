import { expect, test } from 'vitest'
import { withOwnerSession, mcpToken } from '../../../support/actors.ts'
import { managementClients } from '../../../support/management.ts'
import { programRunner } from '../../../support/providers.ts'

// What it costs to open a Task of many Runs, against real Coral. The waterfall reads a
// window for each Run, so the number of reads grows with the Task. This journey proves
// that the waterfall reads every Run before its time breakdown states a value, and bounds
// the reads it took. The bound holds for any size: change `RUNS` in a copy of the tree to
// measure another.

const RUNS = 116
/** Run windows one request reads beside its Task's, as `TRACE_WINDOWS_PER_REQUEST`. */
const WINDOWS_PER_REQUEST = 10
/** Whether a request is one of the reader's own reads of a page. */
const isRead = (address: string) => new URL(address).searchParams.get('load') === 'page'

test('The waterfall of a Task of many Runs reads every Run in bounded reads', async (context) => {
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const api = managementClients(actor.accessToken)
    const workspace = await api.workspaces.createWorkspace({
      displayName: 'Read cost',
      requestId: crypto.randomUUID(),
    })
    const id = workspace.name.split('/')[1]
    const runner = programRunner(page, target, id, {
      accessToken: await mcpToken(page, id),
      taskIntent: 'Cost goal',
    })
    const creating = Date.now()
    // A Run that creates a private tool, one that calls it twice, so the body's calls are
    // read too, and one whose emissions fill two child windows. Every other Run is the shortest there is.
    expect(
      (await runner.addCustomTool('cost.echo', 'Echo', 'const echo = () => 1;', 'echo')).isError,
    ).not.toBe(true)
    for (const program of [
      'await coral.private.cost.echo({}); return (await coral.private.cost.echo({})).data;',
      'for (let i = 0; i < 150; i++) await coral.emit({ i });',
    ])
      expect((await runner.exec(program)).isError).not.toBe(true)
    for (let index = 3; index < RUNS; index++)
      expect((await runner.exec('return null;')).isError).not.toBe(true)
    const created = Date.now() - creating
    const task = `${workspace.name}/tasks/${runner.taskId()}`

    // Coral's own list of the Task's Runs, page by page.
    const runs: string[] = []
    for (let after = '', pages = 0; pages < 50; pages++) {
      const listed = await api.activity.inspectTask({ name: task, pageSize: 100, after })
      runs.push(...listed.interactions.flatMap(({ run }) => (run ? [run.name] : [])))
      after = listed.nextAfter
      if (!after) break
    }
    // Exactly the Runs this journey started, each once.
    expect(runs).toHaveLength(RUNS)
    expect(new Set(runs).size).toBe(RUNS)

    // Each read the page starts, and each answer. Playwright's `requestfinished` does not
    // fire for every streamed `.data` answer, so it undercounts; a started request and
    // its response do not.
    const reads: URLSearchParams[] = []
    const answers: number[] = []
    page.on('request', (request) => {
      if (isRead(request.url())) reads.push(new URL(request.url()).searchParams)
    })
    page.on('response', (response) => {
      if (isRead(response.url())) answers.push(response.status())
    })
    const table = page.getByRole('table', { name: 'Activity records', exact: true })
    let reading = 0
    await step('Open the Task: every Run gets its row, and its records are read', async () => {
      const opened = Date.now()
      await page.goto(`/workspaces/${id}/activity?tree=1&expand=${encodeURIComponent(task)}`)
      // Each Run Coral lists gets a row once the trace reads it.
      const shown = () =>
        table
          .locator('[data-record-key*="/runs/"]')
          .evaluateAll((rows) => rows.map((row) => row.getAttribute('data-record-key')!))
      await expect
        .poll(async () => new Set(await shown()), { timeout: 40_000 })
        .toEqual(new Set(runs))
      await page.getByText(/^Loading records… \d/).waitFor({ state: 'detached', timeout: 40_000 })
      // The time breakdown states a value only once the trace holds every record.
      await page.getByText('Agent model', { exact: true }).waitFor({ timeout: 40_000 })
      reading = Date.now() - opened
      // No row says that records under it still load.
      expect(await table.getByText(/^(Loading|Not loaded)/).count()).toBe(0)
      expect(await page.getByText(/^Loading records…/).count()).toBe(0)
    })

    // What the page asked Coral for. The list's own page already holds the Task's first
    // window; the trace reads the rest.
    const windows = reads.filter((query) => !query.has('record'))
    const details = reads.filter((query) => query.has('record'))
    const asked = new Set(
      windows.flatMap((query) => query.getAll('expand')).filter((name) => name.includes('/runs/')),
    )
    console.log(
      `READCOST runs=${runs.length} create_ms=${created} read_ms=${reading} window_requests=${windows.length} windows=${windows.reduce((sum, query) => sum + query.getAll('expand').length, 0)} run_windows_asked=${asked.size} detail_requests=${details.length}`,
    )
    // Every read was answered, and none with an error. Checked here: a failed assertion in
    // a listener does not fail the journey.
    expect(answers).toEqual(reads.map(() => 200))
    // Every Run's window was asked for, each in a request that names its Task too.
    expect(asked).toEqual(new Set(runs))
    // One request for each batch of Run windows, a few for the windows that follow a first
    // one, and no read of a record's detail.
    expect(windows.length).toBeLessThanOrEqual(Math.ceil(runs.length / WINDOWS_PER_REQUEST) + 6)
    expect(details).toHaveLength(0)
  })
})
