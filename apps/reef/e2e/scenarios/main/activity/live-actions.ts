import { createServer, type ServerResponse } from 'node:http'
import { expect, type TestContext } from 'vitest'
import { managementClients } from '../../../support/management.ts'
import { listen, openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  createWorkspace,
  enableTool,
  programRunner,
  setToolPolicy,
} from '../../../support/providers.ts'
import { withOwner } from '../../../support/topology.ts'
import { highlighted } from './follow-latest.ts'

// A real Run waits on three approvals. Live mode reviews the oldest one, and newer logs
// that Coral streams meanwhile leave it selected. Each successful decision advances the
// review to the next approval, then to the newest record, with live mode as it was. The
// choice rules stay in Reef's `activity.server.test.ts`, `navigation.test.ts` and
// `live.stories.tsx`; this proves them against real Coral, its approvals and its stream.

const program = [
  'const first = coral.providers.actionFixture.charge({})',
  'const second = coral.providers.actionFixture.charge({})',
  'const third = coral.providers.actionFixture.charge({})',
  // A call starts once something awaits it, so the charges start before the gate.
  'const results = Promise.allSettled([first, second, third])',
  'await coral.providers.actionFixture.gate({})',
  "console.info('newer log 1')",
  "console.info('newer log 2')",
  'coral.emit((await results).map((result) => result.status))',
  'for (let i = 0; i < 250; i++) console.info(`long history ${i}`)',
  'await coral.providers.actionFixture.gate({})',
  "return 'ok'",
].join('\n')

/** Answers every held fixture call in `responses`. */
function release(responses: ServerResponse[]) {
  for (const response of responses.splice(0))
    response.writeHead(200, { 'content-type': 'application/json' }).end('{"done":true}')
}

export async function liveActions(context: TestContext) {
  // Every call waits here until the journey releases it, so the Run's newest record
  // changes only when the journey says.
  const held = { gate: [] as ServerResponse[], charge: [] as ServerResponse[] }
  const upstream = await listen(
    context,
    createServer(
      (request, response) =>
        void held[request.url?.startsWith('/gate') ? 'gate' : 'charge'].push(response),
    ),
  )
  context.onTestFinished(() => release([...held.gate, ...held.charge]))
  await withOwner(context, async ({ page, target, step, accessToken }) => {
    const api = managementClients(accessToken)
    const workspace = await createWorkspace(accessToken, `Live actions ${crypto.randomUUID()}`)
    const provider = await addProviderFromFile(
      page,
      workspace,
      openApiDocument(
        'Action fixture',
        {
          charge: { summary: 'Charge one invoice' },
          gate: { summary: 'Wait for the next batch' },
        },
        { server: upstream.origin },
      ),
      'actionFixture',
    )
    await page.goto(provider)
    await setToolPolicy(page, 'charge', 'Needs approval')
    await enableTool(page, 'gate')
    // A User's own calls do not need approval. Run as a real registered Agent so
    // the Owner reviews its calls through the normal authorization path.
    const issued = await api.agents.createAgent({
      parent: `workspaces/${workspace}`,
      displayName: 'Live approval worker',
      requestId: crypto.randomUUID(),
    })
    const execute = programRunner(page, target, workspace, {
      waitMs: 1,
      accessToken: issued.credential,
      taskIntent: `Charge invoices ${crypto.randomUUID()}`,
    })
    const accepted = await execute.exec(program)
    expect(accepted.isError).not.toBe(true)
    const run = `workspaces/${workspace}/runs/${accepted.structuredContent.run_id}`
    const task = `workspaces/${workspace}/tasks/${execute.taskId()}`
    const entries = async (name = run) =>
      (await api.activity.browseRunEntries({ name, pageSize: 100 })).entries
    const logs = async () =>
      (await entries()).filter(
        ({ record }) => record.case === 'event' && record.value.kind === 'program.logged',
      ).length
    /** The Run's newest record, as Activity names it. */
    const newest = async (name = run) => {
      const last = (await entries(name)).at(-1)?.record
      return last?.case === 'invocation'
        ? last.value.name
        : last?.case === 'event'
          ? `${name}/events/${last.value.sequence}`
          : undefined
    }

    const table = page.getByRole('table', { name: 'Activity records', includeHidden: true })
    const rail = page.getByRole('dialog')
    const selected = rail.getByRole('region', { name: 'Selected record' })
    const toggle = rail.getByRole('button', { name: 'Follow latest record', exact: true })
    const address = () => new URL(page.url()).searchParams
    const selectedKey = () =>
      table.locator('[role=row][aria-selected=true]').first().getAttribute('data-record-key')
    /** Live mode is on in the address and on the toggle in the record's own header. */
    const following = async (on: boolean) => {
      await expect.poll(() => address().get('live')).toBe(on ? '1' : null)
      await selected
        .locator('header')
        .getByRole('button', { name: 'Follow latest record', exact: true })
        .waitFor()
      expect(await toggle.getAttribute('aria-pressed')).toBe(String(on))
    }
    // Each waiting call, oldest first, with the approval it waits on and its stored line.
    const waits: { invocation: string; approval: string; line: number }[] = []
    /** The drawer reviews wait `index`, with the source line Coral stored for its call. */
    const reviews = async (index: number) => {
      const { invocation, approval, line } = waits[index]!
      await expect.poll(() => address().get('record'), { timeout: 15_000 }).toBe(invocation)
      expect(address().get('request')).toBe(approval)
      await expect.poll(selectedKey).toBe(invocation)
      await rail
        .getByRole('region', { name: 'Review coral.providers.actionFixture.charge', exact: true })
        .waitFor()
      await expect.poll(() => highlighted(page)).toEqual({ lines: [line], unavailable: false })
    }

    await step('Open the Run while it waits on three approvals and a held call', async () => {
      await expect.poll(() => held.gate.length, { timeout: 15_000 }).toBe(1)
      // Coral opens each approval as it admits the call; wait until all three wait.
      await expect
        .poll(
          async () =>
            (await entries()).filter(
              ({ record }) => record.case === 'invocation' && record.value.requiresAction,
            ).length,
          { timeout: 15_000 },
        )
        .toBe(3)
      expect(held.charge.length).toBe(0)
      // The calls start together, so neither their source lines nor their positions give
      // their order. Coral's start times, then identities, do.
      const calls = (await entries())
        .flatMap(({ record }) =>
          record.case === 'invocation' && record.value.requiresAction ? [record.value] : [],
        )
        .toSorted(
          (a, b) =>
            Number(a.startedAtUnixNanos > b.startedAtUnixNanos) -
              Number(a.startedAtUnixNanos < b.startedAtUnixNanos) ||
            Number(a.name > b.name) - Number(a.name < b.name),
        )
      for (const call of calls) {
        const { requests } = await api.activity.inspectInvocation({ name: call.name })
        const approval = requests.find(({ request }) => request.case === 'approval')?.request
        waits.push({
          invocation: call.name,
          approval: approval?.case === 'approval' ? approval.value : '',
          line: call.sourceLocation?.line ?? 0,
        })
      }
      expect(waits.map(({ approval }) => Boolean(approval))).toEqual([true, true, true])
      // Each call has its own stored line among the program's first three.
      expect(waits.map(({ line }) => line).toSorted()).toEqual([1, 2, 3])
      expect(await logs()).toBe(0)
      const query = new URLSearchParams({ tree: '1', record: run })
      query.append('expand', task)
      query.append('expand', run)
      await page.goto(`/${issued.agent!.name}?${query}`)
      await following(false)
    })

    await step('Turn on live mode: it reviews the oldest approval', async () => {
      await toggle.click()
      await reviews(0)
      await following(true)
    })

    await step('Newer logs arrive: the oldest approval stays selected', async () => {
      const before = page.url()
      release(held.gate)
      await expect.poll(logs, { timeout: 15_000 }).toBe(2)
      // The table shows the newest log, so the stream delivered it to this page.
      const log = await newest()
      expect(log).toMatch(/\/events\/\d+$/)
      // The background table is virtualized beneath the pending-action cards.
      // Scroll its viewport without selecting a record; this does not turn Live off.
      await table.evaluate((port) => {
        port.scrollTop = port.scrollHeight
      })
      await table.locator(`[data-record-key="${log}"]`).waitFor({ state: 'attached' })
      await reviews(0)
      await following(true)
      expect(page.url()).toBe(before)
    })

    await step('Approve it: the review advances to the next approval', async () => {
      await rail.getByRole('button', { name: 'Approve this call', exact: true }).click()
      await reviews(1)
      await following(true)
      expect((await api.activity.inspectApproval({ name: waits[0]!.approval })).state).toBe(
        'approved',
      )
    })

    await step('Turn live mode off and deny the next approval: the review advances', async () => {
      await toggle.click()
      await following(false)
      await rail.getByRole('button', { name: 'Deny this call', exact: true }).click()
      await reviews(2)
      await following(false)
      expect((await api.activity.inspectApproval({ name: waits[1]!.approval })).state).toBe(
        'declined',
      )
    })

    await step('Approve the last one: the review selects the newest record', async () => {
      await rail.getByRole('button', { name: 'Approve this call', exact: true }).click()
      // The advance waits for the decision and Activity's reload after it, so wait for
      // its navigation rather than a fixed second.
      await page.waitForURL((url) => url.searchParams.get('record') !== waits[2]!.invocation)
      // The approved calls wait on the held fixture, so the newest record stays put.
      expect(await newest()).toBe(address().get('record'))
      expect(address().has('request')).toBe(false)
      await expect.poll(selectedKey).toBe(address().get('record'))
      await following(false)
      expect((await api.activity.inspectApproval({ name: waits[2]!.approval })).state).toBe(
        'approved',
      )
    })

    // The page's own live toggle, above the list.
    const latest = page.getByRole('button', { name: 'Live', exact: true })
    /** Reads deep into the long Run's records, away from the newest top-level records. */
    const readDeep = async () => {
      await table.evaluate((port) => {
        port.scrollTop = port.scrollHeight
      })
      // Away from the top, Coral's snapshots hold only the records near the viewport.
      await expect.poll(() => table.evaluate((port) => port.scrollTop)).toBeGreaterThan(1)
    }
    /** Starts a Task whose Run holds on the gate. Returns the Run and its held call. */
    const startHeldTask = async (intent: string) => {
      const waiting = held.gate.length
      const next = programRunner(page, target, workspace, {
        waitMs: 1,
        accessToken: issued.credential,
        taskIntent: `${intent} ${crypto.randomUUID()}`,
      })
      const started = await next.exec("await coral.providers.actionFixture.gate({})\nreturn 'ok'")
      expect(started.isError).not.toBe(true)
      await expect.poll(() => held.gate.length, { timeout: 15_000 }).toBe(waiting + 1)
      const name = `workspaces/${workspace}/runs/${started.structuredContent.run_id}`
      // Its only record is the call that waits on the gate.
      await expect.poll(() => newest(name), { timeout: 15_000 }).toMatch(/\/invocations\//)
      return { run: name, call: (await newest(name))!, gate: held.gate.splice(waiting) }
    }
    /** Page live mode selects `record` in the table, without a click or a scroll. */
    const selects = async (record: string) => {
      await expect.poll(() => address().get('record'), { timeout: 15_000 }).toBe(record)
      await expect.poll(selectedKey).toBe(record)
      expect(address().get('live')).toBe('latest')
      expect(await latest.getAttribute('aria-pressed')).toBe('true')
    }

    await step('Follow a long Run back to its own row when it ends', async () => {
      await latest.click()
      release(held.charge)
      // The second gate holds the Run after enough logs to pass the first child window.
      await expect.poll(() => held.gate.length, { timeout: 15_000 }).toBe(1)
      await expect
        .poll(() => Number(address().get(`offset.${run}`)), { timeout: 15_000 })
        .toBeGreaterThan(100)
      await expect
        .poll(() => address().get('record'), { timeout: 15_000 })
        .toMatch(/\/invocations\//)
      await expect.poll(selectedKey).toBe(address().get('record'))
      await readDeep()
      release(held.gate)
      await selects(run)
      const row = table.locator(`[data-record-key="${run}"]`)
      // Selection must bring the row back into the viewport, not just change the rail.
      await expect
        .poll(() =>
          row.evaluate((element) => {
            const box = element.getBoundingClientRect()
            const port = element.closest('[role=table]')!.getBoundingClientRect()
            return box.top >= port.top && box.bottom <= port.bottom
          }),
        )
        .toBe(true)
    })

    // Live mode finds new work across the list whatever part of it the table shows.
    let next: Awaited<ReturnType<typeof startHeldTask>>
    await step('Follow a new Task that arrives while you read deep in the ended Run', async () => {
      await readDeep()
      next = await startHeldTask('Next live task')
      await selects(next.call)
    })

    await step('A Task that arrives while the followed Run runs waits until it ends', async () => {
      await readDeep()
      const later = await startHeldTask('Later live task')
      // The followed Run still holds live mode. routes/activity-live.test.tsx proves it
      // holds through the snapshot of the arrival; here the arrival moves nothing at once.
      expect(address().get('record')).toBe(next.call)
      expect(await table.evaluate((port) => port.scrollTop)).toBeGreaterThan(1)
      // The followed Run ends: live mode moves to the later Task's Run, which runs.
      release(next.gate)
      await selects(later.call)
      // That Run ends too: live mode rests on the Run itself.
      release(later.gate)
      await selects(later.run)
    })
  })
}
