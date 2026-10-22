import { createServer } from 'node:http'
import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../support/actors.ts'
import { inspectRunEvents, managementClients } from '../../../support/management.ts'
import { listen, openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  createWorkspace,
  enableOperation,
  observeRun,
  programRunner,
} from '../../../support/providers.ts'
import { updateBytes } from '../../../support/update-bytes.ts'

const loop = (count: number) =>
  `for(let i=0;i<${count};i++) await coral.providers.lean.read({}); return ${count};`

// history.test.ts owns revealing the exact terminal Event.
test('Lean MCP updates preserve complete authorized Reef history', async (context) => {
  let calls = 0
  const upstream = await listen(
    context,
    createServer((_request, response) => {
      calls++
      response
        .writeHead(200, { 'content-type': 'application/json' })
        .end('{"retained":"native-lean-result"}')
    }),
  )
  await withOwnerSession(context, async ({ page, target, step, actor }) => {
    const api = managementClients(actor.accessToken)
    const workspace = await createWorkspace(
      actor.accessToken,
      `Lean updates ${crypto.randomUUID()}`,
    )
    const provider = await addProviderFromFile(
      page,
      workspace,
      openApiDocument(
        'Lean fixture',
        { read: { summary: 'Read a tiny result' } },
        { server: upstream.origin },
      ),
      'lean',
    )
    await page.goto(provider)
    await enableOperation(page, 'read')
    const execute = programRunner(page, target, workspace, { waitMs: 30000 })
    const single = await observeRun(execute, await execute.exec(loop(1)))
    const source = loop(128)
    const observed = await observeRun(execute, await execute.exec(source))
    // Invocations are not Events, so 127 more calls leave the update the same
    // size: only the final value gains digits. Run identities can differ in
    // length, so neither size counts its own.
    expect(observed.events).toEqual([])
    expect(observed.responses).toHaveLength(1)
    expect(updateBytes(observed) - updateBytes(single)).toBeLessThan(16)
    expect(observed.update.required_actions).toEqual([])
    expect(observed.update.final_value).toEqual({ kind: 'value', value: 128 })
    expect(calls).toBe(1 + 128)
    const run = `workspaces/${workspace}/runs/${observed.update.run_id}`
    const history = await inspectRunEvents(api, run)
    expect(history.filter((e) => e.payload.case === 'invocationStarted')).toHaveLength(128)
    const ends = history.flatMap((e) =>
      e.payload.case === 'invocationEnded' ? [e.payload.value] : [],
    )
    expect(ends).toHaveLength(128)
    const detail = await api.activity.inspectRun({ name: run, pageSize: 1 })
    expect(detail.submittedSource).toBe(source)
    await step(
      'Read the retained native result without adding it to the agent update',
      async () => {
        await page.goto(
          `/workspaces/${workspace}/activity?record=${encodeURIComponent(ends[127].invocation)}`,
        )
        await page.getByRole('dialog', { name: 'Activity details' }).waitFor()
        await page.locator('pre').filter({ hasText: 'native-lean-result' }).first().waitFor()
        expect(observed.responses.join('')).not.toContain('native-lean-result')
      },
    )
  })
})
