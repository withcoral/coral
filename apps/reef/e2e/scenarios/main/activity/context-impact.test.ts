import { createServer } from 'node:http'
import { expect, test } from 'vitest'
import { authenticatedTarget, loginActor, mcpToken } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import { listen, openApiDocument } from '../../../support/provider-fixtures.ts'
import { shownPayload } from '../../../support/payloads.ts'
import {
  addProviderFromFile,
  createWorkspace,
  enableTool,
  observeRun,
  programRunner,
} from '../../../support/providers.ts'

test('Activity and the Overview summary reconcile discovery costs and saved tool results without loading payloads into the table', async (context) => {
  const actor = await loginActor(context, authenticatedTarget().authentication.owner)
  const api = managementClients(actor.accessToken)
  const upstream = await listen(
    context,
    createServer((_request, response) => {
      response
        .writeHead(200, { 'content-type': 'application/json' })
        .end(JSON.stringify({ payload: 'x'.repeat(1_970_000) }))
    }),
  )
  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      const workspace = await createWorkspace(
        actor.accessToken,
        `Context impact ${crypto.randomUUID()}`,
      )
      const provider = await addProviderFromFile(
        page,
        workspace,
        openApiDocument(
          'Context fixture',
          { read: { summary: 'Read a large response' } },
          { server: upstream.origin },
        ),
        'contextFixture',
      )
      await page.goto(provider)
      await enableTool(page, 'read')
      const intent = `Save context ${crypto.randomUUID()}`
      const execute = programRunner(page, target, workspace, {
        accessToken: await mcpToken(page, workspace),
        taskIntent: intent,
      })
      // The exec states the goal, so the Task holds only the calls measured below.
      const observed = await observeRun(
        execute,
        await execute.exec(
          "await coral.providers.contextFixture.read(); console.log('é'); coral.emit('é'); return 'ok';",
        ),
      )
      // Discovery runs in a Program, so its cost is that Run's.
      const discovery = await observeRun(
        execute,
        await execute.exec(
          "return (await coral.workspace.search({ paths: ['coral.providers.contextFixture.read'] })).data.descriptions[0].typescript_declaration;",
        ),
      )
      expect(
        (
          await execute.saveFunction(
            'context.reuse',
            'Reuse the result calculation',
            'export default () => "ok";',
          )
        ).isError,
      ).not.toBe(true)
      const run = `workspaces/${workspace}/runs/${observed.update.run_id}`
      const task = `workspaces/${workspace}/tasks/${execute.taskId()}`
      const entries = await api.activity.browseRunEntries({ name: run, pageSize: 100 })
      const invocation = entries.entries.find(({ record }) => record.case === 'invocation')!.record
      if (invocation.case !== 'invocation') throw new Error('Expected tool evidence')
      const exact = await api.activity.inspectInvocation({ name: invocation.value.name })
      if (exact.outcome.case !== 'upstreamOutcomeJson') throw new Error('Expected native result')
      const rawBytes = BigInt(Buffer.byteLength(exact.outcome.value))
      expect(entries.run!.contextImpactBytes).toBe(10n - rawBytes)
      const discovered = await api.activity.browseRunEntries({
        name: `workspaces/${workspace}/runs/${discovery.update.run_id}`,
        pageSize: 100,
      })
      // The search result is the Run's own tool result, and only the declaration reaches the agent.
      expect(discovered.run!.contextImpactBytes!).toBeLessThan(0n)
      const goal = await api.activity.inspectTask({ name: task })
      expect(goal.task!.contextImpactBytes).toBe(
        discovered.run!.contextImpactBytes! + 10n - rawBytes,
      )
      await step(
        'Keep the parent total when a saved function has no applicable context measurement',
        async () => {
          const query = new URLSearchParams()
          await page.goto(`/workspaces/${workspace}/activity`)
          const table = page.getByRole('table', { name: 'Activity records', exact: true })
          const root = table
            .getByRole('row')
            .filter({ has: page.getByRole('link', { name: intent, exact: true }) })
          const total = await root.getByRole('cell').last().innerText()
          expect(total).toBe('−1.97 MB')
          query.append('expand', task)
          query.append('expand', run)
          await page.goto(`/workspaces/${workspace}/activity?${query}`)
          expect(await root.getByRole('cell').last().innerText()).toBe(total)
          expect(await table.locator('[aria-label$="B saved"]').count()).toBe(4)
          // The log and the emission; the return value is the Run's own, on its panel.
          expect(await table.locator('[aria-label$="B added"]').count()).toBe(2)
          expect(await page.content()).not.toContain('x'.repeat(100))
          await table.locator(`[data-record-key="${run}"]`).locator('a[href]').click()
          await shownPayload(page, '"ok"').waitFor()
          await page
            .getByRole('region', { name: 'Agent context', exact: true })
            .locator('div', { has: page.getByText('Return value sent', { exact: true }) })
            .getByText('4 B', { exact: true })
            .waitFor()
          await page.getByRole('button', { name: 'Close details', exact: true }).click()
          await page.waitForURL((url) => !url.searchParams.has('record'))
        },
      )
      await step('The Overview counts the same context as the Task', async () => {
        // Only an exact byte count tells the discovery Run apart from the other Run's saving.
        const saved = (-goal.task!.contextImpactBytes!).toLocaleString('en-US')
        await page.goto(`/workspaces/${workspace}?lens=context`)
        await page
          .getByRole('region', { name: 'Activity summary', exact: true })
          .getByRole('table', { name: 'Context by caller', exact: true })
          .getByRole('row')
          .filter({ hasText: 'You' })
          .locator(`[title^="${saved} bytes saved."]`)
          .waitFor()
      })
    },
    { actor: actor.label, storageState: actor.storageState },
  )
})
