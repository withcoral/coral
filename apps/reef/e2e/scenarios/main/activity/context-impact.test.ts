import { createServer } from 'node:http'
import { expect, test } from 'vitest'
import { authenticatedTarget, loginActor, mcpToken } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import { listen, openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  createWorkspace,
  enableOperation,
  observeRun,
  programRunner,
} from '../../../support/providers.ts'

test('Activity and the Overview summary reconcile discovery costs and saved operation results without loading payloads into the table', async (context) => {
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
      await enableOperation(page, 'read')
      const intent = `Save context ${crypto.randomUUID()}`
      const execute = programRunner(page, target, workspace, {
        accessToken: await mcpToken(page, workspace),
        taskIntent: intent,
      })
      const description = await execute.describe('coral.providers.contextFixture.read')
      expect(description.isError).not.toBe(true)
      const observed = await observeRun(
        execute,
        await execute.exec(
          "await coral.providers.contextFixture.read(); console.log('é'); coral.emit('é'); return 'ok';",
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
      if (invocation.case !== 'invocation') throw new Error('Expected operation evidence')
      const exact = await api.activity.inspectInvocation({ name: invocation.value.name })
      if (exact.outcome.case !== 'upstreamOutcomeJson') throw new Error('Expected native result')
      const rawBytes = BigInt(Buffer.byteLength(exact.outcome.value))
      expect(entries.run!.contextImpactBytes).toBe(10n - rawBytes)
      const goal = await api.activity.inspectTask({ name: task })
      expect(goal.task!.contextImpactBytes).toBe(
        BigInt(Buffer.byteLength(JSON.stringify(description.structuredContent))) + 10n - rawBytes,
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
          expect(await table.locator('[aria-label$="B saved"]').count()).toBe(3)
          expect(await table.locator('[aria-label$="B added"]').count()).toBe(4)
          expect(await page.content()).not.toContain('x'.repeat(100))
          await table.getByRole('link', { name: 'final value', exact: true }).click()
          await page.locator('pre').filter({ hasText: '"ok"' }).first().waitFor()
          await page.getByRole('button', { name: 'Close details', exact: true }).click()
          await page.waitForURL((url) => !url.searchParams.has('record'))
        },
      )
      await step('The Overview counts the same context as the Task', async () => {
        // Only an exact byte count tells the describe response apart from the Run's saving.
        const saved = (-goal.task!.contextImpactBytes!).toLocaleString('en-US')
        await page.goto(`/workspaces/${workspace}`)
        await page
          .getByRole('region', { name: 'Activity summary', exact: true })
          .getByRole('region', { name: 'Context by caller', exact: true })
          .getByRole('listitem')
          .filter({ hasText: 'You' })
          .locator(`[title^="${saved} bytes saved."]`)
          .waitFor()
      })
    },
    { actor: actor.label, storageState: actor.storageState },
  )
})
