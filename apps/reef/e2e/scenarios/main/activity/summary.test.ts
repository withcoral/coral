import { createServer } from 'node:http'
import { expect, test } from 'vitest'
import { mcpToken, withOwnerSession } from '../../../support/actors.ts'
import { managementClients } from '../../../support/management.ts'
import { listen, openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  createWorkspace,
  observeRun,
  programRunner,
  type RunUpdate,
} from '../../../support/providers.ts'
import { OperationAccessState } from '../../../generated/coral/management/v1/policy_pb.ts'

const CALL =
  "try { await coral.providers.summaryFixture.read(); return 'sent'; } catch (error) { return error.code; }"

test('The Overview summary counts not allowed calls, reads blocked and answered calls again when you return to it, and keeps the period you choose in the URL', async (context) => {
  const upstream = await listen(
    context,
    createServer((_request, response) => {
      response.writeHead(200, { 'content-type': 'application/json' }).end('{"ok":true}')
    }),
  )
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const api = managementClients(actor.accessToken)
    const workspace = await createWorkspace(actor.accessToken, `Summary ${crypto.randomUUID()}`)
    const overview = `/workspaces/${workspace}`
    const provider = (
      await addProviderFromFile(
        page,
        workspace,
        openApiDocument(
          'Summary fixture',
          { read: { summary: 'Read a small response' } },
          { server: upstream.origin },
        ),
        'summaryFixture',
      )
    ).slice(1)
    const operation = (await api.providers.listOperations({ parent: provider })).operations[0].name
    const setPolicy = (accessState: OperationAccessState) =>
      api.policies.updateOperationPolicies({
        parent: provider,
        policies: [{ operation, accessState }],
      })
    const summary = page.getByRole('region', { name: 'Activity summary', exact: true })
    const lens = (name: RegExp) => summary.getByRole('tab', { name })
    // The full Call Path is each operation cell's title; the cell shows the provider and the operation.
    const operationRow = (table: string) =>
      summary
        .getByRole('table', { name: table, exact: true })
        .getByRole('row')
        .filter({ has: page.locator('[title="coral.providers.summaryFixture.read"]') })
    const runner = programRunner(page, target, workspace, {
      accessToken: await mcpToken(page, workspace),
      taskIntent: 'Summary goal',
    })
    const openOverview = async () => {
      await page.goto(overview)
      // Hydration first: the refresh and the period picker start after it.
      await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
      await summary
        .getByRole('heading', { name: 'Activity by you and your agents', exact: true })
        .waitFor()
    }
    // You come back to the tab, as after running a program elsewhere.
    const returnToOverview = () => page.evaluate(() => window.dispatchEvent(new Event('focus')))

    let paused!: RunUpdate
    await step('A call waits for approval', async () => {
      await setPolicy(OperationAccessState.APPROVAL_REQUIRED)
      const started = await runner.exec(CALL)
      expect(started.isError).not.toBe(true)
      paused = started.structuredContent
      for (let pages = 0; pages < 10 && !paused.required_actions.length; pages++)
        paused = (await runner.wait(paused)).structuredContent
      expect(paused.required_actions).toEqual([
        expect.objectContaining({ kind: 'invocation_approval' }),
      ])
    })

    await step('Deny the call, and the summary counts it as not allowed', async () => {
      await page.goto(paused.required_actions[0].run_status_url)
      await page
        .getByRole('link', { name: 'Review coral.providers.summaryFixture.read', exact: true })
        .click()
      await page.getByRole('button', { name: 'Deny this call', exact: true }).click()
      await page.getByText('Denied', { exact: true }).waitFor()
      const { update } = await observeRun(runner, await runner.wait(paused))
      expect(update.final_value).toEqual({ kind: 'value', value: 'approval_declined' })
      await openOverview()
      const needsYou = lens(/^Needs you/)
      await needsYou.getByText('1 not allowed', { exact: true }).waitFor()
      // The median wait depends on how fast the review ran.
      await needsYou.getByText(/^0 approved · 1 declined/).waitFor()
    })

    await step('Blocked and answered calls arrive when you return, without a reload', async () => {
      await setPolicy(OperationAccessState.DISABLED)
      const blocked = await observeRun(runner, await runner.exec(CALL))
      expect(blocked.update.final_value).toEqual({ kind: 'value', value: 'operation_disabled' })
      await returnToOverview()
      await summary.getByText('1 blocked · 1 not allowed', { exact: true }).waitFor()
      await lens(/^Needs you/).click()
      await operationRow('To fix').getByText('Blocked by a rule', { exact: true }).waitFor()
      await lens(/^Calls/).click()
      await setPolicy(OperationAccessState.ENABLED)
      const sent = await observeRun(runner, await runner.exec(CALL))
      expect(sent.update.final_value).toEqual({ kind: 'value', value: 'sent' })
      await returnToOverview()
      await summary.getByText('33% answered', { exact: true }).waitFor()
      await operationRow('Operations').getByText('3', { exact: true }).waitFor()
      expect(new URL(page.url()).pathname).toBe(overview)
    })

    // Every call falls inside both periods; the loader's tests own which period Coral reads.
    await step('Choosing 24 hours puts it in the URL and finishes loading', async () => {
      const period = summary.getByRole('radiogroup', { name: 'Period', exact: true })
      await period.getByRole('radio', { name: '24 hours', exact: true }).click()
      await page.waitForURL((url) => url.searchParams.get('days') === '1')
      await period.getByRole('radio', { name: '24 hours', exact: true, checked: true }).waitFor()
      await summary.getByRole('status').waitFor({ state: 'detached' })
      expect(await summary.getAttribute('aria-busy')).toBe('false')
    })
  })
})
