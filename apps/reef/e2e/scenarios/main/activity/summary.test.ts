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

test('The Overview summary counts not allowed, blocked and answered calls live, and keeps its period in the URL', async (context) => {
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
    const runner = programRunner(page, target, workspace, {
      accessToken: await mcpToken(page, workspace),
      taskIntent: 'Summary goal',
    })
    const openOverview = async () => {
      await page.goto(overview)
      // Hydration first: the stream and the period picker start after it.
      await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
      await summary
        .getByRole('heading', { name: 'Your activity in this workspace', exact: true })
        .waitFor()
    }

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
      await summary
        .getByRole('region', { name: 'Stopped operations', exact: true })
        .getByText('1 not allowed', { exact: true })
        .waitFor()
      const approvals = summary.getByRole('region', { name: 'Approvals', exact: true })
      // Approved, then declined; the median wait depends on how fast the review ran.
      expect((await approvals.getByRole('definition').allInnerTexts()).slice(0, 2)).toEqual([
        '0',
        '1',
      ])
    })

    await step('Blocked and answered calls arrive without a reload', async () => {
      await setPolicy(OperationAccessState.DISABLED)
      const blocked = await observeRun(runner, await runner.exec(CALL))
      expect(blocked.update.final_value).toEqual({ kind: 'value', value: 'operation_disabled' })
      await summary.getByText('1 blocked · 1 not allowed', { exact: true }).waitFor({
        timeout: 10_000,
      })
      await summary
        .getByRole('region', { name: 'Stopped operations', exact: true })
        .getByText('coral.providers.summaryFixture.read', { exact: true })
        .waitFor()
      await setPolicy(OperationAccessState.ENABLED)
      const sent = await observeRun(runner, await runner.exec(CALL))
      expect(sent.update.final_value).toEqual({ kind: 'value', value: 'sent' })
      await summary.getByText('33% answered', { exact: true }).waitFor({ timeout: 10_000 })
      await summary
        .getByRole('region', { name: 'Most called operations', exact: true })
        .getByText('3 calls', { exact: true })
        .waitFor()
      expect(new URL(page.url()).pathname).toBe(overview)
    })

    await step('The chosen period stays in the URL across a reload', async () => {
      const period = summary.getByRole('radiogroup', { name: 'Period', exact: true })
      const chosen = (name: string) =>
        period.getByRole('radio', { name, exact: true, checked: true }).waitFor()
      await period.getByRole('radio', { name: '24 hours', exact: true }).click()
      await page.waitForURL((url) => url.searchParams.get('days') === '1')
      await chosen('24 hours')
      await page.reload()
      await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
      await chosen('24 hours')
      await summary.getByText('33% answered', { exact: true }).waitFor()
      await period.getByRole('radio', { name: '7 days', exact: true }).click()
      await page.waitForURL((url) => !url.searchParams.has('days'))
      await chosen('7 days')
    })
  })
})
