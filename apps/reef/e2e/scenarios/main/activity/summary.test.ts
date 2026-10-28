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
import { ToolAccessState } from '../../../generated/coral/management/v1/policy_pb.ts'

const CALL =
  "try { await coral.providers.summaryFixture.read(); return 'sent'; } catch (error) { return error.code; }"

test('The Overview summary counts a denied call as decided, not to fix, reads blocked and answered calls again when you return to it, opens a blocked tool to allow it, and keeps the period you choose in the URL', async (context) => {
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
    const tool = (await api.providers.listTools({ parent: provider })).tools[0].name
    const setPolicy = (accessState: ToolAccessState) =>
      api.policies.updateToolPolicies({
        parent: provider,
        policies: [{ tool, accessState }],
      })
    const summary = page.getByRole('region', { name: 'Activity summary', exact: true })
    const lens = (name: RegExp) => summary.getByRole('tab', { name })
    // The full Call Path is each tool cell's title; the cell shows the provider and the tool.
    const toolRow = (table: string) =>
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
      await summary.getByRole('heading', { name: 'Workspace activity', exact: true }).waitFor()
    }
    // You come back to the tab, as after running a program elsewhere.
    const returnToOverview = () => page.evaluate(() => window.dispatchEvent(new Event('focus')))

    let paused!: RunUpdate
    await step('A call waits for approval', async () => {
      await setPolicy(ToolAccessState.APPROVAL_REQUIRED)
      const started = await runner.exec(CALL)
      expect(started.isError).not.toBe(true)
      paused = started.structuredContent
      for (let pages = 0; pages < 10 && !paused.required_actions.length; pages++)
        paused = (await runner.wait(paused)).structuredContent
      expect(paused.required_actions).toEqual([
        expect.objectContaining({ kind: 'invocation_approval' }),
      ])
    })

    await step('Deny the call, and the summary counts it as decided, not to fix', async () => {
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
      await needsYou.getByText('No calls to fix', { exact: true }).waitFor()
      await needsYou.click()
      await summary
        .getByRole('region', { name: 'Approval decisions', exact: true })
        .getByRole('group', { name: 'Declined', exact: true })
        .getByText('1', { exact: true })
        .waitFor()
      await toolRow('Calls not allowed').getByText('1', { exact: true }).waitFor()
    })

    await step('A blocked call arrives when you return, without a reload', async () => {
      await setPolicy(ToolAccessState.DISABLED)
      const blocked = await observeRun(runner, await runner.exec(CALL))
      expect(blocked.update.final_value).toEqual({ kind: 'value', value: 'tool_disabled' })
      await returnToOverview()
      const needsYou = lens(/^Needs you/)
      await needsYou.getByText('1 to fix', { exact: true }).waitFor()
      await needsYou.getByText('1 blocked', { exact: true }).waitFor()
      await needsYou.click()
      await toolRow('To fix').getByText('Blocked by a rule', { exact: true }).waitFor()
    })

    await step('The blocked row opens its tool, and you allow it there', async () => {
      await toolRow('To fix').click()
      await page.waitForURL((url) => url.pathname === `/${provider}/tools/read`)
      const policy = page
        .getByRole('dialog', { name: 'read', exact: true })
        .getByRole('radiogroup', { name: 'Policy', exact: true })
      await policy.getByRole('radio', { name: 'Disabled', exact: true, checked: true }).waitFor()
      const allow = policy.getByRole('radio', { name: 'Always allow', exact: true })
      await allow.click()
      await policy
        .getByRole('radio', { name: 'Always allow', exact: true, checked: true })
        .waitFor()
      // The toggle stays disabled until the save answers.
      await expect.poll(() => allow.isEnabled()).toBe(true)
      expect(await page.getByText('Not saved', { exact: true }).count()).toBe(0)
      await page.goBack()
      await summary.waitFor()
    })

    await step('An answered call arrives when you return, without a reload', async () => {
      const sent = await observeRun(runner, await runner.exec(CALL))
      expect(sent.update.final_value).toEqual({ kind: 'value', value: 'sent' })
      await returnToOverview()
      await summary.getByText('33% answered', { exact: true }).waitFor()
      await lens(/^Calls/).click()
      await toolRow('Tools').getByText('3', { exact: true }).waitFor()
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
