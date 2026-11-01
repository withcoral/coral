import { createServer } from 'node:http'
import { expect, test } from 'vitest'
import { createActors, mcpToken, withOwnerSession } from '../../../support/actors.ts'
import { joinAsOwner, managementClients } from '../../../support/management.ts'
import { listen, openApiDocument } from '../../../support/provider-fixtures.ts'
import { addProviderFromFile, createWorkspace, programRunner } from '../../../support/providers.ts'
import { ToolAccessState } from '../../../generated/coral/management/v1/policy_pb.ts'

const CALL = 'return await coral.providers.approvalsFixture.read();'
const CALL_PATH = 'coral.providers.approvalsFixture.read'

// The real change signal, through Coral's watch and Reef's stream, reaches an open page. Lower
// tests cover the revision and the refresh on either side of that connection.
test('An open Approvals page shows a new request and drops a decided one without a reload', async (context) => {
  let calls = 0
  const upstream = await listen(
    context,
    createServer((_request, response) => {
      calls++
      response.writeHead(200, { 'content-type': 'application/json' }).end('{"ok":true}')
    }),
  )
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const api = managementClients(actor.accessToken)
    const workspace = await createWorkspace(actor.accessToken, `Approvals ${crypto.randomUUID()}`)
    const provider = (
      await addProviderFromFile(
        page,
        workspace,
        openApiDocument(
          'Approvals fixture',
          { read: { summary: 'Read a small response' } },
          { server: upstream.origin },
        ),
        'approvalsFixture',
      )
    ).slice(1)
    const tool = (await api.providers.listTools({ parent: provider })).tools[0].name
    await api.policies.updateToolPolicies({
      parent: provider,
      policies: [{ tool, accessState: ToolAccessState.APPROVAL_REQUIRED }],
    })
    const runner = programRunner(page, target, workspace, {
      accessToken: await mcpToken(page, workspace),
      waitMs: 1,
      taskIntent: 'Approvals goal',
    })
    const rows = page
      .getByRole('table', { name: 'Pending approvals', exact: true })
      .getByRole('row')
      .filter({ hasText: CALL_PATH })
    /** A reload clears this mark, so the page proves that it stayed mounted. */
    const stillLoaded = () =>
      page.evaluate(() => (window as { approvalsMark?: boolean }).approvalsMark)

    await step('Open the page with nothing to approve', async () => {
      await page.goto(`/workspaces/${workspace}/approvals`)
      await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
      await page.getByText('Nothing to approve.', { exact: true }).waitFor()
      await page.evaluate(() => {
        ;(window as { approvalsMark?: boolean }).approvalsMark = true
      })
    })

    await step('An Agent request appears, and another owner decides it', async () => {
      const name = `workspaces/${workspace}`
      const { alice } = await createActors(context)
      await joinAsOwner(api, name, alice.userId)
      const issued = await api.agents.createAgent({
        parent: name,
        displayName: 'Approvals worker',
        requestId: crypto.randomUUID(),
      })
      const agent = programRunner(page, target, workspace, {
        accessToken: issued.credential,
        waitMs: 1,
        taskIntent: 'Agent goal',
      })
      await agent.exec(CALL)
      await rows.first().waitFor()
      const otherOwner = managementClients(alice.accessToken)
      const { actions } = await otherOwner.activity.browseRequiredActions({ workspace: name })
      const request = actions[0].action?.request?.request
      if (request?.case !== 'approval') throw new Error('Expected an approval')
      await otherOwner.activity.decideApproval({ name: request.value, decision: 'approve' })
      await page.getByText('Nothing to approve.', { exact: true }).waitFor()
      expect(await stillLoaded()).toBe(true)
    })

    await step('A decision on the row removes it, and the run continues', async () => {
      const before = calls
      await runner.exec(CALL)
      await rows.first().waitFor()
      await rows
        .first()
        .getByRole('button', { name: /^Approve: / })
        .click()
      await page.getByText('Nothing to approve.', { exact: true }).waitFor()
      await expect.poll(() => calls).toBe(before + 1)
      expect(await stillLoaded()).toBe(true)
    })
  })
})
