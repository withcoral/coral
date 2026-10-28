import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { managementClients } from '../../../../support/management.ts'
import { openApiDocument } from '../../../../support/provider-fixtures.ts'
import {
  observeRun,
  programRunner,
  publishWorkspaceProvider,
  setToolPolicy,
} from '../../../../support/providers.ts'

test("An agent's Program finds the tools an owner disabled in Reef", async (context) => {
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const api = managementClients(actor.accessToken)
    const workspace = await api.workspaces.createWorkspace({
      displayName: 'Program policy',
      requestId: crypto.randomUUID(),
    })
    const { provider } = await publishWorkspaceProvider(
      actor.accessToken,
      workspace.name,
      openApiDocument('Alpha', { lookup: {} }),
      'alpha',
    )
    const issued = await api.agents.createAgent({
      parent: workspace.name,
      displayName: 'Policy reader',
      requestId: crypto.randomUUID(),
    })
    const agent = programRunner(page, target, workspace.name.split('/')[1], {
      accessToken: issued.credential,
      taskIntent: 'Find disabled tools',
    })
    await step('The owner disables lookup', async () => {
      await page.goto(`/${provider}`)
      await setToolPolicy(page, 'lookup', 'Disabled')
    })
    await step('getPolicy names lookup as disabled', async () => {
      const { update } = await observeRun(
        agent,
        await agent.exec('return (await coral.workspace.getPolicy({ namespace: "alpha" })).data;'),
      )
      expect(update.final_value).toEqual({
        kind: 'value',
        value: { namespace: 'alpha', rules: { lookup: 'disabled' } },
      })
    })
  })
})
