import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { managementClients } from '../../../../support/management.ts'
import { openApiDocument } from '../../../../support/provider-fixtures.ts'
import {
  observeRun,
  programRunner,
  publishWorkspaceProvider,
  setOperationPolicy,
} from '../../../../support/providers.ts'

test("An agent's Program finds the operations an owner disabled in Reef", async (context) => {
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const api = managementClients(actor.accessToken)
    const workspace = await api.workspaces.createWorkspace({
      displayName: 'Program policy',
      requestId: crypto.randomUUID(),
    })
    // lookup starts enabled and send needs approval; archive keeps its default.
    const { provider } = await publishWorkspaceProvider(
      actor.accessToken,
      workspace.name,
      openApiDocument('Alpha', {
        lookup: {},
        send: { method: 'post' },
        archive: { method: 'post' },
      }),
      'alpha',
    )
    const issued = await api.agents.createAgent({
      parent: workspace.name,
      displayName: 'Policy reader',
      requestId: crypto.randomUUID(),
    })
    const agent = programRunner(page, target, workspace.name.split('/')[1], {
      accessToken: issued.credential,
      taskIntent: 'Find disabled operations',
    })
    await step('The owner disables lookup and always allows send', async () => {
      await page.goto(`/${provider}`)
      await setOperationPolicy(page, 'lookup', 'Disabled')
      await setOperationPolicy(page, 'send', 'Always allow')
    })
    await step('The agent cannot discover lookup, and getPolicy names it', async () => {
      expect((await agent.describe('coral.providers.alpha.lookup')).isError).toBe(true)
      const { update } = await observeRun(
        agent,
        await agent.exec('return (await coral.workspace.getPolicy({ namespace: "alpha" })).data;'),
      )
      expect(update.final_value).toEqual({
        kind: 'value',
        value: { namespace: 'alpha', rules: { lookup: 'disabled', send: 'enabled' } },
      })
    })
  })
})
