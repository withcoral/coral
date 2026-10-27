import type { Page } from 'playwright'
import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { managementClients } from '../../../../support/management.ts'
import { openApiDocument } from '../../../../support/provider-fixtures.ts'
import {
  observeRun,
  programRunner,
  publishWorkspaceProvider,
  type RunUpdate,
  setToolPolicy,
} from '../../../../support/providers.ts'

/** Waits until the provider page shows `name` with `state` chosen. */
async function expectPolicy(
  page: Page,
  name: string,
  state: 'Always allow' | 'Needs approval' | 'Disabled',
) {
  const row = page.getByRole('row').filter({
    has: page.getByRole('checkbox', { name: `Select ${name}`, exact: true }),
  })
  await row.getByRole('radio', { name: state, exact: true, checked: true }).waitFor()
}

test("An owner's Program changes an added provider's policy once the owner approves it", async (context) => {
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const workspace = await managementClients(actor.accessToken).workspaces.createWorkspace({
      displayName: 'Program policy change',
      requestId: crypto.randomUUID(),
    })
    // A GET runs at once by default, and a POST waits for approval.
    const { provider } = await publishWorkspaceProvider(
      actor.accessToken,
      workspace.name,
      openApiDocument('Alpha', { lookup: {}, remove: { method: 'post' } }),
      'alpha',
    )
    const runner = programRunner(page, target, workspace.name.split('/')[1], {
      taskIntent: 'Change the alpha policy',
    })
    let paused!: RunUpdate
    await step('The Run waits for the owner to approve updatePolicy', async () => {
      const started = await runner.exec(
        'return (await coral.workspace.updatePolicy({ namespace: "alpha", tools: { lookup: "disabled", remove: "enabled" } })).data;',
      )
      expect(started.isError).not.toBe(true)
      paused = started.structuredContent
      for (let pages = 0; pages < 10 && !paused.required_actions.length; pages++)
        paused = (await runner.wait(paused)).structuredContent
      expect(paused.required_actions).toEqual([
        expect.objectContaining({ kind: 'invocation_approval', required_actor: 'invoking_user' }),
      ])
    })
    await step('Approve the call, and the Program gets each new state', async () => {
      await page.goto(paused.required_actions[0].run_status_url)
      await page
        .getByRole('link', { name: 'Review coral.workspace.updatePolicy', exact: true })
        .click()
      await page
        .getByRole('region', { name: 'Review coral.workspace.updatePolicy', exact: true })
        .getByRole('button', { name: 'Approve this call', exact: true })
        .click()
      await page.getByText('Approved', { exact: true }).waitFor()
      const { update } = await observeRun(runner, await runner.wait(paused))
      expect(update.final_value).toEqual({
        kind: 'value',
        value: { namespace: 'alpha', tools: { lookup: 'disabled', remove: 'enabled' } },
      })
    })
    await step('The provider page shows the policy the Program set', async () => {
      await page.goto(`/${provider}`)
      await expectPolicy(page, 'lookup', 'Disabled')
      await expectPolicy(page, 'remove', 'Always allow')
    })
  })
})

test("An owner's Program cannot change the policy of the built-in provider's tools", async (context) => {
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const api = managementClients(actor.accessToken)
    const workspace = await api.workspaces.createWorkspace({
      displayName: 'Program Coral policy',
      requestId: crypto.randomUUID(),
    })
    const coral = (await api.providers.listProviders({ parent: workspace.name })).providers.find(
      (provider) => provider.builtin,
    )!
    const runner = programRunner(page, target, workspace.name.split('/')[1], {
      taskIntent: "Change Coral's policy",
    })
    await step('The owner lets updatePolicy run without approval', async () => {
      await page.goto(`/${coral.name}`)
      await setToolPolicy(page, 'updatePolicy', 'Always allow')
      await expectPolicy(page, 'addProvider', 'Needs approval')
    })
    await step('No namespace reaches Coral, so the Program gets provider_not_found', async () => {
      // The Built-in Provider's tools are called under coral.workspace.
      const { update } = await observeRun(
        runner,
        await runner.exec(
          'try { return await coral.workspace.updatePolicy({ namespace: "workspace", tools: { updatePolicy: "disabled", addProvider: "enabled" } }); } catch (error) { return error.code; }',
        ),
      )
      expect(update.final_value).toEqual({ kind: 'value', value: 'provider_not_found' })
    })
    await step("Coral's policy stays as the owner set it", async () => {
      await page.reload()
      await expectPolicy(page, 'updatePolicy', 'Always allow')
      await expectPolicy(page, 'addProvider', 'Needs approval')
    })
  })
})
