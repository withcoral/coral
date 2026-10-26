import type { Page } from 'playwright'
import { expect, test } from 'vitest'
import { createActors, mcpToken } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import { openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  observeRun,
  programRunner,
  publishWorkspaceProvider,
  setProviderEnabled,
  waitForProviderStatus,
} from '../../../support/providers.ts'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'

function rail(page: Page) {
  return page.getByRole('navigation', { name: 'installed providers', exact: true })
}

/** The logo a provider shows in the rail. A provider without one shows a lettermark. */
function mark(page: Page, provider: string) {
  return rail(page).locator(`a[href="/${provider}"] img`).filter({ visible: true })
}

test('Coral leads the providers page, offers nothing to change, stops calls while disabled, and shows members only the operations they can call', async (context) => {
  const { alice, bob } = await createActors(context)
  const api = managementClients(alice.accessToken)
  const workspace = await api.workspaces.createWorkspace({
    displayName: 'Built-in provider',
    requestId: crypto.randomUUID(),
  })
  await api.memberships.createWorkspaceMembership({
    parent: workspace.name,
    email: bob.email,
    role: WorkspaceRole.MEMBER,
    requestId: crypto.randomUUID(),
  })
  // Its name sorts before Coral's, and no recipe gives it a logo.
  const { provider: alpha } = await publishWorkspaceProvider(
    alice.accessToken,
    workspace.name,
    openApiDocument('Alpha', { hello: {} }),
    'alpha',
  )
  const coral = (await api.providers.listProviders({ parent: workspace.name })).providers.find(
    (provider) => provider.builtin,
  )!
  const providersPage = `/${workspace.name}/providers`
  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      await step('List Coral first, with its mark and no settings', async () => {
        await page.goto(providersPage)
        const gears = rail(page).getByRole('link', { name: / settings$/ })
        await gears.first().waitFor()
        // Coral has no settings: only the added provider has a gear.
        expect(
          await gears.evaluateAll((links) => links.map((link) => link.getAttribute('aria-label'))),
        ).toEqual(['Alpha settings'])
        expect(
          await rail(page)
            .locator(`a[href="/${coral.name}"], a[href="/${alpha}"]`)
            .evaluateAll((links) => links.map((link) => link.getAttribute('href'))),
        ).toEqual([`/${coral.name}`, `/${alpha}`])
        expect(await mark(page, coral.name).count()).toBe(1)
        expect(await mark(page, alpha).count()).toBe(0)
      })
      const id = workspace.name.split('/')[1]
      const runner = programRunner(page, target, id, {
        accessToken: await mcpToken(page, id),
        taskIntent: 'List the providers',
      })
      const listProviders = async () =>
        (
          await observeRun(
            runner,
            await runner.exec(
              'try { await coral.workspace.listProviders({}); return "listed"; } catch (error) { return error.code; }',
            ),
          )
        ).update.final_value
      await step('A Program cannot call Coral while it is disabled', async () => {
        expect(await listProviders()).toEqual({ kind: 'value', value: 'listed' })
        await setProviderEnabled(alice.accessToken, `/${coral.name}`, false)
        await page.reload()
        await waitForProviderStatus(page, 'Coral', 'Provider disabled')
        expect(await listProviders()).toEqual({ kind: 'value', value: 'operation_not_found' })
      })
      await step('A Program calls Coral again once it is restored', async () => {
        await setProviderEnabled(alice.accessToken, `/${coral.name}`, true)
        expect(await listProviders()).toEqual({ kind: 'value', value: 'listed' })
      })
    },
    { actor: alice.label, storageState: alice.storageState },
  )
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('A member sees Coral with listProviders only', async () => {
        await page.goto(providersPage)
        await waitForProviderStatus(page, 'Coral', 'Ready', { available: 1, total: 1 })
        await page.getByRole('link', { name: 'listProviders', exact: true }).waitFor()
        expect(await page.getByRole('link', { name: 'addProvider', exact: true }).count()).toBe(0)
      })
    },
    { actor: bob.label, storageState: bob.storageState },
  )
})
