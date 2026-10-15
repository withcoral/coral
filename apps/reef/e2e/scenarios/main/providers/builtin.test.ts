import type { Page } from 'playwright'
import { expect, test } from 'vitest'
import { createActors, mcpToken } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import { openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  breadcrumb,
  observeRun,
  programRunner,
  publishWorkspaceProvider,
  setProviderEnabled,
} from '../../../support/providers.ts'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'

/** The controls that change a provider an owner added. Coral manages its own. */
function changeControls(page: Page) {
  return [
    page.getByRole('button', { name: 'Edit provider name', exact: true }),
    page.getByText('Edit configuration', { exact: true }),
    page.getByText('Check for updates', { exact: true }),
    page.getByText('Replace source', { exact: true }),
    page.getByText('Remove provider', { exact: true }),
  ]
}

function rail(page: Page) {
  return page.getByRole('navigation', { name: 'installed providers', exact: true })
}

/** The logo a provider shows in the rail. A provider without one shows a lettermark. */
function mark(page: Page, provider: string) {
  return rail(page).locator(`a[href="/${provider}"] img`).filter({ visible: true })
}

test('Coral leads the providers page for owners, offers nothing to change, stops calls while disabled, and stays hidden from members', async (context) => {
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
    (provider) => provider.callRoot === 'coral.admin',
  )!
  const providersPage = `/${workspace.name}/providers`
  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      await step('List Coral first, with its mark', async () => {
        await page.goto(providersPage)
        const gears = rail(page).getByRole('link', { name: / settings$/ })
        await gears.first().waitFor()
        expect(
          await gears.evaluateAll((links) => links.map((link) => link.getAttribute('aria-label'))),
        ).toEqual(['Coral settings', 'Alpha settings'])
        expect(await mark(page, coral.name).count()).toBe(1)
        expect(await mark(page, alpha).count()).toBe(0)
      })
      await step("Coral's settings offer none of the changes an added provider has", async () => {
        await page.goto(`/${alpha}/settings`)
        await breadcrumb(page, 'Settings').waitFor()
        for (const control of changeControls(page)) expect(await control.count()).toBeGreaterThan(0)
        await page.goto(providersPage)
        await rail(page).getByRole('link', { name: 'Coral settings', exact: true }).click()
        await breadcrumb(page, 'Settings').waitFor()
        for (const control of changeControls(page)) expect(await control.count()).toBe(0)
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
              'try { await coral.admin.listProviders({}); return "listed"; } catch (error) { return error.code; }',
            ),
          )
        ).update.final_value
      await step('A Program cannot call Coral while it is disabled', async () => {
        expect(await listProviders()).toEqual({ kind: 'value', value: 'listed' })
        await setProviderEnabled(alice.accessToken, `/${coral.name}`, false)
        await page.reload()
        await page.getByText('Disabled', { exact: true }).waitFor()
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
      await step('A member does not see Coral in the rail', async () => {
        await page.goto(providersPage)
        await rail(page).getByRole('link', { name: 'Alpha settings', exact: true }).waitFor()
        expect(await rail(page).locator(`a[href="/${coral.name}"]`).count()).toBe(0)
      })
      await step("A member finds nothing at Coral's links", async () => {
        for (const path of [`/${coral.name}`, `/${coral.name}/settings`]) {
          await page.goto(path)
          await page.getByText('This page does not exist.', { exact: true }).waitFor()
        }
      })
    },
    { actor: bob.label, storageState: bob.storageState },
  )
})
