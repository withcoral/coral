import { expect, test } from 'vitest'
import { createActors } from '../../../support/actors.ts'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import { breadcrumb } from '../../../support/providers.ts'
import { organizationWorkspaceRow, pickPerson } from '../../../support/members.ts'

test('Organization Owner assigns several Owners to a Workspace in one submission', async (context) => {
  const { owner, alice, bob } = await createActors(context)
  const name = `Several owners ${crypto.randomUUID()}`
  const workspace = await managementClients(alice.accessToken).workspaces.createWorkspace({
    displayName: name,
    requestId: crypto.randomUUID(),
  })
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('Assign yourself and Bob from the Workspaces table', async () => {
        await page.goto('/')
        await page.getByRole('button', { name: /^Open account menu for / }).click()
        await page.getByRole('menuitem', { name: 'Workspaces', exact: true }).click()
        await breadcrumb(page, 'Workspaces').waitFor()
        const row = await organizationWorkspaceRow(page, name)
        await row.getByRole('button', { name: `Actions for ${name}`, exact: true }).click()
        await page.getByRole('menuitem', { name: 'Assign owners', exact: true }).click()
        const dialog = page.getByRole('dialog', { name: 'Assign owners', exact: true })
        await pickPerson(page, owner.email)
        await pickPerson(page, bob.email)
        await dialog.getByRole('button', { name: 'Assign 2 owners', exact: true }).click()
        // The dialog stays open on a refusal.
        await expect.poll(() => dialog.count()).toBe(0)
      })
    },
    { actor: owner.label, storageState: owner.storageState },
  )
  const members = await managementClients(owner.accessToken).memberships.listWorkspaceMemberships({
    parent: workspace.name,
  })
  expect(
    members.memberships.map((m) => [m.user, m.role]).toSorted(([a], [b]) => (a < b ? -1 : 1)),
  ).toEqual(
    [
      [`users/${alice.userId}`, WorkspaceRole.OWNER],
      [`users/${owner.userId}`, WorkspaceRole.OWNER],
      [`users/${bob.userId}`, WorkspaceRole.OWNER],
    ].toSorted(([a], [b]) => (a < b ? -1 : 1)),
  )
})
