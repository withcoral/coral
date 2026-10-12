import { expect, test } from 'vitest'
import { createActors, loginActor, newUser } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import { pickPerson } from '../../../support/members.ts'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'

test('An Organization Owner adds several people at once and retries only the refused ones', async (context) => {
  const { owner, alice, bob } = await createActors(context)
  const carol = await loginActor(context, await newUser(context, 'Carol'))
  const dana = await loginActor(context, await newUser(context, 'Dana'))
  const api = managementClients(owner.accessToken)
  const workspace = await api.workspaces.createWorkspace({
    displayName: 'Several members',
    requestId: crypto.randomUUID(),
  })
  await api.memberships.createWorkspaceMembership({
    parent: workspace.name,
    email: bob.email,
    role: WorkspaceRole.OWNER,
    requestId: crypto.randomUUID(),
  })
  const membership = async (userId: string) =>
    (await api.memberships.listWorkspaceMemberships({ parent: workspace.name })).memberships.find(
      (m) => m.user === `users/${userId}`,
    )
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      const dialog = page.getByRole('dialog')
      await step('Offer organization members who are not in the workspace yet', async () => {
        await page.goto(`/${workspace.name}/members`)
        await page.getByRole('button', { name: 'Add members', exact: true }).click()
        const people = dialog.getByRole('combobox', { name: 'People', exact: true })
        await people.fill(alice.email)
        await page
          .getByRole('option', { name: `${alice.label} (${alice.email})`, exact: true })
          .waitFor()
        // Bob is already a member, so the list offers his email only as typed text.
        await people.fill(bob.email)
        await page.getByRole('option', { name: `Add ${bob.email}`, exact: true }).waitFor()
        await people.fill('')
      })
      await step('A mixed outcome adds Alice and keeps only Bob for a retry', async () => {
        await pickPerson(page, alice.email)
        await pickPerson(page, bob.email)
        await dialog.getByRole('button', { name: 'Add 2 members', exact: true }).click()
        await dialog
          .getByText(new RegExp(`^Coral added ${alice.label}\\. ${bob.email}: `))
          .waitFor()
        await dialog.getByRole('button', { name: `Remove ${bob.email}`, exact: true }).waitFor()
        // The dialog keeps only the refused people once the page reloads.
        await expect
          .poll(() =>
            dialog.getByRole('button', { name: `Remove ${alice.label}`, exact: true }).count(),
          )
          .toBe(0)
        expect((await membership(alice.userId))?.role).toBe(WorkspaceRole.MEMBER)
        expect((await membership(bob.userId))?.role).toBe(WorkspaceRole.OWNER)
        // The person Coral added appears behind the dialog without a reload. The
        // open dialog hides the table from the accessibility tree, so match its text.
        await page.locator('[role=table]').getByText(alice.label, { exact: true }).waitFor()
      })
      await step('Replace Bob and add two owners in one submission', async () => {
        await dialog.getByRole('button', { name: `Remove ${bob.email}`, exact: true }).click()
        await pickPerson(page, carol.email)
        await pickPerson(page, dana.email)
        await dialog.getByRole('button', { name: 'Role', exact: true }).click()
        await page.getByRole('menuitemradio', { name: 'Owner', exact: true }).click()
        await dialog.getByRole('button', { name: 'Add 2 owners', exact: true }).click()
        await expect.poll(() => dialog.count()).toBe(0)
        for (const person of [carol, dana]) {
          expect((await membership(person.userId))?.role).toBe(WorkspaceRole.OWNER)
          await page
            .getByRole('button', { name: `Actions for ${person.label}`, exact: true })
            .waitFor()
        }
      })
    },
    { actor: owner.label, storageState: owner.storageState },
  )
})
