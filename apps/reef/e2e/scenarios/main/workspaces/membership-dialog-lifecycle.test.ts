import { expect, test } from 'vitest'
import { createActors } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { joinAsOwner, managementClients } from '../../../support/management.ts'
import { pickPerson } from '../../../support/members.ts'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'

test('Open Add members dialogs respect archive, restoration and live self-demotion', async (context) => {
  const { owner, alice, bob } = await createActors(context)
  const api = managementClients(owner.accessToken)
  const workspace = await managementClients(alice.accessToken).workspaces.createWorkspace({
    displayName: 'Live authority',
    requestId: crypto.randomUUID(),
  })
  // The organization owner acts from another session, so they need a Membership first.
  await joinAsOwner(api, workspace.name, owner.userId)
  const path = `/${workspace.name}`
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await page.goto(`${path}/members`)
      await step('Archive from another session while Add members is open', async () => {
        await page.getByRole('button', { name: 'Add members', exact: true }).click()
        await pickPerson(page, bob.email)
        await api.workspaces.archiveWorkspace({ name: workspace.name, etag: workspace.etag })
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Add member', exact: true })
          .click()
        await page
          .getByRole('dialog')
          .getByText(/archived/i)
          .waitFor()
        // Alice cannot list the organization, so Bob stays as the email she typed.
        await page
          .getByRole('dialog')
          .getByRole('button', { name: `Remove ${bob.email}`, exact: true })
          .waitFor()
        expect(
          (
            await api.memberships.listWorkspaceMemberships({ parent: workspace.name })
          ).memberships.some((m) => m.user === `users/${bob.userId}`),
        ).toBe(false)
        await page.getByRole('button', { name: 'Cancel', exact: true }).click()
        await page.getByRole('link', { name: 'Live authority settings', exact: true }).click()
        const settings = page.getByRole('dialog', { name: 'Workspace settings', exact: true })
        await settings.getByRole('button', { name: 'Restore workspace', exact: true }).click()
        await settings.getByRole('button', { name: 'Save name', exact: true }).waitFor()
        await page.keyboard.press('Escape')
        await expect.poll(() => settings.count()).toBe(0)
        await page.getByRole('link', { name: 'Members', exact: true }).click()
      })
      await step(
        'Live demotion defeats the already-open Owner form and refreshes navigation',
        async () => {
          await page.getByRole('button', { name: 'Add members', exact: true }).click()
          await pickPerson(page, bob.email)
          const membership = await api.memberships.getWorkspaceMembership({
            name: `${workspace.name}/memberships/${alice.userId}`,
          })
          await api.memberships.updateWorkspaceMembership({
            membership: {
              name: membership.name,
              etag: membership.etag,
              role: WorkspaceRole.MEMBER,
            },
            updateMask: { paths: ['role'] },
          })
          await page
            .getByRole('dialog')
            .getByRole('button', { name: 'Add member', exact: true })
            .click()
          await expect
            .poll(() => page.getByRole('button', { name: 'Add members', exact: true }).count())
            .toBe(0)
          expect(
            await page.getByRole('link', { name: 'Live authority settings', exact: true }).count(),
          ).toBe(0)
          expect(
            (
              await api.memberships.listWorkspaceMemberships({ parent: workspace.name })
            ).memberships.some((m) => m.user === `users/${bob.userId}`),
          ).toBe(false)
          await page.reload()
          expect(
            await page.getByRole('link', { name: 'Live authority settings', exact: true }).count(),
          ).toBe(0)
          const denied = await page.goto(`${path}/settings`)
          expect(denied?.status()).toBe(403)
          // The error replaces the Overview rather than rendering beneath it.
          await page
            .getByRole('navigation', { name: 'breadcrumb', exact: true })
            .getByRole('heading', { name: 'Error', exact: true, level: 1 })
            .waitFor()
          expect(
            await page.getByRole('heading', { name: 'Live authority', exact: true }).count(),
          ).toBe(0)
          expect(
            await page.getByRole('link', { name: 'Live authority settings', exact: true }).count(),
          ).toBe(0)
        },
      )
    },
    { actor: alice.label, storageState: alice.storageState },
  )
})
