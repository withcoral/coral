import { expect, test } from 'vitest'
import { createActors } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { joinAsOwner, managementClients } from '../../../support/management.ts'
import { pickPerson } from '../../../support/members.ts'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'

test('Open Add members dialogs respect archive, in-place restoration and live self-demotion', async (context) => {
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
        // The banner restores the Workspace in place, without leaving Members.
        const banner = page.getByText('This workspace is archived', { exact: true })
        await banner.waitFor()
        await page.getByRole('button', { name: 'Restore workspace', exact: true }).click()
        await expect.poll(() => banner.count()).toBe(0)
        expect(new URL(page.url()).pathname).toBe(`${path}/members`)
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
          const archive = page.getByRole('button', { name: 'Archive Live authority', exact: true })
          expect(await archive.count()).toBe(0)
          expect(
            (
              await api.memberships.listWorkspaceMemberships({ parent: workspace.name })
            ).memberships.some((m) => m.user === `users/${bob.userId}`),
          ).toBe(false)
          await page.reload()
          // The header still offers its endpoint, beside where the archive icon was.
          await page
            .getByRole('button', { name: 'Copy Live authority workspace MCP URL', exact: true })
            .waitFor()
          expect(await archive.count()).toBe(0)
        },
      )
    },
    { actor: alice.label, storageState: alice.storageState },
  )
})
