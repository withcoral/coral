import { Code, ConnectError } from '@connectrpc/connect'
import { expect, test } from 'vitest'
import { WorkspaceRole } from '../../generated/coral/management/v1/workspace_pb.ts'
import { withBrowserSession } from '../../support/browser-session.ts'
import { managementClients } from '../../support/management.ts'
import { createWorkspace, programRunner } from '../../support/providers.ts'

test('The implicit local Owner works through a Membership and does not bypass lifecycle rules', async (context) => {
  await withBrowserSession(context, async ({ page, target, step }) => {
    const api = managementClients()
    const displayName = `Local authority ${crypto.randomUUID()}`
    const id = await createWorkspace(undefined, displayName)
    const name = `workspaces/${id}`
    await step('Keep working through the designated Owner Membership', async () => {
      const workspace = await api.workspaces.getWorkspace({ name })
      expect(workspace.currentUser).toBeTruthy()
      expect(workspace.effectiveRole).toBe(WorkspaceRole.OWNER)
      const owned = async () =>
        (await api.memberships.listWorkspaceMemberships({ parent: name })).memberships.map((m) => [
          m.user,
          m.role,
          m.enabled,
        ])
      const { memberships } = await api.memberships.listWorkspaceMemberships({ parent: name })
      expect(await owned()).toEqual([[workspace.currentUser, WorkspaceRole.OWNER, true]])
      // The local User is the last Owner, so the Membership they work through stays.
      await expect(
        api.memberships.deleteWorkspaceMembership({
          name: memberships[0].name,
          etag: memberships[0].etag,
        }),
      ).rejects.toSatisfy(
        (error: unknown) =>
          error instanceof ConnectError &&
          error.code === Code.FailedPrecondition &&
          error.rawMessage === 'LAST_WORKSPACE_OWNER',
      )
      expect(await owned()).toEqual([[workspace.currentUser, WorkspaceRole.OWNER, true]])
      await page.goto(`/${name}`)
      await page.getByRole('heading', { name: 'Providers', exact: true, level: 2 }).waitFor()
      expect((await api.workspaces.getWorkspace({ name })).currentUser).toBe(workspace.currentUser)
      const result = await programRunner(page, target, id).exec('return "local owner";')
      expect(result.structuredContent.state).toBe('succeeded')
      expect(result.structuredContent.final_value).toEqual({ kind: 'value', value: 'local owner' })
    })
    await step('Apply the normal archive fence to the local Owner', async () => {
      await page
        .getByRole('navigation', { name: 'Coral', exact: true })
        .getByRole('button', { name: `Archive ${displayName}`, exact: true })
        .click()
      await page
        .getByRole('dialog', { name: `Archive ${displayName}`, exact: true })
        .getByRole('button', { name: 'Archive workspace', exact: true })
        .click()
      await page.waitForURL((url) => url.pathname === `/${name}/activity`)
      await page.getByText('This workspace is archived', { exact: true }).waitFor()
      expect(
        await page.getByRole('button', { name: 'Edit workspace name', exact: true }).count(),
      ).toBe(0)
      const workspace = await api.workspaces.getWorkspace({ name })
      await expect(
        api.workspaces.updateWorkspace({
          workspace: { ...workspace, displayName: 'Bypass forbidden' },
          updateMask: { paths: ['display_name'] },
        }),
      ).rejects.toSatisfy(
        (error: unknown) => error instanceof ConnectError && error.code === Code.FailedPrecondition,
      )
      expect((await api.workspaces.getWorkspace({ name })).displayName).toBe(workspace.displayName)
    })
  })
})
