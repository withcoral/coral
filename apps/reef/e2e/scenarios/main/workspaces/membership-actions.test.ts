import { expect, test } from 'vitest'
import { createActors, mcpToken } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'
import {
  assertUserAccountRemainsRetired,
  prepareOffboardingAccount,
} from '../../../support/offboarding.ts'
import { programRunner, type RunUpdate } from '../../../support/providers.ts'

test('Row actions keep the last Owner and removing another person keeps the administrator in place', async (context) => {
  const { owner, bob } = await createActors(context)
  const api = managementClients(owner.accessToken)
  const workspace = await api.workspaces.createWorkspace({
    displayName: 'Row actions',
    requestId: crypto.randomUUID(),
  })
  await api.memberships.createWorkspaceMembership({
    parent: workspace.name,
    email: bob.email,
    role: WorkspaceRole.MEMBER,
    requestId: crypto.randomUUID(),
  })
  const currentWork = await prepareOffboardingAccount(
    owner.accessToken,
    bob.accessToken,
    workspace.name,
  )
  let retainedRun: RunUpdate
  await withBrowserSession(
    context,
    async ({ page, target }) => {
      const id = workspace.name.split('/')[1]
      const runner = programRunner(page, target, id, { accessToken: await mcpToken(page, id) })
      expect(
        (await runner.saveFunction('answer', 'Private answer', 'export default () => 42;')).isError,
      ).not.toBe(true)
      retainedRun = (await runner.exec('return coral.functions.answer();')).structuredContent
      expect(retainedRun.final_value).toEqual({ kind: 'value', value: 42 })
    },
    { actor: bob.label, storageState: bob.storageState, purpose: 'setup' },
  )
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      const path = `/${workspace.name}/members`
      await page.goto(path)
      const lastOwner = page
        .getByRole('dialog')
        .getByText('This workspace must keep one owner. Assign another owner first.', {
          exact: true,
        })
      const ownMembership = () =>
        api.memberships.getWorkspaceMembership({
          name: `${workspace.name}/memberships/${owner.userId}`,
        })
      await step('The last Owner cannot change their role to Member', async () => {
        expect(await page.getByText(`${owner.label} (You)`, { exact: true }).count()).toBe(1)
        await page.getByRole('button', { name: `Actions for ${owner.label}`, exact: true }).click()
        await page.getByRole('menuitem', { name: 'Change role', exact: true }).click()
        await page.getByRole('button', { name: `Role for ${owner.label}`, exact: true }).click()
        await page.getByRole('menuitemradio', { name: 'Member', exact: true }).click()
        await page.getByRole('button', { name: 'Save role', exact: true }).click()
        await lastOwner.waitFor()
        await page.getByRole('button', { name: 'Cancel', exact: true }).click()
        await expect.poll(() => page.getByRole('dialog').count()).toBe(0)
        expect(
          await page.getByRole('button', { name: 'Archive Row actions', exact: true }).count(),
        ).toBe(1)
        expect((await ownMembership()).role).toBe(WorkspaceRole.OWNER)
      })
      await step('Remove another person without navigating away', async () => {
        await page.getByRole('button', { name: `Actions for ${bob.label}`, exact: true }).click()
        await page.getByRole('menuitem', { name: 'Remove from workspace', exact: true }).click()
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Remove from workspace', exact: true })
          .click()
        await expect
          .poll(() =>
            page.getByRole('button', { name: `Actions for ${bob.label}`, exact: true }).count(),
          )
          .toBe(0)
        expect(new URL(page.url()).pathname).toBe(path)
      })
      await step(
        'The last Owner cannot leave, although they are an Organization Owner',
        async () => {
          await page
            .getByRole('button', { name: `Actions for ${owner.label}`, exact: true })
            .click()
          await page.getByRole('menuitem', { name: 'Leave workspace', exact: true }).click()
          await page
            .getByRole('dialog')
            .getByRole('button', { name: 'Leave workspace', exact: true })
            .click()
          await lastOwner.waitFor()
          await page.getByRole('button', { name: 'Cancel', exact: true }).click()
          await expect.poll(() => page.getByRole('dialog').count()).toBe(0)
          expect(new URL(page.url()).pathname).toBe(path)
          expect(
            await page.getByRole('button', { name: 'Archive Row actions', exact: true }).count(),
          ).toBe(1)
          const { memberships } = await api.memberships.listWorkspaceMemberships({
            parent: workspace.name,
          })
          expect(memberships.map((m) => [m.user, m.role])).toEqual([
            [`users/${owner.userId}`, WorkspaceRole.OWNER],
          ])
          expect(await page.getByText(`${owner.label} (You)`, { exact: true }).count()).toBe(1)
        },
      )
    },
    { actor: owner.label, storageState: owner.storageState },
  )
  await api.memberships.createWorkspaceMembership({
    parent: workspace.name,
    email: bob.email,
    role: WorkspaceRole.MEMBER,
    requestId: crypto.randomUUID(),
  })
  const own = managementClients(bob.accessToken)
  await assertUserAccountRemainsRetired(own, workspace.name, currentWork)
  await withBrowserSession(
    context,
    async ({ page, target }) => {
      const id = workspace.name.split('/')[1]
      const runner = programRunner(page, target, id, { accessToken: await mcpToken(page, id) })
      expect((await runner.describe('coral.functions.answer')).isError).toBe(true)
      const retained = (await runner.wait({ ...retainedRun, next_cursor: '' })).structuredContent
      expect(retained.final_value).toEqual({ kind: 'value', value: 42 })
      expect(runner.calls.exec ?? 0).toBe(0)
    },
    { actor: bob.label, storageState: bob.storageState },
  )
})
