import { expect, test } from 'vitest'
import { createActors, loginActor, mcpToken } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import {
  assertUserAccountRemainsRetired,
  prepareOffboardingAccount,
} from '../../../support/offboarding.ts'
import { breadcrumb, programRunner, type RunUpdate } from '../../../support/providers.ts'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'

test('Organization owner removes current work across workspaces through Members while preserving private history', async (context) => {
  const { owner, bob } = await createActors(context)
  const api = managementClients(owner.accessToken)
  const workspaces: { name: string }[] = []
  for (const displayName of ['Offboarding one', 'Offboarding two']) {
    const workspace = await api.workspaces.createWorkspace({
      displayName,
      requestId: crypto.randomUUID(),
    })
    await api.memberships.createWorkspaceMembership({
      parent: workspace.name,
      email: bob.email,
      role: WorkspaceRole.MEMBER,
      requestId: crypto.randomUUID(),
    })
    workspaces.push(workspace)
  }
  const work = new Map<
    string,
    { provider: string; pending: string; task: string; run: RunUpdate }
  >()
  await withBrowserSession(
    context,
    async ({ page, target }) => {
      for (const workspace of workspaces) {
        const account = await prepareOffboardingAccount(
          owner.accessToken,
          bob.accessToken,
          workspace.name,
        )
        const runner = programRunner(page, target, workspace.name.split('/')[1], {
          accessToken: await mcpToken(page, workspace.name.split('/')[1]),
        })
        expect(
          (await runner.saveFunction('answer', 'Private answer', 'export default () => 42;'))
            .isError,
        ).not.toBe(true)
        const run = (await runner.exec('return coral.functions.answer();')).structuredContent
        expect(run.state).toBe('succeeded')
        expect(run.final_value).toEqual({ kind: 'value', value: 42 })
        const task = `${workspace.name}/tasks/${runner.taskId()}`
        await expect(api.tasks.getTask({ name: task })).rejects.toThrow()
        work.set(workspace.name, { ...account, task, run })
      }
    },
    { actor: bob.label, storageState: bob.storageState, purpose: 'setup' },
  )
  const before = await api.organizationMemberships.listOrganizationMemberships({})
  const member = before.memberships.find((m) => m.profile?.user === `users/${bob.userId}`)!
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await page.goto('/')
      await page.getByRole('button', { name: /^Open account menu for / }).click()
      await page.getByRole('menuitem', { name: 'Members', exact: true }).click()
      await step('Remove the selected organization member', async () => {
        await breadcrumb(page, 'Members').waitFor()
        const row = page
          .getByRole('row')
          // The member's hover title holds the name and the email.
          .filter({ has: page.getByTitle(bob.email) })
        for (let pages = 0; pages < 20 && (await row.count()) === 0; pages++) {
          const previous = page.url()
          await page.getByRole('link', { name: 'Next members', exact: true }).click()
          await page.waitForURL((url) => url.href !== previous)
        }
        await row.getByRole('button', { name: `Actions for ${bob.label}`, exact: true }).click()
        await page.getByRole('menuitem', { name: 'Remove from organization', exact: true }).click()
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Remove from organization', exact: true })
          .click()
        await expect.poll(() => row.count()).toBe(0)
        expect(new URL(page.url()).pathname).toBe('/organization/members')
      })
    },
    { actor: owner.label, storageState: owner.storageState },
  )
  expect(
    (await api.organizationMemberships.listOrganizationMemberships({})).memberships.some(
      (m) => m.name === member.name,
    ),
  ).toBe(false)
  for (const workspace of workspaces) {
    expect(
      (await api.memberships.listWorkspaceMemberships({ parent: workspace.name })).memberships.some(
        (m) => m.user === `users/${bob.userId}`,
      ),
    ).toBe(false)
  }
  await expect(managementClients(bob.accessToken).workspaces.listWorkspaces({})).rejects.toThrow()
  // A real new login rejoins the Organization. It must not recover old current work.
  const returned = await loginActor(context, bob)
  const own = managementClients(returned.accessToken)
  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      await step('Rejoin without restoring credentials or current functions', async () => {
        for (const workspace of workspaces) {
          await api.memberships.createWorkspaceMembership({
            parent: workspace.name,
            email: returned.email,
            role: WorkspaceRole.MEMBER,
            requestId: crypto.randomUUID(),
          })
          const previous = work.get(workspace.name)!
          await assertUserAccountRemainsRetired(own, workspace.name, previous)
          expect((await own.tasks.getTask({ name: previous.task })).name).toBe(previous.task)
          await expect(api.tasks.getTask({ name: previous.task })).rejects.toThrow()
          const runner = programRunner(page, target, workspace.name.split('/')[1], {
            accessToken: await mcpToken(page, workspace.name.split('/')[1]),
          })
          expect((await runner.describe('coral.functions.answer')).isError).toBe(true)
          const retained = (await runner.wait({ ...previous.run, next_cursor: '' }))
            .structuredContent
          expect(retained.run_id).toBe(previous.run.run_id)
          expect(retained.state).toBe('succeeded')
          expect(retained.final_value).toEqual({ kind: 'value', value: 42 })
          expect(runner.calls.exec ?? 0).toBe(0)
        }
      })
    },
    { actor: returned.label, storageState: returned.storageState },
  )
})
