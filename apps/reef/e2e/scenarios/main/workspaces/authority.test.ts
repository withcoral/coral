import { Code, ConnectError } from '@connectrpc/connect'
import { expect, test } from 'vitest'
import { createActors, loginActor, newUser } from '../../../support/actors.ts'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import { breadcrumb, dismissOnboarding } from '../../../support/providers.ts'
import { copyNewAgentKey } from '../../../support/agent-key.ts'
import { organizationWorkspaceRow, pickPerson } from '../../../support/members.ts'

test('Organization Owner governance, eligible Memberships, and private server paths', async (context) => {
  const { owner, alice, bob } = await createActors(context)
  const name = `Alice private boundary ${crypto.randomUUID()}`
  let path = ''
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('Alice creates her Workspace as an Organization Member', async () => {
        await page.goto('/')
        await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
        await page
          .getByRole('navigation', { name: 'Coral', exact: true })
          .getByRole('button', { name: 'Create workspace', exact: true })
          .click()
        await page.getByLabel('Workspace name', { exact: true }).fill(name)
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Create workspace', exact: true })
          .click()
        await dismissOnboarding(page)
        await breadcrumb(page, name).waitFor()
        path = new URL(page.url()).pathname
      })
    },
    { actor: alice.label, storageState: alice.storageState },
  )
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('Bob cannot discover or open an unrelated Workspace', async () => {
        await page.goto('/')
        const nav = page.getByRole('navigation', { name: 'Coral', exact: true })
        await nav.getByText('Workspaces', { exact: true }).waitFor()
        expect(await nav.getByRole('button', { name, exact: true }).count()).toBe(0)
        const hidden = await page.request.get(path)
        const absent = await page.request.get('/workspaces/00000000-0000-0000-0000-000000000000')
        expect(hidden.status()).toBe(404)
        expect(absent.status()).toBe(404)
        expect(await hidden.text()).not.toContain(name)
      })
    },
    { actor: bob.label, storageState: bob.storageState },
  )
  const parent = decodeURIComponent(path.slice(1))
  const api = managementClients(owner.accessToken)
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      const nav = page.getByRole('navigation', { name: 'Coral', exact: true })
      await step('Organization Owner has no access without a Membership', async () => {
        await page.goto('/')
        await nav.getByText('Workspaces', { exact: true }).waitFor()
        expect(await nav.getByRole('button', { name, exact: true }).count()).toBe(0)
        const hidden = await page.request.get(path)
        expect(hidden.status()).toBe(404)
        expect(await hidden.text()).not.toContain(name)
        await expect(api.workspaces.getWorkspace({ name: parent })).rejects.toSatisfy(
          (error: unknown) => error instanceof ConnectError && error.code === Code.NotFound,
        )
      })
      await step('Organization Owner assigns themselves as an Owner', async () => {
        await page.getByRole('button', { name: /^Open account menu for / }).click()
        await page.getByRole('menuitem', { name: 'Workspaces', exact: true }).click()
        await breadcrumb(page, 'Workspaces').waitFor()
        const row = await organizationWorkspaceRow(page, name)
        await row.getByText('Active', { exact: true }).waitFor()
        await row.getByTitle(alice.email, { exact: true }).waitFor()
        await row.getByRole('button', { name: `Actions for ${name}`, exact: true }).click()
        await page.getByRole('menuitem', { name: 'Assign owners', exact: true }).click()
        const dialog = page.getByRole('dialog', { name: 'Assign owners', exact: true })
        await pickPerson(page, owner.email)
        await dialog.getByRole('button', { name: 'Assign owner', exact: true }).click()
        await expect.poll(() => dialog.count()).toBe(0)
        await row.getByText(`${owner.label} (You)`, { exact: true }).waitFor()
        // The new Membership puts the Workspace in the Owner's sidebar.
        await nav.getByRole('button', { name, exact: true }).waitFor()
        const members = await api.memberships.listWorkspaceMemberships({ parent })
        expect(
          members.memberships.map((m) => [m.user, m.role]).toSorted(([a], [b]) => (a < b ? -1 : 1)),
        ).toEqual(
          [
            [`users/${alice.userId}`, WorkspaceRole.OWNER],
            [`users/${owner.userId}`, WorkspaceRole.OWNER],
          ].toSorted(([a], [b]) => (a < b ? -1 : 1)),
        )
        await page.goto(`${path}/members`)
        await page.getByRole('table', { name: 'Members', exact: true }).waitFor()
      })
      await step('Owner adds Bob in one submission', async () => {
        await page.getByRole('button', { name: 'Add members', exact: true }).click()
        await pickPerson(page, bob.email)
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Add member', exact: true })
          .click()
        await page.getByRole('button', { name: `Actions for ${bob.label}`, exact: true }).waitFor()
        expect(await page.getByRole('dialog').count()).toBe(0)
        expect(page.url()).not.toContain(bob.email)
      })
      await step('Owner creates a Workspace-owned Agent', async () => {
        await page.getByRole('link', { name: 'Agents', exact: true }).click()
        // The Overview has its own Create agent button; act only once Agents has loaded.
        await page.getByRole('heading', { name: 'Agents', exact: true, level: 1 }).waitFor()
        await page.getByRole('button', { name: 'Create agent', exact: true }).click()
        await page.getByLabel('Agent name', { exact: true }).fill('Private Agent directory')
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Create agent', exact: true })
          .click()
        await copyNewAgentKey(page)
      })
    },
    { actor: owner.label, storageState: owner.storageState },
  )
  const charlie = await loginActor(context, await newUser(context, 'Concealed ordinary Member'))
  await api.memberships.createWorkspaceMembership({
    parent,
    email: charlie.email,
    role: WorkspaceRole.MEMBER,
    requestId: crypto.randomUUID(),
  })
  const agents = await api.agents.listAgents({ parent })
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      const openLeave = async () => {
        await page.getByRole('button', { name: `Actions for ${bob.label}`, exact: true }).click()
        await page.getByRole('menuitem', { name: 'Leave workspace', exact: true }).click()
      }
      await step(
        'Member sees themselves and safe Owners, not other Members or Agents',
        async () => {
          await page.goto(`${path}/members`)
          await breadcrumb(page, 'Members').waitFor()
          // The Owner directory lists the assigned Organization Owner like any other Owner.
          await page.getByText(owner.label, { exact: true }).waitFor()
          expect(await page.getByRole('button', { name: 'Add members', exact: true }).count()).toBe(
            0,
          )
          const nav = page.getByRole('navigation', { name: 'Coral', exact: true })
          expect(
            await nav.getByRole('link', { name: `${name} settings`, exact: true }).count(),
          ).toBe(0)
          await nav
            .getByRole('button', { name: `Copy ${name} workspace MCP URL`, exact: true })
            .waitFor()
          const html = await page.content()
          for (const hidden of [
            'Private Agent directory',
            charlie.email,
            charlie.label,
            charlie.userId,
          ])
            expect(html).not.toContain(hidden)
          // A compact member row's hover title holds the name and the email.
          await page.getByTitle(alice.email).waitFor()
          const denied = await page.request.get(`/${agents.agents[0].name}`, { maxRedirects: 0 })
          const absent = await page.request.get(
            `${path}/agents/00000000-0000-4000-8000-000000000000`,
            { maxRedirects: 0 },
          )
          // Agent history requires Owner authority before looking up either Agent.
          expect(denied.status()).toBe(403)
          expect(absent.status()).toBe(denied.status())
          for (const response of [denied, absent])
            expect(await response.text()).not.toContain('Private Agent directory')
        },
      )
      await step('Forged mutation is denied with genuine session and CSRF', async () => {
        await openLeave()
        const csrf = await page.getByRole('dialog').locator('input[name=csrf]').inputValue()
        await page.getByRole('button', { name: 'Cancel', exact: true }).click()
        const workspace = await api.workspaces.getWorkspace({ name: parent })
        const response = await page.request.post(path, {
          headers: { Origin: new URL(page.url()).origin },
          form: { csrf, intent: 'rename', etag: workspace.etag, displayName: 'Forbidden rename' },
        })
        expect(response.status()).toBe(403)
        expect((await api.workspaces.getWorkspace({ name: parent })).displayName).toBe(name)
      })
      await step('Member leaves without deleting User history', async () => {
        await openLeave()
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Leave workspace', exact: true })
          .click()
        // Home opens another of Bob's Workspaces, or stays home when he has none.
        await page.waitForURL(
          (url) =>
            url.pathname === '/' ||
            (/^\/workspaces\/[^/]+\/activity$/.test(url.pathname) &&
              !url.pathname.startsWith(`${path}/`)),
        )
        expect((await page.request.get(path)).status()).toBe(404)
      })
    },
    { actor: bob.label, storageState: bob.storageState },
  )
})
