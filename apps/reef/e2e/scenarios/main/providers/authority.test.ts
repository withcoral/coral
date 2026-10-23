import { expect, test } from 'vitest'
import { Code } from '@connectrpc/connect'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'
import { createActors } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { joinAsOwner, managementClients } from '../../../support/management.ts'
import { openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  addedProviders,
  acceptOperations,
  chooseFile,
  confirmConfiguration,
  publishWorkspaceProvider,
  setNamespace,
  waitForPublication,
} from '../../../support/providers.ts'

test('A frozen publication request grants no authority after its owner is demoted', async (context) => {
  const { owner, alice } = await createActors(context)
  const api = managementClients(owner.accessToken)
  const workspace = await managementClients(alice.accessToken).workspaces.createWorkspace({
    displayName: 'Publication authority loss',
    requestId: crypto.randomUUID(),
  })
  await joinAsOwner(api, workspace.name, owner.userId)
  // Rejected publication must leave this workspace's exact catalog unchanged.
  const initialProviders = (await api.providers.listProviders({ parent: workspace.name })).providers
    .map((provider) => provider.name)
    .toSorted()
  const changeRole = async (role: WorkspaceRole) => {
    const membership = await api.memberships.getWorkspaceMembership({
      name: `${workspace.name}/memberships/${alice.userId}`,
    })
    await api.memberships.updateWorkspaceMembership({
      membership: { name: membership.name, etag: membership.etag, role },
      updateMask: { paths: ['role'] },
    })
  }
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      let frozenUrl = ''
      await step('Reach publication while Alice owns the active workspace', async () => {
        await page.goto(`/${workspace.name}/providers/new`)
        await chooseFile(page, 'authority.json', openApiDocument('Authority API', { hello: {} }))
        await page.getByRole('button', { name: 'Read', exact: true }).click()
        await confirmConfiguration(page)
        await acceptOperations(page)
        await setNamespace(page, 'authority')
        frozenUrl = new URL(
          (await page.locator('form:has(input[value="publish"])').getAttribute('action'))!,
          page.url(),
        ).toString()
      })
      await step(
        'Demotion refuses publication and replay without publishing resources',
        async () => {
          await changeRole(WorkspaceRole.MEMBER)
          const response = page.waitForResponse(
            (value) => value.request().method() === 'POST' && value.url().includes('/new.data'),
          )
          await page.getByRole('button', { name: 'Add provider', exact: true }).click()
          expect((await response).status()).toBe(403)
          expect((await page.goto(frozenUrl))?.status()).toBe(403)
          expect(
            (await api.providers.listProviders({ parent: workspace.name })).providers
              .map((provider) => provider.name)
              .toSorted(),
          ).toEqual(initialProviders)
        },
      )
      await step('Restored authority can retry the same unconsumed attempt', async () => {
        await changeRole(WorkspaceRole.OWNER)
        await page.goto(frozenUrl)
        expect(await page.locator('input[name="requestId"]').last().inputValue()).toBe(
          new URL(frozenUrl).searchParams.get('requestId'),
        )
        await page.getByRole('button', { name: 'Retry', exact: true }).click()
        await waitForPublication(page)
        expect(
          (await api.providers.listProviders({ parent: workspace.name })).providers,
        ).toHaveLength(2)
      })
    },
    { actor: alice.label, storageState: alice.storageState },
  )
})

test('Members get 403 on Owner-only pages; malformed identifiers get 404', async (context) => {
  const { owner, alice } = await createActors(context)
  const api = managementClients(owner.accessToken)
  const workspace = await api.workspaces.createWorkspace({
    displayName: 'Provider authority',
    requestId: crypto.randomUUID(),
  })
  await api.memberships.createWorkspaceMembership({
    parent: workspace.name,
    email: alice.email,
    role: WorkspaceRole.MEMBER,
    requestId: crypto.randomUUID(),
  })
  const publication = await publishWorkspaceProvider(
    owner.accessToken,
    workspace.name,
    openApiDocument('Team API', { listTeams: { summary: 'List teams' } }),
    'team',
  )
  const hiddenWorkspace = await api.workspaces.createWorkspace({
    displayName: 'Private provider authority',
    requestId: crypto.randomUUID(),
  })
  const hidden = await publishWorkspaceProvider(
    owner.accessToken,
    hiddenWorkspace.name,
    openApiDocument('Private provider canary', { privateOperation: {} }),
    'private_provider',
  )
  const ws = workspace.name.split('/').at(-1)
  const providerId = publication.provider.split('/').at(-1)
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('A Member reads the provider but cannot reach Owner-only pages', async () => {
        await page.goto(`/workspaces/${ws}/providers/${providerId}/settings`)
        await page.getByRole('heading', { level: 2, name: 'Team API', exact: true }).waitFor()
        await page.getByText('No account is required.', { exact: true }).waitFor()
        expect(await page.getByRole('link', { name: 'Add provider', exact: true }).count()).toBe(0)
        expect(
          await page.getByRole('button', { name: 'Edit provider name', exact: true }).count(),
        ).toBe(0)
        // The source is an owner's concern; a Member sees neither it nor its actions.
        expect(await page.getByRole('complementary', { name: 'Source', exact: true }).count()).toBe(
          0,
        )
        expect(
          await page.getByRole('link', { name: 'Check for updates', exact: true }).count(),
        ).toBe(0)
        expect(
          await page.getByRole('button', { name: 'Edit configuration', exact: true }).count(),
        ).toBe(0)
        expect(
          await page.getByRole('button', { name: 'Change account setting', exact: true }).count(),
        ).toBe(0)
        expect(
          await page.getByRole('button', { name: 'Remove provider', exact: true }).count(),
        ).toBe(0)
        for (const path of [
          `/workspaces/${ws}/providers/new`,
          `/workspaces/${ws}/providers/${providerId}/update`,
        ])
          expect((await page.request.get(path)).status(), path).toBe(403)
        // The operations view offers a Member no provider actions.
        await page.goto(`/workspaces/${ws}/providers/${providerId}`)
        await page.getByRole('rowgroup', { name: 'Team API', exact: true }).waitFor()
        expect(await page.getByRole('button', { name: /^Disable|^Enable/ }).count()).toBe(0)
      })
      await step('A forged action is refused without a change', async () => {
        const response = await page.request.post(`/workspaces/${ws}/providers`, {
          form: {
            intent: 'removeProvider',
            provider: providerId!,
            etag: 'stale',
            csrf: 'forged',
          },
          headers: { Origin: new URL(page.url()).origin },
        })
        expect([403, 400]).toContain(response.status())
        await page.goto(`/workspaces/${ws}/providers/${providerId}/settings`)
        await page.getByRole('heading', { level: 2, name: 'Team API', exact: true }).waitFor()
      })
      await step('A valid form still gives a Member no mutation authority', async () => {
        await page.goto(`/workspaces/${ws}/providers/${providerId}`)
        const csrf = await page.locator('input[name="csrf"]').first().inputValue()
        const before = await api.providers.getProvider({ name: publication.provider })
        const response = await page.request.post(`/workspaces/${ws}/providers`, {
          headers: { Origin: new URL(page.url()).origin },
          form: { intent: 'removeProvider', provider: providerId!, etag: before.etag, csrf },
        })
        expect(response.status()).toBe(403)
        expect((await api.providers.getProvider({ name: publication.provider })).etag).toBe(
          before.etag,
        )
      })
      await step('Providers stay concealed outside their exact Workspace', async () => {
        const member = managementClients(alice.accessToken)
        const hiddenId = hidden.provider.split('/').at(-1)
        for (const name of [
          hidden.provider,
          `${workspace.name}/providers/${hiddenId}`,
          `${hiddenWorkspace.name}/providers/${providerId}`,
        ]) {
          const response = await page.request.get(`/${name}`)
          expect(response.status()).toBe(404)
          expect(await response.text()).not.toContain('Private provider canary')
          await expect(member.providers.getProvider({ name })).rejects.toMatchObject({
            code: Code.NotFound,
          })
          await expect(member.providers.listOperations({ parent: name })).rejects.toMatchObject({
            code: Code.NotFound,
          })
        }
        expect(
          addedProviders(
            (await member.providers.listProviders({ parent: workspace.name })).providers,
          ).map((provider) => provider.name),
        ).toEqual([publication.provider])
      })
    },
    { actor: alice.label, storageState: alice.storageState },
  )
})
