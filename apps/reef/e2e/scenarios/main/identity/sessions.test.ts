import { randomUUID } from 'node:crypto'
import { Code } from '@connectrpc/connect'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'
import { expect, test } from 'vitest'
import { createActors, loginActor, newUser } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients, registrationClient } from '../../../support/management.ts'
import { breadcrumb } from '../../../support/providers.ts'

test('Authenticated Users have distinct Coral identities and independent Reef sessions', async (context) => {
  const { owner, alice, bob } = await createActors(context)
  expect(new Set([owner.userId, alice.userId, bob.userId]).size).toBe(3)
  const aliceAgain = await loginActor(context, alice)
  expect(aliceAgain.userId, 'A new login retains the same Coral User identity').toBe(alice.userId)

  // coral-auth's session tests own the token's claims.
  for (const actor of [owner, alice, bob]) {
    const missing = { name: `workspaces/${randomUUID()}/agents/${randomUUID()}/registration` }
    // A validated identity reaches normal concealment; an absent or forged
    // credential must instead fail authentication. No resource is seeded.
    await expect(
      registrationClient(actor.accessToken).getAgentRegistration(missing),
    ).rejects.toMatchObject({ code: Code.NotFound })
    await expect(registrationClient().getAgentRegistration(missing)).rejects.toMatchObject({
      code: Code.Unauthenticated,
    })
    await expect(registrationClient('invalid').getAgentRegistration(missing)).rejects.toMatchObject(
      { code: Code.Unauthenticated },
    )
    await withBrowserSession(
      context,
      async ({ page, step }) => {
        await step(`Open Reef as ${actor.label}`, async () => {
          await page.goto('/')
          await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
        })
      },
      { storageState: actor.storageState, actor: actor.label },
    )
  }
})

test('Signing Alice out does not sign Bob out', async (context) => {
  const alice = await loginActor(context, await newUser(context, 'Alice'))
  const bob = await loginActor(context, await newUser(context, 'Bob'))
  await withBrowserSession(
    context,
    async ({ page: alicePage, step }) => {
      await step('Open Alice’s Reef session', async () => {
        await alicePage.goto('/')
        await alicePage.getByRole('button', { name: /^Open account menu for / }).waitFor()
      })
      await withBrowserSession(
        context,
        async ({ page: bobPage, step: bobStep }) => {
          await bobStep('Open Bob’s independent Reef session', async () => {
            await bobPage.goto('/')
            await bobPage.getByRole('button', { name: /^Open account menu for / }).waitFor()
          })
          await step('Sign Alice out', async () => {
            await alicePage.getByRole('button', { name: /^Open account menu for / }).click()
            await alicePage.getByRole('menuitem', { name: 'Sign out', exact: true }).click()
            await alicePage
              .getByRole('heading', { name: 'You are signed out', exact: true })
              .waitFor()
            const protectedPage = await alicePage.request.get('/', { maxRedirects: 0 })
            expect(protectedPage.status()).toBe(302)
            expect(protectedPage.headers().location).toMatch(/^\/login/)
          })
          await bobStep('Reload Bob’s session and remain signed in', async () => {
            await bobPage.reload()
            await bobPage.getByRole('button', { name: /^Open account menu for / }).waitFor()
            // Bob has no Workspace, so home renders instead of redirecting to one.
            const home = await bobPage.request.get('/', { maxRedirects: 0 })
            expect(home.status()).toBe(200)
            expect(await home.text()).toContain('No workspaces yet')
          })
        },
        { actor: bob.label, storageState: bob.storageState },
      )
    },
    { actor: alice.label, storageState: alice.storageState },
  )
})

test('Unverified email prevents grants, not eligible login or existing access', async (context) => {
  const actor = await loginActor(context, await newUser(context, 'Unverified person', false))
  const api = managementClients(actor.accessToken)
  const workspace = await api.workspaces.createWorkspace({
    displayName: 'Unverified profile',
    requestId: randomUUID(),
  })
  expect(workspace.currentUserProfile?.email).toBe(actor.email)
  await expect(
    api.memberships.createWorkspaceMembership({
      parent: workspace.name,
      email: actor.email,
      role: WorkspaceRole.MEMBER,
      requestId: randomUUID(),
    }),
  ).rejects.toMatchObject({ code: Code.NotFound })
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('Open the Workspace without a second email-verification ceremony', async () => {
        await page.goto(`/${workspace.name}`)
        await breadcrumb(page, 'Unverified profile').waitFor()
        // The Overview's compact member row carries the name and email as its hover title.
        await page.getByTitle(actor.email).waitFor()
        await page.getByRole('button', { name: 'Edit workspace name', exact: true }).click()
        await page.getByRole('textbox', { name: 'Workspace name', exact: true }).waitFor()
      })
    },
    { actor: actor.label, storageState: actor.storageState },
  )
})
