import { expect, test } from 'vitest'
import { authenticatedTarget, createActors, loginActor, newUser } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import { pickPerson } from '../../../support/members.ts'

test('Committed email grants cannot retarget replay after address reassignment', async (context) => {
  const { owner, alice } = await createActors(context)
  const api = managementClients(owner.accessToken)
  const workspace = await api.workspaces.createWorkspace({
    displayName: 'Stable email grant',
    requestId: crypto.randomUUID(),
  })
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      let submitted: Record<string, string> = {}
      await step('Add Alice and retain the exact submitted request', async () => {
        await page.goto(`/${workspace.name}/members`)
        await page.getByRole('button', { name: 'Add members', exact: true }).click()
        await pickPerson(page, alice.email)
        submitted = await page
          .getByRole('dialog')
          .locator('form')
          .evaluate(
            (form) =>
              Object.fromEntries(new FormData(form as HTMLFormElement)) as Record<string, string>,
          )
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Add member', exact: true })
          .click()
        await page
          .getByRole('button', { name: `Actions for ${alice.label}`, exact: true })
          .waitFor()
      })
      await step('Refresh Alice’s email and sign in its new holder', async () => {
        const { authentication } = authenticatedTarget()
        const email = `renamed-${alice.email}`
        const changed = await fetch(`${authentication.oidcURL}/fixture/accounts/${alice.subject}`, {
          method: 'PATCH',
          headers: { Origin: authentication.oidcURL, 'Content-Type': 'application/json' },
          body: JSON.stringify({ email }),
        })
        expect(changed.ok).toBe(true)
        const second = Math.floor(Date.now() / 1000)
        await expect
          .poll(() => Math.floor(Date.now() / 1000), { timeout: 2500, interval: 100 })
          .toBeGreaterThan(second)
        expect((await loginActor(context, { ...alice, email })).userId).toBe(alice.userId)
        const holder = await loginActor(
          context,
          await newUser(context, 'New address holder', true, alice.email),
        )
        const replay = await page.request.post(`/${workspace.name}/members`, {
          headers: { Origin: new URL(page.url()).origin },
          form: submitted,
        })
        expect(replay.status()).toBe(409)
        const members = await api.memberships.listWorkspaceMemberships({ parent: workspace.name })
        expect(members.memberships.some((m) => m.user === `users/${alice.userId}`)).toBe(true)
        expect(members.memberships.some((m) => m.user === `users/${holder.userId}`)).toBe(false)
      })
    },
    { actor: owner.label, storageState: owner.storageState },
  )
})

test('Ambiguous email grants keep the dialog and values, allowing a safe correction', async (context) => {
  const { owner, alice, bob } = await createActors(context)
  const api = managementClients(owner.accessToken)
  const workspace = await api.workspaces.createWorkspace({
    displayName: 'One-submit members',
    requestId: crypto.randomUUID(),
  })
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('Pick a person, then introduce a separately authenticated collision', async () => {
        await page.goto(`/${workspace.name}/members`)
        await page.getByRole('button', { name: 'Add members', exact: true }).click()
        await pickPerson(page, alice.email)
        await page.getByRole('button', { name: 'Role', exact: true }).click()
        await page.getByRole('menuitemradio', { name: 'Owner', exact: true }).click()
        const duplicate = await loginActor(
          context,
          await newUser(context, 'Different person', true, alice.email),
        )
        expect(duplicate.userId).not.toBe(alice.userId)
      })
      await step('Reject ambiguity without candidate disclosure or any grant', async () => {
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Add owner', exact: true })
          .click()
        await page
          .getByRole('dialog')
          .getByText('More than one person matches this email. Coral granted no access.', {
            exact: true,
          })
          .waitFor()
        await page
          .getByRole('dialog')
          .getByRole('button', { name: `Remove ${alice.label}`, exact: true })
          .waitFor()
        expect(await page.getByRole('dialog').locator('input[name=role]').inputValue()).toBe(
          'owner',
        )
        expect(
          (await api.memberships.listWorkspaceMemberships({ parent: workspace.name })).memberships,
        ).toHaveLength(1)
      })
      await step('Replace the person and show the actual person added', async () => {
        await page
          .getByRole('dialog')
          .getByRole('button', { name: `Remove ${alice.label}`, exact: true })
          .click()
        await pickPerson(page, bob.email)
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Add owner', exact: true })
          .click()
        // The dialog closes and the table lists the person; no toast repeats it.
        await expect.poll(() => page.getByRole('dialog').count()).toBe(0)
        await page.getByRole('button', { name: `Actions for ${bob.label}`, exact: true }).waitFor()
        const granted = (
          await api.memberships.listWorkspaceMemberships({ parent: workspace.name })
        ).memberships.find((m) => m.user === `users/${bob.userId}`)
        expect(granted?.role).toBe(1)
      })
    },
    { actor: owner.label, storageState: owner.storageState },
  )
})
