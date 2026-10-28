import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../support/actors.ts'
import { managementClients } from '../../../support/management.ts'
import { openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  addedProviders,
  acceptTools,
  backToConfiguration,
  chooseAccount,
  chooseFile,
  confirmConfiguration,
  createWorkspace,
  openTool,
  openProviderSettings,
  setNamespace,
  waitForPublication,
  committedPublication,
} from '../../../support/providers.ts'

test('Editing configuration after a lost Add starts a new attempt and keeps the namespace', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    let lostRequest = ''
    await step('Lose the request before Coral accepts it', async () => {
      const workspace = await createWorkspace(actor.accessToken, `Revise ${crypto.randomUUID()}`)
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await chooseFile(page, 'revise.json', openApiDocument('Revise API', { hello: {} }))
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await confirmConfiguration(page)
      await acceptTools(page)
      await setNamespace(page, 'revised')
      await page.route(
        '**/new.data*',
        async (route) => {
          lostRequest = new URL(route.request().url()).searchParams.get('requestId') ?? ''
          await route.abort('failed')
        },
        { times: 1 },
      )
      await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      await page.waitForURL(
        (url) => Boolean(lostRequest) && url.searchParams.get('requestId') === lostRequest,
      )
      await page.reload()
      await page.getByRole('button', { name: 'Retry', exact: true }).waitFor()
    })
    await step('Review a revised configuration without reusing the lost request', async () => {
      await backToConfiguration(page)
      await confirmConfiguration(page, 'Bearer token')
      await acceptTools(page)
      expect(await page.locator('input[name="requestId"]').last().inputValue()).not.toBe(
        lostRequest,
      )
      await setNamespace(page, 'revised_caller')
      await chooseAccount(page, 'own-later')
      await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      await waitForPublication(page)
      await openProviderSettings(page)
      await page
        .getByText(
          'Each person connects their own account. Workspace owners connect accounts for agents.',
          { exact: true },
        )
        .waitFor()
    })
  })
})

test('Workspace creation and Update recover exact publication requests', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const workspace = await createWorkspace(
      actor.accessToken,
      `Update replay ${crypto.randomUUID()}`,
    )
    const api = managementClients(actor.accessToken)
    let providerPath = ''
    let frozenUrl = ''
    await step('Keep the namespace through configuration review', async () => {
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await chooseFile(page, 'replay.json', openApiDocument('Replay provider', { hello: {} }))
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await confirmConfiguration(page)
      await acceptTools(page)
      await setNamespace(page, 'replayed')
      await backToConfiguration(page)
      await confirmConfiguration(page)
      await acceptTools(page)
    })
    await step('Recover the committed creation with the same namespace', async () => {
      await page.route(
        '**/new.data*',
        async (route) => {
          frozenUrl = route.request().url()
          expect(new URL(frozenUrl).searchParams.get('namespace')).toBe('replayed')
          const response = await route.fetch()
          expect(await response.text()).toMatch(committedPublication)
          await route.abort('failed')
        },
        { times: 1 },
      )
      await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      await page.waitForURL(
        (url) =>
          Boolean(frozenUrl) &&
          url.searchParams.get('requestId') === new URL(frozenUrl).searchParams.get('requestId'),
      )
      await page.reload()
      expect(await page.getByRole('link', { name: /^Change/ }).count()).toBe(0)
      await page.getByRole('button', { name: 'Retry', exact: true }).click()
      await waitForPublication(page)
      providerPath = new URL(page.url()).pathname
      // The tool's full name carries the namespace the replay kept.
      const drawer = await openTool(page, 'hello')
      await drawer.getByText('coral.providers.replayed.hello', { exact: true }).waitFor()
      const listed = await api.providers.listProviders({ parent: `workspaces/${workspace}` })
      expect(addedProviders(listed.providers).map((provider) => provider.name)).toEqual([
        providerPath.slice(1),
      ])
    })
    await step('Recover Update using the original etag and configuration revision', async () => {
      await page.goto(`${providerPath}/update`)
      await chooseFile(
        page,
        'replay-update.json',
        openApiDocument('Replay provider', { hello: {}, later: {} }),
      )
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await page.getByRole('button', { name: 'Save', exact: true }).waitFor()
      const original = new URL(
        (await page.locator('form:has(input[value="publish"])').getAttribute('action'))!,
        page.url(),
      )
      frozenUrl = ''
      await page.route(
        '**/update.data*',
        async (route) => {
          frozenUrl = route.request().url()
          const response = await route.fetch()
          expect(await response.text()).toMatch(committedPublication)
          await route.abort('failed')
        },
        { times: 1 },
      )
      await page.getByRole('button', { name: 'Save', exact: true }).click()
      await page.waitForURL(
        (url) =>
          Boolean(frozenUrl) &&
          url.searchParams.get('requestId') === new URL(frozenUrl).searchParams.get('requestId'),
      )
      await page.reload()
      for (const key of ['etag', 'configurationRevision', 'requestId'])
        expect(await page.locator(`input[name="${key}"]`).last().inputValue()).toBe(
          original.searchParams.get(key),
        )
      const malformed = new URL(page.url())
      malformed.searchParams.delete('etag')
      expect((await page.request.get(malformed.toString())).status()).toBe(404)
      malformed.searchParams.set('etag', original.searchParams.get('etag')!)
      malformed.searchParams.set('configure', 'garbage')
      expect((await page.request.get(malformed.toString())).status()).toBe(404)
      await page.getByRole('button', { name: 'Retry', exact: true }).click()
      // An update returns to the provider's settings.
      await page.waitForURL((url) => url.pathname === `${providerPath}/settings`)
      await page.goto(providerPath)
      // The tool's full name carries the namespace the replay kept.
      const drawer = await openTool(page, 'hello')
      await drawer.getByText('coral.providers.replayed.hello', { exact: true }).waitFor()
      const listed = await api.providers.listProviders({ parent: `workspaces/${workspace}` })
      expect(addedProviders(listed.providers).map((provider) => provider.name)).toEqual([
        providerPath.slice(1),
      ])
      const tools = await api.providers.listTools({ parent: providerPath.slice(1) })
      expect(tools.tools.map((tool) => tool.relativePath).toSorted()).toEqual(['hello', 'later'])
    })
  })
})

test('Recover a lost Add response by replaying the same request identity', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const displayName = `Replay ${crypto.randomUUID()}`
    let workspace = ''
    let requestId = ''
    let committedBody = ''
    await step('Reach the last step', async () => {
      workspace = await createWorkspace(actor.accessToken, displayName)
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await chooseFile(page, 'replay.json', openApiDocument('Replay API', { hello: {} }))
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await confirmConfiguration(page)
      await acceptTools(page)
      await setNamespace(page, 'recovered')
    })
    await step('Lose only the response, after Coral commits', async () => {
      await page.route(
        '**/new.data*',
        async (route) => {
          if (route.request().method() !== 'POST') return route.continue()
          const body = route.request().postData() ?? ''
          if (!body.includes('publish')) return route.continue()
          requestId = new URL(route.request().url()).searchParams.get('requestId') ?? ''
          const response = await route.fetch()
          committedBody = await response.text()
          await route.abort('failed')
        },
        { times: 1 },
      )
      await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      await page.waitForURL(
        (url) => Boolean(requestId) && url.searchParams.get('requestId') === requestId,
      )
      expect(committedBody).toMatch(committedPublication)
    })
    await step('Archive the Workspace from another tab meanwhile', async () => {
      const other = await page.context().newPage()
      try {
        await other.goto(`/workspaces/${workspace}`)
        await other
          .getByRole('navigation', { name: 'Coral', exact: true })
          .getByRole('button', { name: `Archive ${displayName}`, exact: true })
          .click()
        await other
          .getByRole('dialog', { name: `Archive ${displayName}`, exact: true })
          .getByRole('button', { name: 'Archive workspace', exact: true })
          .click()
        await other.getByText('This workspace is archived', { exact: true }).waitFor()
      } finally {
        await other.close()
      }
    })
    await step('Retry replays the same identity and one provider exists', async () => {
      await page.reload()
      expect(await page.locator('input[name=requestId]').last().inputValue()).toBe(requestId)
      await page.getByRole('button', { name: 'Retry', exact: true }).click()
      await waitForPublication(page)
      expect(committedBody).toContain(new URL(page.url()).pathname)
      const rail = page.getByRole('navigation', { name: 'installed providers', exact: true })
      // Only the added provider has settings; Coral has none.
      const settings = rail.getByRole('link', { name: / settings$/ })
      await settings.first().waitFor()
      expect(await settings.count()).toBe(1)
    })
  })
})
