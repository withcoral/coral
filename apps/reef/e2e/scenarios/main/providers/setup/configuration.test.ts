import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { managementClients } from '../../../../support/management.ts'
import { CredentialRoute_Kind } from '../../../../generated/coral/management/v1/provider_pb.ts'
import { openApiDocument } from '../../../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  chooseFile,
  confirmConfiguration,
  createWorkspace,
  destinationField,
  openTool,
  signInPicker,
  waitForProviderStatus,
  committedPublication,
} from '../../../../support/providers.ts'

test('Update asks again for a tool-specific server whose saved choice is no longer offered', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const document = JSON.parse(
      openApiDocument(
        'Local selection',
        { metrics: {}, logs: {} },
        { server: 'https://api.example.test/v1' },
      ).toString(),
    )
    document.security = []
    document.paths['/logs'].servers = [
      {
        url: 'https://{site}/v2',
        variables: { site: { default: 'old.logs.test', enum: ['old.logs.test'] } },
      },
    ]
    let providerPath = ''
    await step('Publish a credential-free provider with two server declarations', async () => {
      const workspace = await createWorkspace(
        actor.accessToken,
        `Clear selection ${crypto.randomUUID()}`,
      )
      providerPath = await addProviderFromFile(
        page,
        workspace,
        Buffer.from(JSON.stringify(document)),
        'local_selection',
      )
      await waitForProviderStatus(page, { path: providerPath }, 'Ready', {
        available: 2,
        total: 2,
      })
    })
    await step('Retain the declaration but invalidate its saved variable', async () => {
      document.paths['/logs'].servers[0].variables.site = {
        default: 'new.logs.test',
        enum: ['new.logs.test'],
      }
      await page.goto(`${providerPath}/update`)
      await chooseFile(page, 'changed-enum.json', Buffer.from(JSON.stringify(document)))
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      expect(await destinationField(page, 'Server for /logs').inputValue()).toBe(
        'https://old.logs.test/v2',
      )
      // The saved value is no longer one the document offers, so the field
      // says so and Next waits rather than posting for the same answer.
      await page
        .getByText('Choose a value this document offers for site.', { exact: true })
        .waitFor()
      // Next stays reachable from the keyboard. Pressing it moves focus to the
      // field that holds it back and posts nothing.
      const next = page.getByRole('button', { name: 'Next', exact: true })
      expect(await next.isDisabled()).toBe(true)
      let posts = 0
      const countPost = (request: { method(): string }) => {
        if (request.method() === 'POST') posts += 1
      }
      page.on('request', countPost)
      await next.focus()
      await page.keyboard.press('Enter')
      await expect
        .poll(() =>
          destinationField(page, 'Server for /logs').evaluate(
            (field) => field === document.activeElement,
          ),
        )
        .toBe(true)
      page.off('request', countPost)
      expect(posts).toBe(0)
    })
    await step('Ask for the local server without falling back to the main server', async () => {
      // A tool's own server that lists URLs needs an answer, so clearing it
      // leaves Next waiting and the main server as it was.
      await destinationField(page, 'Server for /logs').fill('')
      expect(await destinationField(page, 'Main server').inputValue()).toBe(
        'https://api.example.test/v1',
      )
      await page.getByText('Choose a server for these tools.', { exact: true }).waitFor()
      expect(await page.getByRole('button', { name: 'Next', exact: true }).isDisabled()).toBe(true)
      await destinationField(page, 'Server for /logs').fill('https://new.logs.test/v2')
      await page.keyboard.press('Escape')
      await confirmConfiguration(page)
      await page.getByRole('button', { name: 'Save', exact: true }).click()
      await page.waitForURL((url) => url.pathname === `${providerPath}/settings`)
      await page.goto(providerPath)
      const drawer = await openTool(page, 'logs')
      expect(await drawer.getByText('Unavailable', { exact: true }).count()).toBe(0)
      await page.keyboard.press('Escape')
      await drawer.waitFor({ state: 'hidden' })
      await waitForProviderStatus(page, { path: providerPath }, 'Ready', {
        available: 2,
        total: 2,
      })
    })
  })
})

test('Update retains saved choices and requires acceptance only for incompatible configuration', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const api = managementClients(actor.accessToken)
    let providerPath = ''
    await step('Add a bearer provider with caller accounts', async () => {
      const workspace = await createWorkspace(
        actor.accessToken,
        `Update review ${crypto.randomUUID()}`,
      )
      providerPath = await addProviderFromFile(
        page,
        workspace,
        openApiDocument('Records', { listRecords: {} }, { security: 'bearer' }),
        'records',
        { account: 'own-later' },
      )
    })
    await step(
      'Keep saved bearer settings when the new document declares anonymous access',
      async () => {
        await page.goto(`${providerPath}/update`)
        const document = JSON.parse(
          openApiDocument('Records', { listRecords: {}, newReport: {} }).toString(),
        )
        document.security = []
        await chooseFile(page, 'records-new.json', Buffer.from(JSON.stringify(document)))
        await page.getByRole('button', { name: 'Read', exact: true }).click()
        await page.getByRole('button', { name: 'Save', exact: true }).waitFor()
        expect(await page.getByRole('textbox', { name: 'Display name', exact: true }).count()).toBe(
          0,
        )
        // The saved bearer setting stays, so nobody reconnects and Save asks nothing.
        await page.getByRole('button', { name: 'Save', exact: true }).click()
        await page.waitForURL((url) => url.pathname === `${providerPath}/settings`)
        await page.getByRole('button', { name: 'Edit configuration', exact: true }).click()
        await expect.poll(() => signInPicker(page).textContent()).toBe('Bearer token')
      },
    )
    await step(
      'An explicit incompatible edit warns, permits cancellation, and requires exact acceptance',
      async () => {
        await confirmConfiguration(page, 'No authentication', 'Save')
        const confirm = page.getByRole('dialog', { name: 'Save changes?' })
        await confirm
          .getByText(
            'Save removes the existing account credentials. To switch back, connect new accounts.',
            { exact: true },
          )
          .waitFor()
        // Keep editing publishes nothing; Save asks again.
        await confirm.getByRole('button', { name: 'Keep editing', exact: true }).click()
        await confirm.waitFor({ state: 'detached' })
        expect(await api.providers.getProvider({ name: providerPath.slice(1) })).toMatchObject({
          credentialRoute: { kind: CredentialRoute_Kind.CALLER },
        })
        await page.getByRole('button', { name: 'Save', exact: true }).click()
        let requestId = ''
        await page.route(
          '**/update.data*',
          async (route) => {
            if (
              route.request().method() !== 'POST' ||
              !route.request().postData()?.includes('publish')
            )
              return route.continue()
            const url = new URL(route.request().url())
            requestId = url.searchParams.get('requestId') ?? ''
            expect(url.searchParams.get('allowIncompatibleConfiguration')).toBe('yes')
            const response = await route.fetch()
            expect(await response.text()).toMatch(committedPublication)
            await route.abort('failed')
          },
          { times: 1 },
        )
        await confirm.getByRole('button', { name: 'Save', exact: true }).click()
        await page.waitForURL(
          (url) => Boolean(requestId) && url.searchParams.get('requestId') === requestId,
        )
        await page.reload()
        expect(
          await page.locator('input[name="allowIncompatibleConfiguration"]').inputValue(),
        ).toBe('yes')
        expect(await page.locator('input[name="requestId"]').last().inputValue()).toBe(requestId)
        await page.getByRole('button', { name: 'Retry', exact: true }).click()
        await page.waitForURL((url) => url.pathname === `${providerPath}/settings`)
      },
    )
  })
})
